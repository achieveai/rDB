# ADR-rdb-0021: Check B — a floor guard on landing and on the inventory link

**Status:** Accepted by the owner, 2026-11-15 ([#81 comment](https://github.com/achieveai/rDB/issues/81#issuecomment-6894324501)). Records the lead ruling on PR #118, 2026-11-15, product stage Development.
**Date:** 2026-11-15
**Spec:** `docs/rdb/design-specification.md` §8.1, §8.2
**Decided by:** lead ruling on PR #118, 2026-11-15, product stage Development
([issuecomment-6892114167](https://github.com/achieveai/rDB/pull/118#issuecomment-6892114167)).
**Extends:** [ADR-rdb-0010](0010-generation-inheritance.md) decision 8 (its two callers, landing and
the inventory link). ADR-rdb-0010 is Accepted, so this ADR adds a guard without editing it.
**Issue:** #107 slice 1.

## Context

- ADR-rdb-0010 decision 8 has two callers that link a new generation onto an older one: landing
  (`Host::land_at`) and the inventory link (`Host::inherit_missed_link`). Each runs its guards in a
  fixed order, ending with the depth guard, and writes nothing on a refusal.
- None of those guards checks that the older generation's lineage is whole below its head. A copy
  holding only seqs 3-4 of generation 1 passes every one of them, and the new generation would be
  linked over the hole at seqs 1-2.
- Issue #107 adds a live re-add of a copy. A re-added copy reaches both callers, so the hole has to
  be refused there before re-add can rely on them.

## Decision

1. **Check B runs in both callers, after the depth guard and before anything is written.** The older
   generation's lineage must reach seq 1 through its chain (`Host::has_floor`). If it does not, the
   link is refused by name and nothing is written.
2. **The empty-lineage exception.** A lineage that applied nothing has no seq 1 and passes. An empty
   copy's generation 0, which landing links from at head 0, is that case.
3. **Only seq 1 is read.** `History` falls through a copied level, so one read of seq 1 answers for
   every level of the chain. No other seq is read.
4. **A read error refuses.** A seq 1 read that fails is not treated as a pass. The caller logs the
   fault at error level and refuses, writing nothing.
5. **The reasons.**

   | Caller | Holed lineage | seq 1 read failed | Log line on the read failure |
   |---|---|---|---|
   | landing (`land_at`) | `from_has_no_floor` | `history_read_failed` | `land_floor_read_failed` |
   | inventory (`inherit_missed_link`) | `missed_generation_from_has_no_floor` | `missed_generation_history_read_failed` | `missed_generation_floor_read_failed` |

   When a tail above `base` was released or adopted for the ask (issue #105), the inventory's
   refusal answers `TailHeld` with the same reason instead of a bare `InventoryFailed`.
6. **Refusal-only, and on regardless of the switch.** Check B only refuses; it never repairs, copies
   or writes. It runs whether or not the #107 live re-add switch (`live_readd`) is set.
7. **Adoption is out of scope.** `Adopt::Link` and `Adopt::Resume` do not run check B. Issue #121
   tracks check B at adoption.

## Consequences

- A copy with a hole below its head can no longer be linked by landing or by the inventory. It is
  refused by name.
- A transient `History` read fault refuses a link that would otherwise have passed. That costs a
  retry, not a wrong link.
- Check B depends on seq 1 being kept. A history trim (#61) must revisit this ADR.
- Adoption can still take a holed lineage until #121 lands. (Closed by the 2026-11-19 amendment.)

## Verification

Rows in `crates/rdb-api/src/host/catchup_tests.rs`:

- `issue107_no_floor_no_link`: a holed lineage at the base is refused `from_has_no_floor` by landing
  and `missed_generation_from_has_no_floor` by the inventory, and neither writes anything.
- `issue107_a_landing_above_a_holed_head_refuses_and_writes_nothing`: a landing base above a holed
  head is refused `from_has_no_floor`, with nothing written. It is red when check B is limited to
  `head == base`.
- `issue107_a_landing_whose_floor_read_fails_refuses_and_writes_nothing`: landing refuses
  `history_read_failed`, logs `land_floor_read_failed`, and writes nothing.
- `issue107_an_inventory_whose_floor_read_fails_refuses_and_writes_nothing`: the inventory refuses
  `missed_generation_history_read_failed`, logs `missed_generation_floor_read_failed`, and writes
  nothing.

## Amendment 2026-11-19: check B at adoption (issue #121; proposed on its PR, for the reviewer)

Decision 7 is replaced. Adoption runs check B too.

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

- [ADR-rdb-0010](0010-generation-inheritance.md) decision 8; [ADR-rdb-0009](0009-lineage-and-recovery.md)
  sections 7 and 8 (the landing and inventory notes, amended 2026-11-15).
- `crates/rdb-api/src/host.rs`: `Host::land_at`, `Host::inherit_missed_link`, `Host::has_floor`.
- PR #118; issues #107, #121, #61.
