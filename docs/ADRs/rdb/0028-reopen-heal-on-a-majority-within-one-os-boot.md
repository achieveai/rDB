# ADR-rdb-0028: Reopen heal on a majority within one OS boot

**Status:** Proposed
**Date:** 2026-11-20
**Spec:** `docs/rdb/design-specification.md` §5.2 steps 3, 6 and 7 (ack before fsync), and the
2026-11-20 R5 amendment after the takeover steps.
**Extends:** [ADR-rdb-0009](0009-lineage-and-recovery.md) (recovery and its modes).
**Amends:** [ADR-rdb-0022](0022-formation-by-declaration.md) decision 5, "the heal waits for
every member's boot", for the case below only.
**Issue:** #169 (t1's R5 row: a restarted owner waits for a down copy). PR #181, review round
1, lead ruling A (2026-11-20).

## Context

- An owner that restarts runs the reopen's heal. Until R5 the heal waited for the boot of every
  non-shadow member its records pin. One copy that was down kept every partition that copy
  shared with the owner unopened, until it came back or an operator promoted with `--drop`.
- An owner acknowledges a write once it is in its own WAL, unsynced, and applied on one regular
  secondary. The sync comes later, in groups (spec §5.2 steps 3, 6 and 7).
- A process restart on the same OS boot keeps the WAL, its unsynced tail included: the OS still
  holds those pages. A host crash or reboot may lose that tail, and the acknowledged writes in it
  may then sit only on the down copy.
- No control record proves which boot a down copy runs now. A frozen or expired `grants/{C}` can
  name an older process of a node that has already started again. So the heal cannot tell "down
  since before the restart" from "running again with a late link". (Review threads on blocker 1.)

## Decision

1. **The policy is one predicate.** `admin::may_leave_out(os_boot, live_readd)` decides whether
   the heal may leave out any down copy at all. It allows that only when both hold:
   - the owner's copy runs on the same OS boot as its last ready open (`OsBoot::Same`);
   - `Timeouts::live_readd` (#107) is on for this open.
   Otherwise the heal waits for every copy, as before R5. It logs `heal_wait_all` with reason
   `os_boot_changed`, `os_boot_unknown`, `unsupported_platform` or `live_readd_off`. A later
   change of policy, such as live re-add on by default, edits this one function.
2. **When it may, a down copy is left out only with evidence and a majority.** These hold for
   every partition that pins it:
   - a strict majority of the pinned non-shadow members, the owner included, is up;
   - no partition still to form pins it;
   - its grant is frozen, or its expiry is proven on the owner's clock (`expiry_proven`).
   A copy with no grant record is left out once one heal window has passed since the heal
   began.
3. **Every left-out copy is noted at `UNLINKED_BOOT`**, never at the boot its grant names. The
   owner records it for #107's live re-add as an unreachable drop. Its first link back is then
   a live return, whatever boot it reports, so a copy that never stopped and one that started
   again come back the same way.
4. **The OS boot marker is per copy**, `nodes/<n>/os_boot`. It is written only once an open of
   that node is ready with its heal done. It is removed at open when it differs from the OS's
   id, is empty, or cannot be compared. Node processes that share a folder keep one each.

## Consequences

- **#169 is fixed only with `live_readd` on.** The default stays off, so a default build waits as
  before. SBX runs its gateway with it on (lead, 2026-11-20).
- **No acknowledged write is lost by leaving a copy out**, within one OS boot:
  - the owner's WAL holds every write it acknowledged;
  - the new generation pins its members' boots, so the left-out copy's acknowledgements no
    longer count;
  - the copy cannot become owner without freezing the owner's live grant.
  The cost is availability: the partition runs on fewer copies until the copy is back.
- **The left-out return is memory only**, like a drop's. If the owner restarts again first, the
  copy returns only at its own node's next restart.
- **Mixed settings.** Each step is the owner's own setting: leaving out, then re-adding. On this
  build the returning copy marks a refused landing only with its own setting on. A copy with it
  off can therefore leave a re-added partition read-only until that copy restarts; no write is
  lost. With protocol 10 (#170) the owner's setting travels with the landing and this case goes
  away. No startup refusal is added: the handshake does not carry the setting.
- **Rollback to wait-for-every-copy needs no repair.** A generation this heal committed without a
  copy has the same shape a drop leaves: a pinned set without that copy. Builds before R5
  already handle that shape. Such a build heals with the members the record pins, which no longer
  include the left-out copy. That copy returns by the restart path (`left_out`, rebuilt from the
  placed seats), as after a drop. Nothing in the record or on disk is new.
  - No Accepted ADR says the heal waits for every copy.
  - ADR-rdb-0022 (Proposed) says so in decision 5. This ADR amends that decision for one case
    only: same OS boot with `live_readd` on.
- **Known limits.** A VM or disk snapshot restore, or network storage that drops its write cache
  without a reboot, keeps the OS boot id while losing the tail. The operator deletes the node's
  `nodes/<n>/os_boot` before starting it.

## Verification

- Predicate and evidence:
  - `admin::tests::r5_b_same_os_boot_leaves_out_a_copy_proven_down` (noted at `UNLINKED_BOOT`);
  - `r5_c_the_heal_waits_for_every_copy_without_its_conditions`;
  - `r5_o_with_live_readd_off_the_heal_waits_and_a_late_start_is_pinned`;
  - `r5_s_a_partition_still_to_form_leaves_no_copy_out`;
  - `r5_t_two_pinned_members_with_one_down_is_no_majority`;
  - `r5_q_a_leave_out_answer_past_the_deadline_is_retried_not_failed`.
- Across processes:
  - `db::formation_tests::r5_a_restarted_owner_heals_with_a_majority_when_the_os_did_not_reboot`;
  - `r5_b_a_copy_left_out_by_the_heal_rejoins_when_it_starts_again`;
  - `r5_c_after_a_reboot_the_heal_waits_and_the_boot_is_recorded_only_once_ready`.
- Live return: `host::drop_tests::r5_j_a_copy_the_heal_left_out_with_no_grant_returns_live_at_its_boot`.
- Marker:
  - `os_boot::tests::r5_h_the_os_boot_file_decides_same_changed_or_unknown`, and `r5_k` to `r5_n`;
  - `db::reopen_tests::r5_o_a_node_that_leads_nothing_never_stamps_an_owners_os_boot`.
- Each row names its cut in its doc comment.

## References

- Spec §5.2; the R5 amendment in `docs/rdb/design-specification.md`; `docs/rdb/embed.md`, the
  R5 amendment under node failures.
- ADR-rdb-0009 section 7: the prerequisites for `live_readd`.
- Issue #107: live re-add, and its table of what is still open before default-on.
- PR #181: review round 1 and lead ruling A.
