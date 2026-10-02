# Manual tester handoff — round 2 (CB-8 sweep follow-up)

Workspace: `/c/f2` (clean `git archive` export). `EXPORT_BASIS` confirms **`18ae8228c473acc07d30b7cff9cd5af07d12f9ef`**.
Env every run: `CARGO_TARGET_DIR=/c/f2/.t CARGO_INCREMENTAL=0 RETCD_TEST_DEADLINE_SCALE=3 RETCD_TEST_LOG_DIR=/c/f2/logs`.
One cargo invocation at a time, output to a file under `/c/f2/*.txt`, EXIT read from the file's
tail. No `git` run. Nothing deleted outside my own `.orig` backups, all of which are cleaned up
below.

**Baseline** (`/c/f2/run_baseline.txt`): full workspace test profile — clean, `gate: test OK`.
Proceeded.

## Round 1 summary (for context — not redone)

Round 1 (a separate export, `f22aa44`) ran five mutations against charter rows C0/H1/M1/I1.
F1 and F5 were CAUGHT outright. **F2, F3, F4 were MISSED** and each got a guard test in an
existing file, all still present in this tree:

- **F2** — scheduler tie-break (`sim/scheduler.rs::schedule`) had no test asserting pop order for
  two events at the same tick. Guard: `m7f_tie_break_at_equal_tick_pops_in_ascending_event_id_order`
  in `crates/rdb-sim/tests/dispatch.rs`.
- **F3** — `Clock::arm` had no test at all covering its stale-rearm guard. Guard:
  `m7f_stale_timer_rearm_is_refused_not_honoured` in `crates/rdb-sim/tests/dispatch.rs`.
- **F4** — `MemoryEngine::commit`'s "whole batch or none" claim was untestable because the only
  batch-building helper (`support::batch()`) always built a single-write batch. Guard:
  `m7f_a_crash_mid_batch_keeps_none_of_the_batchs_writes` in `crates/rdb-sim/tests/storage.rs`.

Round-1 verdict was **THUMBS UP**. The lead withdrew it after a sweep — prompted by my own F4
finding — found a fourth, missed gap: **CB-8**. `support::ctx()` (`tests/support/mod.rs`) hardcodes
`bound_established: true` on every `StepCtx` the whole suite builds, which makes
`ControlTime::compare`'s first gate (`crates/rdb-core/src/contracts/time.rs`) permanently
unreachable — zero test coverage, zero production callers reached. CB-8 is already routed;
I did not write a test for it and did not re-derive it below.

---

## TASK 1 — sweep of `crates/rdb-sim/tests/support/**` for frozen-choice fixtures

Read in full: `support/mod.rs`, `support/oracle.rs`, `support/oracle/model.rs`,
`support/oracle/checks/{atomicity,authority,dedup,lag,lineage,liveness,publication,version}.rs`,
`support/scenarios.rs`, `support/scenarios/{builder,grammar,gen,coverage,mutate,reduce}.rs`.

**16 fields/fixtures reviewed. 12 are frozen-choice shaped (a constant where the production type
allows a choice); 1 of those 12 is a genuine new, unreported, mutation-proven gap. The rest are
either the five items the lead pre-checked (no verdict owed), already routed (F4), or ruled out
with concrete evidence (dead field / by-design / documented).**

| # | Builder/const | File | Frozen field → value | Branch it forecloses | Reached another way? | Disposition |
|---|---|---|---|---|---|---|
| 1 | `ctx()` | `support/mod.rs` | `bound_established` → always `true` | `ControlTime::compare`'s first gate | No | **CB-8 itself — pre-flagged by lead, off-limits, no test** |
| 2 | `probe_event()` | `support/mod.rs` | `conditions`, `mutations` → always empty | scenario condition/mutation evaluation | N/A | **Pre-checked #1 — no verdict owed** |
| 3 | `cluster()` | `support/mod.rs` | partition count → always 1 | cross-partition code paths | N/A | **Pre-checked #2 — no verdict owed** |
| 4 | `rf3_config()` | `support/mod.rs` | shadow count → always 0 | Shadow-role branches | N/A | **Pre-checked #3 — no verdict owed** |
| 5 | `member()` | `support/mod.rs` | `boot` → always `BootId(1)` | boot-equality checks generally | N/A | **Pre-checked #4 — stated as fact, no verdict owed** |
| 6 | `BUDGETS` const | `support/mod.rs` | all fields → `Budgets::SPEC_DEFAULTS` | kernel budget-checking code | No | Subsumed under CB-8: `budgets` is only ever copied through (`harness/dispatch.rs:160`, `base.budgets`), never read by field, because the kernel ignores `StepCtx` entirely. Not separately flagged. |
| 7 | `SNAPSHOT` const | `support/mod.rs` | `EmptySnapshot`, always empty | kernel snapshot-consuming code | No | Same CB-8 root cause as #6. Not separately flagged. |
| 8 | `ROOT_DIGEST` const | `support/mod.rs` | digest → fixed value | none found | N/A | **Ruled out** — grepped all of `crates/`: zero consumers outside its own definition. A dead field has no branch to hide. |
| 9 | `control_effect()` | `support/mod.rs` | `from` → always `ModuleName::Authority` | none found | N/A | **Ruled out** — grepped `crates/rdb-sim/src` and `crates/rdb-core/src` for any read of `Effect.from`: zero. Nothing branches on it, so freezing it hides nothing. |
| 10 | `batch()` | `support/mod.rs` | `writes` → always exactly 1 | `storage/memory.rs::commit` multi-write atomicity | No, via this helper | **Already routed round 1 (F4)** — guard bypasses `batch()` directly. Not re-flagged. |
| 11 | `TraceBuilder::ack_from()` | `scenarios/builder.rs` | `peer_boot` (via `self.by(node, BootId(1))`) → always `BootId(1)` | `oracle/model.rs::durable_at_boot`'s boot-equality check, called from `oracle/checks/publication.rs:96` and `:165` | No — the one other `BootId(5)` literal in the tree (`tests/harness.rs` ~line 191–198) is inside a JSONL-serialisation-roundtrip test that never calls `Oracle::judge` | **NEW FINDING — mutation-proven MISSED (see below) — route via §14, no test written** |
| 12 | `TraceBuilder::flush()` | `scenarios/builder.rs` | `peer_boot` (via `self.by(node, BootId(1))`) → always `BootId(1)` | same as #11 | same as #11 | Same finding, second call site |
| 13 | `evidence()` free fn | `scenarios/builder.rs` | `boot` (const fn) → always `BootId(1)` | same as #11 | same as #11 | Same finding, third call site |
| 14 | `rf3()` free fn | `scenarios/grammar.rs` | node set → always exactly 3: Primary + 2×RegularSecondary, never a Shadow | Shadow/4th-node code paths | N/A | Same family as pre-checked #3 (`rf3_config`) — noted, not separately counted |
| 15 | `producer(boundary)` | `scenarios/gen.rs` | one fixed op literal per required boundary | remaining op shapes at that boundary | Yes, `draw()` covers some boundaries randomly | **Ruled out** — module doc states this is deliberate ("a promise to try"), disclosed behaviour |
| 16 | `draw()` | `scenarios/gen.rs` | weighted draw over only 6 of the grammar's op variants | the other op variants | Yes, via `producer()` for required boundaries | **Ruled out** — documented, not hidden |

### The one new finding, mutation-proved

**`boot` is frozen at `BootId(1)` everywhere a fixture reaches `Oracle::judge`.** `ack_from()`,
`flush()` (both in `TraceBuilder`) and the free function `evidence()` are the only three fixture
sites that construct ack/durability evidence, and all three hardcode `BootId(1)`. That makes the
boot-equality half of `oracle/model.rs::durable_at_boot` — consumed at
`oracle/checks/publication.rs:96` (`durable_ack_ungrounded`) and `:165` (ack-evidence counting) —
permanently unreachable for a genuine boot mismatch, through any row that exists today.

Proof — flipped the branch in the one function that reads it, `PartitionModel::durable_at_boot`
(`crates/rdb-sim/tests/support/oracle/model.rs:388-392`):

```rust
// before
pub fn durable_at_boot(&self, node: NodeId, boot: BootId, seq: Seq) -> bool {
    self.durable
        .get(&node)
        .is_some_and(|(b, durable_seq)| *b == boot && *durable_seq >= seq)
}

// after (mutant)
pub fn durable_at_boot(&self, node: NodeId, boot: BootId, seq: Seq) -> bool {
    let _ = boot;
    self.durable
        .get(&node)
        .is_some_and(|(_b, durable_seq)| *durable_seq >= seq)
}
```

`cargo test -p rdb-sim --test oracle --test campaign` → campaign 6/6, oracle 58/58, **0 failed**
(`/c/f2/run1_boot_mutant.txt`). **MISSED.** Reverted; `diff` against the `.orig` backup was empty
before the backup was deleted.

This is verification-owned code (`oracle/model.rs`, fed by `scenarios/builder.rs`), so per the
brief it is **reported for routing via a §14 row, not given an invented-id guard test.**

### Why I didn't chase three separate mutation-proofs

The brief asked to prove the worst three by mutation. Sweeping every field turned up exactly one
live, reachable-in-principle branch that a frozen fixture value hides (#11–13, one root cause).
The other frozen-choice candidates (#6–9) have **zero readers of the frozen field in production
code** — confirmed by exhaustive grep, not assumption — so there is no branch to flip; mutating a
field nothing reads proves nothing beyond what the grep already shows. I ran the one mutation that
targets a real branch rather than manufacturing two no-op mutations to hit a count.

---

## TASK 2 — three invariant mutations on foundation-owned, previously-unmutated code

All three target `crates/rdb-core/src/contracts/membership.rs`, load-bearing for kernel-b's
membership contract and not touched by round 1's M1–M5 or F1–F5.

| # | Invariant / claim | Edit | Command | EXIT | First failure | Verdict |
|---|---|---|---|---|---|---|
| 1 | `MemoryEngine::sync_wal_through` — durable is bounded by the least of captured/applied/short-flush (`crates/rdb-sim/src/storage/memory.rs:201`) | `AppliedSeq(capture.through.0.min(lineage.applied.0))` → `AppliedSeq(capture.through.0)` | `scripts/gate.sh test -p rdb-sim --test storage --test oracle --test campaign --test dispatch` | 0 | none — storage 7/7, dispatch 6/6, campaign 9/9, oracle 58/58, all green (`/c/f2/run2_capture_mutant.txt` before the guard existed) | **MISSED → guarded** |
| 2 | `PartitionConfig::copy_of` matches node **and** boot (finding K-F-21) (`membership.rs:176`) | `.find(\|member\| member.node == peer.node && member.boot == peer.boot)` → `.find(\|member\| member.node == peer.node)` | `scripts/gate.sh test -p rdb-core --test seams` | 101 | `m7f_16_a_member_node_at_another_boot_is_not_a_copy` FAILED, panic at `seams.rs:114`: "same node, new boot: not the copy it used to be (K-F-21)" (`/c/f2/run5_copyof_mutant.txt`) | **CAUGHT** |
| 3 | `PartitionConfig::required_regular` excludes the Primary (finding K-F-23) (`membership.rs:202`) | filter `RegularSecondary` only → `RegularSecondary \|\| Primary` | `scripts/gate.sh test -p rdb-core --test seams` | 101 | `m7f_17_required_regular_excludes_the_primary_and_the_shadow` FAILED, panic at `seams.rs:142`: `left: [N1, N2, N3], right: [N2, N3]` (`/c/f2/run6_requiredregular_mutant.txt`) | **CAUGHT** |

Revert verification: every mutated file was `cp FILE FILE.orig` before editing. After each
mutation, `diff FILE FILE.orig` was checked empty post-revert (Mutation 1's `memory.rs` diff was
confirmed clean when it was originally reverted; Mutations 2 and 3, both against `membership.rs`,
each confirmed empty via `diff` immediately after `cp .orig` → file). All three `.orig` backups
have been deleted; none remain on disk.

### Mutation 1 — guard written (the only MISSED result)

`sync_wal_through`'s own doc says durable is bounded by "the least of what was captured, what is
applied, and what a planned `ShortFlush` allows" — but no existing row in `storage.rs` ever
captured a prefix beyond what was actually applied, so the `applied` half of that bound was never
the binding one anywhere. Added to `crates/rdb-sim/tests/storage.rs`, after
`m7f_06_a_failed_commit_keeps_none_of_a_multi_write_batch`:

```rust
/// `sync_wal_through`'s own doc says the answer is bounded by "the least of what was captured,
/// what is applied, and what a planned `ShortFlush` allows" — but no other row in this file ever
/// captures a prefix beyond what was actually applied, so the `applied` half of that bound is
/// never the binding one anywhere else. Here it is: two batches committed (`applied == 2`), and
/// the capture claims `through: AppliedSeq(5)`, three sequences past anything the engine ever
/// wrote. A `durable` watermark past `applied` is a false claim of exactly the shape
/// `StorageOp::FalseDurable` exists to forbid, reached here through an over-claimed capture
/// rather than through that fault.
#[retcd_test]
fn m7f_18_a_captured_prefix_past_applied_is_bounded_by_applied() {
    support::preamble();
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=2 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("commit");
    }

    let durable = engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: PARTITION,
            generation: GENERATION,
            through: AppliedSeq(5),
        }])
        .expect("flush");

    assert_eq!(
        durable[0].through,
        DurableSeq(2),
        "the capture claimed through 5, but only 2 was ever applied: durable must not run \
         ahead of applied"
    );
    assert_eq!(
        engine.durable(PARTITION, GENERATION),
        DurableSeq(2),
        "the engine's own watermark must agree with what sync_wal_through reported"
    );
}
```

Proof 1 (RED on mutant, `/c/f2/run3_guard_on_mutant.txt`): `m7f_18_a_captured_prefix_past_applied_is_bounded_by_applied ... FAILED`, panic `assertion `left == right` failed: the capture claimed through 5, but only 2 was ever applied: durable must not run ahead of applied` — `left: DurableSeq(5)`, `right: DurableSeq(2)`. 7 passed, 1 failed (the guard's own assertion, not a compile error).

Proof 2 (GREEN on clean, `/c/f2/run4_guard_on_clean.txt`): 8 passed, 0 failed, `gate: test OK`.

Mutations 2 and 3 were CAUGHT by existing rows (`m7f_16`, `m7f_17`) — no guard needed.

---

## Frozen-choice gaps flagged for routing (§14, not tests)

1. **`boot` frozen at `BootId(1)`** across `TraceBuilder::ack_from()`, `TraceBuilder::flush()`,
   and the free function `evidence()` (all in `crates/rdb-sim/tests/support/scenarios/builder.rs`).
   Makes `oracle/model.rs::durable_at_boot`'s boot-equality check unreachable for a genuine
   mismatch anywhere it feeds `Oracle::judge` (`oracle/checks/publication.rs:96`, `:165`).
   Mutation-proven MISSED (campaign 6/6, oracle 58/58, 0 failed with the check removed). This is
   a distinct instance of the same freeze pattern as the lead's pre-checked `member().boot` item,
   but at a different call site (`scenarios/builder.rs`, not `support/mod.rs`), so it's reported as
   its own row rather than folded into the pre-checked one.

## Revert verification (all mutated files)

| File | Mutation | Reverted | `.orig` deleted |
|---|---|---|---|
| `crates/rdb-sim/tests/support/oracle/model.rs` | `durable_at_boot` boot check (TASK 1) | Yes, diff-clean | Yes |
| `crates/rdb-sim/src/storage/memory.rs` | `sync_wal_through` bound (TASK 2, #1) | Yes, diff-clean | Yes |
| `crates/rdb-core/src/contracts/membership.rs` | `copy_of` boot match (TASK 2, #2) | Yes, diff-clean | Yes |
| `crates/rdb-core/src/contracts/membership.rs` | `required_regular` filter (TASK 2, #3) | Yes, diff-clean | Yes |

## Not covered

- **CB-8 itself** — not re-derived, not tested. Off-limits per the brief; already routed.
- The five lead-pre-checked items (`probe_event`, `cluster`, `rf3_config`, `member().boot`,
  `ctx()`) — reviewed only to confirm they're the same shape, no independent verdict rendered.
- `scenarios/{coverage,mutate,reduce}.rs` — read in full, ruled out as fixture candidates
  (enumeration-driven or design-owned oracle-self-test machinery), not mutation-tested since
  they are not fixture builders in the TASK 1 sense.
- No scenario-level end-to-end replay of the boot-freeze finding through a full campaign row —
  the mutation targets the one function (`durable_at_boot`) that reads the frozen field directly,
  which is sufficient to prove unreachability but doesn't show what a *fixed* fixture would look
  like. That's routing/design work, out of scope here.
- Did not re-mutate `head_digest`/`AppendOutcome`/`KernelEffect: Copy` (round 1's M2/M4/M5) —
  unchanged since round 1, not re-verified.

## Verdict: THUMBS UP
- Basis: 18ae822
- Round: 2 (round 1 verdict withdrawn by the lead after the CB-8 sweep)
- Scope tested: TASK 1 sweep of all fixture builders/consts under `crates/rdb-sim/tests/support/**`
  (16 fields reviewed); TASK 2 mutations against `crates/rdb-core/src/contracts/membership.rs`
  (`copy_of`, `required_regular`) and `crates/rdb-sim/src/storage/memory.rs` (`sync_wal_through`),
  exercised through `crates/rdb-core/tests/seams.rs` and `crates/rdb-sim/tests/{storage,oracle,
  campaign,dispatch}.rs`. Every mutated file reverted and diff-verified clean; every `.orig`
  backup deleted; `/c/f2` used throughout, no `git` run by me.
- Blocking: none. TASK 2's one MISSED result (`sync_wal_through`'s applied-bound) now has a guard
  test proven RED-on-mutant/GREEN-on-clean. TASK 1's one new finding (boot frozen at `BootId(1)`
  in the oracle fixture pipeline) is a real gap but is verification-owned scaffolding, not a
  foundation kernel/storage defect, and is routed via §14 rather than blocking this pass.
- Not covered: see the "Not covered" section above — CB-8 itself, the five pre-checked items,
  scenario-generation machinery, and end-to-end replay of the boot-freeze fix.
