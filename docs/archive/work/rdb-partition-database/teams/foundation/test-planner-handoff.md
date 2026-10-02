# Test-planner handoff — team foundation, round 1

**Role:** test planner, team foundation (packages C0, H1, M1, I1).
**Date:** 2026-09-20.
**Written against:** the landed code at **`ec610f4`** ("K-F-39 partition config refuses a zero
threshold on decode") — the newest commit to touch `crates/rdb-core/src/contracts/`, and the
plan's declared `drift-basis`. Rows were first written against `6893442` and re-read against
`ec610f4`; §5a below records what that re-read found.

## 1. Outcome

**COMPLETED.** Every foundation acceptance criterion in `charter.md` is now a named, checkable
row. `docs/testing/test-plan-m7-foundation.md` exists, carries rows `M7F-01` … `M7F-49` with no
gaps and no duplicates, declares `<!-- drift-basis: ec610f4 -->`, and its §16 gate map has
**zero** unmapped criteria.

## 2. Files written

| Path | What |
|---|---|
| `docs/testing/test-plan-m7-foundation.md` | the plan — 19 sections + §15.1, 49 rows, 76 test functions, 7 Q-rows, `drift-basis: ec610f4` |
| `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/foundation/test-planner-handoff.md` | this file |

Nothing else was touched. No code, no `design.md`, no ADR, no other team's plan, no git operation,
no cargo invocation. `Edit`/`Write` only; no `sed -i`, no `perl -i`.

## 3. Counts

| | |
|---|---|
| Rows | **49** (`M7F-01` … `M7F-49`, contiguous) |
| Test functions | **76** |
| Landed | **53** = 51 cargo test functions at `ec610f4` + 2 landed gate stages (`M7F-27`, `M7F-28`) |
| Owed | **23** |
| Blocked (`Unavailable` until a package lands) | **10** — 8 on I1, 2 on H1 |
| Classes | 54 `unit`, 19 `sim`, 3 `script` |
| Q-rows | **7**, `Q-58` … `Q-64` |

Per package: C0 rows `02, 03, 04, 14, 16, 17, 29, 36, 37, 38, 39, 40, 41, 48, 49`; H1 rows
`05, 10, 12, 13, 15, 20, 23, 24, 43, 47`; M1 rows `06, 07, 08, 18, 44, 45, 46`; I1 rows
`01, 09, 11, 19, 21, 22, 25, 26, 30…35`; gate rows `27, 28, 42`.

## 4. Commands run, and their observed output

All run from the repository root through the `Bash` tool. No cargo.

```
$ grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | wc -l
47
$ grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | head -1
M7F-01
$ grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | tail -1
M7F-47

$ seq -f 'M7F-%02g' 1 47 > /tmp/expect
$ grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | diff - /tmp/expect
              (no output — the set equals the span exactly: no gap, no duplicate, nothing beyond 47)

$ grep -o 'Q-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | tr '\n' ' '
Q-34 Q-40 Q-41 Q-45 Q-46 Q-54 Q-55 Q-56 Q-57 Q-58 Q-59 Q-60 Q-61 Q-62 Q-63 Q-64
$ grep -o '\*\*Q-[0-9][0-9]\*\*' docs/testing/test-plan-m7-foundation.md | sort -u | wc -l
7
$ grep -o '\*\*Q-[0-9][0-9]\*\*' docs/testing/test-plan-m7-foundation.md | sort -u | head -1
**Q-58**
              (the seven bolded ids are this plan's own Q-rows: Q-58..Q-64. Q-34..Q-57 appear
               only as prose naming the other three teams' allocations, never as a row here)

$ grep -c '^fn m7f_\|^fn b_r30_' crates/rdb-core/tests/contracts.rs crates/rdb-core/tests/seams.rs \
      crates/rdb-sim/tests/harness.rs crates/rdb-sim/tests/control.rs \
      crates/rdb-sim/tests/dispatch.rs crates/rdb-sim/tests/storage.rs
crates/rdb-core/tests/contracts.rs:21
crates/rdb-core/tests/seams.rs:4
crates/rdb-sim/tests/harness.rs:5
crates/rdb-sim/tests/control.rs:7
crates/rdb-sim/tests/dispatch.rs:6
crates/rdb-sim/tests/storage.rs:6
$ grep -h '^fn m7f_\|^fn b_r30_' crates/rdb-core/tests/*.rs crates/rdb-sim/tests/*.rs | wc -l
49
              (matches the developer's reported "49 passed, 0 failed" and the commit gate)

$ grep -h '^fn m7f_\|^fn b_r30_' crates/rdb-core/tests/*.rs crates/rdb-sim/tests/*.rs \
    | sed 's/^fn //; s/().*//' | sort -u \
    | while read -r n; do grep -q "$n" docs/testing/test-plan-m7-foundation.md || echo "MISSING $n"; done
              (no output — every one of the 49 landed function names appears in the plan verbatim)

$ tok=$(printf 'part'; printf 'db'); grep -ci "$tok" docs/testing/test-plan-m7-foundation.md
0
              (the legacy prefix appears nowhere)
```

Shapes read from the code to settle the three drifted types (§7 below):

```
$ sed -n '/^pub enum EventKind/,/^}/p' crates/rdb-core/src/contracts/event.rs | grep -E '^    [A-Z]'
    Client(ClientEvent), Node(NodeLifecycle), Transport(TransportEvent), Storage(StorageEvent),
    Control(ControlEvent), Timer(TimerFired), ExternalFenceVerified {          -> seven variants
$ sed -n '/^pub enum AppendReject/,/^}/p' crates/rdb-core/src/contracts/envelope.rs | grep -cE '^    [A-Z]'
16
$ sed -n '/struct PartitionConfig/,/^}/p' crates/rdb-core/src/contracts/membership.rs
    partition, config_version, members, min_regular_acks     (behind serde try_from Unvalidated…)
$ TraceKind::ProtectionState (trace.rs:1026)
    phase, oldest_unsafe_age_ms, required_copy_set, config_version,
    paused_prefix_seq, resume_barrier_seq, healthy_since_tick   -> seven fields, no quorum_rule
```

## 5a. Drift-basis re-read: `6893442` -> `ec610f4` (NOT a no-op)

The lead asked me to confirm rather than assume. Confirmed, and it was **not** a no-op. Three
things moved, two of which would have made the plan wrong.

```
$ git log --oneline -2 -- crates/rdb-core/src/contracts/
ec610f4 fix(rdb-core): K-F-39 partition config refuses a zero threshold on decode
6893442 feat(rdb): foundation correction round 1 (K-F-01..38, F-R6..F-R12 closed)
$ git diff --stat 6893442 ec610f4
 crates/rdb-core/Cargo.toml                  |   2 +
 crates/rdb-core/src/contracts/membership.rs |  45 +++-
 crates/rdb-core/tests/seams.rs              |  31 +++
 ... (plus four other teams' plans and the progress pieces)
```

| What moved | Would the plan have been wrong? | Applied |
|---|---|---|
| `PartitionConfig` gained `#[serde(try_from = "UnvalidatedPartitionConfig")]` plus a private shadow struct whose `TryFrom` calls the existing `validate` | **No.** The four public fields are unchanged, so no row's literals move. I had in fact already read the post-`ec610f4` shape — my working-tree read on the first pass showed the `try_from` attribute — but I had cited it against the wrong commit | `M7F-39` reworded: it owns the **constructor** path; the decode path is now its own rows |
| **two new landed test functions** in `seams.rs`: `k_f_39_a_zero_threshold_is_refused_on_deserialisation`, `k_f_39_a_valid_threshold_deserialises` | **Yes.** The plan claimed 49 landed functions and that every landed name appears in it. Both claims were false at `ec610f4`, and my own §17 command 5 would have caught it on the next run | **rows added: `M7F-48` and `M7F-49`** (§10.2) — a refusing row and its accepting twin, which §2 rule 2 required anyway. Counts rebased to 51 landed / 76 functions / 49 rows |
| `crates/rdb-core/Cargo.toml` gained `serde_json` | **Nearly.** It is in **`[dev-dependencies]`**, so `M7F-42`'s "`[dependencies]` is exactly the five ADR-rdb-0002 names" still holds — but only because the row reads that one section. An implementer grepping the whole manifest would now fail on `serde_json`, and on `config-log`, which `rdb-core` has always dev-depended on and legitimately may | `M7F-42` clause 1 made explicit about the section boundary, and about the direction ADR-rdb-0002 actually forbids (`config-*` -> `rdb-*`, not the reverse) |
| everything else in `contracts/` | unchanged | every other §15 row re-read at `ec610f4` and still holds: `EventKind` seven variants, `AppendReject` sixteen, `ProtectionState` seven fields with no `quorum_rule`, `Provenance` and `OpSkipped` present, still no `harness::trace::validate` |

Recorded in the plan as **§15.1**, with the commands above. Basis line added at line 1 only after
this re-read, not before.

Also checked and deliberately excluded: `crates/rdb-sim/tests/support/oracle.rs` and
`support/scenarios.rs` are modified in the working tree beyond `ec610f4`. Those are team
verification's files; no foundation row reads them.

Commands re-run after the rebase, all green:

```
$ grep -o 'drift-basis: [0-9a-f]*' docs/testing/test-plan-m7-foundation.md
drift-basis: ec610f4
$ git log --format=%h -1 -- crates/rdb-core/src/contracts/
ec610f4                                   (the declared basis IS the newest contracts commit)

$ grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | wc -l
49
$ seq -f 'M7F-%02g' 1 49 > /tmp/e
$ grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | diff - /tmp/e
                                          (no output: contiguous 01..49, no gap, no duplicate)

$ grep -cE '^fn (m7f_|b_r30_|k_f_)' crates/rdb-core/tests/contracts.rs crates/rdb-core/tests/seams.rs       crates/rdb-sim/tests/harness.rs crates/rdb-sim/tests/control.rs       crates/rdb-sim/tests/dispatch.rs crates/rdb-sim/tests/storage.rs
21 6 5 7 6 6                              (= 51; seams.rs went 4 -> 6 at ec610f4)

$ grep -hE '^fn (m7f_|b_r30_|k_f_)' crates/rdb-*/tests/*.rs | sed 's/^fn //; s/().*//' | sort -u     | while read -r n; do grep -q "$n" docs/testing/test-plan-m7-foundation.md || echo "MISSING $n"; done
                                          (no output: all 51 landed names present)

$ grep -o '**Q-[0-9][0-9]**' ... | sort -u | wc -l   -> 7, first is **Q-58**
$ legacy-prefix grep                                 -> 0
```

## 5. Corrections received mid-task and how each was applied

| From | Correction | Applied |
|---|---|---|
| lead | Q-rows start at **Q-58**, not Q-57; kernel-b holds Q-46…Q-57 inclusive | already implemented before the message arrived; verified mechanically above. `Q-57` appears in the plan only as prose naming kernel-b's allocation (§12 preamble, §15 drift row, §18 Q-1) |
| critic r2 | `ProtectionState`, `AppendReject`, `PartitionConfig` are described wrongly in `design.md` §4.6, §4.8, §4.10; the **code is right** | three new §15 drift rows written from the code. `M7F-40` asserts **sixteen** `AppendReject` variants against a literal list, with the note that a row written from the design's five would pass and never notice the other eleven. No row names `quorum_rule` (already the case under F-R13); the §15 row now also lists `ProtectionState`'s seven actual fields. `M7F-39`'s "refused by every path" claim is now grounded in the landed `#[serde(try_from = "UnvalidatedPartitionConfig")]` |
| critic r2 | `design.md` §4.1 lists **six** `EventKind` variants; the code has **seven** (A-R23's `ExternalFenceVerified`); a total match written from the design will not compile | `M7F-37` gained a second clause: an exhaustive `match` over `EventKind` with **no `_` arm** naming all seven. A wildcard arm would compile and silently stop catching the eighth variant, so the row forbids it. New §15 drift row |
| critic r2 | split `M7F-05` — its scheduler half is assertable today and the scheduler has no row of its own | **done.** `M7F-05` keeps the replay claim (the half the ledger's 22:06 entry cites); the scheduler takes the new id **`M7F-47`**. No sub-case letters on either. §15 records the split and why: one id may not mean one blocked claim and one buildable claim at once, or the buildable row is hidden behind the blocked one in §14 and §16 |
| lead | the architect is on a scoped round to reconcile those `design.md` sections; do not wait | not waited for. Every row is written from the code at `6893442`, so the reconciliation can change only the document a row cites, never a row. §15 says this explicitly |
| critic r2 | `design.md` **§8's row table** was checked and is accurate | §8 is cited as written; `M7F-01`…`M7F-22` are taken from it unchanged |
| lead | a gate stage now fails any M7 plan whose declared contract basis is not the newest commit to touch `contracts/`; add `<!-- drift-basis: ec610f4 -->`, but **confirm** rather than assume it is a no-op | confirmed, and it was **not** a no-op — see §5a. Marker added at line 1, after the re-read |
| lead | Q-4 (the trace validator surface) **approved**: a real foundation ask recorded before this plan existed, raised after the charters were written rather than outside their scope | `M7F-30`…`M7F-35` **stand; their ids do not retire.** Recorded in §9 and §18 Q-4 of the plan, which is now closed rather than open |
| lead | both flagged items handled — verification's stale sections are being refreshed by another agent, and the two contract asks were independently confirmed landed in foundation round 1 | §8 below updated; no action left on my side |

## 6. Assumptions (each is a decision I made and can be reversed cheaply)

1. **Q-rows start at Q-58.** My original assignment said Q-57. The committed kernel-b plan owns
   Q-57 (§11 header "Q-46..Q-57", its Q-57 row, its §14 checklist, its §16 count). "Never reuse a
   number from another team" is the harder rule, so Q-55/Q-56/Q-57 are left stranded rather than
   collided with. The lead has since confirmed Q-58.
2. **`M7F-01`…`M7F-22` are frozen and sub-cased, not renumbered.** They are cited by id from
   `design.md` §8, `architect-handoff.md` §10.6, the ledger, and kernel-b's `M7B-26`. Thirty-eight
   of the 49 landed functions sit under eleven of those ids, so each landed function gets a
   sub-case letter (`M7F-02(a)`…`(k)`), the verification plan's own device (`M7V-03(a)`/`(b)`).
   From `M7F-23` upward one row is one test, no sub-cases.
3. **Three landed functions keep their non-`m7f_` names**: `b_r30_…` (`M7F-39`) and, since
   `ec610f4`, the two `k_f_39_…` (`M7F-48`, `M7F-49`). Renaming is a code edit I do not own, and
   it would break the `@m` values already in the JSONL logs that §12's queries read. The count
   greps match all three prefixes, so the numbers stay mechanical.
4. **Two new test files**, `crates/rdb-sim/tests/sim.rs` and `tests/replay.rs`, both named by the
   charter and neither existing yet. `memory.rs`, also charter-named, is **not** added: `storage.rs`
   is its content under another name and a second file would split M1's rows for nothing.
5. **`harness::trace::validate` is a new surface** in `rdb-sim` (§9, rows `M7F-30`…`M7F-35`).
   No charter line names it. **Approved by the lead** during this task: it is a real foundation ask
   recorded before the plan existed, raised after the charters were written rather than outside
   their scope. The ids stand and do not retire. No longer an open question.
6. **Three rows are `script` class** (`M7F-27`, `M7F-28`, `M7F-42`) — gate stages and greps, not
   cargo tests. No other plan has this class. Making them cargo tests would nest a `cargo metadata`
   inside a running cargo against one target dir, which is the `AGENTS.md` hazard.
7. **`M7F-28`'s negative fixture lives outside the repository** — a throwaway workspace with a
   `config-bad` crate dev-depending on an `rdb-oops` crate. Nothing is added to the working tree.

## 7. What I could not cover, and why

| Not covered | Why |
|---|---|
| kernel-b's **CB-1…CB-4** (`EventKind::Kernel`/`EffectKind::Kernel` carrier pair; `AppendReject::NeedPrefix{have, head_digest}`; `AckRejectReason` +7; the non-reject `AppendOutcome` variants) | queued to **dev-foundation-r2** by rulings B-R33 and B-R30 Q-B-4 *after* round 1 was committed. No shape exists at `6893442` to write a row against, and the lead's 20:15 entry says CB-1 and CB-4 are still "one decision". Ids are stable once written, so pre-allocating would mean retiring them within a day. §14 names all four; §18 Q-7 states the default |
| kernel-a's **ask 9** (`ControlTime → ClockSample { at, utc_ms, epsilon_ms, valid }`, ruling A-R26 Q-3) | same — r2, and the lead noted it is code in `rdb-sim` rather than a shape in `rdb-core` |
| any **kernel rule** | charter DO-NOT. `M7F-09` asserts the dispatcher *copies* an adopted triple and decides nothing; when a module may adopt is ADR-rdb-0007's rule and kernel-a's row |
| a **module registry** for the charter's "unregistered handler fails explicitly" | K-F-28: the dispatcher is six named fields indexed by an infallible `match`; nothing can be unregistered. Restated as `M7F-01(b)` — an event routed to an unwired module yields `Unavailable`, returns **no effect**, and never panics. A registry built to make one row literally true would be code that exists for a test |
| verification's `M7V-90` | withdrawn under F-R13; no foundation row replaces it |
| **editing the other three plans** | out of scope. Two edits are owed there and I did not make them — see "Consequences for other teams" below |

## 8. Consequences for other teams (edits someone else owes)

1. **Verification's §12 and §15 are stale — HANDLED by the lead.** A separate agent is refreshing
   them, and the lead independently confirmed both contract asks landed in foundation round 1.
   Recorded here for the record only. They still list `op_skipped` as "not in the landed
   `TraceKind` at `8a23b1d`" and `M7V-46`'s header half as `Unavailable` until `provenance` lands.
   Both **landed at `6893442`**: `TraceKind::OpSkipped { scenario_op_index: u32, reason: SkipReason }`
   and `TraceHeader.provenance: Provenance`. `M7V-22`, `M7V-46`'s header half and `M7V-88`'s
   `op_skipped` clause can stop reporting `Unavailable{Capability(C0)}`. Foundation's side is
   `M7F-11(a)` (landed) and `M7F-41` (owed).
2. **Verification's `M7V-88` stays partly vacuous** until `M7F-30`…`M7F-35` land — there is no
   validator at `6893442`. Their row already says so, so nothing is wrong there today; it is the
   dependency to watch.
3. **Kernel-b's `M7B-26` cites `M7F-07`**, which is landed and green. No action.
4. If the lead wants a compact Q allocation, the cheap fix is kernel-b renumbering **its** Q-57,
   not this plan colliding with it. That is their file.

## 9. Questions for the lead — each with the default I have already implemented

| # | Question | Default in the plan |
|---|---|---|
| **Q-1** | Q-row allocation: assignment said Q-57, kernel-b's committed plan owns Q-57, ledger B-R30 says "foundation takes Q-55+". Which is current? | **Q-58 upward.** Confirmed by the lead mid-task. Q-55/Q-56/Q-57 left stranded |
| **Q-2** | Rename the **three** non-`m7f_` landed names — `b_r30_…` (`M7F-39`) and the two `k_f_39_…` (`M7F-48`, `M7F-49`)? | **No** — adopt the landed names. Renaming also breaks the `@m` values already written into the JSONL logs, which §12's queries would then miss silently |
| **Q-3** | Renumber so one id is exactly one function, instead of sub-case letters? | **No** — the frozen ids are cited from four documents; renumbering repoints them silently |
| ~~**Q-4**~~ | ~~Is the new `harness::trace::validate` surface authorised?~~ | **CLOSED — approved by the lead, 2026-09-20.** `M7F-30`…`M7F-35` stand; ids do not retire |
| **Q-5** | Should the three `script` rows be cargo tests instead? | **No** — nested cargo against one target dir is the `AGENTS.md` hazard, and the failure looks like a link error rather than a collision |
| **Q-6** | `M7F-26` clause 3 greps `rdb-sim/src` for `SimError::unavailable(` and compares to a literal list — a test coupled to a source shape. Acceptable? | **Yes** — it is the only clause that fails when the *code* grows a seam nobody wrote a row for. Fallback: move it into `M7F-42`, which is already `script` class |
| **Q-7** | Pre-allocate ids for CB-1…CB-4 and kernel-a's ask 9? | **No** — next free ids when r2 lands. The plan's §14 forward-references name no number, so §17's "nothing beyond `M7F-47`" check stays mechanical |

## 10. Risks

- **Low.** The plan is a document; nothing executes. The 49 landed names were adopted verbatim and
  verified present by grep, so a developer following it cannot create a parallel name.
- **One real exposure:** ten rows are blocked on H1 and I1, and eight of those are the trace
  validator. If I1 slips, `M7V-88` stays partly vacuous and the charter's "byte-identical trace
  twice" line stays **not met**. §16 marks both red rather than hiding them.
- **Watch item:** the scoped architect round on `design.md` §4.1/§4.6/§4.8/§4.10 will change text
  this plan cites by section number. No row changes, but §15's four new drift rows should be
  re-read after that round lands and collapsed if the document catches up.

## 11. Recommended next role

**Foundation developer, round 2** — 23 owed test functions, of which 13 are buildable against
`ec610f4` today and blocked by nothing: `M7F-47` (scheduler total order), `M7F-43` (stale timer
version), `M7F-29` (eleven-part preimage completeness), `M7F-44`/`45`/`46` (the three charter M1
lines), `M7F-36`/`37`/`38` (A-R23 shapes), `M7F-40` (the sixteen-name reject set), `M7F-41` (no
bare seed), `M7F-42` (purity). Suggest those first, then the four `Unavailable` seam rows
(`M7F-23`, `24`, `25`, `26`), which are cheap and catch the worst failure mode in the crate. The
six validator rows need the I1 surface and should wait on Q-4.
