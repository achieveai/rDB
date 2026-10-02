# Plan: M7 complete, gated, and only then a PR

**Done when:** all four M7 acceptance criteria read `proven` with a named row id behind each,
every scope has a Manual Tester THUMBS UP, and `scripts/gate.sh all` is EXIT 0.

## Revision 6 — the seam freeze landed, and there is no cross-team deadlock

Counted by `scripts/m7-census.sh` at `395d535`, not quoted from anywhere:

| Scope | Declared | Landed | Owed | Kernel under test |
|---|---:|---:|---:|---|
| foundation | 56 | 43 | 7 + 6 exempt | — (7 **unblocked** by I1 today) |
| kernel-a | 174 | 2 | 172 | A1 **partial** — control seam only; P1, T1 **41-line stubs** |
| kernel-b | 148 | 0 | 148 | R1 parked, L1, F1 **41-line stubs** |
| verification | 91 | 65 | 26 | — |
| **Total** | **469** | **110** | **353** | |

**Five of six kernels return `Unavailable` for every event.** `crates/rdb-core/src/protection.rs:34`
is the shape. Not new scope: M7's own criterion reads "Kernel-B state recovery and durability *pass
test suite*", and a stub cannot pass one. The row count hid it.

**Revision 7 correction — the sixth kernel is partial too, and I had it as real.** `authority.rs:331`
accepts `EventKind::Control` and returns `Unavailable` for everything else, with the message
*"package A1 answers only the control seam in this build"*. Three fields of state. So my "73 kernel-a
rows writable today" was wrong: the tester drove it by hand and the number is **5** (M7A-05, 119,
121, 122, 129). I classified A1 by line count — stub vs not-stub — which answers *is there code here*
and not *does this kernel accept the events its rows send*. **M7 owes 5 kernels, five-sixths of a
sixth, and 353 rows.**

## Key dots

1. **I1 run loop — THUMBS UP, released.** Two gate rounds, the tester right both times. Closed with
   two mutation-proved guards. My gate: 16 binaries, 205 passed, 0 failed. Unblocked foundation's 7.
2. **Seam freeze done and ruled** → `seam-freeze.md`. Five types frozen, six rulings (R-S1..R-S6).
   I verified five load-bearing claims myself before ruling; all five held.
3. **Three of the four "deadlocks" never existed.** `AdmissionState` and `RecoveryResult` are
   **kernel-b's** shapes, accepted verbatim by kernel-a; `FencingProof` is kernel-a's, in full Rust.
   What blocked 26 rows was **foundation's ask numbering** — two kernel-b shapes labelled `KA-3`/
   `KA-4` — and a §13 cell that routed on the prefix. Second instance this milestone of *a team
   waiting on the wrong owner waits forever, and politely*.
4. **I withdrew half of my own fence ruling.** `FencingProof` and `FenceCredential` are two types,
   not one renamed; the credential is deliberately smaller so a receiver cannot re-derive authority
   from it. Three names, **three** things.
5. **CB-5 is five variants, not three**, and the CAS gate box is **1 of 5, not 3 of 5**. The plan
   carried three different sizes for it. Corrected at every site, not just the one I found first.
6. **Both testers came back NOT YET, and both were right.** Foundation: 1 of 7 reachable, held on
   two rulings that were sentences in the plan — I ruled both (F-1, F-2). Kernel-a: 5 of 73, held
   on an event vocabulary that does not exist. Between them they also found **five vacuous rows**
   that would have passed, and proved a sixth (M7A-33) is repairable rather than dead.
7. **Ruled: a row whose subject does not exist is not written at all.** Kernel-a's §11 offered
   blocked rows a fallback — assert the package reports `Unavailable`. At 54 blocked rows that is
   one constant asserted 54 times: green census, green gate map, nothing tested. An unwritten row
   reads as owed work; a vacuous one reads as finished.
8. **verification and the code review close last**, after the kernels, because the oracle observes
   them.

## Proof

- Per scope: Manual Tester THUMBS UP with hand-driven evidence, before any row is written.
- Per kernel: mutation-proved guards both ways — mutant EXIT 101 from the guard's own assertion,
  clean EXIT 0. A mutation that breaks the build proves nothing.
- Counts: `scripts/m7-census.sh`, never a plan's prose.
- Contracts: `gate.sh drift` EXIT 0 with markers moved after a real §15 re-read.
- Whole: `gate.sh all` EXIT 0, exit code read from a file as the statement after cargo.

## Material risks

- **Five kernels is the milestone now.** Rows are the cheap half. If a kernel sizes larger than its
  test plan assumes, I stop and bring you a number rather than thin the rows.
- **Contracts are one writer and sequential.** That is the real critical path, not parallelism.
- **Drift fires for all four plans** the moment `contracts/` is committed. Markers move only after
  a real §15 re-read — and not by a constant line offset, which is how the last one rotted.
- **Disk at 96%, 98 GB free.** I cap each target dir and report before it bites.
- **Shared checkout, many agents.** No stash/reset/clean by anyone. Exports for clean builds.
- **G-13 can brick restore** — settled as Shape E: evidence, not a gate.

9. **A1's vocabulary is ruled — A-R25..A-R32, and foundation owes almost nothing.** One carrier
   arm (`KernelEvent::Authority` / `KernelEffect::Authority`, leaves owned by kernel-a) plus one
   `StoreEffect` variant. That is foundation's whole bill; four of the six design event names are
   already landed types. **I reversed my own Q-12** — I had ruled that I1 converts a clock sample
   and delivers it as an event, but `Module::step` already takes `StepCtx`, which carries
   `control_time: ControlTime`, field for field the design's `ClockSample`. The ruling would have
   built a second copy of a struct already in the parameter list.
10. **The C0 fence mapping is withdrawn.** It said a fence is a `ControlEffect::Cas`, and eleven
    rows were parked on "rewrite onto it". Verified in §2.4: A1 emits a CAS on exactly two
    transitions, acquire-due and renew-due. **Neither is a fence.** The mapping is right for the
    planner fencing another node; every M7A row is A1 fencing itself, where there is no CAS.
11. **Three rows were blocked on an answer already on the page.** §13's heading says "the
    recommendation is the default"; M7A-47..49 cited Q-5 as a blocker and Q-5's answer was two
    lines below. Released. Separately, Q-6 had already ruled "emit a `Fact`" — and no carrier for
    it was ever asked of foundation, which is what left 54 rows waiting on a decision already
    taken.

**Now / next:** A1's vocabulary ruled (A-R25..A-R32); contracts agent landing the arm now, then
the A1 state build — nine new state items, not an accessor pass. Foundation closed 7 of 7 and its
last three rows are with the developer. Kernel-a is the critical path and it is a build, not
fixtures. Nothing committed; no PR until M7 is complete.
