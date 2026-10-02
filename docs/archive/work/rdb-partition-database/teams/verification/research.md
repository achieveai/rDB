# Research — team verification (architect, 2026-09-20)

Sources actually read, and what each changed in `design.md`. Nothing here is cited from the
non-existent `evidence/*.md` packets (team-rules §authority).

## 1. proptest: does its shrinking fit a causal event trace?

**Read:**
- proptest repository and `Strategy` docs — <https://github.com/proptest-rs/proptest>,
  <https://altsysrq.github.io/rustdoc/proptest/latest/proptest/strategy/trait.Strategy.html>
- `proptest-stateful` docs — <https://docs.rs/proptest-stateful/>,
  <https://crates.io/crates/proptest-stateful/0.1.1>

**What they say.** proptest's `Strategy` is a pair: generate a value, and produce a "simpler"
version of it. Shrinking is therefore coupled to *how the value was generated*. The docs also warn
that filtered strategies shrink badly — "complex filters may largely or entirely prevent shrinking
from substantially altering the original value." A causal trace is exactly a heavily filtered value
space: most randomly-simplified traces are traces the system could never produce.

`proptest-stateful` exists precisely because of that. It generates a **sequence of operations** and
shrinks by **removing whole operations from the sequence**. It explicitly does *not* shrink
individual operations, and says why: doing so "tends to break preconditions in a way that is
difficult to compensate for." It also reports that removal-only shrinking has been sufficient in
practice.

**What this changed.** Two things.

1. It confirmed design decision **D3** independently: op-removal over a re-executed sequence is the
   shape that works, and per-op mutation is the part that breaks.

   **Correction round 1 (F12):** the first draft quoted this source and then added a per-op
   simplification pass anyway, restricted to "monotone" fields. That was inconsistent with the
   citation, and the critic was right. `Advance{ticks}` is not monotone with respect to the failure
   class — ticks drive the 1 s/2 s protection thresholds, grant `expiry_tick`, the ±100 ms skew
   boundary, the 2 s discovery window and the 24 h dedup jump, so shrinking them moves the run
   across guards. The pass is deleted; `design.md` §4.2 is now deletion-only ddmin, which is what
   this source actually recommends.
2. It argued **against** taking the dependency. `proptest-stateful` would give us the loop we are
   already writing, but wrapped in `proptest!`'s own case-count and shrink budgeting. Our budgets
   are `SPIKE_SEEDS` / `SPIKE_MAX_EVENTS` / `SPIKE_SHRINK_STEPS`, set by spike §7 and by the
   charter, and the campaign needs to thread seeds and report per-invariant status. Bending that
   into proptest's runner is more code than a ~150-line `ddmin`, not less. Recorded as Q-1 with the
   default "no proptest in the campaign".

## 2. Deterministic simulation testing: what the field does

**Read:**
- madsim — <https://github.com/madsim-rs/madsim>
- S2, "Deterministic simulation testing for async Rust" — <https://s2.dev/blog/dst>
- RisingWave, "Deterministic Simulation: A New Era of Distributed System Testing" —
  <https://risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/>
- awesome-DST index — <https://github.com/ivanyu/awesome-deterministic-simulation-testing>

**What they say.**
- The FoundationDB lineage: single-threaded execution, all IO mocked, all uncertainty eliminated,
  randomness amplified to create chaos, failures reproduced from a seed.
- madsim reimplements this for Rust as a tokio-shaped runtime; RisingWave notes FoundationDB's own
  framework is unreusable because it is fused to Flow, and that turmoil's time/randomness handling
  "was not comprehensive enough to address what arbitrary dependencies may be doing" — hence
  mad-turmoil.
- S2 validates determinism with a **meta test**: rerun the same seed and compare TRACE-level logs
  byte-for-byte. They also run simulations on every PR and in thousands of nightly trials.

**What this changed.** Three things.

1. **We do not need madsim or turmoil.** Both exist to make *async, IO-performing* code
   deterministic — they intercept the runtime, and in mad-turmoil's case libc symbols. Our kernel is
   already `step(state, event) -> effects` with no async, no clock and no IO (team-rules
   §determinism, spike §4). The hard problem those crates solve is one the architecture removed. A
   dependency that neutralizes a property we already have is pure cost. Recorded in `design.md` §8.
2. **The meta test is worth copying.** S2's byte-for-byte seed-replay comparison is what we should
   ask I1 for: the `oracle_checkpoint_digest` in spike §4's trace seam is the same idea, and the
   replay-equality row belongs to foundation, not to us. Noted as a dependency, not claimed by us.
3. **"Thousands of nightly trials" matches spike §7's budget**, and the S2 framing supports keeping
   a small corpus in every PR run rather than an `#[ignore]`d suite. That reinforces the
   default-small `SPIKE_SEEDS=64` decision (`design.md` §5.1) and lines up with rEtcd ADR-0031's
   reduced-scale-by-default rule.

## 3. Minimizing distributed-system failures

**Read:**
- Scott et al., "Minimizing Faulty Executions of Distributed Systems", NSDI 2016 (DEMi) —
  <https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-scott.pdf>
- "Validity-Preserving Delta Debugging via Generator Trace Reduction" (GReduce), TOSEM 2024 —
  <https://arxiv.org/abs/2402.04623>

**What they say.**
- DEMi minimizes *faulty execution traces* of actor systems (akka-raft, Spark), cutting event counts
  by up to 97% and beating blackbox minimization by up to 16×. It is ~14,000 lines of Scala. The
  size is the point: minimizing a trace directly requires modelling which events caused which, and
  replaying a trace whose causes were deleted requires repair.
- GReduce inverts it: instead of reducing the *output*, reduce the *generator execution* that
  produced it, and re-generate. The result is validity-preserving by construction — every candidate
  is something the generator can actually produce.

**What this changed.** This is the source of decision **D3** and it is the single largest
code-removal in the design. We reduce the `Scenario` (the generator's explicit op list), re-run the
real kernel in the real deterministic environment, and accept whatever trace comes back. Causality
is preserved because the kernel regenerates it. No happens-before graph, no causal repair, no
trace-validity checker — the three things that make DEMi 14,000 lines.

The trade is one full re-run per shrink step. Bounded at ≤2,000 events with no IO, that is cheap;
DEMi's cost model assumed a system it could not re-run cheaply, which is not our situation.

## 4. rEtcd precedent read in-repo (not web)

- `docs/ADRs/0031-evidence-and-known-gaps.md` — the evidence schema, `RETCD_EVIDENCE=1`,
  reduced-scale-by-default, `scale_factor` honesty, the `full_scale: false` gate script, and the
  three unowned fault classes. Reused verbatim in ADR-rdb-0019 §2 and §4. Its closing note ("a
  documented behaviour with nothing behind it", four instances on one branch) is why oracle
  independence is a **test row** (M7V-01) and not a review promise.
- `docs/testing/test-plan-m6.md` §7 — the evidence-row pattern: assert invariants, *record* numbers,
  never a threshold. §11 — the DuckDB Q-row pattern, which the test plan will reuse for log-based
  assertions over the campaign's JSONL.
- M6-107's enumerator rule ("the count is asserted against the enumerator, so a missing arrangement
  fails rather than passing quietly") is exactly the coverage-matrix rule in `design.md` §6.

## 5. Open threads

- No source was found for "what a partition-database oracle should *not* check". The
  no-linearizability-checker argument in `design.md` §8 is derived from spec §5.2 (one admitted
  transaction in flight per partition; the trace declares the publication order), not from a
  citation. Flagged so the critic would attack the derivation rather than a borrowed claim.
  **Round 1: attacked and withdrawn — it holds.**
- `write_evidence()` lives in rEtcd test code. **Settled by V-R5:** `rdb-sim` dev-depends on
  `config-testkit` and reuses it as is; no `config-*` change.

## 6. Added in correction round 1

One source's absence is worth recording. The critic's **signature slippage** finding (F4) — ddmin
accepting a candidate that fails for a *different* root cause with the same signature — is Zeller's
own documented failure mode for delta debugging, and neither the proptest sources nor the DST
sources address it, because neither minimizes against a multi-cause oracle. The fix in
`design.md` §4.4 (adding the active `BoundaryId` set to the signature, and committing the original
scenario next to the minimized one) is derived from the failure mode, not borrowed from a tool.
Stated here so the next critic attacks the derivation.
