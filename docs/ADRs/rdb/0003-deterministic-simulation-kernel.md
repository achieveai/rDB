# ADR-rdb-0003: A deterministic simulation kernel, and the two vocabularies it speaks

**Status:** Proposed
**Date:** 2026-09-20

## Context

rDB's hard properties — no lost acknowledged write, no published rollback, no duplicate effect
from a retry — are properties of *histories*, not of single calls. They only show up under crashes
at exact boundaries, partitions that heal in a particular order, clocks that disagree, and replicas
that hold different prefixes.

A test suite that runs real processes and real clocks cannot produce those histories on demand, and
cannot reproduce one it stumbled on. So the spike (§4, §6) asks for a deterministic simulator: one
kernel, a replaceable environment, and a recorded event stream.

Three things had to be decided before six people wrote code against it.

**How pure is pure?** Spike §4 says "all IO completions return as events". Read literally, that
makes every condition evaluation a three-event dance: a module emits a read effect, waits, resumes
from a saved state. Six modules would each grow a resume state machine, and every test would have
to drive three events to check one condition.

**What judges the run?** If the oracle reads the kernel's own state, it re-derives the kernel's
decisions and agrees with the kernel by construction. That is the second implementation of the
protocol the spike forbids.

**What is a reproducer?** A seed reproduces a run only if the generator, the scheduler and every
library it touches are byte-identical. That is not a property a team can maintain across a
milestone.

## Decision

1. **The kernel is a synchronous fold.** A protocol module is:

   ```rust
   pub trait Module {
       fn name(&self) -> ModuleName;
       fn capability(&self) -> CapabilityState;
       fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError>;
   }
   ```

   No clock, no randomness, no I/O, no async, no shared kernel state object. `&mut self` is
   per-module. A step returns effects in the order the environment must execute them; a step that
   errors returns none. The return type is a plain `Vec<Effect>`, matching
   `KvState::apply_with_effects` in the control plane, rather than a wrapper type.

   `capability` takes `&self` and answers from a constant the module declares. Whether a module
   is built is a question, never a probe: the harness does not step a module with an event the
   protocol never sent to find out, because that would mutate the first real module's state at
   the start of every run, deterministically, and discard whatever effects it produced (K-F-10).

2. **Writes are effects; reads are pure lookups.** `StepCtx` carries one `&dyn SnapshotRead` bound
   to an already-published snapshot at a stated sequence and generation. `SnapshotRead` is total,
   side-effect-free and ordered, and it exposes both the value and the **version** of a key —
   `get` and `version` — because the spec's conditions (`VersionEquals`, `expected_version`) are
   evaluated through this one surface and nothing else (K-F-04). Every mutation still leaves as
   an effect and comes back as an event.

   *Rejected alternative:* route reads through the effect queue too. It buys a purity the read path
   already has — a lookup into an immutable snapshot is deterministic — and costs a resume state
   machine in six modules plus two extra events per condition in every test.

3. **The environment owns all nondeterminism, and every choice is a value.** `rdb-sim` holds the
   scheduler, the clock, the network, the fake control store, the storage engine and the topology.
   Delivery order, fault injection and clock skew are enumerable operations — `NetworkOp`,
   `StorageOp`, `ControlOp` — not method calls, so the coverage matrix can count them and the
   shrinker can drop them one at a time.

4. **One total order.** The scheduler assigns `EventId`, strictly increasing, and orders events by
   `(tick, event_id)`. No module invents an ordering. The oracle is a left-to-right fold that never
   sorts and never searches.

5. **Two vocabularies, deliberately.** `contracts::event::Event` is what the kernel is *fed*.
   `contracts::trace::TraceEvent` is what the system *declares it did*. The oracle reads the trace
   and nothing else — never kernel state, never simulator state.

   The trace carries no key or value bytes: a key is a `KeyId`, a value is a version plus a
   `Digest`. Every outcome, reason, mode and state in it is a closed Rust enum. rEtcd's M6 rows
   M6-118 and M6-122 exist because open reason strings make assertions impossible.

   Certain events are **environment-owned** and a kernel module may not emit them — `TopologyChange`
   above all. If the kernel declared the topology, the oracle would resolve replica roles from the
   belief under test, and a shadow counted as a regular copy would be self-consistent.
   `ControlInteraction` is the second: the H1 control provider records what the store actually
   answered — every CAS, read, watch tick and watch termination with its kind — so the kernel
   cannot misreport it. `FamilyReload` is kernel-emitted, because the *decision* to reload is under
   test, and it carries an explicit back-reference to the termination that justified it. That
   pair is what makes ADR-rdb-0008 §7 item 4 as amended by A-R15 ("no reload unless a termination
   was delivered first") an assertion over two trace events instead of a claim with no evidence
   (K-F-06).

   The trace carries no whole-state digest. Verification withdrew `state_digest_after` and
   `published_state_digest` in writing (lead ruling F-R9): the only way to read one is to compute
   your own and compare, which is the second implementation this decision forbids, and they were
   the only O(state)-per-event cost on the trace path (K-F-24).

6. **The reproducer is the recorded event stream, not the seed.** A trace replays when its
   `schema_version` and `generator_version` match. The header therefore carries a
   `Provenance` — `Generated { seed }`, `Reduced { parent }` or `Authored { case }` — and never a
   bare `seed` field: a reduced or authored scenario is not in the generator's image, so its seed
   reproduces nothing and the checked-in event stream is the reproducer (K-F-09). The header also
   carries the run manifest — the budgets the run resolved to and **which of them were
   overridden** from the spec defaults — so a row that fails under an override is never mistaken
   for one that fails under defaults (K-F-27). There is no second shrinker: no proptest, no
   quickcheck. Shrinking reduces the recorded stream, and a reducer skip is its own trace kind
   (`OpSkipped`), never a fault boundary.

7. **An unwired capability is an explicit `Unavailable` result.** Never `todo!()`, which panics and
   would abort a campaign runner mid-run, turning "not built yet" into "the run crashed". Never a
   fake success, which is indistinguishable from a passing implementation exactly when it matters.
   The harness emits a `Capability { package, state }` trace event per package at trace start,
   from `Module::capability(&self)`, so a green campaign over six unimplemented packages cannot
   look like a passing one. `Wired` is declared positively by the module; it is never inferred
   from "did not return `Unavailable`". An `Unavailable` error proves nothing about mutation
   (`proves_no_mutation()` is `false` for it): "not built" is not a pre-admission rejection, and
   the row that checks a stub asserts the narrower fact — no effect was returned (K-F-26).

8. **Authority is adopted by effect; the environment decides nothing about it.** The generation,
   owner epoch and configuration version a module sees in `StepCtx` are copied by the dispatcher
   from the last `Effect::AdoptAuthority { generation, owner_epoch, config_version }` that
   partition's modules emitted, and are the zero triple before any. When to emit it is
   ADR-rdb-0007's rule and lives in kernel-a's authority module. The alternative — the simulator
   filling those fields from its own knowledge of the control store — would put an authority
   rule in `rdb-sim`, which the foundation charter forbids, and would make the oracle agree with
   the simulator by construction (K-F-05, lead ruling F-R10).

9. **The effect-to-event hop is bounded, and the bound is zero ticks.** The dispatcher drains
   every effect a step returned in the same tick, in vector order, before the scheduler
   advances; a completion is scheduled at `now` plus whatever the scenario's fault plan adds and
   never later by the harness's own doing. Kernel-b's 2,100 ms pause budget is
   `pause_ms (2,000) + eval_cadence (≤ 50 ms) + admission_propagation (≤ 50 ms)`, and
   `admission_propagation` — from L1 emitting an admission effect to T1 refusing the next
   transaction — is this hop (lead ruling B-R23). Zero ticks satisfies it with the whole 50 ms to
   spare. The dispatcher never drops an effect; the only way a completion fails to arrive is
   `ControlOp::DropCompletion`, which is a recorded fault.

## Consequences

- A kernel unit test is three lines and needs no harness.
- Adding a fault means adding an enum variant, which the coverage matrix counts and the shrinker
  handles for free.
- The trace is a second schema that must be kept true as modules land. `TRACE_SCHEMA_VERSION`
  exists for that, and a bump deliberately invalidates checked-in fixtures.
- Traces are larger than a seed. JSONL on disk, one event per line, readable directly by DuckDB —
  a failing campaign is queried, not grepped.
- Purity is a convention inside `rdb-core`, not a compiler-enforced property. Nothing stops a
  module from calling `SystemTime::now()`. It is checked by review, by the crate's dependency set
  (ADR-rdb-0002 decision 5), and by replay equality, which a wall clock breaks immediately.
- `SnapshotRead` being a trait object means a module cannot be generic over storage. That is the
  intent: one read surface, one set of assertions.
- The simulator's providers hold state and are neither `Copy` nor `const`. A scheduler that could
  be silently copied would duplicate its queue rather than alias it, invisibly at the call site
  (K-F-29).
- A crash image carries both watermarks per partition, `durable` and `applied`. A process crash
  keeps the buffered tail; a host crash truncates it to `durable`; reopen restores each to its own
  value. With one watermark the simulator promoted buffered to durable on every process crash and
  "no lost acknowledged write" could not fail (K-F-03).

## Verification

- Row **M7F-01** (`crates/rdb-sim/tests/harness.rs`, `#[retcd_test]`): all six kernel packages
  report `CapabilityState::Unavailable` from `Module::capability(&self)` without being stepped;
  stepping one through the dispatcher returns `RdbError::Unavailable` naming its `Capability`,
  with `RetryRule::NotWired` and **no effect**, and never panics. 3 assertions passing 2026-09-20
  against the seed; restated in correction round 1 against `capability(&self)`.
- Row M7F-05 (owed by package H1): the scheduler's `(tick, event_id)` order is total, and two runs
  of one recorded stream produce byte-identical traces.
- Row M7F-09 (owed by package I1): before any `AdoptAuthority` the dispatcher fills `StepCtx` with
  the zero triple; after a module emits one, the next `StepCtx` for that partition carries exactly
  it and another partition's is unchanged (decision 8).
- Row M7F-11 (owed by package I1): a header with each `Provenance` variant and a run manifest
  round-trips through JSONL byte-identically, and an unknown header field is refused (decision 6).
- Row M7F-21 (owed by package I1): an effect emitted at tick *t* is delivered to the next step at
  tick *t*; with `ControlOp::DelayCompletion { by_millis: 50 }` its completion lands at exactly
  *t + 50* (decision 9).
- `harness::replay::replay` returns `ReplayOutcome::Identical` for a recorded trace, and
  `Unreplayable` for one whose schema or generator version differs.
- No panicking stub and no clock in the kernel, with doc-comment hits excluded. Both commands
  print nothing and exit 1 (observed 2026-09-20); without the second filter each finds only
  comments that explain why the thing is absent, which is what an earlier revision of these
  bullets failed to say (K-F-31):
  - `grep -rn "todo!" crates/rdb-core/src crates/rdb-sim/src | grep -v -E ':[[:space:]]*//'`
  - `grep -rn -E "SystemTime|Instant::now|rand::" crates/rdb-core/src | grep -v -E ':[[:space:]]*//'`

## References

- `docs/rdb/implementation-spikes.md` §4 (the six seams), §6 (required boundary cases and the
  no-second-implementation rule), §8 (explicit unavailable is permitted; fake success is not).
- `docs/rdb/design-specification.md` §5.2, §5.3, §6.1 — the ordered write path, the publication
  barrier, and the buffered/durable split the trace has to make observable.
- `docs/ADRs/0014-test-strategy.md` — one test file per milestone acceptance bullet.
- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/verification/trace-requirements.md`
  — the trace vocabulary this ADR freezes.
- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/foundation/design.md`
  §4 — the committed signatures.
