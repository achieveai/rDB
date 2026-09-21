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
       fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError>;
   }
   ```

   No clock, no randomness, no I/O, no async, no shared kernel state object. `&mut self` is
   per-module. A step returns effects in the order the environment must execute them; a step that
   errors returns none. The return type is a plain `Vec<Effect>`, matching
   `KvState::apply_with_effects` in the control plane, rather than a wrapper type.

2. **Writes are effects; reads are pure lookups.** `StepCtx` carries one `&dyn SnapshotRead` bound
   to an already-published snapshot at a stated sequence and generation. `SnapshotRead` is total,
   side-effect-free and ordered. Every mutation still leaves as an effect and comes back as an
   event.

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

6. **The reproducer is the recorded event stream, not the seed.** A trace replays when its
   `schema_version` and `generator_version` match; a seed appears in the report and in the
   reproducer command, and is never sufficient on its own. There is no second shrinker: no
   proptest, no quickcheck. Shrinking reduces the recorded stream.

7. **An unwired capability is an explicit `Unavailable` result.** Never `todo!()`, which panics and
   would abort a campaign runner mid-run, turning "not built yet" into "the run crashed". Never a
   fake success, which is indistinguishable from a passing implementation exactly when it matters.
   The harness emits a `Capability { package, state }` trace event per package at trace start, so a
   green campaign over six unimplemented packages cannot look like a passing one.

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

## Verification

- Row **M7F-01** (`crates/rdb-sim/tests/harness.rs`): all six kernel packages report
  `CapabilityState::Unavailable`; each names its own `Capability`; the error is `RetryRule::NotWired`
  and proves no mutation. 3 assertions, passing 2026-09-20.
- Row M7F-05 (owed by package H1): the scheduler's `(tick, event_id)` order is total, and two runs
  of one recorded stream produce byte-identical traces.
- `harness::replay::replay` returns `ReplayOutcome::Identical` for a recorded trace, and
  `Unreplayable` for one whose schema or generator version differs.
- `grep -rn "todo!" crates/rdb-core/src crates/rdb-sim/src` finds nothing.
- `grep -rn "SystemTime\|Instant::now\|rand::" crates/rdb-core/src` finds nothing.

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
