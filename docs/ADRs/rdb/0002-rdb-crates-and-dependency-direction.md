# ADR-rdb-0002: rDB crates and the direction dependencies may point

**Status:** Proposed
**Date:** 2026-09-20

## Context

rEtcd's ADR-0004 fixed the control plane's workspace boundaries: a pure core with no I/O, adapters
around it, and a dependency arrow that never reverses. rDB now adds crates to the same Cargo
workspace and has to say where its code lives and what it may depend on — before four teams start
writing in parallel, not after.

Three constraints shape the answer.

**The kernel must be testable without a harness.** Spike §4 asks for a deterministic kernel:
`step(state, event) -> effects`, no clock, no randomness, no I/O. If the kernel holds provider
traits for a clock or a network, every kernel unit test needs fakes for them, and the design that
was supposed to make tests trivial instead makes them ceremonial.

**The control plane must not learn about the data plane.** `config-*` crates are shipped and
gated. If a `config-*` crate ever took an `rdb-*` dependency, the data plane's compile time,
dependency set and failure modes would become the control plane's.

**Native builds are a real cost here.** ADR-0017 already records the `LIBCLANG_PATH` failure mode
for RocksDB builds in this workspace. A new dependency with a build script is not a neutral
choice for a spike whose whole point is fast iteration.

The digest algorithm was the specific decision this forced. BLAKE3 is the obvious modern choice
and is roughly an order of magnitude faster per byte than SHA-256. Its published crate (1.8.7,
2026-08-20) compiles assembly through a build script on the default path, and exposes a `pure`
feature precisely because that is true.

## Decision

1. **Two crates in this milestone**, both new members of the `retcd` workspace:

   | Crate | Package | What it is |
   |---|---|---|
   | `crates/rdb-core` | `rdb-core` | the pure kernel: seam contracts and six protocol modules |
   | `crates/rdb-sim` | `rdb-sim` | the replaceable environment: scheduler, clock, network, control store, storage, harness |

   Later milestones add `rdb-storage` (M8, RocksDB behind `SnapshotRead`) and `rdb-api`. Both names
   are reserved here so nobody invents a different scheme.

2. **The arrow.** `rdb-sim -> rdb-core`. `rdb-* -> config-*` is permitted. **`config-* -> rdb-*` is
   forbidden, always.** `rdb-core`'s `[dependencies]` name no `config-*` crate; both `rdb-core`
   and `rdb-sim` take `config-log` and `config-log-macros` as **dev-dependencies only**, for JSONL
   test logging through `#[retcd_test]`. `rdb-core` also dev-depends on `hex`, for the golden
   digest vectors in its contract tests. Dev-dependencies do not reach a consumer of the crate,
   so the purity rule in decision 3 is about `[dependencies]` alone.

   The forbidden direction is checked mechanically, not by review (correction round 1, finding
   K-F-32, lead ruling F-R11): `scripts/gate.sh deps` and `scripts/gate.ps1 deps` read
   `cargo metadata --format-version 1` and fail when any package whose name starts with
   `config-` lists a dependency whose name starts with `rdb-`, in any dependency kind. `all`
   runs it. Cargo itself accepts a reversed edge; the gate does not.

3. **`rdb-core` is pure**, in ADR-0004's exact sense: no clock, no I/O, no randomness, no async, no
   thread spawning, no global state. `#![deny(missing_docs)]` and `#![forbid(unsafe_code)]`. Its
   public API exposes no vendor type except `bytes::Bytes`, which is the same exception the control
   plane already makes.

4. **`rdb-core` has no provider traits.** The kernel never calls a clock, a network or a store. It
   receives events and returns effects. Its only traits are `Module` (what a protocol module is)
   and `SnapshotRead` (a pure, total, ordered read view). Every provider lives in `rdb-sim`.

5. **`rdb-core`'s dependency set is fixed at:** `bytes`, `serde`, `thiserror`, `tracing`, `sha2`.
   All five are already workspace-pinned and in `Cargo.lock`; none has a build script. Widening
   this set needs an ADR, following ADR-0004's clarification pattern.

6. **The digest is SHA-256, not BLAKE3.** `sha2` is pure Rust, already pinned, already in the lock,
   and already blessed for a pure crate by ADR-0004's 2026-09-18 clarification. BLAKE3's default
   build path needs a C/assembly toolchain, which the spike's budget rules out. The digest is a
   contract value carried in the replication envelope, so changing it later is a new ADR with a
   number, never a silent swap.

7. **No `HashMap` on a trace path.** Ordered collections only — `BTreeMap`, or a `Vec` in a stated
   order. Iteration order is part of the trace, and a trace that differs between runs is not a
   trace.

## Consequences

- A kernel unit test is a plain `#[test]`: build a `StepCtx`, call `step`, inspect the returned
  `Vec<Effect>`. No Tokio, no harness, no fakes.
- Replacing the simulated environment with a real one (M8) touches `rdb-sim` and a new
  `rdb-storage`, and not one line of `rdb-core`.
- SHA-256 costs roughly an order of magnitude per byte against BLAKE3. It does not bind: digests
  here cover envelopes of a few hundred bytes, and spike §7's budget (10,000 histories in under
  ten minutes) is dominated by event scheduling. If a profile later says otherwise, that is a new
  ADR, and the envelope version exists to carry the change.
- `rdb-core` cannot log through `config-log` in its non-test code. It emits `tracing` events and
  the binary decides the subscriber, which is what a pure crate should do anyway.
- The forbidden direction is not a compiler error — Cargo would accept a reversed edge — but it
  is a gate error. The `deps` stage in decision 2 fails the workspace gate on any
  `config-* -> rdb-*` edge, so the invariant that is cheap to check and expensive to undo is
  checked on every run rather than by review. An earlier revision of this ADR called it "a
  convention, checked by review"; that was corrected before four teams started writing in
  parallel (K-F-32).

## Verification

- `CARGO_TARGET_DIR=.rtargets/foundation CARGO_INCREMENTAL=0 cargo build -p rdb-core -p rdb-sim`
  succeeds. Observed 2026-09-20.
- `cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` is clean. Observed 2026-09-20.
- `cargo fmt --all --check` prints no diff. Observed 2026-09-20.
- `crates/rdb-core/Cargo.toml` `[dependencies]` contains exactly the five crates listed in decision
  5, and no `config-*` entry.
- `scripts/gate.sh deps` exits 0: no `config-*` package depends on an `rdb-*` package. The same
  check by hand: `cargo metadata --format-version 1 --no-deps` lists no package named `config-*`
  whose `dependencies[].name` starts with `rdb-`.
- No `HashMap` in code. The command that reproduces, with the doc-comment hits excluded:
  `grep -rn "HashMap" crates/rdb-core/src crates/rdb-sim/src | grep -v -E ':[[:space:]]*//'`
  prints nothing and exits 1. Without the second filter it finds three hits, all in `//!` or `///`
  comments that forbid the type (observed 2026-09-20; an earlier revision of this bullet claimed
  the unfiltered command found nothing, which it did not — K-F-31).

## References

- `docs/ADRs/0004-workspace-boundaries.md` — the purity rule and the arrow this ADR extends.
- `docs/ADRs/0017` — the RocksDB / `LIBCLANG_PATH` build failure that makes native dependencies a
  budgeted cost here.
- `docs/rdb/implementation-spikes.md` §3, §4 — file ownership and the six seams.
- `docs/rdb/design-specification.md` §4.1 — one engine per core set.
- crates.io and docs.rs for `blake3` 1.8.7, fetched 2026-09-20: latest version, feature list,
  and the `pure` feature.
- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/foundation/research.md` §2
  — the full BLAKE3 evaluation.
