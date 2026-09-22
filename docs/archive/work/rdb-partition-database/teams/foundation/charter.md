# Team foundation — charter (M7)

Read `../../team-rules.md` first.

## GOAL
Give every other team a buildable, deterministic place to stand: the `rdb-core` contract seed and the `rdb-sim` environment. Spike packages **C0, H1, I1, M1**.

## DELIVERABLE
- `crates/rdb-core`: contracts (ids, envelopes, errors, seams from spike §4), known-answer vectors, module skeleton for every kernel module (authority, transaction, replication, publication, protection, recovery) as explicit `Unavailable` stubs.
- `crates/rdb-sim`: event scheduler, manual clock and timers, controlled network, fake single-record CAS control store with watch and gaps, configurable cluster, ordered in-memory storage with atomic batches, snapshots and buffered/durable crash images, effect dispatcher, canonical trace and replay.
- ADRs `docs/ADRs/rdb/0000` (series, naming, authority order), `0002` (crates and dependency direction), `0003` (simulation kernel: step(state, event) to effects, time, control, storage, transport, trace seams). Plus `docs/ADRs/rdb/README.md` index listing 0000–0003 and 0001 as Accepted, and `docs/rdb/README.md` (one screen: what each doc is, evidence packets absent).
- Workspace `Cargo.toml` members and dependencies for the two crates.

## CONTEXT
- Spike plan `docs/rdb/implementation-spikes.md` §3 (files), §4 (seams: event/effect, time, storage, control, transport, trace), §5 foundation rows, §6 (one kernel, replaceable environment; storage realism without disk; `sync_wal_through`).
- Spec `docs/rdb/design-specification.md` §5.1 (request/result fields), §5.2 (ordered path identities), §5.4 (errors), §6.1 (envelope), §7.1 (control records).
- rEtcd patterns to copy: `crates/config-core` (purity, typed errors, `#![deny(missing_docs)]`), `crates/config-testkit/src/poll.rs`, `crates/config-log-macros` (`#[retcd_test]`), ADR-0004, ADR-0007 (envelope encoding style), ADR-0013.

## SCOPE and EXCLUSIONS
In: everything above. Out: kernel logic (other teams), oracle and scenario generators (team verification), RocksDB (M8), real network, async runtime.

## OWNED ARTIFACTS (exclusive)
`Cargo.toml` (workspace, members and `[workspace.dependencies]` only), `crates/rdb-core/Cargo.toml`, `crates/rdb-core/src/lib.rs`, `crates/rdb-core/src/contracts.rs`, `crates/rdb-core/src/contracts/**`, the six kernel module **stub files** at first seed only (`src/{authority,transaction,replication,publication,protection,recovery}.rs`, handed to kernel teams after seed), `crates/rdb-core/tests/contracts.rs`, `crates/rdb-sim/Cargo.toml`, `crates/rdb-sim/src/**` except `tests/`, `crates/rdb-sim/tests/{sim,memory,harness,replay}.rs`, `crates/rdb-sim/tests/support/mod.rs` (registry only), the three ADRs, both READMEs, `docs/testing/test-plan-m7-foundation.md`, everything under `teams/foundation/`.

## ACCEPTANCE (spike §5 foundation rows) and EVIDENCE
- C0: crate compiles; identity and envelope vectors are known-answer tests; an unknown mandatory version is refused before any decode of the body. Evidence: `scripts/gate.sh test -p rdb-core` output.
- H1: same event log yields byte-identical trace twice; stale timer version ignored; watch gap forces reload. Evidence: named tests green.
- M1: every injected crash boundary yields whole batch or none; a snapshot never sees partial state; buffered and durable prefixes are distinct; `sync_wal_through` modelled with the write-order mutex. Evidence: named tests green.
- I1: unregistered handler fails explicitly; no live I/O in core tests; minimized trace replays; result manifest records resolved budgets. Evidence: named tests green.
- Row ids `M7F-NN` prefix every test name.
- `scripts/gate.sh all` green for the workspace at handoff (cold build; set `CARGO_TARGET_DIR=.rtargets/foundation`).

## DEPENDENCIES
None to start. Kernel teams and verification depend on your **seed** (contracts + stubs + sim seams). Seed early: land a compiling seed within the architect's design, before the rest of C0 is complete, and say so in the ledger handoff.

## DO-NOT
No kernel decisions in the sim. No fake success for an unwired capability. No `Instant::now()`, no `rand` outside the seeded generator, no `HashMap` iteration order in any trace path.

## BUDGET / STOP
Stop and report BLOCKED if a seam in spike §4 cannot be expressed without a kernel decision, or if a workspace dependency needs a native build.

## HANDOFF
Per team-rules.md, into `teams/foundation/<role>-handoff.md`.
