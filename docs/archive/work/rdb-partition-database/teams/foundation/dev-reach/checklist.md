# Dev "reach" checklist — M7 foundation

> REMINDER: tick each item as it completes. `[x]` done, `[-]` in progress, `[ ]` not started.

## Gate rules I am under
- [x] NO test rows (M7F-*, M7A-*, M7B-*). Scaffolding unit tests only, labelled as such.
- [x] Frozen contract names: 43. Do not change. Escalate instead.
- [x] Do not move any `<!-- drift-basis: -->` marker.
- [x] Do not touch `crates/config-*`.

## Scope 1 — land the frozen contract types (§1.5/§1.7)
- [x] New file `crates/rdb-core/src/contracts/ignore.rs`: `KernelIgnoredReason` (5 arms) + `ReplicaIgnoreReason` (12)
- [x] `AuthorityIgnoreReason` (27) into `crates/rdb-core/src/contracts/authority.rs`
- [x] Register `pub mod ignore;` in `contracts.rs` + doc table row
- [x] `KernelEffect::Ignored.reason` retype `ErrorKind` -> `KernelIgnoredReason`
- [x] Drop `Copy` from `KernelEvent`, `KernelEffect` (§1.7)
- [x] `lib.rs` re-exports
- [x] Fix the 6 edit sites: seams.rs:20/267/297/299, dispatch.rs:20-22/483

## Scope 2 — CB-9b: wire the sim Clock into the harness
- [x] `Dispatcher` owns a `Clock`; `clock()` / `clock_mut()`
- [x] `ctx_for` fills `control_time` from `self.clock.control_time(base.node)`
- [x] Correct `ctx_for`'s doc: `base.control_time` is ignored
- [x] `support::ctx()` doc points at `clock_mut` (§4.5 R2-12 cheap fix)

## Scope 3 — WITHDRAWN by the lead, replaced by (a)+(c)
Lead ruling: brief was wrong; H1 and I1 are packages, not reach fixes. Do NOT build H1.
- [x] ESCALATED and answered
- [x] Build the runner-free two-trace `ReplayOutcome` comparison, its OWN name (not a replay variant)
- [x] `replay(&Trace)` keeps refusing; M7F-26 stays green and UNTOUCHED
- [x] Do not build `Network::send` / `Cluster::suspend` / `environment_capabilities`

## Post
- [x] scripts/gate.sh lint clean (exit 0, workspace, -D warnings)
- [x] scripts/gate.sh test, exit code READ FROM FILE -> CARGO_EXIT=101
  - [x] scoped rdb-core+rdb-sim: CARGO_EXIT=0, 14 binaries ok
  - [x] workspace: 135 binaries ok, 2 red, BOTH config-* (not my scope)
  - [x] root cause: Windows os error 10013, ports 54942/54964/54984 inside
        the administered UDP exclusion range 54934-55033
  - [x] proved not mine: no config-* crate depends on rdb-*; I touched no config-* file
  - [x] NOT fixed, NOT investigated further: config-* is excluded by the brief
- [x] handoff written

## Extra checks done
- [x] fmt/deps/drift all exit 0; no drift marker moved
- [x] 27+12 names diffed mechanically against design §1.5 -> identical
- [x] 4 remaining codes map to landed leaves, all verified present
- [x] formatted ONLY my 10 files (not `cargo fmt --all`) — shared tree
- [x] private CARGO_TARGET_DIR, never `.rtargets/gate`
- [x] scaffolding tests in `src/#[cfg(test)]`, plain `#[test]`, labelled
- [x] M7V-82 failure found and fixed by changing MY data, not the row
