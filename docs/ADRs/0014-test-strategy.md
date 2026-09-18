# ADR-0014: Test strategy — harnesses, conformance, E2E, fault injection

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §20, §21

## Decision

Layers, all with JSONL logs per test (ADR-0013):

1. **Unit / property** (`config-core`): table-driven CAS rules, encoding round trips,
   replay determinism (`proptest` over random command sequences: two fresh state machines fed
   the same sequence must produce identical state hash and responses).
2. **In-process cluster harness** (`config-testkit::Cluster`): spins N `ConfigNode`s in one
   Tokio runtime with real tonic on `127.0.0.1:0` ports, `Ephemeral` or `Rocks` storage in
   temp dirs, `NetFault` controls (block/unblock a pair, delay), `stop(node)`,
   `restart(node)`, `crash_at(node, boundary)`.
3. **Conformance suite** (`config-testkit::conformance`): one list of scenarios executed
   against any `Arc<dyn ConfigStore>`; run for `DirectClient` and `GrpcClient` (TLS and
   insecure). Same expected results asserted → "direct and gRPC clients pass the same suite".
4. **Process-level E2E** (`tests/e2e_daemon.rs`): builds `config-server`, launches 3 real
   processes with generated manifest + certs + allowlist, forms the cluster, runs the
   conformance suite over `GrpcClient`, kills/restarts a process, verifies durability and
   log correlation across processes with DuckDB-readable JSONL.
5. **Fault injection**: storage `FaultInjector` boundaries (ADR-0008); network partitions via
   harness; lost-response simulation for unknown-outcome tests.
6. **Milestone gates**: `tests/m0_*.rs`, `tests/m1_*.rs`, `tests/m2_*.rs`, `tests/m3_*.rs`
   map 1:1 to the §21 acceptance bullets; a gate passes only if all its tests pass.

Rules: tests never sleep for fixed durations to "wait for consensus"; they poll metrics
(`wait_for_leader`, `wait_applied(index)`) with deadlines. Ports are ephemeral. Temp dirs
are cleaned. Every test starts with the test-context macro.

## Verification

- `cargo test --workspace` green; `tests/` file names match milestone bullets; a DuckDB query
  over `target/test-logs` returns rows for every test method.
