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

## Clarifications (2026-09-18, Architect, from test-plan OQ-8, OQ-9)

- Log assertions in tests use `duckdb` (dev-dependency of `config-testkit`, bundled feature).
  A missing/unqueryable log file fails the test loudly; it is never skipped.
- Harness naming: `Cluster::start(n, StorageKind)` / `Cluster::start_with(ClusterConfig)`;
  restart a stopped node with `cluster.start_node(id)`; `cluster.stop(id)` stops it.
- `FaultInjector` is consulted on every crossing and may fire on the *n*-th crossing of a
  boundary (`CrashAt { boundary, nth }`); the earlier `crash_at(node, boundary)` wording means
  this. Restart of a crashed node = `Cluster::reopen_store(id)` + `start_node(id)`.
- Process-level E2E lives in `crates/config-server/tests/e2e_daemon.rs` so
  `CARGO_BIN_EXE_config-server` resolves. Daemon shutdown for tests: `ctrl_c` and
  `--shutdown-file <path>` (Windows has no SIGTERM).
- Daemon health: loopback-only plaintext HTTP `--health-listen 127.0.0.1:0` returning JSON
  `{ role, leader, term, last_applied, cluster_revision, state_hash, capabilities, membership }`
  (digests and counts only, no keys/values). Used by the E2E suite as its cross-process oracle.
- "Authenticated leader hint" = the hint arrives over the mTLS session and the client verifies
  the hinted endpoint's certificate SAN (`retcd://<cluster_id>/node/<node_id>`) matches the
  hinted node id before sending. No hint signature.

### Note (2026-09-18): DuckDB access in tests

`config-testkit::logs` invokes the `duckdb` CLI (`-json`) through `std::process::Command`
instead of linking `duckdb-rs`. Reason: the bundled DuckDB C++ build adds 10+ minutes to a
clean Windows build. Override the binary with `RETCD_DUCKDB`. A missing CLI fails the test
loudly; it never skips.
