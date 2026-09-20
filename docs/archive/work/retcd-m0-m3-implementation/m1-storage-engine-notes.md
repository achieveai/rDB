# M1 config-storage + config-engine — implementation notes (dev-engine)

Status: COMPLETE. Not committed (per instruction).

## Delivered

`crates/config-storage/`
- `src/ephemeral.rs` (new) — `EphemeralStore` + `EphemeralLog` (RaftLogReader/RaftLogStorage),
  `EphemeralSm` (RaftStateMachine), `NoSnapshots`, `EphemeralReader` (StateReader).
- `src/fault.rs` (edited) — added `FaultCounters`, `Boundary::index()`.
- `src/lib.rs` (edited) — module + re-exports.
- `tests/ephemeral.rs` (new) — 10 tests.

`crates/config-engine/`
- `src/config.rs` — `RaftTimers`, `AuthzKind`, `NodeConfig`, `StorageHandle`.
- `src/error.rs` — `EngineError`, `FormationPlan`, `FormationError`, `Timeout`.
- `src/metrics.rs` — `LogIdView`, `NodeRole`, `NodeMetrics`, `Health`, `MembershipView`.
- `src/hint.rs` — `HintVerdict`, `validate_hint`, reason constants.
- `src/network.rs` (private) — `RaftNetworkFactory`/`RaftNetwork` over `PeerTransport`.
- `src/node.rs` — `ConfigNode` (start/form_cluster/put/delete/get/list/metrics/health/
  capabilities/state_hash/committed_membership/leader_hint/wait_*/stop), `PeerSink` impl,
  background gossip-poll + metrics-delta task.
- `src/direct.rs` — `DirectClient` (impl `ConfigStore`), `ConfigNode::direct_client`.
- `src/testing.rs` — `InProcTransport` (public).
- `tests/common/mod.rs`, `tests/m1_cluster.rs` (11), `tests/m1_hints.rs` (2).

## Rulings / deviations to remember

1. `form_cluster` checks **already formed before freshness**, so a second formation on a live
   cluster reports `AlreadyFormed` (brief's test expectation) while a dirty-but-unformed store
   still reports `StoreNotFresh`.
2. No `validate_get` in config-core → engine-private `validate_get` in `node.rs` duplicating
   config-core's key rules byte-for-byte. Recommend adding `validate_get` to config-core later.
3. `PeerTransport::send(meta, endpoint, req, deadline)` — kept the existing shared
   `transport.rs` signature (shared with config-grpc); transport.rs was NOT modified.
4. `InProcTransport` routes by `inproc://<node_id>`; formation plans must use
   `InProcTransport::endpoint(id)`.
5. `wait_for_leader -> Option<NodeId>` (m1-architecture §2), `wait_applied -> Result<(), Timeout>`.
6. `#[allow(clippy::result_large_err)]` on `Shared::boundary` — `StorageError` is openraft's.
7. Store span is caller-supplied; the harness passes `info_span!("store", node_id)` so apply
   lines carry `node_id`.

## Evidence (2026-09-18)

- `cargo test -p config-storage -p config-engine` → 26 pass (3+11+2+10), 3.60 s.
- same with `-- --test-threads=1` → 26 pass, 7.16 s (m1_cluster 7.16 s for 11 tests).
- `cargo clippy -p config-storage -p config-engine --all-targets -- -D warnings` → clean.
- `cargo fmt -p config-storage -p config-engine` → applied.
- `cargo doc --no-deps -p config-storage -p config-engine` → no warnings.
- `cargo build --workspace` → ok (includes config-grpc/config-client).
