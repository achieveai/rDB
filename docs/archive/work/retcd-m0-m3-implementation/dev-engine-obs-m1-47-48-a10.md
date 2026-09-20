# dev-engine-obs: M1-47, M1-48, A10 (2026-09-18)

## M1-47 trace propagation
- `config_core::Command` has NO trace field and must not get one (ADR-0007: canonical bytes are
  the determinism oracle). Side table instead.
- `config-storage/src/trace.rs`: FNV-1a 128-bit `fingerprint(&Command)` -> `TraceRegistry`
  (bounded 1024, FIFO evict).
- Leader records fingerprint->trace_id in `mutate_inner` before `client_write`.
- `EngineNetwork::meta()` stamps the client trace onto the EXISTING `PeerEnvelopeMeta.trace`
  when every Normal entry in the AppendEntries maps to the same trace. No wire change.
- `PeerSink::handle` records the envelope trace for each Normal entry on ingress.
- Both stores' apply look it up and emit `trace_id` on the debug apply line.
- Known limits: identical commands share a fingerprint (newest trace wins); mixed batch
  propagates the replication RPC's own trace.

## M1-48
- netfault: every line carries `node_id` = originating side; `unblock_all` emits one line per
  node whose rules it cleared.
- Q2 split in two: node_id half scoped by `current_run_filter()` (target/test-logs accumulates
  across cargo invocations -> repo-wide is non-hermetic); test-context half repo-wide but
  restricted to config_engine%/config_grpc%/config_gossip%.
- Exempt: config_core%/config_storage% (also run as pure library units, no node), openraft%,
  memberlist%.
- Non-vacuity assertion added (run must produce >0 node-scoped lines).
- ADR-0013 amended with the exact rule.

## A10
- `recovery_epoch: RecoveryEpoch` added to `config_core::ObservedPeerHint` (after cluster_id).
- `validate_hint`: `REASON_EPOCH_MISMATCH = "recovery_epoch mismatch"`, checked AFTER
  cluster_mismatch and BEFORE self_claim.
- HINT_WIRE_VERSION stays 1 (nothing shipped; golden vector updated, now 50 bytes).
- `PoisonSpec::WrongEpoch` + `GossipControl.recovery_epoch` added to testkit cluster.rs.
- config-server/src/run.rs already had `recovery_epoch:` (dev-server added it) - no edit needed.

## Open for other owners
- config_grpc::{server,client_plane,peer_plane} lines lack node_id in config-grpc's and
  config-client's OWN test runs (bare server started outside a node span). Production path is
  fine. dev-grpc: wrap the serve_*/client construction in those tests in a node/test span.
