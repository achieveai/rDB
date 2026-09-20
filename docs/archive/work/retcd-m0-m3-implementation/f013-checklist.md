# F-013 checklist — gRPC message-size caps

> Reminder: tick each item as it completes. `[x]` done, `[-]` in progress, `[ ]` not started.

## Implementation

- [x] `config_engine::MAX_PAYLOAD_ENTRIES = 16`, used by `NodeConfig::openraft_config`
- [x] `config_grpc::limits` with `client_plane_message_limit` / `peer_plane_message_limit`
- [x] `serve_client_plane` takes `Limits`, caps the generated server both directions
- [x] `serve_peer_plane` takes `Limits`, caps the generated server both directions
- [x] `GrpcPeerTransport::new` takes `Limits`, caps the peer stub both directions
- [x] `GrpcClientOptions::limits` (defaults to `Limits::DEFAULT`), caps the client stub
- [x] `config-server/src/run.rs` threads `node_cfg.limits` into all three
- [x] `config-testkit/src/cluster.rs` threads `cfg.limits` into all four
- [x] ADR-0010 dated note: derivation + `max_payload_entries` choice + known timing limitation

## Tests

- [x] Unit tests for both limit functions (coverage, monotonicity, saturation)
- [x] M3-86 harness row: max-size 0xFF value replicates to every voter
- [x] M3-87 harness row: over-4-MiB `List` reply comes back `truncated`, not `Unavailable`
- [x] Rows added to `docs/testing/test-plan-m2-m3.md` §4.10
- [x] Both rows fail with the caps stubbed to 4 MiB, pass with the real derivation

## Review

- [x] No unnecessary complexity: one module, two functions, four threaded parameters
- [x] No duplication: the two formulas exist once; `MAX_PAYLOAD_ENTRIES` exists once
- [x] No code smells: no long functions, no new branching
- [x] Documented: every new item carries doc comments explaining *why*, not just what
- [x] `cargo test -p config-grpc -p config-client -p config-testkit -p config-server` green
- [x] `cargo test -p config-engine -p config-core -p config-storage` green (no regression)
- [x] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [x] `cargo fmt --all -- --check` clean
- [x] Did not touch `crates/config-engine/src/node.rs` or ADR-0015 (owned by another dev)
