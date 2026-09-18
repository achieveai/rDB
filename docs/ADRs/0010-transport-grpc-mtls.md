# ADR-0010: gRPC/Protobuf transport, `protox`, mTLS planes, error mapping

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §6.2, §15.1, §21 M1/M3

## Decision

- Protobuf lives in `proto/retcd/v1/config.proto` (client plane) and `proto/retcd/v1/peer.proto`
  (Raft plane). `config-grpc/build.rs` compiles them with `protox` + `tonic-build`; no
  `protoc` binary is required.
- `ConfigService` is exactly the §6.2 schema with the given field numbers. `Watch*` messages
  and RPC names are *not* declared; a comment block documents that the spec pre-assigns their
  tags for a later milestone.
- `PeerService` carries OpenRaft RPCs as opaque `bytes` payloads (serde-encoded openraft
  request/response types) plus `cluster_id`, `recovery_epoch`, `from_node_id`, `to_node_id`
  headers for identity binding. The snapshot RPC exists in the OpenRaft network trait but our
  implementation returns an error and is never triggered (ADR-0008).
- Two tonic servers per node, each with its own listener and TLS profile: client plane and
  peer plane. `rustls` via `tonic::transport::ServerTlsConfig` with a client CA → mutual TLS.
  Plain-TCP mode exists only behind `TlsMode::Insecure` for M1 in-process tests and is refused
  by `config-server` unless `--allow-insecure-dev` is passed.
- Error mapping (semantic → gRPC status) is exactly the §6.2 table; `NotLeader` adds metadata
  `retcd-leader-node-id` and `retcd-leader-endpoint` derived from committed membership.
- Trace context is propagated in metadata `retcd-trace-id`, `retcd-request-id`,
  `retcd-parent-span` (ADR-0013).

## Consequences

- OpenRaft's `RaftNetwork` impl in `config-engine` uses a tonic `PeerServiceClient` per peer
  with lazy connect and bounded backoff; connection failures map to OpenRaft `Unreachable`.

## Verification

- Conformance suite runs identically over `DirectClient` and `GrpcClient` (ADR-0014).
- M3 identity tests: wrong cluster id / node id / cert → rejected with `UNAUTHENTICATED` /
  `PERMISSION_DENIED`.
