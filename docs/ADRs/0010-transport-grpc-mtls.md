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

## Notes (2026-09-18, M1 delivery)

- Additive response metadata: `retcd-leader-node-id` / `retcd-leader-endpoint` on `NotLeader`, and
  `retcd-conflict-exists` / `retcd-conflict-mod-revision` when a `FAILED_PRECONDITION` carries a
  CAS conflict. Both `NotLeader` and `Conflict` map to `FAILED_PRECONDITION` (§6.2), so the
  client needs a structured marker instead of parsing prose. No key or value bytes travel in
  metadata or status messages.
- `PeerService.InstallSnapshot` answers `UNIMPLEMENTED` without decoding (ADR-0008: no snapshots).
- `MtlsConfig.server_domain` optionally names the DNS identity used for peer dialing, because
  committed endpoints are `host:port` while certificates carry names.
- `GrpcClientOptions.max_hint_follows = 3` means at most three follows, four sends per call.
- `GrpcClient::capabilities()` is configuration (`expected_capabilities`), not discovery: the
  normative schema has no capabilities RPC. `config-server` always sets it.
- Dependencies `x509-parser` (SAN/CN parsing) and `tokio-stream` are pinned in the root manifest.
- `ConfigError::NotFound` maps to `NOT_FOUND` (not to an empty successful response). Absence of a
  key is reported by `GetResponse.record == None` on the success path; `NOT_FOUND` is reserved for
  a store that was asked for something it cannot name.
- Client-plane principal derivation (ADR-0012) is bound to the serving cluster:
  `serve_client_plane` takes the `ClusterId`, and a leaf certificate asserting a `retcd://` client
  identity for a *different* cluster — or a node identity, or a malformed rEtcd URI — is
  `UNAUTHENTICATED`. Common-name fallback applies only when the leaf asserts no `retcd://` SAN URI
  at all, so a cert cannot downgrade itself to CN by presenting an identity that fails to parse.
- Peer responses are stamped with the *responder's* own identity (`PeerIdentity`), and
  `GrpcPeerTransport` verifies `from`/`to`/`cluster_id` against what it sent before decoding the
  payload. A misrouted or foreign answer is `TransportError::IdentityRejected` (ADR-0011).
- Every rejection that happens before the handler runs logs one `warn!` inside the RPC span with
  `rpc`, `reason`, `latency_ms` and the principal when known (ADR-0013). Without it an
  authentication failure was invisible in the JSONL: the handler never ran, so nothing logged.

## Notes (2026-09-18, M3): per-target peer identity and authenticated hints

- **`peer_server_domain(cluster_id, node_id)` is the one definition** of `node-<id>.<cluster>.retcd`,
  the DNS SAN a node certificate carries. It lives in `config_grpc::tls` and is re-exported at
  the crate root; the testkit fixture, the peer transport and the client all call it. tonic can
  express a per-connection DNS name and cannot express a per-call URI-SAN check, so the node
  identity the peer plane verifies *inside* the envelope is mirrored into a name TLS itself can
  check *before* any payload moves (m3-architecture §3). Two spellings of that string would be a
  hole that shows up only as an accepted impostor, never as a test failure.
- **`GrpcPeerTransport` derives the verified name from the envelope**, `peer_server_domain(meta.cluster_id, meta.to)`,
  and caches channels by `(endpoint, to)`. A dial is therefore verified against the node the
  envelope addresses. Without it, every check runs *after* the handshake — same CA, same cluster,
  `from`/`to`/`cluster_id` all agreeing — and a member holding its own entirely valid certificate
  satisfies a dial addressed to a different member. `MtlsConfig::server_domain` is consequently
  **ignored on the peer plane**: it names one server, and this transport needs one name per peer.
  It still applies to the client plane. `config-server` needed no change: the transport does it.
- **Authenticated leader hints (OQ-21).** `GrpcClient::with_cluster_id` makes a hint follow
  verifiable: the hint dial is pinned to `peer_server_domain(cluster_id, hinted_node_id)`, so an
  impostor at a hinted endpoint fails the handshake and the mutation is never written (M3-54).
  Restricting follows to the configured endpoint set was never enough — it proves the operator
  trusts the *address*, not who answers there. Under `MutualTls` **without** a cluster id the
  hint is not followed at all and one `hint_identity_unverified` warning is logged per client:
  a deployment that issued certificates plainly cares who it talks to, and following an
  unverifiable redirect would quietly undo that. `TlsMode::Insecure` is unchanged — nothing is
  verifiable there and nothing is claimed. The builder keeps `GrpcClient::connect` source
  compatible.

## Note (2026-09-18, fix round): a TLS rejection is observable on the dialling side only

A peer or client refused at the TLS layer leaves **no structured line on the accepting node**,
and in this release it stays that way.

tonic 0.12.3 performs the server handshake inside `transport::server::incoming::tcp_incoming`:
each accepted stream is handed to `TlsAcceptor::accept` on a `JoinSet` task, and a failure
reaches `handle_accept_error`, which writes `tracing::debug!(error = %e, "accept loop error")`
and then *continues the loop* for `InvalidData` and `UnexpectedEof` — the two kinds a failed
handshake produces. There is no hook, no callback and no error stream a server can subscribe to,
and by the time the error exists the `TcpStream` has been consumed, so the remote address is no
longer available to attach to a line even if one were emitted. Re-acquiring it would mean
accepting and terminating TLS ourselves and handing tonic already-negotiated streams — which
also means reconstructing the `TlsConnectInfo` plumbing that `Request::peer_certs` reads, and
that type's constructor is private. That is a transport rewrite to gain one log line, so it is
not cheap and it is not done.

What remains observable:

- **The dialler sees it and says so.** `GrpcPeerTransport` maps the failure to a
  `TransportError` (`IdentityRejected` for an `Unauthenticated` refusal, `Unreachable`/`Network`
  for a handshake that dies as a connection error) and logs it with the endpoint and the peer
  the envelope addressed. `config-client` reports it as `ConfigError::Unavailable` from the
  connect phase, with a `client connect attempt failed` line (ADR-0015, fix-round note).
- **A refusal *above* TLS is fully logged on the accepting node.** A certificate that completes
  the handshake but yields no client principal produces one `warn` `rpc rejected` line with
  `reason="unauthenticated"` inside the caller's trace, and increments
  `HealthPayload::authn_rejected` (M3-81). Only the handshake itself is silent.

The operational consequence, stated so nobody has to rediscover it: **diagnosing a rejected
certificate starts at the dialling side.** An operator looking only at the accepting node's log
for a peer that "cannot connect" will find nothing, and the absence is not evidence that the
connection was never attempted. Revisit if tonic exposes handshake errors, or when a listener
that terminates TLS itself is needed for another reason.
