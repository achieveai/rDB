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

## Note (2026-09-18, fix round): gRPC message caps are derived from `Limits`, not defaulted

Neither plane configured a codec cap, so tonic's 4 MiB receive default applied to both servers
and both clients — a number below what this system legally produces, on both planes.

**Peer plane.** OpenRaft payloads travel as serde JSON (`PAYLOAD_ENCODING_JSON`), and
`serde_json` renders `bytes::Bytes` as an array of decimal numbers: a `0xFF` byte becomes
`255,`, four wire bytes for one. A single `Put` of a `max_value_bytes` (1 MiB) value is
therefore already ~4.2 MiB of `AppendEntries`. The follower answered `OUT_OF_RANGE`, which
`map_status` classifies as `TransportError::Remote` and OpenRaft treats as retryable — so
replication did not fail, it **wedged**, and the write surfaced as
`DeadlineExceededUnknownOutcome` (ADR-0015) forever.

**Client plane.** `Limits::max_list_bytes` is 8 MiB and a `List` reply is filled to that budget
*before* `truncated` is set, so an over-cap prefix produced `Unavailable` where §10.2 promises a
truncated page.

Both caps are now computed in `config_grpc::limits` and applied to the generated server *and*
client types in both directions:

    MESSAGE_FRAMING_SLACK_BYTES = 1 MiB      (one formula, both planes)
    JSON_EXPANSION_FACTOR       = 4          (serde-JSON worst case for a byte string)

    client_plane_message_limit(l) = l.max_list_bytes + MESSAGE_FRAMING_SLACK_BYTES
    peer_plane_message_limit(l)   = JSON_EXPANSION_FACTOR * l.max_request_bytes
                                    * MAX_PAYLOAD_ENTRIES + MESSAGE_FRAMING_SLACK_BYTES

Derived, not literal, so raising a cap in `config-core` cannot leave the transport behind; and
generous in one direction only, because an over-large cap costs a bound nobody reaches while an
under-large one costs a wedged replication stream. `Limits` is consequently threaded into
`serve_client_plane`, `serve_peer_plane`, `GrpcPeerTransport::new` and
`GrpcClientOptions::limits` — it is used for codec sizing there and for nothing else; every
limit is still enforced below the transport.

**`max_payload_entries = 16`** (`config_engine::MAX_PAYLOAD_ENTRIES`, was OpenRaft's default of
300). The peer cap has to bound the largest `AppendEntries` a leader may legally build, and that
is `max_payload_entries` max-size commands; at 300 the honest cap would be ~2.4 GiB, which is not
a cap. 16 keeps it at 128 MiB + slack and costs at most one extra round trip per 16 entries while
a follower catches up. The constant lives next to the config that sets it so the batch size and
the cap cannot drift apart.

**Known, not fixed here: a max-size value is slow, not just large.** Encoding 1 MiB as a JSON
number array, moving 4.2 MiB and decoding it does not finish inside OpenRaft's per-RPC budget —
which is `heartbeat_interval`, 250 ms in the harness default — in an unoptimized build. Rows
M3-86/M3-87 therefore run with a 1000 ms heartbeat. Sizing the caps correctly is necessary but
not sufficient for megabyte values; a production deployment that intends to carry them needs a
heartbeat interval sized for the payload, or a peer encoding that does not expand bytes 4× (this
note deliberately does not redesign that encoding).

### Follow-up (2026-09-18, same fix round): the peer payload encoding is postcard, not serde JSON

The paragraph above closed the *cap* defect and left the *encoding* defect open, with the
consequence written down: a maximum-size value was slow as well as large, so rows M3-86/M3-87
had to run on a 1000 ms heartbeat. That is now fixed at the source.

`PeerRequest` / `PeerResponse` are carried as **postcard**
(`config_engine::transport::PAYLOAD_ENCODING_POSTCARD = 2`) instead of serde JSON. Postcard is a
compact, non-self-describing binary serde format that writes a byte string as a length varint
followed by the bytes themselves: a 1 MiB value is ~1 MiB on the wire rather than ~4.2 MiB of
decimal digits, and neither end pays to render or parse those digits. It is already the format
`config-storage` persists `Entry<TypeConfig>` with, so the OpenRaft types are known to round-trip
through it.

**One encoding per protocol version, never a negotiation.** The `payload_encoding` envelope field
is kept, and a receiver accepts exactly tag `2` and refuses every other value with
`INVALID_ARGUMENT` before decoding — including tag `1`, the retired serde-JSON encoding. A node
from the previous build is therefore turned away by a typed refusal instead of handing bytes to a
decoder that would misread them. The JSON path is deleted rather than kept alongside: two live
encodings would mean two code paths, two sets of size arithmetic, and a downgrade an attacker
could ask for. Peer plane compatibility across this change is a cluster-wide restart, which is
what a pre-release protocol change is allowed to cost.

The peer cap loses its expansion factor accordingly:

    peer_plane_message_limit(l) = l.max_request_bytes * MAX_PAYLOAD_ENTRIES
                                  + MESSAGE_FRAMING_SLACK_BYTES

which is 33 MiB at `Limits::DEFAULT`, down from 129 MiB. There is no binary-overhead factor
because there is nothing to multiply: postcard's framing is a handful of varint bytes per field,
and the 1 MiB slack swallows that many thousands of times over. `MAX_PAYLOAD_ENTRIES` stays at
16 — the smaller cap does not make a bigger batch cheaper to receive, and 16 max-size commands
per `AppendEntries` is already more than a real write burst produces.

**Consequence for the rows.** M3-86/M3-87 run on the harness's default 250 ms heartbeat again;
the 1000 ms override is gone. The caveat the previous note recorded — "a production deployment
that intends to carry megabyte values needs a heartbeat sized for the payload" — no longer
applies at the default limits, because one maximum-size entry now fits inside the default
per-RPC budget in an unoptimized build. It would return for a deployment that raises
`max_request_bytes` far above 2 MiB; the budget is still `heartbeat_interval` per
`AppendEntries`, and that is the number to size against.

## Note (2026-09-18, fix round): the Common Name fallback is opt-in (F-015)

This ADR's cluster binding — "signed by our CA" is not "minted for our cluster" — has one hole
that the SAN grammar cannot close: a certificate that asserts no `retcd://` SAN at all has no
cluster id anywhere in it, so the Common Name it falls back to cannot be checked against the
listener's cluster. Under the shared CA this ADR assumes, a CN-only certificate minted for a
neighbouring cluster was therefore accepted here as that CN, bounded only by the allowlist.

The fallback is now gated by `MtlsConfig::allow_common_name_principals`, default `false`, which
the daemon reads from `tls.allow_common_name_principals` and logs as
`common_name_principals_enabled` at `warn` when it is on. With the gate shut a CN-only client
certificate is refused on the same `UNAUTHENTICATED` path as a certificate carrying no identity
at all. Nothing about certificates that *do* assert a `retcd://` SAN changes: a node identity,
a foreign cluster or a URI the grammar rejects is still refused outright, at either setting. The
peer plane never had a CN fallback and still does not. Covered by M3-88.
