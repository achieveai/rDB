# Correction round 1 — config-grpc / config-client

Branch `feature/m0-m3`, base `7014701`. Owner: dev-grpc (Opus). Lead: Fable.

## Codebase facts established by reading

- `config-grpc/src`: `client_plane.rs`, `peer_plane.rs`, `transport.rs`, `server.rs`,
  `tls.rs`, `error.rs`, `convert.rs`. `config-client/src/lib.rs` is the whole client.
- `error_from_status` currently maps *every* unknown code (incl. transport RST) to
  `ConfigError::Unavailable` → "safe to resubmit". That is B1.
- `config-engine::transport` contract (do not change): `PeerTransport::send(meta, endpoint,
  req, deadline)`, `PeerHandler::handle(meta, req) -> Result<PeerResponse, PeerReject>`.
- `NetFault::guard` (config-engine, NOT owned) applies `delay` *before* running the send
  future. A3 is therefore fixed on our side by moving the per-call `timeout` to wrap the
  whole `guard(...)` call in `GrpcPeerTransport::send`.
- `scan.rs` markers: `// testkit:allow-port`, `// testkit:allow-sleep`. Host prefixes scanned:
  `127.0.0.1:`, `0.0.0.0:`, `localhost:`, `[::1]:`. `203.0.113.9:2379` is NOT scanned.
- 5 literal-port fixture lines: `config-grpc/tests/client_plane.rs:131,149,155,296` and
  `config-client/tests/hint_following.rs:254`.
- Test plan §6 rule 13: names begin with the plan id (`m1_23_…`). Rule 4: ephemeral ports.

## tonic 0.12.3 facts (verified in the vendored source)

- `Request::set_timeout(d)` inserts the `grpc-timeout` metadata directly; format is the most
  precise unit within 8 digits (`400ms` → `400000u`).
- Server-side `GrpcTimeout` layer *reads* `grpc-timeout` from the headers and does **not**
  remove it, so a server handler sees it in `request.metadata()`. It also enforces it and
  fails the call with `Status::cancelled("Timeout expired")`.
  → a hung server now races client-local timeout vs server-side cancel; both land on an
  *unmarked* status, so B1's classification makes both outcomes identical. No flake.

## Rulings implemented

- B1: server stamps `retcd-outcome: rejected` on every status it generates; client treats an
  unmarked error status (except `DEADLINE_EXCEEDED`) as transport-originated →
  `DeadlineExceededUnknownOutcome` for put/delete, `Unavailable` for get/list.
- M3: `request_deadline` is the TOTAL budget; every attempt sets `Request::set_timeout(remaining)`
  and a local `tokio::time::timeout(remaining, …)`; exhausted budget returns the last error.
- M6: `serve_peer_plane(handler, listener, tls, PeerIdentity)`; the responder stamps its own
  identity; `GrpcPeerTransport` verifies the swap before decoding.

## Deviation / blocker note

`crates/config-testkit/src/cluster.rs` (owned by dev-harness, untracked WIP) calls
`serve_peer_plane`/`serve_client_plane` with the old arities. The signature changes are
mandated by M1/M6, so the harness must be updated by its owner. Verification is therefore
`-p config-grpc -p config-client`.

## Round-1 completion state (2026-09-18)

All code, test and doc edits are written. Remaining work is verification only.

Blocker on verification: `config-storage` (another agent, untracked `rocks.rs`/`util.rs`) changed
`config_storage::TypeConfig::Node` from `openraft::BasicNode` to a new `RaftNode { peer, client }`.
`crates/config-engine/src/node.rs` and `network.rs` are *unmodified* and no longer compile
(E0271/E0277 `IntoNodes<u64, RaftNode>` not satisfied; E0609 `no field addr on &RaftNode`).
config-grpc depends on config-engine, so `cargo test -p config-grpc -p config-client` cannot run
until that lands. A separate "engine fix round" developer is on it.

Scanner evidence taken by exact-token grep instead of `config_testkit::scan`, because
config-testkit transitively depends on the same broken crate:
- `tokio::time::sleep|thread::sleep|yield_now` in both `tests/` trees: 0 hits.
- `127.0.0.1:|0.0.0.0:|localhost:|[::1]:` followed by a non-zero port, excluding lines carrying
  `// testkit:allow-port`: 0 hits.

A2 done: root `Cargo.toml` tonic features are now `["tls"]`. No code referenced
`with_native_roots`/`tls_roots`.

## Verification (2026-09-18, after the engine fix landed)

- `cargo test -p config-grpc -p config-client` green three times, the third with
  `--test-threads=1`: 7 lib + 9 client_plane + 6 mtls + 10 peer_plane + 1 doctest (config-grpc),
  10 hint_following (config-client).
- `cargo clippy -p config-grpc -p config-client --all-targets -- -D warnings` clean after two
  fixes: an unused `ClusterId` import in `tests/mtls.rs`, and a justified
  `#[allow(deprecated)]` on `set_linger(Some(Duration::ZERO))` (deprecated because a *non-zero*
  linger blocks the closing thread; zero is the non-blocking RST this fake needs).
- `cargo fmt -- --check` clean; `cargo doc --no-deps` warning-free.
