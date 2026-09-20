# dev-grpc-2: research for lead rulings R1/R2/R3 (2026-09-18)

Owner: config-grpc / config-client developer. Scope: crates/config-grpc, crates/config-client,
docs/ADRs/0015, one call in crates/config-testkit/src/tls.rs.

## Codebase facts gathered (read, not inferred)

- `config_testkit::tls::peer_dns_name(cluster, node) = format!("node-{node_id}.{cluster_id}.retcd")`
  (crates/config-testkit/src/tls.rs). `ClusterId: Display` = 32 lowercase hex
  (config-core/src/identity.rs:25). `NodeId: Display` = bare u64.
- Node leaf certs carry: URI SAN `retcd://<cluster>/node/<id>`, DNS `node-<id>.<cluster>.retcd`,
  DNS `localhost`, IP 127.0.0.1, IP ::1. So pinning the derived DNS name works against fixture certs.
- `MtlsConfig.server_domain: Option<String>` + `with_server_domain` + `client_tls_config()`
  (config-grpc/src/tls.rs). Used by the harness `PerTargetTransport` via `CertPair::mtls_verifying`.
- `GrpcPeerTransport` caches `HashMap<String /*endpoint*/, Channel>` and uses `connect_lazy`
  (config-grpc/src/transport.rs). `cached_endpoints()` is asserted in
  config-grpc/tests/peer_plane.rs:188.
- `crates/config-testkit/src/cluster.rs:2134` builds one `GrpcPeerTransport` per target with
  `own.mtls_verifying(fixture.peer_domain(to))`. NOT ours to edit. After R1 the derived domain is
  the same string, so the harness keeps working (server_domain becomes a no-op on the peer plane).
- `config-server/src/run.rs:232` `GrpcPeerTransport::new(tls.clone(), NetFault::new())` — no domain.
  R1 fixes it library-side with no server change.
- `config-client`: `build_channel` uses `connect_lazy`; channels built eagerly in `connect()` and
  stored in `Arc<HashMap<String, Channel>>`. `classify()` implements the ADR-0015 table.
- `config_grpc::server::spawn` wraps the serving task in `config_log::testing::in_current_span`,
  so wrapping a `serve_*_plane` call site in a span propagates `node_id` to the server task.
- ADR-0013 Q2 enforcement (config-testkit/tests/m1_observability.rs) is scoped to
  `testRun = test_run_id()`, which is per *process*. config-grpc/config-client test binaries have
  their own run id, so they are not caught by that query today — but the ADR's rule text
  ("every line on a `config_grpc*` target carries node_id") does bind them. Cheap fix: node span.

## Test-plan rows

- M3-54 (line 574): hint at an endpoint whose server cert is for a different node id → client
  refuses to follow, does not send the mutation there.
- M3-64 (line 589): stop the pinned node, then `put` → client reconnects at most N times and
  returns `Unavailable`; `sends` counts the attempts and is <= 4; mutation never enters a log.
  => `sends` counts *attempts*, including failed connects. Reconnect bound = `max_hint_follows`
  (3), so 1 + 3 = 4 attempts max.

## Design

### R1
- `config_grpc::tls::peer_server_domain(&ClusterId, NodeId) -> String`, re-exported at crate root.
- `MtlsConfig::client_tls_config_for(&str) -> ClientTlsConfig` (ignores `server_domain`).
- `MtlsConfig.server_domain` doc: client plane only; the peer plane derives its own name.
- `GrpcPeerTransport.channels: HashMap<(String, NodeId), Channel>` keyed by (endpoint, envelope
  `to`), so an endpoint is always verified against the node the envelope addresses.
- testkit `peer_dns_name` delegates to `config_grpc::peer_server_domain`.

### R2
- `GrpcClient::with_cluster_id(ClusterId) -> Self` builder (no new field on `GrpcClientOptions`,
  so struct literals keep compiling).
- Hint follow under `MutualTls`: with cluster_id -> dial with
  `server_domain = peer_server_domain(cid, hint.node_id)`; without -> do not follow, warn once
  (`hint_identity_unverified`). `Insecure` unchanged.

### R3
- Channels are connected with an explicit, budget-bounded `Endpoint::connect().await` at first
  use, cached per (endpoint, pinned-domain) key, evicted on any transport-level failure.
- Connect failure (refused TCP, refused handshake, rejected cert) -> `ConfigError::Unavailable`
  for reads AND mutations, because nothing was written.
- Connect failures are retried up to `max_hint_follows` times inside the remaining budget.
- Residual (documented): a channel already connected whose peer then dies fails at *request*
  time and is indistinguishable from a mid-flight drop, so a mutation there stays
  `DeadlineExceededUnknownOutcome`. Safe direction per ADR-0015. Eviction shrinks the window to
  one operation.

## Final state (2026-09-18)

- R1/R2/R3 implemented and green. `cargo test -p config-grpc -p config-client` green on two
  consecutive runs; `cargo test -p config-testkit --test m3_harness_smoke --test m1_harness_smoke`
  green (the `PerTargetTransport` path still works with the peer-plane pin).
- `ClientStats::sends` deliberately counts only requests put on the wire. An earlier version
  counted connect attempts too and broke `e2e_15`, which synchronises on `sends >= 1` before
  killing the leader: the kill then raced the connect phase and the mutation came back
  `Unavailable` instead of unknown-outcome. Connect attempts are counted in the new
  `ClientStats::reconnects`, which is what the M3-64 "reconnect bounded" assertion reads.
- Blocked on one out-of-scope line: `client_for` in `crates/config-server/tests/e2e_daemon.rs:38`
  builds an mTLS `GrpcClient` with no cluster id, so under R2 it no longer follows leader hints.
  `e2e_05` and `e2e_15` fail on that (`NotLeader { hint: Some(..) }` from a post-re-election
  call), leader-placement dependent so they flap. Fix is `.with_cluster_id(harness.cluster_id)`
  on the returned client. dev-server owns it.
- Workspace-wide `cargo fmt --check` and `cargo clippy --workspace` fail only in
  `crates/config-testkit/tests/m3_client_mtls.rs` and `m3_peer_mtls.rs` — untracked, in-progress
  files owned by tester-m3. Scoped `-p config-grpc -p config-client -p config-server` is clean.
