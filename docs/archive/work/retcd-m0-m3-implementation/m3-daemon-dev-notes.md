# M3 daemon (config-server) developer notes — dev-server

Research and rulings collected before writing `crates/config-server`, `config_testkit::tls`
and `config_testkit::manifest`. Binding inputs: ADR-0018, `m3-architecture.md` §3-§5,
`docs/testing/test-plan-m2-m3.md` TA-17..TA-27 and §5.

## 1. Library surface the daemon composes (verified by reading the sources)

| Need | API | File |
|---|---|---|
| open store | `RocksStore::open_with(dir, identity, limits, faults, span, RocksOptions{sync_writes, create_if_missing})` | `config-storage/src/rocks.rs` |
| store handle | `StorageHandle::Rocks(RocksStore)` (`From<RocksStore>`) | `config-engine/src/config.rs` |
| node | `ConfigNode::start(NodeConfig, StorageHandle, Arc<dyn PeerTransport>, Arc<dyn GossipObservationSource>, Arc<dyn Authorizer>)` | `config-engine/src/node.rs` |
| formation | `ConfigNode::form_cluster(FormationPlan::with_client_endpoints(..))` | `config-engine/src/error.rs` |
| health oracle | `ConfigNode::health_payload().await -> HealthPayload` (17 fields, `Serialize`) | `config-engine/src/metrics.rs` |
| capabilities | `ConfigNode::capabilities()`; `Capabilities` is `Serialize` | `config-core/src/capabilities.rs` |
| peer transport | `GrpcPeerTransport::new(TlsMode, NetFault)` | `config-grpc/src/transport.rs` |
| planes | `serve_peer_plane(handler, listener, tls, PeerIdentity)`, `serve_client_plane(Arc<dyn ClientBackend>, listener, tls, cluster_id)` → `ServerHandle{local_addr, shutdown}` | `config-grpc/src/{peer,client}_plane.rs` |
| gossip | `GossipNode::start(GossipConfig, ObservedPeerHint)`, `.advertise_addr()`, `.join(&seeds)`, `.shutdown()` | `config-gossip/src/node.rs` |
| authz | `AllowAll`, `StaticAllowlist::new(AllowlistPolicy)`; `AllowlistPolicy: Deserialize` | `config-core/src/authz.rs` |
| logging | `config_log::layer::JsonlLayer::new(app, Box<dyn Write+Send>, test_log_dir, include_location)` | `config-log/src/layer.rs` |

The composition is already demonstrated by `config-testkit/src/cluster.rs::start_running`;
the daemon is the out-of-process version of the same wiring.

## 2. Decisions taken (and why)

1. **`--log-field k=v` cannot be a `tracing` span field.** `tracing` field names are fixed at
   the macro callsite; arbitrary runtime keys are impossible through `info_span!`. Neither is
   `config_log::init` usable, because it installs the whole registry and takes no extra layer.
   Ruling: `config-server` builds its own subscriber
   (`EnvFilter` + `JsonlLayer` + a local `ConstFieldsLayer`). `ConstFieldsLayer` merges the
   `--log-field` map into the root span's `config_log::layer::JsonFields` (a `pub` newtype over
   `serde_json::Map`), so every line carries them exactly like a span field. Layer order
   matters: `registry().with(filter).with(jsonl).with(const_fields)` — `Layered` calls the
   inner layer's `on_new_span` first, so `ConstFieldsLayer` merges into what `JsonlLayer`
   already inserted instead of being overwritten.
2. **Log file naming.** `--log-dir/<node_id>.jsonl` (assignment ruling) *and* `test_log_dir =
   Some(--log-dir)`, so when the harness passes `--log-field testModule=… testMethod=…`
   `JsonlLayer` additionally routes those lines to `<log-dir>/<module>/<method>.jsonl`. That
   satisfies both ADR-0018 and test-plan E2E-11/TA-20.3 without an env var.
3. **Ports.** The daemon supports `port 0` and reports the real port, but 3-node *formation*
   needs every peer's endpoint in the manifest *before* any node starts. E2E therefore
   pre-allocates with `config_testkit::ports::ephemeral_listener` (bind, read port, drop) and
   writes explicit addresses. Tiny TOCTOU window, documented at the call site.
4. **`DaemonSpec`/`DaemonProcess` live in `crates/config-server/tests/support/daemon.rs`**, not
   in the testkit (assignment ruling): `CARGO_BIN_EXE_config-server` only exists for
   integration tests of the package declaring the binary, and a testkit→binary dependency
   would be circular.
5. **TLS key determinism.** rcgen 0.13 has no seeded-RNG constructor (`KeyPair::generate()` and
   `generate_for()` use `SystemRandom`). It *does* accept supplied PKCS#8 material via
   `KeyPair::from_pkcs8_pem_and_sign_algo(pem, &rcgen::PKCS_ED25519)`. `TlsFixture` therefore
   derives each key's 32-byte Ed25519 seed as `SHA-256("retcd-testkit-tls-v1" || seed_le ||
   label)` and turns it into PKCS#8 PEM with `ed25519_dalek::SigningKey::to_pkcs8_pem`
   (features `pkcs8`, `pem`). Serial numbers are derived from the same digest. Result: the
   fixture is fully deterministic from `(cluster_id, seed)` — no "print the seed and hope".
6. **Extra SANs on node certificates.** Peer and client certificates are served on
   `127.0.0.1:<ephemeral>` and `GrpcPeerTransport`/`GrpcClient` dial that literal address with
   `MtlsConfig::server_domain = None`, so rustls verifies the certificate against the IP.
   Node certs therefore carry, in addition to the two required SANs: `IP:127.0.0.1`,
   `IP:::1`, `DNS:localhost`. Without them no mTLS E2E row can connect at all.
7. **Health endpoint** is hand-written HTTP/1.1 over `tokio::net::TcpListener`; non-loopback
   `health_listen` is rejected during config validation (ADR-0018 §2: loopback only).

## 3. Exit codes

`0` clean; `2` configuration / identity / TLS-gate / manifest rejection (always before any
listener binds, except formation which happens after bind); `3` fatal storage.

## 4. Corrections found while building (supersede §2 where they conflict)

1. **`JsonlLayer` routing is exclusive, not additive.** `write_line` sends a line carrying
   `testMethod` to `<test_log_dir>/<module>/<method>.jsonl` and **returns** — the default
   writer never sees it. With `--log-field testMethod=…` the process file
   `<log-dir>/<node_id>.jsonl` was therefore almost empty. Two `JsonlLayer`s cannot be stacked
   either: both insert `JsonFields` into the same span extensions and
   `tracing_subscriber::registry::Extensions::insert` asserts the slot is empty (the daemon
   panicked at startup). Fix: one layer with `test_log_dir = None` and a `Tee` writer in
   `logging.rs` that writes each line to the process file and, when `--log-field
   testModule/testMethod` were given, to `config_log::layer::test_file_path(...)` as well. The
   destination is constant for the process, so no per-line inspection is needed.
2. **`shutdown_complete` cannot be logged from inside the runtime.** OpenRaft's tick loop logs
   "TickLoop received cancel signal, quit" *after* `node.stop()` returns, so it was the real
   last line. `run::shutdown` now logs `drained`, and `main` logs `shutdown_complete` after
   `runtime.shutdown_timeout(...)`, when no task can still write.
3. **Serde casing differs between the two reports.** `Capabilities` serializes PascalCase
   (`"Persistent"`, `"StaticAllowlist"`, `"MutualTls"`); `HealthPayload.authz_kind` is
   snake_case (`"static_allowlist"`) because `AuthzKind` renames. E2E-02 asserts both shapes.
4. **The E2E allowlist grant must use an empty prefix.** `ConformanceConfig::unique` namespaces
   keys under `__conformance/…`, so a `prefix = "/"` grant denies E2E-03 for an unrelated
   reason.
5. **Apply log line.** `config_storage::rocks` logs `@m="applied command entry"` with
   `op="apply"` and `command="put"` (the field split changed during M3). E2E-10 joins on
   `trace_id` and matches `op="apply"`.
6. **`ObservedPeerHint` gained `recovery_epoch`** (config-core, ADR-0011 hint epoch check);
   `run::start_gossip` now sets `recovery_epoch: cfg.identity.recovery_epoch`.

## 5. Status

`crates/config-server` complete: binary, README, `tests/support/{mod,daemon}.rs`,
`tests/e2e_daemon.rs` with E2E-01..E2E-17 (plus `e2e_17b_drop_kills_the_child`). All 18 rows
pass; whole suite 2.5 s parallel / 13.7 s single-threaded; slowest row ~1.1 s.
