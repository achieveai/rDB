# rEtcd

A distributed configuration service in Rust. One static three-voter OpenRaft group, a
byte-key/byte-value state machine, durable RocksDB storage, gRPC + mTLS transport, JSONL
logging queried with DuckDB. Windows-first (MSVC), Linux expected to work but not gated yet.

Design authority: [`docs/DesignSpec-01.md`](docs/DesignSpec-01.md). Decisions:
[`docs/ADRs/`](docs/ADRs/README.md).

## What this is — and is not

**First release scope is M0–M3.** It delivers:

- `Get`, bounded one-response prefix `List`, `Put`, `Delete`, revision-based CAS;
- one static 3-voter OpenRaft group, leader-linearizable reads, quorum-committed writes;
- durable RocksDB vote/log/state-machine storage with restart correctness;
- a Rust in-process client (`DirectClient`) and a remote gRPC client (`GrpcClient`) over mTLS;
- a thin full-node embedding (`ConfigNode`) and a standalone daemon (`config-server`);
- day-zero encrypted `memberlist` gossip — advisory only, never authoritative;
- stable cluster/Node identity, static formation, typed unknown-outcome behavior, a static
  deployment allowlist.

**It explicitly excludes, not partially:**

- watches (no Watch API, no streaming — first post-release milestone, M4);
- snapshots, log purging, backup/restore;
- dynamic membership (add/remove/promote voters or learners);
- an admin gRPC API;
- multi-key transactions, leases/TTL/sessions/locks, pagination cursors, request
  deduplication, distributed/signed RBAC.

See [`docs/DesignSpec-01.md` §21](docs/DesignSpec-01.md#21-delivery-milestones-and-release-boundary)
for the milestone-by-milestone gate definitions.

## Crate map

Dependency order (left depends on nothing right of it; `config-core` is the hub everything
else points inward to):

`config-log-macros` → `config-log` → `config-core` → {`config-gossip`, `config-storage`} →
`config-engine` → `config-grpc` → `config-client` → `config-testkit` → `config-server`

| Crate | Responsibility |
|---|---|
| `config-log-macros` | `#[retcd_test]` proc-macro: wraps a test in a root tracing span (`testModule`/`testMethod`/`testRun`). |
| `config-log` | JSONL structured logging: the `tracing` layer, process init, trace-context propagation, test-log routing. |
| `config-core` | Stable requests/responses, typed errors, `ConfigStore` trait, deterministic KV state machine, command envelope, identity, authorization contracts, capabilities. No async, no network, no I/O. |
| `config-gossip` | Thin `memberlist =0.8.5` adapter; advisory-only peer hints, never authoritative over membership or data. |
| `config-storage` | OpenRaft `TypeConfig` and the two storage implementations: `EphemeralStore` (M1, in-memory) and `RocksStore` (M2, durable), plus the fault-injection contract. |
| `config-engine` | OpenRaft node lifecycle (`ConfigNode`): formation, client writes, linearizable reads, `DirectClient`. |
| `config-grpc` | Two gRPC planes (client, peer) over mTLS, Protobuf via `protox`, the semantic → gRPC status mapping. |
| `config-client` | `GrpcClient`: remote `ConfigStore` implementation with authenticated leader-hint following. |
| `config-testkit` | Test-only: N-node in-process cluster harness over real gRPC, conformance suite, TLS/manifest fixtures, DuckDB log-query helpers, anti-flake source scanners. |
| `config-server` | The standalone daemon binary: one process = one node, per [ADR-0018](docs/ADRs/0018-daemon-lifecycle-and-cli.md). |

## Build prerequisites (Windows)

- **MSVC** toolchain, target `x86_64-pc-windows-msvc`; Rust `stable` per
  [`rust-toolchain.toml`](rust-toolchain.toml) (components `rustfmt`, `clippy`).
- **LLVM**, for RocksDB's `bindgen` build step. Install via `winget install LLVM.LLVM`.
  `LIBCLANG_PATH` is set in [`.cargo/config.toml`](.cargo/config.toml)
  (`C:\Program Files\LLVM\bin`). The `rocksdb` crate is pinned with the
  `bindgen-runtime` feature so `clang-sys` loads `libclang.dll` through `libloading` and
  honors `LIBCLANG_PATH` directly — putting LLVM's `bin` on `PATH` also works but is not the
  recorded fix (see [ADR-0017](docs/ADRs/0017-build-toolchain.md)'s 2026-09-18 note).
- **No `protoc`.** Protobuf is compiled at build time by `protox` (pure Rust).
- **DuckDB CLI**, for querying test/daemon JSONL logs (see [`docs/logging.md`](docs/logging.md)).
  Either put `duckdb` on `PATH` or set `RETCD_DUCKDB` to its full path.

## Build and test

```
cargo build --workspace
cargo test --workspace
cargo test -p <crate>              # e.g. cargo test -p config-engine
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

`clippy -D warnings` and `fmt --check` are CI gates (ADR-0017). The first RocksDB build is
slow — measured cold at ~2m47s on the authoring host — because `librocksdb-sys` compiles the
bundled C++ library. `EphemeralStore` (no RocksDB) keeps the day-to-day M1 test loop fast;
reach for `cargo test -p config-core -p config-engine` etc. while iterating.

## Running the daemon

`config-server` is the standalone binary: one process, one node, TOML config, JSONL logs.

```
config-server --config node.toml --form
```

It prints exactly one line on stdout — the ready line — then everything else goes to the log
file:

```json
{"ready":true,"node_id":1,"peer":"127.0.0.1:52431","client":"127.0.0.1:52432","gossip":"127.0.0.1:52433","health":"127.0.0.1:52434"}
```

Exit codes: `0` clean shutdown or `--capabilities`; `2` configuration/identity/TLS/manifest
refusal; `3` fatal storage failure.

Full flag reference, TOML schema, authorization policy format, and the health endpoint are in
[`crates/config-server/README.md`](crates/config-server/README.md) — that is the canonical
operator doc; this section only orients you to it.

## Embedding the library

An embedding application builds a `ConfigNode` directly instead of running the daemon. This is
the real M1 shape (single-voter, in-process transport, allow-all authorization, no gossip) —
production embedding additionally wires `RocksStore`, `GrpcPeerTransport`/mTLS, and a static
allowlist:

```rust
use std::sync::Arc;
use config_core::{
    AllowAll, ClusterId, ClusterIdentity, ConfigStore, Limits, NoGossip, NodeId, Principal,
    PrincipalKind, PutRequest, RecoveryEpoch,
};
use config_engine::{ConfigNode, FormationPlan, InProcTransport, NetFault, NodeConfig, StorageHandle};
use config_storage::{EphemeralStore, NoFaults};

let identity = ClusterIdentity { cluster_id: ClusterId::from_bytes([1; 16]),
    recovery_epoch: RecoveryEpoch(0), node_id: NodeId(1) };
let cfg = NodeConfig::new(identity, InProcTransport::endpoint(identity.node_id));
let store = EphemeralStore::new(identity, Limits::DEFAULT, Arc::new(NoFaults), tracing::Span::current());
let transport = Arc::new(InProcTransport::new(NetFault::new()));

let node = ConfigNode::start(cfg.clone(), StorageHandle::Ephemeral(store),
    transport.clone(), Arc::new(NoGossip), Arc::new(AllowAll)).await?;
transport.register(identity.node_id, node.peer_handler());
node.form_cluster(FormationPlan::new(&identity, [(identity.node_id, cfg.peer_endpoint.clone())])).await?;

let client = node.direct_client(Principal::new("app", PrincipalKind::Embedded));
client.put(PutRequest { key: b"k".to_vec().into(), value: b"v".to_vec().into(),
    expected_mod_revision: None }).await?;
```

`DirectClient` does not bypass Raft, authorization, or the linearizable-read barrier — it is
the same semantic path a remote gRPC caller takes (spec §6.1). Signatures verified against
`crates/config-engine/src/{node.rs,direct.rs,config.rs,testing.rs,error.rs}` and
`crates/config-core/src/{identity.rs,authz.rs,hint.rs,limits.rs,types.rs}`.

## Logging and observability

Every crate logs structured JSONL via `config-log` (ADR-0013). Full field reference, file
layout, redaction rules, and a DuckDB query cookbook: [`docs/logging.md`](docs/logging.md).

## Testing philosophy

Test plans (the contract for what each milestone must prove, and the per-row backlog):

- [`docs/testing/test-plan-m0-m1.md`](docs/testing/test-plan-m0-m1.md) — M0 (deterministic
  state machine) and M1 (three-node in-process core).
- [`docs/testing/test-plan-m2-m3.md`](docs/testing/test-plan-m2-m3.md) — M2 (persistence,
  restart correctness) and M3 (safe remote use baseline).

Anti-flake rules enforced across the suite (normative; a violation is rejected in review):

- **No fixed sleeps.** Every wait is deadline-bounded (`wait_for_leader`, `wait_applied`,
  `poll_until`), expressed as a multiple of the configured election timeout, not a literal
  duration.
- **No literal ports.** Every listener binds `127.0.0.1:0`; the OS assigns the port.
- **Per-test JSONL.** Every test opens `#[config_log::retcd_test]`; every line it (or a node
  it spawns) emits carries `testModule`/`testMethod`/`testRun` and lands in
  `target/test-logs/<module>/<method>.jsonl`.
- **DuckDB assertions, not eyeballing.** Log-based test assertions query that JSONL through
  `config_testkit::logs::query()`, and an empty result where rows are expected is itself a
  test failure (`assert_nonempty`), never a silent pass.
- **The scanners.** `config_testkit::scan` source-scans `tests/**` for banned sleep patterns
  and literal ports so these rules are machine-enforced, not review-enforced.

## ADR index

[`docs/ADRs/README.md`](docs/ADRs/README.md) indexes every Accepted decision (ADR-0000
through ADR-0018): consensus, storage, transport/mTLS, authorization, logging, build
toolchain, daemon lifecycle, and more.

## Milestone status

| Milestone | Status (2026-09-18) |
|---|---|
| M0 — deterministic state-machine laboratory | Committed (`7014701`). |
| M1 — three-node distributed core | Delivered in `01a58b8`; test-plan rows M1-01..49 covered. |
| M2 — persistence and restart correctness | Delivered in `01a58b8`; `RocksStore` and the M2 harness delivered. |
| M3 — safe remote use baseline (first release gate) | Delivered in `01a58b8`; `config-server` daemon, TLS/manifest fixtures and E2E-01..17 delivered. |

Each gate commit follows a critic review of that milestone's code and tests. The progress
dashboard at [`docs/progress/index.html`](docs/progress/index.html) carries the live state.

## License

`Apache-2.0`, per the workspace [`Cargo.toml`](Cargo.toml) (`license = "Apache-2.0"`). No
`LICENSE` file exists at the repository root yet — add one before external distribution.
