# M1 notes — `config-testkit::cluster` (Developer, 2026-09-18)

Research + decisions for the `Cluster` harness (test plan §4.1) and the M1-01..M1-16 rows.
Owned files: `crates/config-testkit/src/cluster.rs`, `lib.rs` re-exports, `Cargo.toml`,
`crates/config-testkit/tests/{m1_cluster.rs, m1_harness_smoke.rs}`.

## Verified facts from the source (not assumptions)

- `ConfigNode` is a cheap `Clone` handle (`Arc<NodeInner>` inside). `ConfigNode::start` builds
  its own `info_span!("node", node_id, cluster_id, recovery_epoch)` as a **child of the current
  span**, so calling `start` inside the test span is what attributes every later Raft line.
- `serve_client_plane` / `serve_peer_plane` capture `tracing::Span::current()` at call time and
  re-parent each per-connection RPC span into it. Therefore the harness must call them while a
  per-node span is entered, or RPC lines carry no `node_id`.
- `GrpcPeerTransport::send` reads `(from, to)` out of `PeerEnvelopeMeta`, so **one** transport
  instance per cluster is correct; `NetFault` blocks by node pair, not by channel.
- `NetFault::guard` refuses blocked pairs before dialing and cancels in-flight calls
  (`wait_blocked` + `select!`), which is TA-5.3.
- `EphemeralStore::new(identity, limits, Arc<dyn FaultInjector>, span)`.
- `GossipNode` itself implements `config_core::GossipObservationSource`.
- `FormationPlan.voters` endpoints become `BasicNode::addr` — they are the dialed address and
  the `NotLeader` hint. So all peer listeners must be bound before the plan is built.
- `config_log::retcd_test` forwards attribute args to `#[tokio::test]`, so
  `#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]` works.
- The engine logs role/leader/term transitions as `"raft role changed"` /
  `"raft leader changed"` / `"raft term changed"` with `old`/`new` fields. There is no
  `leader_elected` / `became_leader` message, so plan §5 Q4 is asserted against those names.

## Decisions (deviations to report)

1. `node(id)` returns `ConfigNode` (a clone) rather than `&ConfigNode`: the harness must be able
   to replace a node on `start_node`, which needs interior mutability, and a reference cannot
   escape the lock. `ConfigNode` is an `Arc` handle so this is not a copy.
2. `GossipKind` keeps the plan's name and adds `Static(Vec<ObservedPeerHint>)` (the brief's
   `GossipMode::Static`). `Poisoned(PoisonSpec)` is sugar that seeds the injected-hint list.
3. `GossipKind::Partitioned` is implemented as "the observation source stops reporting", not as
   a blocked gossip socket: there is no `NetFault` seam on the gossip transport in M1.
4. `partition` keeps the plan's `(a, b)` pair form; `partition_sets(&[..], &[..])` is the
   brief's set form.
5. Waits return `Result<_, poll::Timeout>` whose `last_diagnostic` is every node's
   `NodeMetrics` Debug (anti-flake rule 2); `leader()` is the panicking convenience wrapper.
6. `start_node` rebinds the previous `SocketAddr` (not a literal port) so committed membership
   stays valid; the rebind is polled because Windows can hold the port briefly.
7. `StorageKind::Rocks` exists (plan) and panics with an explicit M2 message.

## Timing risks (Windows)

- Default timers 250/750-1500 ms mean an election after a partition costs up to ~1.5 s. The
  M1-16 matrix runs six arrangements, so it uses faster timers (100/300-600) to stay inside the
  10 s budget.
- Real gRPC adds a connect per peer pair on first contact; formation is polled, never slept on.

## Rulings received (2026-09-18)

1. **openraft `loosen-follower-log-revert`** — lead ACCEPTED the workspace `Cargo.toml` feature
   addition. Without it, M1-09 (stopped voter restarts with a *fresh* `EphemeralStore`) panics the
   leader core at `openraft-0.9.25/src/progress/entry/mod.rs:165`:
   `"follower log reversion is not allowed without --features loosen-follower-log-revert;
   matching: T1-N1-1; conflict: 1"`. A restart with an empty store *is* a log reversion from the
   leader's point of view, so this is inherent to the M1 "rejoin as an empty voter" semantics.
   Risk recorded in ADR-0008 Note 2026-09-18. M2 (reopen the same store) will not need it.

2. **Leader-hint endpoint defect** — `ConfigNode::hint_for` builds `LeaderHint::endpoint` from
   `committed_membership().endpoint_of(id)`, which holds `FormationPlan::voters` **peer**
   endpoints. `GrpcClient::execute` only follows a hint whose endpoint is in its own configured
   endpoint list (client-plane addresses), so it never follows. Routed by the lead to the engine
   correction round (Raft `Node` becomes `{peer, client}`). Harness keeps
   `grpc_client_at_leader()` as the workaround, `m1_harness_smoke` asserts the *current* (wrong)
   behaviour so the gap is proven, and rows M1-21 / M1-24 are written against the *intended*
   behaviour and marked `#[ignore = "M1-21|M1-24: engine hint endpoint fix pending ..."]` for the
   Tester to un-ignore after the fix.

3. **Pending config-grpc signature change** (coordinator notice): `serve_client_plane` gains a
   4th `cluster_id: ClusterId` and `serve_peer_plane` a 4th
   `identity: PeerIdentity { cluster_id, recovery_epoch, node_id }`. Both calls live in exactly
   one place — `Cluster::start_running()` in `cluster.rs` — so adapting is a two-line change.

## Self-scan (§6 rules 1 and 4) applies to this crate's own tests

`tests/scan.rs::m1_tests_contain_no_fixed_sleeps_or_literal_ports` scans `crates/config-testkit/tests`.
Three things had to change for it to pass honestly:
- `tests/poll.rs:72` `tokio::task::yield_now()` carries `// testkit:allow-sleep` (the async
  predicate *is* the subject of that test).
- `tests/scan.rs` builds its violating fixture at runtime from split tokens
  (`format!("tokio::time::{}(d);", "sleep")`), because writing it literally would be a violation of
  the scanning file itself — and marking it `allow-sleep` would exempt the very thing the test
  claims to catch.
- the assert message no longer spells the forbidden token.

## Switch points for the engine fix round (dev-engine-fix)

The harness was finished against the **pre-fix** engine API. When the additive engine delta lands
(`engine-api-delta.md`), these are the only places that change. Nothing else in the harness cares.

| # | File:line | Today | After the delta |
|---|-----------|-------|-----------------|
| 1 | `crates/config-testkit/src/cluster.rs:1304` | `NodeConfig::new(identity, peer_endpoint)` | also set `node_cfg.client_endpoint = Some(slot.client_addr.to_string())` — the value is already in scope as `client_addr` in `start_running` |
| 2 | `crates/config-testkit/src/cluster.rs:802` `formation_plan()` | voters carry peer endpoints only | carry the client endpoint too; `Cluster::client_endpoint(id)` already returns it |
| 3 | `crates/config-testkit/src/cluster.rs:988` `grpc_client_at_leader()` | workaround: resolve the leader, then pin | becomes redundant. Keep it as a thin convenience (some rows want a *pinned* leader client) but stop using it to route around the hint |
| 4 | `crates/config-testkit/tests/m1_harness_smoke.rs:90-91` | asserts `hint.endpoint == cluster.peer_endpoint(leader)` — deliberately pins the defect | flip to `cluster.client_endpoint(leader)`, and assert the *new* peer-endpoint field equals `cluster.peer_endpoint(leader)` |
| 5 | `crates/config-testkit/tests/m1_cluster.rs:833, 866` | `#[ignore = "M1-21/M1-24: engine hint endpoint fix pending"]` | delete both `#[ignore]` lines; the bodies are already written against the intended behaviour |
| 6 | `crates/config-testkit/src/cluster.rs:1312` `AuthzKind` match | `AllowAll` / `StaticAllowlist` | add arms for the new `Missing` / `Invalid` kinds |
| 7 | `crates/config-testkit/src/cluster.rs:557, 930` | `StorageKind::Rocks` panics "lands in M2"; `store(id) -> EphemeralStore` | build a `RocksStore` via `StorageHandle::Rocks`; `store()` must become an enum or return `StorageHandle` |
| 8 | root `Cargo.toml:35` | `openraft` features include `loosen-follower-log-revert` | moves to `[dev-dependencies]` of config-engine and config-testkit; dev-engine-fix owns that one-line addition to `crates/config-testkit/Cargo.toml` |

## Switch points 1-7: DONE (2026-09-18)

| # | Done |
|---|------|
| 1 | `start_running` sets `node_cfg.client_endpoint = Some(<bound client addr>)` |
| 2 | `formation_plan()` uses `FormationPlan::with_client_endpoints`, so both planes are committed together |
| 3 | `grpc_client_at_leader()` kept as a convenience (a pinned-to-the-leader client), no longer a workaround |
| 4 | `m1_harness_smoke` now asserts `hint.endpoint == client_endpoint(leader)`, `membership.endpoint_of(leader) == peer_endpoint(leader)`, `hint.endpoint != peer_endpoint(leader)`, and that a follower-pinned `GrpcClient` *follows* the hint |
| 5 | M1-21 / M1-24 un-ignored; both pass |
| 6 | `AuthzKind` gained `Static(String)` (M3, rejected at `start_with` with a message), `Missing`, `Invalid`; the last two map to `config_engine::AuthzKind::{Missing,Invalid}` plus an empty allowlist |
| 7 | `StorageKind::Rocks(RocksSpec)` implemented (see below) |
| 8 | `loosen-follower-log-revert` moved to `[dev-dependencies]` by dev-engine-fix; the workspace dep no longer carries it |

### Formation race (delta §4.1) — audited

`wait_formed` now polls **committed** membership (`membership_of(id).voters == expected` and an
identical `membership_log_id` on every running node) instead of `NodeMetrics::membership_voter_ids`,
which is the *effective* set and moves one apply earlier. Every M1 row that reads `membership_of`
runs straight after this wait, so the old version was a live race.

Other waits audited and left alone: `wait_applied*` and `wait_revision*` read the state machine via
metrics; `wait_converged` compares `state_hash` **and** `last_applied`; `wait_for_leader` reads a
role. None of them touch effective membership.

## M2 harness API (new this round)

```rust
// storage
pub enum StorageKind { Ephemeral, Rocks(RocksSpec) }
impl StorageKind { pub const ROCKS: Self; pub const fn is_persistent(self) -> bool; }
pub struct RocksSpec { pub sync_writes: bool }
impl RocksSpec { pub const DEFAULT: Self; pub const NO_SYNC: Self; }

// lifecycle
pub async fn restart(&self, id: NodeId) -> Result<(), StorageOpenError>;
pub async fn try_start_node(&self, id: NodeId) -> Result<(), StorageOpenError>;
pub async fn stop_all(&self);
pub async fn start_all(&self) -> Result<(), StorageOpenError>;

// storage inspection
pub fn store(&self, id: NodeId) -> StorageHandle;          // was -> EphemeralStore
pub fn ephemeral_store(&self, id: NodeId) -> EphemeralStore;
pub fn rocks_store(&self, id: NodeId) -> RocksStore;
pub fn reopen_store(&self, id: NodeId) -> Result<RocksStore, StorageOpenError>;
pub fn data_dir(&self, id: NodeId) -> PathBuf;             // plan says &Path; see deviations
pub fn injector(&self, id: NodeId) -> Arc<dyn FaultInjector>;
pub fn counters(&self, id: NodeId) -> Arc<FaultCounters>;
pub fn durability(&self, id: NodeId) -> Durability;
pub fn state_hash(&self, id: NodeId) -> [u8; 32];
pub async fn health(&self, id: NodeId) -> HealthPayload;
pub fn assert_crash_invariants(&self, id: NodeId);

// waits added because the M2 restart path needs them
pub async fn wait_rejoined(&self, id: NodeId, deadline: Duration) -> Result<(), Timeout>;
pub async fn wait_revision_all(&self, revision: u64, deadline: Duration) -> Result<(), Timeout>;
pub async fn wait_revision_on(&self, ids: &[NodeId], revision: u64, deadline: Duration)
    -> Result<(), Timeout>;
```

### Two traps the M2 rows will hit (both found by writing the smoke tests)

1. **`wait_applied*` takes a Raft log index, not a revision.** A log carries blank and membership
   entries that allocate no revision, so a log index always runs ahead of the revision a client
   was given. Passing `written.revision` to `wait_applied_all` is a silently *weak* wait that
   returns immediately — the first draft of the restart smoke test did exactly that and passed by
   luck, then failed under parallel load. Use `wait_revision_all` for a number a client saw.
2. **A persistent node comes back with its state machine already loaded.** `state_hash`,
   `cluster_revision` and `applied_commands` are correct *before* the Raft core has published any
   metrics, so every applied-state wait succeeds instantly after a `restart` and proves nothing
   about the node being back in the cluster. `wait_rejoined(id, ..)` is the wait that does, and
   `assert_crash_invariants` refuses to run before it with a message saying so.

### Deviations from test plan §6 / TA-16

* `data_dir(id)` returns `PathBuf`, not `&Path`: slots live behind a `Mutex`, so a borrow cannot
  outlive the guard.
* `RocksSpec` has no `dir` and no `injector` field. The harness owns the directory (one `TempDir`
  per cluster, `node-<id>` inside it) so a restart is guaranteed to reuse the same path and
  `shutdown` can still delete it on Windows; the injector is per node and already configured via
  `ClusterBuilder::faults(id, injector)`.
* `reopen_store(id)` returns the `RocksStore` rather than `()`. The plan's inspection case needs
  the handle, and returning it makes the lock contract visible: the caller must drop it before the
  node starts again. It is synchronous because `RocksStore::open` is.
* `injector(id)` returns `Arc<dyn FaultInjector>`, not `Arc<BoundaryCounter>` — `config-storage`
  implements the counting side as `FaultCounters`, reachable through `counters(id)`. There is no
  `BoundaryCounter` type in the codebase; `crash_on_nth`/`fail_on_nth` do not exist yet and will
  need a `config-storage` addition before the §3.3 crash rows can be written.
* `SyncMode` is spelled `RocksSpec { sync_writes: bool }`, matching `config_storage::RocksOptions`.
* `assert_crash_invariants(id)` is per node, not per cluster, and asserts the subset provable from
  the current APIs: log ahead of applied state, log length consistent with the last index, store
  not poisoned, core running, and revision/command counts not exceeding applied entries. Vote
  non-regression and full log-hole detection need a store-level API that does not exist yet.

## M3 harness API

Round 3 (M3 wiring, Step 1). Everything below is in `crates/config-testkit/src/cluster.rs`
unless stated; `tests/m3_harness_smoke.rs` is new.

### Transport security

```rust
pub enum ClusterTls { Insecure /* default */, MutualTls(Arc<TlsFixture>) }
impl ClusterTls {
    pub fn mutual(cluster_id: ClusterId, seed: u64) -> Self;
    pub fn fixture(&self) -> Option<&Arc<TlsFixture>>;
    pub fn is_mutual(&self) -> bool;
    pub fn transport_security(&self) -> config_core::TransportSecurity;
}
pub struct ClusterConfig { /* ... */ pub tls: ClusterTls,
                           pub node_cert_overrides: BTreeMap<NodeId, CertOverrides> }
ClusterBuilder::tls(ClusterTls)
ClusterBuilder::mutual_tls(seed)                  // fixture for the cfg's own cluster_id
ClusterBuilder::node_cert_override(id, CertOverrides)
Cluster::fixture(&self) -> &Arc<TlsFixture>       // panics on an insecure cluster
```

`ClusterConfig.tls` changed type (`TlsMode` -> `ClusterTls`). No existing test set it, so
nothing outside this crate had to move.

`start_with` asserts `fixture.cluster_id() == cfg.cluster_id`: a fixture for another cluster
mints SANs no node accepts, and the resulting failure is a 30 s election timeout rather than
anything that names the cause.

### Why one transport per (source, target) pair

`GrpcPeerTransport` carries exactly one `TlsMode`, and `MtlsConfig` carries exactly one
`server_domain`. A single shared transport therefore cannot both present the *dialling*
node's certificate and verify the *dialled* node's DNS SAN. So:

```rust
struct PerTargetTransport { by_target: BTreeMap<NodeId, Arc<GrpcPeerTransport>>,
                            fallback: Arc<GrpcPeerTransport> }
```

built by `fn peer_transport_for(cfg, from, netfault)`. One entry per target, each
`fixture.issue_with(CertProfile::node(from), overrides).mtls_verifying(fixture.peer_domain(to))`,
dispatching on `PeerEnvelopeMeta::to`. All of them share the one cluster `NetFault`, so
`partition` / `isolate` / `heal` are unaffected. `ClusterTls::Insecure` still gets one plain
transport per node (previously one for the whole cluster — same behaviour, more channels).

### mTLS clients

```rust
Cluster::grpc_client_tls(id, principal_name) -> GrpcClient          // pinned
Cluster::grpc_client_multi_tls(principal_name) -> GrpcClient
Cluster::try_grpc_client_multi_tls(principal_name) -> Result<GrpcClient, ClientError>
Cluster::grpc_client_with_cert(id, CertProfile, CertOverrides) -> Result<GrpcClient, ClientError>
Cluster::grpc_client_with_tls(id, TlsMode) -> Result<GrpcClient, ClientError>  // foreign CA
Cluster::grpc_client_plaintext(id) -> Result<GrpcClient, ClientError>          // plaintext -> TLS
```

Client certificates carry **no** `server_domain`, so rustls verifies the node against the
`IP:127.0.0.1` SAN in the endpoint string. That is what makes one client usable across all
endpoints (and therefore hint-followable). `grpc_client_multi` on an mTLS cluster now
presents the `dev` principal, matching `Cluster::client`.

`Ok` from these says only that the *configuration* is usable: tonic connects lazily, so a
rejected handshake surfaces on the first request.

### Authorization

`AuthzKind::Static(String)` is now implemented: the TOML is parsed with `toml` into
`config_core::AllowlistPolicy` and installed as a `StaticAllowlist`. A document that does not
parse becomes engine `AuthzKind::Invalid` with a deny-everything allowlist — fail closed,
which is the §4.4 behaviour under test. `AllowAll`, `StaticAllowlist(policy)`, `Missing` and
`Invalid` are unchanged. The old `start_with` assertion rejecting `Static` is gone.

### Formation over a subset

```rust
Cluster::form_with(&[NodeId]) -> Result<(), FormationError>
Cluster::formation_plan_of(&[NodeId]) -> FormationPlan
Cluster::wait_formed_on(&[NodeId], deadline) -> Result<NodeId, Timeout>
Cluster::leaders_now() -> Vec<NodeId>
```

`wait_formed` now delegates to `wait_formed_on(&self.ids(), _)`; same logic as before.

`leaders_now` exists because `leader_now` returns the *lowest*-id node in `Leader` role. An
isolated leader keeps reporting `Leader` until it learns of a higher term, so a row that
isolates node 1 and polls `leader_now().filter(|l| *l != old)` can never see node 2 — it will
time out with node 1 still Leader. Rows that force an election must use `leaders_now`.

### Smoke rows (`tests/m3_harness_smoke.rs`, 5 tests, ~0.2 s total)

1. `mtls_cluster_forms_and_serves_a_certificate_client` — 3-node mTLS cluster forms (that *is*
   the peer-plane proof: no quorum without successful mutual handshakes), then `svc-a`
   put/gets over the TLS client plane under a `Static` policy.
2. `a_wrong_ca_client_is_refused` — a client from `TlsFixture::other_ca` is refused; the
   cluster's own client still works straight afterwards.
3. `a_static_policy_denies_an_unlisted_principal` — `svc-z` gets `PermissionDenied`, `svc-a`
   is served; the only difference between them is the certificate SAN.
4. `one_bad_cert_node_does_not_stop_the_quorum` — node 3 holds a wrong-cluster certificate and
   is still a voter; nodes 1-2 elect a leader and commit, node 3 never learns of a leader.
5. `a_certificate_without_a_san_uri_is_still_usable` — CN-fallback path: served or refused,
   never a silent grant.

### Observations for the lead

* **A refused handshake is reported as `DeadlineExceededUnknownOutcome`, not `Unavailable`.**
  `GrpcClient` treats a connect failure as retriable and spends the whole request budget on
  it. The smoke row accepts either shape, because which one a §4.2 row must assert is the
  client crate's contract, not the harness's. Worth a ruling before the §4.2 rows are written.
* **`GrpcPeerTransport` cannot vary `server_domain` per endpoint.** Worked around inside the
  testkit with `PerTargetTransport`; `config-server` will hit the same wall the moment one
  process dials more than one peer.
* **`config_grpc::peer_plane::PeerIdentity` is still not re-exported** from the crate root.
* **`ConfigNode::start` failure is not surfaceable.** `start_running` ends in
  `.expect("node start")`, so a replay that crashes panics the harness instead of returning.
  M2-17 needs this; fixing it means a `NodeStartError` and a signature change to
  `try_start_node` / `restart` / `start_all`, which has to be coordinated with tester-m2.

## R4 — node start failures are returned, not panicked

`crates/config-testkit/src/cluster.rs`, exported from `lib.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum NodeStartError {
    #[error("store did not open: {0}")]
    Storage(#[from] config_storage::StorageOpenError),
    #[error("engine did not start: {0}")]
    Engine(#[from] config_engine::EngineError),
}

Cluster::try_start_node(id) -> Result<(), NodeStartError>   // was StorageOpenError
Cluster::restart(id)        -> Result<(), NodeStartError>   // was StorageOpenError
Cluster::start_all()        -> Result<(), NodeStartError>   // was StorageOpenError
Cluster::start_node(id)                                      // unchanged: still panics
```

`start_running` propagates `ConfigNode::start` with `?` instead of `.expect("node start")`.
`start_with` (the very first start) still `.expect`s — a fresh directory that will not open is
a harness bug, not a row's subject.

**Which variant a crashed replay is:** `Engine`. OpenRaft surfaces an injected apply-boundary
crash as `EngineError::Raft("when Write StateMachine: injected storage crash at
before_state_batch; storage is poisoned")`. The store itself opened fine, so `Storage` would
have been a lie. Observed verbatim from m2_17 after the change:

```
Engine(Raft("when Write StateMachine: injected storage crash at before_state_batch; storage is poisoned"))
```

`thiserror = { workspace = true }` added to `crates/config-testkit/Cargo.toml`
`[dependencies]` (it was only a transitive dep before).

New smoke row `an_engine_start_failure_is_returned_not_panicked` in `m2_harness_smoke.rs`
(now 8 tests) with a file-local `CrashOnce` injector — deliberately not `support::ScriptedInjector`,
so a harness proof never depends on a row file.

Note the shape it needed: a replay only crosses `BeforeStateBatch` if the log is *ahead* of the
state machine. One armed crash during normal operation creates that gap; arming again before the
restart is what makes the replay itself crash.

## Item 4 — after dev-grpc-2 landed

**`PerTargetTransport` is gone.** `GrpcPeerTransport` now keys its channel cache by
`(endpoint, meta.to)` and pins each dial to `peer_server_domain(meta.cluster_id, meta.to)`
under mTLS, so the library does the per-target DNS-SAN verification the map existed for.
`peer_transport_for` is back to one `GrpcPeerTransport` per node; what still varies per node is
the certificate *presented*, which is why it is not one for the whole cluster. The fixture's
`server_domain` is a no-op on the peer plane, so the plain `.mtls()` profile goes in. The
shared `NetFault` wiring is unchanged, so `partition`/`isolate`/`heal` behave as before.

**Every mTLS client carries the cluster id.** `try_grpc_client_with_tls` now calls
`.with_cluster_id(self.cfg.cluster_id)` whenever the mode is `MutualTls`. All the mTLS builders
funnel through it, so there is one place. Without it a mutual-TLS client cannot name the node a
hint points at and silently refuses to follow hints — every follower-directed request would end
in `NotLeader`. `grpc_client_plaintext` deliberately does not get the id: it is an insecure
client by construction.

**Refusal shape tightened.** `a_wrong_ca_client_is_refused` now asserts `Unavailable` alone.
The client does an explicit connect under mTLS, so a refused handshake is a connect-phase
error, not a spent request budget.

**New row** `an_mtls_client_follows_a_leader_hint` (m3_harness_smoke, now 6 tests): asserts
`cluster_id() == Some(..)`, pins to a follower, reads the leader's data, and requires
`stats().hint_follows >= 1`.

No harness API changed for the testers — only behaviour: mTLS clients from the harness now
follow hints. `PerTargetTransport` was never public.

### Load note

Under heavy parallel load (several agents building and testing at once) the *whole* M1 suite
degrades into `DeadlineExceededUnknownOutcome` / `NotLeader` on writes that pass cleanly
otherwise. Observed once at 12 concurrent test-binary rounds. Not a defect in any row: the same
files ran green in eight consecutive clean rounds before and after. Worth remembering before
chasing a "flake" that only appears while another agent is compiling.
