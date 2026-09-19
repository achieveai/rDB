# Test Plan — M2 and M3

**Status:** Proposed (Tester Planner deliverable)
**Date:** 2026-09-18
**Scope:** M2 (persistence and restart correctness) and M3 (safe remote use baseline — the first
release gate), including the process-level E2E daemon suite.
**Authority:** `docs/DesignSpec-01.md` §4.2, §4.3, §9, §10, §12, §13, §15, §16, §17, §18, §19,
§20, §21 (M2 and M3) and `docs/ADRs/0003, 0004, 0005, 0006, 0007, 0008, 0009, 0010, 0011, 0012,
0013, 0014, 0015, 0016, 0017`.
**Companion:** `docs/testing/test-plan-m0-m1.md`. This document **extends** it. TA-1..TA-12,
the `Cluster` harness API (§4.1 there), the conformance scenario list (§4.3 there, C-01..C-15),
and the anti-flake rules (§6 there) are all still in force and are not restated. New
test-architecture requirements continue at **TA-13**. New DuckDB queries continue at **Q7**.
New open questions continue at **OQ-11**.
Where this plan and the spec/ADRs disagree, the spec/ADRs win and this plan is a defect — except
for the items listed in §11, which are places where the spec/ADRs disagree with *each other*.

**How to use this document**

- Developers: §1 and §6 are contracts on the production code and the harness. Code that does not
  expose these seams is not done, because §3/§4/§5 cannot be written against it.
- Testers: §3, §4 and §5 are the backlog. One row = one test. The row ID must prefix the test
  name (`m2_10_crash_after_commit_before_apply_replays`), because §7 queries and §9's gate
  mapping both work by string match.
- Both: §10 lists the open questions. Each has a **default** recommendation; the default is what
  you implement if the Architect does not answer before you need it. Record the answer in the
  owning ADR.

**File mapping (ADR-0014 §6 — gates map 1:1 to §21 bullets)**

| Area | Path |
|---|---|
| M2 store-level unit/fault tests | `crates/config-storage/tests/m2_store_*.rs` |
| M2 cluster gates | `tests/m2_*.rs` (workspace-level test crate) |
| M3 cluster gates | `tests/m3_*.rs` |
| Process-level E2E | `crates/config-server/tests/e2e_daemon.rs` (**not** workspace `tests/` — see TA-25) |
| Harness | `crates/config-testkit/src/{cluster.rs, netfault.rs, conformance.rs, certs.rs, manifest.rs, daemon.rs, faults.rs, logq.rs}` |
| Test logs | `target/test-logs/<testModule>/<testMethod>.jsonl` |
| Daemon logs (E2E) | `<tempdir>/node<N>/logs/<testModule>/<testMethod>.jsonl` via `RETCD_TEST_LOG_DIR` |

---

## 1. Test-architecture requirements (TA-13 …)

Requirements on the **production code and harness**, not on the tests. "Must" is normative. A
review may reject a PR by number.

### TA-13 — Eight storage boundaries, with distinguishable append and flush

ADR-0008 lists six fault boundaries. §20 ("crash injection before/after vote sync, log append,
**log flush**, and state batch") and §21 M2 require eight. Extend the TA-4 enum:

```rust
pub enum Boundary {
    BeforeVoteSync, AfterVoteSync,
    BeforeLogAppend, AfterLogAppend,
    BeforeLogFlush,  AfterLogFlush,
    BeforeStateBatch, AfterStateBatch,
}
pub enum FaultAction { Proceed, Fail(StorageErrorKind), Crash }
```

Consequences for `RocksStore`:

1. `append` must be **write-then-explicit-sync**: `db.write_opt(batch, WriteOptions::default())`
   (no sync) → `BeforeLogFlush` → `db.flush_wal(true)` → `AfterLogFlush` →
   `callback.log_io_completed(Ok(()))`. A single `set_sync(true)` write collapses
   `AfterLogAppend`/`BeforeLogFlush`/`AfterLogFlush` into one instant and makes M2-19..M2-22
   untestable. ADR-0008's "one WriteBatch with sync" is satisfied by this sequence; the batch is
   still atomic and still synced before the callback.
2. `save_vote` keeps `set_sync(true)`; its two boundaries bracket that one call.
3. `apply` keeps **one** `WriteBatch` with `set_sync(true)`; `BeforeStateBatch` and
   `AfterStateBatch` bracket that one call. There must be no other boundary inside it — M2-16
   asserts exactly that by counting crossings.
4. The hook is consulted on **every** crossing (so an injector can fire on the *k*-th only) and
   the default injector is a zero-cost no-op.

### TA-14 — `Crash` poisons the store; it does not unwind through a flushing `Drop`

`FaultAction::Crash` must:

1. return a `StorageError` to OpenRaft (which makes the node `Fatal`), **and**
2. set a poison flag so every subsequent call on that store instance returns
   `StorageErrorKind::Poisoned` **without touching the database**, **and**
3. cause `Drop` for the poisoned store to close RocksDB **without** `flush_wal`, `flush()`,
   `cancel_all_background_work(true)`, or any other write — otherwise "crash" is fiction and
   the test proves nothing.

The harness must then call `cluster.reopen_store(id)` (TA-16) before the node can be restarted.
`BeforeX` crashes before the effect; `AfterX` crashes after the effect but before the caller is
told. A test asserting "the effect did/did not land" must use a boundary whose side is defined
by this rule, not by hope.

### TA-15 — Injector counters are part of the harness

```rust
pub struct BoundaryCounter { /* Arc<[AtomicU64; 8]> */ }
impl BoundaryCounter {
    pub fn new() -> Self;
    pub fn count(&self, b: Boundary) -> u64;
    pub fn snapshot(&self) -> BoundaryCounts;              // PartialEq + Debug
    pub fn crash_on_nth(&self, b: Boundary, n: u64);       // arm a one-shot crash
    pub fn fail_on_nth(&self, b: Boundary, n: u64, kind: StorageErrorKind);
}
impl FaultInjector for BoundaryCounter { .. }
```

Counts are the **fsync accounting oracle** (M2-45, M2-46): one `AfterVoteSync` crossing = one
vote fsync; one `AfterLogFlush` crossing = one log WAL sync; one `AfterStateBatch` crossing =
one state-machine sync. No separate metric is needed and none may be trusted instead.

### TA-16 — Rocks storage in the harness, and explicit reopen

```rust
pub enum StorageKind {
    Ephemeral,
    Rocks(RocksSpec),                 // dir owned by the harness, under a per-test TempDir
}
pub struct RocksSpec {
    pub dir: Option<PathBuf>,         // None => fresh TempDir; Some => reuse (restart tests)
    pub injector: Option<Arc<dyn FaultInjector>>,
    pub sync_mode: SyncMode,          // Full (default) | NoSync (only for OQ-15 / M2-44)
}

impl Cluster {
    pub async fn restart(&self, id: NodeId) -> Result<(), StartError>;   // stop + reopen + start_node
    pub async fn reopen_store(&self, id: NodeId) -> Result<(), StorageOpenError>;
    pub fn data_dir(&self, id: NodeId) -> &Path;
    pub fn injector(&self, id: NodeId) -> Arc<BoundaryCounter>;
    pub fn state_hash(&self, id: NodeId) -> [u8; 32];   // node-local, no Raft barrier
    pub async fn stop_all(&self) -> ();
    pub async fn start_all(&self) -> ();                // cold cluster restart from disk
}
```

Requirements:

1. `Cluster::restart(id)` must fully drop the old `ConfigNode` **and** its `RocksStore` before
   reopening. RocksDB holds an exclusive `LOCK` file; a partial drop turns a restart test into a
   `IO error: lock hold by current process` flake. `restart` must await store closure, not hope.
2. `state_hash(id)` reads the local state machine directly, with no leader check, because the
   whole point is to compare a follower with the leader (TA-2 makes this one assertion).
3. Data dirs live under the test's `TempDir` (TA-11). On Windows a `TempDir` whose RocksDB is
   still open cannot be deleted; `Cluster::shutdown` must close all stores before dropping dirs,
   including on panic.
4. `SyncMode::NoSync` exists only to test the capability downgrade (M2-44) and must be
   unreachable from `config-server` without `--unsafe-no-sync` (OQ-15).

### TA-17 — Health payload is the cross-process state oracle

The E2E suite cannot call `state_hash()` in-process. The health payload (§18.1, ADR-0016) must
therefore carry, as a stable serialized shape:

```rust
pub struct HealthPayload {
    pub health: Health,                       // Ready | NotLeader | Unavailable | Fatal
    pub identity: ClusterIdentity,            // cluster_id, recovery_epoch, node_id
    pub role: Role, pub current_term: u64, pub current_leader: Option<NodeId>,
    pub last_log_index: u64, pub last_applied: u64,
    pub cluster_revision: u64,
    pub state_hash: String,                   // 64 lowercase hex, TA-2 digest
    pub membership_voter_ids: Vec<NodeId>, pub membership_log_id: Option<LogIdView>,
    pub capabilities: Capabilities,
    pub policy: PolicySummary,                // authz kind + grant count + policy file hash
}
```

`state_hash` in the health payload is what makes E2E-05/E2E-06/E2E-12 one assertion instead of a
guessing game. It is a digest, not data, so it leaks nothing (§15.2). Health must be reachable
without client-plane authorization credentials on a separate local surface (OQ-16).

### TA-18 — `TlsFixture`: deterministic, in-memory certificate generation

```rust
pub struct TlsFixture { /* CA key+cert, issued leaves */ }
pub enum CertProfile { Peer { node_id: NodeId }, Client { name: String }, Admin }

impl TlsFixture {
    pub fn new(cluster_id: ClusterId, seed: u64) -> Self;      // deterministic key material
    pub fn ca_pem(&self) -> String;
    pub fn issue(&self, p: CertProfile) -> CertPair;           // SAN URI per ADR-0011/0012
    pub fn issue_with(&self, p: CertProfile, o: CertOverrides) -> CertPair;
    pub fn other_ca(seed: u64) -> TlsFixture;                  // an unrelated, valid CA
}
pub struct CertOverrides {
    pub cluster_id: Option<ClusterId>,     // wrong-cluster variant
    pub node_id: Option<NodeId>,           // wrong-node variant
    pub not_after: Option<OffsetDateTime>, // expired variant
    pub san_uri: Option<String>,           // malformed / missing-SAN variant
    pub omit_san: bool,                    // CN-only, for the CN-fallback row
    pub self_signed: bool,                 // not chained to the fixture CA
}
```

Requirements:

1. `rcgen` generates everything in memory; certs are written to files only for the E2E daemon
   (into that test's `TempDir`). No fixture certificate is ever committed to the repo, and none
   has a lifetime beyond the test process except the deliberately-expired ones.
2. SAN URIs are exactly ADR-0011/ADR-0012: peer `retcd://<cluster_id>/node/<node_id>`, client
   `retcd://<cluster_id>/client/<name>`. `<cluster_id>` is the 32-hex `Display` form of
   `ClusterId`.
3. `seed` makes key generation reproducible so a failure can be replayed; the seed is printed in
   every TLS-row failure message (anti-flake rule 7).
4. Expired certificates are generated with `not_after` in the past; the test must not sleep to
   wait for expiry.

### TA-19 — `ManifestFixture`: Ed25519 bootstrap manifests

```rust
pub struct ManifestFixture { signing_key: SigningKey, pub key_id: String }
impl ManifestFixture {
    pub fn new(seed: u64) -> Self;
    pub fn write(&self, dir: &Path, m: &Manifest) -> ManifestPaths;   // manifest.toml + manifest.sig
    pub fn write_tampered(&self, dir: &Path, m: &Manifest, t: Tamper) -> ManifestPaths;
}
pub enum Tamper { FlipByteInToml, FlipByteInSig, WrongSigningKey, Expired, WrongClusterId,
                  WrongNodeEndpoint, TruncatedSig, MissingSigFile }
```

`config-server` must verify signature **over the exact `manifest.toml` bytes** before parsing
any field it will act on, and must reject expiry using the manifest's own `expires_at` against
the system clock (the only place in the system where a clock is allowed — the state machine is
still clock-free, TA-9).

### TA-20 — `DaemonProcess`: spawning the real binary

```rust
pub struct DaemonSpec {
    pub node_id: NodeId, pub dir: PathBuf,
    pub peers: Vec<(NodeId, String)>,          // from the manifest
    pub tls: DaemonTls,                        // ca, peer pair, client-plane pair
    pub allowlist: Option<String>,             // TOML text; None => no policy file
    pub extra_args: Vec<String>,
}
pub struct DaemonProcess { /* Child, Ports, paths */ }
impl DaemonProcess {
    pub fn spawn(spec: DaemonSpec) -> io::Result<DaemonProcess>;
    pub async fn wait_ready(&self, deadline: Duration) -> Result<Ports, Timeout>;
    pub fn ports(&self) -> Ports;              // peer, client, gossip — actual bound ports
    pub fn log_path(&self) -> &Path;
    pub fn kill(&mut self) -> io::Result<()>;                       // hard: Child::kill
    pub async fn shutdown_graceful(&mut self, d: Duration) -> io::Result<ExitStatus>;
    pub async fn wait(&mut self, d: Duration) -> io::Result<ExitStatus>;
}
```

Requirements:

1. The binary path comes from `env!("CARGO_BIN_EXE_config-server")` — never from a hand-built
   `target/debug/...` path, which breaks under `--release` and custom `CARGO_TARGET_DIR`.
2. Ports are ephemeral. The daemon binds `127.0.0.1:0` when the config says port `0` and then
   prints **one machine-readable ready line** on stdout, e.g.
   `{"ready":true,"node_id":1,"peer":"127.0.0.1:51234","client":"127.0.0.1:51235","gossip":"127.0.0.1:51236"}`.
   `wait_ready` parses that line. Anything else (scraping logs, sleeping, pre-allocating a
   listener and closing it) is a race and is rejected in review.
3. Each daemon gets `RETCD_TEST_LOG_DIR=<dir>/logs` and the same `testModule`/`testMethod`
   values as the spawning test (passed as `--log-field testModule=… --log-field testMethod=…`,
   OQ-18) so §7's cross-process DuckDB joins work.
4. `Drop` for `DaemonProcess` must kill any still-running child, including when the test panics.
   An orphaned `config-server` holding a RocksDB lock breaks every later test in the binary.

### TA-21 — `config-server` command-line and lifecycle surface

The E2E suite depends on these existing and being stable:

| Flag / surface | Required behavior |
|---|---|
| `--config <file>` | Whole node config; no env-var-only settings (TA-11) |
| `--form` | Perform `form_cluster` once, then serve. Idempotent-safe: a second run on a formed store errors and exits non-zero |
| `--capabilities` | Print `Capabilities` as JSON and exit 0, without opening listeners |
| `--allow-insecure-dev` | The **only** way `TlsMode::Insecure` is accepted (ADR-0010). Without it, exit non-zero with a typed error |
| `--dev-allow-all` | The **only** way `AllowAll` authz is accepted (ADR-0012). Without it and without a policy file, the node is unready |
| `--unsafe-no-sync` | Enables `SyncMode::NoSync`; forces `durability = PersistentUnverified` (OQ-15) |
| ready line | One JSON line on stdout with actual bound ports (TA-20.2) |
| graceful shutdown | Cross-platform trigger (OQ-17). Exit code 0, stores closed, final log line `msg="shutdown_complete"` |
| exit codes | `0` clean; `2` config/identity/manifest rejection; `3` fatal storage. Distinct, asserted |

### TA-22 — The authorization seam is transport-independent and observable

`Authorizer::authorize(&Principal, Action, &[u8]) -> Decision` (ADR-0012) must be called on
exactly one path shared by `DirectClient` and the gRPC service, **before** `client_write` /
`ensure_linearizable`. Every call emits one JSONL line with fields `principal`, `action`,
`key_hex`, `decision` (`allow`/`deny`), `policy_kind`, `grant_prefix_hex` (on allow). M3-26,
M3-27 and Q9 assert on those fields. A denied mutation must not reach the log (assert
`raft_log_len` unchanged, exactly like M1-27).

### TA-23 — The client counts its own sends

`GrpcClient` must expose `ClientStats { sends: u64, hint_follows: u64, reconnects: u64 }`
(per-client, monotonic). ADR-0015 says "the client library never re-sends the mutation on its
own"; M3-41 proves it by asserting `sends == 1` for a mutation that timed out. A test that only
checks the final key state cannot distinguish "did not retry" from "retried and the second
attempt conflicted".

### TA-24 — The server counts applies per command

`NodeMetrics` must carry `applied_commands: u64` (count of `EntryPayload::Normal` entries
applied, excluding `Blank` and `Membership`). M3-40 asserts exactly one apply for the
unknown-outcome mutation. `cluster_revision` alone is insufficient: a rejected duplicate would
allocate no revision and look identical to no duplicate at all.

### TA-25 — E2E test crate placement

`CARGO_BIN_EXE_<name>` is set by Cargo only for integration tests **of the package that declares
that binary**. ADR-0014 says `tests/e2e_daemon.rs`; if that is the workspace root test crate the
variable does not exist. The E2E suite must live at `crates/config-server/tests/e2e_daemon.rs`.
A workspace-root `tests/e2e_daemon.rs` that shells out to a guessed path is rejected.

### TA-26 — Windows process control

Development and gating host is Windows Server 2022 (ADR-0017).

1. Hard kill is `std::process::Child::kill()` (`TerminateProcess`) — the `kill -9` equivalent.
   No handler runs, nothing is flushed. That is the intended semantics for E2E-04.
2. There is no `SIGTERM`. Graceful shutdown must not depend on one (OQ-17).
3. `Child::kill()` does not kill grandchildren. `config-server` must not spawn child processes.
4. After `kill()`, the OS releases the RocksDB `LOCK` file, but the test must still
   `wait()` on the child before reopening the directory from another process, or the reopen
   races on Windows file handles.
5. Tokio timer slop on Windows is ~15 ms (research §9). All deadlines in M2/M3 are expressed as
   multiples of the harness election timeout (anti-flake rule 3), never as literals.

### TA-27 — `Capabilities` gains `PersistentUnverified`

ADR-0016 declares `durability: Ephemeral|Persistent` but ADR-0008/§21 M2 and the M0-M1 plan both
refer to `PersistentUnverified`. The enum must be
`Durability { Ephemeral, PersistentUnverified, Persistent }` (§11 item 4). `RocksStore` reports
`PersistentUnverified` when opened with sync disabled or when identity verification was skipped;
`Persistent` otherwise. `EphemeralStore` can produce neither (M1-39).

---

## 2. Taxonomy and budgets (extends §2 of the M0-M1 plan)

| Layer | Runner | Fault tools | Per-test budget |
|---|---|---|---|
| M2 store-level | `cargo test -p config-storage` | `BoundaryCounter`, TempDir | < 5 s |
| M2 cluster restart/crash | `tests/m2_*.rs` | `BoundaryCounter`, `NetFault` | < 20 s |
| M3 TLS / authz cluster | `tests/m3_*.rs` | `TlsFixture`, `NetFault` | < 20 s |
| M3 conformance ×2 clients (mTLS) | `tests/m3_conformance.rs` | none | < 30 s total |
| E2E daemon | `crates/config-server/tests/e2e_daemon.rs` | process kill, `TlsFixture`, `ManifestFixture` | < 60 s per test |

Hard ceilings: **no in-process test may exceed 30 s**; **no E2E test may exceed 90 s**. The whole
M0+M1+M2+M3 suite must finish in under **20 minutes** on the dev host including the first
RocksDB build (which is separately budgeted — ADR-0017 warns it takes minutes). A test that needs
longer is sleeping, and anti-flake rule 1 bans that.

Rocks tests are I/O-bound; they must still pass under default `cargo test` parallelism. If they
do not, the fix is per-test temp dirs and smaller data sets, never `--test-threads=1` in CI.

---

## 3. M2 — persistence and restart correctness

Storage is `StorageKind::Rocks` throughout unless a row says otherwise. Every row that restarts a
node uses `Cluster::restart` / `reopen_store` (TA-16), never a hand-rolled stop/start.

### 3.1 Ordinary restart preserves every acknowledged mutation (§21 M2 line 1)

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M2-01 | restart_follower_preserves_state | `start(3, Rocks)`; 20 puts on distinct keys, all `APPLIED`, revisions 1..20; `wait_applied_all` | `restart(f)` for a follower `f` | `f` comes back with `last_applied` ≥ pre-stop value, `cluster_revision == 20`, `state_hash(f)` equals the leader's | `state_hash` equality; Q7 shows `msg="store_opened"` with `identity` and `last_applied` before any Raft line |
| M2-02 | restart_leader_preserves_state | same | `restart(L)` | a new leader elects among the survivors; after `L` rejoins, `state_hash(L) == state_hash(new_leader)`; `cluster_revision == 20` on all three | as above |
| M2-03 | restart_each_node_in_turn | same | table-driven: for `i in 1..=3` → `restart(i)`, `wait_applied_all(20)` | after every iteration all three `state_hash`es are equal and `cluster_revision == 20`; no revision was reused (revisions observed by a fresh `list` are exactly 1..20) | one `assert_eq!` over a `[hash;3]` array |
| M2-04 | revision_monotonic_across_restart | M2-03 state | after each restart, put 1 more key | revisions continue `21, 22, 23` — strictly increasing, no gap, no reuse; `create_revision`/`mod_revision` of untouched keys unchanged | `list` full dump compared to an expected `Vec<Record>` |
| M2-05 | acknowledged_mutation_survives_immediate_restart | `start(3, Rocks)` | put `k=v`; the instant `APPLIED{r}` returns, `restart` **all three** nodes (`stop_all` then `start_all`) | after the cold start, `get(k)` returns `v` with `mod_revision == r`; `cluster_revision == r` | §21 M2 line 1 in its strictest form: the ack is the promise |
| M2-06 | cold_cluster_restart_does_not_reform | M2-05 state | `stop_all`; `start_all` with `form: false` | a leader appears from the persisted membership without any `form_cluster` call; `membership_log_id` identical to before; **no** new membership entry | Q8: zero rows with `msg="formation_started"` after the first |
| M2-07 | data_dir_reuse_ten_cycles | `start(3, Rocks)`, 5 puts | 10 × `restart(node 2)` in a loop, 1 put between each | final `cluster_revision == 15`; `state_hash` equal on all three; RocksDB opens 10 times with no `LOCK` error | counter: `store_opened` lines == 10 for node 2 |
| M2-08 | second_formation_after_restart_rejected | M2-06 state | call `form_cluster` on a restarted node | typed `AlreadyFormed` error; membership and term unchanged; no log entry | ADR-0011 "succeeds only if the local store is fresh" |
| M2-09 | restart_with_stopped_peer | `start(3, Rocks)`, 5 puts | `stop(3)`; 5 more puts (quorum 2); `restart(1)`; then `start_node(3)` | all 10 revisions readable; node 3 catches up to `last_applied` of the leader; all `state_hash`es equal | §9.3.6 replay + catch-up on real storage |
| M2-10 | ephemeral_vs_rocks_divergence_documented | `start(3, Ephemeral)` and `start(3, Rocks)`, same command sequence | restart one node in each | Rocks node retains state; Ephemeral node starts empty and re-replicates (M1-43). Both converge; the test asserts the *documented* difference so nobody mistakes one for the other | one table, two rows |

### 3.2 Committed-but-unapplied replay (§21 M2 line 2; §9.3.6; research §8.2)

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M2-11 | crash_after_commit_before_apply_replays | `start(3, Rocks)`, 5 puts applied | on the leader arm `crash_on_nth(BeforeStateBatch, 1)`; issue `put(k6)`; the entry commits (followers ack) but the leader's apply crashes | leader becomes `Fatal`; `reopen_store(L)`; `restart(L)`; after restart `cluster_revision == 6`, `get(k6)` returns the value, `state_hash(L)` equals both followers | Q7: exactly one `msg="replay_committed"` line with `from`/`to` log indexes; zero `msg="apply"` lines for log index 6 **before** the crash |
| M2-12 | replay_allocates_no_duplicate_revision | M2-11 | after restart, `list` everything | exactly 6 records/revisions; no key has two revisions; `cluster_revision == 6`, not 7 | full `list` dump equality; `applied_commands` (TA-24) == 6 |
| M2-13 | committed_is_persisted | `start(3, Rocks)`, 5 puts | read `raft_meta/"committed"` directly from the store after each put | the key exists and its `LogId.index` is ≥ the last applied index; it advances | store-level test; proves `save_committed` is implemented, not defaulted (research §8.2 — the defaults silently disable replay) |
| M2-14 | read_committed_drives_replay_window | store-level: build a store with `last_applied = 3` and `committed = 7`, logs 1..7 present | construct `Raft::new` over it | `apply` is called with exactly indexes `4..=7`, contiguous, in order, once each | record the apply calls in a test state machine; assert the index vector equals `[4,5,6,7]` |
| M2-15 | replay_chunked_over_64_entries | store-level: `last_applied = 0`, `committed = 200`, logs 1..200 | start | apply receives 200 entries in contiguous chunks (openraft uses 64), no gap, no duplicate, no reorder | index vector equals `1..=200`; chunk boundaries recorded but not asserted |
| M2-16 | apply_batch_is_one_atomic_crossing | `start(3, Rocks)` | snapshot `BoundaryCounts`; issue 1 put; snapshot again | `BeforeStateBatch` and `AfterStateBatch` each increased by exactly 1; no other state-boundary crossing occurred within the apply | proves ADR-0008's "one WriteBatch" claim by counting, not by reading the code |
| M2-17 | crash_during_replay_is_idempotent | M2-11 setup | arm `crash_on_nth(BeforeStateBatch, 1)` **again** so the restart's replay also crashes; then reopen and restart a second time | second restart completes replay; `cluster_revision == 6`; still exactly 6 revisions; `state_hash` equals peers | the replay path must be as idempotent as the apply path |
| M2-18 | follower_committed_unapplied_replay | `start(3, Rocks)` | arm `crash_on_nth(BeforeStateBatch,1)` on a **follower**; 3 puts; crash; reopen; restart | follower replays and converges; leader never blocked (writes kept committing with quorum 2) | `state_hash` equality; leader's `current_term` unchanged |

### 3.3 Crash injection at every boundary (§21 M2 line 3; §20)

One row per boundary. Each row is a table over `role ∈ {leader, follower}` — 16 executions.
Common shape: `start(3, Rocks)`; 5 puts applied; arm `crash_on_nth(B, 1)` on the target node;
drive the workload that crosses `B`; observe the crash; `reopen_store`; `restart`;
`wait_applied_all`. Common assertions for **every** row, asserted by a shared helper
`assert_crash_invariants(&cluster)`:

- no acknowledged mutation is lost (every revision that returned `APPLIED` before the crash is
  still readable, with the same value and `mod_revision`);
- no log hole: `raft_log` CF indexes are contiguous from `last_purged+1` to `last_log_index`;
- no vote regression: the persisted `Vote` after restart is ≥ the one observed before (by
  openraft's `Vote` ordering);
- `last_log_index >= last_applied` (research §8.2 startup trap — a violation makes openraft
  *delete* log entries);
- all three `state_hash`es equal after convergence;
- `cluster_revision` equals the number of `APPLIED` responses observed by the test.

| ID | Name | Boundary | Driver | Boundary-specific expectation |
|---|---|---|---|---|
| M2-19 | crash_before_vote_sync | `BeforeVoteSync` | isolate the leader to force an election on the target | the new vote was **not** persisted; after restart the node holds its old vote; it never granted a vote it could not remember; cluster re-elects |
| M2-20 | crash_after_vote_sync | `AfterVoteSync` | same | the new vote **is** persisted; term after restart ≥ term before; no regression |
| M2-21 | crash_before_log_append | `BeforeLogAppend` | one put | the entry is absent after restart; the client saw an error or `DeadlineExceededUnknownOutcome`, never `APPLIED`; leader re-replicates; no hole |
| M2-22 | crash_after_log_append | `AfterLogAppend` | one put | the entry may or may not be present (written, not synced); if present, indexes are contiguous; the flush callback never fired, so nothing was acknowledged; no acknowledged loss |
| M2-23 | crash_before_log_flush | `BeforeLogFlush` | one put | same as M2-22; explicitly distinct crossing (TA-13.1) |
| M2-24 | crash_after_log_flush | `AfterLogFlush` | one put | the entry **is** durable after restart; the callback never fired so the entry was not counted toward commit by this node; after restart it participates normally; no hole |
| M2-25 | crash_before_state_batch | `BeforeStateBatch` | one put | KV unchanged, `last_applied` unchanged; the entry replays after restart (M2-11 invariants) |
| M2-26 | crash_after_state_batch | `AfterStateBatch` | one put | KV change, revision, `last_applied` and membership are **all** durable; the client saw an unknown outcome; after restart a re-read shows the mutation applied **exactly once** (`applied_commands` +1, `cluster_revision` +1) |
| M2-27 | crash_boundary_table_is_exhaustive | — | — | a compile-time/table assertion that the crash matrix covers `Boundary::ALL` and `ALL.len() == 8`; prevents a silently-dropped boundary when the enum grows |
| M2-28 | repeated_crash_cycles_no_vote_regression | rotating over all 8 | 10 cycles of {arm random-but-seeded boundary, crash, reopen, restart, 1 put} | term and vote never regress across the whole run; final `cluster_revision == 10`; all `state_hash`es equal; the seed is printed on failure | 
| M2-29 | crash_matrix_loses_no_acknowledged_mutation | all 8 × 2 roles | the shared helper above | aggregate assertion over the whole matrix, reported as one table so a single boundary failure is named | 

### 3.4 Log integrity, truncate, purge (§9.3.2, §12, ADR-0008)

Store-level tests (`crates/config-storage/tests/m2_store_log.rs`) — no cluster.

| ID | Name | Action | Expected |
|---|---|---|---|
| M2-30 | append_rejects_index_gap | `append` entries at index `last+2` | typed `StorageError` (not a silent write, not a panic); the log is unchanged; ADR-0008's `index == last_index + 1` assertion is enforced at runtime, not only in debug |
| M2-31 | append_entries_readable_on_return | `append(batch)`; immediately `try_get_log_entries(range)` before the flush callback fires | all entries readable (§9.3.3, openraft "when this method returns, the entries must be readable") |
| M2-32 | flush_callback_only_after_sync | arm `Fail(Io)` on `BeforeLogFlush` | `log_io_completed(Err(..))` is delivered, never `Ok(())`; no code path calls `log_io_completed(Ok)` before `AfterLogFlush` is crossed | counter: `AfterLogFlush` count == count of `Ok` callbacks |
| M2-33 | truncate_removes_suffix_only | logs 1..10; `truncate(log_id@6)` | 1..5 remain, 6..10 gone, `get_log_state().last_log_id == 5`, no hole |
| M2-34 | truncate_then_append_contiguous | after M2-33, append at 6 | accepted; indexes contiguous |
| M2-35 | purge_rejects_above_last_applied | `last_applied = 4`; `purge(log_id@7)` | typed error; nothing removed. Purging an entry the state machine has not applied destroys the replay window (§9.3.6) |
| M2-36 | purge_is_never_invoked | run a 200-mutation cluster workload | the store's `purge` call counter is **0**; `last_purged_log_id` stays `None`; §12's "M0–M3 perform no log purging" is measured, not assumed. This is also what keeps the no-snapshot design safe (research §3.6) |
| M2-37 | snapshot_never_built | same workload | `build_snapshot` call counter is 0; `get_current_snapshot()` returns `Ok(None)` throughout; `SnapshotPolicy::Never` is the configured value | 
| M2-38 | snapshot_methods_are_typed_unsupported | call the snapshot trait methods directly | typed `Unsupported` storage error (or documented `unreachable!` guarded by M2-36/M2-37); never a panic reaching a client |
| M2-39 | log_state_never_under_reports | after 50 appends + a crash at `AfterLogFlush` + reopen | `get_log_state().last_log_id >= applied_state().0`; a deliberately-broken reader (mutation test) must make this row fail | research §8.2 startup trap: openraft deletes log entries when `last_log_id < last_applied` |
| M2-40 | log_indexes_contiguous_after_every_crash | invoked by `assert_crash_invariants` | scan the `raft_log` CF keys (big-endian u64) and assert `k[i+1] == k[i] + 1` | direct CF scan, not a metric |

### 3.5 Identity binding (§21 M2 line 4; §4.2; §19.10; ADR-0011)

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M2-41 | identity_written_on_first_open | fresh dir | open `RocksStore` with `ClusterIdentity{c,e,n}` | `state_meta/identity` contains exactly that triple, serialized; readable after close/reopen | direct CF read |
| M2-42 | wrong_cluster_id_blocks_startup | dir from M2-41 | reopen with a different `cluster_id` | `Err(IdentityMismatch{stored, configured})` from `open()`; **`Raft::new` is never called** | Q7: a `level="error"`, `msg="identity_mismatch"` line carrying `stored_cluster_id`, `configured_cluster_id`, `node_id`; and **zero** lines with `@logger LIKE 'openraft%'` in that test |
| M2-43 | wrong_node_id_blocks_startup | same | reopen with a different `node_id` | same shape | same |
| M2-44 | wrong_recovery_epoch_blocks_startup | same | reopen with `recovery_epoch + 1` | same shape | same |
| M2-45 | cloned_data_dir_rejected | copy node 1's dir; configure it as node 2 | start node 2 on the copy | `IdentityMismatch`; node 2 does not start; §4.2 "cloning a node data directory or identity is forbidden" | same |
| M2-46 | identity_mismatch_is_not_a_panic | all of M2-42..M2-45 | — | in every case the error is the typed `IdentityMismatch`, the process/test does not panic, and `config-server` exits with code 2 (TA-21) | |
| M2-47 | identity_survives_crash_at_every_boundary | crash matrix (§3.3) | after each restart, read `state_meta/identity` | unchanged in all 16 cases; identity is written once and never rewritten | |
| M2-48 | formation_identity_must_match_manifest | fresh cluster, manifest for cluster A | `form_cluster` with a plan for cluster B | typed `IdentityMismatch` before `Raft::initialize`; no log entries anywhere | extends M1-06 to real storage |

### 3.6 Durability capability and fsync accounting (§21 M2 line 5; ADR-0016)

| ID | Name | Action | Expected | Oracle |
|---|---|---|---|---|
| M2-49 | rocks_reports_persistent | `start(3, Rocks)` with default sync | `capabilities()` on every node equals `Capabilities{ durability: Persistent, watch_resumption: Unsupported, authz: Development, transport_security: Insecure, pagination: Unsupported, dedup: Unsupported }` — one struct equality | this row is the M2 gate for §21 M2 line 5 and must be in the same binary as §3.1–§3.5 (OQ-11) |
| M2-50 | ephemeral_never_persistent | `start(3, Ephemeral)` | `durability == Ephemeral`; `EphemeralStore` cannot construct `Persistent`/`PersistentUnverified` | type-level or exhaustive assertion (M1-39 extended) |
| M2-51 | no_sync_downgrades_capability | `Rocks` with `SyncMode::NoSync` | `durability == PersistentUnverified`; a `warn` line `msg="durability_unverified"` with `reason="sync_disabled"` | TA-27 |
| M2-52 | capabilities_identical_on_all_nodes_and_health | `start(3, Rocks)` | `capabilities()` on all three == the `HealthPayload.capabilities` on all three | ADR-0016 |
| M2-53 | fsync_count_per_mutation | `start(3, Rocks)`; snapshot `BoundaryCounts` on the leader; 10 sequential puts; snapshot again | `AfterLogFlush` increased by ≥ 10 (one per append batch; batching may merge, so the assertion is `>= 10` and `<= 10 + blank/membership entries`); `AfterStateBatch` increased by exactly 10 when applies are not batched, and by ≥ 1 and ≤ 10 when they are — the test asserts the *exact* value recorded as a golden and fails loudly if batching behavior changes | the golden is a documented constant so a silent loss of syncing is a test failure, not a performance win |
| M2-54 | vote_fsync_per_term_change | force 3 elections via `isolate`/`heal` | `AfterVoteSync` count on each node equals the number of vote changes it persisted; never 0 while the term advanced | proves §9.3.1 is not optimized away |
| M2-55 | zero_sync_is_impossible_in_default_mode | default `SyncMode::Full` | `AfterLogFlush` and `AfterStateBatch` counts are both > 0 after any mutation | catches a build where `set_sync(true)` was dropped |

### 3.7 Storage-fatal behavior (§9.3.7; §21 M2 scope)

| ID | Name | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|
| M2-56 | io_error_on_append_is_fatal_not_panic | `Fail(Io)` on `BeforeLogAppend` | node health becomes `Fatal`; no panic; all client calls on that node return `ConfigError::FatalStorage` | Q7: one `level="error"`, `msg="storage_fatal"` line with `subject="log"`, `verb="write"` |
| M2-57 | enospc_on_state_batch_is_fatal | `Fail(NoSpace)` on `BeforeStateBatch` | same; the mutation is **not** acknowledged as `APPLIED` | |
| M2-58 | fatal_node_stops_acknowledging | after M2-57 | the other two nodes elect a leader and keep committing; the fatal node acknowledges nothing and is not counted in quorum | §19.12 bounded rejection |
| M2-59 | fatal_node_does_not_continue_optimistically | after M2-57 | subsequent `get`/`put`/`list` on the fatal node all return `FatalStorage`, never a stale success; the node never re-enters `Ready` without a restart | `health()` stays `Fatal` for 10 × election timeout |
| M2-60 | corrupt_log_entry_detected_on_open | store-level: write garbage bytes into a `raft_log` CF value, reopen | typed `StorageError` naming the log index; node does not start; no panic; exit code 3 for the daemon | the daemon clause is E2E-19: a second `config-server` on a held data directory exits 3 with one `msg="startup_failed"`, `reason="storage_open_failed"` line |
| M2-61 | corrupt_state_meta_detected_on_open | store-level: corrupt `state_meta/last_applied` | typed `StorageError`; startup refused | |
| M2-62 | missing_column_family_fails_clearly | create a DB with only `raft_log`, reopen expecting all four CFs | typed `StorageOpenError::MissingColumnFamily{name}` naming the missing CF; **not** a RocksDB string error leaked to the caller, and not a silent auto-create (auto-creating `kv` on a DB that has data elsewhere is silent data loss) | assert the error's `Display` names `kv`, `raft_meta`, `state_meta` as appropriate |
| M2-63 | unknown_extra_column_family_rejected_or_documented | create a DB with an extra `events` CF (the M4 name) and open with the M2 set | per OQ-13: default is **reject with a typed error** naming the unexpected CF, because an `events` CF means the directory belongs to a later schema version (§17 migration rule) | |
| M2-64 | locked_data_dir_fails_clearly | open the same dir twice in one process, and from a second process | typed `StorageOpenError::Locked{path}`; no hang, no 10-minute retry loop | this is the single most common Windows restart-test failure; make the error legible |
| M2-65 | blocking_rocksdb_does_not_starve_raft | inject a 2 s stall on `BeforeStateBatch` (Proceed-after-delay) on a follower | the leader keeps heartbeating, no election occurs, `current_term` unchanged; the stall is absorbed by `spawn_blocking` (ADR-0008) | `current_term` before == after; leader unchanged |
| M2-66 | format_version_stamped_on_first_open | store-level: open a fresh directory, read `state_meta/format_version` raw, then reopen | the marker is exactly `FORMAT_VERSION` as a bare little-endian `u32`, written in the same synced batch as the identity bind; the reopen is an ordinary success and leaves the marker untouched | ADR-0008 note 2026-09-18 (F-030); the marker is a gate, not a one-shot |
| M2-67 | unsupported_format_version_refused | store-level: stamp `state_meta/format_version = 2`, reopen | typed `StorageOpenError::UnsupportedFormat{found: 2, supported: 1}`; startup refused before any stored value is decoded; exit code 3 for the daemon (`reason="storage_open_failed"`, per M2-60/E2E-19 mapping) | `Display` names both versions; no best-effort decode of an unknown layout |
| M2-68 | missing_format_version_on_non_empty_store_refused | store-level: append an entry, delete `state_meta/format_version`, reopen | typed `StorageOpenError::UnsupportedFormat{found: 0, supported: 1}` — a pre-marker store is refused, never adopted into the current format | `found: 0` is reserved for "written before the marker existed"; an *empty* unstamped directory is stamped instead (M2-66) |

---

## 4. M3 — safe remote use baseline (first release gate)

Unless a row says otherwise: `start_with(ClusterConfig{ nodes: 3, storage: Rocks, tls:
MutualTls(fixture), authz: StaticAllowlist(policy), .. })`. Every TLS row uses `TlsFixture`
(TA-18). No row sleeps for a handshake.

### 4.1 Peer plane mTLS and identity binding (§21 M3 line 1; §15.1; ADR-0010, ADR-0011)

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M3-01 | peer_mtls_happy_path | CA + 3 peer certs with SAN `retcd://<cid>/node/<n>` | form and write 5 keys | leader elected, all writes commit, all three converge; `capabilities().transport_security == MutualTls` | baseline; every negative row below must differ from this by exactly one variable |
| M3-02 | peer_cert_wrong_cluster_id_rejected | node 3's peer cert issued with `cluster_id = other` | start node 3, let it dial | node 1/2 reject the connection; node 3 never joins; the cluster of 2 still forms and commits | Q9: `level="warn"`, `msg="peer_identity_rejected"`, `reason="cluster_id_mismatch"`, fields `expected_cluster_id`, `presented_cluster_id`, `peer_addr`; ≥ 1 row |
| M3-03 | peer_cert_wrong_node_id_rejected | node 3's cert claims `node/9` | same | rejected; `from_node_id` header (ADR-0010) does not match the cert SAN | `reason="node_id_mismatch"` |
| M3-04 | peer_cert_other_ca_rejected | node 3's cert signed by `TlsFixture::other_ca` | same | TLS handshake fails; typed `Unauthenticated`; no Raft RPC is processed | `reason="unknown_ca"` |
| M3-05 | peer_cert_self_signed_rejected | node 3 presents a self-signed cert with a *correct* SAN | same | rejected — a correct SAN on an untrusted chain must not pass | `reason="unknown_ca"`; this row catches "we validated the SAN but forgot the chain" |
| M3-06 | peer_cert_expired_rejected | node 3's cert `not_after` in the past | same | rejected; no sleeping to reach expiry | `reason="cert_expired"` with `not_after` field |
| M3-07 | peer_cert_missing_san_rejected | node 3's cert has CN only, no SAN URI | same | rejected on the **peer** plane (CN fallback is a client-plane affordance only, ADR-0012) | `reason="missing_san"` |
| M3-08 | peer_plaintext_connection_rejected | a raw TCP/h2 client with no TLS | dial the peer port and send a `PeerService` request | connection refused/closed at TLS; no `PeerService` handler runs; no panic; the server keeps serving valid peers afterwards | zero `@logger LIKE 'config_grpc::peer%'` request lines for that connection |
| M3-09 | peer_destination_binding_enforced | valid certs | send a peer RPC whose `to_node_id` header is another node's id | rejected with `PermissionDenied`; the RPC is not executed | `reason="destination_mismatch"`, fields `to_node_id`, `self_node_id`; §21 M3 "wrong destination … rejected" |
| M3-10 | peer_from_node_id_must_match_cert | valid cert for node 2 | send a peer RPC with `from_node_id = 3` | rejected; a node cannot speak for another identity even with a valid cert | `reason="from_node_id_mismatch"` |
| M3-11 | peer_cluster_id_header_must_match | valid cert | send a peer RPC with a different `cluster_id` header | rejected | `reason="cluster_id_mismatch"` |
| M3-12 | peer_client_cert_required | server configured for mTLS; client offers no cert | dial | handshake fails; `Unauthenticated` | proves the server actually requires client auth rather than merely offering TLS |
| M3-13 | rejected_peer_does_not_affect_quorum | M3-02..M3-12 each | while the bad peer hammers the port | the remaining two nodes keep a stable leader and keep committing; `current_term` does not churn | §19.12 |
| M3-14 | gossip_hint_cannot_bypass_peer_mtls | poisoned gossip hint for node 2 pointing at an endpoint served by a cert for node 3 | engine consumes the hint | the candidate endpoint fails identity binding and is never used for Raft transport; committed endpoint continues to be used | ADR-0003; joins M1-32 with real mTLS |

### 4.2 Client plane mTLS and principal derivation (§15.2, ADR-0012)

| ID | Name | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|
| M3-15 | principal_from_san_uri | client cert SAN `retcd://<cid>/client/svc-a`; call `get` | server derives `Principal{name:"svc-a", kind:Client}` | Q9: the request line carries `principal="svc-a"` |
| M3-16 | principal_cn_fallback | client cert with no SAN, CN `svc-b` | `Principal{name:"svc-b", kind:Client}` (ADR-0012 "CN fallback") — on a listener that set `allow_common_name_principals`, which the harness does; that it is **not** the default is M3-88 | as above |
| M3-17 | client_cert_wrong_cluster_id_rejected | client SAN `retcd://<other>/client/svc-a` | `Unauthenticated`; request not executed | `reason="cluster_id_mismatch"` |
| M3-18 | client_cert_other_ca_rejected | client cert from an unrelated CA | handshake fails; `Unauthenticated` | |
| M3-19 | client_cert_expired_rejected | expired client cert | `Unauthenticated` | |
| M3-20 | client_no_cert_rejected | plain TLS, no client cert | `Unauthenticated`; never a default/anonymous principal | this row blocks the "fall back to allow-all when identity is absent" bug |
| M3-21 | client_plaintext_rejected | raw plaintext to the client port | connection closed at TLS; no RPC handled | |
| M3-22 | principal_not_forgeable_from_metadata | valid cert for `svc-a`; attach metadata `retcd-principal: admin` (and any proto field claiming identity) | the metadata is ignored; the effective principal is `svc-a`; authorization is evaluated against `svc-a`'s grants | §6.2 "never accepted from a Protobuf field"; Q9 row shows `principal="svc-a"` |
| M3-23 | direct_client_principal_is_scoped_at_construction | `node.direct_client(Principal{name:"embedder-x"})` | every request from that handle is evaluated as `embedder-x`; no request field can change it; two handles with different principals get different decisions | §15.2; TA-22 |
| M3-24 | peer_cert_cannot_be_used_on_client_plane | present a **peer** cert (`/node/1`) to the client port | rejected (`Unauthenticated`) — separate identity profiles (§15.1, §21 M3 "separate identity profiles") | `reason="wrong_profile"` |
| M3-25 | client_cert_cannot_be_used_on_peer_plane | present a **client** cert to the peer port | rejected | `reason="wrong_profile"` |

### 4.3 Static allowlist authorization (§21 M3 line 4; ADR-0012)

Policy under test (written by the fixture):

```toml
[[grant]] principal="svc-a" prefix="/app/a/" access=["read","write"]
[[grant]] principal="svc-r" prefix="/app/a/" access=["read"]
```

| ID | Name | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|
| M3-26 | unlisted_principal_denied | `svc-z` `get("/app/a/k")` | `PermissionDenied` | Q9: `decision="deny"`, `principal="svc-z"`, `reason="no_grant"` |
| M3-27 | listed_principal_wrong_prefix_denied | `svc-a` `put("/app/b/k")` | `PermissionDenied`; no log entry appended (`raft_log_len` unchanged on all nodes) | `decision="deny"`, `reason="prefix_not_granted"` |
| M3-28 | listed_principal_allowed | `svc-a` `put("/app/a/k")` → `get` | `APPLIED` then the record | `decision="allow"`, `grant_prefix_hex` present |
| M3-29 | read_grant_does_not_permit_write | `svc-r` `put("/app/a/k")` | `PermissionDenied` | `reason="action_not_granted"` |
| M3-30 | read_grant_permits_get_and_list | `svc-r` `get`/`list("/app/a/")` | success | |
| M3-31 | list_prefix_must_be_inside_grant | `svc-a` `list("/app/")` (a *superset* of the grant) | `PermissionDenied`, **not** a filtered result — ADR-0012 "the requested prefix must be within a granted prefix (not just overlap)" | `reason="prefix_not_contained"`; a filtered result would leak the existence of unauthorized keys |
| M3-32 | list_exact_grant_prefix_allowed | `svc-a` `list("/app/a/")` and `list("/app/a/sub/")` | both succeed | prefix containment, not equality |
| M3-33 | delete_requires_write | `svc-r` `delete("/app/a/k")` | `PermissionDenied` | |
| M3-34 | denied_mutation_creates_no_log_entry | M3-27, M3-29, M3-33 | `raft_log_len` and `cluster_revision` unchanged on all three nodes | mirrors M1-27; authorization must run before `client_write` |
| M3-35 | missing_policy_file_fails_closed | start a node with no allowlist file and no `--dev-allow-all` | node is **unready for client traffic**; client calls return `PermissionDenied`/`Unavailable` per OQ-19; peer plane still works | Q9: `level="error"`, `msg="policy_missing"`; readiness false in `HealthPayload` |
| M3-36 | unparsable_policy_fails_closed | malformed TOML | same, with `msg="policy_invalid"` and a parse location | never "fall back to allow-all" |
| M3-37 | empty_policy_denies_everything | a valid file with zero grants | every client request denied; the node **is** ready (the policy loaded fine, it just grants nothing) | distinguishes "no policy" from "empty policy" |
| M3-38 | allowall_requires_dev_flag | `AllowAll` configured without `--dev-allow-all` | `config-server` exits non-zero (code 2) with a typed error; the in-process node builder returns the same typed error | §21 M3: a release build must not silently be open |
| M3-39 | allowall_with_dev_flag_reports_development | `--dev-allow-all` | node starts; `capabilities().authz == Development`; a `warn` line `msg="authz_allow_all_enabled"` on every startup | ADR-0016 |
| M3-40 | static_allowlist_reports_static_allowlist | policy file present | `capabilities().authz == StaticAllowlist` | |
| M3-41 | authz_applies_to_direct_client | `direct_client(svc-r)` `put` | `PermissionDenied` — the embedded path is not privileged (§6.1 "a direct client does not bypass … authorization hooks") | TA-22: the same code path, asserted by one shared decision log line |
| M3-42 | policy_summary_in_health | — | `HealthPayload.policy` reports kind, grant count and the policy file hash; identical on all three nodes | operator surface; also used by E2E |

### 4.4 Transport-security gating and capabilities (§21 M3; ADR-0010, ADR-0016)

| ID | Name | Action | Expected |
|---|---|---|---|
| M3-43 | daemon_refuses_insecure_without_flag | `config-server` with `tls_mode = "insecure"` and no `--allow-insecure-dev` | exits code 2 before binding any listener; typed error names the flag |
| M3-44 | daemon_accepts_insecure_with_flag_and_warns | with `--allow-insecure-dev` | starts; every startup logs `level="warn"`, `msg="insecure_transport_enabled"`; `capabilities().transport_security == Insecure` |
| M3-45 | capabilities_exact_values_m3 | mTLS + static allowlist + Rocks | exactly `Capabilities{ durability: Persistent, watch_resumption: Unsupported, authz: StaticAllowlist, transport_security: MutualTls, pagination: Unsupported, dedup: Unsupported }` — one struct equality |
| M3-46 | capabilities_cli_matches_runtime | `config-server --capabilities` JSON vs the running node's `capabilities()` | identical after deserialization; the CLI path must not build a separate struct by hand |

### 4.5 Conformance parity over mTLS (§21 M3 line 2)

| ID | Name | Action | Expected |
|---|---|---|---|
| M3-47 | conformance_direct_client_m3 | `conformance::run_all(cluster.client(leader, svc_a), cfg)` on Rocks + mTLS + allowlist (with `svc-a` granted the whole test prefix) | all C-01..C-15 pass |
| M3-48 | conformance_grpc_client_mtls | `conformance::run_all(cluster.grpc_client_multi_tls(svc_a), cfg)` | all scenarios pass, same expected values |
| M3-49 | conformance_reports_identical_m3 | compare both `ConformanceReport`s scenario by scenario, including returned revisions, outcomes and truncation flags | equal modulo transport-only fields; any difference names the scenario. This is the §21 M3 "direct and gRPC clients pass the same semantic conformance suite" gate |
| M3-50 | conformance_unchanged_from_m1 | diff the scenario ID list executed in M1-44/45 against M3-47/48 | identical set; the suite was not weakened to make TLS pass |

### 4.6 Leader hints over mTLS (§21 M3 "authenticated leader hints"; ADR-0009, ADR-0015)

| ID | Name | Action | Expected | Oracle |
|---|---|---|---|---|
| M3-51 | not_leader_hint_over_grpc_mtls | `grpc_client` pinned to a follower; `put` | `FAILED_PRECONDITION` with metadata `retcd-leader-node-id` / `retcd-leader-endpoint` matching committed membership | metadata asserted directly |
| M3-52 | hint_follow_succeeds_within_3_hops | `grpc_client_multi_tls` first contacting a follower | the put succeeds; `ClientStats.hint_follows <= 3` (TA-23) | |
| M3-53 | hint_follow_bounded_when_all_deny | force every node to return `NotLeader` | the client returns the last error after ≤ 3 attempts and does not loop; `sends <= 4` | ADR-0009 default N = 3 |
| M3-54 | hint_target_identity_validated_before_use | hint the client at an endpoint whose server cert is for a **different** node id | the client refuses to follow it (`Unauthenticated`), and does not send the mutation there | "authenticated leader hints" means the follower's claim is checked against the target's cert, not trusted |
| M3-55 | hint_is_committed_endpoint_not_gossip | poisoned gossip endpoint for the leader + `NotLeader` on a follower | the hint carries the committed endpoint; a `warn` records the mismatch | M1-19 re-run over mTLS |
| M3-56 | unknown_leader_returns_unavailable | isolate a follower | `Unavailable`, not a hint to a stale leader | M1-20 over mTLS |

### 4.7 Unknown outcome and no automatic replay (§21 M3 line 3; ADR-0015)

| ID | Name | Precondition | Action | Expected | Oracle |
|---|---|---|---|---|---|
| M3-57 | deadline_unknown_outcome_after_commit | `start(3, Rocks, MutualTls)`; `k` absent; record `applied_commands` and `cluster_revision` on all nodes | `netfault().drop_response(leader, client, 1)`; `put(k, v)` with a 2 s deadline; the entry **commits and applies** on all nodes | the client gets `ConfigError::DeadlineExceededUnknownOutcome` (gRPC `DEADLINE_EXCEEDED`), never `APPLIED`, never `Unavailable` | typed error assertion |
| M3-58 | unknown_outcome_client_does_not_retry | M3-57 | inspect `ClientStats` | `sends == 1`, `hint_follows == 0`, `reconnects == 0` | TA-23 — the only assertion that actually proves "no automatic replay" |
| M3-59 | unknown_outcome_applied_exactly_once | M3-57 | re-read via a healthy path | `get(k)` returns `v` with a single `mod_revision`; `cluster_revision` increased by exactly 1 on every node; `applied_commands` increased by exactly 1 on every node | TA-24 |
| M3-60 | unknown_outcome_revision_count_check | M3-57 | `list` the whole prefix | exactly one record for `k`; no duplicate revision anywhere; `state_hash` equal on all three | §19.3 |
| M3-61 | unknown_outcome_on_uncommitted_write | drop the response **and** isolate the leader before commit | client gets `DeadlineExceededUnknownOutcome` or `Unavailable`; after heal, the mutation is either fully applied once or not at all — never half | the "unknown" in the name is honest both ways |
| M3-62 | cas_recovery_recipe_works | after M3-57 | follow the documented recipe: `get(k)` then `put(k, v2, expected = observed mod_revision)` | `APPLIED`; exactly one additional revision; no duplicate | ADR-0015 recovery recipe is executable, not prose |
| M3-63 | retry_storm_does_not_multiply_mutations | 20 concurrent clients each issuing the same CAS `put(k, v, expected=r)` under an aggressive deadline, with `drop_response` armed on half | at most one `APPLIED` for expected `r`; `cluster_revision` increases by at most 1 for that CAS generation; the rest are `CONFLICT` or unknown-outcome | §20 "retry storms"; the M0-62 property, at the transport layer |
| M3-64 | unavailable_before_submission_reconnect_bounded | stop the pinned node, then `put` | the client reconnects at most N times and returns `Unavailable`; `sends` counts the attempts and is ≤ 4; the mutation never enters any log | ADR-0015 "reconnecting on Unavailable returned before submission" |
| M3-65 | deadline_exceeded_maps_to_grpc_deadline | M3-57 over gRPC | status code is exactly `DEADLINE_EXCEEDED` (§6.2), not `UNAVAILABLE`, not `INTERNAL` | status table conformance |

### 4.8 Signed bootstrap manifest formation (§4.3, ADR-0011)

| ID | Name | Action | Expected | Oracle |
|---|---|---|---|---|
| M3-66 | valid_manifest_forms_cluster | 3 daemons/nodes with a valid signed manifest; `--form` on node 1 | cluster forms; `membership_voter_ids == {1,2,3}` on all three | |
| M3-67 | tampered_toml_rejected | flip one byte in `manifest.toml` | signature verification fails **before** any field is acted on; exit code 2; `msg="manifest_signature_invalid"` | fields `manifest_path`, `key_id`; never `msg="formation_started"` |
| M3-68 | tampered_signature_rejected | flip one byte in `manifest.sig` | same | |
| M3-69 | wrong_signing_key_rejected | sign with a different Ed25519 key | same, with `reason="unknown_key_id"` or `"bad_signature"` | |
| M3-70 | truncated_or_missing_signature_rejected | truncate / delete `manifest.sig` | typed error, never "no signature = OK" | the classic fail-open bug |
| M3-71 | expired_manifest_rejected | `expires_at` in the past | rejected with `msg="manifest_expired"` carrying `expires_at` and `now`; no sleeping | §4.3 |
| M3-72 | manifest_cluster_id_must_match_node_identity | node configured for cluster A, manifest for cluster B | `IdentityMismatch` before formation | joins M2-48 |
| M3-73 | manifest_is_not_authority_after_formation | after formation, rewrite the manifest with a different endpoint for node 2 and restart node 1 | node 1 uses the **committed membership** endpoint; the manifest change is ignored (a `warn` may record the divergence); replication to node 2 never breaks | §4.3 "after formation, committed Raft membership is authoritative" |
| M3-74 | manifest_node_set_must_match_formation_plan | manifest lists nodes {1,2,4}; formation plan {1,2,3} | formation proceeds with the manifest voter set {1,2,4}: the manifest **is** the formation plan (ADR-0018 note "the manifest is the formation plan"); own-id, endpoint, signature and expiry checks still refuse | `m3_74_manifest_node_set_must_match_formation_plan` asserts membership {1,2,4} |

### 4.9 Trace context and audit over gRPC (ADR-0013; §15.2; §18.2)

| ID | Name | Action | Expected | Oracle |
|---|---|---|---|---|
| M3-75 | trace_id_propagates_client_to_server | `GrpcClient` put with a known `TraceContext` over mTLS | the server's request span and the leader's apply lines carry the same `trace_id`; `parent_span_id` equals the client's `span_id` | Q10 (cross-process/client-server join) |
| M3-76 | request_id_propagates | same | `request_id` identical on client and server lines | Q10 |
| M3-77 | trace_id_reaches_both_followers_over_mtls | same | ≥ 1 `op="apply"` line per follower shares the `trace_id` | Q1 (M0-M1 plan) re-run with mTLS + Rocks |
| M3-78 | invalid_trace_header_does_not_break_request | send a malformed `retcd-trace-id` (not 32 hex) | the request succeeds; the server mints a fresh root trace id; a `debug` line records the replacement; no panic | `TraceContext::from_headers` already does this — assert it |
| M3-79 | audit_line_per_mutation | a put and a delete | one `level="info"` audit line each with `principal`, `op`, `key_hex`, `outcome`, `revision`; **no** `value` field | §18.2 "audit mutations without values" |
| M3-80 | no_value_or_credential_in_logs_over_grpc | write a value containing `SENSITIVE_SENTINEL_VALUE`; also pass a cert and policy through the daemon config | Q3 (M0-M1 plan) returns zero rows against the M3 logs, including the daemon logs; additionally no PEM block (`-----BEGIN`) and no private key bytes appear anywhere | Q3 + Q11 |
| M3-81 | authn_authz_failure_metrics | run M3-17..M3-26 | each failure increments an authn/authz failure counter exposed in `NodeMetrics`/health, and emits exactly one `warn` line | §18.2 required metric |

### 4.10 Transport payload sizing (ADR-0010 fix-round note; §7.1, §10.2)

Ids start at M3-86 because OQ-23 reserves M3-82..M3-85 for certificate rotation should it be
pulled into M3.

| ID | Name | Action | Expected | Oracle |
|---|---|---|---|---|
| M3-86 | max_size_value_replicates_to_every_voter | `put` a `max_value_bytes` value of `0xFF` bytes on a 3-node cluster | `APPLIED`, and every voter's applied state holds the value; the `AppendEntries` carrying it (~4× the raw size once serde-JSON encoded) is neither rejected nor retried | `config_grpc::peer_plane_message_limit` is above tonic's 4 MiB default; without it replication wedges and the put ends `DeadlineExceededUnknownOutcome` |
| M3-87 | oversize_list_reply_is_truncated_not_unavailable | `list` a prefix whose reply fills a `max_list_bytes` budget set above 4 MiB | the gRPC client gets `truncated = true` and a page larger than 4 MiB, not `Unavailable` | `config_grpc::client_plane_message_limit`; without it the server's own encoder refuses with "encoded message length too large" |

### 4.11 Common Name principals are opt-in (ADR-0010/ADR-0012 fix-round notes; F-015)

| ID | Name | Action | Expected | Oracle |
|---|---|---|---|---|
| M3-88 | common_name_principals_are_refused_unless_enabled | present a client certificate from the shared test CA that asserts **no** `retcd://` SAN, whose CN is a name the cluster grants (i.e. indistinguishable from one the same CA minted for a neighbouring cluster), to a listener with `allow_common_name_principals` unset, then to one with it set | unset: `UNAUTHENTICATED` and the store sees no call; set: served with `Principal{name: CN, kind: Certificate}` | `config_grpc::tls::principal_from_certs`; a Common Name carries no cluster id, so with the gate forced open the first half of the row passes a foreign cluster's certificate through — `m3_88_common_name_principals_are_refused_unless_enabled` in `config-grpc/tests/mtls.rs` |

---

## 5. E2E — process-level daemon suite

`crates/config-server/tests/e2e_daemon.rs` (TA-25). Shape shared by every row: build a
`TlsFixture` + `ManifestFixture`; create a `TempDir` with `node1/ node2/ node3/`; write a config
file, certs, allowlist and manifest into each; `DaemonProcess::spawn` × 3 with `--form` on node 1;
`wait_ready` on all three; drive the test; assert; `Drop` kills everything.

| ID | Name | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|
| E2E-01 | three_processes_form_cluster | spawn 3, `--form` on node 1 | all three report ready within 10 × election timeout; `HealthPayload.membership_voter_ids == {1,2,3}` and identical `membership_log_id` on all three; exactly one leader | health payload over mTLS |
| E2E-02 | capabilities_from_cli | `config-server --capabilities` on the node 1 config | JSON equals the running node's capabilities: `durability=Persistent`, `authz=StaticAllowlist`, `transport_security=MutualTls`, `watch_resumption=Unsupported` | |
| E2E-03 | conformance_over_grpc_mtls_to_daemons | `conformance::run_all(GrpcClient over the 3 daemon client endpoints, cfg)` | all C-01..C-15 pass against real processes | this is the row that makes the suite an integration gate, not a unit test |
| E2E-04 | kill_leader_process_elects_new_leader | write 10 keys; `kill()` the leader process (TA-26) | a new leader appears among the survivors within 10 × election timeout; the killed process is confirmed exited via `wait()` | `HealthPayload.current_leader` on survivors |
| E2E-05 | no_data_loss_after_process_kill | after E2E-04 | all 10 acknowledged revisions readable from the new leader; `cluster_revision == 10`; `state_hash` identical on both survivors | TA-17 health `state_hash` |
| E2E-06 | killed_node_restarts_and_catches_up | respawn the killed node on the **same data dir and node id** | it rejoins as a follower without re-forming; `last_applied` reaches the leader's; `state_hash` equals the leader's; writes made while it was dead are present | zero `msg="formation_started"` in the restarted process's log |
| E2E-07 | writes_continue_while_one_process_dead | with one process killed, write 5 more keys | all `APPLIED`; quorum of 2 suffices; revisions 11..15 | |
| E2E-08 | cold_restart_whole_cluster | `shutdown_graceful` all three; respawn all three without `--form` | cluster re-forms from disk; all 15 revisions present; `state_hash` identical on all three; membership log id unchanged | §21 M2 line 1 at process level |
| E2E-09 | graceful_shutdown_is_clean | `shutdown_graceful` on one node | exit code 0 within the deadline; final log line `msg="shutdown_complete"`; the RocksDB dir reopens immediately afterwards with no `LOCK` error | TA-21, TA-26.4 |
| E2E-10 | cross_process_trace_correlation | one client put through the leader daemon | Q10 joins the client's `trace_id` to lines in **all three** daemon log files; ≥ 1 `op="apply"` line per node | the three log files are separate; the join is the proof that propagation works across processes |
| E2E-11 | per_process_log_files_exist_and_are_tagged | any E2E test | each node's `logs/<testModule>/<testMethod>.jsonl` exists, is non-empty, carries exactly one `testMethod`, and every line carries `node_id` | Q12 |
| E2E-12 | insecure_refused_at_process_level | config with `tls_mode="insecure"` and no flag | process exits code 2 before binding; stderr/log names `--allow-insecure-dev` | |
| E2E-13 | unlisted_principal_denied_at_process_level | client cert for `svc-z` not in the allowlist | `PERMISSION_DENIED` from the daemon; a deny audit line in that node's log | Q9 over daemon logs |
| E2E-14 | identity_mismatch_at_process_level | swap node 2's and node 3's data dirs and restart both | both exit code 2 with `msg="identity_mismatch"`; neither serves traffic; the third node keeps running | §4.2 |
| E2E-15 | unknown_outcome_at_process_level | issue a put with a short deadline while killing the leader process mid-flight | client gets `DEADLINE_EXCEEDED`/`DeadlineExceededUnknownOutcome`; `ClientStats.sends == 1`; after a new leader is elected, the key is applied **zero or one** times — never twice (`list` shows at most one record, `applied_commands` delta ≤ 1) | §21 M3 line 3 at process level |
| E2E-16 | crash_kill_loses_no_acknowledged_mutation | loop 5×: write 3 keys, `kill()` a random-but-seeded node, respawn it, wait for catch-up | every acknowledged revision is readable at the end; `state_hash` identical on all three; no log hole in any node's `raft_log` (checked by reopening each store read-only after shutdown) | the seed is printed on failure |
| E2E-17 | no_fixed_ports_and_clean_temp_dirs | any E2E test | every bound port came from the ready line; a source scan of `crates/config-server/tests/**` finds no literal port; after the test the `TempDir` is gone and no `config-server` process survives | anti-flake rules 4, 5 + TA-20.4 |
| E2E-18 | full_suite_parity_on_target_host | CI job: `cargo test --workspace` (M0+M1+M2+M3+E2E) on the target VM/disk class | all green in one run, within the §2 budget; the job name and host class are recorded in the run log | §21 M3 line 5 — this is the release gate itself |
| E2E-19 | locked_data_dir_exits_storage_code | start node 1 with `--form` and wait for ready; spawn a second `config-server` on the **same config and data directory**, with its own `--log-dir` | the second process exits **3** (not 2), prints no ready line, and logs exactly one `msg="startup_failed"` with `reason="storage_open_failed"`; the holder keeps serving `/health` and still stops cleanly with exit 0 | ADR-0018 §5 — the only row that observes exit code 3 at process level; covers M2-60's daemon clause and M2-64's second-process half |

---

## 6. Harness additions (summary of the required surface)

Everything below is additive to §4.1 of the M0-M1 plan.

```rust
// ---- storage ------------------------------------------------------------
pub enum StorageKind { Ephemeral, Rocks(RocksSpec) }          // TA-16
pub struct RocksSpec { pub dir: Option<PathBuf>, pub injector: Option<Arc<dyn FaultInjector>>,
                       pub sync_mode: SyncMode }
pub enum SyncMode { Full, NoSync }

// ---- faults -------------------------------------------------------------
pub enum Boundary { BeforeVoteSync, AfterVoteSync, BeforeLogAppend, AfterLogAppend,
                    BeforeLogFlush, AfterLogFlush, BeforeStateBatch, AfterStateBatch }
impl Boundary { pub const ALL: [Boundary; 8]; }
pub struct BoundaryCounter;                                    // TA-15
pub struct BoundaryCounts { /* PartialEq + Debug, one u64 per boundary */ }

// ---- tls / manifest -----------------------------------------------------
pub struct TlsFixture;  pub enum CertProfile; pub struct CertOverrides; // TA-18
pub struct ManifestFixture; pub enum Tamper;                            // TA-19

// ---- cluster ------------------------------------------------------------
impl Cluster {
    pub async fn restart(&self, id: NodeId) -> Result<(), StartError>;
    pub async fn reopen_store(&self, id: NodeId) -> Result<(), StorageOpenError>;
    pub async fn stop_all(&self);  pub async fn start_all(&self);
    pub fn data_dir(&self, id: NodeId) -> &Path;
    pub fn injector(&self, id: NodeId) -> Arc<BoundaryCounter>;
    pub fn state_hash(&self, id: NodeId) -> [u8; 32];
    pub fn health(&self, id: NodeId) -> HealthPayload;
    pub fn client_as(&self, id: NodeId, p: Principal) -> DirectClient;
    pub async fn grpc_client_tls(&self, id: NodeId, c: &CertPair) -> GrpcClient;
    pub async fn grpc_client_multi_tls(&self, c: &CertPair) -> GrpcClient;
    pub fn assert_crash_invariants(&self);                     // §3.3 shared helper
}

// ---- processes ----------------------------------------------------------
pub struct DaemonSpec; pub struct DaemonProcess; pub struct Ports;      // TA-20

// ---- log assertions -----------------------------------------------------
pub mod logq {
    pub fn query(sql: &str) -> duckdb::Result<Vec<Row>>;       // duckdb-rs, ADR-0014 clarification
    pub fn test_logs_glob() -> String;                          // target/test-logs/**/*.jsonl
    pub fn daemon_logs_glob(root: &Path) -> String;             // <tempdir>/node*/logs/**/*.jsonl
    pub fn assert_nonempty(rows: &[Row], what: &str);           // anti-flake rule 11
}
```

`ClusterConfig` gains: `authz: AuthzKind { AllowAll, Static(String /*TOML*/), Missing, Invalid }`
and `tls: TlsMode { Insecure, MutualTls(Arc<TlsFixture>) }` (the M0-M1 plan already reserves the
`tls` field).

---

## 7. Log-based assertions (DuckDB) — Q7 …

**Field-name warning (see §11 item 1):** the implemented `config-log` JSONL layer emits
`@t`, `@l`, `@m`, `@logger`, `application`, `thread`, plus flattened span fields. It does **not**
emit `ts`, `level`, `msg`, `target`. The queries below use the implemented names. Q1..Q6 in the
M0-M1 plan use the ADR-0013 names and will return zero rows as written; fix them in the same
change that resolves OQ-20.

All queries run after the appender has flushed (TA-8.4). Every assertion must first check for a
**positive** row count where rows are expected (anti-flake rule 11).

### Q7 — storage boundary and identity events (M2-11, M2-42..M2-46, M2-56..M2-62)

```sql
SELECT node_id, "@l" AS level, "@m" AS msg, boundary, fault_action,
       cluster_id, stored_cluster_id, configured_cluster_id, log_index, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND ("@m" IN ('store_opened','identity_mismatch','storage_fatal','replay_committed',
                'fault_injected','apply') OR boundary IS NOT NULL)
GROUP BY ALL ORDER BY node_id, msg;
```

Assertions per row are named in §3. The identity rows additionally assert **zero** lines with
`"@logger" LIKE 'openraft%'` for the failing node — proof that `open()` refused *before*
`Raft::new` (ADR-0011).

### Q8 — formation happened exactly once (M2-06, M2-08, E2E-06, E2E-08)

```sql
SELECT node_id, "@m" AS msg, count(*) AS n
FROM read_json_auto(?, union_by_name=true)         -- test logs or daemon logs
WHERE testMethod = ?
  AND "@m" IN ('formation_started','formation_completed','raft_initialize')
GROUP BY ALL;
```

**Assertion:** for a cold-restart test, zero rows. For a formation test, exactly one
`formation_completed` on exactly one node.

### Q9 — identity rejection and authorization decisions (M3-02..M3-14, M3-26..M3-42, E2E-13)

```sql
SELECT node_id, "@l" AS level, "@m" AS msg, reason, principal, decision,
       action, key_hex, policy_kind, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ?
  AND ("@m" IN ('peer_identity_rejected','client_identity_rejected','authz_decision',
                'policy_missing','policy_invalid') )
GROUP BY ALL ORDER BY n DESC;
```

**Assertions:** the expected `reason`/`decision` row exists with `n >= 1`; for a deny row there
is **no** `decision='allow'` row for the same `principal` + `key_hex`; no row contains a `value`
column (redaction, §15.2).

### Q10 — client→server→followers trace join over gRPC and across processes (M3-75..M3-77, E2E-10)

```sql
WITH lines AS (SELECT * FROM read_json_auto(?, union_by_name=true) WHERE testMethod = ?),
c AS (SELECT trace_id, request_id, span_id FROM lines
      WHERE application = 'retcd-tests' AND op = 'put' AND "@m" = 'client_request'),
s AS (SELECT l.trace_id, l.node_id, l.parent_span_id,
             count(*) FILTER (WHERE l.op = 'apply')          AS apply_lines,
             count(*) FILTER (WHERE l."@m" = 'server_request') AS server_lines
      FROM lines l JOIN c ON l.trace_id = c.trace_id
      GROUP BY 1,2,3)
SELECT c.trace_id, count(DISTINCT s.node_id) AS nodes_touched,
       sum(s.apply_lines) AS applies, sum(s.server_lines) AS server_reqs,
       bool_or(s.parent_span_id = c.span_id) AS parent_linked
FROM c LEFT JOIN s ON s.trace_id = c.trace_id GROUP BY 1;
```

**Assertion:** exactly one row; `nodes_touched == 3`; `applies >= 3`; `server_reqs >= 1`;
`parent_linked` is true. For E2E-10 the glob spans three separate daemon log directories — the
join working across files is the point.

### Q11 — no credential material anywhere (M3-80)

```sql
SELECT count(*) AS leaks
FROM read_text(?)                                   -- raw text scan over all jsonl files
WHERE content ILIKE '%-----BEGIN%'
   OR content ILIKE '%PRIVATE KEY%'
   OR content ILIKE '%SENSITIVE_SENTINEL_VALUE%';
```

**Assertion:** `leaks == 0`. Raw-text scan, because a leak could appear in any field including
ones we did not anticipate.

### Q12 — every daemon log line is tagged (E2E-11)

```sql
SELECT coalesce(testModule,'<null>') m, coalesce(testMethod,'<null>') t,
       coalesce(testRun,'<null>') r, coalesce(node_id::VARCHAR,'<null>') n, count(*) c
FROM read_json_auto(?, union_by_name=true)
WHERE testModule IS NULL OR testMethod IS NULL OR node_id IS NULL
GROUP BY ALL;
```

**Assertion:** zero rows; plus each of the three files exists, is non-empty, and has exactly one
distinct `node_id`.

### Q13 — unknown-outcome apply count (M3-59, E2E-15)

```sql
SELECT node_id, count(*) AS applies
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND op = 'apply' AND key_hex = ?
GROUP BY 1;
```

**Assertion:** every node reports exactly 1 (or, for E2E-15's uncommitted variant, all report 0
or all report 1 — never a mix, never 2). This is corroboration; `applied_commands` (TA-24) is the
primary oracle.

---

## 8. Anti-flake additions (extend §6 of the M0-M1 plan)

14. **Close before you reopen.** Any test that reopens a data directory must have awaited the
    previous store's close. `Cluster::restart` does this; a hand-rolled `stop` + `Rocks::open`
    does not and will flake on Windows.
15. **No test asserts on wall-clock durations of I/O.** "The sync took < 5 ms" is a benchmark
    (§9.3.8), not a test.
16. **Process tests always `wait()`.** After `kill()` or graceful shutdown, `wait()` for the exit
    status before touching the data directory or asserting on the log file.
17. **Every spawned process is killed on `Drop`**, including on panic and on test timeout.
18. **Certificates are generated, never checked in**, and are seeded (TA-18.3).
19. **Crash rows must prove the crash happened.** Assert the injector's counter for the armed
    boundary is ≥ 1 *before* asserting the post-restart invariants. A crash test where the
    boundary was never crossed is a silent pass and is the single most likely defect in §3.3.
20. **E2E tests assert on the ready line, never on log scraping or sleeps**, to learn ports.

---

## 9. Gate checklist — §21 acceptance lines → test IDs

A gate passes only when **every** listed ID passes. An acceptance line with no green ID is an
open gate regardless of the rest of the suite.

### M2 — persistence and restart correctness

| §21 M2 acceptance line | Test IDs |
|---|---|
| every acknowledged mutation survives ordinary stop/restart of each node | M2-01, M2-02, M2-03, M2-04, M2-05, M2-07, M2-09, E2E-08, E2E-16 |
| committed-but-unapplied entries replay without duplicate public revisions | M2-11, M2-12, M2-13, M2-14, M2-15, M2-17, M2-18, M2-25 |
| injected crashes around vote sync, log append/flush, and state batch boundaries lose no acknowledged mutation and create no log hole | M2-19, M2-20, M2-21, M2-22, M2-23, M2-24, M2-25, M2-26, M2-27, M2-28, M2-29, M2-40, E2E-16 |
| storage identity mismatch prevents startup | M2-41, M2-42, M2-43, M2-44, M2-45, M2-46, M2-47, M2-48, E2E-14 |
| capability output states `durability=Persistent` only after all M2 gates pass | M2-49, M2-50, M2-51, M2-52, E2E-02 (and OQ-11 for the "only after" mechanism) |
| *(scope: fatal/unready on uncertain persistence, corruption, disk-full)* | M2-56, M2-57, M2-58, M2-59, M2-60, M2-61, M2-62, M2-63, M2-64 |
| *(scope: no snapshots, log purging, dynamic membership)* | M2-30..M2-39, M2-06, M2-08 |

### M3 — safe remote use baseline (first release gate)

| §21 M3 acceptance line | Test IDs |
|---|---|
| wrong cluster, node, destination, and client identities are rejected | M3-02, M3-03, M3-04, M3-05, M3-06, M3-07, M3-08, M3-09, M3-10, M3-11, M3-12, M3-17, M3-18, M3-19, M3-20, M3-21, M3-24, M3-25, M3-72, E2E-14 |
| direct and gRPC clients pass the same semantic conformance suite | M3-47, M3-48, M3-49, M3-50, E2E-03 |
| a lost mutation response cannot trigger an automatic duplicate mutation | M3-57, M3-58, M3-59, M3-60, M3-61, M3-62, M3-63, M3-64, M3-65, E2E-15 |
| static authorization denies unlisted principals and unauthorized key prefixes | M3-26, M3-27, M3-28, M3-29, M3-30, M3-31, M3-32, M3-33, M3-34, M3-35, M3-36, M3-37, M3-38, M3-39, M3-40, M3-41, M3-42, E2E-13 |
| the complete M0, M1, M2, and M3 suites pass together on the target VM and disk class | E2E-18 |
| *(scope: authenticated leader hints)* | M3-51, M3-52, M3-53, M3-54, M3-55, M3-56 |
| *(scope: certificate-bound cluster/node/destination validation)* | M3-01, M3-13, M3-14, M3-66..M3-74 |
| *(scope: minimal health and structured/redacted logging)* | M3-75..M3-81, E2E-10, E2E-11 |
| *(scope: capability output)* | M3-43, M3-44, M3-45, M3-46, E2E-02 |

---

## 10. Open questions (OQ-11 …) — recommendation is the default

Implement the recommendation unless the Architect answers otherwise. Record the answer in the
owning ADR before the blocked row is written.

| ID | Question | Blocks | Owner ADR | Recommendation (default) |
|---|---|---|---|---|
| OQ-11 | §21 M2 says `durability=Persistent` is reported "only after all M2 gates pass" — that is self-referential. What mechanism enforces it? | M2-49, M2-51, E2E-02 | ADR-0016 | `RocksStore` reports `Persistent` whenever it is opened with full sync and verified identity, and `PersistentUnverified` otherwise (sync disabled, or identity check skipped). The "only after the gates pass" clause is enforced by CI: `tests/m2_*.rs` is a required gate, so no artifact claiming `Persistent` can ship without it. Do **not** invent a build-time `m2_verified` feature — a cfg-gated capability is a different code path from the one under test. |
| OQ-12 | ADR-0008 lists six fault boundaries; §20 requires log-flush injection too. Extend to eight? | M2-19..M2-29 | ADR-0008 | Yes — extend to eight and make `append` write-then-explicit-sync (TA-13). Keeping six means §20's "log flush" gate has no test. |
| OQ-13 | Opening a data dir with an **extra** column family (e.g. a future `events`) — reject, ignore, or migrate? | M2-63 | ADR-0008 / §17 | Reject with a typed error naming the CF. An unexpected CF means the directory belongs to a different schema version; §17 requires explicit migrations, and silently ignoring it risks a later downgrade writing an inconsistent pair of CFs. |
| OQ-14 | `apply` batching: does the state machine ever receive more than one entry per `WriteBatch`, and is that one sync or many? | M2-53, M2-16 | ADR-0008 | One `WriteBatch` per `apply()` call covering **all** entries in that call, one sync. Record the observed sync count for a 10-put workload as a golden constant in M2-53 so a change is a test failure, not a silent durability regression. |
| OQ-15 | Is there a supported no-sync mode at all, or is `SyncMode::NoSync` test-only? | M2-51, TA-21 | ADR-0008 | Test-only plus a `--unsafe-no-sync` daemon flag that forces `durability=PersistentUnverified` and logs a `warn` on every startup. It is needed to test the capability downgrade, and an undocumented test-only path tends to become an undocumented production path. |
| OQ-16 | Is the health/readiness surface reachable without client-plane mTLS credentials? E2E needs it for `state_hash`, but §15.1 puts admin on a separate privileged plane. | TA-17, E2E-01, E2E-05, E2E-06 | ADR-0010 / §18.1 | A **local-only** health listener bound to `127.0.0.1` on its own port, plaintext, read-only, no key material and no values (only counts, revisions and the `state_hash` digest). Document it as loopback-only; do not fold it into the admin plane, which is post-release. |
| OQ-17 | Graceful shutdown trigger on Windows (no `SIGTERM`). | TA-21, E2E-09, E2E-08 | ADR-0017 / a new ops ADR | Support both: `tokio::signal::ctrl_c` **and** `--shutdown-file <path>` (the daemon watches for the file's creation and shuts down cleanly). The file is what the E2E suite uses, because sending Ctrl-C to one child on Windows without hitting the whole console group is unreliable. |
| OQ-18 | How do daemon processes inherit `testModule`/`testMethod` so cross-process DuckDB joins work? | E2E-10, E2E-11, TA-20.3 | ADR-0013 | Repeatable `--log-field k=v` CLI flags that add constant fields to the root span of the process. Environment variables would violate anti-flake rule 6 for the spawning test, and hardcoding test names in the daemon is worse. |
| OQ-19 | A node that is unready for client traffic because policy is missing: what does a client call return — `PermissionDenied` or `Unavailable`? | M3-35, M3-36 | ADR-0012 / §16 | `PermissionDenied`. Fail closed means "denied", and `Unavailable` is documented as retryable (§16), which would make clients hammer a node that will never serve them. Readiness is separately false so load balancers drain it. |
| OQ-20 | ADR-0013 specifies `ts`/`level`/`target`/`msg`; `config-log` emits `@t`/`@l`/`@logger`/`@m`. Which is normative? | every query in §7, and Q1..Q6 of the M0-M1 plan | ADR-0013 | Keep the implemented CLEF-style names (`@t`, `@l`, `@m`, `@logger`) — they are already shipped and they avoid colliding with a user field named `level`. Update ADR-0013 and the M0-M1 plan's queries in the same change. |
| OQ-21 | What makes a leader hint "authenticated" (§21 M3 scope)? A signature, or the mTLS session plus target-cert validation? | M3-51, M3-54 | ADR-0009 / ADR-0010 | The mTLS session (the hint came from an authenticated node) **plus** the client validating the hinted target's certificate SAN against `retcd://<cluster_id>/node/<hinted_node_id>` before sending anything to it. No separate hint signature; a signature would need key distribution we do not have in M3. M3-54 is the test that makes this real. |
| OQ-22 | Does the client plane enforce a `Principal` on `Get`/`List` too, or only on mutations? | M3-26, M3-30, M3-31 | ADR-0012 | Both. `List` requires `read` on the requested prefix and the prefix must be contained in a grant (already in ADR-0012); `Get` requires `read` on the key. Otherwise the allowlist leaks every value to any authenticated principal. |
| OQ-23 | Certificate rotation with one voter unavailable (§20 "Operations" gate) — M3 or M5? | none (scoping) | ADR-0010 / §21 | M5. §21 M3 does not list rotation, and the first release documents overlapping-CA support without gating it. This plan contains no rotation row; if the Architect pulls it into M3, add rows M3-82..M3-85 (new CA added to both trust stores → rolling re-issue → old CA removed) before the gate closes. |
| OQ-24 | VM pause / power-loss simulation (§20) — gated in M2 or deferred? | none (scoping) | §20 / ADR-0014 | Deferred past the first release and documented as such. §21 M2's acceptance lines require crash injection and process kill, both of which this plan covers; a true power-loss test needs hardware or hypervisor control that the dev host does not have. Do not claim the §20 bullet is met. |
| OQ-25 | `max_in_snapshot_log_to_keep`: ADR-0008 says `u64::MAX`; the OpenRaft research recommends leaving the default `1000`. | M2-36, M2-37 | ADR-0008 | Leave the default. With `SnapshotPolicy::Never` the purge arithmetic can never fire either way (research §3.6 step 5), and `u64::MAX` risks tripping `Config::validate` or an overflow in a future version. M2-36 proves purging never happens, which is the property that actually matters. |

---

## 11. Spec / ADR contradictions found (for the Architect)

These are places where the authoritative documents disagree with **each other** or with shipped
code. Each needs a decision, not a test.

1. **Log field names.** ADR-0013 mandates `ts`, `level`, `target`, `msg`. The implemented
   `crates/config-log/src/layer.rs` emits `@t`, `@l`, `@logger`, `@m` (plus `application`,
   `thread`, `file`, `line`). Every DuckDB query in `test-plan-m0-m1.md` §5 is written against
   the ADR names and will silently return zero rows — which anti-flake rule 11 would catch as a
   failure, but only after someone writes the test. **Blocking for M1's log tests, not just M2/M3.**
   See OQ-20.
2. **Fault boundaries: six vs eight.** ADR-0008 and TA-4 list six; §20 requires log-flush
   injection as well. See OQ-12 / TA-13.
3. **`FaultInjector` semantics for `Crash`.** ADR-0008 describes `Crash` only as a fault
   position. Nothing states that the store must not flush on `Drop` — without TA-14 the crash
   tests are unfalsifiable (a flushing `Drop` makes every boundary look like `AfterX`).
4. **`Capabilities::durability` variants.** ADR-0016 declares `Ephemeral|Persistent`. ADR-0008,
   §21 M2, and the M0-M1 plan (M1-39) all refer to `PersistentUnverified`, which the enum does
   not contain. See TA-27.
5. **E2E test location.** ADR-0014 §4 says `tests/e2e_daemon.rs`. If that is the workspace-root
   test crate, `CARGO_BIN_EXE_config-server` is not defined there. It must live in
   `crates/config-server/tests/`. See TA-25.
6. **Harness restart naming.** ADR-0014's Decision list says `restart(node)` and
   `crash_at(node, boundary)`; its 2026-09-18 Clarifications say `cluster.start_node(id)` /
   `cluster.stop(id)`. This plan provides `restart(id)` as a convenience over
   `stop` + `reopen_store` + `start_node`, and replaces `crash_at` with
   `injector(id).crash_on_nth(boundary, n)` (the *n*-th crossing is required by §3.3 and
   `crash_at` cannot express it). Fold this into ADR-0014.
7. **`save_committed` is not in ADR-0008's rules.** The `raft_meta` CF lists a `"committed"`
   key, but no rule states that `save_committed`/`read_committed` must be implemented or that
   `committed` must be synced. Per the OpenRaft research (§8.2), the trait **defaults** silently
   disable committed-but-unapplied replay, which is a §21 M2 acceptance line. ADR-0008 should
   state the requirement explicitly; M2-13/M2-14 test it.
8. **§20 gates not covered by any §21 acceptance line.** VM pause, power-loss simulation, long
   compaction, certificate rotation with a voter down, and mixed-version rolling upgrade are all
   §20 "production designation" requirements with no M2/M3 acceptance line. This plan does not
   test them and the release notes must not imply otherwise (§21 M3's own "not yet
   production-capable" label already covers this — keep it). See OQ-23, OQ-24.
9. **Admin plane.** §15.1 lists an admin plane with its own port and credentials; §21 M3 ships
   no admin API. No admin rows exist in this plan. If `config-server` binds an admin listener at
   all in M3, that is scope creep and needs a row proving it is disabled.

## Architect answers (2026-09-18)

All OQ-11..OQ-25 defaults are adopted as written. Contradictions in §11 resolved:
1. Log field names: shipped CLEF names (`@t`,`@l`,`@logger`,`@m`) are canonical; ADR-0013 and
   the M0-M1 plan §5 queries updated.
2. `Durability::PersistentUnverified` added to ADR-0016.
3. Eight fault boundaries, explicit `save_committed`/`read_committed`, crash poisoning, and
   the `Persistent` reporting rule recorded in ADR-0008 Clarifications.
4. ADR-0014 Clarifications: `CrashAt{boundary,nth}`, `reopen_store`+`start_node`, E2E in
   `crates/config-server/tests/`, `--shutdown-file`, loopback health listener with
   `state_hash`, authenticated-hint definition.
5. §20 gates without §21 acceptance lines (VM pause, power loss, cert rotation with a voter
   down, mixed-version upgrade) are out of scope for the first release and will be listed as
   such in the release notes.
