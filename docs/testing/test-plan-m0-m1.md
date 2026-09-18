# Test Plan — M0 and M1

**Status:** Proposed (Tester Planner deliverable)
**Date:** 2026-09-17
**Scope:** M0 (deterministic state-machine laboratory) and M1 (three-node in-process core).
M2/M3 appear only as a roadmap in §7.
**Authority:** `docs/DesignSpec-01.md` §6, §7, §8, §10, §13, §19, §20, §21 and
`docs/ADRs/0003, 0004, 0005, 0006, 0007, 0008, 0009, 0011, 0013, 0014, 0015, 0016`.
Where this plan and the spec/ADRs disagree, the spec/ADRs win and this plan is a defect.

**How to use this document**

- Developers: §1 is a contract. Code that does not expose these seams is not done, because the
  tests in §3/§4 cannot be written against it.
- Testers: §3 and §4 are the test backlog. One row = one `#[test]`/`#[tokio::test]`. IDs are
  stable and must appear in the test name (e.g. `m0_07_put_expected_n_absent_conflict`).
- Both: §8 lists open questions that block specific rows. Do not guess; resolve them, record
  the answer in the owning ADR, then write the golden test.

**File mapping (ADR-0014 §6 — gates map 1:1 to §21 bullets)**

| Area | Path |
|---|---|
| M0 unit/table/property | `crates/config-core/tests/m0_*.rs` |
| M0 source-scan guard | `crates/config-core/tests/m0_purity.rs` |
| M1 cluster gates | `tests/m1_*.rs` (workspace-level test crate) |
| Harness | `crates/config-testkit/src/{cluster.rs, netfault.rs, conformance.rs, certs.rs}` |
| Test logs | `target/test-logs/<testModule>/<testMethod>.jsonl` |

---

## 1. Test-architecture requirements

These are requirements **on the production code**, not on the tests. Each has an ID so a
review can reject a PR by number. "Must" is normative.

### TA-1 — Pure, synchronous state machine

`config-core` must expose the M0 state machine as:

```rust
pub struct KvState { /* BTreeMap<Bytes, Record>, cluster_revision: u64 */ }

impl KvState {
    pub fn new() -> Self;
    pub fn apply(&mut self, cmd: &Command) -> CommandResponse;   // total, infallible, sync
    pub fn state_hash(&self) -> [u8; 32];
    pub fn get(&self, key: &[u8]) -> Option<&Record>;
    pub fn list(&self, req: &ListRequest) -> ListResponse;       // read-only
}
```

Requirements:

1. `apply` is **not** `async`, takes `&mut self`, returns `CommandResponse` by value, and
   returns `CommandResponse` for **every** input including invalid ones. It must never return
   `Result`, never panic, and never allocate a revision for a rejected command (ADR-0005,
   ADR-0006).
2. `apply` must be callable with no Tokio runtime, no OpenRaft types in scope, and no I/O.
   `config-core` must not depend on an async runtime or network crate (ADR-0004).
3. `KvState: Default + Clone` so a test can snapshot state, fork it, and diff. `Clone` is what
   makes the "N commands built from the same snapshot" test in M0-46 expressible.
4. `CommandResponse: PartialEq + Debug + Clone` (and `Serialize`) so a whole response vector
   can be compared in one assertion and printed on failure.

### TA-2 — `state_hash()` contract

`state_hash()` must be a stable 32-byte digest (SHA-256) over, in this exact order:

1. `cluster_revision` as u64 LE;
2. the record count as u64 LE;
3. each `(key, value, create_revision, mod_revision)` in **unsigned bytewise ascending key
   order**, each field length-prefixed with u32/u64 LE so no two distinct states can collide
   by concatenation.

It must **exclude** `last_applied`, membership, and any node-local field, so two state
machines on different nodes with the same applied prefix hash identically. `state_hash()` of a
fresh `KvState::new()` must be a documented golden constant (M0-55).

### TA-3 — One deterministic validator, two call sites

Request validation (size caps, `Delete expected=0`) must live in **one** pure module used by
both the API edge and `apply`:

```rust
pub fn validate_put(req: &PutRequest, caps: &Limits) -> Result<(), ConfigError>;
pub fn validate_delete(req: &DeleteRequest, caps: &Limits) -> Result<(), ConfigError>;
pub fn validate_list(req: &ListRequest, caps: &Limits) -> Result<ListRequest, ConfigError>;
```

`Limits` must be a plain value struct passed in, never read from a global, env var, or file, so
a test can shrink caps to 8 bytes and exercise truncation without building megabyte payloads
(M0-27..M0-31, M0-22..M0-26). `apply` re-checks deterministically and rejects, never panics
(ADR-0006).

### TA-4 — Storage accepts a `FaultInjector`

Both `EphemeralStore` and `RocksStore` constructors must accept a fault handle (ADR-0008):

```rust
pub trait FaultInjector: Send + Sync + 'static {
    fn before(&self, b: Boundary) -> FaultAction;   // Proceed | Fail(StorageErrorKind) | Crash
}
pub enum Boundary { BeforeVoteSync, AfterVoteSync, BeforeLogAppend, AfterLogAppend,
                    BeforeStateBatch, AfterStateBatch }
```

Requirements: the default is a zero-cost no-op; the hook is consulted on **every** crossing so
an injector can fire on the *k*-th crossing only; `Crash` aborts the store task without
unwinding through a `Drop` that would flush (otherwise the crash test is fiction). M1 uses this
only for `Fail`; M2 uses `Crash`. It must exist in M1 so the seam is not retrofitted later.

### TA-5 — Network layer accepts a `NetFault` handle

The Raft peer network factory and the gossip transport must each take a fault handle so
partitions are simulated **in-process, before RPC dispatch**:

```rust
#[derive(Clone)] pub struct NetFault(Arc<RwLock<FaultTable>>);
impl NetFault {
    pub fn block(&self, from: NodeId, to: NodeId);      // one direction
    pub fn block_pair(&self, a: NodeId, b: NodeId);     // both directions
    pub fn unblock_all(&self);
    pub fn delay(&self, from: NodeId, to: NodeId, d: Duration);
    pub fn drop_response(&self, from: NodeId, to: NodeId, n: usize); // for M3 unknown-outcome
}
```

Requirements:

1. Directions are independent (one-way loss is a §20 gossip requirement and a real Raft
   hazard).
2. A blocked call must fail like a transport error (`Unreachable`), **not** hang forever, or
   the harness cannot distinguish a partition from a deadlock.
3. Blocking must take effect for calls **already in flight** at the next await point, not only
   for new connections — otherwise M1-10 (isolated former leader) races.
4. The same handle must be reachable from the test as `cluster.netfault()`; `cluster.partition`
   / `cluster.heal` are thin wrappers.

### TA-6 — `ConfigNode` exposes deadline-pollable state

No test may sleep to wait for consensus (ADR-0014). `ConfigNode` must expose:

```rust
impl ConfigNode {
    pub async fn wait_for_leader(&self, deadline: Duration) -> Result<NodeId, Timeout>;
    pub async fn wait_applied(&self, index: u64, deadline: Duration) -> Result<(), Timeout>;
    pub fn metrics(&self) -> NodeMetrics;      // cheap, sync, snapshot
    pub fn applied_index(&self) -> u64;
    pub fn capabilities(&self) -> Capabilities;
    pub fn health(&self) -> Health;            // Ready | NotLeader | Unavailable | Fatal
    pub fn direct_client(&self, principal: Principal) -> DirectClient;
}
```

`NodeMetrics` must be a public, non-OpenRaft value type (ADR-0004) carrying at least:
`node_id`, `role`, `current_term`, `current_leader: Option<NodeId>`, `last_log_index`,
`last_applied`, `raft_log_len`, `membership_voter_ids: Vec<NodeId>`,
`membership_log_id: Option<LogIdView>`, `cluster_revision`.

`raft_log_len` and `membership_voter_ids` are load-bearing: M1-17/M1-18 prove the direct client
goes through Raft by asserting the log grew and every node applied; M1-22 proves gossip cannot
change membership by asserting `membership_voter_ids` and `membership_log_id` are byte-identical
before and after gossip abuse.

`wait_for_leader` must return the observed leader id, and must return `Timeout` rather than
panicking, so M1-01 can assert "no leader was ever elected" as a positive result.

### TA-7 — Gossip is injectable

`config-engine` must accept `Arc<dyn GossipObservationSource>` (ADR-0003, ADR-0004) so the
harness can supply `DisabledGossip`, `PoisonedGossip { wrong_cluster_id | wrong_node_id |
hijacked_endpoint }`, and the real memberlist adapter. Hint validation must be a pure function
`validate_hint(&ObservedPeerHint, &CommittedMembership, &ClusterIdentity) -> HintVerdict` so
M1-20..M1-26 can be table-driven without sockets, in addition to the end-to-end runs.

### TA-8 — Every test opens the test context span

`config-log` must provide `config_log::test_context!()` and `#[retcd_test]` (ADR-0013). The
macro must:

1. open a root span with `testModule`, `testMethod`, `testRun` (per-process UUID) and, where
   applicable, `testNode`;
2. install the JSONL appender to `target/test-logs/<testModule>/<testMethod>.jsonl`;
3. be safe to call once per test in a process where many tests run concurrently — fields must
   propagate to nodes spawned **inside** that test, including across `tokio::spawn`, so
   follower `apply` lines carry the test's `testMethod`;
4. flush on test end (including on panic) so the DuckDB assertions in §5 see complete files.

A workspace test asserts (1)–(4) empirically: see M1-35.

### TA-9 — Purity is machine-checked, not reviewed

`config-core`'s state-machine and command modules must contain no reference to `std::time`,
`std::env`, `std::fs`, `rand`, `SystemTime`, `Instant`, `HashMap`/`HashSet` iteration, or
`.iter()` over an unordered collection in an order-sensitive position (ADR-0007). This is
enforced by a source-scanning unit test (M0-42) plus a `Cargo.toml` dependency assertion
(M0-43), not by review.

### TA-10 — Conformance runs against the trait, not the impl

`config-testkit::conformance` must be `pub async fn run_all(store: Arc<dyn ConfigStore>, cfg:
ConformanceConfig) -> ConformanceReport`, with one scenario list (§4.3) and per-scenario
pass/fail. It must take the effective `Limits` in `ConformanceConfig` because truncation
scenarios depend on them. Both `DirectClient` and `GrpcClient` must be `ConfigStore`
implementors with no extra methods needed to make the suite pass.

### TA-11 — Isolation primitives

`Cluster` must bind all listeners on `127.0.0.1:0` (ephemeral), allocate any on-disk state under
a per-test temp dir, and release ports and delete temp dirs on `Drop` — including when the test
panics. `Cluster::start` must not read process-wide env vars or a repo-relative config file.

### TA-12 — `Capabilities` is an asserted value

`Capabilities` must be `PartialEq + Debug + Serialize` so M1-27 asserts the whole struct in one
comparison rather than six string greps.

---

## 2. Test taxonomy and budgets

| Layer | Runner | Fault tools | Per-test budget |
|---|---|---|---|
| M0 unit / table-driven | `cargo test -p config-core` | none | < 50 ms |
| M0 property (`proptest`) | `cargo test -p config-core` | none | < 5 s (256 cases default) |
| M0 fuzz round-trip | `cargo test` (bounded) + optional `cargo fuzz` | none | < 5 s in CI |
| M1 in-process cluster | `tests/m1_*.rs`, `#[tokio::test]` multi-thread | `NetFault`, gossip fakes | < 10 s |
| M1 conformance ×2 clients | `tests/m1_conformance.rs` | none | < 20 s total |

Hard ceiling: **no single test may exceed 30 s**; the whole M0+M1 suite must finish in under
5 minutes on the dev host (Windows Server 2022, ADR-0017). A test that needs longer is
mis-designed — it is sleeping.

---

## 3. M0 — deterministic state-machine laboratory

Pure Rust, no async, no Raft, no storage. Every row constructs a `KvState` directly and calls
`apply`. All rows use `Limits` values chosen for the test, per TA-3.

### 3.1 ADR-0006 CAS/outcome table — one row each

`Pre` is the state before the command. `Rev` column = effect on `cluster_revision`.

| ID | Name | Inputs | Expected | Rev | Proves |
|---|---|---|---|---|---|
| M0-01 | put_unconditional_absent | Pre: empty. `Put{k,v1, expected: None}` | `APPLIED`, `revision=1`, record `{create_rev:1, mod_rev:1}` | +1 | ADR-0006 r1; §7.3 Put |
| M0-02 | put_unconditional_present | Pre: `k@mod_rev=3`, `cluster_rev=3`. `Put{k,v2,None}` | `APPLIED`, `revision=4`, `create_rev` unchanged=1, `mod_rev=4` | +1 | ADR-0006 r1; ADR-0005 create/mod split |
| M0-03 | put_create_only_absent | Pre: `cluster_rev=5`, k absent. `Put{k,v,Some(0)}` | `APPLIED`, `revision=6`, `create_rev=mod_rev=6` | +1 | ADR-0006 r2 |
| M0-04 | put_create_only_present_conflict | Pre: `k@mod_rev=2`. `Put{k,v,Some(0)}` | `CONFLICT{exists:true, current_mod_revision:2}`, `revision = cluster_rev` (unchanged) | 0 | ADR-0006 r3; §19.3 |
| M0-05 | put_expected_match | Pre: `k@mod_rev=4`, `cluster_rev=4`. `Put{k,v2,Some(4)}` | `APPLIED`, `revision=5`, `mod_rev=5` | +1 | ADR-0006 r4 |
| M0-06 | put_expected_mismatch | Pre: `k@mod_rev=4`. `Put{k,v2,Some(3)}` | `CONFLICT{exists:true, current:4}` | 0 | ADR-0006 r5; §19.3 |
| M0-07 | put_expected_n_absent | Pre: k absent, `cluster_rev=9`. `Put{k,v,Some(7)}` | `CONFLICT{exists:false, current:0}` | 0 | ADR-0006 r6 (the easy-to-get-wrong row) |
| M0-08 | delete_unconditional_present | Pre: `k@mod_rev=2`, `cluster_rev=2`. `Delete{k,None}` | `APPLIED`, `revision=3`, key gone, tombstone `MutationEvent{rev:3,kind:Delete}` | +1 | ADR-0006 r7; §7.3 Delete |
| M0-09 | delete_unconditional_absent | Pre: empty, `cluster_rev=0`. `Delete{k,None}` | `NOT_FOUND`, `revision=0`, no event | 0 | ADR-0006 r8; §19.3 |
| M0-10 | delete_expected_zero_invalid_at_edge | `Delete{k, Some(0)}` through `validate_delete` | `Err(InvalidArgument)`; never encoded, never applied | n/a | ADR-0006 r9; §7.3 |
| M0-11 | delete_expected_match | Pre: `k@mod_rev=5`, `cluster_rev=5`. `Delete{k,Some(5)}` | `APPLIED`, `revision=6`, key gone | +1 | ADR-0006 r10 |
| M0-12 | delete_expected_mismatch_present | Pre: `k@mod_rev=5`. `Delete{k,Some(4)}` | `CONFLICT{exists:true, current:5}`, key still present | 0 | ADR-0006 r11 |
| M0-13 | delete_expected_positive_absent | Pre: k absent, `cluster_rev=9`. `Delete{k,Some(4)}` | `NOT_FOUND` (**not** `CONFLICT`), `revision=9` | 0 | ADR-0006 r12; §7.3 "missing key returns NOT_FOUND including when a positive expected revision was supplied" |
| M0-14 | delete_expected_zero_reaches_apply | Craft `Command::Delete{expected:Some(0)}` bytes directly and `apply` | deterministic rejection response (`InvalidArgument`-equivalent outcome per OQ-6), no panic, no revision | 0 | ADR-0006 "malformed entry that reached the log yields a deterministic rejection, never a panic" |
| M0-15 | cas_table_is_exhaustive | Table-driven runner over all 12 ADR-0006 rows in one `#[test]` with a compile-time count assertion (`ROWS.len() == 12`) | all rows pass; count matches | — | ADR-0006 completeness; prevents silent row loss when the ADR changes |

### 3.2 Revision allocation (ADR-0005, §19.3)

| ID | Name | Inputs | Expected | Proves |
|---|---|---|---|---|
| M0-16 | revision_starts_at_zero | fresh `KvState` | `cluster_revision == 0`; `Get` on empty store → `read_revision = 0` | ADR-0005 start value |
| M0-17 | revision_increments_by_exactly_one | 5 applied Puts on distinct keys | revisions `1,2,3,4,5`; `cluster_revision == 5` | ADR-0005 "+1 per state-changing mutation" |
| M0-18 | same_value_put_bumps_revision | Pre: `k=v@mod_rev=1`. `Put{k, v /*identical*/, None}` | `APPLIED`, `revision=2`, `mod_rev=2`, `create_rev=1` | §7.3 "a successful same-value Put is a state-changing mutation"; ADR-0005 |
| M0-19 | rejections_allocate_nothing | Sequence: Put(ok), Put(conflict), Delete(not_found), Delete(conflict), Put(ok) | revisions `1,_,_,_,2`; `cluster_revision == 2`; both rejected responses carry `revision == cluster_revision` at that moment (`1`) | ADR-0005 `MutationResponse.revision` rule; §19.3 |
| M0-20 | create_revision_resets_after_delete | Put k (rev1), Delete k (rev2), Put k (rev3) | final record `{create_rev:3, mod_rev:3}` | ADR-0005 "`create_revision` = revision of the Put that created it after absence" |
| M0-21 | create_revision_stable_across_updates | Put k (rev1), Put k (rev2), Put k (rev3) | `{create_rev:1, mod_rev:3}` | ADR-0005 |
| M0-22 | read_revision_on_get_and_list | After 3 mutations, `get` and `list` | both report `read_revision == 3`, and `get` of a missing key still reports `read_revision == 3` with `record: None` | §7.3 Get; ADR-0005 "every read response carries read_revision" |

### 3.3 List ordering, truncation, caps (§7.3, §10.2, ADR-0005)

| ID | Name | Inputs | Expected | Proves |
|---|---|---|---|---|
| M0-23 | list_bytewise_order_not_utf8 | keys `[0x41 "A", 0x5F "_", 0x61 "a", 0x7E "~", 0xFF]` inserted in shuffled order, prefix `b""` | returned order is `0x41,0x5F,0x61,0x7E,0xFF` — ascending **unsigned** bytewise | §7.1 "unsigned bytewise lexical order"; ADR-0005 |
| M0-24 | list_prefix_boundary_exactness | keys `a`, `ab`, `ab\xff`, `ac`, `b`; prefix `ab` | exactly `[ab, ab\xff]`; `a`, `ac`, `b` excluded | §7.3 List; catches a `>= prefix` scan with a missing upper bound |
| M0-25 | list_prefix_all_0xff_upper_bound | keys `\xff`, `\xff\x00`, `\xff\xff`; prefix `\xff` | all three returned, no overflow panic computing the exclusive upper bound | §7.3; the classic prefix-increment overflow bug |
| M0-26 | list_empty_prefix_returns_all | 10 keys, prefix `b""`, caps generous | all 10, ordered, `truncated=false` | §7.3 |
| M0-27 | list_truncated_by_max_items | 10 keys, `max_items=3`, `max_bytes` generous | first 3 in order, `truncated=true`, `read_revision` = current | §10.2; §6.2 `truncated` |
| M0-28 | list_truncated_by_max_bytes | 10 keys sized so 4 fit under `max_bytes`, `max_items=1000` | first 4, `truncated=true` | §10.2 byte cap |
| M0-29 | list_exact_fit_not_truncated | N keys that exactly consume `max_items` (and separately exactly `max_bytes`) | all N returned, `truncated=false` | off-by-one: exact fit must not set `truncated` |
| M0-30 | list_first_record_exceeds_max_bytes | one record larger than `max_bytes` | per OQ-3/OQ-5 decision — either 1 record + `truncated=true`, or 0 records + `truncated=true`; asserted against the locked rule | §10.2; blocks on OQ-3, OQ-5 |
| M0-31 | list_server_caps_clamp_request | `max_items=100_000`, `max_bytes=64 GiB` | clamped to `Limits` (1000 / 8 MiB per §7.1) rather than rejected; `truncated` set if clamping cut results | §7.1 caps; §10.2 "both capped by the server" |
| M0-32 | list_no_continuation_token | `ListResponse` type inspection + truncated response | no cursor/token field is populated or exposed | §2.3, §10.2 "no continuation token" |
| M0-33 | list_is_read_only | `state_hash()` before and after several `list` calls, incl. truncated | identical hash; `cluster_revision` unchanged | §7.3; reads allocate no revision |

### 3.4 Size caps (§7.1, ADR-0006)

| ID | Name | Inputs | Expected | Proves |
|---|---|---|---|---|
| M0-34 | key_cap_boundary | keys of 1024 and 1025 bytes through `validate_put` | 1024 → `Ok`; 1025 → `InvalidArgument` | §7.1 key 1 KiB |
| M0-35 | value_cap_boundary | values of 1 MiB and 1 MiB+1 | accept / `ResourceExhausted` (per OQ-4 mapping) | §7.1 value 1 MiB; §6.2 status table |
| M0-36 | request_cap_boundary | Put whose encoded envelope is 2 MiB and 2 MiB+1 | accept / `ResourceExhausted` | §7.1 request 2 MiB |
| M0-37 | empty_key_rejected | `Put{key: b"", ...}` | `InvalidArgument` (empty key is not a valid opaque key; locked by OQ-7) | §7.1; blocks on OQ-7 |
| M0-38 | empty_value_allowed | `Put{k, b""}` | `APPLIED`; `Get` returns a record with a zero-length value, distinct from absence | §7.1 opaque bytes; absence vs empty is a real bug class |
| M0-39 | oversize_reaching_apply_is_rejected | craft over-cap `Command` bytes and `apply` | deterministic rejection, no panic, no revision, `state_hash()` unchanged | ADR-0006 re-check in apply |

### 3.5 Command envelope — golden bytes, round-trip, fuzz (ADR-0007)

| ID | Name | Inputs | Expected | Proves |
|---|---|---|---|---|
| M0-40 | encode_golden_put_no_expected | `Put{key:b"a", value:b"b", expected:None}` | exact bytes `52 43 4D 44 01 00 01 01 00 00 00 61 01 00 00 00 62 00 00 00 00 00 00 00 00 00` (26 bytes) asserted as a hex literal | ADR-0007 layout, byte for byte |
| M0-41 | encode_golden_put_expected_7 | `Put{b"a", b"b", Some(7)}` | same as M0-40 with `has_expected=01` and `07 00 00 00 00 00 00 00` | ADR-0007 LE u64 |
| M0-42 | encode_golden_delete | `Delete{b"a", Some(7)}` | exact bytes per the OQ-1 decision; both candidate encodings are written in the test as the accepted/rejected pair | ADR-0007; blocks on OQ-1 |
| M0-43 | decode_rejects_bad_magic | `b"XCMD" + valid tail` | typed `DecodeError::BadMagic`, no panic | ADR-0007 |
| M0-44 | decode_rejects_unknown_version | version `0x0002` | `DecodeError::UnsupportedVersion(2)` | ADR-0007 "unknown version → typed decode error"; §17 |
| M0-45 | decode_rejects_unknown_op | `op = 0x03` | `DecodeError::UnknownOp(3)` | ADR-0007 |
| M0-46 | decode_rejects_truncated_and_overlong | for every prefix length `0..n` of a valid encoding, and for a valid encoding plus one trailing byte | every prefix → `DecodeError`; trailing byte → `DecodeError::TrailingBytes` (canonical encoding admits no slack) | ADR-0007 canonicality |
| M0-47 | decode_rejects_length_overflow | `key_len = u32::MAX` with a 20-byte buffer | `DecodeError`, no allocation of 4 GiB, no panic | ADR-0007; OOM/DoS guard |
| M0-48 | encode_decode_roundtrip_proptest | `proptest` over `Command` (op, key 0..1 KiB, value 0..64 KiB, expected `None`/`Some(0..u64::MAX)`) | `decode(encode(c)) == c` for all cases | ADR-0007 round-trip |
| M0-49 | decode_fuzz_never_panics | `proptest` over arbitrary `Vec<u8>` (0..4 KiB), plus a corpus of mutated valid encodings (bit flips, byte truncation, length-field tampering) | `decode` returns `Ok` or a typed `Err`; never panics, never hangs, never allocates beyond the input length bound | ADR-0007; §20 "oversized requests"; catches the `key_len` trust bug |
| M0-50 | encode_is_canonical_and_stable | same logical command built two ways (different `Bytes` backing, different construction order) | identical bytes; also `encode` output is byte-identical across two process runs (no `HashMap` ordering, no padding) | ADR-0007 "no dependence on serde container ordering"; §7.4 |
| M0-51 | serde_is_not_the_canonical_form | `serde_json`/`bincode` of `Command` vs `Command::encode()` | test documents and asserts they differ, and that **only** `encode()` is used for determinism assertions and (in M2) on-disk log bytes | ADR-0007 "canonical bytes come from `Command::encode()`" — prevents a later refactor from silently swapping them |

### 3.6 Replay determinism (§21 M0 bullet 1, ADR-0007, ADR-0014)

| ID | Name | Inputs | Expected | Proves |
|---|---|---|---|---|
| M0-52 | replay_determinism_proptest | `proptest` generates a random `Vec<Command>` (length 1..200) over a small key space (8 keys, so CAS hits and misses both occur frequently) and a mix of `expected` values including revisions harvested from earlier responses. Two fresh `KvState`s are fed the identical sequence. | `hash_a == hash_b`; `responses_a == responses_b` (whole vector equality); and `hash` equals a third run in a **separate** `KvState` built by cloning after each step | §21 M0 "replaying an identical command sequence yields byte-identical state and responses"; ADR-0014 layer 1 |
| M0-53 | replay_from_encoded_bytes | same as M0-52 but the second machine consumes `decode(encode(cmd))` rather than the original struct | identical hash and responses | §7.4 + ADR-0007: determinism must survive the wire form, not just the in-memory struct |
| M0-54 | replay_prefix_hash_sequence | record `state_hash()` after **every** command in the sequence; compare the full hash vectors of the two machines | vectors equal element-wise; on failure, the report names the first divergent index and the command at it | §21 M0; makes a determinism failure debuggable instead of "hashes differ" |
| M0-55 | empty_state_hash_golden | `KvState::new().state_hash()` | equals a documented 32-byte constant | TA-2; freezes the hash definition so an accidental field addition is caught |
| M0-56 | state_hash_sensitivity | hash of states differing only in: value bytes / `create_revision` / `mod_revision` / `cluster_revision` / key set / key order of insertion | all differ except insertion order, which must be identical | TA-2; prevents a hash too weak to detect divergence |
| M0-57 | apply_is_order_sensitive | apply `[Put(k,v1), Put(k,v2)]` vs `[Put(k,v2), Put(k,v1)]` | different final `state_hash()` | §19.4 "CAS is evaluated against state immediately preceding the command in committed apply order" — a hash insensitive to order would make M0-52 vacuous |

### 3.7 Purity (§7.4, §21 M0 bullet 4, ADR-0007)

| ID | Name | Inputs | Expected | Proves |
|---|---|---|---|---|
| M0-58 | source_scan_no_ambient_inputs | read `crates/config-core/src/**/*.rs` at test time (path derived from `CARGO_MANIFEST_DIR`) and scan for `std::time`, `SystemTime`, `Instant`, `std::env`, `env!`, `std::fs`, `rand`, `thread_rng`, `HashMap`, `HashSet`, `std::net`, `tokio`, `reqwest` | no match outside an explicit allowlist of comment/test lines; failure message names file and line | §21 M0 "apply uses no clock, randomness, environment lookup, unordered output, or external I/O"; ADR-0007 "checked by a unit test that scans the source" |
| M0-59 | dependency_surface_is_minimal | parse `crates/config-core/Cargo.toml` | dependencies ⊆ `{bytes, serde, thiserror, sha2}` (+ dev-deps `proptest`, `hex`); no `tokio`, `openraft`, `rocksdb`, `tonic`, `rand`, `chrono`, `time` | ADR-0004 "config-core has no async runtime or network deps" |
| M0-60 | apply_signature_is_sync_and_total | compile-time: a `const fn`-style assertion / trait-object test that `KvState::apply` is callable from a non-async `fn` with no runtime, and that `CommandResponse` is returned unconditionally | compiles and runs outside any runtime | TA-1.1, TA-1.2 |
| M0-61 | no_unordered_iteration_in_output | build a state via 200 keys inserted in randomized order (seeded, deterministic per run), then `list` and `state_hash` twice in the same process and compare | identical both times and equal to the sorted expectation | §7.4 "unordered iteration"; catches a `HashMap` that happens to be stable in one run |

### 3.8 Concurrent CAS, M0 form (§20, §21 M0 bullet 2)

| ID | Name | Inputs | Expected | Proves |
|---|---|---|---|---|
| M0-62 | exactly_one_of_n_cas_applies | Pre: `Put(k,v0)` → `mod_rev = r`. Snapshot the state (`KvState::clone`). Build **N = 8** commands `Put{k, v_i, Some(r)}` — each constructed from the *same* observed snapshot revision `r`, exactly as 8 independent clients would. Apply all 8 **sequentially** to the single state machine. | exactly one response is `APPLIED` (the first, deterministically), and it allocated `r+1`; the other 7 are `CONFLICT{exists:true, current_mod_revision: r+1}`; `cluster_revision == r+1`; final value is `v_0` | §20 "concurrent CAS where exactly one expected-revision mutation succeeds"; §21 M0 bullet 2; §19.4 |
| M0-63 | exactly_one_of_n_cas_create_only | same shape with `expected = Some(0)` on an absent key, N = 8 | exactly one `APPLIED` (create), 7 × `CONFLICT{exists:true, current: allocated_rev}` | ADR-0006 r2/r3 under contention |
| M0-64 | exactly_one_of_n_cas_delete | Pre: `k@mod_rev=r`. N = 8 `Delete{k, Some(r)}` | exactly one `APPLIED`; the other 7 are `NOT_FOUND` (key now absent — **not** `CONFLICT`) | ADR-0006 r12 under contention; a very common implementation error |
| M0-65 | cas_contention_proptest | `proptest` over N (2..16) and interleavings of competing CAS on 2 keys | invariant per key: `count(APPLIED with expected==r) <= 1` for each distinct `r`; revisions strictly increasing with no gaps across all applied mutations | §19.3, §19.4 generalized |
| M0-66 | state_hash_isolates_create_revision | two `from_parts` states identical except one `create_revision` | hashes differ | TA-2 (added after critic review: M0-56 did not isolate the field) |
| M0-67 | state_hash_length_prefixes_variable_fields | `{"ab"→""}` vs `{"a"→"b"}` at the same revision | hashes differ | TA-2 length prefixes |
| M0-68 | from_parts_preserves_limits | `with_limits(custom)` → `from_parts(custom, ..)` | `limits()` equal, hash equal, over-cap command rejected | replicated caps survive restart |
| M0-69 | apply_rejects_at_revision_exhaustion | `from_parts(.., u64::MAX, ..)` then Put/Delete | `Rejected`, revision and hash unchanged, no panic | TA-1.1 |
| M0-70 | allowlist_denies_unverified_principal_kind | `Development` principal named like a grant | `Deny`; `Certificate` with same name → `Allow` | ADR-0012 defense in depth |
| M0-71 | default_limits_match_spec | `Limits::DEFAULT` vs spelled-out §7.1 literals | equal | caps typo cannot ship green |

---

## 4. M1 — three-node in-process cluster (Ephemeral storage)

### 4.1 Required harness API

`config-testkit` must provide exactly this surface. Tests may not reach past it into
OpenRaft or tonic types (ADR-0004).

```rust
pub enum StorageKind { Ephemeral, Rocks }          // Rocks unused until M2
pub enum GossipKind {
    Real,                                          // encrypted memberlist, ephemeral port
    Disabled,
    Poisoned(PoisonSpec),                          // WrongClusterId | WrongNodeId | HijackedEndpoint
    Partitioned,                                   // gossip transport blocked, Raft untouched
}

pub struct ClusterConfig {
    pub nodes: u64,                    // 3 for M1
    pub storage: StorageKind,
    pub gossip: GossipKind,
    pub form: bool,                    // default true; false => no form_cluster call
    pub limits: Limits,
    pub tls: TlsMode,                  // Insecure for M1; MutualTls in M3
}

impl Cluster {
    // Construction
    pub async fn start(n: u64, storage: StorageKind) -> Cluster;   // forms the 3-voter group
    pub async fn start_with(cfg: ClusterConfig) -> Cluster;        // for no-form / gossip variants

    // Discovery (all deadline-bounded, never sleeping)
    pub async fn leader(&self) -> NodeId;                          // panics on timeout
    pub async fn try_leader(&self, deadline: Duration) -> Option<NodeId>;
    pub fn followers(&self) -> Vec<NodeId>;
    pub fn node(&self, id: NodeId) -> &ConfigNode;
    pub fn metrics(&self, id: NodeId) -> NodeMetrics;
    pub fn membership(&self) -> MembershipView;   // voter ids + membership log id, from node 1
    pub fn membership_of(&self, id: NodeId) -> MembershipView;
    pub async fn wait_applied_all(&self, index: u64, deadline: Duration) -> Result<(), Timeout>;

    // Clients
    pub fn client(&self, id: NodeId) -> DirectClient;              // Arc<dyn ConfigStore>
    pub async fn grpc_client(&self, id: NodeId) -> GrpcClient;     // pinned to one node
    pub async fn grpc_client_multi(&self) -> GrpcClient;           // all endpoints, follows hints

    // Faults
    pub fn partition(&self, a: NodeId, b: NodeId);                 // symmetric block
    pub fn partition_one_way(&self, from: NodeId, to: NodeId);
    pub fn isolate(&self, id: NodeId);                             // block id <-> every peer
    pub fn heal(&self);
    pub fn netfault(&self) -> NetFault;
    pub fn gossip(&self) -> GossipControl;                         // inject/poison/stop hints

    // Lifecycle
    pub async fn stop(&self, id: NodeId);                          // graceful shutdown
    pub async fn start_node(&self, id: NodeId);                    // restart same id/endpoint
    pub async fn shutdown(self);
}
```

Notes that are requirements, not commentary:

- `Cluster::start(3, Ephemeral)` must return only after a leader exists and all three nodes
  report the same `membership_voter_ids`; otherwise every test re-implements formation waiting.
- `start_node` must reuse the same `NodeId` and the same listener port as before `stop`, so
  restart tests do not need to rewrite peer config.
- `client()` and `grpc_client()` must both yield `Arc<dyn ConfigStore>` for §4.3.
- Method name: the task brief says `cluster.start(node)`; that collides with the associated
  constructor `Cluster::start`. This plan names the instance method **`start_node`**. If the
  Developer prefers `Cluster::spawn(n, ..)` + `cluster.start(node)`, that is acceptable — the
  requirement is that both operations exist and are distinguishable.

### 4.2 M1 test cases

| ID | Name | Inputs / actions | Expected outcome | Proves (§21 M1 bullet / ADR) |
|---|---|---|---|---|
| **No self-form** | | | | |
| M1-01 | empty_node_never_self_forms | `start_with(ClusterConfig{nodes:1, form:false, ..})`; poll `try_leader` for 10 × election timeout | `None` throughout; `metrics().role` stays `Learner`/`Idle`; `current_leader == None`; `last_log_index == 0` | §21 M1 "no empty node self-forms"; ADR-0011 "no leader after 10 election timeouts" |
| M1-02 | three_empty_nodes_never_self_form | `nodes:3, form:false`, peers reachable, 10 × election timeout | no leader on any node; no log entries anywhere; `membership_voter_ids` empty on all three | §13.1 "an empty data directory never self-forms"; §19.8 |
| M1-03 | unformed_node_serves_unavailable | on the unformed cluster, `client(1).get(k)` and `.put(k,v)` | both → `ConfigError::Unavailable` (not `NotLeader`, not a hang, not success) | ADR-0011 "an empty node without this call stays idle and serves `Unavailable`" |
| M1-04 | explicit_formation_succeeds | `form_cluster(FormationPlan{3 voters})` on node 1 only | a leader appears within the deadline; all three report identical `membership_voter_ids == {1,2,3}` and identical `membership_log_id` | §13.1; §21 M1 scope bullet 1 |
| M1-05 | double_formation_is_rejected | call `form_cluster` twice (and once on a second node) | second call → typed error; membership and leader unchanged; no second term churn | §13.1; ADR-0011 "succeeds only if the local store is fresh" |
| M1-06 | formation_requires_matching_identity | `FormationPlan` whose `cluster_id` differs from the node's identity | `IdentityMismatch` before Raft starts; no log entries | ADR-0011; §19.10 |
| **One stopped voter still commits** | | | | |
| M1-07 | write_commits_with_one_voter_stopped | `start(3)`; `stop(follower_a)`; then 5 × `put` via leader's direct client | all 5 → `APPLIED` with revisions `1..5`; `wait_applied_all` on the two live nodes succeeds; the stopped node is not required | §21 M1 "one stopped voter does not stop committed writes"; §2.1 |
| M1-08 | read_succeeds_with_one_voter_stopped | same; then `get`/`list` on the leader | success with `read_revision == 5` | §10.1 quorum of 2 of 3 suffices |
| M1-09 | stopped_voter_catches_up_on_restart | `start_node(follower_a)`; `wait_applied_all(5, deadline)` | all three reach `last_applied == 5`; `state`-visible values match on all three (via each node's leader-independent internal read used by the harness) | §21 M1; §9.3.6 replay path exercised even on Ephemeral |
| M1-10 | leader_stop_elects_new_leader | `stop(leader)`; poll `try_leader` on the remaining two | a new leader (one of the survivors) within the deadline; previously acknowledged revisions still readable; no revision reuse | §19.1, §19.3; leader-change path |
| **Isolated / former leader** | | | | |
| M1-11 | isolated_former_leader_rejects_strict_read | `start(3)`; `L = leader()`; `isolate(L)`; then `client(L).get(k)` | `ConfigError::Unavailable` (retryable), never a successful read, never stale data | §21 M1 "an isolated/former leader rejects successful strict reads"; §10.1; ADR-0009 |
| M1-12 | isolated_former_leader_rejects_write | on the isolated `L`, `put(k,v)` | `Unavailable` or `DeadlineExceededUnknownOutcome`; **never** `APPLIED`; after `heal`, `get(k)` shows the write was not applied on any node (`cluster_revision` unchanged) | §21 M1; §19.1 "only a quorum-committed entry changes authoritative configuration" |
| M1-13 | isolated_former_leader_list_rejected | on isolated `L`, `list(prefix)` | `Unavailable`; no partial result | §10.1 (List is leader-linearized too — easy to forget) |
| M1-14 | surviving_majority_still_serves | while `L` is isolated, the two survivors elect a new leader; `put` + `get` on the new leader | success; revision continues from the last committed value with no gap and no reuse | §19.1, §19.3, §19.11 |
| M1-15 | heal_reconverges_old_leader | `heal()`; `wait_applied_all(latest, deadline)` | old `L` steps down, rejoins as follower, `last_applied` catches up, `state_hash`-equivalent state on all three; `metrics().current_term` on old `L` ≥ its pre-isolation term | §19.11 "cannot leave two writable authorities"; convergence |
| M1-16 | every_pair_partition_matrix | table-driven over `{(1,2),(1,3),(2,3)}` symmetric partitions and the three `isolate(n)` cases | in every arrangement: at most one node ever serves a successful strict read/write; the minority side returns `Unavailable`/`NotLeader`; after `heal` all nodes converge to one identical state | §20 "every three-node partition arrangement"; §19.11 |
| **Follower NotLeader + hint** | | | | |
| M1-17 | follower_get_returns_not_leader_with_hint | `start(3)`; pick `f` in `followers()`; `client(f).get(k)` | `ConfigError::NotLeader{ hint }` where `hint.node_id == leader()` and `hint.endpoint` equals the **committed membership** endpoint for that id | §21 M1; ADR-0009 "the hint is the committed membership endpoint, never a gossip endpoint" |
| M1-18 | follower_put_returns_not_leader_with_hint | `client(f).put(k,v)` | same `NotLeader{hint}`; the command **never enters the log** (`metrics(f).last_log_index` and the leader's `raft_log_len` unchanged) | ADR-0009; ADR-0015 "rejected before entering the log" |
| M1-19 | hint_is_not_a_gossip_endpoint | inject a `PoisonSpec::HijackedEndpoint` hint for the leader id, then trigger `NotLeader` on a follower | the returned hint still carries the committed endpoint, not the gossip-advertised one; a `warn` line records the mismatch | ADR-0003, ADR-0009; §5.3; §19.9 |
| M1-20 | unknown_leader_returns_unavailable | isolate a follower `f` (so it knows no leader), then `client(f).get(k)` | `Unavailable`, not `NotLeader{hint: garbage}` | ADR-0009 "if the leader is unknown → Unavailable" |
| M1-21 | grpc_client_follows_hint_bounded | `grpc_client_multi()` pinned first to a follower; `put` | succeeds by following the hint; hint-follow count ≤ 3 (default N); with all nodes returning `NotLeader` (forced), the client returns the last error after ≤ 3 attempts and does not loop | ADR-0009 "at most N (default 3)"; ADR-0015 bounded retries |
| M1-22 | direct_client_does_not_follow_hints | `client(f)` (a follower) `put` | `NotLeader` returned to the embedder; no internal forwarding | ADR-0009 "DirectClient returns NotLeader to the embedder" |
| **Direct client goes through Raft** | | | | |
| M1-23 | direct_write_advances_applied_index_on_all_nodes | `start(3)`; record `before_i = metrics(i).last_applied` and `before_log_i = metrics(i).raft_log_len` for i in 1..3; `client(leader).put(k,v)` → `APPLIED{revision}`; `wait_applied_all(leader.last_applied, deadline)` | `metrics(i).last_applied > before_i` for **all three** nodes; `metrics(i).raft_log_len > before_log_i` for all three; the applied index advanced by exactly 1 entry on each | §21 M1 "direct access demonstrably uses the same Raft path"; §6.1 "a direct client does not bypass Raft" |
| M1-24 | direct_write_visible_via_grpc_on_other_nodes | write with `client(leader)`; then `grpc_client(other).get(k)` following the hint | returns the same record with the same `mod_revision` | §6.1 identical semantics |
| M1-25 | direct_read_uses_linearizable_barrier | `partition` the leader from both peers **after** the read call is issued (using `netfault().block_pair` from a second task); the in-flight `client(leader).get` | resolves to `Unavailable`, never to a locally-read stale success | §10.1 `ensure_linearizable`; ADR-0009; §19.2 |
| M1-26 | direct_write_log_growth_is_one_entry_per_mutation | 10 sequential `put`s | leader `raft_log_len` grows by exactly 10 (plus at most the formation/blank entries recorded before the loop); `cluster_revision == 10` | §7.2 "the public cluster revision is not the raw Raft log index" — measured, not assumed |
| M1-27 | rejected_mutation_creates_no_log_entry | a `Delete{expected:0}` (edge-invalid) and an over-cap `Put` through `client(leader)` | `InvalidArgument`/`ResourceExhausted`; `raft_log_len` unchanged on all nodes; `cluster_revision` unchanged | ADR-0006 "rejected at the API edge, never enters the log"; §19.3 |
| **Gossip cannot confer authority** | | | | |
| M1-28 | cluster_forms_and_works_with_gossip_disabled | `start_with(gossip: Disabled)`; full conformance smoke (put/get/list/delete) | identical behavior to `GossipKind::Real`; leader elected within the same deadline | §21 M1 "disabled gossip cannot alter … static-cluster liveness"; ADR-0003 "Raft must form and progress with gossip disabled" |
| M1-29 | gossip_partition_does_not_affect_raft | `gossip: Partitioned` (gossip transport blocked, Raft untouched) mid-test | leader unchanged (`current_leader` and `current_term` stable); writes keep committing; `membership_log_id` unchanged | §21 M1; §5.3; §19.9 |
| M1-30 | poisoned_gossip_wrong_cluster_id_rejected | inject `ObservedPeerHint{cluster_id: other}` | hint rejected by `validate_hint`; a `warn` log line records the mismatch; Raft transport never dials the endpoint | ADR-0003; ADR-0011; §5.3 |
| M1-31 | poisoned_gossip_wrong_node_id_rejected | inject a hint whose `node_id` is not in committed membership (e.g. 99) | rejected; `membership_voter_ids` unchanged | ADR-0003 verification bullet; §19.9 |
| M1-32 | poisoned_gossip_hijacked_endpoint_not_used | inject a hint for node 2 pointing at node 3's port (or a dead port) | Raft peer transport for node 2 continues using the committed endpoint; replication to node 2 never breaks; endpoint-mismatch metric/log increments | §5.3; ADR-0003 "the engine consumes hints only as candidate endpoints that must pass mTLS identity plus cluster/node id binding" |
| M1-33 | gossip_cannot_change_membership | capture `membership()` (voter ids + membership log id) before; then apply all poison specs, stop gossip on one node, and inject a `dead` observation for the leader | `membership()` byte-identical before and after on all three nodes; no membership log entry appended (`raft_log_len` delta from membership = 0) | §21 M1; ADR-0003 "`membership_config` before == after"; §19.8, §19.9 |
| M1-34 | gossip_dead_observation_cannot_remove_or_demote_leader | inject `dead` for the current leader while it is healthy | leader and term unchanged; only telemetry emitted (assert a `warn`/`info` line, and no state change) | §5.3 "a `dead` observation produces telemetry … never removes a voter"; §19.9 |
| M1-35 | gossip_cannot_change_data | write `k=v1`; then inject hints carrying every field gossip is allowed to carry (§5.2) | `get(k)` still `v1`; `cluster_revision` unchanged | §21 M1 "cannot alter … data"; §5.2 "do not gossip configuration values" |
| M1-36 | no_memberlist_types_cross_the_boundary | inspect the public API of `config-gossip` (doc/`cargo public-api`-style check or a compile test that names only `GossipObservationSource` / `ObservedPeerHint`) | no `memberlist` type is nameable from outside | ADR-0003; ADR-0004 |
| **Capabilities** | | | | |
| M1-37 | capabilities_exact_values_m1 | `cluster.node(1).capabilities()` on the ephemeral, allow-all node | exactly `Capabilities{ durability: Ephemeral, watch_resumption: Unsupported, authz: Development, transport_security: Insecure, pagination: Unsupported, dedup: Unsupported }` — one struct equality assertion | §21 M1 "capability output states `durability=Ephemeral`, `watch_resumption=Unsupported`, `authz=Development`"; ADR-0016 |
| M1-38 | capabilities_identical_on_all_nodes_and_in_health | compare `capabilities()` across all three nodes and against the `health()` payload | all identical | ADR-0016 "exposed via `capabilities()` … and in the health payload" |
| M1-39 | ephemeral_store_never_reports_persistent | assert `EphemeralStore` cannot produce `durability: Persistent` or `PersistentUnverified` | type-level or exhaustive assertion | ADR-0016 |
| **Lifecycle / embedding** | | | | |
| M1-40 | node_lifecycle_configure_start_client_health_stop | build → `start` → `direct_client` → `health()` → `stop` | `health()` is `Ready` on the leader and `NotLeader` on followers while running; `stop` completes within the deadline; after `stop`, client calls return a typed error rather than hanging | §21 M1 "thin full-node embedding lifecycle"; §6.3 |
| M1-41 | library_creates_no_global_runtime | start a `ConfigNode` from inside a caller-owned `current_thread` runtime and from a multi-thread runtime | works in both; no second runtime is created (assert via a runtime-handle check) | §6.3 "must not silently create a global Tokio runtime"; ADR-0004 |
| M1-42 | graceful_stop_is_not_a_data_loss_event | `put` 5 keys, `stop(leader)` gracefully, elect a new leader | all 5 acknowledged revisions remain readable on the survivors | §19.1; note: Ephemeral storage means a **restarted** node starts empty — that is expected in M1 and is why durability is an M2 gate (see M1-43) |
| M1-43 | ephemeral_restart_loses_local_state_by_design | `stop(follower)`, `start_node(follower)` | the node starts empty and is re-replicated from the leader; the test asserts this is the *documented* Ephemeral behavior so nobody mistakes M1 for durability | §21 M1 scope "in-memory OpenRaft storage, unmistakably marked `Ephemeral`"; ADR-0008 |
| **Conformance parity** | | | | |
| M1-44 | conformance_direct_client | `conformance::run_all(cluster.client(leader), cfg)` | all scenarios in §4.3 pass | §21 M3 "direct and gRPC clients pass the same semantic conformance suite" (suite lands in M1 per ADR-0014 layer 3) |
| M1-45 | conformance_grpc_client | `conformance::run_all(cluster.grpc_client_multi(), cfg)` (insecure transport in M1) | all scenarios pass, with the same expected values as M1-44 | §6.1, §6.2 status mapping; ADR-0014 layer 3 |
| M1-46 | conformance_reports_are_identical | run both, compare the two `ConformanceReport`s scenario-by-scenario, including returned revisions and outcomes | reports equal (modulo transport-only fields); any difference names the scenario | §21 M1 scope "thin gRPC adapter and Rust `DirectClient` with identical semantics" |
| **Log-based** | | | | |
| M1-47 | trace_id_spans_leader_and_both_followers | see §5, query Q1 | ≥ 1 leader `client_write` line and ≥ 1 `apply` line per follower share one `trace_id` | ADR-0013 verification bullet |
| M1-48 | every_log_line_carries_test_context | see §5, query Q2 | zero rows missing `testModule`/`testMethod`/`testRun` | ADR-0013; ADR-0014 "every test starts with the test-context macro" |
| M1-49 | logs_are_redacted | see §5, query Q3 | no `value` field anywhere; `key_hex` ≤ 64 hex chars; no credential-shaped field | §15.2; ADR-0013 |

### 4.3 Conformance scenario list

One list, run against any `Arc<dyn ConfigStore>` (TA-10). Scenario IDs are stable and appear in
the report. Executed by M1-44/M1-45/M1-46 and re-executed unchanged in M3 over mTLS.

| ID | Scenario | Expected |
|---|---|---|
| C-01 | get-missing | `GetResponse{record: None, read_revision: <current>}`; **not** an error |
| C-02 | put-get | `Put{k,v}` → `APPLIED{revision: r}`; `Get{k}` → record with `value == v`, `create_revision == mod_revision == r`, `read_revision >= r` |
| C-03 | put-same-value-bumps-rev | `Put{k,v}` twice with identical bytes → second is `APPLIED{revision: r2 > r}`; `Get` shows `mod_revision == r2`, `create_revision == r` |
| C-04 | cas-create-only | `Put{k,v,expected:0}` on absent key → `APPLIED`; repeat → `CONFLICT{exists:true, current_mod_revision: r}` |
| C-05 | cas-conflict | `Put{k,v2,expected: r-1}` on `k@mod_rev=r` → `CONFLICT{exists:true, current: r}`; `Get` shows the value unchanged |
| C-06 | delete-missing-not-found | `Delete{absent_k, None}` → `NOT_FOUND`, `revision == current cluster_revision`, no revision allocated (verified by a following read) |
| C-07 | delete-cas-mismatch | `Delete{k, Some(wrong_positive)}` on present `k` → `CONFLICT{exists:true, current}`; key still present |
| C-08 | list-ordering | insert keys out of order incl. bytes > 0x7F → `List{prefix}` returns unsigned-bytewise ascending order, `truncated=false` |
| C-09 | list-truncated-by-max-items | `max_items` smaller than the match count → that many records, in order, `truncated=true` |
| C-10 | list-truncated-by-max-bytes | `max_bytes` smaller than the total → prefix of the ordered results, `truncated=true` |
| C-11 | invalid-delete-expected-zero | `Delete{k, Some(0)}` → `InvalidArgument` / gRPC `INVALID_ARGUMENT`; no state change |
| C-12 | oversize-key-and-value | key > 1 KiB → `InvalidArgument`; value > 1 MiB → `ResourceExhausted` (per the §6.2 status table and OQ-4); no state change |
| C-13 | read-revision-monotonic | interleave 10 mutations and 10 reads on one client against one leader → the observed `read_revision` sequence is non-decreasing, and strictly increases across each `APPLIED` mutation |
| C-14 | conflict-does-not-leak-value | trigger `CONFLICT` on a key whose value is a known sentinel → response contains only `exists` and `current_mod_revision`; the sentinel never appears in the response bytes | §7.3 "returning a value requires independent read permission" |
| C-15 | empty-value-is-not-absence | `Put{k, b""}` → `Get{k}` returns a present record with a zero-length value, distinguishable from C-01 | §7.1 |

C-14 and C-15 are additions to the brief's list; they close real leakage/absence bugs and cost
nothing to run. Keep them.

---

## 5. Log-based assertions (DuckDB over `target/test-logs/**/*.jsonl`)

> Field names follow the shipped `config-log` layer (CLEF style, ADR-0013): `@t` timestamp,
> `@l` level (`Trace|Debug|Information|Warning|Error`), `@logger` Rust target, `@m` message.
> Quote them in DuckDB: `"@m" = 'client_write'`.

ADR-0013 makes JSONL the debugging substrate; these queries make it an **assertion**
substrate. Each query runs after the test's tracing appender has flushed (TA-8.4). A helper
`config_testkit::logq::query(sql) -> RecordBatch` shells out to DuckDB (or uses `duckdb-rs`)
and the test asserts on the rows. A test must fail if the query returns **no** rows when rows
are expected — an empty result is the most likely failure mode and must not read as a pass.

### Q1 — distributed trace correlation (test M1-47)

Proves one direct-client write on the leader is visible as an `apply` on **both** followers
under the same `trace_id`, i.e. the direct client really went through Raft (§21 M1) and
cross-wire propagation works (ADR-0013).

```sql
WITH lines AS (
  SELECT * FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
  WHERE testMethod = 'm1_47_trace_id_spans_leader_and_both_followers'
),
w AS (  -- the leader's client_write span
  SELECT trace_id, node_id AS leader_id
  FROM lines WHERE op = 'put' AND "@m" = 'client_write' AND role = 'leader'
),
a AS (  -- apply spans, per node, for that trace
  SELECT l.trace_id, l.node_id, count(*) AS apply_lines
  FROM lines l JOIN w ON l.trace_id = w.trace_id
  WHERE l.op = 'apply'
  GROUP BY 1, 2
)
SELECT w.trace_id,
       w.leader_id,
       count(DISTINCT a.node_id)                                   AS nodes_that_applied,
       list(DISTINCT a.node_id)                                    AS node_ids,
       count(DISTINCT a.node_id) FILTER (WHERE a.node_id <> w.leader_id) AS followers_that_applied
FROM w LEFT JOIN a ON a.trace_id = w.trace_id
GROUP BY 1, 2;
```

**Assertion:** exactly one row; `nodes_that_applied == 3`; `followers_that_applied == 2`;
`node_ids` is a permutation of `{1,2,3}`.

### Q2 — every line carries test context (test M1-48)

Proves TA-8 and the ADR-0014 rule, including for lines emitted from nodes spawned inside the
test and from `tokio::spawn`ed Raft tasks.

```sql
SELECT coalesce(testModule, '<null>') AS m,
       coalesce(testMethod, '<null>') AS t,
       coalesce(testRun,    '<null>') AS r,
       "@logger", "@l", "@m", count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testModule IS NULL OR testMethod IS NULL OR testRun IS NULL
   OR node_id IS NULL AND "@logger" LIKE 'config_engine%'   -- node-scoped lines must carry node_id
GROUP BY ALL
ORDER BY n DESC;
```

**Assertion:** zero rows. Companion assertion in the same test: the per-method file
`target/test-logs/<module>/<method>.jsonl` exists for every executed M1 test and is non-empty,
and its distinct `testMethod` count is exactly 1 (no cross-test bleed).

### Q3 — redaction (test M1-49)

Proves §15.2 / ADR-0013: values and credentials never reach logs and keys are truncated hex.

```sql
SELECT *
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE
     -- a raw value field must not exist at all
     ("value" IS NOT NULL)
     -- keys only as bounded hex
  OR (key_hex IS NOT NULL AND (length(key_hex) > 64 OR NOT regexp_matches(key_hex, '^[0-9a-f]*$')))
     -- the sentinel value bytes used by the test must appear nowhere, in any field
  OR (to_json(COLUMNS(*))::VARCHAR ILIKE '%SENSITIVE_SENTINEL_VALUE%')
     -- credential-shaped fields
  OR (lower(coalesce("@m",'')) SIMILAR TO '%(private_key|password|bearer |-----begin)%');
```

**Assertion:** zero rows. The test writes a value containing `SENSITIVE_SENTINEL_VALUE` so a
non-empty result is a genuine leak, not a theoretical one. (If the `COLUMNS(*)` form is awkward
on the pinned DuckDB version, substitute a raw-text scan: `read_text('target/test-logs/**/*.jsonl')`
and `ILIKE`.)

### Q4 — no self-form has no leadership evidence (test M1-01/M1-02)

Log-side corroboration of the metrics assertion: a node that never formed must never have
logged a leader-elected event or a vote it granted to itself.

```sql
SELECT node_id, "@m", raft_term, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod IN ('m1_01_empty_node_never_self_forms',
                     'm1_02_three_empty_nodes_never_self_form')
  AND (role = 'leader' OR "@m" IN ('leader_elected','became_leader','initialize','vote_granted'))
GROUP BY ALL;
```

**Assertion:** zero rows.

### Q5 — gossip never precedes a membership change (test M1-33)

Proves §19.9 causally, not just by before/after equality: no membership log entry exists at all
in a test where gossip was poisoned.

```sql
SELECT node_id, "@m", log_index, raft_term
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = 'm1_33_gossip_cannot_change_membership'
  AND ("@m" ILIKE '%membership%' AND "@l" IN ('Information','Warning','Error'))
  AND "@m" NOT ILIKE '%formation%'
ORDER BY node_id, log_index;
```

**Assertion:** zero rows after the formation phase. Companion: the same test asserts
≥ 1 `warn` line with `"@m" = 'gossip_hint_rejected'` per poison spec — proving rejection
happened rather than the hint never arriving (the silent-pass trap).

### Q6 — isolated leader logged a barrier failure, not a local read (test M1-11)

```sql
SELECT node_id, role, op, "@m", count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = 'm1_11_isolated_former_leader_rejects_strict_read'
  AND op = 'get'
GROUP BY ALL;
```

**Assertion:** ≥ 1 row with `msg` indicating `ensure_linearizable` failure / `Unavailable`
returned; **zero** rows with `"@m" = 'read_served'` (or the equivalent success event) from the
isolated node after the partition timestamp. A time-bounded variant adds
`AND "@t" > '<partition_ts>'`, where the test records `partition_ts` and passes it into the query.

### DuckDB availability note

The `duckdb` MCP server failed to connect in the authoring session. Log assertions must
therefore not depend on an MCP tool: `config-testkit::logq` must use the in-process
`duckdb-rs` crate (dev-dependency) or a vendored DuckDB CLI path resolved from config, and
must **skip with an explicit `eprintln!` + test failure** — not a silent pass — if DuckDB is
unavailable in CI. Decide via OQ-8.

---

## 6. Anti-flake rules

Normative. A test violating any of these is rejected in review.

1. **No fixed sleeps.** `tokio::time::sleep`, `thread::sleep`, and `yield_now` loops are
   banned as synchronization. Enforced by a source-scan test over `tests/**` and
   `crates/*/tests/**` (same mechanism as M0-58), allowlisting only sleeps that are the
   *subject* of a test (e.g. an explicit `NetFault::delay`).
2. **Poll with deadlines.** Every wait is `wait_for_leader(d)` / `wait_applied(i, d)` /
   `poll_until(pred, d, interval)`. Default deadline 5 s; 15 s for election-after-partition;
   never above 30 s. A timeout must fail with the last observed `NodeMetrics` of every node in
   the message, or the failure is undiagnosable.
3. **Deadlines are derived, not literal.** Waits are expressed as multiples of the harness's
   configured election timeout (e.g. `10 * election_timeout`), so a CI machine 3× slower is
   handled by one config change, not 40 edits.
4. **Ephemeral ports only.** All listeners bind `127.0.0.1:0` (Raft peer, client gRPC, gossip).
   No literal port anywhere in `tests/**`. Enforced by the same source scan.
5. **Per-test temp dirs.** Any on-disk state goes under a `tempfile::TempDir` created inside
   the test; deleted on `Drop`, including on panic. No shared `target/tmp`.
6. **No shared mutable global state.** No process env mutation (`std::env::set_var`) — it races
   across concurrently running tests in the same binary. Config is passed as values (TA-3,
   TA-11).
7. **Seeded randomness only.** `proptest` uses its own deterministic seed; any ad-hoc random
   data uses a fixed seed printed in the failure message so a failure is reproducible.
8. **One cluster per test.** No cross-test cluster reuse or `OnceCell` cluster. Tests must pass
   under `cargo test -- --test-threads=1` **and** the default parallelism, and under
   `--test-threads=1 --shuffle`.
9. **Budgets.** Per-test ≤ 30 s hard, ≤ 10 s target for M1, ≤ 50 ms for M0 unit rows. Whole
   M0+M1 suite ≤ 5 min. CI fails the build on a per-test timeout rather than hanging.
10. **Assert on observable state, not on timing.** Never "after 200 ms the leader should have
    changed". Always "poll until `current_leader != old_leader` or deadline".
11. **Empty evidence is a failure.** Any assertion over logs, metrics lists, or conformance
    reports must assert a positive row/element count first. `assert!(rows.iter().all(...))`
    over an empty vector is a banned pattern.
12. **Flake quarantine is time-boxed.** A test that fails intermittently is fixed or deleted
    within one working iteration; `#[ignore]` requires a linked issue in the attribute comment.
    No permanent `#[ignore]`.
13. **Every test opens the context macro** (TA-8) and its name begins with its plan ID
    (`m0_07_…`, `m1_23_…`), so §5's queries and the gate mapping in ADR-0014 §6 both work by
    string match.

---

## 7. Durability testing roadmap (headings only — to be expanded)

### M2 — persistence and restart correctness: crash boundaries

- `FaultInjector::Crash` at each ADR-0008 boundary: `BeforeVoteSync`, `AfterVoteSync`,
  `BeforeLogAppend`, `AfterLogAppend`, `BeforeStateBatch`, `AfterStateBatch` — one gate test
  per boundary, per node role (leader / follower).
- Acknowledged-mutation survival: every mutation that returned `APPLIED` is readable after an
  ordinary stop/restart of each node in turn, and after a crash at each boundary.
- Committed-but-unapplied replay: no duplicate public revision, no gap, `last_applied` and
  `cluster_revision` consistent — assert equal `state_hash()` across all three nodes after
  replay (TA-2 is what makes this a one-line assertion).
- Log integrity: no holes (`index == last_index + 1` invariant), no vote regression across
  crash/restart cycles; assert directly over the `raft_log` / `raft_meta` CFs.
- Storage-fatal behavior: injected I/O error, ENOSPC, and corruption make the node
  `Fatal`/unready, all client calls return `FatalStorage`, and the process does not continue
  optimistically (§9.3.7).
- Identity binding: `state_meta/identity` mismatch (wrong cluster id, epoch, or node id) fails
  `open()` before Raft starts (ADR-0011); a data dir cannot be attached to a different cluster.
- Capability transition: `durability` reports `PersistentUnverified` until the M2 gate suite
  passes, then `Persistent` (ADR-0016) — asserted by the gate suite itself.

### M3 — identity, authorization, and unknown outcome

- mTLS identity rejection matrix: wrong cluster id, wrong node id, wrong destination binding,
  wrong client profile, expired cert — each rejected and audit-logged (ADR-0010, ADR-0011).
- Static allowlist: unlisted principal denied; listed principal denied outside its granted
  prefixes; missing/invalid policy fails closed (ADR-0012, §15.2).
- Unknown outcome: harness drops the response of a **committed** Put →
  `DeadlineExceededUnknownOutcome`; a following `Get` shows exactly one revision allocated and
  server-side apply count is one; the client library performed zero automatic replays
  (ADR-0015).
- Bounded retry proof: only `NotLeader` hint-following and pre-submission `Unavailable`
  reconnects retry, both bounded at N = 3; a retry storm does not multiply mutations.
- Principal non-forgeability: a Protobuf field claiming another principal is ignored; the
  direct client's scoped principal cannot be overridden by request content (§6.2, §15.2).
- Full-suite parity on the target VM/disk class: M0 + M1 + M2 + M3 green together, with the
  §4.3 conformance suite run over `GrpcClient` on mTLS and `DirectClient`, reports identical.

---

## 8. Open questions that block specific rows

Resolve each in the named ADR, then write the blocked test. Do not guess in code.

| ID | Question | Blocks | Owner ADR | Recommendation |
|---|---|---|---|---|
| OQ-1 | ADR-0007 lists `value len u32 LE \| value (Put only)`. For `Delete`, is the **length field** also omitted, or present as `0`? | M0-42, M0-46 | ADR-0007 | Omit both for `Delete` (decode branches on `op`). Delete of `b"a"` with `expected=7` is then 21 bytes: `52 43 4D 44 01 00 02 01 00 00 00 61 01 07 00 00 00 00 00 00 00`. Lock it as a golden test. |
| OQ-2 | `MutationResponse.exists` / `current_mod_revision` on `APPLIED` and `NOT_FOUND`: defined values or "unspecified"? | M0-01..M0-13, M0-52 (response-vector equality), C-02..C-07 | ADR-0006 | Define exactly: `APPLIED` → `exists = (op == Put)`, `current_mod_revision = allocated revision`; `NOT_FOUND` → `exists = false`, `current_mod_revision = 0`. Anything "unspecified" makes replay-determinism assertions untestable. |
| OQ-3 | `max_bytes` accounting: key+value only, or including `create_revision`/`mod_revision`/proto framing? | M0-28, M0-30, C-10 | ADR-0005 or a new ADR | Count `key.len() + value.len() + 16` (two u64s) per record, transport-independent, so Direct and gRPC truncate identically — otherwise M1-46 parity fails for a legitimate reason. |
| OQ-4 | Over-cap value/request: `InvalidArgument` or `ResourceExhausted`? §6.2 lists both; ADR-0006 says "`INVALID_ARGUMENT` / `RESOURCE_EXHAUSTED`" without assigning. | M0-35, M0-36, C-12 | ADR-0006 | Structural violations (empty key, `Delete expected=0`, key > 1 KiB) → `InvalidArgument`; size-budget violations (value > 1 MiB, request > 2 MiB, list caps) → `ResourceExhausted`. |
| OQ-5 | Does a truncated List return at least one record when the first match alone exceeds `max_bytes`? | M0-30 | §10.2 / ADR-0005 | Return one record with `truncated=true` (progress guarantee); a zero-record truncated response gives the caller nothing to act on. |
| OQ-6 | Outcome for an edge-invalid command that nonetheless reached the log (`Delete expected=0`, over-cap). `MutationOutcome` has only `APPLIED/CONFLICT/NOT_FOUND`. | M0-14, M0-39 | ADR-0006 / ADR-0007 | Return `CommandResponse::Rejected { reason }` as a variant **outside** `MutationOutcome`, mapped at the edge to `InvalidArgument`. Reusing `CONFLICT` would corrupt CAS semantics. |
| OQ-7 | Is an empty key valid? | M0-37, C-15 | ADR-0006 / §7.1 | Reject empty keys with `InvalidArgument` (an empty prefix already means "all"; an empty key invites boundary bugs in prefix scans). |
| OQ-8 | DuckDB in CI: `duckdb-rs` dev-dependency, vendored CLI, or a `log-assertions` feature gate? | M1-47..M1-49, §5 | ADR-0013 / ADR-0014 | `duckdb-rs` as a dev-dependency of `config-testkit` behind a default-on feature; when disabled, the log tests must **fail loudly**, never skip silently. |
| OQ-9 | `Cluster` instance method name for restart (`start` collides with the constructor). | §4.1 | ADR-0014 | `Cluster::start(n, kind)` (constructor) + `cluster.start_node(id)` / `cluster.stop(id)`. |
| OQ-10 | Does `state_hash()` ship in release builds or behind `#[cfg(feature = "test-hash")]`? | TA-2, M2 cross-node comparison | ADR-0007 | Ship it unconditionally: M2 needs it to compare nodes after replay, and a `cfg`-gated hash is a different code path from the one under test. |
