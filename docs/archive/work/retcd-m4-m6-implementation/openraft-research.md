# OpenRaft `=0.9.25` (storage-v2) — authoritative API research for rEtcd M4–M6

Status: research note. Written for ADR authors and implementers. Every claim is labelled
**FACT** (read from the pinned source or an official doc), **INFERENCE** (derived by reading
code paths, not stated anywhere), or **UNVERIFIED** (not established; settling step given).

## 0. Provenance and how to re-check a claim

**FACT** — The pinned crate is genuine upstream `openraft 0.9.25`:

| Check | Result |
|---|---|
| `Cargo.lock` checksum | `a97014fb78acb77be3a40ac2da305f6dd3a6b243f3a908ace87d29b3972eaafd` |
| `sha256(~/.cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/openraft-0.9.25.crate)` | identical |
| `diff -rq <tarball>/src <registry-src>/src` | **no differences** |
| `.cargo_vcs_info.json` | `git sha1 = 8815cdba2826f74e848acef361ad03f93bb1c3f8`, `path_in_vcs = openraft` |

Note: the extracted registry dir has **no** `.cargo-checksum.json`, so per-file integrity is not
enforced by cargo on that tree; the tarball diff above is what establishes authenticity. Re-run the
diff if you suspect local edits.

**Source root used throughout this note** (abbreviated `$OR`):

```
C:\Users\gautamb\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\openraft-0.9.25\
```

Docs.rs mirror of the same code: <https://docs.rs/openraft/0.9.25/openraft/>.
Upstream branch: <https://github.com/databendlabs/openraft/tree/release-0.9>.

**FACT** — rEtcd's enabled feature set is `["serde", "storage-v2"]` (`Cargo.toml`, workspace deps).
`generic-snapshot-data`, `singlethreaded`, `single-term-leader`, `compat` and `bt` are **off**.
`loosen-follower-log-revert` is a dev-dependency-only feature of `config-engine`/`config-testkit`.
This feature choice determines several answers below — re-read §1.6 and §2.4 if it ever changes.

**FACT** — rEtcd's `TypeConfig` (`crates/config-storage/src/types.rs:55-72`) sets
`SnapshotData = Cursor<Vec<u8>>`, `Node = RaftNode`, `Entry = openraft::Entry<TypeConfig>`,
`Responder = OneshotResponder<TypeConfig>`, `AsyncRuntime = TokioRuntime`.

**FACT** — rEtcd's current openraft `Config` (`crates/config-engine/src/config.rs:246-259`):
`snapshot_policy = Never`, `max_in_snapshot_log_to_keep = u64::MAX`, `purge_batch_size = 1`,
`max_payload_entries = MAX_PAYLOAD_ENTRIES`. See §4.5 for why that combination is currently a
correct "never purge" latch and what M5 must change.

---

## 1. Snapshots: build, policy, purge coupling

### 1.1 Trait obligations

**FACT** — `$OR/src/storage/mod.rs:200-218`
(<https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftSnapshotBuilder.html>):

```rust
pub trait RaftSnapshotBuilder<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<C::NodeId>>;
}
```

Documented obligation, verbatim intent: *"A snapshot has to contain state of all applied log,
including membership."* There is **no** argument telling the builder which index to snapshot at —
the builder alone decides `meta.last_log_id`, and that must be the state machine's own
`last_applied` at the instant the view was taken.

**FACT** — `$OR/src/storage/v2.rs:206-256`
(<https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftStateMachine.html>):

| Method | Contract as documented in source |
|---|---|
| `get_snapshot_builder(&mut self) -> Self::SnapshotBuilder` | "Usually it returns a snapshot **view** of the state machine (i.e., subsequent changes to the state machine won't affect the return snapshot view), or just a copy of the entire state machine." Intentionally `async` so the impl may take a lock. |
| `begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError>` | "Create a new blank snapshot, returning a **writable** handle." Openraft writes received bytes into it. |
| `install_snapshot(&mut self, meta: &SnapshotMeta<..>, snapshot: Box<C::SnapshotData>)` | Before returning: (1) state machine **replaced** with snapshot contents, (2) the input snapshot **saved** so `get_current_snapshot` returns it, (3) **all other snapshots deleted**. |
| `get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError>` | Returns a readable handle to the current snapshot. "A proper snapshot implementation will store last-applied-log-id and the last-applied-membership config as part of the snapshot." |

**FACT** — `$OR/src/storage/v2.rs:171-173`, `applied_state()` returns
`(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>)`. Documented relaxation: *"It is all right
to return a membership with greater log id than the last-applied-log-id"* — openraft rescans logs
from `last_applied` at startup.

### 1.2 `SnapshotMeta` / `Snapshot` / `LogState`

**FACT** — `$OR/src/storage/mod.rs:45-62`
(<https://docs.rs/openraft/0.9.25/openraft/storage/struct.SnapshotMeta.html>):

```rust
pub struct SnapshotMeta<NID, N> {
    pub last_log_id: Option<LogId<NID>>,          // logs up to here, inclusive
    pub last_membership: StoredMembership<NID, N>,// the last applied membership
    pub snapshot_id: SnapshotId,                  // = String; identity during transfer
}
```

- `SnapshotId` is `String` (`$OR/src/raft_types.rs`, re-exported as `openraft::SnapshotId`).
- Source caveat, verbatim: *"even when two snapshot is built with the same `last_log_id`, they
  still could be different in bytes."* So `snapshot_id` must be **unique per build**, not derived
  solely from `last_log_id`. **INFERENCE** for rEtcd: use `format!("{}-{}-{}", term, index, uuid_v4)`
  or a monotonic counter persisted alongside the snapshot; do **not** use `last_log_id` alone.
- `SnapshotMeta::signature()` (`mod.rs:95-101`) returns `SnapshotSignature { last_log_id,
  last_membership_log_id, snapshot_id }` — used internally for in-flight snapshot equality.

**FACT** — `$OR/src/storage/mod.rs:111-119`: `Snapshot<C> { meta: SnapshotMeta, snapshot:
Box<C::SnapshotData> }`.

**FACT** — `$OR/src/storage/mod.rs:141-149`:

```rust
/// Invariance: last_purged_log_id <= last_applied <= last_log_id
pub struct LogState<C> {
    pub last_purged_log_id: Option<LogId<C::NodeId>>,
    pub last_log_id: Option<LogId<C::NodeId>>, // == last_purged_log_id when no entries present
}
```

`StoredMembership` (`$OR/src/membership/stored_membership.rs:22-57`) has **private** fields with
accessors `log_id() -> &Option<LogId>`, `membership() -> &Membership`, `voter_ids()`, `nodes()`.
Construct with `StoredMembership::new(log_id, membership)`.

### 1.3 When `SnapshotPolicy::LogsSinceLast(n)` fires

**FACT** — `$OR/src/config/config.rs:39-48`:

```rust
SnapshotPolicy::LogsSinceLast(threshold) =>
    state.committed().next_index() >= state.snapshot_last_log_id().next_index() + threshold
```

Note it is **committed**, not applied, and the baseline is the *snapshot's* last log id, not the
last purge point. `next_index()` of `None` is `0`.

**FACT** — the predicate is evaluated at exactly two call sites, both immediately after `committed`
advances:

- Leader: `$OR/src/engine/handler/replication_handler/mod.rs:243-245`
- Follower/learner: `$OR/src/engine/handler/following_handler/mod.rs:179-181` (inside
  `commit_entries`)

So **followers build their own snapshots too** — it is not a leader-only activity.

**FACT** — `$OR/src/engine/handler/snapshot_handler/mod.rs:31-45` (`trigger_snapshot`): if
`io_state.building_snapshot()` is already true, the trigger is a **no-op and returns false**. Only
one build is ever in flight. The flag is cleared in `finish_building_snapshot`
(`$OR/src/engine/engine_impl.rs:566-580`).

**FACT** — `SnapshotPolicy::Never` makes `should_snapshot` always `false`
(`$OR/src/config/config.rs:46`); the only way to build is `Raft::trigger_snapshot()` /
`raft.trigger().snapshot()` (`$OR/src/raft/trigger.rs:58-62`).

### 1.4 Build completion → purge ordering

**FACT** — `$OR/src/engine/engine_impl.rs:566-580`:

```rust
pub(crate) fn finish_building_snapshot(&mut self, meta: SnapshotMeta<..>) -> bool {
    self.state.io_state_mut().set_building_snapshot(false);
    let updated = self.snapshot_handler().update_snapshot(meta);
    if !updated { return false; }
    self.log_handler().schedule_policy_based_purge();
    self.try_purge_log();
    true
}
```

**FACT** — `update_snapshot` (`snapshot_handler/mod.rs:52-67`) **rejects** a snapshot whose
`last_log_id <= state.snapshot_last_log_id()` and returns `false`; the engine then does not schedule
any purge.

**FACT** — `$OR/src/engine/handler/log_handler/mod.rs:78-113` (`calc_purge_upto`):

```rust
let purge_end = state.snapshot_meta.last_log_id.next_index().saturating_sub(max_in_snapshot_log_to_keep);
if last_purged_log_id.next_index() + purge_batch_size > purge_end { return None; }   // "no need to purge"
state.log_ids.get(purge_end - 1)                                                     // inclusive upper bound
```

Consequences:

- **Only logs already covered by the *current* snapshot are ever purge candidates** (the doc comment
  on the method says exactly this, and `max_in_snapshot_log_to_keep`'s field doc says "Logs that are
  not in snapshot will never be purged").
- `max_in_snapshot_log_to_keep` = how many of the snapshot-covered entries to **retain** (default
  `1000`). `0` means every applied-and-snapshotted log may go.
- `purge_batch_size` (default `1`) is a **minimum batch**: purge is skipped until at least this many
  entries would be removed. It is *not* a chunking size — a single `purge(log_id)` call may span far
  more than `purge_batch_size` entries.
- `saturating_sub` means `max_in_snapshot_log_to_keep = u64::MAX` makes `purge_end == 0`, which makes
  the guard always true, which returns `None` — **purge is never scheduled**. This is why rEtcd's
  current belt-and-braces setting works (§4.5).

**FACT** — leader-side extra gate, `$OR/src/engine/handler/replication_handler/mod.rs` (`try_purge_log`):
if any replication progress entry has the to-be-purged range in flight
(`prog_entry.is_log_range_inflight(&purge_upto)`), the purge is **postponed**, not cancelled; it is
retried on later engine ticks. This matches the `Raft::trigger().purge_log()` doc: *"Openraft won't
purge logs at once, e.g. it may be delayed by several seconds"* (`$OR/src/raft/trigger.rs:74-77`).

**FACT** — execution: `Command::PurgeLog { upto }` is run by RaftCore as
`self.log_store.purge(upto).await?` then `io_state.update_purged(Some(upto))`
(`$OR/src/core/raft_core.rs:1659-1662`). It is awaited inline on the RaftCore task — a slow `purge`
stalls the whole node.

**Ordering guarantee — leader/self-build path (FACT + INFERENCE).** `Command::PurgeLog` is only
pushed from `finish_building_snapshot`, which only runs on receipt of
`sm::Response::BuildSnapshot(meta)` (`$OR/src/core/raft_core.rs:1399-1414`), which the sm worker
sends only **after `build_snapshot()` has returned `Ok`** (`$OR/src/core/sm/worker.rs:186-191`).
**INFERENCE:** therefore, on this path, `purge()` is never called before `build_snapshot()` returned.
It **is** called before anyone confirms the snapshot is *durable* — openraft has no fsync callback
for snapshots. Durability of the snapshot before returning `Ok` from `build_snapshot` is entirely the
implementation's obligation. See trap T2.

**Ordering — follower install path: there is NO such guarantee. See §2.5 / trap T1.**

### 1.5 `build_snapshot` returning `Err`

**FACT** — `$OR/src/core/sm/worker.rs:186-191`: the spawned task maps the result into
`CommandResult::new(seq, res)` and sends it. **FACT** — `$OR/src/core/raft_core.rs:1382`:
`let res = command_result.result?;` — a `StorageError` propagates out of the RaftCore loop and
becomes a **`Fatal` error; the node shuts down**. There is no retry, no backoff, no degraded mode.

**FACT** — `io_state.building_snapshot` is *only* cleared inside `finish_building_snapshot`, which is
not reached on error. **INFERENCE:** the flag is therefore permanently stuck on error, but this is
moot because RaftCore is already terminating.

**Design consequence for M5 (INFERENCE):** `build_snapshot` must not return `Err` for transient
conditions (disk full mid-write, a RocksDB checkpoint retry, a cancelled task). Absorb-and-retry
internally, or accept that any snapshot failure kills the node. rEtcd's existing poison/guard
pattern in `crates/config-storage/src/rocks.rs` is the right shape; just be sure a *recoverable*
error does not reach openraft.

### 1.6 Is the state machine locked during build? (No.)

**FACT** — `$OR/src/core/sm/worker.rs:168-193`, doc comment verbatim: *"Building snapshot is a
read-only operation, so it can be run in another task in parallel. This parallelization depends on
the `RaftSnapshotBuilder` implementation returned by `get_snapshot_builder()`: The builder **must**:
- hold a consistent view of the state machine that won't be affected by further writes such as
applying a log entry, - or it must be able to acquire a lock that prevents any write operations."*

The code: `get_snapshot_builder().await` runs on the worker task (serialized with `apply`), then
`C::AsyncRuntime::spawn(async move { builder.build_snapshot().await; ... })` — **the worker loop
returns immediately and continues processing `Apply` commands concurrently.**

**FACT** — RaftCore explicitly tolerates out-of-order completion:
`$OR/src/core/raft_core.rs:1383-1396` skips the monotonic `finished_sm_seq` debug assertion for
`sm::Response::BuildSnapshot` with the comment *"BuildSnapshot is a read operation that does not have
to be serialized by sm::Worker. Thus it may finish out of order."*

**Implication for rEtcd's RocksDB store (INFERENCE):** the correct primitive is a **RocksDB snapshot
or checkpoint captured synchronously inside `get_snapshot_builder()`**, handed to the returned
builder. Capturing state lazily inside `build_snapshot()` would race with concurrent `apply()` and
can produce a snapshot whose contents are newer than its `meta.last_log_id` — silently corrupting a
follower that installs it. A blanket `Mutex` held across `build_snapshot()` also "works" per the doc,
but serialises all applies for the duration of the build (trap T3).

### 1.7 `SnapshotData` type and alternatives

**FACT** — `$OR/src/type_config.rs:73-94`. Without `generic-snapshot-data`:

```rust
type SnapshotData: tokio::io::AsyncRead + AsyncWrite + AsyncSeek + Unpin + OptionalSend + 'static;
```

With `generic-snapshot-data`: `type SnapshotData: OptionalSend + 'static;` (no IO bounds).

**FACT** — `$OR/src/docs/feature_flags/feature-flags.md`
(<https://docs.rs/openraft/0.9.25/openraft/docs/feature_flags/index.html>), verbatim structure:

| | sender (leader) | receiver (follower) |
|---|---|---|
| `generic-snapshot-data` **off** (rEtcd today) | `RaftNetwork::full_snapshot()` has a default impl that calls `RaftNetwork::install_snapshot()` per chunk | `Raft::install_snapshot()` available |
| `generic-snapshot-data` **on** | must implement `full_snapshot()` yourself; `install_snapshot()` not needed | `Raft::install_snapshot()` is **not available**; use `begin_receiving_snapshot()` + `install_full_snapshot()` |

Alternatives to `Cursor<Vec<u8>>` **without** flipping the feature: any type implementing
`AsyncRead + AsyncWrite + AsyncSeek + Unpin` — notably `tokio::fs::File` (a real on-disk snapshot
file), which removes the whole-snapshot-in-RAM problem (trap T4) and keeps the default chunked
transport. **INFERENCE:** this is the lowest-risk option for rEtcd M5; switching to
`generic-snapshot-data` obliges rEtcd to implement its own streaming transport over tonic and loses
`Raft::install_snapshot()`.

---

## 2. Snapshot install on a lagging follower

### 2.1 When does the leader decide to send a snapshot? (Not what the config doc says.)

**FACT** — `$OR/src/progress/entry/mod.rs:199-245` is the only decision site. Two conditions:

1. `self.searching_end < purge_upto_next` — "every candidate matching position is purged"; log
   replication cannot make progress because the lowest usable `prev` is already at/above a position
   known not to match.
2. `start == end && matching_next < searching_end` — "still probing, but the leader log is fully
   purged, so there is no entry for the probe to carry".

**FACT — important and counter-intuitive:** `replication_lag_threshold` is used in **exactly one
place** in the whole crate, `$OR/src/raft/mod.rs:772`, inside
`Raft::check_replication_upto_date()` — i.e. only by `add_learner(.., blocking = true)`'s
catch-up wait. Verified by `grep -rn replication_lag_threshold $OR/src/` (only hits:
`config/config.rs`, `config/config_test.rs`, `raft/mod.rs:772`).

**INFERENCE:** the `Config::replication_lag_threshold` doc comment ("A follower falls behind this
index are replicated with snapshot") is **stale in 0.9.25**. The real trigger is *log purging*, not
lag. A follower can be arbitrarily far behind and still be caught up by plain `AppendEntries` as long
as the leader has not purged the entries it needs. Corollary for rEtcd: **snapshot install is only
ever exercised once purge is enabled.** Do not expect M5 snapshot transfer to trigger while
`SnapshotPolicy::Never` holds.

### 2.2 Leader-side send sequence

**FACT** — `$OR/src/replication/mod.rs:724-812`:

1. `self.snapshot_reader.get_snapshot().await` → sends `sm::Command::GetSnapshot` to the sm worker →
   `RaftStateMachine::get_current_snapshot()` (`$OR/src/core/sm/handle.rs`, `worker.rs:195-207`).
2. `None` ⇒ hard `StorageError::IO { read_snapshot: "snapshot not found" }` — **the leader errors
   out**. `get_current_snapshot` must return a snapshot whenever the leader has purged below its own
   log head.
3. `RPCOption::new(config.install_snapshot_timeout())`, `option.snapshot_chunk_size =
   Some(config.snapshot_max_chunk_size as usize)`.
4. Streaming runs in a **spawned task** (`C::spawn(Self::send_snapshot(..))`) with a cancel oneshot;
   the network handle is behind `Arc<Mutex<N::Network>>` and is **held locked for the entire
   transfer** (`let mut net = network.lock().await;` at `replication/mod.rs:789`).
5. `net.full_snapshot(vote, snapshot, cancel, option).await`.
6. Result posted back as `Data::SnapshotCallback`.

### 2.3 What a transport must implement in 0.9.25

**FACT** — `$OR/src/network/mod.rs` exports exactly: `Backoff`, `RaftNetworkFactory`, `RaftNetwork`,
`RPCOption`, `RPCTypes`, plus the `snapshot_transport` module. **There is no `RaftNetworkV2` in
0.9.25** (`grep -rn RaftNetworkV2 $OR/src/` → zero hits). `RaftNetworkV2` is a 0.10 concept. Any ADR
or design doc that names it against this pin is wrong.

**FACT** — `storage::Adaptor` (`$OR/src/storage/adapter.rs`) is the **v1-storage-to-v2 shim** and is
`#[cfg(not(feature = "storage-v2"))]` — it does not exist in rEtcd's build and has nothing to do with
networking.

**FACT** — `$OR/src/network/network.rs:37-163`, required methods with `generic-snapshot-data` off:

| Method | Required? |
|---|---|
| `append_entries` | yes |
| `vote` | yes |
| `install_snapshot(InstallSnapshotRequest<C>, RPCOption)` | **yes** — no default body in this cfg (`network.rs:65-73`) |
| `full_snapshot(vote, Snapshot<C>, cancel, option)` | **has a default** = `Chunked::send_snapshot(self, ..)` (`network.rs:135-148`) |
| `backoff()` | default: constant 500 ms |

So a rEtcd tonic transport needs one new peer RPC (`InstallSnapshot`) and can inherit chunking for
free.

**FACT** — `$OR/src/raft/message/install_snapshot.rs:13-26`:

```rust
pub struct InstallSnapshotRequest<C> {
    pub vote: Vote<C::NodeId>,
    pub meta: SnapshotMeta<C::NodeId, C::Node>,
    pub offset: u64,   // byte offset of this chunk within the snapshot
    pub data: Vec<u8>, // the chunk
    pub done: bool,    // last chunk
}
pub struct InstallSnapshotResponse<NID> { pub vote: Vote<NID> }
pub struct SnapshotResponse<NID> { pub vote: Vote<NID> }  // From<SnapshotResponse> for InstallSnapshotResponse
```

**FACT** — `$OR/src/network/snapshot_transport.rs:98-130`, the chunked sender has a **bounded retry
policy built in** (new-ish in 0.9.23): `SNAPSHOT_CHUNK_MAX_RETRIES = 5`, exponential backoff base
10 ms capped at 200 ms for `Timeout`/`Network` errors; `Unreachable` uses the caller's
`RaftNetwork::backoff()` iterator (fallback 500 ms). A successful chunk resets both the counter and
the backoff iterator.

**FACT** — `$OR/src/replication/mod.rs:348-350`: an `InstallSnapshot` RPC that exceeds the transport's
size limit is logged as *"InstallSnapshot RPC is too large, but it is not supported yet"* — openraft
does **not** adapt the chunk size. rEtcd must size `snapshot_max_chunk_size` (default 3 MiB) below the
tonic `max_decoding_message_size` on the peer service, with headroom for `meta` and framing.

### 2.4 Receiver side

**FACT** — `$OR/src/raft/mod.rs:485-524` (`Raft::install_snapshot`, available because
`generic-snapshot-data` is off, and requiring `C::SnapshotData: AsyncRead + AsyncWrite + AsyncSeek +
Unpin`):

1. Read local vote via `with_raft_state`; if `req.vote < my_vote`, return `InstallSnapshotResponse {
   vote: my_vote }` **without touching storage** (early rejection, explicitly "not mandatory ...
   but prevent unnecessary snapshot transfer early").
2. `let mut streaming = self.inner.snapshot.lock().await;` — per-`Raft` streaming state, so a node
   receives **one** snapshot stream at a time.
3. `Chunked::receive_snapshot(&mut *streaming, self, req)` — internally calls
   `Raft::begin_receiving_snapshot()` on the first chunk, writes each chunk at its `offset` (via
   `AsyncSeek`), and returns `Some(Snapshot)` only when `req.done`.
4. On `Some(snapshot)`: `self.install_full_snapshot(req_vote, snapshot).await` →
   `SnapshotResponse` → `.into()` → `InstallSnapshotResponse`.

**FACT** — `Raft::install_full_snapshot(vote, snapshot) -> Result<SnapshotResponse<NID>,
Fatal<NID>>` (`$OR/src/raft/mod.rs:452-468`) is public and usable directly if rEtcd ever implements
its own streaming. `Raft::begin_receiving_snapshot() -> Result<Box<SnapshotDataOf<C>>,
RaftError<NID, Infallible>>` (`mod.rs:438-444`) likewise.

### 2.5 Engine sequence on install, and what the SM must guarantee

**FACT** — `$OR/src/engine/handler/following_handler/mod.rs:278-330`, in order:

```
if meta.last_log_id <= state.committed()            -> return None (no install)
snapshot_handler().update_snapshot(meta)            -> false if not newer, return None
if local log at meta.last_log_id.index conflicts    -> truncate_logs(committed().next_index())
                                                       (deletes ALL non-committed logs)
state.update_accepted(Some(snap_last_log_id))
state.committed = Some(snap_last_log_id)
update_committed_membership(meta.last_membership, snap_last_log_id.index)
push Command::StateMachine(sm::Command::install_full_snapshot(snapshot))
state.purge_upto = Some(snap_last_log_id)
log_handler().purge_log()                            -> push Command::PurgeLog { upto }
return Some(Condition::StateMachineCommand { command_seq: last_sm_seq })
```

Rationale for the wholesale truncation is documented in
`$OR/src/docs/protocol/snapshot_replication.md`
(<https://docs.rs/openraft/0.9.25/openraft/docs/protocol/snapshot_replication/index.html>): all four
snapshot/log alignment cases collapse to "if `snapshot.last_log_id` does not match the local entry,
delete **all** non-committed logs", which is safe because `snapshot.last_log_id` is committed on a
quorum.

**FACT** — post-install bookkeeping in RaftCore (`$OR/src/core/raft_core.rs:1415-1427`): on
`sm::Response::InstallSnapshot(Some(meta))` it sets `io_state.update_applied(meta.last_log_id)` and
`io_state.update_snapshot(meta.last_log_id)`.

**What the state machine must guarantee after `install_snapshot` returns (FACT, from
`storage/v2.rs:226-241` plus the above):**

- `applied_state()` must subsequently return `(meta.last_log_id, meta.last_membership)` — openraft
  does **not** re-read `applied_state()` at this point, it trusts `meta`; a divergence only surfaces
  after a restart, as silent state loss.
- `get_current_snapshot()` must subsequently return this snapshot (so this node can in turn serve it).
- All older snapshots must be gone.

**FACT** — `applied_state()` is re-read in exactly one place: `StorageHelper::get_initial_state()`
(`$OR/src/storage/helper.rs:78`), i.e. **only at node startup**, and again indirectly via
`get_membership()` (`helper.rs:243`).

**FACT — the install/purge hazard.** `Command::PurgeLog` has `condition() == None`
(`$OR/src/engine/command.rs:192` — the `#[rustfmt::skip]` condition table lists
`Command::PurgeLog { .. } => None`). RaftCore's `run_command` only postpones commands that carry a
`Condition` (`raft_core.rs:1595-1626`), and `Command::StateMachine` merely **forwards** to the sm
worker's unbounded channel. **INFERENCE:** on the follower install path, `log_store.purge(upto)` is
awaited on the RaftCore task while `state_machine.install_snapshot(..)` is still running on the sm
worker task — the log can be purged *before* the snapshot is installed or durable. This is trap T1;
openraft's own recovery for it is described in §4.3.

---

## 3. Membership

### 3.1 API shapes

**FACT** — `$OR/src/raft/impl_raft_blocking_write.rs`
(<https://docs.rs/openraft/0.9.25/openraft/struct.Raft.html#method.change_membership>). Both methods
require `C: RaftTypeConfig<Responder = OneshotResponder<C>>` — satisfied by rEtcd's `TypeConfig`.

```rust
pub async fn add_learner(&self, id: C::NodeId, node: C::Node, blocking: bool)
    -> Result<ClientWriteResponse<C>, RaftError<C::NodeId, ClientWriteError<C::NodeId, C::Node>>>;

pub async fn change_membership(&self, members: impl Into<ChangeMembers<C::NodeId, C::Node>>, retain: bool)
    -> Result<ClientWriteResponse<C>, RaftError<C::NodeId, ClientWriteError<C::NodeId, C::Node>>>;
```

**FACT** — `add_learner` internals (`lines 125-169`): it is literally
`ChangeMembers::AddNodes(btreemap!{id => node})` with **`retain: true` hard-coded**. If `blocking`:
it waits on `wait(None).metrics(|m| check_replication_upto_date(m, id, Some(membership_log_id)).is_ok())`.
Notes: `blocking` is skipped when `id == self.inner.id`; **the wait result is logged and then
discarded** (`let wait_res = ...; tracing::info!(...); Ok(resp)`) — `add_learner(.., true)` returning
`Ok` does **not** prove the learner caught up. Verify separately (§3.3).
Documented: *"If the node to add is already a voter or learner, it will still re-add it."*

**FACT** — `change_membership` internals (`lines 49-106`): two sequential `RaftMsg::ChangeMembership`
round-trips. First proposes the **joint** config and returns when it is committed; if the returned
membership already has `get_joint_config().len() == 1` it returns early; otherwise it proposes the
**uniform** config with the same `changes` and `retain`. Documented failure mode, verbatim: *"If it
loses leadership or crashed before committing the second uniform config log, the cluster is left in
the joint config."*

**FACT** — `$OR/src/docs/cluster_control/dynamic-membership.md`: *"If there are nodes in the given
membership that are not `Learners`, this method will fail. Therefore, the application should always
call `Raft::add_learner()` first."* Error is `ClientWriteError::ChangeMembershipError(LearnerNotFound)`.

### 3.2 `retain`

**FACT** — `impl_raft_blocking_write.rs:34-44` and the same example in the docs: from
`{voters:{1,2,3}, learners:{}}`, `change_membership(voters={3,4,5})`:

- `retain = true` → `{voters:{3,4,5}, learners:{1,2}}` — demoted, replication to 1 and 2 continues.
- `retain = false` → `{voters:{3,4,5}, learners:{}}` — 1 and 2 removed entirely.

**INFERENCE for rEtcd learner replacement (M6):** use `retain = false` for a *decommission* (the node
is going away; you want the leader to stop replicating to it), and `retain = true` for a *demotion*
you intend to reverse.

### 3.3 Detecting that a learner has caught up

**FACT** — `$OR/src/raft/mod.rs:735-778` (`check_replication_upto_date`), the exact predicate
openraft itself uses:

```
if metrics.membership_config.log_id() < &membership_log_id      -> keep waiting (stale metrics)
if membership_config.membership().get_node(&node_id).is_none()  -> Ok(None)  (learner was removed)
let repl = metrics.replication else                             -> Ok(None)  (no longer leader)
let matched = repl.get(&node_id) else                           -> keep waiting
if replication_lag(&matched.index(), &metrics.last_log_index) <= config.replication_lag_threshold
                                                                -> Ok(matched)  (caught up)
```

`metrics.replication` is `Option<BTreeMap<NodeId, Option<LogId<NodeId>>>>`
(`$OR/src/metrics/mod.rs:51`; the alias `ReplicationMetrics` is `pub(crate)` but the field type is
usable structurally) and is `Some` **only on a leader**.

**Recommended rEtcd check (INFERENCE, mirrors the above but observable):**

```rust
let target_index = raft.metrics().borrow().last_log_index;            // leader head at decision time
raft.wait(Some(timeout)).metrics(|m| {
        m.membership_config.log_id() >= &Some(learner_membership_log_id)
        && m.replication.as_ref()
             .and_then(|r| r.get(&learner_id).copied().flatten())
             .map(|matched| matched.index + LAG_BUDGET >= target_index.unwrap_or(0))
             .unwrap_or(false)
    }, "learner caught up").await?;
```

`Raft::wait()` conveniences available (`$OR/src/metrics/wait.rs`): `log_at_least`,
`log_index_at_least`, `applied_index`, `applied_index_at_least`, `snapshot`, `purged`, `members`,
`voter_ids`, `current_leader`, `state`, `vote`, plus generic `metrics(f, msg)`, `ge(Metric, msg)`,
`eq(Metric, msg)`. `Metric` variants (`$OR/src/metrics/metric.rs`): `Term`, `Vote`, `LastLogIndex`,
`Applied`, `AppliedIndex`, `Snapshot`, `Purged`. **Note:** `log_at_least` observes *this node's*
`last_log_index`/`last_applied`, **not** a peer's replication progress — it cannot answer "is learner
N caught up". Only `metrics.replication` can.

### 3.4 Recommended safe sequences

**Add a voter (FACT, from the docs + API):**
`add_learner(id, node, blocking=false)` → wait on the replication-map predicate (§3.3) →
`change_membership(ChangeMembers::AddVoterIds({id}), retain=true)`.
Using `AddVoterIds` (promote an existing learner) rather than a full `ReplaceAllVoters` set makes the
intent explicit and fails loudly if the learner is not present.

**Remove a voter (FACT, `dynamic-membership.md`):** call `change_membership` on the leader; the leader
proposes joint `[{1,2,3},{3,4,5}]` then uniform `{3,4,5}`. *"As soon as the leader commits the second
config log, the node to remove can be safely terminated."* And verbatim: *"An application does not
have to wait for the config log to be replicated to the node to remove. Because a distributed
consensus protocol tolerates a minority member crash."*

**Replace a learner (INFERENCE, M6):** `add_learner(new)` → wait caught up →
`change_membership(RemoveNodes({old}), ..)` **or**, if the old node is still a voter, first
`change_membership` to drop it from voters (which requires `retain=false` to also remove the node
entry). Note `ChangeMembers::RemoveNodes` returns `LearnerNotFound` if the node is still a voter
(`$OR/src/change_members.rs:46-50`).

### 3.5 Why `ChangeMembers::SetNodes` is dangerous

**FACT** — `$OR/src/change_members.rs:37-44` and `dynamic-membership.md` §"Update Node". `SetNodes`
**replaces** an existing node's `Node` payload (e.g. its address); `AddNodes` **will not** replace.
The documented split-brain: a 3-node cluster `a:x, b:y, c:z` plus an uninitialized `d:w`. Mistakenly
setting `b`'s address from `y` to `w` lets `{x,y}` and `{z,w}` each form a quorum and elect a leader —
two leaders in the same term-space, divergent logs. Doc verbatim: *"Directly updating node addresses
with `ChangeMembers::SetNodes` should be replaced with `ChangeMembers::RemoveNodes` and
`Raft::add_learner` whenever possible. Do not use `ChangeMembers::SetNodes` unless you know what you
are doing."*

**INFERENCE for rEtcd:** address changes must go through remove-then-re-add-as-learner. Since rEtcd's
`RaftNode` carries both `peer` and `client` endpoints, any endpoint rotation is a `SetNodes`
temptation — it should be blocked at the API boundary, not just documented.

**FACT** — the same doc's §"Ensure connection to the correct node": it is the `RaftNetworkFactory` /
`RaftNetwork` implementation's responsibility to verify it reached the intended node. rEtcd's mTLS
peer identity check (ADR-0010) satisfies this if and only if the certificate identity is compared
against the target node id, not merely validated against the CA.

### 3.6 Crash mid-joint: what is persisted, and recovery

**FACT** — nothing membership-specific is persisted beyond **the membership log entries themselves**
plus whatever `apply()` stored for `applied_state()`. On restart,
`StorageHelper::get_membership()` (`$OR/src/storage/helper.rs:243-270`) reconstructs:

1. `(last_applied, sm_mem) = state_machine.applied_state()`
2. `log_mem = last_membership_in_log(last_applied.next_index())` — scans the log **backwards in 64-entry
   steps** from the head down to `max(last_purged+1, last_applied+1)`, collecting at most the **last 2**
   membership entries (`helper.rs:277-305`).
3. If 2 found → `MembershipState::new(committed = log_mem[0], effective = log_mem[1])`.
   Else → `MembershipState::new(committed = sm_mem, effective = log_mem[0] or sm_mem)`.

Doc rationale verbatim: *"a raft node will only need to store at most two recent membership logs"*,
because a follower may need to revert one uncommitted membership when the leader truncates it.

**INFERENCE:** if the node crashed with a committed joint config and an uncommitted uniform config,
the effective membership on restart is the **uniform** one (the later log entry), reverting to joint
only if the new leader truncates it. If it crashed with only the joint config committed, it comes back
**in joint consensus** and stays there until some leader re-proposes the uniform config. rEtcd must
therefore treat "membership has 2 joint config sets" as an operational state to detect and repair
(re-issue the same `change_membership`), not an impossible one. Check via
`metrics.membership_config.membership().get_joint_config().len() > 1`.

**INFERENCE (storage requirement):** `RaftStateMachine::apply` must store the membership from
`RaftEntry::get_membership()` (`storage/v2.rs:188`) *and* survive purge — once entries below the
snapshot are purged, step 2 above finds nothing and openraft falls back entirely to `sm_mem`. A store
that drops the membership on install/apply loses the cluster.

---

## 4. Purge

### 4.1 Contract

**FACT** — `$OR/src/storage/v2.rs:138-143`:

```rust
/// Purge logs upto `log_id`, inclusive
/// ### To ensure correctness:
/// - It must not leave a **hole** in logs.
async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;
```

`purge` is **inclusive** of `log_id`. Compare `truncate(log_id)` — "Truncate logs since `log_id`,
inclusive", i.e. removes the tail from `log_id` upward. Both must avoid holes.

**FACT** — trait-level correctness rule (`v2.rs:43-48`) applying to the whole `RaftLogStorage`:
*"All write-IO must be serialized, i.e., the internal implementation must NOT apply a latter write
request before a former write request is completed. This rule applies to both `vote` and `log` IO."*

### 4.2 `LogState` after purge / `last_purged_log_id` persistence

**FACT** — after `purge(x)`, the next `get_log_state()` must report:

- `last_purged_log_id = Some(x)`
- `last_log_id = ` the last present entry, **or `Some(x)` if no entries remain** (`storage/mod.rs:146-148`)
- invariant `last_purged_log_id <= last_applied <= last_log_id` (`storage/mod.rs:140`)

`last_purged_log_id` **must be durable**, because it is the only way openraft learns, after restart,
that index range `(.., x]` is intentionally absent rather than lost. rEtcd already persists it as
`raft_meta/last_purged` (`crates/config-storage/src/rocks.rs:125`, read back at `rocks.rs:1035-1036`,
returned at `rocks.rs:1190-1191`) — that shape is correct.

**FACT** — openraft consumes it at `StorageHelper::get_initial_state()` (`helper.rs:74-76`) and it
becomes `RaftState.purged_next` / `purge_upto` (`helper.rs:164,173`), and the lower bound for the log
scan `LogIdList::load_log_ids(last_purged_log_id, last_log_id, ..)` (`helper.rs:126`) and for
`last_membership_in_log` (`helper.rs:305`).

### 4.3 Startup repair paths (both matter for rEtcd)

**FACT** — `$OR/src/storage/helper.rs:107-119`:

```rust
// Clean up dirty state: snapshot is installed but logs are not cleaned.
if last_log_id < last_applied {
    self.log_store.purge(last_applied.clone().unwrap()).await?;   // openraft calls purge() itself
    last_log_id = last_applied.clone();
    last_purged_log_id = last_applied.clone();
}
```

This is openraft's stated recovery for the install-then-crash-before-purge case
(`snapshot_replication.md`: *"If this node crashes after installing snapshot and before purging logs,
the log will be purged the next start-up, in `get_initial_state()`"*). It does **not** cover the
reverse order (purged first, install lost) — see trap T1.

**FACT** — `$OR/src/storage/helper.rs:128-143`:

```rust
let snapshot = self.state_machine.get_current_snapshot().await?;
let snapshot = match snapshot {
    None => if last_purged_log_id.is_some() {
        let mut b = self.state_machine.get_snapshot_builder().await;
        Some(b.build_snapshot().await?)          // openraft BUILDS a snapshot at startup
    } else { None },
    s @ Some(_) => s,
};
```

**INFERENCE:** the moment rEtcd purges anything, **`build_snapshot` becomes reachable on the startup
path**, synchronously, before the node serves anything. A store that has ever purged must therefore
have a working builder — a stub that returns `Err` turns a restart into a permanent failure to start.
`RaftStateMachine` stubs must be replaced in the same milestone that enables purge, not later.

**FACT** — also on startup, `helper.rs:91-103`: if `read_committed() > last_applied`, openraft
**re-applies** committed entries `[last_applied+1, committed]` through `state_machine.apply()` in
64-entry chunks (`reapply_committed`), erroring hard if the log range is not exactly present. So if
rEtcd implements `save_committed`, `apply` must be idempotent-safe for re-application of that window.

### 4.4 Interaction with `get_log_state` on restart

**FACT** — `get_log_state()` is documented to **ignore the state machine**: *"The impl should not
consider the applied log id in state machine"* (`v2.rs:60-63`). It reports only what the log store
itself knows. `get_initial_state` is what reconciles log store and state machine.

**FACT** — after openraft's own repair `purge()` at `helper.rs:116`, it does **not** re-read
`get_log_state()`; it patches its local variables. **INFERENCE:** the store's own persisted
`last_purged` must be updated by that `purge()` call for the next restart to agree.

### 4.5 rEtcd's current settings, and what M5 changes

**FACT** — `crates/config-engine/src/config.rs:246-259`: `snapshot_policy = Never`,
`max_in_snapshot_log_to_keep = u64::MAX`, `purge_batch_size = 1`.

**INFERENCE (verified against `calc_purge_upto`):** this is a correct double latch.
`Never` ⇒ `should_snapshot` always false ⇒ `snapshot_meta.last_log_id` stays `None` ⇒
`purge_end = 0.saturating_sub(u64::MAX) = 0` ⇒ guard `0 + 1 > 0` is true ⇒ `None` ⇒ no purge.
The in-repo comment's reasoning matches the source.

**M5 must change all three together (INFERENCE):** setting `snapshot_policy =
LogsSinceLast(n)` while leaving `max_in_snapshot_log_to_keep = u64::MAX` would build snapshots and
still never purge (a silent no-op, plus unbounded log). Setting a small
`max_in_snapshot_log_to_keep` before `install_snapshot` works would let a leader purge below what a
lagging follower needs, and then fail to serve the snapshot. **Order: implement
build/install/get_current first, prove them, then relax the config.**

**FACT** — manual purge escape hatch: `Raft::purge_log(upto: u64)` / `raft.trigger().purge_log(upto)`
(`$OR/src/raft/trigger.rs:64-80`). Documented semantics: *"Logs that are not included in a snapshot
will NOT be purged. In such scenario it will delete as many log as possible. The
`max_in_snapshot_log_to_keep` config is **not** taken into account when purging logs."* And: purging
may be delayed because a replication task may be reading the range.

---

## 5. Metrics available for M5 baseline

**FACT** — `$OR/src/metrics/raft_metrics.rs:19-86`
(<https://docs.rs/openraft/0.9.25/openraft/metrics/struct.RaftMetrics.html>). Obtain with
`raft.metrics() -> tokio::sync::watch::Receiver<RaftMetrics<NodeId, Node>>`.

| Field | Type | Notes for rEtcd metric export |
|---|---|---|
| `running_state` | `Result<(), Fatal<NID>>` | **health gate** — `Err` means RaftCore is dead |
| `id` | `NID` | label |
| `current_term` | `u64` | gauge |
| `vote` | `Vote<NID>` | includes `committed` flag |
| `last_log_index` | `Option<u64>` | gauge |
| `last_applied` | `Option<LogId<NID>>` | gauge (`.index`, `.leader_id.term`) |
| `snapshot` | `Option<LogId<NID>>` | last log id included in the current snapshot |
| `purged` | `Option<LogId<NID>>` | last purged log id, inclusive; *"also the first log id Openraft knows"* |
| `state` | `ServerState` | Learner / Follower / Candidate / Leader |
| `current_leader` | `Option<NID>` | gauge/label |
| `millis_since_quorum_ack` | `Option<u64>` | **leader-only**; ms since last quorum ack. Prime partition-detection signal. `None` if not leader or not yet acked |
| `membership_config` | `Arc<StoredMembership<NID, N>>` | `.log_id()`, `.membership()`, `.voter_ids()`, `.nodes()` |
| `replication` | `Option<BTreeMap<NID, Option<LogId<NID>>>>` | **leader-only**; per-peer *matched* log id. Lag = `last_log_index - matched.index` |

**FACT** — cheaper split channels exist and are worth using to avoid waking on unrelated changes:
`raft.data_metrics() -> watch::Receiver<RaftDataMetrics<NID>>` (`last_log`, `last_applied`,
`snapshot`, `purged`, `millis_since_quorum_ack`, `replication`) and
`raft.server_metrics() -> watch::Receiver<RaftServerMetrics<NID, N>>` (`id`, `vote`, `state`,
`current_leader`, `membership_config`) — `raft_metrics.rs:162-237`, `raft/mod.rs:843-856`.

**Derived metrics rEtcd should export (INFERENCE):**
`log_backlog = last_log_index - last_applied.index`;
`unsnapshotted = last_log_index - snapshot.index`;
`unpurged = last_log_index - purged.index`;
`per_peer_lag{peer} = last_log_index - replication[peer].index`;
`is_leader = state == Leader`; `leader_quorum_ack_ms = millis_since_quorum_ack`.
All are pure functions of the table above — no new openraft API needed.

**FACT** — `raft.with_raft_state(|st| ...)` (`raft/mod.rs:796-820`) gives read-only access to the full
`RaftState` (e.g. `st.committed`) for anything metrics does not expose. It is a round-trip to
RaftCore; do not call it on a hot path.

---

## 6. Linearizable read + the watch barrier

**FACT** — `$OR/src/raft/mod.rs:572-589`:

```rust
pub async fn ensure_linearizable(&self)
    -> Result<Option<LogId<C::NodeId>>, RaftError<C::NodeId, CheckIsLeaderError<C::NodeId, C::Node>>>
{
    let (read_log_id, applied) = self.get_read_log_id().await?;
    if read_log_id.index() > applied.index() {
        self.wait(None).applied_index_at_least(read_log_id.index(), "ensure_linearizable").await
            .map_err(/* Timeout unreachable; ShuttingDown -> Fatal::Stopped */)?;
    }
    Ok(read_log_id)   // <-- the high-water mark
}
```

**FACT** — `Raft::get_read_log_id() -> Result<(Option<LogId>, Option<LogId>), RaftError<..,
CheckIsLeaderError>>` returns `(read_log_id, last_applied_log_id)` (`raft/mod.rs:621-631`).

**FACT** — `$OR/src/docs/protocol/read.md`
(<https://docs.rs/openraft/0.9.25/openraft/docs/protocol/read/index.html>): *"The `read_log_id` is
determined as the maximum of the `last_committed_log_id` and the log id of the first log entry in the
current leader's term (the 'blank' log entry)."* Leadership is confirmed by heartbeating a **quorum**
before returning.

**Answer to "how to capture a high-water applied index after the barrier":**
`ensure_linearizable()`'s own `Ok` value **is** the high-water mark. **INFERENCE:** when it returns
`Ok(Some(read_log_id))`, this node's state machine has `last_applied.index >= read_log_id.index`, and
any read that completed before this call observed state no newer than `read_log_id`. So for M4 watch:

```rust
let hw = raft.ensure_linearizable().await?;                    // Option<LogId>
let hw_index = hw.map(|l| l.index).unwrap_or(0);
// snapshot the keyspace at/after hw_index, then stream watch events with index > hw_index
```

Two caveats, both **FACT**:

- `ensure_linearizable()` waits with `timeout = None`, i.e. `Wait` treats it as ~10 years
  (`raft/mod.rs:858-861`). If rEtcd needs a bounded barrier, use `get_read_log_id()` +
  `wait(Some(d)).applied_index_at_least(idx, msg)` yourself and handle `WaitError::Timeout`.
- It fails with `CheckIsLeaderError` on a non-leader or on quorum loss — rEtcd must route watch
  establishment to the leader, or forward.
- `read.md` states explicitly that comparing **indices** (`last_applied.index() >= read_log_id.index()`)
  is legal, not just full `LogId` comparison — useful because rEtcd's watch cursor is an index.

**UNVERIFIED:** whether a follower/learner can serve a bounded-staleness watch safely in rEtcd's
model. openraft gives no follower-read helper in 0.9.25; `ensure_linearizable` is leader-only.
**To settle:** decide the rEtcd consistency contract for watches (linearizable vs. serializable) in
the M4 ADR; if serializable is acceptable, `metrics.last_applied` on the follower is the cursor and
no openraft API is needed.

---

## 7. Version skew and mixed-version operation

**FACT (source)** — Nothing in `$OR/src/` carries a protocol version number. `AppendEntriesRequest`,
`VoteRequest`, `InstallSnapshotRequest` and their responses have no version field
(`$OR/src/raft/message/*.rs`). Wire compatibility is therefore **structural**: it depends entirely on
the serialization rEtcd chooses for these types, not on anything openraft negotiates.

**FACT (doc)** — `$OR/src/docs/upgrade_guide/upgrade.md` defines the only compatibility contract
openraft offers, by commit-message keyword: `DataChange` = breaking data types, migration needed;
`Change` = breaking API; `Feature`/`Improve`/`Fix` = no application change required. There is no
documented statement of wire compatibility *between* 0.9.x patch releases beyond this.

**FACT (fetched changelog, <https://github.com/databendlabs/openraft/blob/release-0.9/change-log.md>)**
— entries from v0.9.18 to v0.9.25 and their keywords:

| Version | Kind | Substance |
|---|---|---|
| 0.9.25 | Fix ×7 | `VecProgress` ordering above granted threshold (quorum-calculation safety); RaftCore no longer blocks when a response channel closes; empty log range treated as heartbeat instead of panic; `current_leader()` based on committed vote alone; election trigger ignored when already Leader; **snapshot delivery to followers with fully purged logs**; **snapshot install replaces effective membership when the backing log entry is purged** |
| 0.9.24 | Fix | snapshot update gated on actual engine advancement (duplicate-trigger debug assertion) |
| 0.9.23 | Fix ×3 | replication accepts newer snapshot in flight ack (TOCTOU); async-std/Tokio example fix; **bounded snapshot-chunk retry with exponential backoff** |
| 0.9.22 | Fix | redundant election prevented for single-voter node already electing |
| 0.9.20 | Fix | heartbeat conflict messages processed; progress reset retransmits (with `loosen-follower-log-revert`) |
| 0.9.19 | Improve | leader state-reversion tolerance on restart |
| 0.9.18 | **Change** | `declare_raft_types!` no longer auto-detects `serde`; applications add per-type attributes |

Only 0.9.18 is a `Change`; everything after is `Fix`/`Improve`. **INFERENCE:** per openraft's own
keyword contract, no wire-format change occurred between 0.9.18 and 0.9.25, so mixed 0.9.18–0.9.25
peers are protocol-compatible. The two 0.9.25 snapshot fixes are directly load-bearing for M5 — a
cluster running an older 0.9.x would hit the "fully purged log" bugs that 0.9.25 fixes.

**UNVERIFIED:** whether any 0.9.x **before** 0.9.18 is wire-compatible with 0.9.25 (not examined).
**To settle:** read the full `release-0.9` changelog for `DataChange`/`Change` entries below 0.9.18,
or simply declare 0.9.25 the floor for rEtcd, which costs nothing since rEtcd has no deployed fleet.

**INFERENCE — the skew that actually matters for rEtcd is rEtcd's own.** openraft carries rEtcd's
`Command` bytes opaquely as `C::D` inside `Entry`. A newer node proposing a `Command` variant an older
node cannot decode will fail **inside `apply()`**, i.e. at the point of no return — the entry is
already committed. Implications for the M6 mixed-version command gating design:

- The gate must be on **propose**, not on apply. Never commit a command the current committed
  membership cannot all decode.
- The cluster's effective feature level must itself be a **replicated, committed** value (a log
  entry), not gossip-derived state, or two leaders could compute different levels.
- `metrics.membership_config.nodes()` gives the authoritative peer set to reason over; rEtcd's
  `RaftNode` would need a version field to make the level computable from committed state.
- openraft provides no hook for this. It is entirely rEtcd's layer.

**UNVERIFIED:** whether rEtcd's `postcard`/serde encoding of `Command` is forward-compatible for
unknown enum variants. **To settle:** a round-trip test that decodes a newer-variant payload with an
older schema; `postcard` is not self-describing, so the expected answer is "no" — which makes the
propose-side gate mandatory rather than optional.

---

## 8. Traps — known 0.9.25 pitfalls

**T1 — Follower install: the log is purged without waiting for the snapshot to be installed.**
`FollowingHandler::install_full_snapshot` pushes the sm install command and then, in the same engine
output batch, `Command::PurgeLog` (`following_handler/mod.rs:321-327`). `Command::PurgeLog::condition()`
is `None` (`engine/command.rs:192`), and RaftCore only defers commands that carry a condition
(`raft_core.rs:1595-1626`). `Command::StateMachine` is just forwarded to the sm worker's channel, while
`log_store.purge()` is awaited inline. **INFERENCE:** a crash in that window leaves purged logs and an
un-installed snapshot; `get_initial_state`'s repair only handles the opposite order (`helper.rs:107-119`).
*Mitigation:* make `purge` in rEtcd's store **refuse to purge above the state machine's durable
`last_applied`** — `crates/config-storage/src/rocks.rs:1445-1455` already has exactly this guard.
Keep it, promote it from a debug guard to a hard invariant, and cover it with a crash test.

**T2 — Purge assumes snapshot durability that openraft never confirms.** There is no
`SnapshotFlushed` callback analogous to `LogFlushed`. `finish_building_snapshot` schedules purge the
instant `build_snapshot()` returns `Ok`. **INFERENCE:** `build_snapshot` must not return `Ok` until
the snapshot is fsynced and atomically visible to `get_current_snapshot()`, including after a crash.

**T3 — The snapshot builder runs concurrently with `apply`, and blocking it stalls the node.**
`worker.rs:177-192` spawns the build. If rEtcd's builder holds a mutex that `apply` also needs, every
write blocks for the whole build; if it holds nothing, the snapshot content races ahead of its own
`meta.last_log_id`. The correct answer is a point-in-time RocksDB snapshot/checkpoint captured
**inside `get_snapshot_builder()`** (which *is* serialized with `apply` on the worker task).
Related: `build_snapshot` also runs **synchronously on the startup path** (`helper.rs:135-136`) once
anything has been purged, where a long build delays node start.

**T4 — `SnapshotData = Cursor<Vec<u8>>` means the whole snapshot is in RAM, repeatedly.** On the
leader: one full copy per `get_current_snapshot()` handed to replication, per lagging peer, held for
the duration of a transfer, plus `snapshot_max_chunk_size` (default 3 MiB) copied into each
`InstallSnapshotRequest.data: Vec<u8>` and again by the transport's serialization. On the follower:
a full in-RAM buffer while receiving. With rEtcd's config keyspace this is probably fine at first and
becomes a cliff later. `tokio::fs::File` satisfies the same `AsyncRead + AsyncWrite + AsyncSeek +
Unpin` bound and is a drop-in; it needs no feature-flag change.

**T5 — `build_snapshot` returning `Err` kills the node.** `raft_core.rs:1382` propagates it as a
`Fatal`. No retry exists. Absorb transient failures inside the implementation.

**T6 — `replication_lag_threshold` does not do what its doc says.** It only affects
`add_learner(blocking=true)` (§2.1). Tuning it will not make openraft prefer snapshots; purging will.

**T7 — `add_learner(.., blocking=true)` returning `Ok` does not mean the learner is caught up.**
The wait result is logged and dropped (`impl_raft_blocking_write.rs:154-168`). Always verify with the
replication map before `change_membership`.

**T8 — `change_membership` is two round-trips; crashing between them leaves the cluster in joint
consensus** (`impl_raft_blocking_write.rs:46-47`). Detect via
`membership_config.membership().get_joint_config().len() > 1` and re-issue.

**T9 — `ChangeMembers::SetNodes` can cause split-brain.** §3.5. Prefer remove + `add_learner`.

**T10 — The leader errors out if `get_current_snapshot()` returns `None` when a snapshot is needed.**
`replication/mod.rs:745-752` turns it into `StorageError::IO { read_snapshot: "snapshot not found" }`.
Once purge is on, `get_current_snapshot` must always be able to answer.

**T11 — Snapshot chunk size is not negotiated.** An oversized `InstallSnapshot` RPC is only logged
("*too large, but it is not supported yet*", `replication/mod.rs:348-350`). Size
`snapshot_max_chunk_size` against tonic's decode limit with headroom.

**T12 — `Command::PurgeLog` is awaited on the RaftCore task.** A slow `purge` (large RocksDB
`delete_range` + compaction) blocks elections, heartbeats and applies. Keep purge O(fast); prefer a
range delete over per-key deletes, and consider whether compaction should be deferred.

**T13 — Only one snapshot build in flight, cluster-wide-per-node.** `trigger_snapshot` returns
`false` and does nothing if `building_snapshot` is already set (`snapshot_handler/mod.rs:34-37`). A
manual `Raft::trigger_snapshot()` during an automatic build is silently dropped — there is no error
and no queueing.

**T14 — Followers build snapshots too.** `following_handler/mod.rs:179` evaluates the same policy.
Any per-node resource budgeting for snapshot building must assume every node builds, not just the
leader.

**T15 — The network handle is locked for the whole snapshot transfer.** `replication/mod.rs:789`
holds `Arc<Mutex<N::Network>>` across `full_snapshot()`. **INFERENCE:** if rEtcd's
`RaftNetworkFactory` returns a network object that multiplexes ordinary `append_entries` over the
same handle, a large snapshot transfer will block heartbeats to that peer. Give the snapshot path its
own connection/clone.

---

## 9. Open items (consolidated UNVERIFIED list)

| # | Unverified | What would settle it |
|---|---|---|
| U1 | Wire compatibility of 0.9.x releases **older than 0.9.18** with 0.9.25 | Read `release-0.9/change-log.md` below 0.9.18 for `DataChange`/`Change` entries — or declare 0.9.25 the floor (recommended; rEtcd has no deployed fleet) |
| U2 | Whether `postcard`-encoded `Command` tolerates unknown enum variants | A decode test: encode a newer variant, decode with the older schema. Expected "no" (postcard is not self-describing) ⇒ propose-side gating is mandatory |
| U3 | Whether rEtcd watches may be served from a follower | An M4 ADR decision on the watch consistency contract; openraft offers no follower-read helper in 0.9.25 |
| U4 | Actual RSS/latency cost of `Cursor<Vec<u8>>` at rEtcd's expected keyspace size | Measure a synthetic snapshot at target key count before choosing `Cursor` vs `tokio::fs::File` |
| U5 | Whether T1's purge-before-install window is reachable in practice given rEtcd's existing `purge` guard | A crash-injection test: kill the follower between `Command::PurgeLog` completing and `install_snapshot` returning |
| U6 | Behaviour of `openraft::testing::Suite` (`$OR/src/testing/`) against a v2 store — it is written around `Adaptor`/v1 (`testing/store_builder.rs:38-48`) and `storage-v2` disables `Adaptor` | Try to instantiate the suite for rEtcd's `RocksStore`; if it does not apply, M5's snapshot conformance tests must be hand-written |

---

## 10. Quick citation index

| Topic | Path under `$OR` | docs.rs |
|---|---|---|
| `RaftLogStorage`, `RaftStateMachine` | `src/storage/v2.rs` | [`storage::RaftLogStorage`](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftLogStorage.html), [`storage::RaftStateMachine`](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftStateMachine.html) |
| `SnapshotMeta`, `Snapshot`, `LogState`, `RaftSnapshotBuilder`, `RaftLogReader` | `src/storage/mod.rs` | [`storage`](https://docs.rs/openraft/0.9.25/openraft/storage/index.html) |
| `Config`, `SnapshotPolicy` | `src/config/config.rs` | [`Config`](https://docs.rs/openraft/0.9.25/openraft/struct.Config.html) |
| purge arithmetic | `src/engine/handler/log_handler/mod.rs` | — |
| snapshot trigger / update | `src/engine/handler/snapshot_handler/mod.rs` | — |
| build→purge sequencing | `src/engine/engine_impl.rs:566-580` | — |
| sm worker (build concurrency) | `src/core/sm/worker.rs` | — |
| command condition table | `src/engine/command.rs:180-225` | — |
| RaftCore command execution | `src/core/raft_core.rs:1595-1700`, `:1377-1435` | — |
| follower install sequence | `src/engine/handler/following_handler/mod.rs:278-330` | — |
| leader snapshot-vs-log decision | `src/progress/entry/mod.rs:199-245` | — |
| leader snapshot send | `src/replication/mod.rs:724-812` | — |
| `RaftNetwork` | `src/network/network.rs` | [`network::RaftNetwork`](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetwork.html) |
| chunked transport + retry policy | `src/network/snapshot_transport.rs` | [`network::snapshot_transport`](https://docs.rs/openraft/0.9.25/openraft/network/snapshot_transport/index.html) |
| `InstallSnapshotRequest/Response`, `SnapshotResponse` | `src/raft/message/install_snapshot.rs` | [`raft::InstallSnapshotRequest`](https://docs.rs/openraft/0.9.25/openraft/raft/struct.InstallSnapshotRequest.html) |
| `Raft` API | `src/raft/mod.rs` | [`Raft`](https://docs.rs/openraft/0.9.25/openraft/struct.Raft.html) |
| `add_learner`, `change_membership` | `src/raft/impl_raft_blocking_write.rs` | [`Raft::change_membership`](https://docs.rs/openraft/0.9.25/openraft/struct.Raft.html#method.change_membership) |
| `ChangeMembers` | `src/change_members.rs` | [`ChangeMembers`](https://docs.rs/openraft/0.9.25/openraft/enum.ChangeMembers.html) |
| `Trigger` (snapshot / purge_log) | `src/raft/trigger.rs` | [`raft::Trigger`](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Trigger.html) |
| `RaftMetrics`, `RaftDataMetrics`, `RaftServerMetrics` | `src/metrics/raft_metrics.rs` | [`RaftMetrics`](https://docs.rs/openraft/0.9.25/openraft/metrics/struct.RaftMetrics.html) |
| `Wait`, `Metric` | `src/metrics/wait.rs`, `src/metrics/metric.rs` | [`metrics::Wait`](https://docs.rs/openraft/0.9.25/openraft/metrics/struct.Wait.html) |
| `StorageHelper::get_initial_state`, `get_membership` | `src/storage/helper.rs` | [`storage::StorageHelper`](https://docs.rs/openraft/0.9.25/openraft/storage/struct.StorageHelper.html) |
| `StoredMembership` | `src/membership/stored_membership.rs` | [`StoredMembership`](https://docs.rs/openraft/0.9.25/openraft/struct.StoredMembership.html) |
| `RaftTypeConfig` / `SnapshotData` bounds | `src/type_config.rs` | [`RaftTypeConfig`](https://docs.rs/openraft/0.9.25/openraft/trait.RaftTypeConfig.html) |
| snapshot replication protocol | `src/docs/protocol/snapshot_replication.md` | [docs::protocol::snapshot_replication](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/snapshot_replication/index.html) |
| linearizable read | `src/docs/protocol/read.md` | [docs::protocol::read](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/read/index.html) |
| dynamic membership / `SetNodes` warning | `src/docs/cluster_control/dynamic-membership.md` | [docs::cluster_control::dynamic_membership](https://docs.rs/openraft/0.9.25/openraft/docs/cluster_control/dynamic_membership/index.html) |
| feature flags | `src/docs/feature_flags/feature-flags.md` | [docs::feature_flags](https://docs.rs/openraft/0.9.25/openraft/docs/feature_flags/index.html) |
| upgrade / compat keywords | `src/docs/upgrade_guide/upgrade.md` | [docs::upgrade_guide](https://docs.rs/openraft/0.9.25/openraft/docs/upgrade_guide/index.html) |
