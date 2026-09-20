# OpenRaft `=0.9.25` — verified API reference for rEtcd M0–M3

**Date:** 2026-09-17
**Method:** read the real crate source downloaded by cargo, plus the official `release-0.9` examples from GitHub.
**Verification level:** everything below with a file:line citation was read from source. The "minimal skeleton" at the end **compiles cleanly** (`cargo build`, rustc 1.93, openraft 0.9.25, features `serde,storage-v2`). Items marked **UNVERIFIED** were not confirmed by reading source or running code.

**Source paths used:**

- Crate source: `C:\Users\gautamb\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\openraft-0.9.25\`
  (abbreviated below as `$OR`)
- Macro crate: `C:\Users\gautamb\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\openraft-macros-0.9.25\`
- Examples (downloaded): `C:\Users\gautamb\AppData\Local\Temp\orx\ex\`
- Compile-verified skeleton: `C:\Users\gautamb\AppData\Local\Temp\orx\src\main.rs`

---

## 0. Headline findings (read these first)

1. **`storage-v2` is mandatory.** The v2 traits are sealed. `impl<T> Sealed for T` exists **only** under `#[cfg(feature = "storage-v2")]` (`$OR/src/storage/v2.rs:33-34`). Without the feature you physically cannot implement `RaftLogStorage`/`RaftStateMachine`.
2. **`Adaptor` does not exist when `storage-v2` is on.** `#[cfg(not(feature = "storage-v2"))] pub use adapter::Adaptor;` (`$OR/src/storage/mod.rs:15-16`). Same for `RaftStorage` re-export (`$OR/src/lib.rs:108-109`). There is no v1→v2 escape hatch in our configuration — implement v2 directly.
3. **There is no `tokio-rt` feature in 0.9.25.** Tokio is an unconditional dependency (`$OR/Cargo.toml` `[dependencies.tokio]`, no `optional`). The full feature list is exactly: `bench, bt, compat, generic-snapshot-data, loosen-follower-log-revert, serde, single-term-leader, singlethreaded, storage-v2, tracing-log`.
4. **Traits use native AFIT, not `async_trait`.** `#[add_async_trait]` rewrites `async fn` into `fn(...) -> impl Future<Output=...> + Send` and pushes a `Send` supertrait (`openraft-macros-0.9.25/src/lib.rs:71-110`). **Do not put `#[async_trait]` on our impls** — write plain `async fn`. Verified by compiling.
5. **`apply()` must return exactly one response per entry — including `Blank` and `Membership`.** `debug_assert_eq!(n_entries, n_replies)` in `$OR/src/core/sm/worker.rs:151-155`.
6. **`ensure_linearizable()` returns `Result<Option<LogId<NodeId>>, RaftError<NodeId, CheckIsLeaderError<..>>>`** — not `Result<(), _>`. See §5.
7. **With `feature = "serde"`, `AppData`/`AppDataResponse` require `Serialize + DeserializeOwned`.** `AppData: ... + OptionalSerde`, and `OptionalSerde: Serialize + for<'a> Deserialize<'a>` under `serde` (`$OR/src/lib.rs:127-132, 177`). Our `Cmd`/`CmdResp` must derive both.
8. **`Config::validate(self)` consumes `self`** and returns `Result<Config, ConfigError>` (`$OR/src/config/config.rs:291`).
9. **A no-snapshot store is safe only if you also prevent purging.** `get_current_snapshot() -> Ok(None)` is fine, and `build_snapshot()` can be `unreachable!()`, **provided** `SnapshotPolicy::Never` and no manual `trigger().snapshot()`. See §3.6 for the exact chain of reasoning.
10. **`save_committed`/`read_committed` are optional with defaults, but rEtcd §9.3.6 needs them.** Without them, `read_committed()` returns `None`, so committed-but-unapplied entries are **not** replayed at startup. See §8.

---

## 1. `declare_raft_types!` — exact syntax

Macro definition: `$OR/src/raft/mod.rs:143-179`.

```rust
// $OR/src/raft/mod.rs:143-179
#[macro_export]
macro_rules! declare_raft_types {
    // Add a trailing colon to    `declare_raft_types(MyType)`,
    // Make it the standard form: `declare_raft_types(MyType:)`.
    ($(#[$outer:meta])* $visibility:vis $id:ident) => {
        $crate::declare_raft_types!($(#[$outer])* $visibility $id:);
    };

    // The main entry of this macro
    ($(#[$outer:meta])* $visibility:vis $id:ident: $($(#[$inner:meta])* $type_id:ident = $type:ty),* $(,)? ) => {
        $(#[$outer])*
        #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
        $visibility struct $id {}

        impl $crate::RaftTypeConfig for $id {
            // `expand!(KEYED, ...)` ignores the duplicates.
            // Thus by appending default types after user defined types,
            // the absent user defined types are filled with default types.
            $crate::openraft_macros::expand!(
                KEYED,
                (T, ATTR, V) => {ATTR type T = V;},
                $(($type_id, $(#[$inner])*, $type),)*

                // Default types:
                (D            , , String                                ),
                (R            , , String                                ),
                (NodeId       , , u64                                   ),
                (Node         , , $crate::impls::BasicNode              ),
                (Entry        , , $crate::impls::Entry<Self>            ),
                (SnapshotData , , Cursor<Vec<u8>>                       ),
                (Responder    , , $crate::impls::OneshotResponder<Self> ),
                (AsyncRuntime , , $crate::impls::TokioRuntime           ),
            );
        }
    };
}
```

**All eight fields are optional.** Defaults are exactly as listed above. Note `SnapshotData`'s default is the bare token `Cursor<Vec<u8>>` — **you must have `std::io::Cursor` in scope** at the macro call site if you omit `SnapshotData`. Doc comments are allowed on the type alias (`$(#[$inner])*`) and on the struct (`$(#[$outer])*`).

The generated struct derives `Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd` — required by `RaftTypeConfig`'s supertraits (`$OR/src/type_config.rs:49-51`).

### `RaftTypeConfig` associated types

```rust
// $OR/src/type_config.rs:49-107
pub trait RaftTypeConfig:
    Sized + OptionalSend + OptionalSync + Debug + Clone + Copy + Default + Eq + PartialEq + Ord + PartialOrd + 'static
{
    type D: AppData;
    type R: AppDataResponse;
    type NodeId: NodeId;
    type Node: Node;
    type Entry: RaftEntry<Self::NodeId, Self::Node> + FromAppData<Self::D>;

    #[cfg(not(feature = "generic-snapshot-data"))]
    type SnapshotData: tokio::io::AsyncRead
        + tokio::io::AsyncWrite
        + tokio::io::AsyncSeek
        + OptionalSend
        + Unpin
        + 'static;

    #[cfg(feature = "generic-snapshot-data")]
    type SnapshotData: OptionalSend + 'static;

    type AsyncRuntime: AsyncRuntime;
    type Responder: Responder<Self>;
}
```

Recommended rEtcd form (compile-verified):

```rust
openraft::declare_raft_types!(
    pub TypeConfig:
        D            = Cmd,
        R            = CmdResp,
        NodeId       = u64,
        Node         = openraft::BasicNode,   // or our own Node struct
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        Responder    = openraft::impls::OneshotResponder<TypeConfig>,
        AsyncRuntime = openraft::TokioRuntime,
);
```

`Responder` trait (`$OR/src/raft/responder/mod.rs:23-39`):

```rust
pub trait Responder<C>: OptionalSend + 'static
where C: RaftTypeConfig
{
    type Receiver;
    fn from_app_data(app_data: C::D) -> (C::D, Self, Self::Receiver) where Self: Sized;
    fn send(self, result: ClientWriteResult<C>);
}
```

`OneshotResponder<C>::Receiver = OneshotReceiverOf<C, ClientWriteResult<C>>` (`$OR/src/raft/responder/impls.rs:34`). Keep the default unless we want fire-and-forget writes via `client_write_ff`.

Useful re-exports in `openraft::impls` (`$OR/src/impls/mod.rs`): `TokioRuntime, Entry, BasicNode, EmptyNode, OneshotResponder`.

---

## 2. `RaftLogStorage<C>` — exact signatures

File: `$OR/src/storage/v2.rs`.

```rust
// $OR/src/storage/v2.rs:25-35
pub(crate) mod sealed {
    pub trait Sealed {}

    /// Implement non-public trait [`Sealed`] for all types so that [`RaftLogStorage`] and
    /// [`RaftStateMachine`] can be implemented by 3rd party crates.
    #[cfg(feature = "storage-v2")]
    impl<T> Sealed for T {}
}
```

```rust
// $OR/src/storage/v2.rs:49-144  (doc comments trimmed; contract notes preserved below)
#[add_async_trait]
pub trait RaftLogStorage<C>: Sealed + RaftLogReader<C> + OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    /// Log reader type.
    type LogReader: RaftLogReader<C>;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>;

    async fn get_log_reader(&mut self) -> Self::LogReader;

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>>;

    async fn save_committed(&mut self, _committed: Option<LogId<C::NodeId>>) -> Result<(), StorageError<C::NodeId>> {
        // By default `committed` log id is not saved
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        // By default `committed` log id is not saved and this method just return None.
        Ok(None)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend;

    /// Truncate logs since `log_id`, inclusive
    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    /// Purge logs upto `log_id`, inclusive
    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;
}
```

### Trait-level correctness contract (verbatim, `$OR/src/storage/v2.rs:43-48`)

> - Logs must be consecutive, i.e., there must **NOT** leave a **hole** in logs.
> - All write-IO must be serialized, i.e., the internal implementation must **NOT** apply a latter write request before a former write request is completed. This rule applies to both `vote` and `log` IO. E.g., Saving a vote and appending a log entry must be serialized too.

This maps 1:1 onto rEtcd §9.3 invariants 1–4. Note the *serialization* requirement covers **vote and log together**, not just log.

### Per-method contract notes from source

| Method | Contract (source lines) |
|---|---|
| `get_log_state` | "The impl should **not** consider the applied log id in state machine. The returned `last_log_id` could be the log id of the last present log entry, or the `last_purged_log_id` if there is no entry at all." (`:59-66`) |
| `get_log_reader` | "intentionally async to give the implementation a chance to use asynchronous primitives to serialize access to the common internal object" (`:68-72`) |
| `save_vote` | "The vote must be persisted on disk before returning." (`:74-79`) — rEtcd §9.3.1 |
| `save_committed` | Optional. If the SM flushes before `apply()` returns you *may* skip it; otherwise "your application has to deal with state reversion of state machine carefully upon restart" (`:84-100`) |
| `append` | "returns immediately after saving the input log entries in memory, and calls the `callback` when the entries are persisted on disk, i.e., avoid blocking." **"When this method returns, the entries must be readable"**; **"When the `callback` is called, the entries must be persisted on disk"**; "the `callback` can be called either before or after this method returns" (`:108-129`) |
| `truncate` | "Truncate logs since `log_id`, inclusive… must not leave a **hole**" (`:131-136`) |
| `purge` | "Purge logs upto `log_id`, inclusive… must not leave a **hole**" (`:138-143`) |

### `LogFlushed<C>` callback

```rust
// $OR/src/storage/callback.rs:14-47
pub struct LogFlushed<C>
where C: RaftTypeConfig
{
    log_io_id: LogIOId<C::NodeId>,
    tx: OneshotSenderOf<C, Result<LogIOId<C::NodeId>, io::Error>>,
}

impl<C> LogFlushed<C>
where C: RaftTypeConfig
{
    /// Report log io completion event.
    ///
    /// It will be called when the log is successfully appended to the storage or an error occurs.
    pub fn log_io_completed(self, result: Result<(), io::Error>) { /* ... */ }
}
```

Only **one** public method: `log_io_completed(Result<(), std::io::Error>)`. It consumes `self`. `LogFlushed::new` is `pub(crate)` — you cannot construct one. The `log_io_id` is private and set by openraft; it is what gets echoed back on success.

**rEtcd note:** §9.3.4 ("a log-flushed notification fires only after the promised durable boundary") is exactly `log_io_completed(Ok(()))` after the RocksDB WAL sync. On a sync failure, call `log_io_completed(Err(io_err))` — openraft turns that into a `StorageIOError::write_logs` / `Fatal::StorageError` and shuts the node down (see `raft_log_storage_ext.rs:36-39` for the same mapping in the blocking helper).

### `LogState<C>`

```rust
// $OR/src/storage/mod.rs:138-149
/// The state about logs.
///
/// Invariance: last_purged_log_id <= last_applied <= last_log_id
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogState<C: RaftTypeConfig> {
    pub last_purged_log_id: Option<LogId<C::NodeId>>,
    pub last_log_id: Option<LogId<C::NodeId>>,
}
```

Exported both as `openraft::LogState` (`$OR/src/lib.rs:105`) and `openraft::storage::LogState`.

### `RaftLogReader<C>` — the `LogReader` type

```rust
// $OR/src/storage/mod.rs:158-189
#[add_async_trait]
pub trait RaftLogReader<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    /// Get a series of log entries from storage.
    ///
    /// The start value is inclusive in the search and the stop value is non-inclusive: `[start, stop)`.
    ///
    /// Entry that is not found is allowed.
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>;

    /// Returns log entries within range `[start, end)`, `end` is exclusive,
    /// potentially limited by implementation-defined constraints.
    ///
    /// If the specified range is too large, the implementation may return only the first few log
    /// entries to ensure the result is not excessively large.
    ///
    /// It must not return empty result if the input range is not empty.
    ///
    /// The default implementation just returns the full range of log entries.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        self.try_get_log_entries(start..end).await
    }
}
```

`RaftLogStorage<C>: RaftLogReader<C>` — so the store itself must also be a reader. The `LogReader` associated type is used by **replication tasks concurrently** with the main store (`$OR/src/storage/v2.rs:53-57`). Both examples set `type LogReader = Self;` and return `self.clone()` from `get_log_reader()` — the store is an `Arc`-wrapped handle.

**0.9.25 note:** if a store violates the `limited_get_log_entries` non-empty contract, 0.9.25 no longer panics the replication task — it treats the empty read as a heartbeat, sleeps 10 ms and warns (changelog `546ed868`). Don't rely on it; keep the default impl.

### `RaftLogStorageExt` — test-only convenience

```rust
// $OR/src/storage/v2/raft_log_storage_ext.rs:17-49
#[add_async_trait]
pub trait RaftLogStorageExt<C>: RaftLogStorage<C>
where C: RaftTypeConfig
{
    /// Blocking mode append log entries to the storage.
    ///
    /// It blocks until the callback is called by the underlying storage implementation.
    async fn blocking_append<I>(&mut self, entries: I) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    { /* ... */ }
}

impl<C, T> RaftLogStorageExt<C> for T where T: RaftLogStorage<C>, C: RaftTypeConfig {}
```

Blanket-implemented. Handy for M2 crash-injection tests that need a synchronous append.

---

## 3. `RaftStateMachine<C>` — exact signatures

```rust
// $OR/src/storage/v2.rs:152-257  (doc comments trimmed)
#[add_async_trait]
pub trait RaftStateMachine<C>: Sealed + OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    /// Snapshot builder type.
    type SnapshotBuilder: RaftSnapshotBuilder<C>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<C::NodeId>>, StoredMembership<C::NodeId, C::Node>), StorageError<C::NodeId>>;

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend;

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder;

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>>;

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C::NodeId, C::Node>,
        snapshot: Box<C::SnapshotData>,
    ) -> Result<(), StorageError<C::NodeId>>;

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>>;
}
```

Note: `apply` takes `I: IntoIterator<Item = C::Entry>` **by value** — entries are moved in, so you can destructure `ent.payload` without cloning.

### 3.1 `applied_state` contract (`$OR/src/storage/v2.rs:162-173`)

> Returns the last applied log id which is recorded in state machine, and the last applied membership config.
>
> ### Correctness requirements
>
> It is all right to return a membership with greater log id than the last-applied-log-id. Because upon startup, the last membership will be loaded by scanning logs from the `last-applied-log-id`.

### 3.2 `apply` contract (`$OR/src/storage/v2.rs:175-204`)

> For every entry to apply, an implementation should:
> - Store the log id as last applied log id.
> - Deal with the business logic log.
> - Store membership config if `RaftEntry::get_membership()` returns `Some`.
>
> Note that for a membership log, the implementation need to do nothing about it, except storing it.
>
> An implementation may choose to persist either the state machine or the snapshot:
> - An implementation with persistent state machine: persists the state on disk before returning from `apply()`. So that a snapshot does not need to be persistent.
> - An implementation with persistent snapshot: `apply()` does not have to persist state on disk. But every snapshot has to be persistent. And when starting up the application, the state machine should be rebuilt from the last snapshot.

**rEtcd takes the first option** (persistent state machine, no snapshot), matching §9.3.5.

### 3.3 Response arity — hard requirement

```rust
// $OR/src/core/sm/worker.rs:145-155
let n_entries = applying_entries.len();

let apply_results = self.state_machine.apply(entries).await?;

let n_replies = apply_results.len();

debug_assert_eq!(
    n_entries, n_replies,
    "n_entries: {} should equal n_replies: {}",
    n_entries, n_replies
);
```

**One `C::R` per entry, in the same order, for all three payload kinds.** In release builds `debug_assert_eq!` is compiled out, and the response index would silently shift — a client would receive another entry's response. Treat this as a correctness invariant with our own `assert!` or careful construction.

### 3.4 `EntryPayload` handling

```rust
// $OR/src/entry/payload.rs:9-20
/// Log entry payload variants.
#[derive(PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum EntryPayload<C: RaftTypeConfig> {
    /// An empty payload committed by a new cluster leader.
    Blank,

    Normal(C::D),

    /// A change-membership log entry.
    Membership(Membership<C::NodeId, C::Node>),
}
```

| Variant | rEtcd must do |
|---|---|
| `Blank` | Update `last_applied` only. **Allocate no public revision.** Push a neutral `CmdResp`. Emitted by every new leader on election. |
| `Normal(Cmd)` | Run the deterministic KV mutation, allocate a revision only on a real state change (§7.4 / M0 acceptance), push the real response. |
| `Membership(m)` | `last_membership = StoredMembership::new(Some(ent.log_id), m)`. **No revision.** Push a neutral `CmdResp`. |

Both official examples do exactly this (`ex/examples_raft-kv-memstore_src_store_mod.rs:153-173`, `ex/examples_raft-kv-rocksdb_src_store.rs:216-237`).

### 3.5 `StoredMembership`

```rust
// $OR/src/membership/stored_membership.rs:19-58
#[derive(Clone, Debug, Default)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct StoredMembership<NID, N>
where N: Node, NID: NodeId,
{
    log_id: Option<LogId<NID>>,
    membership: Membership<NID, N>,
}

impl<NID, N> StoredMembership<NID, N> where N: Node, NID: NodeId {
    pub fn new(log_id: Option<LogId<NID>>, membership: Membership<NID, N>) -> Self;
    pub fn log_id(&self) -> &Option<LogId<NID>>;
    pub fn membership(&self) -> &Membership<NID, N>;
    pub fn voter_ids(&self) -> impl Iterator<Item = NID>;
    pub fn nodes(&self) -> impl Iterator<Item = (&NID, &N)>;
}
```

Fields are **private**; use `new()` + accessors. `Default` gives an uninitialized membership. It is `Serialize`/`Deserialize` under `serde` with `serde(bound = "")`, so it round-trips into the `state_meta` column family directly.

### 3.6 Minimal implementation with NO snapshot support — is it safe?

**Short answer: yes, if and only if you also guarantee no purge ever happens.** Chain of reasoning, each step verified:

1. **Can `get_current_snapshot()` return `Ok(None)`?** Yes. `Option<Snapshot<C>>` is the signature, and `StorageHelper::get_initial_state` handles `None` (`$OR/src/storage/helper.rs:128-144`).

2. **But there is a startup trap:**
   ```rust
   // $OR/src/storage/helper.rs:128-143
   let snapshot = self.state_machine.get_current_snapshot().await?;

   // If there is not a snapshot and there are logs purged, which means the snapshot is not persisted,
   // we just rebuild it so that replication can use it.
   let snapshot = match snapshot {
       None => {
           if last_purged_log_id.is_some() {
               let mut b = self.state_machine.get_snapshot_builder().await;
               let s = b.build_snapshot().await?;
               Some(s)
           } else {
               None
           }
       }
       s @ Some(_) => s,
   };
   ```
   `build_snapshot()` is called at startup **iff `last_purged_log_id.is_some()`**. So `unreachable!()` in `build_snapshot` is safe only while the log has never been purged.

3. **And a replication trap:**
   ```rust
   // $OR/src/replication/mod.rs:733-751
   let snapshot = self.snapshot_reader.get_snapshot().await.map_err(...)?;
   ...
   let snapshot = match snapshot {
       None => {
           let io_err = StorageIOError::read_snapshot(None, AnyError::error("snapshot not found"));
           let sto_err = StorageError::IO { source: io_err };
           return Err(ReplicationError::StorageError(sto_err));
       }
       Some(x) => x,
   };
   ```
   If openraft ever decides to replicate by snapshot and `get_current_snapshot()` gives `None`, the node takes a `StorageError` — i.e. fatal.

4. **When does openraft decide to replicate by snapshot?** Only when the follower's needed log range has been **purged** (`$OR/src/progress/entry/mod.rs:206-240`):
   - condition 1: `self.searching_end < purge_upto_next` — every candidate matching position is purged;
   - condition 2: still probing but the leader's log is fully purged so a probe carries no entry.

   Both require a non-`None` `purge_upto`. `replication_lag_threshold` is **not** part of this decision — grep shows its only non-test use is in `Raft::wait`-style learner catch-up accounting at `$OR/src/raft/mod.rs:772`, not in snapshot selection.

5. **When does purging happen?**
   ```rust
   // $OR/src/engine/handler/log_handler/mod.rs:78-84
   pub(crate) fn calc_purge_upto(&self) -> Option<LogId<C::NodeId>> {
       let st = &self.state;
       let max_keep = self.config.max_in_snapshot_log_to_keep;
       let batch_size = self.config.purge_batch_size;

       let purge_end = self.state.snapshot_meta.last_log_id.next_index().saturating_sub(max_keep);
       ...
       if st.last_purged_log_id().next_index() + batch_size > purge_end { return None; }
   ```
   `purge_end` derives from `snapshot_meta.last_log_id`. **"Only log included in snapshot will be purged."** With no snapshot ever built, `snapshot_meta.last_log_id == None`, `next_index() == 0`, `purge_end == 0`, and `0 + purge_batch_size > 0` → always `None` → **never purges**.

6. **Is snapshot building ever triggered?** Two call sites, both gated by `SnapshotPolicy::should_snapshot()`:
   `$OR/src/engine/handler/following_handler/mod.rs:179-180` and `$OR/src/engine/handler/replication_handler/mod.rs:243-244`. And:
   ```rust
   // $OR/src/config/config.rs:39-48
   impl SnapshotPolicy {
       pub(crate) fn should_snapshot<NID>(&self, state: &...) -> bool {
           match self {
               SnapshotPolicy::LogsSinceLast(threshold) => {
                   state.committed().next_index() >= state.snapshot_last_log_id().next_index() + threshold
               }
               SnapshotPolicy::Never => false,
           }
       }
   }
   ```
   `SnapshotPolicy::Never` → never. The only other path is the explicit `raft.trigger().snapshot()` external command (`$OR/src/core/raft_core.rs:1221`).

**Conclusion (M1/M2 configuration rule):**

```
snapshot_policy = SnapshotPolicy::Never
AND never call raft.trigger().snapshot()
AND never call raft.trigger().purge_log(..)
  ⇒ get_current_snapshot() -> Ok(None) is safe for the whole node lifetime
  ⇒ build_snapshot() / install_snapshot() may be unreachable!()
  ⇒ begin_receiving_snapshot() must still return a real Box<SnapshotData> (it is cheap: Cursor::new(vec![]))
```

Note `Raft::trigger()` and `RuntimeConfigHandle` are public — enforce "never call" as a rEtcd-side rule (do not expose `trigger()` through our own API).

`max_in_snapshot_log_to_keep = 0` in `Config` does **not** by itself enable purging (see step 5 arithmetic) but I recommend leaving it at the default `1000` for M1/M2 since it changes nothing while `snapshot_policy = Never`, and gives headroom if snapshots land in M5+. **UNVERIFIED:** whether `max_in_snapshot_log_to_keep = 0` plus a future manual snapshot trigger would purge aggressively enough to break a lagging follower — not tested.

### 3.7 `RaftSnapshotBuilder` and `Snapshot`/`SnapshotMeta`

```rust
// $OR/src/storage/mod.rs:199-218
#[add_async_trait]
pub trait RaftSnapshotBuilder<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<C::NodeId>>;
}
```

```rust
// $OR/src/storage/mod.rs:45-62
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct SnapshotMeta<NID, N>
where NID: NodeId, N: Node,
{
    pub last_log_id: Option<LogId<NID>>,
    pub last_membership: StoredMembership<NID, N>,
    pub snapshot_id: SnapshotId,       // = String, $OR/src/raft_types.rs
}

// $OR/src/storage/mod.rs:109-119
#[derive(Debug, Clone)]
pub struct Snapshot<C>
where C: RaftTypeConfig
{
    pub meta: SnapshotMeta<C::NodeId, C::Node>,
    pub snapshot: Box<C::SnapshotData>,
}
```

`SnapshotMeta::signature() -> SnapshotSignature<NID>` and `last_log_id() -> Option<&LogId<NID>>` (`$OR/src/storage/mod.rs:95-107`). Fields are public, so `Snapshot { meta, snapshot }` struct-literal construction works (`Snapshot::new` is `pub(crate)`).

### 3.8 `Adaptor` — present in 0.9.25?

**Present in the crate, but NOT available to us.**

```rust
// $OR/src/storage/mod.rs:3-4, 15-16
#[cfg(not(feature = "storage-v2"))]
pub(crate) mod adapter;
...
#[cfg(not(feature = "storage-v2"))]
pub use adapter::Adaptor;
```

`$OR/src/storage/adapter.rs` exists, and `RaftStorage` (v1) is defined at `$OR/src/storage/mod.rs:234-417` with
`#[cfg_attr(feature = "storage-v2", deprecated(since = "0.8.4", note = "use `RaftLogStorage` and `RaftStateMachine` instead"))]`.
But since we need `storage-v2` for the `Sealed` blanket impl, **`Adaptor` and `openraft::RaftStorage` are both cfg'd out**. The feature doc confirms: *"This feature disables `Adapter`, which is for v1 storage to be used as v2."*

---

## 4. `RaftNetworkFactory<C>` / `RaftNetwork<C>`

```rust
// $OR/src/network/factory.rs:16-33
#[add_async_trait]
pub trait RaftNetworkFactory<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    /// Actual type of the network handling a single connection.
    type Network: RaftNetwork<C>;

    /// Create a new network instance sending RPCs to the target node.
    ///
    /// This function should **not** create a connection but rather a client that will connect when
    /// required. ... But this method does not return an error because openraft can only ignore it.
    async fn new_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network;
}
```

```rust
// $OR/src/network/network.rs:36-163
#[add_async_trait]
pub trait RaftNetwork<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    /// Send an AppendEntries RPC to the target.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>>;

    /// Send an InstallSnapshot RPC to the target.
    #[cfg(not(feature = "generic-snapshot-data"))]
    async fn install_snapshot(
        &mut self,
        _rpc: crate::raft::InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        crate::raft::InstallSnapshotResponse<C::NodeId>,
        RPCError<C::NodeId, C::Node, RaftError<C::NodeId, crate::error::InstallSnapshotError>>,
    >;

    /// Send a RequestVote RPC to the target.
    async fn vote(
        &mut self,
        rpc: VoteRequest<C::NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>>;

    // If generic-snapshot-data disabled,
    // provide a default implementation that relies on AsyncRead + AsyncSeek + Unpin
    #[cfg(not(feature = "generic-snapshot-data"))]
    async fn full_snapshot(
        &mut self,
        vote: Vote<C::NodeId>,
        snapshot: Snapshot<C>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C::NodeId>, StreamingError<C, Fatal<C::NodeId>>> {
        use crate::network::snapshot_transport::Chunked;
        use crate::network::snapshot_transport::SnapshotTransport;

        let resp = Chunked::send_snapshot(self, vote, snapshot, cancel, option).await?;
        Ok(resp)
    }

    /// Build a backoff instance if the target node is temporarily(or permanently) unreachable.
    /// By default it returns a constant backoff of 500 ms.
    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(500)))
    }
}
```

### Feature gating of the snapshot methods (`$OR/src/network/network.rs:48-148`)

| Feature | `install_snapshot` | `full_snapshot` |
|---|---|---|
| `generic-snapshot-data` **off** (our case) | **required** (no default body) | default impl provided — chunks via `install_snapshot` |
| `generic-snapshot-data` **on** | `#[deprecated]`, default body `unimplemented!()` | **required** (no default body) |

And from the feature doc: with `generic-snapshot-data` on, **`Raft::install_snapshot()` (the receiving side) is not available**.

**rEtcd decision:** keep `generic-snapshot-data` **off**. `SnapshotData = Cursor<Vec<u8>>` satisfies the `AsyncRead + AsyncWrite + AsyncSeek + Unpin` bound. We must still write an `install_snapshot` RPC method in the gRPC network impl to satisfy the trait — it can be a `todo!()`/typed-error stub for M1–M3 since §3.6 proves it is never invoked. Prefer returning an error over `todo!()` in shipped code.

### `RPCOption`

```rust
// $OR/src/network/rpc_option.rs:7-54
#[derive(Clone, Debug)]
pub struct RPCOption {
    hard_ttl: Duration,                      // private
    pub(crate) snapshot_chunk_size: Option<usize>,
}

impl RPCOption {
    pub fn new(hard_ttl: Duration) -> Self;
    /// `soft_ttl` is 3/4 of `hard_ttl` but it may change in future, do not rely on this ratio.
    pub fn soft_ttl(&self) -> Duration;
    /// When exceeding this limit, the RPC will be dropped by Openraft at once.
    pub fn hard_ttl(&self) -> Duration;
    pub fn snapshot_chunk_size(&self) -> Option<usize>;
}
```

Map `hard_ttl()` onto the tonic request deadline, and start graceful cancellation at `soft_ttl()`. This is the natural hook for rEtcd §16 / M3 `DeadlineExceededUnknownOutcome` on the **peer** plane.

### Error types the network impl must produce

```rust
// $OR/src/error.rs:277-292
pub enum RPCError<NID: NodeId, N: Node, E: Error = Infallible> {
    Timeout(#[from] Timeout<NID>),
    /// The node is temporarily unreachable and should backoff before retrying.
    Unreachable(#[from] Unreachable),
    /// The RPC payload is too large and should be split into smaller chunks.
    PayloadTooLarge(#[from] PayloadTooLarge),
    /// Failed to send the RPC request and should retry immediately.
    Network(#[from] NetworkError),
    RemoteError(#[from] RemoteError<NID, N, E>),
}
```

```rust
// $OR/src/error.rs:36-47
pub enum RaftError<NID, E = Infallible>
where NID: NodeId
{
    APIError(E),
    Fatal(#[from] Fatal<NID>),
}

// $OR/src/error.rs:133-142  (approx; read at :125-180 offset block)
pub enum Fatal<NID> where NID: NodeId {
    StorageError(#[from] StorageError<NID>),
    Panicked,
    /// Raft stopped normally.
    Stopped,
}
```

```rust
// $OR/src/error.rs:374-412
/// Error that indicates a **temporary** network error and when it is returned, Openraft will retry
/// immediately.
pub struct NetworkError { source: AnyError }
impl NetworkError { pub fn new<E: Error + 'static>(e: &E) -> Self; }

/// Error indicating a node is unreachable. Retries should be delayed.
/// ... Openraft will invoke [`backoff()`] to implement a delay before attempting to resend
pub struct Unreachable { source: AnyError }
impl Unreachable { pub fn new<E: Error + 'static>(e: &E) -> Self; }
```

```rust
// $OR/src/error.rs:319-337
pub struct RemoteError<NID: NodeId, N: Node, T: Error> {
    pub target: NID,
    pub target_node: Option<N>,
    pub source: T,
}
impl RemoteError { pub fn new(target: NID, e: T) -> Self; pub fn new_with_node(target: NID, node: N, e: T) -> Self; }
```

**Semantic difference that matters for rEtcd:** `NetworkError` → openraft retries **immediately**; `Unreachable` → openraft sleeps per `backoff()` first. The memstore example maps connect-refused to `Unreachable` and everything else to `NetworkError`:

```rust
// ex/examples_raft-kv-memstore_src_network_raft_network_impl.rs:45-52
let resp = client.post(url).json(&req).send().await.map_err(|e| {
    // If the error is a connection error, we return `Unreachable` so that connection isn't retried
    // immediately.
    if e.is_connect() {
        return openraft::error::RPCError::Unreachable(Unreachable::new(&e));
    }
    openraft::error::RPCError::Network(NetworkError::new(&e))
})?;
```

`PayloadTooLarge::new_entries_hint(entries_hint: u64)` (`$OR/src/error.rs:496`) lets the network tell openraft to split an `AppendEntries` into smaller chunks — useful if rEtcd hits the tonic max-message size. "If the request cannot be divided (contains only one entry), Openraft interprets it as `Unreachable`."

Convenient type aliases, as both examples do (`ex/examples_raft-kv-rocksdb_src_lib.rs:51-69`):

```rust
pub mod typ {
    pub type Entry = openraft::Entry<TypeConfig>;
    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<NodeId, Node, RaftError<E>>;
    pub type ClientWriteError  = openraft::error::ClientWriteError<NodeId, Node>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, Node>;
    pub type ForwardToLeader    = openraft::error::ForwardToLeader<NodeId, Node>;
    pub type InitializeError    = openraft::error::InitializeError<NodeId, Node>;
    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}
```

---

## 5. `Raft` API

### 5.1 `Raft::new`

```rust
// $OR/src/raft/mod.rs:229-237
#[tracing::instrument(level="debug", skip_all, fields(cluster=%config.cluster_name))]
pub async fn new<LS, N, SM>(
    id: C::NodeId,
    config: Arc<Config>,
    network: N,
    mut log_store: LS,
    mut state_machine: SM,
) -> Result<Self, Fatal<C::NodeId>>
where
    N: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
```

Argument order is `(id, config, network, log_store, state_machine)`. `Raft<C>` is `Clone` (cheap, `Arc<RaftInner<C>>`, `$OR/src/raft/mod.rs:200-205`).

What `new` does, in order (`$OR/src/raft/mod.rs:242-325`):
1. spawns the tick task at `Duration::from_millis(config.heartbeat_interval * 3 / 2)` with `config.enable_tick`;
2. calls `StorageHelper::new(&mut log_store, &mut state_machine).get_initial_state()` — **this is the restart-replay point** (§8);
3. spawns the state-machine worker task (owns `state_machine` from then on);
4. spawns `RaftCore::main`.

**Must be called inside a Tokio runtime** — it calls `C::AsyncRuntime::spawn` three times.

### 5.2 `initialize` — semantics and idempotency

```rust
// $OR/src/raft/mod.rs:711-729
#[tracing::instrument(level = "debug", skip(self))]
pub async fn initialize<T>(
    &self,
    members: T,
) -> Result<(), RaftError<C::NodeId, InitializeError<C::NodeId, C::Node>>>
where
    T: IntoNodes<C::NodeId, C::Node> + Debug,
```

Doc (`$OR/src/raft/mod.rs:690-710`), key sentences verbatim:

> This command should be called on pristine nodes — where the log index is 0 and the node is in Learner state — as if either of those constraints are false, it indicates that the cluster is already formed and in motion. **If `InitializeError::NotAllowed` is returned from this function, it is safe to ignore**, as it simply indicates that the cluster is already up and running.
>
> Once a node successfully initialized it will commit a new membership config log entry to store. Then it starts to work, i.e., entering Candidate state and try electing itself as the leader.
>
> **More than one node performing `initialize()` with the same config is safe, with different config will result in split brain condition.**

Error type:

```rust
// $OR/src/error.rs:231-243  (read via the :125-320 block)
pub enum InitializeError<NID, N>
where NID: NodeId, N: Node,
{
    NotAllowed(#[from] NotAllowed<NID>),
    NotInMembers(#[from] NotInMembers<NID, N>),
}

// $OR/src/error.rs:627-641
pub struct NotAllowed<NID: NodeId> {
    pub last_log_id: Option<LogId<NID>>,
    pub vote: Vote<NID>,
}
pub struct NotInMembers<NID, N> where NID: NodeId, N: Node {
    pub node_id: NID,
    pub membership: Membership<NID, N>,
}
```

So: **not idempotent in the "returns Ok twice" sense**, but idempotent in effect — the second call returns `RaftError::APIError(InitializeError::NotAllowed(..))`, which you swallow. `NotInMembers` means *this node's own id is not in the members map you passed* — a configuration bug, not a benign race.

`T: IntoNodes<NodeId, Node>` — `BTreeMap<NodeId, Node>` works, and so does `BTreeSet<NodeId>` when `Node: Default` (`$OR/src/membership/into_nodes.rs`, **UNVERIFIED** for the `BTreeSet` case; `BTreeMap` verified by compiling).

Also useful:

```rust
// $OR/src/raft/mod.rs:682-688
/// Return `true` if this node is already initialized and can not be initialized again with
/// [`Raft::initialize`]
pub async fn is_initialized(&self) -> Result<bool, Fatal<C::NodeId>>
```

**rEtcd §13.1 mapping:** check `is_initialized()` first; if false and the operator-supplied genesis config + cluster id match, call `initialize(BTreeMap<NodeId, Node>)` on all three fresh nodes with **byte-identical** membership; treat `NotAllowed` as success and `NotInMembers` as fatal misconfiguration. "An empty data directory never self-forms" is satisfied because openraft never initializes on its own — a pristine node stays a Learner until `initialize()` is called.

### 5.3 `client_write`

```rust
// $OR/src/raft/mod.rs:650-665
#[tracing::instrument(level = "debug", skip(self, app_data))]
pub async fn client_write<E>(
    &self,
    app_data: C::D,
) -> Result<ClientWriteResponse<C>, RaftError<C::NodeId, ClientWriteError<C::NodeId, C::Node>>>
where
    ResponderReceiverOf<C>: Future<Output = Result<ClientWriteResult<C>, E>>,
    E: Error + OptionalSend,
```

The `<E>` generic is inferred from `C::Responder::Receiver`. With the default `OneshotResponder` it is the oneshot recv error; you write `raft.client_write(cmd).await` and never name `E`.

```rust
// $OR/src/raft/message/client_write.rs:13-31
pub type ClientWriteResult<C> = Result<ClientWriteResponse<C>, ClientWriteError<NodeIdOf<C>, NodeOf<C>>>;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = "C::R: crate::AppDataResponse"))]
pub struct ClientWriteResponse<C: RaftTypeConfig> {
    /// The id of the log that is applied.
    pub log_id: LogId<C::NodeId>,

    /// Application specific response data.
    pub data: C::R,

    /// If the log entry is a change-membership entry.
    pub membership: Option<Membership<C::NodeId, C::Node>>,
}
```

Accessors added in 0.9.5: `log_id()`, `response()`, `membership()` (`:47-61`). Fields are public too.

```rust
// $OR/src/error.rs:188-199
pub enum ClientWriteError<NID, N> where NID: NodeId, N: Node {
    ForwardToLeader(#[from] ForwardToLeader<NID, N>),
    /// When writing a change-membership entry.
    ChangeMembershipError(#[from] ChangeMembershipError<NID>),
}
```

Fire-and-forget variant:

```rust
// $OR/src/raft/mod.rs:673-680
pub async fn client_write_ff(&self, app_data: C::D) -> Result<ResponderReceiverOf<C>, Fatal<C::NodeId>>
```

**0.9.25 fix relevant here:** `client_write` used to hang forever if the responder was dropped without a reply (e.g. the SM worker panicked). 0.9.25 bounds that wait to ~1 s and then returns `Fatal::Stopped` (changelog `211b91ba`). Good for our M3 `DeadlineExceededUnknownOutcome` story — but note that a `Fatal::Stopped` from `client_write` is genuinely an **unknown outcome** for the caller.

### 5.4 `ensure_linearizable` and `get_read_log_id`

```rust
// $OR/src/raft/mod.rs:571-589
#[tracing::instrument(level = "debug", skip(self))]
pub async fn ensure_linearizable(
    &self,
) -> Result<Option<LogId<C::NodeId>>, RaftError<C::NodeId, CheckIsLeaderError<C::NodeId, C::Node>>> {
    let (read_log_id, applied) = self.get_read_log_id().await?;

    if read_log_id.index() > applied.index() {
        self.wait(None)
            .applied_index_at_least(read_log_id.index(), "ensure_linearizable")
            .await
            .map_err(|e| match e {
                WaitError::Timeout(_, _) => {
                    unreachable!("did not specify timeout")
                }
                WaitError::ShuttingDown => Fatal::Stopped,
            })?;
    }
    Ok(read_log_id)
}
```

Doc (`:548-570`) verbatim:

> This method confirms the node's leadership at the time of invocation by sending heartbeats to a quorum of followers, and the state machine is up to date. This method blocks until all these conditions are met.
>
> Returns:
> - `Ok(read_log_id)` on successful confirmation that the node is the leader. `read_log_id` represents the log id up to which the state machine has applied to ensure a linearizable read.
> - `Err(RaftError<CheckIsLeaderError>)` if it detects a higher term, or if it fails to communicate with a quorum of followers.

**Return type is `Option<LogId<NodeId>>`, not `()`.** rEtcd §10.1 should capture the returned `read_log_id` — it is the exact log id the SM has reached, and it is the correct source for the response's `read_revision` barrier.

`ensure_linearizable()` **has no timeout** (`wait(None)` = 100 years). rEtcd must wrap it in `tokio::time::timeout` to satisfy §16 retry/deadline behavior. If you need the two-phase form:

```rust
// $OR/src/raft/mod.rs:620-630
#[tracing::instrument(level = "debug", skip(self))]
pub async fn get_read_log_id(
    &self,
) -> Result<
    (Option<LogId<C::NodeId>>, Option<LogId<C::NodeId>>),
    RaftError<C::NodeId, CheckIsLeaderError<C::NodeId, C::Node>>,
>
```

Returns `(read_log_id, last_applied_log_id)`. Then wait yourself with your own timeout.

```rust
// $OR/src/error.rs:159-170
pub enum CheckIsLeaderError<NID, N> where NID: NodeId, N: Node {
    ForwardToLeader(#[from] ForwardToLeader<NID, N>),
    QuorumNotEnough(#[from] QuorumNotEnough<NID>),
}

// $OR/src/error.rs:564-590
pub struct ForwardToLeader<NID, N> where ... {
    pub leader_id: Option<NID>,
    pub leader_node: Option<N>,
}
impl ForwardToLeader { pub const fn empty() -> Self; pub fn new(leader_id: NID, node: N) -> Self; }

// $OR/src/error.rs:604-607
pub struct QuorumNotEnough<NID: NodeId> {
    pub cluster: String,
    pub got: BTreeSet<NID>,
}
```

**rEtcd §10.1 error mapping (both fields are `Option`!):**

| openraft error | rEtcd gRPC/Direct outcome |
|---|---|
| `CheckIsLeaderError::ForwardToLeader { leader_id: Some(id), leader_node: Some(n) }` | typed `NOT_LEADER` + validated leader hint |
| `CheckIsLeaderError::ForwardToLeader { leader_id: None, .. }` | retryable `UNAVAILABLE`, **no** hint (leader unknown) |
| `CheckIsLeaderError::QuorumNotEnough` | retryable `UNAVAILABLE` — isolated/former leader, must **not** serve the read |
| `RaftError::Fatal(_)` | node unready/fatal |

Helpers: `RaftError::forward_to_leader()` / `into_forward_to_leader()` (`$OR/src/error.rs:86-110`) and `RPCError::forward_to_leader()` (`:304-317`), plus `TryAsRef<ForwardToLeader<..>>` impls for both `CheckIsLeaderError` and `ClientWriteError`.

**Important:** `ForwardToLeader`'s `leader_node` comes from openraft's committed membership. rEtcd M3 requires *authenticated* leader hints — validate the hint against the static deployment allowlist before returning it to a client; do not trust it blindly as an address to dial.

### 5.5 `is_leader` — deprecated

```rust
// $OR/src/raft/mod.rs:540-546
#[deprecated(since = "0.9.0", note = "use `Raft::ensure_linearizable()` instead")]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn is_leader(&self) -> Result<(), RaftError<C::NodeId, CheckIsLeaderError<C::NodeId, C::Node>>> {
    let (tx, rx) = C::AsyncRuntime::oneshot();
    let _ = self.inner.call_core(RaftMsg::CheckIsLeaderRequest { tx }, rx).await?;
    Ok(())
}
```

**Do not use.** It performs the quorum check but discards `read_log_id`, so it does **not** wait for the state machine to catch up — it is the "leader check without the apply barrier". `ensure_linearizable()` is the correct primitive for §10.1.

```rust
// $OR/src/raft/mod.rs:530-533
/// This method is based on the Raft metrics system ... however, the `is_leader` method must still
/// be used to guard against stale reads.
pub async fn current_leader(&self) -> Option<C::NodeId> {
    self.metrics().borrow().current_leader.clone()
}
```

`current_leader()` is metrics-derived and **stale by design** — routing hint only, never a read guard. 0.9.25 removed the `is_voter` check from it (changelog `89629e33`).

### 5.6 `metrics()` and `RaftMetrics`

```rust
// $OR/src/raft/mod.rs:843-856
/// Get a handle to the metrics channel.
pub fn metrics(&self) -> watch::Receiver<RaftMetrics<C::NodeId, C::Node>>;
pub fn data_metrics(&self) -> watch::Receiver<RaftDataMetrics<C::NodeId>>;
pub fn server_metrics(&self) -> watch::Receiver<RaftServerMetrics<C::NodeId, C::Node>>;
```

`watch::Receiver` is `tokio::sync::watch::Receiver` (`use tokio::sync::watch;` at `$OR/src/raft/mod.rs:45`).

```rust
// $OR/src/metrics/raft_metrics.rs:16-86
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct RaftMetrics<NID, N>
where NID: NodeId, N: Node,
{
    pub running_state: Result<(), Fatal<NID>>,

    /// The ID of the Raft node.
    pub id: NID,

    // --- data ---
    /// The current term of the Raft node.
    pub current_term: u64,
    /// The last accepted vote.
    pub vote: Vote<NID>,
    /// The last log index has been appended to this Raft node's log.
    pub last_log_index: Option<u64>,
    /// The last log index has been applied to this Raft node's state machine.
    pub last_applied: Option<LogId<NID>>,
    /// The id of the last log included in snapshot.
    pub snapshot: Option<LogId<NID>>,
    /// The last log id that has purged from storage, inclusive.
    pub purged: Option<LogId<NID>>,

    // --- cluster ---
    /// The state of the Raft node.
    pub state: ServerState,
    /// The current cluster leader.
    pub current_leader: Option<NID>,
    /// For a leader, it is the elapsed time in milliseconds since the most recently acknowledged
    /// timestamp by a quorum.
    pub millis_since_quorum_ack: Option<u64>,
    /// The current membership config of the cluster.
    pub membership_config: Arc<StoredMembership<NID, N>>,

    // --- replication ---
    /// The replication states. It is Some() only when this node is leader.
    pub replication: Option<ReplicationMetrics<NID>>,
}
```

- `ReplicationMetrics<NID> = BTreeMap<NID, Option<LogId<NID>>>` (`$OR/src/metrics/mod.rs:52`) — **`pub(crate)` type alias** but the field is public, so you can iterate it; you just can't name the alias.
- `membership_config` is `Arc<StoredMembership<..>>` (not `StoredMembership` directly).
- `state: ServerState` — `openraft::ServerState` (`$OR/src/lib.rs:80`).
- **There is no `current_term` on `RaftDataMetrics`**; only `RaftMetrics` has it.

```rust
// $OR/src/metrics/raft_metrics.rs:165-189
pub struct RaftDataMetrics<NID> where NID: NodeId {
    pub last_log: Option<LogId<NID>>,
    pub last_applied: Option<LogId<NID>>,
    pub snapshot: Option<LogId<NID>>,
    pub purged: Option<LogId<NID>>,
    pub millis_since_quorum_ack: Option<u64>,
    pub replication: Option<ReplicationMetrics<NID>>,
}
```

Module doc warning (`$OR/src/metrics/mod.rs:26-28`):

> Metrics is not a stream thus it only guarantees to provide the latest state but not every change of the state. Because internally, `watch::channel()` only stores one last state.

**rEtcd §18.2 mapping:** `millis_since_quorum_ack` is the right signal for "leader may be partitioned" alerting. `running_state: Result<(), Fatal<NID>>` is the right signal for the fatal/unready health surface (§9.3.7, §18.1).

### 5.7 `wait`, `shutdown`, `trigger`, `runtime_config`

```rust
// $OR/src/raft/mod.rs:878-888
/// If `timeout` is `None`, then it will wait forever(10 years).
pub fn wait(&self, timeout: Option<Duration>) -> Wait<C::NodeId, C::Node, C::AsyncRuntime>
```

(Comment says 10 years; code is `Duration::from_secs(86400 * 365 * 100)` = 100 years.)

```rust
// $OR/src/raft/mod.rs:890-908
/// Shutdown this Raft node.
///
/// It sends a shutdown signal and waits until `RaftCore` returns.
pub async fn shutdown(&self) -> Result<(), <C::AsyncRuntime as AsyncRuntime>::JoinError>
```

Sends the shutdown oneshot, joins `RaftCore`, then shuts down the tick task. For `TokioRuntime`, `JoinError = tokio::task::JoinError`. Note the source TODO: `shutdown()` returns `Ok(())` even when `RaftCore` stopped with a `Fatal` — **read `metrics().borrow().running_state` to learn why it stopped**. This matters for rEtcd's "graceful stop" acceptance in M1.

```rust
// $OR/src/raft/mod.rs:336-368
pub fn runtime_config(&self) -> RuntimeConfigHandle<'_, C>;  // .tick(bool) .heartbeat(bool) .elect(bool)
pub fn config(&self) -> &Arc<Config>;
pub fn trigger(&self) -> Trigger<'_, C>;                     // .elect() .heartbeat() .snapshot() .purge_log(u64)
```

Deprecated shims for all of these exist (`enable_tick`, `enable_heartbeat`, `enable_elect`, `trigger_elect`, `trigger_heartbeat`, `trigger_snapshot`, `purge_log`) — all `#[deprecated(since = "0.8.4")]`. Use the handles.

**rEtcd rule:** do not expose `trigger().snapshot()` or `trigger().purge_log()` anywhere in the public surface — §3.6 depends on them never being called. `trigger().elect()` is useful in M1 tests only. 0.9.25 now ignores an election trigger on the current leader (changelog `d94e232e`).

### 5.8 `Config` — all fields

```rust
// $OR/src/config/config.rs:112-231  (clap attrs preserved: they carry the defaults)
#[derive(Clone, Debug, Parser)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Config {
    #[clap(long, default_value = "foo")]
    pub cluster_name: String,

    #[clap(long, default_value = "150")]
    pub election_timeout_min: u64,          // ms

    #[clap(long, default_value = "300")]
    pub election_timeout_max: u64,          // ms

    #[clap(long, default_value = "50")]
    pub heartbeat_interval: u64,            // ms

    #[clap(long, default_value = "200")]
    pub install_snapshot_timeout: u64,      // ms

    #[deprecated(since = "0.9.0", note = "Sending snapshot by chunks is deprecated; Use `install_snapshot_timeout` instead")]
    #[clap(long, default_value = "0")]
    pub send_snapshot_timeout: u64,

    #[clap(long, default_value = "300")]
    pub max_payload_entries: u64,

    #[clap(long, default_value = "5000")]
    pub replication_lag_threshold: u64,

    #[clap(long, default_value = "since_last:5000", value_parser=parse_snapshot_policy)]
    pub snapshot_policy: SnapshotPolicy,

    #[clap(long, default_value = "3MiB", value_parser=parse_bytes_with_unit)]
    pub snapshot_max_chunk_size: u64,

    /// The maximum number of logs to keep that are already included in **snapshot**.
    ///
    /// Logs that are not in snapshot will never be purged.
    #[clap(long, default_value = "1000")]
    pub max_in_snapshot_log_to_keep: u64,

    /// The minimal number of applied logs to purge in a batch.
    #[clap(long, default_value = "1")]
    pub purge_batch_size: u64,

    /// Enable or disable tick.
    ///
    /// If ticking is disabled, timeout based events are all disabled:
    /// a follower won't wake up to enter candidate state, and a leader won't send heartbeat.
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub enable_tick: bool,

    /// Whether a leader sends heartbeat log to following nodes, i.e., followers and learners.
    #[clap(long, default_value_t = true, ...)]
    pub enable_heartbeat: bool,

    /// Whether a follower will enter candidate state if it does not receive message from the
    /// leader for a while.
    #[clap(long, default_value_t = true, ...)]
    pub enable_elect: bool,
}
```

```rust
// $OR/src/config/config.rs:27-37
pub enum SnapshotPolicy {
    /// A snapshot will be generated once the log has grown the specified number of logs since
    /// the last snapshot.
    LogsSinceLast(u64),

    /// Openraft will never trigger a snapshot building.
    /// With this option, the application calls
    /// [`Raft::trigger_snapshot()`] to manually trigger a snapshot.
    Never,
}
```

`SnapshotPolicy::Never` **exists** and is exported as `openraft::SnapshotPolicy` (`$OR/src/lib.rs:79`). CLI string form is `"never"` or `"since_last:<num>"` (`parse_snapshot_policy`, `:61-86`).

```rust
// $OR/src/config/config.rs:248-311
impl Default for Config {
    fn default() -> Self {
        <Self as Parser>::parse_from(Vec::<&'static str>::new())
    }
}

impl Config {
    pub fn new_rand_election_timeout<RT: AsyncRuntime>(&self) -> u64;
    pub fn install_snapshot_timeout(&self) -> Duration;
    /// The first element in `args` must be the application name.
    pub fn build(args: &[&str]) -> Result<Config, ConfigError>;

    /// Validate the state of this config.
    pub fn validate(self) -> Result<Config, ConfigError> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(ConfigError::ElectionTimeout { min: ..., max: ... });
        }
        if self.election_timeout_min <= self.heartbeat_interval {
            return Err(ConfigError::ElectionTimeoutLTHeartBeat { ... });
        }
        if self.max_payload_entries == 0 {
            return Err(ConfigError::MaxPayloadIs0);
        }
        Ok(self)
    }
}
```

**`validate` takes `self` by value** — pattern is `let config = Arc::new(config.validate()?);`. Only three checks:
1. `election_timeout_min < election_timeout_max`
2. `election_timeout_min > heartbeat_interval` (strict)
3. `max_payload_entries != 0`

`Default::default()` works because `Config` derives `clap::Parser` and parses an empty arg list. **Gotcha:** `Config` derives `Parser` with `#[clap(long)]` on every field. If rEtcd's own CLI also derives `Parser`, either `#[command(flatten)]` openraft's `Config` deliberately or build it programmatically. I recommend building programmatically (both examples do) to avoid leaking openraft's flags into rEtcd's CLI surface.

---

## 6. Feature flags of 0.9.25

Exact list from `$OR/Cargo.toml`:

```toml
[features]
bench = []
bt = ["anyerror/backtrace", "anyhow/backtrace"]
compat = []
generic-snapshot-data = []
loosen-follower-log-revert = []
serde = ["dep:serde"]
single-term-leader = []
singlethreaded = ["openraft-macros/singlethreaded"]
storage-v2 = []
tracing-log = ["tracing/log"]
```

> By default openraft enables no features. — `$OR/src/docs/feature_flags/feature-flags.md`

| Feature | rEtcd decision | Why (verified) |
|---|---|---|
| `storage-v2` | **ON (required)** | `Sealed` blanket impl only exists under it (`$OR/src/storage/v2.rs:33-34`). Also removes `Adaptor` + `RaftStorage`. |
| `serde` | **ON (required)** | "Derives `serde::Serialize, serde::Deserialize` for type that are used in storage and network, such as `Vote` or `AppendEntriesRequest`." Needed to persist `Vote`/`LogId`/`Entry` into RocksDB and to send RPCs over Protobuf/JSON. Verified `#[cfg_attr(feature = "serde", ...)]` on `Vote`, `LogId`, `Entry` (`$OR/src/entry/mod.rs:21`), `EntryPayload` (`$OR/src/entry/payload.rs:11`), `SnapshotMeta`, `StoredMembership`, `RaftMetrics`, all error types. |
| `generic-snapshot-data` | **OFF** | Keeps the default chunked `full_snapshot()` impl and keeps `Raft::install_snapshot()` available. `Cursor<Vec<u8>>` satisfies the `AsyncRead + AsyncWrite + AsyncSeek + Unpin` bound. |
| `singlethreaded` | **OFF** | Removes `Send`/`Sync`; would force `tokio::task::spawn_local`. rEtcd is multi-threaded Tokio. |
| `single-term-leader` | **OFF** | Changes `LogId` from `(term, node_id, index)` to `(term, index)` — an on-disk format change. Not needed; would be a migration if adopted later. |
| `compat` | **OFF** | v1 compatibility types. |
| `bt` | **OFF** | Requires nightly (`error_generic_member_access`). |
| `tracing-log` | OFF (optional) | Only if rEtcd wires `log` records. |
| `loosen-follower-log-revert` | **OFF** | Doc: "Do not use it unless you know what you are doing." Would mask exactly the log-revert bug M2 must catch. |
| `bench` | OFF | Nightly. |

**`Cargo.toml` for rEtcd:**

```toml
[dependencies]
openraft = { version = "=0.9.25", features = ["serde", "storage-v2"] }
```

**There is no `tokio-rt` feature.** Tokio is an unconditional dependency with `features = ["io-util", "macros", "rt", "rt-multi-thread", "sync", "time"], default-features = false`.

### Serde consequence you must plan for

```rust
// $OR/src/lib.rs:127-132, 177-179, 198-200
#[cfg(feature = "serde")]
pub trait OptionalSerde: serde::Serialize + for<'a> serde::Deserialize<'a> {}
#[cfg(feature = "serde")]
impl<T> OptionalSerde for T where T: serde::Serialize + for<'a> serde::Deserialize<'a> {}

pub trait AppData: OptionalSend + OptionalSync + 'static + OptionalSerde {}
pub trait AppDataResponse: OptionalSend + OptionalSync + 'static + OptionalSerde {}
```

With `serde` on, `C::D` and `C::R` **must** be `Serialize + DeserializeOwned` (note: `for<'a> Deserialize<'a>`, i.e. owned — no borrowed lifetimes in `Cmd`). rEtcd's versioned command envelope (§7) must therefore be a `serde`-able owned type. If we want Protobuf-on-the-wire rather than serde JSON for the command bytes, the clean shape is `Cmd { version: u32, payload: Vec<u8> }` with a serde derive on the wrapper and prost for the payload.

---

## 7. Official examples (branch `release-0.9`)

### 7.1 `examples/raft-kv-memstore` — file layout

```
examples/memstore/                         # shared generic in-memory store crate
  Cargo.toml
  src/lib.rs                               # 7 lines: `pub mod log_store;` etc.
  src/log_store.rs                         # generic RaftLogStorage<C> impl  (211 lines)

examples/raft-kv-memstore/
  Cargo.toml                               # openraft features = ["serde", "storage-v2"]
  src/lib.rs                               # declare_raft_types!, typ aliases, start_example_raft_node
  src/app.rs                               # App struct held by the HTTP layer
  src/client.rs
  src/bin/main.rs
  src/store/mod.rs                         # Request/Response + RaftStateMachine impl (238 lines)
  src/network/mod.rs
  src/network/raft_network_impl.rs         # RaftNetworkFactory + RaftNetwork
  src/network/raft.rs                      # inbound peer RPC handlers
  src/network/api.rs                       # client read/write/consistent_read
  src/network/management.rs                # init / add_learner / change_membership / metrics
  src/test.rs
  tests/cluster/{main.rs,test_cluster.rs}
```

There is **no** `examples/rocksstore` or `rocksstore-v2` in `release-0.9`. The RocksDB example is `examples/raft-kv-rocksdb` (same layout, `src/store.rs` is one 500-line file).

### 7.2 `declare_raft_types!` + bootstrap (`ex/examples_raft-kv-memstore_src_lib.rs:29-40, 59-88`)

```rust
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
);

pub type LogStore = store::LogStore;
pub type StateMachineStore = store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;
```

```rust
let config = Config {
    heartbeat_interval: 500,
    election_timeout_min: 1500,
    election_timeout_max: 3000,
    ..Default::default()
};
let config = Arc::new(config.validate().unwrap());

let log_store = LogStore::default();
let state_machine_store = Arc::new(StateMachineStore::default());
let network = Network {};

let raft = openraft::Raft::new(
    node_id,
    config.clone(),
    network,
    log_store.clone(),
    state_machine_store.clone(),
)
.await
.unwrap();
```

Note `Arc<StateMachineStore>` is what implements `RaftStateMachine` (see below), so the app keeps its own `Arc` clone for direct reads. `LogStore` is `Clone` (inner `Arc<Mutex<..>>`) so the app keeps a handle too. `app.rs` is the whole sharing story:

```rust
// ex/examples_raft-kv-memstore_src_app.rs
use std::sync::Arc;

use crate::LogStore;
use crate::NodeId;
use crate::Raft;
use crate::StateMachineStore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub log_store: LogStore,
    pub state_machine_store: Arc<StateMachineStore>,
    pub config: Arc<openraft::Config>,
}
```

**This is the shape rEtcd's `DirectClient` / full-node embedding should mirror** (§6.3): a single struct holding `Raft<TypeConfig>` + an `Arc` handle to the state machine for local linearized reads + the validated `Arc<Config>`.

### 7.3 `examples/memstore/src/log_store.rs` — the v2 log store (key parts)

```rust
// ex/examples_memstore_src_log_store.rs:18-37
/// RaftLogStore implementation with a in-memory storage
#[derive(Clone, Debug, Default)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

#[derive(Debug)]
pub struct LogStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    last_purged_log_id: Option<LogId<C::NodeId>>,
    /// The Raft log.
    log: BTreeMap<u64, C::Entry>,
    /// The commit log id.
    committed: Option<LogId<C::NodeId>>,
    /// The current granted vote.
    vote: Option<Vote<C::NodeId>>,
}
```

```rust
// ex/examples_memstore_src_log_store.rs:96-131
async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<C::NodeId>>
where I: IntoIterator<Item = C::Entry> {
    // Simple implementation that calls the flush-before-return `append_to_log`.
    for entry in entries {
        self.log.insert(entry.get_log_id().index, entry);
    }
    callback.log_io_completed(Ok(()));

    Ok(())
}

async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
    let keys = self.log.range(log_id.index..).map(|(k, _v)| *k).collect::<Vec<_>>();
    for key in keys { self.log.remove(&key); }
    Ok(())
}

async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
    {
        let ld = &mut self.last_purged_log_id;
        assert!(ld.as_ref() <= Some(&log_id));
        *ld = Some(log_id.clone());
    }
    {
        let keys = self.log.range(..=log_id.index).map(|(k, _v)| *k).collect::<Vec<_>>();
        for key in keys { self.log.remove(&key); }
    }
    Ok(())
}
```

```rust
// ex/examples_memstore_src_log_store.rs:149-210
impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C> where C::Entry: Clone {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self, range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C> where C::Entry: Clone {
    type LogReader = Self;
    // ... each method just locks `self.inner` and delegates ...
    async fn get_log_reader(&mut self) -> Self::LogReader { self.clone() }
}
```

**Pattern to copy:** `Clone`-able outer handle wrapping `Arc<tokio::sync::Mutex<Inner>>`; `type LogReader = Self`; `get_log_reader()` returns `self.clone()`. This is how the replication tasks get concurrent read access while satisfying the "all write-IO serialized" contract — the single `Mutex` *is* the serialization.

Note the example writes `append` with the *relaxed* bound `where I: IntoIterator<Item = C::Entry>` (dropping `I::IntoIter: OptionalSend`). That compiles because `OptionalSend` is blanket-implemented; but be explicit in rEtcd.

### 7.4 `examples/raft-kv-memstore/src/store/mod.rs` — the state machine

```rust
// ex/examples_raft-kv-memstore_src_store_mod.rs:27, 35-92
pub type LogStore = memstore::LogStore<TypeConfig>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request { Set { key: String, value: String } }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response { pub value: Option<String> }

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    /// Application data.
    pub data: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    /// The last received snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}
```

```rust
// ex/examples_raft-kv-memstore_src_store_mod.rs:136-175, 235-237
impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        Ok((state_machine.last_applied_log, state_machine.last_membership.clone()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where I: IntoIterator<Item = Entry<TypeConfig>> + Send {
        let mut res = Vec::new(); //No `with_capacity`; do not know `len` of iterator

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    Request::Set { key, value } => {
                        sm.data.insert(key.clone(), value.clone());
                        res.push(Response { value: Some(value.clone()) })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response { value: None })
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder { self.clone() }
}
```

Note `impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore>` — the impl is on the `Arc`, not the struct, so `type SnapshotBuilder = Self` can be a cheap clone and the app can hold a second `Arc` for direct reads. `&mut self` on an `Arc` is fine because all interior state is behind `RwLock`.

### 7.5 `examples/raft-kv-memstore/src/network/raft_network_impl.rs` — network

Full file is pasted in §4's error-mapping discussion; the structural parts:

```rust
// ex/examples_raft-kv-memstore_src_network_raft_network_impl.rs:22-107
pub struct Network {}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented
// directly.
impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection { owner: Network {}, target, target_node: node.clone() }
    }
}

pub struct NetworkConnection {
    owner: Network,
    target: NodeId,
    target_node: BasicNode,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, typ::RPCError> {
        self.owner.send_rpc(self.target, &self.target_node, "raft-append", req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, typ::RPCError<InstallSnapshotError>> {
        self.owner.send_rpc(self.target, &self.target_node, "raft-snapshot", req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, typ::RPCError> {
        self.owner.send_rpc(self.target, &self.target_node, "raft-vote", req).await
    }
}
```

Three inbound peer RPCs are needed on the server side: `append_entries`, `vote`, `install_snapshot` — each dispatched to `Raft::append_entries` / `Raft::vote` / `Raft::install_snapshot`. rEtcd needs all three Protobuf methods on the peer plane even though snapshot is never exercised in M1–M3.

### 7.6 `examples/raft-kv-rocksdb/src/store.rs` — column families for logs / vote / committed / last_applied

**Only two column families**, `store` and `logs`:

```rust
// ex/examples_raft-kv-rocksdb_src_store.rs:485-500
pub(crate) async fn new_storage<P: AsRef<Path>>(db_path: P) -> (LogStore, StateMachineStore) {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let store = ColumnFamilyDescriptor::new("store", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    let db = DB::open_cf_descriptors(&db_opts, db_path, vec![store, logs]).unwrap();
    let db = Arc::new(db);

    let log_store = LogStore { db: db.clone() };
    let sm_store = StateMachineStore::new(db).await.unwrap();

    (log_store, sm_store)
}
```

Keys inside `store` (all values `serde_json`):

| CF | Key | Value | Written by |
|---|---|---|---|
| `store` | `b"last_purged_log_id"` | `LogId<u64>` | `LogStore::set_last_purged_` (`:316-327`) |
| `store` | `b"committed"` | `Option<LogId<NodeId>>` | `LogStore::set_committed_` (`:329-336`) |
| `store` | `b"vote"` | `Vote<NodeId>` | `LogStore::set_vote_` (`:348-357`) |
| `store` | `b"snapshot"` | `StoredSnapshot` | `StateMachineStore::set_current_snapshot_` (`:179-187`) |
| `logs` | `id_to_bin(index)` (8-byte big-endian u64) | `Entry<TypeConfig>` | `append` (`:439-459`) |

**Big-endian index keys — this is the load-bearing detail:**

```rust
// ex/examples_raft-kv-rocksdb_src_store.rs:282-292
/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}
```

Durability: every metadata write is followed by `flush_wal(true)`:

```rust
// ex/examples_raft-kv-rocksdb_src_store.rs:303-306
fn flush(&self, subject: ErrorSubject<NodeId>, verb: ErrorVerb) -> Result<(), StorageIOError<NodeId>> {
    self.db.flush_wal(true).map_err(|e| StorageIOError::new(subject, verb, AnyError::new(&e)))?;
    Ok(())
}
```

`set_vote_` → `flush(ErrorSubject::Vote, ErrorVerb::Write)` — this is how the example satisfies "vote must be persisted before returning" (rEtcd §9.3.1).

**Range ops:**

```rust
// ex/examples_raft-kv-rocksdb_src_store.rs:462-478
async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
    let from = id_to_bin(log_id.index);
    let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
    self.db.delete_range_cf(self.logs(), &from, &to).map_err(|e| StorageIOError::write_logs(&e).into())
}

async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
    self.set_last_purged_(log_id)?;
    let from = id_to_bin(0);
    let to = id_to_bin(log_id.index + 1);
    self.db.delete_range_cf(self.logs(), &from, &to).map_err(|e| StorageIOError::write_logs(&e).into())
}
```

`get_log_state` uses a reverse iterator on `logs` (`:401-417`); `try_get_log_entries` a forward iterator from the start bound with `take_while(|(id,_)| range.contains(id))` (`:370-395`).

**The state machine in this example does NOT persist `kvs` on apply:**

```rust
// ex/examples_raft-kv-rocksdb_src_store.rs:91-99, 208-239
#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, Node>,
    /// State built from applying the raft logs
    pub kvs: Arc<RwLock<BTreeMap<String, String>>>,
}

async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
where I: IntoIterator<Item = typ::Entry> + OptionalSend, I::IntoIter: OptionalSend {
    let entries = entries.into_iter();
    let mut replies = Vec::with_capacity(entries.size_hint().0);

    for ent in entries {
        self.data.last_applied_log_id = Some(ent.log_id);
        let mut resp_value = None;
        match ent.payload {
            EntryPayload::Blank => {}
            EntryPayload::Normal(req) => match req {
                Request::Set { key, value } => {
                    resp_value = Some(value.clone());
                    let mut st = self.data.kvs.write().await;
                    st.insert(key, value);
                }
            },
            EntryPayload::Membership(mem) => {
                self.data.last_membership = StoredMembership::new(Some(ent.log_id), mem);
            }
        }
        replies.push(Response { value: resp_value });
    }
    Ok(replies)
}
```

`last_applied_log_id`, `last_membership` and `kvs` all live **in memory**; the only durable SM state is the `b"snapshot"` blob. `StateMachineStore::new` rebuilds from it (`:138-155`). **This is the "persistent snapshot" option from the trait doc, and it is NOT what rEtcd wants.**

> **rEtcd deviation from the RocksDB example — this is the single most important design note.**
> The official RocksDB example does **not** satisfy rEtcd §9.3.5. rEtcd must instead, inside **one atomic RocksDB `WriteBatch` per `apply()` call**, write:
> - `kv` CF mutations,
> - `state_meta:public_revision`,
> - `state_meta:last_applied_log_id`,
> - `state_meta:last_membership` (when the batch contains a membership entry),
>
> then `write_opt` with `sync = true` (or `flush_wal(true)`) **before returning from `apply()`**. That is the "persistent state machine" option, and it is what makes `SnapshotPolicy::Never` + `get_current_snapshot() -> Ok(None)` correct. The example's `flush_wal(true)`-per-key style is fine for vote/log metadata but must be replaced by a batched sync for the state machine.
>
> Also note the example shares **one `Arc<DB>`** between `LogStore` and `StateMachineStore`. rEtcd §9.2 permits that ("remain separate even when they share a physical database") — keep the Rust types separate and never let one type touch the other's column families.

Example's `Node` type (`ex/examples_raft-kv-rocksdb_src_lib.rs:30-49`) shows the custom-`Node` pattern rEtcd will want for peer endpoints:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl Display for Node { /* ... */ }

pub type SnapshotData = Cursor<Vec<u8>>;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node,
);
```

`Node` requires `Serialize + Deserialize + Debug + Clone + PartialEq + Eq + Default + Display` (openraft's `Node` trait, `$OR/src/node.rs`). **UNVERIFIED:** exact `Node` supertrait list — inferred from the example's derives plus the compile of `BasicNode`.

---

## 8. Determinism: apply ordering and restart re-apply

### 8.1 Does `apply()` receive entries in strictly increasing log index? — **Yes, contiguous and gap-free.**

```rust
// $OR/src/core/raft_core.rs:734-759
pub(crate) async fn apply_to_state_machine(
    &mut self,
    seq: CommandSeq,
    since: u64,
    upto_index: u64,
) -> Result<(), StorageError<C::NodeId>> {
    let end = upto_index + 1;

    debug_assert!(
        since <= end,
        "last_applied index {} should <= committed index {}",
        since, end
    );

    if since == end {
        return Ok(());
    }

    let entries = self.log_store.get_log_entries(since..end).await?;
```

`since` is `last_applied.next_index()`; `end` is `committed + 1`. `get_log_entries` (`RaftLogReaderExt`, `$OR/src/storage/log_store_ext.rs`) errors if any index in the range is missing, so the batch handed to `apply()` is **exactly `[last_applied+1 ..= committed]`, contiguous, strictly increasing**.

The SM worker then computes, before calling `apply`:

```rust
// $OR/src/core/sm/worker.rs:129-165
let since = entries.first().map(|x| x.get_log_id().index).unwrap();
let end = entries.last().map(|x| x.get_log_id().index + 1).unwrap();
let last_applied = entries.last().map(|x| x.get_log_id().clone()).unwrap();
```

and there is exactly **one** SM worker task consuming a single unbounded mpsc channel (`$OR/src/core/sm/worker.rs:43-127`), so `apply()` calls are strictly serialized. Batches never overlap and never go backwards.

**M0/§7.4 consequence:** deterministic replay is achievable — the entry stream into `apply()` is a total order with no gaps, duplicates, or reordering.

### 8.2 Does openraft ever re-apply an already-applied entry after restart? — **No, if `applied_state()` is correct.**

```rust
// $OR/src/storage/helper.rs:65-103
pub async fn get_initial_state(&mut self) -> Result<RaftState<...>, StorageError<C::NodeId>> {
    let vote = self.log_store.read_vote().await?;
    let vote = vote.unwrap_or_default();

    let mut committed = self.log_store.read_committed().await?;

    let st = self.log_store.get_log_state().await?;
    let mut last_purged_log_id = st.last_purged_log_id;
    let mut last_log_id = st.last_log_id;

    let (mut last_applied, _) = self.state_machine.applied_state().await?;
    ...
    // TODO: It is possible `committed < last_applied` because when installing snapshot,
    //       new committed should be saved, but not yet.
    if committed < last_applied {
        committed = last_applied.clone();
    }

    // Re-apply log entries to recover SM to latest state.
    if last_applied < committed {
        let start = last_applied.next_index();
        let end = committed.next_index();

        self.reapply_committed(start, end).await?;

        last_applied = committed;
    }
```

The replay window is exactly `(last_applied, committed]` — **strictly above** the reported `last_applied`. And `committed` is first clamped up to `last_applied`, so a stale/behind `committed` can never cause a re-apply.

```rust
// $OR/src/storage/helper.rs:178-226
pub(crate) async fn reapply_committed(&mut self, mut start: u64, end: u64) -> Result<(), StorageError<C::NodeId>> {
    let chunk_size = 64;
    let mut log_reader = self.log_store.get_log_reader().await;

    while start < end {
        let chunk_end = std::cmp::min(end, start + chunk_size);
        let entries = log_reader.try_get_log_entries(start..chunk_end).await?;
        ...
        if first != Some(start) { return Err(StorageIOError::read_log_at_index(start, make_err()).into()); }
        if last != Some(chunk_end - 1) { return Err(StorageIOError::read_log_at_index(chunk_end - 1, make_err()).into()); }
        ...
        self.state_machine.apply(entries).await?;
        start = chunk_end;
    }
    Ok(())
}
```

**Three things to note about restart replay:**

1. It replays in **64-entry chunks**, still contiguous and increasing. Gaps are a hard `StorageIOError`, so the store's no-hole invariant is actually checked at startup — free M2 verification.
2. **The responses from replay `apply()` are discarded.** There is no client waiting. So `apply()` must not depend on anyone consuming its `Vec<C::R>`.
3. **Replay happens before `RaftCore` starts** (inside `Raft::new`, `$OR/src/raft/mod.rs:265-269`), on the same task, with `&mut state_machine` — no concurrency.

**The correctness condition rEtcd must meet (§9.3.5 + §9.3.6):** `last_applied` must be made durable **atomically** with the KV mutations and the public revision in the same `apply()` call. If they can diverge, a crash between them either (a) loses a mutation whose `last_applied` was already bumped — silent data loss, no replay — or (b) re-applies a mutation whose KV write already landed — duplicate public revision. One `WriteBatch` with `sync = true` eliminates both.

**And the condition for §9.3.6 specifically:** you **must** implement `save_committed`/`read_committed`. The trait defaults are `Ok(())` / `Ok(None)`:

```rust
// $OR/src/storage/v2.rs:97-106
async fn save_committed(&mut self, _committed: Option<LogId<C::NodeId>>) -> Result<(), StorageError<C::NodeId>> {
    // By default `committed` log id is not saved
    Ok(())
}
async fn read_committed(&mut self) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
    // By default `committed` log id is not saved and this method just return None.
    Ok(None)
}
```

With the defaults, `committed = None`, which is then clamped to `last_applied`, so `last_applied == committed` and `reapply_committed` is a no-op. Committed-but-unapplied entries would then only be applied after the new leader re-establishes commit — which for rEtcd's M2 acceptance ("committed-but-unapplied entries replay without duplicate public revisions") is not the guarantee we want to rest on. Persist `committed` in `raft_meta`.

**Startup trap worth a test:** `if last_log_id < last_applied` the helper **purges the log up to `last_applied`** (`$OR/src/storage/helper.rs:107-119`). That only fires after a snapshot install, which we never do — but if our `get_log_state()` under-reports `last_log_id` (e.g. a bug reading the last log key), openraft will **delete log entries**. Make `get_log_state()` bulletproof and assert `last_log_id >= last_applied` in an M2 test.

### 8.3 `StorageError` / `StorageIOError` constructors for §9.3.7

```rust
// $OR/src/storage_error.rs (line numbers from grep)
:183  pub enum StorageError<NID>          // variants: Defensive { .. } | IO { source: StorageIOError<NID> }
:220  pub fn from_io_error(subject: ErrorSubject<NID>, verb: ErrorVerb, io_error: std::io::Error) -> Self
:233  pub struct StorageIOError<NID>
:253  pub fn new(subject: ErrorSubject<NID>, verb: ErrorVerb, source: impl Into<AnyError>) -> Self
:262  pub fn write_log_entry(log_id: LogId<NID>, source: impl Into<AnyError>) -> Self
:266  pub fn read_log_at_index(log_index: u64, source: impl Into<AnyError>) -> Self
:270  pub fn read_log_entry(log_id: LogId<NID>, source: impl Into<AnyError>) -> Self
:274  pub fn write_logs(source: impl Into<AnyError>) -> Self
:278  pub fn read_logs(source: impl Into<AnyError>) -> Self
:282  pub fn write_vote(source: impl Into<AnyError>) -> Self
:286  pub fn read_vote(source: impl Into<AnyError>) -> Self
:290  pub fn apply(log_id: LogId<NID>, source: impl Into<AnyError>) -> Self
:294  pub fn write_state_machine(source: impl Into<AnyError>) -> Self
:298  pub fn read_state_machine(source: impl Into<AnyError>) -> Self
:302  pub fn write_snapshot(signature: Option<SnapshotSignature<NID>>, source: impl Into<AnyError>) -> Self
:306  pub fn read_snapshot(signature: Option<SnapshotSignature<NID>>, source: impl Into<AnyError>) -> Self
:311  pub fn read(source: impl Into<AnyError>) -> Self
:316  pub fn write(source: impl Into<AnyError>) -> Self
```

`StorageIOError<NID>: Into<StorageError<NID>>` via `#[from]`. Any `StorageError` returned from a storage method becomes `Fatal::StorageError` (`$OR/src/error.rs:133-142`) and **shuts the Raft node down** — which is exactly rEtcd §9.3.7's required behavior for disk-full / corruption / uncertain-sync. Surface it through `metrics().borrow().running_state` on the health endpoint. Exported at top level: `openraft::{StorageError, StorageIOError, ErrorSubject, ErrorVerb, Violation, ToStorageResult, DefensiveError}` (`$OR/src/lib.rs:113-119`).

---

## 9. Windows / Tokio notes for 0.9.25

**Verified from source:**

1. **`Raft::new` must run inside a Tokio runtime.** It calls `C::AsyncRuntime::spawn` for the tick task, the SM worker and `RaftCore::main` (`$OR/src/raft/mod.rs:249-322`). `TokioRuntime::spawn` → `tokio::spawn`, which panics outside a runtime context.
2. **Tick interval is `heartbeat_interval * 3 / 2`, not `heartbeat_interval`.** `Tick::spawn(Duration::from_millis(config.heartbeat_interval * 3 / 2), ...)` (`$OR/src/raft/mod.rs:249-253`). Election detection granularity is therefore 1.5× the heartbeat interval, and openraft requires `election_timeout_min > heartbeat_interval` (only strictly greater — `Config::validate`). With the defaults (heartbeat 50, election_min 150) the tick is 75 ms and there are only two ticks per minimum election timeout.
3. **`TickHandle::drop` signals stop without waiting** (`$OR/src/core/tick.rs:40-50`). Dropping the last `Raft` clone stops ticking but does not join. Always call `shutdown().await` for a clean M1 "graceful stop".
4. `openraft` needs tokio features `io-util, macros, rt, rt-multi-thread, sync, time` — it enables them itself with `default-features = false`, so a rEtcd `tokio` dependency with `features = ["full"]` or the specific set both work.

**Verified from external sources:**

5. **Windows Tokio timer resolution: timers fire ~15 ms late.** A 1 ms `tokio::time::sleep` is late by an average of 15 ms on Windows vs ~1.13 ms on Linux ([tokio-rs/tokio#5021](https://github.com/tokio-rs/tokio/issues/5021)) — the default Windows timer resolution is ~15.6 ms. Combined with note 2, openraft's **default** `heartbeat_interval: 50` / `election_timeout_min: 150` is genuinely tight on Windows Server: the 75 ms tick can slip to ~90 ms, and two slipped ticks approach the 150 ms floor, producing spurious elections.

   **Recommendation for rEtcd on Windows Server 2022 VMs:** `heartbeat_interval: 250`, `election_timeout_min: 750`, `election_timeout_max: 1500` (tick = 375 ms, ~2× Windows slop margin). This is in the same range as the official examples (`raft-kv-memstore` uses 500/1500/3000; `raft-kv-rocksdb` uses 250/299 — note the latter's 299 ms `election_timeout_min` is *barely* above its 250 ms heartbeat and would be fragile on Windows). Treat the exact numbers as a benchmarking output per rEtcd §9.3.8, not a fixed choice.

**UNVERIFIED (flagging for M2 work, not confirmed by me):**

6. **RocksDB on Windows build toolchain.** The `rocksdb` crate (0.22 in the example) builds C++ via `cc`/bindgen and needs MSVC Build Tools + `libclang` (`LIBCLANG_PATH`). Not tested here. Budget time for this in M2.
7. **RocksDB `flush_wal(true)` durability on Windows volumes.** Whether `flush_wal(true)` gives a true device-level barrier on the target VM disk class is a §9.3.8 benchmarking question, not an API question. Untested.
8. No openraft issue specific to Windows was found in the change log for 0.9.x.
9. `single-term-leader` off means `LogId` is `(term, node_id, index)`. I did not check whether `LeaderId` ordering behaves differently on any platform — it is pure integer comparison, so no platform concern expected.

### 0.9.25 change-log items that matter to rEtcd

From `https://raw.githubusercontent.com/databendlabs/openraft/release-0.9/change-log.md` (v0.9.25, 2026-07-28 — **this is a very recent patch; pinning `=0.9.25` is pinning near the tip of `release-0.9`**):

| Commit | Fix | Why it matters |
|---|---|---|
| `7d8bf714` | keep `VecProgress` sorted above `granted` | **Safety bug**: could "grant a value held by fewer than a quorum, committing on a quorum that never existed." Reproduced with a five-voter sequence. Do not downgrade below 0.9.25. |
| `211b91ba` | bound the wait for RaftCore to stop on a dropped responder | `client_write` / `get_snapshot` / `begin_receiving_snapshot` used to hang forever if the SM worker died. Now returns `Fatal::Stopped` within ~1 s. Directly relevant to §16 unknown-outcome behavior. |
| `546ed868` | handle empty `limited_get_log_entries` instead of panicking | A store contract violation degrades replication instead of panicking in release builds. |
| `89629e33` | remove the `is_voter` check from `current_leader()` | `current_leader()` semantics changed in 0.9.25. |
| `d94e232e` | ignore an election trigger on the current leader | Relevant if M1 tests call `trigger().elect()`. |
| `81e304b5` | send a snapshot to a follower behind a fully purged log | The `progress/entry` snapshot conditions quoted in §3.6 are from this fix. |
| `2447bae4` | avoid blocking RaftCore on closed replication | Liveness. |
| `7b66adf2` | reset purged membership on snapshot install | Not exercised by rEtcd M1–M3. |

---

## 10. Minimal skeleton (compile-verified)

This is the exact content of `C:\Users\gautamb\AppData\Local\Temp\orx\src\main.rs`, which **built cleanly** with:

```toml
[dependencies]
openraft = { version = "=0.9.25", features = ["serde", "storage-v2"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
anyhow = "1"
```

```
$ cargo build
   Compiling openraft v0.9.25
   Compiling orx v0.1.0 (C:\Users\gautamb\AppData\Local\Temp\orx)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 31.58s
```

Zero warnings, zero errors. Ports directly into `retcd-raft`.

```rust
//! Compile-check of the minimal openraft 0.9.25 v2-storage skeleton.
#![allow(dead_code, unused_variables, clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::BasicNode;
use openraft::Entry;
use openraft::LogId;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::Vote;
use tokio::sync::Mutex;

pub type NodeId = u64;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum Cmd {
    Put { key: Vec<u8>, value: Vec<u8> },
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CmdResp {
    pub revision: u64,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Cmd,
        R = CmdResp,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder = openraft::impls::OneshotResponder<TypeConfig>,
        AsyncRuntime = openraft::TokioRuntime,
);

// ---------------- log store ----------------

#[derive(Debug, Default)]
struct LogInner {
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
}

#[derive(Clone, Debug, Default)]
pub struct MemLogStore {
    inner: Arc<Mutex<LogInner>>,
}

impl RaftLogReader<TypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let last = inner.log.iter().next_back().map(|(_, e)| e.log_id).or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // rEtcd: must be durable (fsync) BEFORE returning.
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> Result<(), StorageError<NodeId>> {
        // rEtcd: REQUIRED (not the default no-op) so committed-but-unapplied entries replay.
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.lock().await;
            for e in entries {
                inner.log.insert(e.log_id.index, e);
            }
        }
        // Entries are readable now. Callback fires only after the durable boundary.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.last_purged = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }
}

// ---------------- state machine ----------------

#[derive(Debug, Default)]
pub struct SmInner {
    pub last_applied: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub revision: u64,
    pub kv: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, Default)]
pub struct MemStateMachine {
    inner: Mutex<SmInner>,
}

pub struct NoSnapshots;

impl RaftSnapshotBuilder<TypeConfig> for NoSnapshots {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        unreachable!("snapshots are not supported: SnapshotPolicy::Never + no purge + no manual trigger")
    }
}

impl RaftStateMachine<TypeConfig> for Arc<MemStateMachine> {
    type SnapshotBuilder = NoSnapshots;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        let g = self.inner.lock().await;
        Ok((g.last_applied, g.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CmdResp>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut g = self.inner.lock().await;
        let it = entries.into_iter();
        let mut out = Vec::with_capacity(it.size_hint().0);
        for ent in it {
            // Exactly one response per entry, in order — including Blank and Membership.
            g.last_applied = Some(ent.log_id);
            match ent.payload {
                openraft::EntryPayload::Blank => out.push(CmdResp { revision: g.revision }),
                openraft::EntryPayload::Membership(m) => {
                    g.last_membership = StoredMembership::new(Some(ent.log_id), m);
                    out.push(CmdResp { revision: g.revision });
                }
                openraft::EntryPayload::Normal(cmd) => {
                    let rev = match cmd {
                        Cmd::Put { key, value } => {
                            g.revision += 1;
                            g.kv.insert(key, value);
                            g.revision
                        }
                    };
                    out.push(CmdResp { revision: rev });
                }
            }
        }
        // rEtcd RocksDB impl: one atomic WriteBatch (kv + revision + last_applied + membership),
        // written with sync=true, BEFORE returning.
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        NoSnapshots
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        // Must return a real handle even though it is never used in M1/M2.
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        unreachable!("snapshot install not supported in M1/M2")
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(None)
    }
}

// ---------------- network ----------------

use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

pub struct NetFactory;
pub struct NetConn {
    target: NodeId,
    node: BasicNode,
}

impl RaftNetworkFactory<TypeConfig> for NetFactory {
    type Network = NetConn;
    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetConn {
            target,
            node: node.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for NetConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // map connect-refused -> RPCError::Unreachable (backoff)
        // map other transport failure -> RPCError::Network (immediate retry)
        // map peer-side RaftError -> RPCError::RemoteError(RemoteError::new(target, e))
        todo!()
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        // Required by the trait (generic-snapshot-data off), never invoked in M1-M3.
        todo!()
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        todo!()
    }
}

// ---------------- bootstrap ----------------

pub type MyRaft = openraft::Raft<TypeConfig>;

pub async fn start(id: NodeId, peers: BTreeMap<NodeId, BasicNode>) -> anyhow::Result<MyRaft> {
    let config = openraft::Config {
        cluster_name: "retcd".to_string(),
        heartbeat_interval: 250,
        election_timeout_min: 750,
        election_timeout_max: 1500,
        snapshot_policy: openraft::SnapshotPolicy::Never,
        max_in_snapshot_log_to_keep: 0,
        purge_batch_size: 1,
        enable_tick: true,
        enable_heartbeat: true,
        enable_elect: true,
        ..Default::default()
    };
    let config = Arc::new(config.validate()?);

    let log_store = MemLogStore::default();
    let sm = Arc::new(MemStateMachine::default());

    let raft = MyRaft::new(id, config, NetFactory, log_store, sm).await?;

    // Idempotent-ish: returns InitializeError::NotAllowed if already formed.
    match raft.initialize(peers).await {
        Ok(()) => {}
        Err(RaftError::APIError(openraft::error::InitializeError::NotAllowed(_))) => {}
        Err(e) => return Err(e.into()),
    }

    Ok(raft)
}

pub async fn linearizable_get(raft: &MyRaft, sm: &Arc<MemStateMachine>, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    // rEtcd: wrap in tokio::time::timeout — ensure_linearizable() has NO internal timeout.
    raft.ensure_linearizable().await?;
    let g = sm.inner.lock().await;
    Ok(g.kv.get(key).cloned())
}

pub async fn put(raft: &MyRaft, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<u64> {
    let resp = raft.client_write(Cmd::Put { key, value }).await?;
    Ok(resp.data.revision)
}

fn main() {}
```

### Notes proven by the compile

- `LogId<u64>` and `Vote<u64>` are **`Copy`** — `*vote`, `ent.log_id`, `inner.last_purged` all work without `.clone()`. (Would change under `single-term-leader`; **UNVERIFIED** for that feature.)
- `Entry<TypeConfig>` is `Clone` because `Cmd: Clone` (`$OR/src/entry/mod.rs:31-42` — `impl Clone for Entry<C> where C::D: Clone`). **`Cmd` must derive `Clone`** or the log store can't clone entries out for `try_get_log_entries`.
- `impl RaftStateMachine<TypeConfig> for Arc<MemStateMachine>` with `&mut self` compiles — interior mutability via `Mutex` is what makes it work. The app keeps its own `Arc` clone for direct reads.
- `SnapshotBuilder` does **not** have to be `Self` — a separate unit struct works, which is the clean way to express "no snapshots".
- `RaftError::APIError(InitializeError::NotAllowed(_))` pattern-matches as written.
- No `#[async_trait]` anywhere.

### Adaptation checklist for the real rEtcd code

1. `MemLogStore` → RocksDB `raft_log` + `raft_meta` CFs, big-endian u64 index keys, `flush_wal(true)` before `save_vote` returns and before `log_io_completed(Ok(()))`.
2. `MemStateMachine` → RocksDB `kv` + `state_meta` CFs with **one atomic synced `WriteBatch` per `apply()`** covering kv, public revision, `last_applied`, membership.
3. `NetConn` → tonic client over mTLS; map `hard_ttl()` to the request deadline; map errors per §4.
4. Add cluster-id / node-id / recovery-epoch identity records to `state_meta` and refuse startup on mismatch (M2 acceptance) — openraft has no hook for this, do it before `Raft::new`.
5. Wrap `ensure_linearizable()` in `tokio::time::timeout` and map the errors per §5.4.
6. Never expose `trigger().snapshot()` / `trigger().purge_log()`.
7. Expose `metrics().borrow().running_state` on the health surface.
