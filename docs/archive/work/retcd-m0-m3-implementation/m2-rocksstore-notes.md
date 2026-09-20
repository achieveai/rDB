# M2 `RocksStore` — implementation notes (Developer/Opus)

Date: 2026-09-18. Branch `feature/m0-m3`. Files: `crates/config-storage/src/rocks.rs`,
`src/util.rs`, `src/lib.rs`, `Cargo.toml`, `tests/rocks.rs`, root `Cargo.toml`.

## Build blocker found and fixed (root `Cargo.toml`)

`rocksdb = { version = "0.23", default-features = false, features = ["lz4"] }` **cannot build on
Windows**. `default-features = false` drops `bindgen-runtime`, so `clang-sys` is compiled without
`libloading` and links `libclang.dll` as a *load-time* import. The Windows loader does not read
`LIBCLANG_PATH` (which ADR-0017 sets in `.cargo/config.toml`), so the `librocksdb-sys` build
script fails to start:

```
process didn't exit successfully: ...\build-script-build (exit code: 0xc0000135, STATUS_DLL_NOT_FOUND)
```

`dumpbin /dependents` on the build-script exe confirms `libclang.dll` in the import table.
Fix: add the build-script-only feature `bindgen-runtime` to the pin. It changes nothing about the
compiled database. RocksDB then builds in **2m47s** cold (librocksdb-sys C++ + bindgen).

## Encoding trap (cost one test cycle)

Values are postcard-encoded. postcard is **not self-describing**: writing `Option<LogId>` and
reading `LogId` back does not fail — the `Some` tag byte shifts every field, so
`LogId{term:7,node:1,index:42}` came back as `{term:1,node:7,index:1}`. Rule for this store:
**the stored value is always the bare `T`; absence is the key being absent.** Applies to
`raft_meta/committed` and `state_meta/last_applied`.

## Design decisions

- CF names exactly per ADR-0008 §9.2: `raft_log`, `raft_meta`, `kv`, `state_meta`.
- Log keys are 8-byte big-endian so RocksDB's bytewise order *is* log order.
- Append is `write_opt(sync=false)` → `BeforeLogFlush` → `flush_wal(true)` → `AfterLogFlush` →
  callback (TA-13.1). A single `set_sync(true)` write would collapse three boundaries into one.
- `apply` is one `set_sync(true)` `WriteBatch`: kv delta + `cluster_revision` + `last_applied`
  (+ membership when present). The kv delta comes from `CommandResponse::event()`, which already
  carries key/value/create_revision — no second `KvState` lookup.
- `KvState` is mirrored in memory so `StateReader` stays synchronous for the engine read path.
  A *real* RocksDB error poisons the store (ADR-0008 "node marked Fatal"), which is what keeps
  the mirror and the disk from diverging. An *injected* `Fail` never poisons.
- Every DB call runs on `tokio::task::spawn_blocking` via `RocksShared::run`, inside the node span.
- `Drop for RocksShared` is empty (TA-14). Caveat: rocksdb 0.23 does not expose
  `avoid_flush_during_shutdown`, and bytes from an unsynced `write_opt` are already in the OS
  file, so a same-process crash cannot un-write them. That is exactly M2-22's "may or may not be
  present"; the per-boundary visibility table in `tests/rocks.rs` encodes it.

## Per-boundary crash visibility (asserted by `m2_storage_15`)

Seed: vote term 1, entries 1..=2 appended + applied. Crash phase: vote term 5, entry 3.

| Boundary | vote term after reopen | entry 3 | last_applied | cluster_revision |
|---|---|---|---|---|
| BeforeVoteSync | 1 | absent | 2 | 2 |
| AfterVoteSync | 5 | absent | 2 | 2 |
| BeforeLogAppend | 5 | absent | 2 | 2 |
| AfterLogAppend | 5 | either | 2 | 2 |
| BeforeLogFlush | 5 | either | 2 | 2 |
| AfterLogFlush | 5 | present | 2 | 2 |
| BeforeStateBatch | 5 | present | 2 | 2 |
| AfterStateBatch | 5 | present | 3 | 3 |

## Engine wiring still needed (not mine to edit)

`config-engine` needs a `StorageHandle::Rocks(RocksStore)` arm: `log_store()`, `state_machine()`,
`reader()`, `is_fresh()`, `durability()`, `applied_commands()`, `counters()` all mirror
`EphemeralStore`'s names, so the arm is mechanical. `open()` must be called before `Raft::new`
and its `StorageOpenError` surfaced (exit code 2 on `IdentityMismatch`, TA-21).
