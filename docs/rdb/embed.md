# Embed rDB in your own Rust project

rDB runs inside your process. It keeps its data in one folder you name, and it needs a running
rEtcd cluster for its control records. This guide takes a new crate outside this repository to a
working app. `crates/rdb-pilot` (`pilot_kv`) is the worked example; every step here is how it is
built.

> **Warning: your folder belongs to one rEtcd.** The folder records which rEtcd it was opened
> against. If you `clean` that rEtcd, or point the app at another one, the folder refuses to open
> for good (`Mismatch`, below). Nothing is lost from the folder, but nothing opens it either.
> Never run `local-cluster.sh clean` on the rEtcd your app's data needs.

## 1. What you need

- **Rust** stable, 1.89 or later, MSVC target on Windows.
- **LLVM**, because RocksDB's build runs `bindgen`. Windows: `winget install LLVM.LLVM`.
- **`LIBCLANG_PATH`** set to LLVM's `bin` folder. This repository sets it in
  `.cargo/config.toml`, but that file **does not apply outside the repository**. Set it yourself,
  either as an environment variable or in your own project's `.cargo/config.toml`:

  ```toml
  [env]
  LIBCLANG_PATH = { value = "C:\\Program Files\\LLVM\\bin", force = false }
  ```

- **Time for the first build.** It compiles RocksDB from source: several minutes
  (6 min 37 s measured on the dev host). Later builds reuse it.
- A checkout of this repository. You depend on its crates by path; nothing is published.

## 2. Your `Cargo.toml`

Replace `<checkout>` with the absolute path of this repository, forward slashes on Windows too.

```toml
[package]
name = "my-app"
version = "0.1.0"
edition = "2021"

[dependencies]
rdb-api = { path = "<checkout>/crates/rdb-api" }          # the database
config-core = { path = "<checkout>/crates/config-core" }  # the ConfigStore trait
config-client = { path = "<checkout>/crates/config-client" } # the rEtcd gRPC client
tokio = { version = "1", features = ["rt-multi-thread", "time"] }

# RocksDB is unusably slow unoptimized. Optimize dependencies, keep your own code debuggable.
[profile.dev.package."*"]
opt-level = 2
```

- Never use `workspace = true` keys; they only work inside this repository.
- Copy this repository's `Cargo.lock` into your crate before the first build. It pins the
  versions rDB was tested with; cargo adds your crate and drops what you do not use.
- Optional: `config-log = { path = "<checkout>/crates/config-log" }` gives JSONL logs you can
  query with DuckDB. `pilot_kv` uses it.

## 3. Run rEtcd for development

One way, from the checkout, in Git Bash:

```bash
scripts/local-cluster.sh up --dir C:/my-app/rEtcd --base-port 28400
```

- It starts three rEtcd nodes and prints their client ports: `base + 2`, `base + 12`,
  `base + 22` (here 28402, 28412, 28422). Your app passes all three.
- Give it its own `--dir` and `--base-port`; never share them with another project.
- **One rEtcd holds one rDB folder.** A new folder opened against an rEtcd that already serves
  another folder is refused with `Mismatch` ("this control store already holds another
  database"). The new folder is left empty. For a second folder, start a second rEtcd with
  another `--dir` and `--base-port`.
- Stop it with `scripts/local-cluster.sh down --dir C:/my-app/rEtcd`; start it again with `up`
  and the same arguments; rEtcd keeps its data across a `down` and `up`.
- `clean` deletes the cluster's data. See the warning at the top.

It runs without TLS and with every caller allowed. That is for one machine, never production.

## 4. Open, wait, use, shut down

```rust
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use rdb_api::{Db, DbConfig, Timeouts};

fn main() -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    // Plain gRPC (TlsMode::Insecure is the default): for a local rEtcd only.
    let client = config_client::GrpcClient::connect(
        vec!["127.0.0.1:28402".into(), "127.0.0.1:28412".into(), "127.0.0.1:28422".into()],
        config_client::GrpcClientOptions::default(),
    )?;
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(client);
    let config = DbConfig {
        dir: "C:/my-app/data".into(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
    };
    // The same call opens a new folder or one that already holds data.
    let mut db = rt.block_on(Db::open(config, store, rt.handle().clone()))?;
    // Ready: node 1 admits writes. Errors if not ready in 60 s, or if a reopen's repair failed.
    db.wait_ready(Duration::from_secs(60))?;
    db.put(b"greeting", b"hello", None).map_err(|refused| refused.error)?;
    let got = db.get(b"greeting")?;
    if got.value.map(|(_, bytes)| bytes.to_vec()) != Some(b"hello".to_vec()) {
        return Err("read back something other than what was written".into());
    }
    db.shutdown();
    Ok(())
}
```

- `Db::open` is async; run it on your runtime. `wait_ready`, `put` and `get` are plain blocking
  calls; do not call them from inside an async task.
- Every error here returns from `main`, which prints it and exits with code 1. A `Db` that is
  dropped on the way out shuts itself down. In a real app, match on the error instead (section 6).
- A refused `put` hands back its request in `refused.request`, so you can send it again with
  `db.resend`. The sample just returns the reason.
- Open-to-ready took about 2.7-2.9 s on the dev host for a new folder, and about 5.5 s for a
  reopen (`pilot_kv`, see section 7). Most of it is fixed waits inside rDB, not work. Do not treat
  those figures as a promise.
- Call `shutdown` before you exit. A killed process is safe too: see section 5.

## 5. Reopening your folder

Open the same folder against the same rEtcd with the same `Db::open` call. There is nothing
extra to do, after a clean `shutdown` or after the process was killed.

- **Every acknowledged write is there, with its value.** A `put` that returned `Ok` survives a
  kill and a reopen.
- **A write in flight when the process died may or may not be there.** Look it up by its request
  id with `status` after reopening; never assume either way.
- **The reopen repairs in the background.** `Db::open` returns once the nodes are started and the
  repair has begun. `wait_ready` waits for both and returns the repair's error if it failed.
  `db.heal_state()` shows where it stands: `NotNeeded` (a new folder), `Running`, `Handed` (done)
  or `Failed`.
- **One process opens a folder at a time.** A second open, in this process or another, is refused
  with `InUse`. The first is not disturbed.
- **If rEtcd is down at open,** the open fails with `Control` within a few seconds (5 s on the
  dev host). Open again once rEtcd is back.
- Each open counts as one "reopen" of the folder's history. See the ceilings in section 7 for
  what that costs over many reopens.

### Known limits in this release

- **A reopen after an open that wrote nothing does not become ready.** `wait_ready` returns
  `TimedOut` (`pilot_kv` exits 5). This is an open bug in the reopen work. Until it is fixed,
  write at least once in every open of a folder.
- **Damaged data on one node is reported, not repaired.** If one node's stored data is corrupt,
  rDB does not move ownership to a clean copy. Reads and writes that reach it fail with an
  `Unavailable` error whose message says `storage read fault Corrupt`. You never get a wrong or
  missing value instead. Moving to a clean copy comes with a later release (rejoin and recovery).

## 6. When an open is refused

`Db::open` returns `OpenError`; `wait_ready` returns `ReadyError` (in `rdb_api::db`). Match on
them. Nothing below removes data from the folder.

**Refused for good.** No open of this folder, against this rEtcd, will ever succeed. Retrying
does not help; the "what to do" column does.

| Error | When | What to do |
|---|---|---|
| `OpenError::Mismatch` | The folder and the rEtcd do not belong together: the rEtcd was cleaned, restored, or is another cluster; or a new folder meets an rEtcd that already serves another one | Point at the right rEtcd, or start a separate rEtcd |
| `OpenError::Stale` | The folder is an old copy: the rEtcd records a newer state than any copy here holds | Open the current folder; this copy is history |
| `OpenError::Unbound` | The folder was made before M10 (M9's `rdb_dev --keep`) and never bound to an rEtcd | Start a new folder; M9 dev data is not migrated |
| `OpenError::CopyMissing` | One of the folder's `nodes/<n>` directories is missing or empty while another holds data | Restore that directory from where it came from |
| `OpenError::Counter` | The folder's open counter is used up (2^24 opens), unreadable, or absent from this rEtcd | Start a new folder; `Absent` means the wrong rEtcd, as for `Mismatch` |
| `ReadyError::Heal` | The reopen's repair failed | Read the message; open again only after fixing its cause |

**Worth retrying.**

| Error | When | What to do |
|---|---|---|
| `OpenError::InUse` | Another open, in this process or another, holds the folder | Shut that one down first |
| `OpenError::Control` | rEtcd did not answer a read the open needs | Open again once rEtcd is up |
| `ReadyError::TimedOut` | Node 1 did not admit writes in time; the message names the recovery phase | Shut down and open again; report it if it repeats |

`pilot_kv` prints `open refused: <Variant>: <detail>` and exits 3 for an `OpenError` or a
failed repair, and prints `not ready: ...` and exits 5 for `TimedOut`.

## 7. The pilot, `pilot_kv`

```bash
cargo run -p rdb-pilot -- --dir C:/my-app/data --control 127.0.0.1:28402,127.0.0.1:28412,127.0.0.1:28422 --write 1000 --verify --bench
```

| Flag | What it does |
|---|---|
| `--dir D` | The rDB folder, new or already holding data. Required |
| `--control E` | rEtcd client endpoints, comma-separated. Required |
| `--write N` | Put N new keys. Each acknowledged put is appended to `D/pilot_kv.journal` before the next |
| `--reopens K` | Shut down and open again, K times, after the writes. `--verify` and `--bench` run after the last |
| `--verify` | Read back every journalled key and check its value. `verified J/J`, or exit 4 |
| `--bench` | Read each journalled key once; print get p50 and p99 |
| `--ready-secs S` | How long to wait for ready (default 60); exit 5 after |
| `--log-dir L` | JSONL log folder (default `D.logs`). Logs carry counts, never keys or values |

Every open prints `ready ms=<open-to-ready> heal=<state>`; a reopen prints
`reopen <i> ready ...`. `heal=NotNeeded` is a new folder; `heal=Handed` is a reopen whose
repair finished.

Exits: 0 done; 1 a put or read failed; 2 bad arguments; 3 open refused, or a reopen's repair
failed (`open refused: <Variant>: ...`); 4 `--verify` found a journalled key missing or wrong;
5 not ready in time (`not ready: ...`).

The journal is written without buffering, so a killed process keeps every line it wrote. It is
not synced to disk, so it proves nothing about power loss. After a kill, `--verify` may also find
the one key after the last journalled line: that put was in flight. It is reported apart as
`unjournalled_present` and does not fail the check.

### Measured on the dev host

`pilot_kv`, dev profile, three local rEtcd nodes, 2026-10-23:

| Run | Open-to-ready | Result |
|---|---|---|
| New folder, `--write 200 --verify` | 2.7-2.9 s | `verified 200/200` |
| `--write 300 --reopens 1 --verify --bench` | 2.7 s, reopen 5.7 s | `verified 300/300`; get p50 305 us, p99 629 us |
| Reopen after a kill mid-write, `--verify --bench` | 5.5 s | `verified 1246/1246`; get p50 280 us, p99 709 us |

`--reopens` above 1 runs into the first known limit in section 5: only the first reopen follows
writes.

### Ceilings: many reopens

Each open adds one level to the folder's history. Reads walk those levels, so they slow as the
count grows. rDB caps it (ruling A-R166):

- A reopen whose history is already 32 levels deep copies the merged data into a fresh level 1
  instead of adding a 33rd. No read walks more than 32 levels.
- That reopen blocks while it copies, once every 32 reopens. Measured on the dev host (dev
  profile, `snapshot_bench --flatten-keys`, 2026-10-22):

  | Keys | Copy time | Records copied | get p50 after |
  |---|---|---|---|
  | 1k | 40 ms | 2,311 | 3.0 us |
  | 10k | 356 ms | 20,311 | 2.6 us |
  | 100k | 3.05 s | 200,311 | 4.6 us |

  About 15 us per record copied.
- Just before the cap (32 levels, 1k keys): get p50 60 us, a 10-record page p50 178 us. At 1
  level: 3 us and 17 us.
- Not yet bounded: old levels are kept, so the folder grows by one full copy every 32 reopens
  until garbage collection lands (M11). The request-history chain still grows one level per
  reopen.

Source: `crates/rdb-storage/README.md`, "Chain-depth ceiling".
