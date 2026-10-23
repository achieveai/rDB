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

- **Rust** stable, 1.85 or later, MSVC target on Windows.
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
- **One rEtcd holds one rDB folder.** A new, empty folder opened against an rEtcd that already
  serves a folder is refused with `Bootstrap(AlreadyExists { .. })`, and nothing is written.
  For a second folder, start a second rEtcd with another `--dir` and `--base-port`.
- Stop it with `scripts/local-cluster.sh down --dir C:/my-app/rEtcd`; start it again with `up`
  and the same arguments; rEtcd keeps its data across a `down` and `up`.
- `clean` deletes the cluster's data. See the warning at the top.

It runs without TLS and with every caller allowed. That is for one machine, never production.

## 4. Open, wait, use, shut down

```rust
use std::sync::Arc;
use rdb_api::{Db, DbConfig, Timeouts};

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    // Plain gRPC (TlsMode::Insecure is the default): for a local rEtcd only.
    let client = config_client::GrpcClient::connect(
        vec!["127.0.0.1:28402".into(), "127.0.0.1:28412".into(), "127.0.0.1:28422".into()],
        config_client::GrpcClientOptions::default(),
    )
    .unwrap();
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(client);
    let config = DbConfig {
        dir: "C:/my-app/data".into(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
    };
    let mut db = rt.block_on(Db::open(config, store, rt.handle().clone())).unwrap();
    // Ready: node 1 admits writes.
    while !db.node_status().iter().any(|s| s.node.0 == 1 && s.admits == Some(true)) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    db.put(b"greeting", b"hello", None).unwrap();
    let got = db.get(b"greeting").unwrap();
    assert_eq!(got.value.map(|(_, bytes)| bytes.to_vec()), Some(b"hello".to_vec()));
    db.shutdown();
}
```

- `Db::open` is async; run it on your runtime. `put` and `get` are plain blocking calls; do not
  call them from inside an async task.
- Open-to-ready took about 7.7 s on the dev host (measured 2026-10-22, `pilot_kv`). Almost all of
  it is three fixed waits inside rDB, not work. Do not treat that figure as a promise.
- Call `shutdown` before you exit. A killed process is safe too: see section 5.

## 5. Reopening your folder

> **Status: lands with M10 slice 1 (S1-1).** Until then, a second `Db::open` on a folder that
> already holds data is refused with `NotEmpty`, and `pilot_kv` exits 3. This section states
> the contract that change delivers; it is rewritten against the real API when it lands.

- Open the same folder against the same rEtcd, after a clean `shutdown` or after the process was
  killed. The open recovers, and every write that was acknowledged is there with its value.
- A write in flight when the process died may or may not be there. Look it up by its request id
  with `status` after reopening; never assume either way.
- Only one process opens a folder at a time. A second one is refused (`InUse`), and the first is
  not disturbed.
- If rEtcd is down at open, the open fails at its deadline and the folder is untouched. Open again
  once rEtcd is back.

## 6. Refused for good

These errors mean no open of this folder, against this rEtcd, will ever succeed. Nothing was
written. Retrying does not help; the "what to do" column does.

| Error | When | What to do | `pilot_kv` exit |
|---|---|---|---|
| `Mismatch` | The rEtcd is not the one this folder was opened against: it was cleaned, restored, or is another cluster | Point at the right rEtcd, or start a new folder | 3 |
| `Stale` | The folder is an old copy: the rEtcd records a newer state than any copy here holds | Open the current folder; this copy is history | 3 |
| `Unbound` | The folder was made by M9's `rdb_dev --keep` and was never bound to an rEtcd | Start a new folder; M9 dev data is not migrated | 3 |
| `CopyMissing` | One of the folder's `nodes/<n>` directories is missing or cannot be opened | Restore that directory from where it came from | 3 |
| Caps exhausted | More than 2^24 opens, or the restart or request-id range used up | Start a new folder (not reachable in practice) | 3 |

`pilot_kv` prints `open refused: <Error>: <detail>` and exits 3 for every one of them.

## 7. The pilot, `pilot_kv`

```bash
cargo run -p rdb-pilot -- --dir C:/my-app/data --control 127.0.0.1:28402,127.0.0.1:28412,127.0.0.1:28422 --write 1000 --verify --bench
```

| Flag | What it does |
|---|---|
| `--dir D` | The rDB folder. Required |
| `--control E` | rEtcd client endpoints, comma-separated. Required |
| `--write N` | Put N new keys. Each acknowledged put is appended to `D/pilot_kv.journal` before the next |
| `--verify` | Read back every journalled key and check its value. `verified J/J`, or exit 4 |
| `--bench` | Read each journalled key once; print get p50 and p99 |
| `--ready-secs S` | How long to wait for ready (default 60); exit 5 after |
| `--log-dir L` | JSONL log folder (default `D.logs`). Logs carry counts, never keys or values |

Exits: 0 done; 1 a put or read failed; 2 bad arguments; 3 open refused; 4 `--verify` found a
journalled key missing or wrong; 5 not ready in time.

The journal is written without buffering, so a killed process keeps every line it wrote. It is
not synced to disk, so it proves nothing about power loss.

### Ceilings

Measured with `pilot_kv --bench` on the dev host. Owed: the 1k and 10k key figures and reopen
depth 1, 10 and 100 (S1-7).
