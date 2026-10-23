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

- **Time for the first build.** It compiles RocksDB from source: about 3 minutes
  (3 min 2 s for an outside crate on the dev host, 2026-10-22). Later builds reuse it.
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
use rdb_api::{Db, DbConfig, Timeouts};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    // Plain gRPC (TlsMode::Insecure is the default): for a local rEtcd only.
    let client = config_client::GrpcClient::connect(
        vec![
            "127.0.0.1:28402".into(),
            "127.0.0.1:28412".into(),
            "127.0.0.1:28422".into(),
        ],
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
    // Ready: node 1 admits writes. Errors if not ready in 60 s, if a reopen's repair failed,
    // or if node 1 stopped in this open.
    db.wait_ready(Duration::from_secs(60))?;
    db.put(b"greeting", b"hello", None)
        .map_err(|refused| refused.error)?;
    // `value` is `Some((version, bytes))`, or `None` when the object is absent.
    match db.get(b"greeting")?.value {
        Some((version, bytes)) if bytes.as_ref() == b"hello" => {
            println!("read back hello at version {version}");
        }
        other => return Err(format!("read back {other:?}, not hello").into()),
    }
    db.shutdown();
    Ok(())
}
```

- `Db::open` is async; run it on your runtime. `wait_ready`, `put` and `get` are plain blocking
  calls; do not call them from inside an async task.
- Every error here returns from `main`, which prints it and exits with code 1. A `Db` that is
  dropped on the way out shuts itself down. In a real app, match on the error instead (section 6).
- On success it prints `read back hello at version N`. Run it a second time: the same code
  reopens the folder (section 5), the `put` writes a new version over the old one, and the line
  shows a higher `N`. `got.value` is `Some((version, bytes))`, or `None` for an absent object.
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
- **Never run two copies of one folder at once.** The lock covers one directory only. A copy of
  the folder in another directory (a backup you restored beside it, say) opens against the same
  rEtcd without being refused. Run both and the writes of one are lost. Keep at most one copy
  open.
- **If node 1 restarts inside an open (`Db::restart_node`), writes stop for that open.** `wait_ready` then returns
  `ReadyError::Stopped`, and every refused `put` or `get` says the same next step: shut this `Db`
  down and open the folder again.
- **If rEtcd is down at open,** the open fails with `Control` within a few seconds (5 s on the
  dev host). Open again once rEtcd is back.
- Each open counts as one "reopen" of the folder's history. See the ceilings in section 7 for
  what that costs over many reopens.

### After a reopen: did my write land?

To ask later, you need something saved before the process stopped. There are two ways.

- **Save the request after its answer.** It is in `ok.sent` on success, or `refused.request` on
  `UNKNOWN_OUTCOME`. A `TxnRequest` serializes with `serde_json`. Write it to a file, and sync
  the file (`File::sync_data`) before you rely on it. This does not cover a kill before the
  answer: `put` builds its request inside `Db`, so there is nothing to save until it returns.
- **Save the id before the send.** For a write you must be able to settle after a kill, choose
  its request id yourself. Build it with `rdb_api::app_request_id(n)` from your own number `n`
  (it returns `None` when `n` is 2^63 or more). Save the id, synced, then send with
  `db.put_as(id, ..)`, `db.txn_as(id, ..)` or `db.write_as(id, ..)` (`put_saved` below does it
  for a put). Never use an id twice. `put`, `txn` and `write` never use an id built this way,
  so you can mix them in one folder. The `_as` calls refuse any other id with
  `INVALID_ARGUMENT` and send nothing. A send refused before it went out, and `write_as`'s
  `Ok(None)` (the change came to nothing), say nothing about an earlier send under that id:
  ask `status`.

With neither, a kill before any answer leaves nothing to ask about. Read the data with `get`.

Then ask:

- **A resend answers `GENERATION_CHANGED`.** The reopen started a new generation (one chapter of
  the folder's history), and a request from an older one is never replayed or run again. That
  send changed nothing. Look the request up with `status`; do not resend it as a new request.
- **Ask `db.status(id, generation)`.** `Resolved` means it landed. Its `outcome` is
  `RecoveredApplied` when the answer comes from history kept across the reopen.
- **The second argument is the generation the request was sent under**: the request's
  `expected_generation`, or the `generation` of its answer. Pass `None` when you do not know it,
  as after `put_saved`. A request that landed still answers `Resolved`. Only the not-landed
  answers differ: without it, a record from an older generation that is gone answers `Unknown`
  instead of `Expired`.
- **`Unknown` or `Expired` is not proof that it did not land.** `Expired` means the record is
  older than the dedup retention (`DbConfig::dedup_retention`, 24 h by default). Ask only after
  `wait_ready` returns `Ok`. After that, do not keep asking: decide with `get`, or give up and
  report it. A request of the current open still in flight can answer `Unknown` until it
  publishes. Ask again a few times within a few seconds at most. Never poll `status` forever.

`crates/rdb-pilot/examples/did_it_land.rs` holds this code and is built with the workspace.

```rust
use std::error::Error;
use std::fs::File;
use std::io::Write;

use rdb_api::{
    app_request_id, ApiError, Db, ErrorKind, Generation, PutOk, RequestId, TxnRequest, TxnStatus,
};

/// A put you can look up after a kill: its id is saved, synced, before it is sent.
/// `n` is your own number for it, used once; the id is `app_request_id(n)`.
fn put_saved(
    db: &Db,
    ids: &mut File,
    n: u64,
    object: &[u8],
    value: &[u8],
) -> Result<PutOk, Box<dyn Error>> {
    let id = app_request_id(n).ok_or("n must be below 2^63")?;
    writeln!(ids, "{}", id.0)?;
    ids.sync_data()?;
    Ok(db
        .put_as(id, object, value, None)
        .map_err(|refused| refused.error)?)
}

/// `true`: request `id` landed. `false`: not known, which is not proof that it did not land.
/// `sent_in`: the generation it was sent under, when you know it.
fn landed(db: &Db, id: RequestId, sent_in: Option<Generation>) -> Result<bool, ApiError> {
    Ok(matches!(db.status(id, sent_in)?, TxnStatus::Resolved(_)))
}

/// The same question for a request saved after its answer (`ok.sent` or `refused.request`).
fn did_it_land(db: &Db, saved: TxnRequest) -> Result<bool, ApiError> {
    let (id, sent_in) = (saved.identity.request, saved.expected_generation);
    match db.resend(saved) {
        Ok(_) => return Ok(true), // same generation: applied now, or its answer replayed
        Err(refused) if refused.error.kind != ErrorKind::GenerationChanged => {
            return Err(refused.error)
        }
        Err(_) => {} // after a reopen: this send applied nothing; ask what the first one did
    }
    landed(db, id, sent_in)
}
```

### How many writes a partition takes: the dedup window

rDB keeps each write's answer for a while, so a resend of the same request replays it instead
of applying twice, and `status` can answer. That store is capped. Two `DbConfig` options set
it:

- `dedup_retention`: how long an answer is kept. Default 24 h (`None`). This is a supported app
  option.
- `dedup_cap`: how many answers are kept at once. Default and maximum 65,536.

So a partition sustains about `cap / retention` writes per second:

| `dedup_retention` | Sustained writes/s |
|---|---|
| 24 h (default) | 0.76 |
| 1 h | 18 |
| 10 min | 109 |

- **Bursts are free.** A burst may use the whole cap at once.
- **A full cap answers `OVERLOADED`.** Its `retry_after` (on `ApiError`, also in the message)
  is the earliest time an entry frees. Wait at least that long before you send again. It is
  left out when no time is known.
- **The cost of a shorter window:** a resend or `status` query made after it can no longer be
  settled. A resend is then treated as a new request.
- **Memory:** about 1.8 KB per kept answer, so about 114 MB per partition when the cap is full
  (measured on a debug build).
- **After a reopen,** old answers free on their original schedule, not a full window after the
  reopen. One exception: after a crash or kill, writes from the last mark period before it (up
  to 60 s at defaults) count against the cap for one retention after the reopen. A clean quit
  (`Db::shutdown`, or dropping the `Db`) does not have this cost.

### When writes pause

Writes can pause without any error in your code. They resume by themselves; nothing is lost.
A `put` made meanwhile is refused with a retryable error (`PROTECTION_PAUSED`, say).

- **A copy falls behind or stops answering.** Node 1 pauses writes rather than accept writes
  only one copy holds. They resume once that copy catches up.
- **The host is starved** (CPU or disk saturated by other work). Timers run late, so writes
  can pause and then recover once the host has room.
- **rEtcd stalls for more than about 2 s,** for example during its own leader election. Node
  1's permission to write can run out. Writes resume once it is granted again. Tuning that
  margin is planned.

### Known limits in this release

- **Damaged data on one node is reported, not repaired.** If one node's stored data is corrupt,
  rDB does not move ownership to a clean copy. Reads and writes that reach it fail with an
  `Unavailable` error whose message says `storage read fault Corrupt`. You never get a wrong or
  missing value instead. Moving to a clean copy comes with a later release (rejoin and recovery).
- **Clearing a damaged object removes all of it.** `db.write(object, Change::Clear, None)`
  (rdb_dev `admin clear`) deletes the whole map or set, not only the bad entry. One bad entry
  costs every healthy entry too. Repairing one entry comes with a later release.
  **If you need the healthy entries, copy them out first**, then clear. Read the object a page
  at a time with `db.read(object, Look::Members { after, limit })`. A page that reaches the bad
  entry fails with `CORRUPT_HISTORY`, and the error does not name the entry. Read with a small
  `limit` up to it; if you know the bad key, set `after` to it to read the rest.
- **A restarted node stays out until the folder is opened again.** See section 5.
- **On a very slow or overloaded machine, an open can fail with a "blocked" error**
  (`ReadyError::Blocked`) when a copy answers too late. Open the folder again. An automatic
  retry is planned.

## 6. When an open is refused

`Db::open` returns `OpenError`; `wait_ready` returns `ReadyError` (in `rdb_api::db`). Match on
them. Nothing below removes data from the folder.

**Refused for good.** No open of this folder, against this rEtcd, will ever succeed. Retrying
does not help; the "what to do" column does.

| Error | When | What to do |
|---|---|---|
| `OpenError::Mismatch` | The folder and the rEtcd do not belong together: the rEtcd was cleaned, restored, or is another cluster; or a new folder meets an rEtcd that already serves another one | Point at the right rEtcd, or start a separate rEtcd |
| `OpenError::Stale` | The folder is an old copy: the rEtcd records a newer generation than any copy here holds | Do not delete this directory: its acknowledged writes are intact. Open the folder, or restore the backup, that holds the generation the message names |
| `OpenError::Unbound` | The folder was made before M10 (M9's `rdb_dev --keep`) and never bound to an rEtcd | Start a new folder; M9 dev data is not migrated |
| `OpenError::CopyMissing` | One of the folder's `nodes/<n>` directories is missing or empty while another holds data | Restore that directory from where it came from |
| `OpenError::Counter` | The folder's open counter is used up (2^24 opens), unreadable, or absent from this rEtcd | Start a new folder; `Absent` means the wrong rEtcd, as for `Mismatch` |

A `Stale` whose message says "the open counter marks generation N activated" means a later
open already served that generation, so this copy cannot catch up to it. A crash during the
commit of a reopen is not that case: the next open finishes the commit and serves, with
nothing to do. One exception: if a crash happened during the reopen that followed a crash, the
reopen refuses with `Stale` and changes nothing. Open it again.

**Not in this open.** Retrying in the same `Db` does not help; a new open may.

| Error | When | What to do |
|---|---|---|
| `ReadyError::Heal` | The reopen's repair failed. The message says why: for example, the last run's grant is still held by another open of this folder's data | Do what the message says (often: shut that other open down), then open again |
| `ReadyError::Stopped` | Node 1 restarted in this open | Shut this `Db` down and open the folder again |
| `ReadyError::Blocked` | The open's recovery stopped before node 1 took writes; the message names why (often a copy answered too late) | Shut this `Db` down and open the folder again |

**Not fixed by opening again.**

| Error | When | What to do |
|---|---|---|
| `ReadyError::Quarantined` | The copies' histories disagree, so node 1 takes no writes. The acknowledged data is kept | Do not delete the folder, and do not keep opening it. This build has no repair command yet: stop and report it with the message |

**Worth retrying.**

| Error | When | What to do |
|---|---|---|
| `OpenError::InUse` | Another open, in this process or another, holds the folder | Shut that one down first |
| `OpenError::Control` | rEtcd did not answer a read the open needs | Open again once rEtcd is up |
| `ReadyError::TimedOut` | Node 1 did not admit writes in time; the message names the recovery phase | Shut down and open again, once. If it times out again, stop and report it with the message; do not retry in a loop |

`pilot_kv` prints `open refused: <Variant>: <detail>` and exits 3 for an `OpenError` or a
failed repair, and prints `not ready: ...` and exits 5 for `TimedOut`, `Stopped`, `Blocked` and
`Quarantined`.

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
