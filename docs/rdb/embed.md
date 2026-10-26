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

- **Time for the first build.** It compiles RocksDB from source: 5 to 10 minutes on a cold
  build (7 min 08 s for an outside crate on the dev host, 2026-10-24). Later builds reuse it.
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
serde_json = "1"                                          # to save a request (section 5)

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
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

// Print an error with `{}`, not `{:?}`: its text says what failed, and most end with the next step.
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
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
        ack_floor: true,
        dir: "C:/my-app/data".into(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: rdb_api::db::ALL_NODES.to_vec(),
        links: None,
        client: rdb_api::db::DEFAULT_CLIENT,
        boot: None,
        storage: rdb_api::StorageOptions::default(),
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
- The runtime whose handle you pass must keep running until `db.shutdown()` returns. Control
  calls to rEtcd, watches and the reopen's repair run as tasks on it, also while your thread
  sits in `wait_ready`, `put` or `get`. A multi-thread runtime, as above, does that. A
  current-thread runtime runs its tasks only inside its own `block_on`: once `block_on`
  returns, those tasks stop, and an open that still needs them does not reach ready.
- Every error here returns from `run`. `main` prints its text, which says what failed and, for
  most errors, the next step (`InUse`, for one, says to close the other open), and exits with
  code 1. A `Db` that is dropped on the way out shuts itself down. In a real app, match on the
  error instead: section 6 gives the step for every refused open.
- On success it prints `read back hello at version N`. Run it a second time: the same code
  reopens the folder (section 5), the `put` writes a new version over the old one, and the line
  shows a higher `N`. `got.value` is `Some((version, bytes))`, or `None` for an absent object.
- A refused `put` says how to send it again in `refused.error.no_mutation`. False: it may have
  applied, so send the same request, from `refused.request`, with `db.resend`. True: it wrote
  nothing, so send it as a new write (section 5, "When writes pause"). The sample just returns
  the reason.
- Open-to-ready took about 2.7-2.9 s on the dev host for a new folder, and about 5.5 s for a
  reopen (`pilot_kv`, see section 7). Most of it is fixed waits inside rDB, not work. Do not treat
  those figures as a promise.
- Call `shutdown` before you exit. A killed process is safe too: see section 5.

### A transaction and a map

`txn` writes several objects at once: all of them land, or none. `write` with `Change::Map`
changes a map's entries; `read` with `Look::Members` reads them a page at a time. Both functions
take the `db` from the sample above. `rdb_api::db::DEFAULT_GROUP` is the group `put` and `get`
use; name it in every `TxnPut`.

```rust
use rdb_api::db::DEFAULT_GROUP;
use rdb_api::{Change, Db, ElemOp, Look, Seen, TxnPut, Value};
use std::error::Error;
use std::time::Duration;

/// Two objects in one transaction: both are written, or neither. `if_version: Some(v)` writes
/// only while the object is still at version `v`; a change since then refuses the whole
/// transaction with `CONDITION_FAILED`, nothing written.
fn transfer(db: &Db) -> Result<(), Box<dyn Error>> {
    let seen = db.get(b"acct-a")?.value.map(|(version, _)| version);
    let ok = db
        .txn(
            &[
                TxnPut {
                    group: DEFAULT_GROUP, // where `put` and `get` work
                    object: b"acct-a",
                    value: b"90",
                    if_version: seen,
                },
                TxnPut {
                    group: DEFAULT_GROUP,
                    object: b"acct-b",
                    value: b"10",
                    if_version: None, // write over whatever is there, or create it
                },
            ],
            Duration::from_secs(5), // the deadline; at most 30 s
        )
        .map_err(|refused| refused.error)?;
    println!("both written at {:?}", ok.seq);
    Ok(())
}

/// A map: put two entries in one write, then read every entry, 100 at a time.
fn profile(db: &Db) -> Result<Vec<(Value, Value)>, Box<dyn Error>> {
    let text = |s: &str| Value::Text(s.to_owned());
    db.write(
        b"profile",
        Change::Map(vec![
            ElemOp::Put(text("name"), text("ada")),
            ElemOp::Put(text("lang"), text("rust")),
        ]),
        None,
    )
    .map_err(|refused| refused.error)?;
    let mut entries = Vec::new();
    let mut after = None; // start from the first entry
    loop {
        let read = db.read(b"profile", Look::Members { after, limit: 100 })?;
        let Seen::Members(page) = read.seen else {
            return Err(format!("profile is not a map: {:?}", read.seen).into());
        };
        let full = page.members.len() == 100;
        after = page.members.last().map(|last| last.key.clone());
        for member in page.members {
            entries.extend(member.value.map(|value| (member.key, value)));
        }
        if !full {
            return Ok(entries);
        }
    }
}
```

- `transfer` refuses with `CONDITION_FAILED` when `acct-a` changed after the `get`. Read again
  and decide; nothing was written.
- One transaction holds at most `rdb_api::MAX_OPS` puts, all in one group.
- A map key or value is any `Value`; here, text. `Look::Members` lists them in key order.
- Sets work the same way with `Change::Set` and `ElemOp::Add` / `ElemOp::Remove`.
- This code is compiled and run by the rows in `crates/rdb-api/src/db/reopen_tests.rs`
  (`guide_sample`), so it matches this release.

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

### After a restart: did my write land?

To ask later, you need something saved before the process stopped. There are two ways.

- **Save the id before the send.** For a write you must be able to settle after a kill, choose
  its request id yourself. Build it with `rdb_api::app_request_id(n)` from your own number `n`
  (it returns `None` when `n` is 2^63 or more). Save the id, synced, then send with
  `db.put_as(id, ..)`, `db.txn_as(id, ..)` or `db.write_as(id, ..)` (`put_saved` below does it
  for a put). Never use an id twice. `put`, `txn` and `write` never use an id built this way,
  so you can mix them in one folder. The `_as` calls refuse any other id with
  `INVALID_ARGUMENT` and send nothing. A send refused before it went out, and `write_as`'s
  `Ok(None)` (the change came to nothing), say nothing about an earlier send under that id:
  ask `status`.
- **Save a request whose answer was not definitive, and only that.** That is any refusal with
  `no_mutation` false that hands the request back in `refused.request`, boxed: save
  `*request`. `UNKNOWN_OUTCOME` is the common one; `UNAVAILABLE` "went out of reach while this
  call waited ...; a write sent here may have applied" is another. That refusal has a second
  form, with `no_mutation` true, no request, and the words "this call writes nothing ... so
  nothing was applied": the owner went away before the write was sent. There is nothing to
  save; send the write again, as new, once the owner is back. A `TxnRequest`
  serializes with `serde_json` (in the `Cargo.toml` of section 2). Write it to a file and sync
  it (`File::sync_data`). **Delete it on any definitive answer**: `Ok`, or a refusal with
  `no_mutation` true. Never save a request after `Ok`: the ack floor (below) drops its answer
  once a later write is answered, and then asking can only say "read the object". So after a
  restart only unanswered requests remain. The owner keeps their answers too, with three
  limits: at most 8 unsettled writes per open, not across a restart of the owner, and not past
  the dedup retention. Past those, asking answers "read the object".
  A `put` builds its request inside `Db`, so a kill before any answer leaves nothing to save:
  use the first way for writes that must survive that.

With neither, a kill before any answer leaves nothing to ask about. Read the data with `get`.

**What a resend answers after the app restarts** depends on where the copies run:

| Mode | An app restart is | A saved request, resent, answers |
|---|---|---|
| Embedded (this page: `local` lists the nodes) | a reopen, so a new generation | `GENERATION_CHANGED`, with nothing applied by that send. Then ask `status` |
| Three processes ([`local-cluster.md`](local-cluster.md): `local` empty) | a new open in the **same** generation | its own answer, replayed (`Ok`); or `Ok` because it applied now, if it never reached the owner; or `STATUS_EXPIRED` if its answer was dropped. `GENERATION_CHANGED` only if a node process restarted meanwhile |

`did_it_land` below handles both. Rows
`m10_r357_the_guide_s_did_it_land_answers_an_embedded_app_after_its_reopen` (embedded) and
`m10_r357_the_guide_s_did_it_land_answers_an_app_restarted_over_the_links` (three processes) in
`crates/rdb-api/src/db/reopen_tests.rs` run it.

Then ask:

- **Ask `db.status(id, generation)`.** `Resolved` means it landed. Its `outcome` is
  `RecoveredApplied` when the answer comes from history kept across the reopen.
- **With three processes, after a node restart,** a resend answers `GENERATION_CHANGED`. Then
  `status`, with the generation it was sent in or with none, answers `Resolved` (outcome
  `RecoveredApplied`) for a write the new generation kept. Row
  `m10_r394_e2_a_write_carried_across_a_secondary_restart_is_resolved_by_status` runs it.
- **The second argument is the generation the request was sent under**: the request's
  `expected_generation`, or the `generation` of its answer. Pass `None` when you do not know it,
  as after `put_saved`. A request that landed still answers `Resolved`. Only the not-landed
  answers differ: without it, a record from an older generation that is gone answers `Unknown`
  instead of `Expired`.
- **`Unknown`, `Expired` and `STATUS_EXPIRED` are never proof that it did not land** (spec
  §8.1). `STATUS_EXPIRED` and `Expired` mean the answer was dropped. That happens when the app
  was answered for it earlier, when it was one of more than 8 unsettled writes of one open,
  when the owner restarted while it was unsettled, or when it is older than the dedup retention
  (`DbConfig::dedup_retention`, 24 h by default). The refusal's text says whose floor it is
  below: "below this open's ack floor", or "an earlier open of this client received an answer
  for it, or it stayed unsettled past the dedup retention". Either way, read the object: its
  value is the answer. Never send
  it as a new write on that refusal alone.
- **Ask only after `wait_ready` returns `Ok`.** After that, do not keep asking: decide with
  `get`, or give up and report it. A request of the current open still in flight can answer
  `Unknown` until it publishes. Ask again a few times within a few seconds at most. Never poll
  `status` forever.
- **After `GENERATION_CHANGED`, a `NotKnown` that stays means: read the object.** `did_it_land`
  then asks `status` about the first send. A write the new generation kept answers `Yes`, as
  above. If it still answers `NotKnown` after a few asks over a few seconds, the write was cut
  off or is not known, and asking more will not settle it. Read the object: its value is the
  answer. Never send the request as a new write.

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

/// What became of a saved request, and what the app does next.
#[derive(Debug, PartialEq, Eq)]
enum Landed {
    /// It landed. Delete the saved copy.
    Yes,
    /// An answer for it came earlier, or it is too old to settle. Not proof either way: read
    /// the object to see its value, then delete the saved copy.
    ReadTheObject,
    /// Not known yet, which is not proof that it did not land: ask again in a few seconds, or
    /// read the object. Never send it as a new write.
    NotKnown,
}

/// What became of request `id`. `sent_in`: the generation it was sent under, when you know it.
fn landed(db: &Db, id: RequestId, sent_in: Option<Generation>) -> Result<Landed, ApiError> {
    Ok(match db.status(id, sent_in)? {
        TxnStatus::Resolved(_) => Landed::Yes,
        TxnStatus::Expired => Landed::ReadTheObject,
        TxnStatus::Unknown | TxnStatus::Unresolved { .. } => Landed::NotKnown,
    })
}

/// The same question for a request saved because its answer was not definitive:
/// `refused.request` of an `UNKNOWN_OUTCOME`. `Err`: this resend was refused for another
/// reason (`UNAVAILABLE`, say); its text says when to ask again.
fn did_it_land(db: &Db, saved: TxnRequest) -> Result<Landed, ApiError> {
    let (id, sent_in) = (saved.identity.request, saved.expected_generation);
    let refused = match db.resend(saved) {
        Ok(_) => return Ok(Landed::Yes), // applied now, or its answer replayed
        Err(refused) => refused.error,
    };
    match refused.kind {
        // Its answer was dropped: answered earlier, or too old. This resend applied nothing.
        ErrorKind::StatusExpired => Ok(Landed::ReadTheObject),
        // An embedded folder was reopened: this send applied nothing; ask about the first one.
        ErrorKind::GenerationChanged => landed(db, id, sent_in),
        _ => Err(refused),
    }
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

**The ack floor lifts that limit (`DbConfig::ack_floor`, on by default).** Each write tells the
owner which earlier writes of this open already have their answer, and the owner drops those
answers. An open writing steadily then holds about two entries (its newest answer and its
floor), so the table above no longer limits it.

- **It keeps up to 8 writes whose outcome is unknown**, so `status` or a resend can still
  settle them. The ninth drops the oldest, which then answers `STATUS_EXPIRED`.
- **A dropped answer answers `STATUS_EXPIRED`** on a resend; that resend applies nothing. It is
  never proof that the write did not run: read the object to see its value. `status` with its
  generation says `Expired`; with none, `Unknown`.
- **A write is never applied twice.** A resend of a write that was answered, even `Ok`, answers
  `STATUS_EXPIRED` once its answer was dropped. After the owner restarts, a kept write that never
  applied answers `STATUS_EXPIRED` too: read the object.
- **Ids you choose (`put_as`, `txn_as`, `write_as`) get no relief.** The table above applies.
- **A cap below 3 is refused at open.** The floor and the newest answer would fill it.
- `ack_floor: false` keeps every answer for the retention, as before.

### When writes pause

Writes can pause without any error in your code. Most pauses end by themselves, and none
loses data. A write made meanwhile is refused.

**What happened to the write is in `refused.error.no_mutation`, not in the refusal's name.**

- **`no_mutation` true:** this send wrote nothing, and no earlier send exists: a first send of
  a new write. Send it again as a **new** write (`db.put`, `db.txn` or `db.write`), not with
  `db.resend`. The ack floor counts this refusal as settled: once a later write is answered, a
  resend of it answers `STATUS_EXPIRED` and applies nothing, so the write is lost.
- **`no_mutation` false:** an earlier send of this request may have applied. This is always the
  case for a resend, whatever the refusal. Send the **same** request again with
  `db.resend(..)` (the request is in `refused.request`), or ask `db.status(..)`. Never send it
  as a new write under a new id: it could apply twice.
- **`GENERATION_CHANGED` is the exception:** never replay it as it is. A resend is refused
  the same way, so ask `db.status(..)` with its request id whether an earlier send applied it.

What each refusal means, and when to retry. How to retry follows `no_mutation`, as above: a
first send refused with it true goes again as a new write, and anything else as a resend.

| Refusal | Why | When to retry |
|---|---|---|
| `PROTECTION_PAUSED` | The owner holds new writes until a copy confirms the older ones. | After a short wait. `db.node_status()` shows what writes wait on. |
| `LEASE_EXPIRED` | The owner's permission to write ran out. An owner cut off from rEtcd answers this for as long as it is cut. | After `db.wait_ready(..)`. |
| `NOT_PRIMARY` | This node no longer owns the partition: a promote moved it. The refusal names the new owner (`hint`). `no_mutation` is true: nothing was written. | At once. The `Db` re-reads the owner on this answer, so the retry goes to the new one. |
| `GENERATION_CHANGED` | A node restarted and the generation moved on. This send wrote nothing. | At once. If it was sent before, ask `db.status(..)` first whether an earlier send applied it. |
| `UNKNOWN_OUTCOME` | No outcome yet. Two ways: **after 2 s**, the owner applied the write but no second copy confirmed it (`POST_APPLY_DEADLINE_MILLIS`), and later writes pause (`PROTECTION_PAUSED`) until it resolves; or, **at the call's deadline**, no answer came at all (a put: `Timeouts::put`, 5 s; a txn: its own deadline, up to 30 s). | Ask `db.status(..)`, or resend the same request. |
| `UNAVAILABLE`, "went out of reach ...; a write sent here may have applied" | The owner's process stopped after the write was sent (`no_mutation` false). | Once it is back: `db.resend(..)` the request, or ask `db.status(..)`. |
| `UNAVAILABLE`, "went out of reach ...; this call writes nothing" | It stopped before the write was sent, or under a read or status look (`no_mutation` true). | Once it is back, as a new write. |
| `UNAVAILABLE`, "is not reachable ... nothing was sent" | No link to the owner. A read is refused this way after 1 s (`Timeouts::read`). | After a few seconds. |
| `UNAVAILABLE`, "generation N of partition P on node 1 is not yet marked activated in the open counter; nothing was sent" | The owner moved to a new generation and has not yet heard that rEtcd records it. It sends no write until then. `no_mutation` is true on a first send. Seen after a copy is killed or started. | After a few seconds. It clears by itself. |

`did_it_land` above does the "ask" part. A request that may have applied (`no_mutation` false)
is resent unchanged until it lands or is answered, once the pause ends. A write that wrote
nothing (`no_mutation` true) is sent again as a new write; it lands once the pause ends.

**Limit a retry loop by time, not by count.** With three processes, a write whose owner link
is down first asks rEtcd who owns the partition now, and waits up to `Timeouts::read` (1 s)
for that answer. With rEtcd down too, each refused `put` took about 0.8 s. A loop of 2,000
retries then runs for about 27 minutes and looks like a hang. Stop retrying a set time after
the first refusal.

While the whole rEtcd is down, each `LEASE_EXPIRED` also makes the `Db` ask rEtcd who owns
the partition. That is at most one ask every 250 ms, and each can wait up to the 1 s read
timeout.

Measured with three processes on the dev host (local-cluster.md section 5 has the walk), an
app writing all the time:

| Event | Writes refused for | Refusals seen, in order |
|---|---|---|
| A secondary killed and left down for 60 s | 15.4 s, then writes resumed **with the copy still down** | `PROTECTION_PAUSED`, `LEASE_EXPIRED`, `PROTECTION_PAUSED` |
| A secondary restarted (`lr restart 2`) | 8.6 s (older build, see below) | `UNKNOWN_OUTCOME`, `LEASE_EXPIRED`, `GENERATION_CHANGED`, `PROTECTION_PAUSED` |
| The owner restarted (`lr restart 1`) | 3.6 s | `UNAVAILABLE` (went out of reach, either form), `PROTECTION_PAUSED` |

Every acknowledged write read back afterwards (28,734, 6,217 and 13,644 of them, 0 missing).
One run each, on a busy host: treat the times as rough.

The 8.6 s was measured before a restarted secondary came back as a standby. A secondary
restart with all three copies up now pauses writes for about 0.2 s (the bullet below).

A later walk killed node 2 and started it 20 s later. The kill showed `PROTECTION_PAUSED`,
`LEASE_EXPIRED`, `UNAVAILABLE` "not yet marked activated", `PROTECTION_PAUSED`. The start gave
two more short pauses (2.2 s and 1.6 s), each `LEASE_EXPIRED` then the same `UNAVAILABLE`.
31,428 of 31,428 acknowledged writes read back.

- **A copy falls behind or stops answering.** After a short time (about 3 s), node 1 pauses
  new writes. They resume once that copy catches up, or once rDB stops waiting for it: in the
  walk, 15 s after the kill, while the copy stayed down.
- **A copy cut off long enough to be dropped stays out after the link heals.** That is the
  copy rDB stopped waiting for, in the bullet above. Writes keep working on the other two
  copies. Healing the link alone does not add it back. Restart that node: `lr restart N` with
  three processes, or `db.restart_node(NodeId(N))` embedded. It comes back as a standby first,
  then as a full copy.
- **A restarted copy waits about 5 s before it counts again.** Once it has caught up, it must
  stay within 250 ms of the owner for 5 s in a row before writes resume (`resume_hold_millis`
  and `resume_lag_millis`). Falling behind again restarts the 5 s. That wait was most of the
  3.6 to 8.6 s restart pauses above; opening the node's store takes only 37 to 55 ms. The
  first resume after an open skips the wait. An app cannot change these times in this release.
  A secondary restarted with all three copies up no longer makes writes wait this way: see the
  next bullet.
- **A secondary restarts while all three copies are up.** It comes back as a standby first. A
  standby does not count as a copy, so writes go on with the other two. Writes pause about
  0.2 s, or about 2 s twice if its data was wiped. Then it is a full copy again.
- **The owner is cut off from rEtcd.** While it is cut, it answers `LEASE_EXPIRED`. On that
  answer the `Db` re-reads who owns the partition, at most once every 250 ms. So it follows a
  promote made elsewhere, and your retries go to the new owner.
- **The cut ends after a promote.** The old owner is now fenced. It reads the owner record
  and answers `NOT_PRIMARY` naming the new owner (`hint`), with `no_mutation` true: this send
  wrote nothing. A first send goes again as a new write; a resend, whose answer has
  `no_mutation` false, goes again as a resend. Its log shows `host_owner_learned`.
- **The old owner starts again after a promote.** It is added back automatically, as a
  standby first.
- **A copy cut off across two or more reopens with no writes in between.** When it is back, it
  catches up by itself, as a copy behind one reopen does:
  - It links each generation it missed onto the data it holds, and takes the writes it lacks.
    Writes may pause until it has, then go on.
  - Nothing is lost: the other copies hold every acknowledged write.
  - Checked in rDB's simulator (`m10_d3_a_copy_behind_two_writeless_reopens_catches_up`) and on
    one node's storage (`a_copy_that_missed_two_writeless_generations_links_the_newest_onto_its_own`).
    Checked through a real open of its folder too
    (`m10_s2_59_a_copy_behind_two_writeless_reopens_catches_up_through_a_real_open`).
- **The host is starved** (CPU or disk saturated by other work). Timers run late, so writes
  can pause and then recover once the host has room.
- **rEtcd stalls for more than about 2 s,** for example during its own leader election. Node
  1's permission to write can run out. Writes resume once it is granted again. Tuning that
  margin is planned.

### Known limits in this release

- **A three-process database runs on one machine only, for now.** It is formed once, in one
  process, on one machine. Each node process then opens its own `nodes/<n>` in that shared
  folder. Forming a new database over the network is not supported yet. A node process started
  on a folder that was never formed is refused with "form the database first (lr up)".
- **A copy missed while the database is first formed never joins.** This happens when a node's
  report misses the first discovery window, for example when the host stalls during `lr up`.
  The partition then runs on 2 copies until a fix lands. Reads and writes keep working. Lose
  one more copy and the partition becomes read-only. A copy wiped after forming is not
  affected: it rejoins through the heal.
- **With 2 of 3 copies down, an app can read but not write.** Node 1 then finishes its
  recovery with only its own copy. That is what `wait_ready`'s "node 1's recovery phase is
  Committed" (or `Rebuilding`) means here: the recovery is done, but with too few copies to take
  writes. So `wait_ready` never returns `Ok`, and the sample's `?` exits after 60 s.
  - `get` and `read` do not need `wait_ready`. To start for reads, treat
    `ReadyError::TimedOut` as "no writes yet" and keep the `Db`, instead of returning the error.
  - A write is refused `RECOVERY_READ_ONLY` ("reads still work; writes return when the lost
    copies are back"). Its `no_mutation` is true: this send wrote nothing (A-R401). Send it
    again later as a new write, not with `db.resend`. A resend reports `no_mutation` false, as
    every resend does, because an earlier send may have applied it: that one is resent.
  - Writes return once a second copy is back.
  - **Reads say when they come from a read-only recovery.** `GetOk::recovery_mode` and
    `ReadOk::recovery_mode` are true whenever the partition serves read-only after a recovery
    with one copy, the old owner's included. Most at risk: after `admin promote` to a lone copy
    that was not the owner, reads serve that copy's prefix, so a write acknowledged just before
    the loss may be missing.
  - Seen in an embedded run with node 1's links to 2 and 3 held (`rdb_dev --hold 1-2,1-3`):
    `get` answered, `put` was refused as above.
- **Damaged data on one node is reported, not repaired.** If one node's stored data is corrupt,
  rDB does not move ownership to a clean copy. Reads and writes that reach it fail with an
  `Unavailable` error whose message says `storage read fault Corrupt`. You never get a wrong or
  missing value instead. Moving to a clean copy comes with a later release (rejoin and recovery).
- **A damaged map or set entry: write over it first.** A read that reaches it fails with
  `CORRUPT_HISTORY`. Repair it with a write that names its key:
  - a map: `put` the key again (or `del` it);
  - a set: `del` the member, or `del` then `add` it in one write.

  That repairs the one entry and keeps every healthy one (ADR-rdb-0013). In `rdb_dev`,
  `admin repair-element map|set <object> <K>` does this: it deletes key `K` only if it is there,
  and answers `CONDITION_FAILED` if it is not.
- **Finding the bad key.** The error does not name it; a later release will. Until then, read
  one entry at a time, `db.read(object, Look::Members { after, limit: 1 })`, with `after` set to
  the key you read last (`None` to start). The read that fails is the entry right after the last
  good key. Your app's own record of its keys says which key that is.
- **What a write does not repair.** An `add` or a `need` on a damaged set member succeeds and
  leaves it damaged. A damaged root no write repairs.
- **Clearing an object.** `db.write(object, Change::Clear, None)` (rdb_dev `admin clear`)
  deletes the object when its root **or any one entry** is damaged. **It deletes the whole map
  or set, healthy entries too.** To keep the rest:
  - a damaged entry: write over it first, as above, instead of clearing;
  - a damaged root: copy the healthy entries out first with the walk above. When a read fails,
    carry on from the next entry with `after` set to the bad key. Then clear.
- **Nothing in this build repairs a newer build's record.** An entry damaged in header byte 0 or
  3 reads as a newer build's, and so does a real newer build's entry. A write that names it, a
  full read of the object, and a clear of it are all refused `INCOMPATIBLE_VERSION`, with
  nothing changed. Open the folder with the newer build that wrote it. Repairing one such entry
  (`repair_element`) comes with a later release.
- **Rolling back to an older build is not always possible.** Once this build has written an
  ack floor or an epoch revocation into a folder, its copies are format 2. A format-1 build
  refuses to open them, and its error names the format range it reads. So roll back only onto
  a folder this build has written neither into. Otherwise, restore a backup taken before the
  upgrade.
- **An older build cannot open a folder after this build wrote `partitions/1`.** This build
  writes that record with its member list (layout 4). A build without layout 4 reads it as
  undecodable and refuses the open (`Undecodable`), and keeps refusing. Run this build or a
  newer one.
- **A restarted secondary rejoins by itself,** as a standby first: writes pause briefly, then
  resume ("When writes pause" above). **Embedded only: a restarted owner stays out until the
  folder is opened again.** That is node 1 restarted inside this open (`Db::restart_node`);
  see section 5. With three processes, a restarted owner process comes back and serves with no app reopen: writes
  were refused for 3.6 s (`lr restart 1`, local-cluster.md section 5).
- **Moving ownership off a dead node: `db.promote(NodeId(2), None)`** (`lr promote 2`, rdb_dev
  `admin promote 2`). Node 2 becomes the owner and keeps two copies, its own and node 3's.
  It first waits out node 1's permission to write, so it takes a few seconds. An app-only `Db`
  can promote too. A promote starts with rEtcd writes to `partitions/` and `grants/`, and the
  node it promotes checks the fence it is sent against those records and refuses one that is
  not true. The plan sent with the fence, which names the members, is not checked. So while a
  promote is stalled with partition 1 fencing, any app holding the link cert can finish it with
  members of its choosing. That is a known gap (issue #70). **The dropped node is added back
  automatically when it starts again, or at the next reopen heal; membership is fixed at three
  nodes.**
  - **After every node restarts, a promote still works,** even with the owner's copy lost.
    `partitions/1` keeps the member list and each member's role (record layout 4), and a
    promote plans from it.
  - **A record an older build wrote has no member list yet.** This build adds one at its first
    recovery: the open's heal, or a promote. Until then, a promote of a node that restarted
    while the owner is down answers `Unconfigured` ("node N holds no configuration since its
    restart"). The way round: bring back the owner's node, or promote a node that has not
    restarted since the owner stopped.
  - **The old owner says "moved" within about a second.** While the promote waits out its
    permission to write, the old owner answers `LEASE_EXPIRED`. Once the promote is done,
    the old owner answers `NOT_PRIMARY` naming the new owner (`hint`), with `no_mutation` true. It
    re-reads the owner record every 250 ms until then. Either answer means nothing was
    written: send again, as its step says. Its log shows `host_owner_learned`.
- **On a very slow or overloaded machine, an open can fail with a "blocked" error**
  (`ReadyError::Blocked`) when a copy answers too late. Open the folder again. If that open is
  blocked too, it is not the machine: stop and report it with the message. An automatic retry
  is planned.

## 6. When an open is refused

`Db::open` returns `OpenError`; `wait_ready` returns `ReadyError` (in `rdb_api::db`). Match on
them. Nothing below removes data from the folder.

A refused call (`put`, `get`, `read`, `write`, `status`) returns `ApiError`. Its text, `{}`, is
`NAME (detail; next: step)`: the step is what to do now. `ApiError::next_step()` gives the step
alone, and `ApiError::explain()` gives the detail with it. For example, a refusal while the
database is not serving says `UNAVAILABLE (...; next: the database is not serving right now:
wait a few seconds and try again; ...)`.

**Refused for good.** No open of this folder, against this rEtcd, will ever succeed. Retrying
does not help; the "what to do" column does.

| Error | When | What to do |
|---|---|---|
| `OpenError::Mismatch` | The folder and the rEtcd do not belong together: the rEtcd was cleaned, restored, or is another cluster; or a new folder meets an rEtcd that already serves another one | Point at the right rEtcd, or start a separate rEtcd |
| `OpenError::Stale` | The folder is an old copy: the rEtcd records a newer generation than any copy here holds | Do not delete this directory: its acknowledged writes are intact. Open the folder, or restore the backup, that holds the generation the message names |
| `OpenError::Unbound` | A copy holds data but names no rDB instance. Either the whole folder was made before M10 (M9's `rdb_dev --keep`), or the other copies name one and this copy lost its binding | M9 folder: open it with the M9 build, or start a new folder; M9 dev data is not migrated. Lost binding: restore that copy from a backup of this folder. For a node process, you can instead empty that copy and start the node again; it rejoins from the other copies |
| `OpenError::CopyMissing` | One of the folder's `nodes/<n>` directories is missing or empty while another holds data | Restore that directory from where it came from |
| `OpenError::Counter` | The folder's open counter is used up (2^23 opens, `admin::MAX_OPENS`), or holds a value this build does not write | Used up (the message says "the most one directory may have"): start a new folder. A value this build does not write (the message says "is not an open counter"): do not delete the folder, its acknowledged writes are intact; stop and report it with the message |

A `Stale` whose message says "the open counter marks generation N activated" means a later
open already served that generation, so this copy cannot catch up to it. A crash during the
commit of a reopen is not that case: the next open finishes the commit and serves, with
nothing to do. Neither is a power loss right after a reopen's commit: node 1 makes the new
generation durable before the counter can mark it. One exception remains: two crashes in a
row, the second during the reopen that followed the first, can leave a `Stale` on a folder
that lost nothing. Opening again gives the same refusal. Do not delete the folder: stop and
report it with the message.

**Not in this open.** Retrying in the same `Db` does not help; a new open may.

| Error | When | What to do |
|---|---|---|
| `ReadyError::Heal` | The reopen's repair failed. The message says why: for example, the last run's grant is still held by another open of this folder's data | Do what the message says (often: shut that other open down), then open again |
| `ReadyError::Stopped` | Node 1 restarted in this open | Shut this `Db` down and open the folder again |
| `ReadyError::Blocked` | The open's recovery stopped before the owner (the message names its node) took writes; the message names why (often a copy answered too late) | Shut this `Db` down and open the folder again, once. If that open is blocked too, stop and report it with the message; do not retry in a loop |
| `ReadyError::Faulted` | The owner (the message names its node) stopped on a fault, and it takes no writes in this open; the message carries the fault (a storage or link step that failed, say) | Shut this `Db` down and open the folder again, once. If that open stops the same way, stop and report it with the message; do not delete the folder |

**Not fixed by opening again.**

| Error | When | What to do |
|---|---|---|
| `ReadyError::Quarantined` | The copies' histories disagree, so the owner (the message names its node) takes no writes. The acknowledged data is kept | Do not delete the folder, and do not keep opening it. Run `admin rebuild N` (`Db::rebuild`) for a copy whose history disagrees: it takes that copy out and back, wiped aside. A rebuild of a copy the quarantine does not name is refused and names the ones it does. A write refused while quarantined carries the same text. It happens only if two copies hold different valid records at one position, which fencing prevents |

**Fix the cause first.** Opening again unchanged gives the same error; the "what to do"
column says what to change before the next open.

| Error | When | What to do |
|---|---|---|
| `OpenError::Io` | The filesystem refused: a wrong path, no permission, or a full disk | Fix what the message names, then open again |
| `OpenError::DedupCap` | `DbConfig::dedup_cap` is above the most allowed; nothing was started | Set it to at most the number the message names, or leave it unset |
| `OpenError::Node` | A node did not start: a copy's storage refused to open, or a copy could not be bound or linked. Nothing was removed | A newer format: open the folder with the build that wrote it. A disk or permission error: fix it, then open again. Anything else, or the same error again: stop and report it with the message |

**Worth retrying.**

| Error | When | What to do |
|---|---|---|
| `OpenError::InUse` | Another open, in this process or another, holds the folder | Shut that one down first |
| `OpenError::Control` | rEtcd did not answer, or failed, a read or the open count the open needs (a leader change, say) | Open again once rEtcd is up |
| `OpenError::Bootstrap` | A new folder's first open could not set it up in rEtcd | Check that rEtcd is running and reachable, then open again |
| `ReadyError::TimedOut` | The node that owns partition 1 did not admit writes in time. The message names that node (its `owner` field: node 1 normally, node 2 after `lr promote 2`) and what it last answered | Shut down and open again, once. If it times out again, stop and report it with the message; do not retry in a loop |

What a `TimedOut` message says the owner last answered:

- "node N is not reachable from this process: no link to it is up": node N's process is not
  running, or cannot be reached. Start it.
- "node N answered no status look in that time (it may be busy ...)": node N was reached and did
  not answer. Wait longer.

`pilot_kv` prints `open refused: <Variant>: <detail>` and exits 3 for an `OpenError` or a
failed repair, and prints `not ready: ...` and exits 5 for `TimedOut`, `Stopped`, `Blocked`,
`Quarantined` and `Faulted`.

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
`unjournalled_present` and does not fail the check. A last line a kill cut short is left out
and said; the next `--write` drops it from the file before it appends.

### Measured on the dev host

`pilot_kv`, dev profile, three local rEtcd nodes, 2026-10-23:

| Run | Open-to-ready | Result |
|---|---|---|
| New folder, `--write 200 --verify` | 2.7-2.9 s | `verified 200/200` |
| `--write 300 --reopens 1 --verify --bench` | 2.7 s, reopen 5.7 s | `verified 300/300`; get p50 305 us, p99 629 us |
| Reopen after a kill mid-write, `--verify --bench` | 5.5 s | `verified 1246/1246`; get p50 280 us, p99 709 us |

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
