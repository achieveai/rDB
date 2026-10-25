# Run rDB on three processes

`scripts/local-rdb.sh` (call it `lr`) runs rDB the way it runs for real: three node processes,
one per copy, plus a three-node rEtcd. Your app holds no copy. It talks to the nodes over TLS.

This is for one machine and for testing. It is not production guidance.

If you want every copy inside your own process instead, read [`embed.md`](embed.md).

## 1. What you need

- The build prerequisites in [`embed.md`](embed.md#1-what-you-need) section 1, and a checkout of
  this repository.
- `openssl` and `curl` on `PATH`, as for [`../quickstart-local.md`](../quickstart-local.md).
- **Git Bash** on Windows, or bash on Linux. `lr` is a bash script; there is no PowerShell one.
- DuckDB, only to query the logs (section 4).

Run every `lr` command from the checkout.

Where things go:

- Binaries: `$CARGO_TARGET_DIR/debug` (default `./target/debug`). Every `up` builds them first,
  so a run never uses a stale binary. The first build takes several minutes (452 s on the dev
  host, 2026-10-24), on top of your app's own first build: they build into different target
  folders. After that, cargo skips what has not changed. `LR_BUILD=0` skips the build and uses the binaries as they are.
- Each `up` makes a new **run folder** under `C:/rdb_test_data/lr` (or `$RDB_LR_ROOT`, or
  `--root DIR`). Its name is `<commit>-<base port>-<time>`. `<root>/current` names the last one.
- In the run folder:
  - `db/nodes/1..3`: each node's copy.
  - `ctl/`: rEtcd's data and logs.
  - `certs/`: the run's TLS certificates. Apps `pilot`, `pilot-a`, `pilot-b` and `admin` have one.
  - `logs/rdb_node-N.jsonl`: node N's log. `out/node-N.out` and `.err`: its console.
  - `run.env`: the ports and binaries.
- Ports: rEtcd uses `P+1` to `P+24`. Node N listens on `P+40+N`. `P` is 19800 unless you pass
  `--base-port P`. **Keep `P+1` to `P+43` free:** `up` refuses to start if anything holds one.

## 2. The commands

```bash
scripts/local-rdb.sh up
```

- Starts rEtcd, makes certificates, forms the database, then starts nodes 1, 2 and 3.
- **Ready** is `lr: node N: rdb_node ready node=N` for each node, then
  `lr: up. control=... nodes=...`. Exit 0.
- If a node is not ready, it exits 1 and leaves the processes running for you to look at.
- Another run on this machine? Give `up` its own `--base-port P` and `--root DIR`.

Every other verb acts on the last run. If you passed `--root DIR` to `up`, pass it to every verb
too, `env` included. Or set it once with `export RDB_LR_ROOT=DIR`, and every command on this page
works as written. `--run DIR` picks another run.

| Command | What it does |
|---|---|
| `up --release` | The same, with release binaries from `.../release`. `start` and `restart` keep the run's build. |
| `status` | One line per node: `running pid ...` or `stopped`, and its last console line. Then rEtcd's table. |
| `env` | The run's ports and paths, as `KEY=value` lines. Your app reads them (section 3). |
| `stop N` | Stops node N cleanly. Prints `lr: node N stopped`. |
| `start N` | Starts node N and waits for `rdb_node ready`. Exit 1 with the node's reason if it is not ready. |
| `restart N` | `stop N`, then `start N`, with the same wait for ready. |
| `kill N` | Kills node N at once, like a crash. Prints `lr: node N killed (pid ...)`. |
| `promote N` | Makes node N the owner, through rEtcd. Done is `promoted: node N owns partition 1 at generation G ...`. `--drop M` names the node to leave out. An open app follows it; no reopen. The node left out is added back automatically when it starts again. |
| `down` | Stops the nodes and rEtcd. The run folder stays, with its data and logs. |
| `up --resume` | Starts the last run again after a `down`: its rEtcd, then nodes 1, 2 and 3, with its own ports and binary paths (a later `up` rebuilds those binaries in place). The data written before `down` is still there. A run that is still up is refused: "the run is up; 'lr down' first". Plain `up` always makes a **new**, empty run. |
| `clean` | `down`, then deletes the run folder, **its logs included**: copy what you need first. Prints `lr: removed <run folder>`. |

`scripts/local-rdb.sh` with no verb lists every verb, including the fault verbs.

## 3. Your app

Make a crate outside this repository, with the `Cargo.toml` from
[`embed.md`](embed.md#2-your-cargotoml) section 2. Its first build takes 5 to 10 minutes on a
cold build (7 min 08 s on the dev host). Put this in `src/main.rs`:

```rust
use rdb_api::net::faults::Faults;
use rdb_api::net::wire::Label;
use rdb_api::net::{certs, Net, NetConfig, PROTOCOL_VERSION};
use rdb_api::transport::Links;
use rdb_api::{BootId, ClientId, Db, DbConfig, NodeId, Timeouts};
use std::collections::BTreeMap;
use std::error::Error;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

// Arguments: CONTROL, PEERS and RUN, as `lr env` prints them.
fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [control, peers, run] = args.as_slice() else {
        return Err("usage: my-app CONTROL PEERS RUN".into());
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let client = config_client::GrpcClient::connect(
        control.split(',').map(String::from).collect(),
        config_client::GrpcClientOptions::default(),
    )?;
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(client);

    // The nodes, by address: "1=127.0.0.1:19841,2=...,3=...".
    let mut dial = BTreeMap::new();
    for entry in peers.split(',') {
        let (node, addr) = entry.split_once('=').ok_or("PEERS: want N=ADDR")?;
        dial.insert(NodeId(node.parse()?), addr.to_owned());
    }
    let nodes: Vec<NodeId> = dial.keys().copied().collect();
    // The app's name must have a certificate in the run's certs folder.
    let label = Label::App("pilot-a".to_owned());
    let links = Links::new();
    let net = {
        let _entered = rt.enter();
        Net::start(
            NetConfig {
                tls: certs::load(format!("{run}/certs").as_ref(), &label)?,
                label,
                boot: BootId(0),
                peers: nodes.iter().copied().collect(),
                dial,
                protocol_version: PROTOCOL_VERSION,
                faults: Faults::new(),
            },
            &links,
            None,
        )?
    };
    // Wait for the links. Two of three is enough to start: `wait_ready` below says whether
    // the owner takes writes.
    if let Err(down) = rt.block_on(net.wait_up(&nodes, Duration::from_secs(10))) {
        eprintln!("starting without: {down}");
    }

    let config = DbConfig {
        ack_floor: true,
        dir: format!("{run}/my-app").into(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: Vec::new(),   // this process holds no copy
        links: Some(links),  // it reaches the nodes through these links
        client: ClientId(7), // one id per app process
        boot: None,
        storage: rdb_api::StorageOptions::default(),
    };
    let mut db = rt.block_on(Db::open(config, store, rt.handle().clone()))?;
    db.wait_ready(Duration::from_secs(60))?;
    db.put(b"greeting", b"hello", None)
        .map_err(|refused| refused.error)?;
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

**The one config that differs from `embed.md`:** `local: Vec::new()` and `links: Some(links)`.
Empty `local` means this process holds no copy; `links` is how it reaches the three nodes.
Everything else is the same `Db` you would embed.

- The app needs three values: rEtcd's endpoints, the nodes' addresses and the certs folder.
  `lr env` prints all three as `CONTROL`, `PEERS` and `RUN`.
- With `local` empty, nothing is written to `dir` today. Give it a folder of the app's own
  anyway; here it is in the run folder, so `clean` removes it with the run.
- Give each app process its own `ClientId`; it keeps their writes apart in the logs. It is not
  needed for safety: two processes, even on one `dir` with one `ClientId`, never send the same
  request id. Each `Db::open` counts once in rEtcd, and each open numbers its requests from its
  own base (row `m10_r357_two_app_processes_on_one_dir_with_one_client_never_share_an_id`).
  That is also why no folder lock is taken here.

Run it from your crate's folder in Git Bash, with `<checkout>` as in `embed.md`:

```bash
vars="$(<checkout>/scripts/local-rdb.sh env)" && eval "$vars" &&
  cargo run -- "$CONTROL" "$PEERS" "$RUN"
```

- The `&&` matters. With no run, `env` prints `lr: no run ...` and exits 1. A bare
  `eval "$(... env)"` hides that and leaves the three values empty.
- It prints `read back hello at version N`. Run it again and `N` goes up. Once built, a run
  takes about 2 seconds.
- `N` counts every write to the partition, not writes to `greeting`. On a new run the first
  `hello` reads version 2: the partition already holds one entry that rDB writes itself.
- **Two of three nodes are enough to start.** With node 3 killed, the app printed
  `starting without: node 3: not reachable ...`, then `ready after 10065 ms`, and wrote and
  read `hello`. Those 10 s are the `wait_up` limit in the sample: lower it if you start often
  with a node down. With node 1 (the owner) down, `wait_ready` times out and names node 1.

### What embed.md sections 5 and 6 mean for this app

[`embed.md`](embed.md) sections 5 and 6 are written for a process that holds the copies. This
app holds none, so:

- **An app restart is not a reopen.** The nodes keep running: no new generation, no repair.
  `heal_state()` is always `NotNeeded`, and `wait_ready` only waits for the owner to take writes.
- **A saved request, resent after the app restarts,** answers its own answer again, or
  `STATUS_EXPIRED` if its answer was dropped. It answers `GENERATION_CHANGED` only when a node
  restarted meanwhile: `lr restart 2` moved the generation from 4 to 5 in the walk below. The
  `did_it_land` in embed.md section 5 handles all three. Follow its rule on what to save.
- **No `InUse`.** Nothing is locked, and two app processes may run at once (the `ClientId`
  bullet above).
- **Each open still counts once in rEtcd**, so the open counter's limit (`OpenError::Counter`,
  2^23 opens, `admin::MAX_OPENS`) covers app opens too.
- **The refusals an app open can see:** `Control` (rEtcd not reachable), `Counter`, and
  `Config` (no `links`, or a database not formed yet: "form the database first (lr up)").
  Folder errors (`Mismatch`, `Stale`, `CopyMissing`, `Node`) belong to the node processes:
  `lr status` and the node's console show them. `ReadyError::TimedOut` names the owner when it
  does not take writes in time.
- **Reopen costs and ceilings** (embed.md section 7) apply to a node process restart, not to the
  app.
- **What a write sees while the database pauses** is in embed.md, "When writes pause". Every
  refusal there is retryable; none needs the app to reopen.

When you are done, from the checkout. `clean` deletes the logs too, so read them first if you
need them (section 4):

```bash
scripts/local-rdb.sh down
scripts/local-rdb.sh clean
```

## 4. When it fails

- `lr status` first: which node is stopped, and its last console line.
- A node's own words: `out/node-N.err`, and the last `rdb_node not-ready` line in
  `out/node-N.out`.
- Its full log: `logs/rdb_node-N.jsonl` in the run folder. One JSON object per line, with `@t`
  (time), `@l` (level) and `@m` (message).

Query a copy, never a file a node is still writing. Warnings and errors from all three nodes:

```bash
vars="$(scripts/local-rdb.sh env)" && eval "$vars" &&
  mkdir -p "$RUN/logs-copy" && cp "$RUN"/logs/rdb_node-*.jsonl "$RUN/logs-copy/"
duckdb -c "SELECT json->>'@t' AS t, json->>'@l' AS level, json->>'@m' AS message, filename FROM read_ndjson_objects('$RUN/logs-copy/*.jsonl', filename=true) WHERE (json->>'@l') IN ('Warning','Error') ORDER BY t DESC LIMIT 20"
```

Warnings a healthy run shows, most often while the machine is busy (a cargo build, say):

- `db_send_unreachable`, a few, just after a node starts: it tried node `node` before their link
  was up. Every `up` shows some; the nodes still reach ready.
- `host_stall`: a node's timer fired late, by `late_ms`. The machine was slow to run the node.
  Many of them, with `late_ms` near a second, are normal under load.
- `authority_fence` with `reason=Expired scope=Node`, now and then: the node's permission to
  write ran out, most often while rEtcd elected a new leader. The node takes it again by itself;
  a write refused in between says so, with a retry hint. Retry it.

If your app still reads and writes, these are noise. Anything else at `Warning` or `Error` is
worth reading.

## 5. Known limits today

- **A restarted node rejoins.** `lr restart N` exits 0 once the node prints `rdb_node ready`.
  Measured in one walk on the dev host (`--base-port 18500`), with an app writing all the
  time:

  | Step | `lr` took | Writes refused for |
  |---|---|---|
  | `kill 3`, left down 60 s, then `start 3` | `start`: 1.6 s | 15.4 s, then writes resumed with node 3 still down |
  | `restart 2` | 12.0 s (stop, then start) | 8.6 s (older build; now about 0.2 s, below) |
  | `restart 1`, the owner | 5.2 s | 3.6 s |
  | `down`, then `up --resume` | 52 s | until `up --resume`; the data was all there |

  Every acknowledged write read back (0 missing). The `restart 2` time was measured before a
  restarted secondary came back as a standby. With all three nodes up, it now pauses writes
  about 0.2 s, or about 2 s twice if the node's data was wiped. The refusals the app saw, and
  what to do with each, are in embed.md, "When writes pause". A write sent to the owner as it stops answers
  `UNAVAILABLE (node 1, the owner, went out of reach while this call waited ...)`. With
  `no_mutation` false ("a write sent here may have applied"), resend that same request once it
  is back. With `no_mutation` true ("this call writes nothing"), nothing was applied: send it
  again as a new write. A read with the owner down is refused after 1 s
  (`Timeouts::read`): `UNAVAILABLE (node 1, the owner, is not reachable ...; nothing was sent)`.
- **`lr down` stops writes until `up --resume`.** An app open across it gets the `UNAVAILABLE`
  lines above for every write and read. The walk did not keep an app open across
  `up --resume`; restart the app after it to be sure.
- **A restart can move the generation** (4 to 5 after `restart 2`), so a resend of a request
  sent before it answers `GENERATION_CHANGED`. `did_it_land` handles that.
