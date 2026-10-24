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

- Binaries: `$CARGO_TARGET_DIR/debug` (default `./target/debug`). The first `up` builds them.
  That takes several minutes.
- Each `up` makes a new **run folder** under `C:/rdb_test_data/lr` (or `$RDB_LR_ROOT`, or
  `--root DIR`). Its name is `<commit>-<base port>-<time>`. `<root>/current` names the last one.
- In the run folder:
  - `db/nodes/1..3`: each node's copy.
  - `ctl/`: rEtcd's data and logs.
  - `certs/`: the run's TLS certificates. Apps `pilot`, `pilot-a`, `pilot-b` and `admin` have one.
  - `logs/rdb_node-N.jsonl`: node N's log. `out/node-N.out` and `.err`: its console.
  - `run.env`: the ports and binaries.
- Ports: rEtcd uses `P+1` to `P+24`. Node N listens on `P+40+N`. `P` is 19800 unless you pass
  `--base-port P`.

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
too. `--run DIR` picks another run.

| Command | What it does |
|---|---|
| `up --release` | The same, with release binaries from `.../release`. `start` and `restart` keep the run's build. |
| `status` | One line per node: `running pid ...` or `stopped`, and its last console line. Then rEtcd's table. |
| `env` | The run's ports and paths, as `KEY=value` lines. Your app reads them (section 3). |
| `stop N` | Stops node N cleanly. Prints `lr: node N stopped`. |
| `start N` | Starts node N and waits for `rdb_node ready`. Exit 1 with the node's reason if it is not ready. |
| `restart N` | `stop N`, then `start N`, with the same wait for ready. |
| `kill N` | Kills node N at once, like a crash. Prints `lr: node N killed (pid ...)`. |
| `promote N` | Makes node N the owner, through rEtcd. Done is `promoted: node N owns partition 1 at generation G ...`. `--drop M` names the node to leave out. |
| `down` | Stops the nodes and rEtcd. The run folder stays, so you can read its logs. |
| `clean` | `down`, then deletes the run folder, **its logs included**: copy what you need first. Prints `lr: removed <run folder>`. |

`scripts/local-rdb.sh` with no verb lists every verb, including the fault verbs.

## 3. Your app

Make a crate outside this repository, with the `Cargo.toml` from
[`embed.md`](embed.md#2-your-cargotoml) section 2. Its first build takes about 7 minutes.
Put this in `src/main.rs`:

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
    rt.block_on(net.wait_up(&nodes, Duration::from_secs(30)))?;

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
- Use one `ClientId` per app process. Two processes with one id can send the same request id.

Run it from your crate's folder in Git Bash, with `<checkout>` as in `embed.md`:

```bash
eval "$(<checkout>/scripts/local-rdb.sh env)"
cargo run -- "$CONTROL" "$PEERS" "$RUN"
```

It prints `read back hello at version N`. Run it again and `N` goes up.

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
eval "$(scripts/local-rdb.sh env)"
mkdir -p "$RUN/logs-copy" && cp "$RUN"/logs/rdb_node-*.jsonl "$RUN/logs-copy/"
duckdb -c "SELECT json->>'@t' AS t, json->>'@l' AS level, json->>'@m' AS message, filename FROM read_ndjson_objects('$RUN/logs-copy/*.jsonl', filename=true) WHERE (json->>'@l') IN ('Warning','Error') ORDER BY t DESC LIMIT 20"
```

## 5. Known limits today

- **A restarted owner does not rejoin by itself (#42).** A restarted secondary rejoins once the
  owner re-plans. A restarted owner stays out until the app opens its folder again.
- **After `promote`, the app must reopen.** Shut the `Db` down and open it again to write to the
  new owner. This lasts until W5's P1 fix lands.
