//! `rdb_node`: one rDB node in its own process (M10 S2, W4). `scripts/local-rdb.sh` starts
//! three of them; run it by hand only to walk one start.
//!
//! ```text
//! rdb_node --gen-certs --cert-dir C [--apps a,b] [--nodes K]
//!          [--control-cluster CID --control-voters 1=HOST,...]
//! rdb_node --form --dir D --control E [--control-tls T] [--log-dir L] [--partitions P]
//! rdb_node --declare --control E [--control-tls T] --peers 1=ADDR,...,K=ADDR
//!          [--machines N=LABEL,...] [--partitions P] [--nodes K] [--log-dir L]
//! rdb_node --node N --dir D --listen ADDR --peers 1=ADDR,2=ADDR,3=ADDR --control E
//!          [--control-tls T]
//!          --cert-dir C [--protocol-version V] [--faults F] [--stop-file S] [--log-dir L]
//!          [--ready-secs S] [--admin-apps a,b] [--partitions P]
//!          [--clock-offset-ms M] [--clock-step-file F] [--unreachable-drop-after-ms M]
//!          [--failover] [--failover-silent-ms M] [--failover-max-concurrent K]
//! every form: [--log-max-mb M] [--log-files N]
//! ```
//!
//! * `--log-dir` is where the JSONL log goes (default `.`), as `rdb_node-N.jsonl`. It rotates
//!   by size (#158): before a write would take it past `--log-max-mb` (default 256), it becomes
//!   `rdb_node-N.jsonl.1`, the older ones shift up, and at most `--log-files` (default 8, at
//!   least 2) files are kept, the live one included. `--log-max-mb 0` never rotates. A second
//!   process writing the same log is refused at start (exit 2). At info a node logs state
//!   changes and faults, not each write; `RUST_LOG` sets the filter.
//!
//! * `--gen-certs` writes a fresh development CA and certificates for nodes 1 to `--nodes`
//!   (default 3) and each app named by `--apps` into `--cert-dir`, then exits. `--cert-dir` pointing at another run's
//!   certificates is how a walk presents a certificate this run's nodes do not trust.
//!   With `--control-cluster CID` (32 hex digits) and `--control-voters 1=HOST,...` (both or
//!   neither) it also writes `C/control/`: the rEtcd client certificate every rDB process
//!   presents (`retcd://CID/client/rdb`), and one certificate per rEtcd voter, for its node id,
//!   `node-N.CID.retcd` and HOST, all signed by the same development CA (#80 S2,
//!   ADR-rdb-0024). Production brings its own files in that layout.
//! * `--control-tls T` dials rEtcd over mutual TLS with `T/ca.pem`, `T/client.pem` and
//!   `T/client.key`, the cluster id read from the client certificate's SAN; it prints
//!   `rdb_node control tls cluster=CID principal=P`. Without it the link is plaintext, and
//!   `--declare`, and a node process's start, are refused for a database whose `--peers` or
//!   `nodes/` records name any host that is not a loopback IP (#80 S2 ruling 1): one machine on
//!   127.0.0.N needs no certificates; anything else does.
//! * `--form` makes the database once (ruling A-R231): all three copies in this process on
//!   `--dir`, until ready, then shuts down. A node process opens only a formed or declared database.
//!   `--partitions` is how many partitions the database is formed with (issue #79), fixed from
//!   then on. Unset, a new database has one and a node process reads the count from the store.
//! * `--declare` makes the database with no copy at all (#80 S1a, ADR-rdb-0022): it writes the
//!   control records and, last, the formation record naming the database's instance, then exits.
//!   Each node process then opens its own empty `--dir`, and the owner of each partition forms
//!   it. `--partitions` defaults to 1. `--peers` is required: nodes 1 to K, K from 3 to 8, each
//!   with the address its node process will `--listen` on, as the node processes take
//!   `--peers` (#80 S1b, ADR-rdb-0023). It writes one `nodes/N` record per node. `--machines`
//!   gives a node's machine label; unset, it is the host as `parse_addr` parses it (so
//!   `DB-A.example.` gives `db-a.example`, and `[0:0:0:0:0:0:0:1]` gives `::1`). Two nodes on one
//!   machine are refused: the same label, or the same host whatever the labels say. So 127.0.0.1
//!   and 127.0.0.2 are two machines, and a whole database runs on one box over loopback addresses.
//!   `--nodes`, if given, must count the `--peers` nodes. It prints the records it reads back,
//!   `nodes=N@ADDR/MACHINE,...`. Run again with the same arguments it says `already`; with
//!   another count or another address it is refused. It takes no `--form`, `--gen-certs`,
//!   `--node`, `--dir` or `--live-readd`: each of these five is refused, and so are `--failover`
//!   and its knobs. `--listen` and the other node-process flags still parse and are ignored.
//! * Otherwise the process runs node `N` of the database in `--dir`, holding only `nodes/N`.
//!   `--peers` names every other node; with this one they are nodes 1 to K, K at least 3 (M11
//!   WS A). On a database `--form` made, nodes past 3 are spares: they hold nothing until
//!   `admin move` pins them. On a declared one, partitions are placed round the ring of all K
//!   nodes, and before it writes its boot or binds, the process checks `--listen` and every
//!   `--peers` entry against the `nodes/` records and is refused, naming the record, when one
//!   disagrees; a store an S1a build declared (no `nodes/` records) is refused too (#80 S1b).
//!   It serves its peers and apps on `--listen` and dials each lower-numbered node. Its boot is
//!   `next_boot(--dir)`, the same value in the handshake and in the `Db` (ruling A-R227).
//!   `--protocol-version` makes it speak another wire version, to walk the refusal.
//!   `--faults` names the fault file it polls every 50 ms (`hold <n>|hold all|drop-request N|
//!   drop-reply N|cut-control|mute`). It stops cleanly when `--stop-file` appears.
//!   `--admin-apps` names the apps whose certificate may send `admin move` and `admin rebuild`
//!   (ruling A-R479). Unset or empty, no app may (ruling A-R480), as in the library. `lr`
//!   passes `--admin-apps admin`, the app `lr` and `rdb_dev` run as; a production node lists
//!   its operator apps. Any other app's move or rebuild is refused before the node sees it.
//!   Names are trimmed, so `ops, admin` lists `ops` and `admin`.
//!   `--unreachable-drop-after-ms` is how long this node, as an owner, waits with no link to a
//!   copy before it drops that copy (default 10000, least 2000; `Timeouts::unreachable_drop_after`).
//!   `--live-readd` lets this node, as an owner, add back a copy its own drop clock left out
//!   once that copy's link is back, with no restart of the copy (`Timeouts::live_readd`, #107).
//!   Off unless given.
//!   `--app-addr A` writes A, the address this node's app takes forwarded requests on, into the
//!   node's `nodes/{id}` record before the node starts, so `Db::route` names it (#83 B2). Only
//!   a declared database has the record: a node of a database made by `--form` is refused it at
//!   open. `--form`, `--declare` and `--gen-certs` each refuse the flag, since none of them starts
//!   a node that would write it. Unset, the record keeps what an earlier start wrote; an address
//!   cannot be cleared. Unset is refused while the folder's `app-addr-pending` file says an
//!   earlier start's write may still apply: start again with `--app-addr` and the address the
//!   node should hold, which settles it. On a new or emptied `--dir`, an unset start fences the
//!   record (it writes the record's own bytes back), so no late write replaces its address.
//!   `--clock-offset-ms` shifts this process's `Tick` once at start, real skew (#80 S2c).
//!   `--clock-step-file` names a file it polls every 20 ms: its contents, whole milliseconds,
//!   step `Tick` back once, then the file is removed. `kill -STOP`, wait, write the file,
//!   `kill -CONT` is a frozen-clock pause.
//!   `--failover` runs the automatic-failover planner in this process (#81 C1,
//!   `DbConfig::failover`): while it holds `planner/grant` it promotes each partition of a node
//!   whose link has echoed no clock probe for `--failover-silent-ms` (default 2000, least
//!   1000), that answers no look, and whose grant has run out; or of a node whose grant is
//!   frozen. At most `--failover-max-concurrent` (default 16, least 1) promotes run at once.
//!   Off by default. Either knob without `--failover` is refused, and so is `--failover` with
//!   `--form` or `--gen-certs`: neither runs a planner.
//!
//! Stdout, one line per step, for a script to wait on:
//! `rdb_node listening node=N addr=ADDR boot=B`, then `rdb_node open node=N`, then
//! `rdb_node ready node=N` or `rdb_node not-ready node=N: <why>`, then `rdb_node stopped node=N`.
//! While it runs, `rdb_node ownership node=N partition=P token=G.E` when this node starts leading
//! a partition, and `rdb_node ownership-lapsed node=N partition=P token=G.E` when
//! `Db::still_owner` turns false for that token (#84; logged `ownership_held`, `ownership_lapsed`).
//! And each event of `Db::ownership_events` (#83), as it arrives:
//! `rdb_node ownership-event node=N partition=P state=activated token=G.E`, `state=deactivating`
//! with `deadline_in_ms=D` (time left until writes stop), or `state=deactivated`. The host logs
//! each as `ownership_event` with its clock tick; a dropped subscriber subscribes again
//! (`rdb_node_ownership_resubscribed`).
//! V10 (#81): about once a second the node reads its own `NodeStatus`. When its self-replace
//! has stayed stopped for 5 s (its grant was frozen by a promote), or its host faulted, and no
//! `partitions/` record in the control store names it owner in any lifecycle, it prints
//! `rdb_node restart-wanted node=N reason=R` (logged `rdb_node_restart_wanted`), shuts down,
//! prints `rdb_node stopped node=N` and exits 75: its supervisor should start it again, at a new
//! boot, and the owner re-adds it. A failed list keeps it up (`db_restart_check_unread`). The
//! rule is `Db::restart_wanted`'s, which an embedder polls the same way.
//! Errors go to stderr with the step that failed. Exit codes: 0 stopped or formed, 1 a start
//! step failed, 2 bad arguments, 75 restart wanted (V10).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use config_core::ConfigStore;
use rdb_api::db::ReadyError;
use rdb_api::db::{next_boot, ALL_NODES, DEFAULT_CLIENT, MAX_NODES};
use rdb_api::net::faults::{CutStore, Faults};
use rdb_api::net::wire::Label;
use rdb_api::net::{certs, Net, NetConfig, PROTOCOL_VERSION};
use rdb_api::transport::Links;
use rdb_api::{
    Db, DbConfig, NodeId, OwnerToken, Ownership, OwnershipEvent, PartitionId, RestartReason,
    Timeouts,
};

#[derive(Debug, Default)]
struct Args {
    gen_certs: bool,
    form: bool,
    declare: bool,
    apps: Vec<String>,
    node: Option<u32>,
    dir: Option<PathBuf>,
    listen: Option<String>,
    /// #83 B2: `--app-addr`, the address this node's app takes forwarded requests on. Unset is
    /// refused while an earlier start's write may still apply (`app-addr-pending`).
    app_addr: Option<String>,
    peers: BTreeMap<NodeId, String>,
    control: Vec<String>,
    cert_dir: Option<PathBuf>,
    protocol_version: Option<u32>,
    faults: Option<PathBuf>,
    stop_file: Option<PathBuf>,
    log_dir: Option<PathBuf>,
    /// `--log-max-mb`; `None` is `SizeRotation::DEFAULT`'s, 0 never rotates (#158).
    log_max_mb: Option<u64>,
    /// `--log-files`; `None` is `SizeRotation::DEFAULT`'s.
    log_files: Option<usize>,
    ready_secs: Option<u64>,
    /// `--gen-certs`: certificates for nodes 1 to this (M11 WS A); `None` is 3.
    nodes: Option<u32>,
    /// `--admin-apps`: the apps that may move or rebuild a copy (A-R479); `None` is none
    /// (A-R480).
    admin_apps: Option<BTreeSet<String>>,
    /// `--partitions`: the partition count (issue #79); `None` reads it from the control store.
    partitions: Option<u32>,
    clock_offset_ms: i64,
    clock_step_file: Option<PathBuf>,
    /// `--unreachable-drop-after-ms`; `None` is `Timeouts::default()`'s.
    unreachable_drop_after_ms: Option<u64>,
    /// `--failover`, with its two knobs: `DbConfig::failover` (#81 C1).
    failover: bool,
    failover_silent_ms: Option<u64>,
    failover_max_concurrent: Option<usize>,
    /// `--live-readd` (#107).
    live_readd: bool,
    /// `--machines N=label,...` (#80 S1b): `--declare`'s machine label per node; a node left
    /// out takes its `--peers` address's host as `parse_addr` parses it.
    machines: BTreeMap<NodeId, String>,
    /// `--control-tls DIR` (#80 S2): mutual TLS to rEtcd from `certs::load_control(DIR)`.
    control_tls: Option<PathBuf>,
    /// `--control-cluster CID` (#80 S2): `--gen-certs` writes the rEtcd material for it.
    control_cluster: Option<config_core::ClusterId>,
    /// `--control-voters 1=HOST,...` (#80 S2): the rEtcd voters `--gen-certs` certifies.
    control_voters: Vec<(u64, String)>,
}

/// The apps this node lets move or rebuild a copy: `--admin-apps` as given, or none when the
/// flag is absent, as in the library (A-R479, A-R480).
fn admin_apps(args: &Args) -> BTreeSet<String> {
    args.admin_apps.clone().unwrap_or_default()
}

const USAGE: &str = "usage:
  rdb_node --gen-certs --cert-dir C [--apps a,b] [--nodes K]
           [--control-cluster CID --control-voters 1=HOST,...]
  rdb_node --form --dir D --control E [--control-tls T] [--log-dir L] [--partitions P]
  rdb_node --declare --control E [--control-tls T] --peers 1=ADDR,...,K=ADDR
           [--machines N=LABEL,...] [--partitions P] [--nodes K] [--log-dir L]
  rdb_node --node N --dir D --listen ADDR --peers 1=ADDR,2=ADDR,3=ADDR --control E --cert-dir C
           [--control-tls T]
           [--protocol-version V] [--faults F] [--stop-file S] [--log-dir L] [--ready-secs S]
           [--admin-apps a,b] [--partitions P]
           [--clock-offset-ms M] [--clock-step-file F] [--unreachable-drop-after-ms M]
           [--failover] [--failover-silent-ms M] [--failover-max-concurrent K]
           [--live-readd] [--app-addr A]
  every form: [--log-max-mb M] [--log-files N] (default 256 MiB x 8 files; M = 0 never rotates)
  --faults F: a file of lines, each one of
           hold N | hold all | drop-request N | drop-reply N | cut-control | mute
";

fn parse(mut argv: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut args = Args::default();
    while let Some(flag) = argv.next() {
        let mut value = |name: &str| argv.next().ok_or_else(|| format!("{name} needs a value"));
        match flag.as_str() {
            "--gen-certs" => args.gen_certs = true,
            "--form" => args.form = true,
            "--declare" => args.declare = true,
            "--apps" => {
                args.apps = value("--apps")?
                    .split(',')
                    .filter(|a| !a.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
            "--admin-apps" => {
                args.admin_apps = Some(
                    value("--admin-apps")?
                        .split(',')
                        .map(str::trim)
                        .filter(|a| !a.is_empty())
                        .map(str::to_owned)
                        .collect(),
                );
            }
            "--node" => {
                let n: u32 = value("--node")?
                    .parse()
                    .map_err(|_| format!("--node takes a node number, 1 to {MAX_NODES}"))?;
                if !(1..=MAX_NODES).contains(&n) {
                    return Err(format!("--node {n}: a node number is 1 to {MAX_NODES}"));
                }
                args.node = Some(n);
            }
            "--partitions" => {
                args.partitions = Some(
                    value("--partitions")?
                        .parse()
                        .map_err(|_| "--partitions takes a partition count".to_owned())?,
                );
            }
            "--dir" => args.dir = Some(value("--dir")?.into()),
            "--listen" => args.listen = Some(value("--listen")?),
            "--app-addr" => args.app_addr = Some(value("--app-addr")?),
            "--peers" => {
                for pair in value("--peers")?.split(',') {
                    let (n, addr) = pair
                        .split_once('=')
                        .ok_or_else(|| format!("--peers: {pair:?} is not N=host:port"))?;
                    let n: u32 = n
                        .parse()
                        .map_err(|_| format!("--peers: {n:?} is not a node number"))?;
                    if let Some(first) = args.peers.insert(NodeId(n), addr.to_owned()) {
                        return Err(format!(
                            "--peers names node {n} twice, as {first} and {addr}; name each \
                             node once"
                        ));
                    }
                }
            }
            "--control" => {
                args.control = value("--control")?
                    .split(',')
                    .filter(|e| !e.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
            "--cert-dir" => args.cert_dir = Some(value("--cert-dir")?.into()),
            "--control-tls" => args.control_tls = Some(value("--control-tls")?.into()),
            "--control-cluster" => {
                let text = value("--control-cluster")?;
                args.control_cluster = Some(text.parse().map_err(|_| {
                    format!("--control-cluster {text:?} is not a cluster id: 32 hex digits")
                })?);
            }
            "--control-voters" => {
                for pair in value("--control-voters")?.split(',') {
                    let (n, host) = pair
                        .split_once('=')
                        .filter(|(_, host)| !host.is_empty())
                        .ok_or_else(|| format!("--control-voters: {pair:?} is not N=HOST"))?;
                    let n: u64 = n
                        .parse()
                        .map_err(|_| format!("--control-voters: {n:?} is not a voter id"))?;
                    if args.control_voters.iter().any(|(m, _)| *m == n) {
                        return Err(format!("--control-voters names voter {n} twice"));
                    }
                    args.control_voters.push((n, host.to_owned()));
                }
            }
            "--protocol-version" => {
                args.protocol_version = Some(
                    value("--protocol-version")?
                        .parse()
                        .map_err(|_| "--protocol-version takes a number".to_owned())?,
                );
            }
            "--faults" => args.faults = Some(value("--faults")?.into()),
            "--stop-file" => args.stop_file = Some(value("--stop-file")?.into()),
            "--log-dir" => args.log_dir = Some(value("--log-dir")?.into()),
            "--log-max-mb" => {
                args.log_max_mb = Some(
                    value("--log-max-mb")?
                        .parse()
                        .map_err(|_| "--log-max-mb takes whole megabytes".to_owned())?,
                );
            }
            "--log-files" => {
                args.log_files = Some(
                    value("--log-files")?
                        .parse()
                        .map_err(|_| "--log-files takes a file count".to_owned())?,
                );
            }
            "--nodes" => {
                let k: u32 = value("--nodes")?
                    .parse()
                    .map_err(|_| format!("--nodes takes a count, 3 to {MAX_NODES}"))?;
                if !(3..=MAX_NODES).contains(&k) {
                    return Err(format!(
                        "--nodes {k}: a database has 3 to {MAX_NODES} nodes"
                    ));
                }
                args.nodes = Some(k);
            }
            "--ready-secs" => {
                args.ready_secs = Some(
                    value("--ready-secs")?
                        .parse()
                        .map_err(|_| "--ready-secs takes whole seconds".to_owned())?,
                );
            }
            "--clock-offset-ms" => {
                args.clock_offset_ms = value("--clock-offset-ms")?
                    .parse()
                    .map_err(|_| "--clock-offset-ms takes whole milliseconds".to_owned())?;
            }
            "--clock-step-file" => args.clock_step_file = Some(value("--clock-step-file")?.into()),
            "--unreachable-drop-after-ms" => {
                args.unreachable_drop_after_ms =
                    Some(value("--unreachable-drop-after-ms")?.parse().map_err(|_| {
                        "--unreachable-drop-after-ms takes whole milliseconds".to_owned()
                    })?);
            }
            "--failover" => args.failover = true,
            "--failover-silent-ms" => {
                let millis: u64 = value("--failover-silent-ms")?
                    .parse()
                    .map_err(|_| "--failover-silent-ms wants whole milliseconds".to_owned())?;
                if millis < FAILOVER_SILENT_MIN_MS {
                    return Err(format!(
                        "--failover-silent-ms {millis} is under one clock-probe period \
                         ({FAILOVER_SILENT_MIN_MS} ms): a healthy link would read as silent"
                    ));
                }
                args.failover_silent_ms = Some(millis);
            }
            "--failover-max-concurrent" => {
                let count: usize = value("--failover-max-concurrent")?.parse().map_err(|_| {
                    "--failover-max-concurrent wants a count of at least 1".to_owned()
                })?;
                if count == 0 {
                    return Err("--failover-max-concurrent wants a count of at least 1".to_owned());
                }
                args.failover_max_concurrent = Some(count);
            }
            "--live-readd" => args.live_readd = true,
            "--machines" => {
                for pair in value("--machines")?.split(',') {
                    let (n, label) = pair
                        .split_once('=')
                        .ok_or_else(|| format!("--machines: {pair:?} is not N=label"))?;
                    let n: u32 = n
                        .parse()
                        .map_err(|_| format!("--machines: {n:?} is not a node number"))?;
                    if let Some(first) = args.machines.insert(NodeId(n), label.to_owned()) {
                        return Err(format!(
                            "--machines names node {n} twice, as {first} and {label}; name each \
                             node once"
                        ));
                    }
                }
            }
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    failover_refusal(&args)?;
    declare_refusal(&args)?;
    // #83 B2, PR #159 review: only a node process writes `--app-addr`. Forming opens with no
    // addresses, declaring writes control records only, and making certificates opens nothing,
    // so each would drop the flag without a word.
    if args.app_addr.is_some() {
        for (given, mode) in [
            (args.form, "--form"),
            (args.declare, "--declare"),
            (args.gen_certs, "--gen-certs"),
        ] {
            if given {
                return Err(format!(
                    "--app-addr is written by a node process of a declared database, not with \
                     {mode}"
                ));
            }
        }
    }
    control_certs_refusal(&args)?;
    Ok(args)
}

/// `--control-cluster` and `--control-voters` (#80 S2) are read by `--gen-certs` only, and
/// only together: one names the rEtcd cluster, the other the voters to certify for it.
fn control_certs_refusal(args: &Args) -> Result<(), String> {
    let cluster = args.control_cluster.is_some();
    let voters = !args.control_voters.is_empty();
    if (cluster || voters) && !args.gen_certs {
        return Err("--control-cluster and --control-voters are read only by --gen-certs".into());
    }
    if cluster != voters {
        return Err(
            "--control-cluster and --control-voters go together: the rEtcd cluster, and its \
             voters as 1=HOST,..."
                .into(),
        );
    }
    Ok(())
}

/// Five mode and copy flags `--declare` refuses (PR #138 review): it writes control records
/// only, so it makes no copy (`--form`), certificates (`--gen-certs`) or node process (`--node`,
/// `--dir`, `--live-readd`). Not every flag it does not read: `--failover` and its knobs are
/// refused by `failover_refusal`, while `--listen` and the other node-process flags still parse
/// and are ignored. #80 S1b: it needs `--peers`, which it records; `--nodes`, when given, must
/// count the same nodes; `--machines` may name only nodes `--peers` names, and is refused
/// without `--declare`.
fn declare_refusal(args: &Args) -> Result<(), String> {
    if !args.declare {
        if !args.machines.is_empty() {
            return Err("--machines is read only by --declare".to_owned());
        }
        return Ok(());
    }
    for (given, flag) in [
        (args.form, "--form"),
        (args.gen_certs, "--gen-certs"),
        (args.node.is_some(), "--node"),
        (args.dir.is_some(), "--dir"),
        (args.live_readd, "--live-readd"),
    ] {
        if given {
            return Err(format!(
                "--declare writes only control records and takes no {flag}"
            ));
        }
    }
    // #80 S1b: the declaration records each node's address, from `--peers` as the node
    // processes take it.
    if args.peers.is_empty() {
        return Err(
            "--declare needs --peers 1=ADDR,...,K=ADDR: each node's address, as its node \
             process takes it"
                .to_owned(),
        );
    }
    let k = args.peers.len();
    if let Some(nodes) = args.nodes.filter(|nodes| usize::try_from(*nodes) != Ok(k)) {
        return Err(format!(
            "--nodes {nodes} disagrees with --peers, which names {k} nodes"
        ));
    }
    if let Some(stray) = args.machines.keys().find(|n| !args.peers.contains_key(n)) {
        return Err(format!(
            "--machines names node {}, which --peers does not",
            stray.0
        ));
    }
    Ok(())
}

/// The shortest `--failover-silent-ms` taken: one clock-probe period, the same floor
/// `Db::open_with` holds `Failover::silent_after` to (`net::PROBE_PERIOD`, 1 s). The CLI
/// refuses below it while parsing, before this process writes its boot or listens (PR #119
/// review); `Db::open_with` still refuses it for an embedder.
const FAILOVER_SILENT_MIN_MS: u64 = 1_000;

/// `--failover` and its knobs where nothing would read them, refused rather than ignored (PR
/// #119 review): `--form` and `--gen-certs` run no planner, and a knob does nothing without
/// `--failover`.
fn failover_refusal(args: &Args) -> Result<(), String> {
    if args.failover {
        for (given, mode) in [
            (args.form, "--form"),
            (args.declare, "--declare"),
            (args.gen_certs, "--gen-certs"),
        ] {
            if given {
                return Err(format!(
                    "--failover runs only in a node process, not with {mode}"
                ));
            }
        }
        return Ok(());
    }
    for (given, knob) in [
        (args.failover_silent_ms.is_some(), "--failover-silent-ms"),
        (
            args.failover_max_concurrent.is_some(),
            "--failover-max-concurrent",
        ),
    ] {
        if given {
            return Err(format!("{knob} needs --failover"));
        }
    }
    Ok(())
}

/// Poll `file`: whole milliseconds in it step this process's `Tick` back once. A file that
/// does not parse is said out loud and removed, not read as zero.
fn watch_clock_step(file: PathBuf, links: Arc<Links>) {
    std::thread::spawn(move || loop {
        if let Ok(text) = std::fs::read_to_string(&file) {
            match text.trim().parse::<u64>() {
                Ok(millis) => links.clock().step_back(millis),
                Err(e) => {
                    tracing::error!(file = %file.display(), error = %e, "clock_step_file_bad")
                }
            }
            if let Err(e) = std::fs::remove_file(&file) {
                tracing::error!(file = %file.display(), error = %e, "clock_step_file_stuck");
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    });
}

/// `DbConfig::failover` from `--failover` and its knobs; `None` without `--failover`.
fn failover(args: &Args) -> Option<rdb_api::Failover> {
    let default = rdb_api::Failover::default();
    args.failover.then(|| rdb_api::Failover {
        silent_after: args
            .failover_silent_ms
            .map_or(default.silent_after, Duration::from_millis),
        max_concurrent: args
            .failover_max_concurrent
            .unwrap_or(default.max_concurrent),
        ..default
    })
}

fn need<T: Clone>(value: Option<&T>, name: &str) -> Result<T, String> {
    value.cloned().ok_or_else(|| format!("{name} is required"))
}

fn main() -> ExitCode {
    run(
        std::env::args().skip(1),
        |config| {
            config_log::init(config)
                .map(Some)
                .map_err(|e| e.to_string())
        },
        serve,
    )
}

/// `main`, with how logging starts and how a node process serves passed in: a row runs this
/// same path under the subscriber its `#[config_log::retcd_test]` installed, where a second
/// `config_log::init` is refused, and over its own store in place of [`serve`]'s `--control`.
fn run(
    argv: impl Iterator<Item = String>,
    start_logging: impl FnOnce(config_log::LogConfig) -> Result<Option<config_log::LogGuard>, String>,
    serve: impl FnOnce(&Args, &tokio::runtime::Runtime) -> Result<Served, String>,
) -> ExitCode {
    let args = match parse(argv) {
        Ok(args) => args,
        Err(message) => {
            if !message.is_empty() {
                eprintln!("rdb_node: {message}");
            }
            eprint!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    if args.gen_certs {
        return gen_certs(&args);
    }
    let name = match args.node {
        Some(n) => format!("rdb_node-{n}"),
        None if args.declare => "rdb_node-declare".to_owned(),
        None => "rdb_node-form".to_owned(),
    };
    let log_dir = args.log_dir.clone().unwrap_or_else(|| PathBuf::from("."));
    let mut config = config_log::LogConfig::for_application("rdb_node");
    config.dir.clone_from(&log_dir);
    config.file_name = format!("{name}.jsonl");
    config.rotate = match config_log::SizeRotation::from_flags(args.log_max_mb, args.log_files) {
        Ok(rotate) => rotate,
        Err(e) => {
            eprintln!("rdb_node: {e}");
            return ExitCode::from(2);
        }
    };
    let guard = match start_logging(config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("rdb_node: log init: {e}");
            return ExitCode::from(2);
        }
    };
    eprintln!("log={}", log_dir.join(format!("{name}.jsonl")).display());
    let code = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => {
            let result = if args.form {
                form(&args, &rt).map(|()| Served::Stopped)
            } else if args.declare {
                declare(&args, &rt).map(|()| Served::Stopped)
            } else {
                serve(&args, &rt)
            };
            exit_code(result)
        }
        Err(e) => {
            eprintln!("rdb_node: runtime: {e}");
            ExitCode::from(1)
        }
    };
    drop(guard);
    code
}

/// The exit code for how a run ended: 0 at the stop file, [`RESTART_EXIT`] when it asks its
/// supervisor for a restart (V10, #81), 1 on a failure, which is logged and printed.
fn exit_code(result: Result<Served, String>) -> ExitCode {
    match result {
        Ok(Served::Stopped) => ExitCode::SUCCESS,
        Ok(Served::Restart(_)) => ExitCode::from(RESTART_EXIT),
        Err(message) => {
            tracing::error!(%message, "rdb_node_failed");
            eprintln!("rdb_node: {message}");
            ExitCode::from(1)
        }
    }
}

fn gen_certs(args: &Args) -> ExitCode {
    let Some(dir) = &args.cert_dir else {
        eprintln!("rdb_node: --gen-certs needs --cert-dir");
        return ExitCode::from(2);
    };
    let labels: Vec<Label> = (1..=args.nodes.unwrap_or(3))
        .map(|n| Label::Node(NodeId(n)))
        .chain(args.apps.iter().cloned().map(Label::App))
        .collect();
    let generated = certs::generate(dir, &labels).and_then(|()| match args.control_cluster {
        Some(cluster) => certs::generate_control(dir, cluster, &args.control_voters),
        None => Ok(()),
    });
    match generated {
        Ok(()) => {
            let names: Vec<String> = labels.iter().map(ToString::to_string).collect();
            println!(
                "rdb_node certs dir={} labels={}",
                dir.display(),
                names.join(",")
            );
            if let Some(cluster) = args.control_cluster {
                let voters: Vec<String> = args
                    .control_voters
                    .iter()
                    .map(|(n, host)| format!("{n}={host}"))
                    .collect();
                println!(
                    "rdb_node control certs dir={} cluster={cluster} client={} voters={}",
                    dir.join(certs::CONTROL_DIR).display(),
                    certs::CONTROL_CLIENT,
                    voters.join(",")
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rdb_node: --gen-certs: {e}");
            ExitCode::from(1)
        }
    }
}

fn control(args: &Args) -> Result<Arc<dyn ConfigStore>, String> {
    if args.control.is_empty() {
        return Err("--control is required: the rEtcd client endpoints (lr up starts one)".into());
    }
    let Some(dir) = &args.control_tls else {
        return config_client::GrpcClient::connect(
            args.control.clone(),
            config_client::GrpcClientOptions::default(),
        )
        .map(|client| Arc::new(client) as Arc<dyn ConfigStore>)
        .map_err(|e| format!("--control {}: {e}", args.control.join(",")));
    };
    // #80 S2 (ADR-rdb-0024): mutual TLS, and the cluster id from the client certificate, so
    // a leader hint is followed with the hinted voter's identity checked.
    let tls = certs::load_control(dir).map_err(|e| format!("--control-tls: {e}"))?;
    let client = config_client::GrpcClient::connect(
        args.control.clone(),
        config_client::GrpcClientOptions {
            tls: config_client::TlsMode::MutualTls(tls.mtls),
            ..config_client::GrpcClientOptions::default()
        },
    )
    .map_err(|e| format!("--control {}: {e}", args.control.join(",")))?
    .with_cluster_id(tls.cluster);
    tracing::info!(
        cluster = %tls.cluster,
        principal = %tls.principal,
        endpoints = ?args.control,
        "control_tls"
    );
    println!(
        "rdb_node control tls cluster={} principal={}",
        tls.cluster, tls.principal
    );
    Ok(Arc::new(client))
}

/// Make the database once: all three copies here, until ready, then shut down (A-R231).
fn form(args: &Args, rt: &tokio::runtime::Runtime) -> Result<(), String> {
    let dir = need(args.dir.as_ref(), "--dir")?;
    let store = control(args)?;
    let config = DbConfig {
        failover: None,
        partitions: args.partitions,
        ack_floor: true,
        dir: dir.clone(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        history_keep: None,
        history_budget_bytes: None,
        local: ALL_NODES.to_vec(),
        links: None,
        client: DEFAULT_CLIENT,
        boot: None,
        storage: rdb_api::StorageOptions::default(),
        addresses: None,
    };
    let mut db = rt
        .block_on(Db::open(config, store, rt.handle().clone()))
        .map_err(|e| format!("form: open {}: {e}", dir.display()))?;
    let within = Duration::from_secs(args.ready_secs.unwrap_or(60));
    let ready = db.wait_ready(within);
    db.shutdown();
    ready.map_err(|e| format!("form: not ready within {within:?}: {e}"))?;
    tracing::info!(dir = %dir.display(), "rdb_node_formed");
    println!("rdb_node formed dir={}", dir.display());
    Ok(())
}

/// Declare the database (#80 S1a): control records only, no copy, then exit.
fn declare(args: &Args, rt: &tokio::runtime::Runtime) -> Result<(), String> {
    let store = control(args)?;
    let line = declare_on(args, &store, rt)?;
    println!("{line}");
    Ok(())
}

/// [`declare`] on `store`, answering the line it prints: each `--peers` node with its address
/// and `--machines` label, and `--partitions`, go to `admin::declare` as given, so a row can
/// run this path on its own store. The line's `nodes=` lists the records read back from the
/// store, `N@addr/machine` each (#80 S1b).
fn declare_on(
    args: &Args,
    store: &Arc<dyn ConfigStore>,
    rt: &tokio::runtime::Runtime,
) -> Result<String, String> {
    let declaration = rdb_api::admin::Declaration {
        nodes: args
            .peers
            .iter()
            .map(|(node, addr)| rdb_api::admin::NodeDecl {
                node: *node,
                addr: addr.clone(),
                machine: args.machines.get(node).cloned(),
            })
            .collect(),
        partitions: args.partitions.unwrap_or(1),
    };
    let (word, formation) = match rt.block_on(rdb_api::admin::declare(store, &declaration)) {
        Ok(rdb_api::admin::Declared::Created(formation)) => ("declared", formation),
        Ok(rdb_api::admin::Declared::Already(formation)) => ("already", formation),
        Err(e) => return Err(format!("declare: {e}")),
    };
    let records = rt
        .block_on(rdb_api::admin::read_nodes(&**store))
        .map_err(|e| format!("declare: read back the node records: {e}"))?;
    let instance = rdb_api::admin::instance_hex(&formation.instance);
    let nodes: Vec<String> = records
        .values()
        .map(|record| format!("{}@{}/{}", record.node.0, record.addr, record.machine))
        .collect();
    tracing::info!(%instance, partitions = formation.partitions, word, "rdb_node_declared");
    Ok(format!(
        "rdb_node {word} instance={instance} partitions={} nodes={}",
        formation.partitions,
        nodes.join(",")
    ))
}

/// The node-start check (#80 S1b rulings 2 and 4, ADR-rdb-0023 Decision 5): on a declared
/// database, `--listen` and every `--peers` entry must be what the `nodes/` records say, and an
/// S1a store (no records) is refused. Run before this process writes its boot or binds, so a
/// refused start leaves nothing behind. An undeclared database passes.
fn start_check_on(
    args: &Args,
    node: NodeId,
    listen: &str,
    store: &dyn ConfigStore,
    rt: &tokio::runtime::Runtime,
) -> Result<(), String> {
    let formation = rt
        .block_on(rdb_api::admin::read_formation(store))
        .map_err(|e| format!("start check: {e}"))?;
    let records = match &formation {
        Some(_) => rt
            .block_on(rdb_api::admin::read_nodes(store))
            .map_err(|e| format!("start check: {e}"))?,
        None => BTreeMap::new(),
    };
    // #80 S2 ruling 1 (ADR-rdb-0024): before the boot is written or anything is bound, as the
    // open would refuse it after.
    let plaintext = formation.as_ref().and_then(|_| {
        rdb_api::admin::control_tls_refusal(
            records
                .values()
                .map(|record| (record.node, record.addr.as_str())),
            store.capabilities().transport_security,
        )
        .map(|why| format!("{why}; nothing was started"))
    });
    match plaintext.or_else(|| {
        rdb_api::admin::node_start_refusal(node, listen, &args.peers, formation.as_ref(), &records)
    }) {
        Some(refused) => {
            tracing::warn!(node = node.0, %refused, "rdb_node_start_refused");
            Err(format!("start check: {refused}"))
        }
        None => Ok(()),
    }
}

/// How often [`watch_ownership`] reads each partition's ownership.
const OWNERSHIP_POLL: Duration = Duration::from_millis(10);

/// One change [`poll_ownership`] saw for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnershipSeen {
    /// This node started leading it, or leads it again after a lapse.
    Held(Ownership),
    /// `still_owner` turned false for the token last held.
    Lapsed(PartitionId, OwnerToken),
}

/// One poll of partitions `1..=partitions`: a held token whose `still_owner` reads false is
/// reported lapsed and forgotten, then a partition not held whose `ownership` answers is
/// reported held. `held` carries the tokens from poll to poll.
fn poll_ownership(
    held: &mut BTreeMap<PartitionId, OwnerToken>,
    partitions: u32,
    ownership: impl Fn(PartitionId) -> Option<Ownership>,
    still_owner: impl Fn(PartitionId, OwnerToken) -> bool,
) -> Vec<OwnershipSeen> {
    let mut seen = Vec::new();
    for partition in (1..=partitions).map(PartitionId) {
        if let Some(token) = held.get(&partition).copied() {
            if still_owner(partition, token) {
                continue;
            }
            held.remove(&partition);
            seen.push(OwnershipSeen::Lapsed(partition, token));
        }
        if let Some(owned) = ownership(partition) {
            held.insert(partition, owned.token);
            seen.push(OwnershipSeen::Held(owned));
        }
    }
    seen
}

/// Poll every [`OWNERSHIP_POLL`] until `done`, handing each change to `report`.
fn watch_ownership_with(
    done: &AtomicBool,
    partitions: impl Fn() -> u32,
    ownership: impl Fn(PartitionId) -> Option<Ownership>,
    still_owner: impl Fn(PartitionId, OwnerToken) -> bool,
    mut report: impl FnMut(OwnershipSeen),
) {
    let mut held: BTreeMap<PartitionId, OwnerToken> = BTreeMap::new();
    while !done.load(Ordering::Relaxed) {
        for seen in poll_ownership(&mut held, partitions(), &ownership, &still_owner) {
            report(seen);
        }
        std::thread::sleep(OWNERSHIP_POLL);
    }
}

/// #84's manual entry point: log each partition this node starts leading, `ownership_held`
/// with its owner token, and the first poll at which `Db::still_owner` is false for that token,
/// `ownership_lapsed`. Polled every [`OWNERSHIP_POLL`], so a lapse line is up to that much after
/// the tick write admission stopped; the lapse itself is never later than admission's.
fn watch_ownership(db: &Db, node: NodeId, done: &AtomicBool) {
    watch_ownership_with(
        done,
        || db.partition_count(),
        |partition| db.ownership(partition),
        |partition, token| db.still_owner(partition, token),
        |seen| match seen {
            OwnershipSeen::Lapsed(partition, token) => {
                tracing::warn!(
                    node = node.0,
                    partition = partition.0,
                    generation = token.generation.0,
                    owner_epoch = token.owner_epoch.0,
                    token = %token,
                    "ownership_lapsed"
                );
                println!(
                    "rdb_node ownership-lapsed node={} partition={} token={token}",
                    node.0, partition.0
                );
            }
            OwnershipSeen::Held(owned) => {
                tracing::info!(
                    node = node.0,
                    partition = owned.partition.0,
                    generation = owned.token.generation.0,
                    owner_epoch = owned.token.owner_epoch.0,
                    token = %owned.token,
                    remaining_ms = u64::try_from(owned.remaining.as_millis()).unwrap_or(u64::MAX),
                    "ownership_held"
                );
                println!(
                    "rdb_node ownership node={} partition={} token={}",
                    node.0, owned.partition.0, owned.token
                );
            }
        },
    );
}

/// #83's manual entry point: print each ownership event as it arrives. A receiver that ends
/// (this subscriber lagged, or a node stopped) is subscribed again, which starts from the
/// current state.
fn print_ownership_events(db: &Db, done: &AtomicBool) {
    let mut events = db.ownership_events();
    while !done.load(Ordering::Relaxed) {
        let event = match events.recv_timeout(Duration::from_millis(100)) {
            Ok(event) => event,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                tracing::warn!("rdb_node_ownership_resubscribed");
                std::thread::sleep(Duration::from_millis(100));
                events = db.ownership_events();
                continue;
            }
        };
        let tail = match event {
            OwnershipEvent::Deactivating { deadline, .. } => format!(
                " deadline_in_ms={}",
                deadline
                    .saturating_duration_since(std::time::Instant::now())
                    .as_millis()
            ),
            _ => String::new(),
        };
        println!(
            "rdb_node ownership-event node={} partition={} state={} token={}{tail}",
            event.node().0,
            event.partition().0,
            event.state(),
            event.token()
        );
    }
}

/// Run node `--node` of the database in `--dir` until `--stop-file` appears.
/// Prints a wait's outcome. True when it timed out, so `serve` keeps waiting: the
/// line then ends `(retrying)`, and `lr` reads a not-ready line without it as final.
fn report_ready(node: NodeId, waited: Result<(), ReadyError>) -> bool {
    match waited {
        Ok(()) => {
            println!("rdb_node ready node={}", node.0);
            false
        }
        Err(e) => {
            let retrying = matches!(e, ReadyError::TimedOut { .. });
            tracing::warn!(node = node.0, error = %e, retrying, "rdb_node_not_ready");
            let tail = if retrying { " (retrying)" } else { "" };
            println!("rdb_node not-ready node={}: {e}{tail}", node.0);
            retrying
        }
    }
}

/// The nodes `node` links to: every node `--peers` names. With `node` they must be nodes 1
/// to K, K at least 3 (M11 WS A): each formed member, then any spares, with no gap.
fn peer_set(node: NodeId, peers: &BTreeMap<NodeId, String>) -> Result<BTreeSet<NodeId>, String> {
    for n in ALL_NODES {
        if n != node && !peers.contains_key(&n) {
            return Err(format!("--peers names no address for node {}", n.0));
        }
    }
    // `lr` lists every node, this one included; its own address is not dialled.
    let mut all: BTreeSet<NodeId> = peers.keys().copied().collect();
    all.insert(node);
    let k = u32::try_from(all.len()).unwrap_or(u32::MAX);
    if all.iter().map(|n| n.0).ne(1..=k) {
        return Err(format!(
            "--peers and --node name nodes {:?}: they must be nodes 1 to K with no gap",
            all.iter().map(|n| n.0).collect::<Vec<_>>()
        ));
    }
    all.remove(&node);
    Ok(all)
}

fn serve(args: &Args, rt: &tokio::runtime::Runtime) -> Result<Served, String> {
    // The client may start tasks as it connects, so it is made inside the runtime.
    let store = {
        let _entered = rt.enter();
        control(args)?
    };
    serve_on(args, rt, store)
}

/// [`serve`] on `store`, so a row can run the node process's start on its own store.
fn serve_on(
    args: &Args,
    rt: &tokio::runtime::Runtime,
    store: Arc<dyn ConfigStore>,
) -> Result<Served, String> {
    serve_on_ready(args, rt, store, None, &|_| {})
}

/// [`serve_on`], calling `on_ready` with the node each time it prints its `ready` line, so a
/// row can wait for every node of a database to be ready (#80 S1b). Given `bound`, a listener
/// already bound to `--listen`, the node listens on it instead of binding `--listen` itself, so
/// a row that picked the port never releases it for another process to take (PR #144 review).
/// A `bound` on another address is refused before anything is written.
fn serve_on_ready(
    args: &Args,
    rt: &tokio::runtime::Runtime,
    store: Arc<dyn ConfigStore>,
    bound: Option<tokio::net::TcpListener>,
    on_ready: &(dyn Fn(NodeId) + Sync),
) -> Result<Served, String> {
    let node = NodeId(need(args.node.as_ref(), "--node")?);
    let dir = need(args.dir.as_ref(), "--dir")?;
    let listen = need(args.listen.as_ref(), "--listen")?;
    let cert_dir = need(args.cert_dir.as_ref(), "--cert-dir")?;
    let peers = peer_set(node, &args.peers)?;
    let _entered = rt.enter();
    let faults = Faults::new();
    if let Some(path) = &args.faults {
        faults.watch_file(path.clone());
    }
    // #80 S1b: before the boot is written or anything is bound.
    start_check_on(args, node, &listen, &*store, rt)?;
    if let Some(bound) = &bound {
        let at = bound
            .local_addr()
            .map_err(|e| format!("--listen {listen}: the listener given: {e}"))?;
        if at.to_string() != listen {
            return Err(format!(
                "--listen {listen}: the listener given is bound to {at}; nothing was started"
            ));
        }
    }
    let store: Arc<dyn ConfigStore> = CutStore::new(store, Arc::clone(&faults));
    let boot = next_boot(&dir).map_err(|e| format!("boot: {}: {e}", dir.display()))?;
    let label = Label::Node(node);
    let tls = certs::load(&cert_dir, &label)?;
    let listener = match bound {
        Some(bound) => bound,
        None => rt
            .block_on(tokio::net::TcpListener::bind(&listen))
            .map_err(|e| format!("--listen {listen}: {e}"))?,
    };
    let links = Links::with_clock_offset(args.clock_offset_ms);
    if let Some(file) = args.clock_step_file.clone() {
        watch_clock_step(file, Arc::clone(&links));
    }
    let net = Net::start(
        NetConfig {
            label,
            boot,
            tls,
            peers,
            dial: NetConfig::dial_lower(node, &args.peers),
            protocol_version: args.protocol_version.unwrap_or(PROTOCOL_VERSION),
            faults,
            admin_apps: admin_apps(args),
        },
        &links,
        Some(listener),
    )?;
    let addr = net.addr().map_or_else(|| listen.clone(), |a| a.to_string());
    tracing::info!(node = node.0, %addr, boot = boot.0, "rdb_node_listening");
    println!(
        "rdb_node listening node={} addr={addr} boot={}",
        node.0, boot.0
    );

    let config = DbConfig {
        partitions: args.partitions,
        ack_floor: true,
        dir: dir.clone(),
        hold: Vec::new(),
        timeouts: Timeouts {
            unreachable_drop_after: args.unreachable_drop_after_ms.map_or(
                Timeouts::default().unreachable_drop_after,
                Duration::from_millis,
            ),
            live_readd: args.live_readd,
            ..Timeouts::default()
        },
        dedup_cap: None,
        dedup_retention: None,
        history_keep: None,
        history_budget_bytes: None,
        local: vec![node],
        links: Some(Arc::clone(&links)),
        client: DEFAULT_CLIENT,
        boot: Some(boot),
        storage: rdb_api::StorageOptions::default(),
        failover: failover(args),
        // #80 S1b ruling 2: checked against the records by the open too, as an embedded node is.
        addresses: Some(rdb_api::NodeAddresses {
            listen: listen.clone(),
            peers: args.peers.clone(),
            app: args.app_addr.clone(),
        }),
    };
    let mut db = rt
        .block_on(Db::open(config, store, rt.handle().clone()))
        .map_err(|e| format!("open {} as node {}: {e}", dir.display(), node.0))?;
    println!("rdb_node open node={}", node.0);
    let within = Duration::from_secs(args.ready_secs.unwrap_or(60));
    let waited = std::time::Instant::now();
    // A missed deadline is a line, not a stop (tester-s2 `lr up` race, A-R271): only a time-out
    // keeps waiting, and the late `ready` line says how long it took. Any other refusal ends it.
    let first = db.wait_ready(within);
    if first.is_ok() {
        on_ready(node);
    }
    let mut retrying = report_ready(node, first);
    let stopped = || args.stop_file.as_ref().is_some_and(|stop| stop.exists());
    let done = AtomicBool::new(false);
    // Each watcher logs in the span this serve runs in, as the node's own lines do, so a row's
    // node logs land in that row's file, not untagged.
    let span = tracing::Span::current();
    let served = std::thread::scope(|scope| {
        scope.spawn(|| span.in_scope(|| watch_ownership(&db, node, &done)));
        scope.spawn(|| span.in_scope(|| print_ownership_events(&db, &done)));
        let mut served = Served::Stopped;
        let mut checked = std::time::Instant::now();
        while !stopped() {
            // V10 (#81): about once a second, not every turn: `node_status` can wait out the
            // read timeout.
            if checked.elapsed() >= RESTART_CHECK_EVERY {
                checked = std::time::Instant::now();
                if let Some(reason) = db.restart_wanted() {
                    tracing::warn!(node = node.0, %reason, "rdb_node_restart_wanted");
                    println!("rdb_node restart-wanted node={} reason={reason}", node.0);
                    served = Served::Restart(reason);
                    break;
                }
            }
            if !retrying {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            match db.wait_ready(Duration::from_secs(1)) {
                Ok(()) => {
                    let after = waited.elapsed().as_secs();
                    tracing::info!(node = node.0, after_s = after, "rdb_node_ready_late");
                    println!("rdb_node ready node={} (after {after}s)", node.0);
                    on_ready(node);
                    retrying = false;
                }
                Err(ReadyError::TimedOut { .. }) => {}
                ended => retrying = report_ready(node, ended),
            }
        }
        done.store(true, Ordering::Relaxed);
        served
    });
    if let (Served::Stopped, Some(stop)) = (served, &args.stop_file) {
        tracing::info!(node = node.0, stop = %stop.display(), "rdb_node_stop_file");
    }
    db.shutdown();
    // `Db::shutdown` leaves the links up; the process exits next anyway, but a peer should
    // see this boot go at once, not when the sockets close.
    net.shutdown();
    drop(db);
    drop(net);
    println!("rdb_node stopped node={}", node.0);
    Ok(served)
}

/// How a node process's serve ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Served {
    /// `--stop-file` appeared: exit 0.
    Stopped,
    /// V10 (#81): this boot can serve nothing again, so it asks its supervisor for a restart,
    /// [`RESTART_EXIT`]. The reason `Db::restart_wanted` gave.
    Restart(RestartReason),
}

/// The exit code that asks the supervisor to start this node process again (V10, #81):
/// `EX_TEMPFAIL`. `docs/rdb/embed.md` states the contract.
const RESTART_EXIT: u8 = 75;

/// How often the serve loop reads its own `NodeStatus` for V10.
const RESTART_CHECK_EVERY: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        admin_apps, declare_on, parse, serve_on, serve_on_ready, ConfigStore, RestartReason, Served,
    };

    /// The `--peers` value of three nodes on three loopback hosts (#80 S1b).
    const PEERS: &str = "1=127.0.0.1:7100,2=127.0.0.2:7100,3=127.0.0.3:7100";

    fn apps(argv: &[&str]) -> Vec<String> {
        let args = parse(argv.iter().map(|a| (*a).to_owned())).expect("parses");
        admin_apps(&args).into_iter().collect()
    }

    /// Critic-479 A6 (A-R479), ruling A-R480: how `--admin-apps` is read. Unset is no app, as
    /// the library's empty `NetConfig::admin_apps` is, so a production node grants nobody a
    /// move by default; `lr` passes `--admin-apps admin`. An empty value is no app too; a list
    /// is that list. A regression here fails open without a word. Unit.
    #[test]
    fn pr74_r480_admin_apps_unset_and_empty_are_none() {
        assert_eq!(apps(&[]), Vec::<String>::new());
        assert_eq!(apps(&["--admin-apps", ""]), Vec::<String>::new());
        assert_eq!(apps(&["--admin-apps", "ops,admin"]), ["admin", "ops"]);
    }

    /// Revobot R3 4489400304: each name is trimmed, so `--admin-apps "ops, admin"` lists
    /// `admin`, not ` admin`, which the gate would refuse; a blank name is dropped, so a list
    /// of blanks is still none. Unit.
    #[test]
    fn pr74_r3_admin_apps_names_are_trimmed_and_blanks_dropped() {
        assert_eq!(apps(&["--admin-apps", "ops, admin"]), ["admin", "ops"]);
        assert_eq!(apps(&["--admin-apps", " ops ,  admin "]), ["admin", "ops"]);
        assert_eq!(apps(&["--admin-apps", " , "]), Vec::<String>::new());
    }

    fn parsed(argv: &[&str]) -> Result<super::Args, String> {
        parse(argv.iter().map(|a| (*a).to_owned()))
    }

    /// #80 S1a: `--declare` runs no planner either, so `--failover` with it is refused; plain
    /// `--declare` with its count parses. Unit. (#80 S1b: with the `--peers` it now needs.)
    #[test]
    fn s1a_declare_rejects_failover() {
        assert_eq!(
            parsed(&["--declare", "--failover"]).map(|_| ()),
            Err("--failover runs only in a node process, not with --declare".to_owned())
        );
        assert!(
            parsed(&["--declare", "--partitions", "8", "--peers", PEERS])
                .is_ok_and(|args| args.declare && args.partitions == Some(8) && !args.failover)
        );
    }

    /// PR #138 review: each planner knob with `--declare` is refused, with `--failover` (no
    /// planner runs) and without it (an orphan knob). Unit.
    #[test]
    fn s1a_declare_rejects_each_planner_knob() {
        for knob in [
            ["--failover-silent-ms", "2000"],
            ["--failover-max-concurrent", "2"],
        ] {
            assert_eq!(
                parsed(&["--declare", "--failover", knob[0], knob[1]]).map(|_| ()),
                Err("--failover runs only in a node process, not with --declare".to_owned()),
                "{knob:?} with --failover"
            );
            assert_eq!(
                parsed(&["--declare", knob[0], knob[1]]).map(|_| ()),
                Err(format!("{} needs --failover", knob[0])),
                "{knob:?} alone"
            );
        }
    }

    /// PR #138 review: `--declare` refuses at parse time the five mode and copy flags
    /// `declare_refusal` names: three it would ignore (`--node`, `--dir`, `--live-readd`) and
    /// two (`--form`, `--gen-certs`) that would run instead of declaring. Unit.
    /// Cut: drop `declare_refusal` from `parse`; every pair parses, red.
    #[test]
    fn s1a_declare_rejects_five_mode_and_copy_flags() {
        for (flags, named) in [
            (&["--form"][..], "--form"),
            (&["--gen-certs"][..], "--gen-certs"),
            (&["--node", "2"][..], "--node"),
            (&["--dir", "d"][..], "--dir"),
            (&["--live-readd"][..], "--live-readd"),
        ] {
            let argv: Vec<&str> = ["--declare"].iter().chain(flags).copied().collect();
            assert_eq!(
                parsed(&argv).map(|_| ()),
                Err(format!(
                    "--declare writes only control records and takes no {named}"
                )),
                "{argv:?}"
            );
        }
    }

    /// PR #138 review: `--declare --nodes` outside 3 to `MAX_NODES`, or not a number, is
    /// refused while parsing; the shared parser still takes 4 and 8 for `--gen-certs`. Unit.
    #[test]
    fn s1a_declare_node_counts_parse_as_gen_certs_does() {
        for bad in ["0", "9", "three"] {
            assert!(
                parsed(&["--declare", "--nodes", bad]).is_err(),
                "--declare --nodes {bad}"
            );
        }
        for good in [4, 8] {
            let text = good.to_string();
            assert!(
                parsed(&["--gen-certs", "--cert-dir", "c", "--nodes", &text])
                    .is_ok_and(|args| args.nodes == Some(good)),
                "--gen-certs --nodes {good}"
            );
        }
    }

    /// #80 S1b row 15: `--declare` takes the nodes and their addresses from `--peers`, and
    /// optional machine labels from `--machines`; it declares five nodes and prints each record
    /// read back from the store, `N@addr/machine`, a node with no label taking its host. Run
    /// again it says `already`. While parsing it refuses no `--peers`, a `--nodes` that counts
    /// otherwise, and a `--machines` node `--peers` does not name; `--machines` without
    /// `--declare` is refused too, and so is a node `--peers` or `--machines` names twice,
    /// within one flag or across two, rather than the later entry silently winning (PR #144
    /// review). And two nodes on one machine reach `admin::declare`'s refusal with nothing
    /// written. Unit (<100 ms, an in-memory store). This replaces the S1a
    /// row that declared `--nodes 4` to reach a refusal: four nodes are now legal (row 6).
    /// Cut: `declare_on` ignores `--machines`; node 4's label is its host, red at the line.
    /// Cut: the parser's `insert` overwrites a repeated node; the four twice-named cases parse,
    /// red at the first.
    #[config_log::retcd_test]
    fn s1b_declare_takes_peers_and_machines_and_prints_the_records() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        for (argv, refused) in [
            (&["--declare"][..], "--declare needs --peers"),
            (
                &["--declare", "--peers", PEERS, "--nodes", "4"][..],
                "--nodes 4 disagrees with --peers, which names 3 nodes",
            ),
            (
                &["--declare", "--peers", PEERS, "--machines", "4=m4"][..],
                "--machines names node 4, which --peers does not",
            ),
            (
                &["--node", "1", "--machines", "1=m1"][..],
                "--machines is read only by --declare",
            ),
            (
                &[
                    "--declare",
                    "--peers",
                    "1=127.0.0.1:7100,2=127.0.0.2:7100,2=127.0.0.3:7100",
                ][..],
                "--peers names node 2 twice, as 127.0.0.2:7100 and 127.0.0.3:7100",
            ),
            (
                &["--declare", "--peers", PEERS, "--peers", "3=127.0.0.9:7100"][..],
                "--peers names node 3 twice",
            ),
            (
                &["--declare", "--peers", PEERS, "--machines", "1=a,1=b"][..],
                "--machines names node 1 twice, as a and b",
            ),
            (
                &[
                    "--declare",
                    "--peers",
                    PEERS,
                    "--machines",
                    "1=a",
                    "--machines",
                    "1=a",
                ][..],
                "--machines names node 1 twice",
            ),
        ] {
            assert!(
                matches!(parsed(argv), Err(text) if text.contains(refused)),
                "{argv:?}"
            );
        }
        let five = "1=127.0.0.1:7100,2=127.0.0.2:7100,3=127.0.0.3:7100,4=127.0.0.4:7100,\
                    5=127.0.0.5:7100";
        let args = parsed(&[
            "--declare",
            "--peers",
            five,
            "--machines",
            "4=Rack-4,5=r5",
            "--partitions",
            "16",
        ])
        .expect("parses");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let line = declare_on(&args, &store, &rt).expect("five nodes declare");
        assert!(line.starts_with("rdb_node declared "), "{line}");
        assert!(
            line.ends_with(
                " partitions=16 nodes=1@127.0.0.1:7100/127.0.0.1,2@127.0.0.2:7100/127.0.0.2,\
                 3@127.0.0.3:7100/127.0.0.3,4@127.0.0.4:7100/rack-4,5@127.0.0.5:7100/r5"
            ),
            "{line}"
        );
        let again = declare_on(&args, &store, &rt).expect("the same declaration");
        assert!(again.starts_with("rdb_node already "), "{again}");

        let empty: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let clash = parsed(&[
            "--declare",
            "--peers",
            "1=127.0.0.1:7100,2=127.0.0.1:7101,3=127.0.0.3:7100",
        ])
        .expect("parses");
        let refused = declare_on(&clash, &empty, &rt);
        assert!(
            matches!(&refused, Err(text) if text.contains("one data node per machine")
                && text.contains("nothing was written")),
            "{refused:?}"
        );
        assert_eq!(
            rt.block_on(rdb_api::admin::read_formation(&*empty))
                .expect("the store answers"),
            None,
            "no formation record"
        );
    }

    /// #80 S1b row 16 (rulings 2 and 4): a node process whose `--listen` disagrees with its
    /// `nodes/` record is refused by `serve` before it writes its boot or binds: the error
    /// names `nodes/3` and both addresses, and no `--dir` is created. `serve` writes the boot
    /// (`next_boot` creates `--dir`) before it binds, so no folder means no bind either. So is
    /// one whose `--peers` names another address for node 2, naming `nodes/2`. Unit (<200 ms,
    /// an in-memory store).
    /// Cut: drop the `start_check_on` call from `serve_on`; the start goes on, writes the boot
    /// and fails at the missing certificates, red at the error's text (and the folder exists).
    #[config_log::retcd_test]
    fn s1b_a_listen_that_disagrees_with_its_record_is_refused_before_boot_or_bind() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let root = config_testkit::fs::temp_dir();
        let wrong = {
            let free = std::net::TcpListener::bind("127.0.0.3:0").expect("a free port");
            free.local_addr().expect("bound").to_string()
        };
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        declare_on(
            &parsed(&["--declare", "--peers", PEERS, "--partitions", "8"]).expect("parses"),
            &store,
            &rt,
        )
        .expect("declared");
        let path = |p: &std::path::Path| p.display().to_string();
        let dir = root.path().join("node-3");
        for (listen, peers, names) in [
            (
                wrong.as_str(),
                PEERS.to_owned(),
                vec![
                    format!("listen address {wrong}"),
                    "nodes/3".to_owned(),
                    "127.0.0.3:7100".to_owned(),
                ],
            ),
            (
                "127.0.0.3:7100",
                "1=127.0.0.1:7100,2=127.0.0.2:7101".to_owned(),
                vec![
                    "peer address 2=127.0.0.2:7101".to_owned(),
                    "nodes/2".to_owned(),
                ],
            ),
        ] {
            let args = parsed(&[
                "--node",
                "3",
                "--dir",
                &path(&dir),
                "--listen",
                listen,
                "--peers",
                &peers,
                "--control",
                "http://127.0.0.1:1",
                "--cert-dir",
                &path(&root.path().join("certs")),
            ])
            .expect("parses");
            let refused = serve_on(&args, &rt, Arc::clone(&store));
            assert!(
                matches!(&refused, Err(text) if text.starts_with("start check: ")
                    && names.iter().all(|name| text.contains(name.as_str()))),
                "{listen} {peers}: {refused:?}"
            );
            assert!(
                !dir.exists(),
                "{listen}: a boot was written in {}",
                dir.display()
            );
        }
    }

    /// #80 S1b, PR #144 review r2: a node process handed a listener (`serve_on_ready`'s
    /// `bound`) that is bound to another address than `--listen` is refused before it writes
    /// its boot: the error names `--listen` and where the listener is bound, and no `--dir` is
    /// created. `--listen` and `--peers` are the records', so the start check passes and only
    /// the listener's address can refuse it. Unit (<200 ms, an in-memory store).
    /// Cut: drop the bound-address check from `serve_on_ready`; the start goes on, writes the
    /// boot and fails at the missing certificates, red at the error's text (and the folder
    /// exists).
    #[config_log::retcd_test]
    fn s1b_a_listener_bound_to_another_address_is_refused_before_boot() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let root = config_testkit::fs::temp_dir();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        declare_on(
            &parsed(&["--declare", "--peers", PEERS, "--partitions", "8"]).expect("parses"),
            &store,
            &rt,
        )
        .expect("declared");
        let bound = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.3:0"))
            .expect("a free port");
        let at = bound.local_addr().expect("bound").to_string();
        let path = |p: &std::path::Path| p.display().to_string();
        let dir = root.path().join("node-3");
        let args = parsed(&[
            "--node",
            "3",
            "--dir",
            &path(&dir),
            "--listen",
            "127.0.0.3:7100",
            "--peers",
            PEERS,
            "--control",
            "http://127.0.0.1:1",
            "--cert-dir",
            &path(&root.path().join("certs")),
        ])
        .expect("parses");
        let refused = serve_on_ready(&args, &rt, Arc::clone(&store), Some(bound), &|_| {});
        assert!(
            matches!(&refused, Err(text)
                if text.contains("--listen 127.0.0.3:7100")
                    && text.contains(&format!("the listener given is bound to {at}"))
                    && text.ends_with("nothing was started")),
            "{refused:?}"
        );
        assert!(!dir.exists(), "a boot was written in {}", dir.display());
    }

    /// #80 S1b ruling 1 (single machine): a whole declared database runs on one box. Four
    /// node processes' start paths (`serve_on`, one thread each) on 127.0.0.1 to .4, the hosts
    /// `--declare` recorded, pass the start check, open their own empty folders and form every
    /// partition: each `partitions/{p}` gains its root, and every node reaches ready (its
    /// `ready` line, through `serve_on_ready`). Only then is the stop file written, so no node
    /// is still in a readiness wait when its peers go. Stopped through `--stop-file`, each ends
    /// cleanly and its folder holds only its own `nodes/N`. #83 B2 (PR #159 review): nodes 1
    /// to 3 start with `--app-addr`, node 4 without; each of the first three records holds its
    /// flag's bytes unchanged, node 4's none. Cut: `serve_on_ready` passes `app: None`, red at
    /// node 1's record. Cluster row (~0.5 s measured,
    /// bounded at 30 s): four nodes link over TLS, form four partitions and admit writes. The
    /// node threads log outside this row's span, so their lines land in the run's
    /// `_untagged-<pid>.jsonl`.
    /// Cut: `machine_clash` compares only a host's first three characters, so every loopback
    /// address is one machine; the declaration is refused, red. Cut: `serve_on_ready` never
    /// calls `on_ready`; red at "every node is ready" after the 30 s bound. Cut: `serve_on_ready`
    /// drops the listener it is handed and binds `--listen` again; the row's second handle
    /// holds each port, so every node ends `Address already in use`, red after the 30 s bound.
    #[config_log::retcd_test]
    fn s1b_four_declared_node_processes_run_on_one_machine() {
        const K: u32 = 4;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        let root = config_testkit::fs::temp_dir();
        let certs = root.path().join("certs");
        let labels: Vec<rdb_api::net::wire::Label> = (1..=K)
            .map(|n| rdb_api::net::wire::Label::Node(rdb_api::NodeId(n)))
            .collect();
        rdb_api::net::certs::generate(&certs, &labels).expect("certificates");
        // Each port stays bound from its pick to its node's start, which listens on the very
        // socket (PR #144 review). The row keeps a second handle on each socket throughout, so
        // a start that dropped the listener and bound `--listen` again fails `AddrInUse`.
        let mut handed: Vec<Option<tokio::net::TcpListener>> = Vec::new();
        let mut kept: Vec<std::net::TcpListener> = Vec::new();
        let addrs: Vec<String> = (1..=K)
            .map(|n| {
                let host = std::net::Ipv4Addr::new(127, 0, 0, u8::try_from(n).expect("small"));
                let (listener, addr) =
                    rt.block_on(config_testkit::ports::ephemeral_listener_on(host));
                let std = listener.into_std().expect("a std listener");
                kept.push(std.try_clone().expect("a second handle"));
                let _entered = rt.enter();
                handed.push(Some(
                    tokio::net::TcpListener::from_std(std).expect("a tokio listener"),
                ));
                addr.to_string()
            })
            .collect();
        let peers: Vec<String> = addrs
            .iter()
            .zip(1..)
            .map(|(addr, n)| format!("{n}={addr}"))
            .collect();
        let peers = peers.join(",");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let line = declare_on(
            &parsed(&["--declare", "--peers", &peers, "--partitions", "4"]).expect("parses"),
            &store,
            &rt,
        )
        .expect("four nodes on four loopback hosts declare");
        assert!(line.starts_with("rdb_node declared "), "{line}");
        let stop = root.path().join("stop");
        let path = |p: &std::path::Path| p.display().to_string();
        let ready = std::sync::Mutex::new(std::collections::BTreeSet::new());
        let on_ready = |node: rdb_api::NodeId| {
            ready.lock().expect("the ready set").insert(node.0);
        };
        let dirs: Vec<std::path::PathBuf> = (1..=K)
            .map(|n| root.path().join(format!("node-{n}")))
            .collect();
        let (certs_path, stop_path) = (path(&certs), path(&stop));
        let apps: Vec<String> = (1..K)
            .map(|n| format!("https://u:p@gw-{n}.example:8443/sbx"))
            .collect();
        let ended = std::thread::scope(|scope| {
            let running: Vec<_> = (1..=K)
                .map(|n| {
                    let node = n.to_string();
                    let dir = path(&dirs[n as usize - 1]);
                    let mut argv = vec![
                        "--node",
                        &node,
                        "--dir",
                        &dir,
                        "--listen",
                        &addrs[n as usize - 1],
                        "--peers",
                        &peers,
                        "--control",
                        "http://127.0.0.1:1",
                        "--cert-dir",
                        &certs_path,
                        "--stop-file",
                        &stop_path,
                        "--ready-secs",
                        "30",
                    ];
                    // #83 B2: nodes 1 to 3 register an app address, node 4 none.
                    if n < K {
                        argv.extend(["--app-addr", &apps[n as usize - 1]]);
                    }
                    let args = parsed(&argv).expect("parses");
                    let store = Arc::clone(&store);
                    let (rt, on_ready) = (&rt, &on_ready);
                    let bound = handed[n as usize - 1].take();
                    scope.spawn(move || serve_on_ready(&args, rt, store, bound, on_ready))
                })
                .collect();
            let rooted = |p: u32| {
                let key = rdb_core::contracts::control::ControlKey::Partition(
                    rdb_core::contracts::ids::PartitionId(p),
                )
                .encode();
                rt.block_on(store.get(config_core::GetRequest {
                    key: bytes::Bytes::from(key),
                }))
                .ok()
                .and_then(|read| read.record)
                .is_some_and(|record| {
                    rdb_core::authority::partition::PartitionRoot::decode(&record.value).is_some()
                })
            };
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            let (mut formed, mut all_ready) = (false, false);
            while std::time::Instant::now() < deadline {
                formed = formed || (1..=4).all(rooted);
                all_ready = ready.lock().expect("the ready set").len() == K as usize;
                if formed && all_ready {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            std::fs::write(&stop, b"").expect("the stop file");
            let ended: Vec<Result<Served, String>> = running
                .into_iter()
                .map(|handle| handle.join().expect("a node thread"))
                .collect();
            assert!(formed, "every partition formed: {ended:?}");
            assert!(
                all_ready,
                "every node is ready: {:?} of {K}: {ended:?}",
                ready.lock().expect("the ready set")
            );
            ended
        });
        drop(kept);
        for (n, outcome) in (1..=K).zip(ended) {
            assert_eq!(outcome, Ok(Served::Stopped), "node {n} ends cleanly");
            let held: Vec<String> = std::fs::read_dir(dirs[n as usize - 1].join("nodes"))
                .expect("its nodes/ folder")
                .map(|entry| {
                    entry
                        .expect("an entry")
                        .file_name()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect();
            assert_eq!(held, [n.to_string()], "node {n} holds only its own copy");
            let key = rdb_core::contracts::control::ControlKey::Node(rdb_api::NodeId(n)).encode();
            let body = rt
                .block_on(store.get(config_core::GetRequest {
                    key: bytes::Bytes::from(key),
                }))
                .expect("a read")
                .record
                .expect("the node's record")
                .value;
            let app = rdb_core::authority::node::NodeRecord::decode_with_app(&body)
                .and_then(|(_, app)| app);
            assert_eq!(
                app.as_deref(),
                apps.get(n as usize - 1).map(String::as_str),
                "node {n}'s --app-addr reached its record unchanged"
            );
        }
    }

    /// PR #119 review: `--failover` with `--form` is a parse error, since forming runs no
    /// planner; plain `--form` parses. Unit.
    /// Cut: drop the mode check in `failover_refusal`; the pair parses, red.
    #[test]
    fn own81_form_rejects_failover() {
        assert_eq!(
            parsed(&["--form", "--failover"]).map(|_| ()),
            Err("--failover runs only in a node process, not with --form".to_owned())
        );
        assert!(parsed(&["--form"]).is_ok_and(|args| args.form && !args.failover));
    }

    /// #83 B2, PR #159 review: `--app-addr` reaches a node process's args byte for byte, a
    /// URL with a credential and a space included; a node process without it has none.
    /// `--form` with `--app-addr` is a parse error, since forming would drop it; plain
    /// `--form` still parses. `--declare` and `--gen-certs` refuse it too, for the same reason;
    /// plain `--gen-certs` still parses. Cut: `--declare` dropped from the refusal's list, red
    /// at `--declare`. Unit.
    #[test]
    fn own83_b2_app_addr_parses_and_form_refuses_it() {
        let app = "https://user:s3cret@gw-1.example:8443/sbx path";
        let args = parsed(&["--node", "1", "--app-addr", app]).expect("parses");
        assert_eq!(args.app_addr.as_deref(), Some(app));
        let args = parsed(&["--node", "1"]).expect("parses");
        assert_eq!(args.app_addr, None);
        for mode in ["--form", "--declare", "--gen-certs"] {
            let mut argv = vec![mode, "--app-addr", "gw:1"];
            if mode == "--declare" {
                argv.extend([
                    "--peers",
                    "1=127.0.0.1:7100,2=127.0.0.2:7100,3=127.0.0.3:7100",
                ]);
            }
            assert_eq!(
                parsed(&argv).map(|_| ()),
                Err(format!(
                    "--app-addr is written by a node process of a declared database, not with \
                     {mode}"
                )),
                "{mode}"
            );
        }
        assert!(parsed(&["--form"]).is_ok_and(|args| args.form && args.app_addr.is_none()));
        assert!(parsed(&["--gen-certs"]).is_ok_and(|args| args.gen_certs));
    }

    /// PR #119 review: `--failover` with `--gen-certs` is a parse error; plain `--gen-certs`
    /// parses. Unit.
    /// Cut: as above.
    #[test]
    fn own81_certs_reject_failover() {
        assert_eq!(
            parsed(&["--gen-certs", "--failover"]).map(|_| ()),
            Err("--failover runs only in a node process, not with --gen-certs".to_owned())
        );
        assert!(parsed(&["--gen-certs"]).is_ok_and(|args| args.gen_certs));
    }

    /// PR #119 review: `--failover-silent-ms` without `--failover` is a parse error; with it,
    /// the knob is taken. Unit.
    /// Cut: drop the knob check in `failover_refusal`; the orphan parses, red.
    #[test]
    fn own81_silent_knob_needs_failover() {
        assert_eq!(
            parsed(&["--failover-silent-ms", "3000"]).map(|_| ()),
            Err("--failover-silent-ms needs --failover".to_owned())
        );
        let args = parsed(&["--failover", "--failover-silent-ms", "3000"]).expect("parses");
        assert_eq!(
            super::failover(&args).map(|f| f.silent_after),
            Some(std::time::Duration::from_millis(3_000))
        );
    }

    /// PR #119 review: `--failover-max-concurrent` without `--failover` is a parse error; with
    /// it, the knob is taken. Unit.
    /// Cut: as above.
    #[test]
    fn own81_concurrency_knob_needs_failover() {
        assert_eq!(
            parsed(&["--failover-max-concurrent", "4"]).map(|_| ()),
            Err("--failover-max-concurrent needs --failover".to_owned())
        );
        let args = parsed(&["--failover-max-concurrent", "4", "--failover"]).expect("parses");
        assert_eq!(super::failover(&args).map(|f| f.max_concurrent), Some(4));
    }

    /// PR #119 review: a zero `--failover-max-concurrent` and a `--failover-silent-ms` under
    /// one clock-probe period are refused while parsing, before the process writes its boot
    /// or listens; exactly 1000 ms is taken. Unit.
    /// Cut: drop either parse-time check, red (the flags parse).
    #[config_log::retcd_test]
    fn own81_failover_knobs_refused_while_parsing() {
        assert_eq!(
            parsed(&["--failover", "--failover-max-concurrent", "0"]).map(|_| ()),
            Err("--failover-max-concurrent wants a count of at least 1".to_owned())
        );
        for millis in ["0", "999"] {
            assert_eq!(
                parsed(&["--failover", "--failover-silent-ms", millis]).map(|_| ()),
                Err(format!(
                    "--failover-silent-ms {millis} is under one clock-probe period (1000 ms): \
                     a healthy link would read as silent"
                ))
            );
        }
        let args = parsed(&["--failover", "--failover-silent-ms", "1000"]).expect("parses");
        assert_eq!(
            super::failover(&args).map(|f| f.silent_after),
            Some(std::time::Duration::from_millis(1_000))
        );
    }

    /// Runs `main`'s path on a node process's arguments plus `extra`, with node 1's data
    /// directory, cert dir and log dir under a fresh root, and `--listen` on a port this row
    /// holds, and asserts it stopped as a bad argument (exit 2) before any startup effect: no
    /// boot written (`next_boot` creates `--dir`), and no bind tried (the held port would make
    /// a bind fail, exit 1).
    fn refused_before_start(extra: &[&str]) {
        let root = config_testkit::fs::temp_dir();
        let held = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
        let listen = held.local_addr().expect("bound").to_string();
        let dir = root.path().join("node-1");
        let path = |p: &std::path::Path| p.display().to_string();
        let mut argv: Vec<String> = [
            "--node".to_owned(),
            "1".to_owned(),
            "--dir".to_owned(),
            path(&dir),
            "--listen".to_owned(),
            listen.clone(),
            "--peers".to_owned(),
            format!("2={listen},3={listen}"),
            "--control".to_owned(),
            format!("http://{listen}"),
            "--cert-dir".to_owned(),
            path(&root.path().join("certs")),
            "--log-dir".to_owned(),
            path(&root.path().join("logs")),
        ]
        .into();
        argv.extend(extra.iter().map(|a| (*a).to_owned()));
        let code = super::run(argv.into_iter(), |_| Ok(None), super::serve);
        assert_eq!(code, std::process::ExitCode::from(2), "{extra:?}: exit 2");
        assert!(
            !dir.exists(),
            "{extra:?}: a boot was written in {}",
            dir.display()
        );
        drop(held);
    }

    /// PR #119 review: a zero `--failover-max-concurrent` stops the node process before it
    /// writes its boot or binds `--listen`, and the parse error is the one the parser row pins.
    /// Unit.
    /// Cut: move the zero check from `parse` to `serve`, after `next_boot` and the bind; the
    /// boot is written and the bind fails (exit 1), red.
    #[config_log::retcd_test]
    fn own81_zero_concurrency_refused_before_boot_or_listen() {
        refused_before_start(&["--failover", "--failover-max-concurrent", "0"]);
        assert_eq!(
            parsed(&["--failover", "--failover-max-concurrent", "0"]).map(|_| ()),
            Err("--failover-max-concurrent wants a count of at least 1".to_owned())
        );
    }

    /// PR #119 review: a `--failover-silent-ms` of 0 or 999 stops the node process before it
    /// writes its boot or binds `--listen`; exactly 1000 ms is taken. Unit.
    /// Cut: move the floor check from `parse` to `serve`, after `next_boot`; red.
    #[config_log::retcd_test]
    fn own81_short_silence_refused_before_boot_or_listen() {
        for millis in ["0", "999"] {
            refused_before_start(&["--failover", "--failover-silent-ms", millis]);
        }
        let args = parsed(&["--failover", "--failover-silent-ms", "1000"]).expect("parses");
        assert_eq!(
            super::failover(&args).map(|f| f.silent_after),
            Some(std::time::Duration::from_millis(1_000))
        );
    }

    /// PR #119 review: a value that is no count, or a negative one, is refused for
    /// `--failover-max-concurrent`; one that is no number, or past `u64`, for
    /// `--failover-silent-ms`. Each with the flag's existing parse error. Unit.
    #[config_log::retcd_test]
    fn own81_failover_knob_values_that_are_no_number_are_refused() {
        for value in ["abc", "-1"] {
            assert_eq!(
                parsed(&["--failover", "--failover-max-concurrent", value]).map(|_| ()),
                Err("--failover-max-concurrent wants a count of at least 1".to_owned()),
                "{value}"
            );
        }
        for value in ["abc", "18446744073709551616"] {
            assert_eq!(
                parsed(&["--failover", "--failover-silent-ms", value]).map(|_| ()),
                Err("--failover-silent-ms wants whole milliseconds".to_owned()),
                "{value}"
            );
        }
    }

    /// PR #119 review: `--help` prints `USAGE`, and it lists every command a `--faults` file
    /// takes; each listed command is one the fault file accepts. Unit.
    /// Cut: drop the fault-command line from `USAGE`; red.
    #[config_log::retcd_test]
    fn own81_help_lists_the_fault_commands() {
        assert_eq!(parsed(&["--help"]).map(|_| ()), Err(String::new()));
        let commands = [
            "hold N",
            "hold all",
            "drop-request N",
            "drop-reply N",
            "cut-control",
            "mute",
        ];
        for command in commands {
            assert!(
                super::USAGE.contains(command),
                "{command}: {}",
                super::USAGE
            );
            let (_, refused) = rdb_api::net::faults::parse(&command.replace('N', "2"));
            assert!(refused.is_empty(), "{command}: {refused:?}");
        }
    }

    /// own84_10 (#84 review): the watcher's transitions. A partition is reported held once, then
    /// lapsed at the first poll `still_owner` reads false, then held again on reacquire, with
    /// the same token after a self-replace or a higher one after a promote. Unit.
    #[test]
    fn own84_10_the_watcher_reports_held_lapsed_and_reacquired() {
        use super::{poll_ownership, OwnershipSeen};
        use rdb_api::{Generation, NodeId, OwnerEpoch, OwnerToken, Ownership, PartitionId};
        use std::cell::Cell;
        use std::collections::BTreeMap;
        use std::time::Duration;

        let p = PartitionId(1);
        let token = |g, e| OwnerToken {
            generation: Generation(g),
            owner_epoch: OwnerEpoch(e),
        };
        let owned = |t| Ownership {
            node: NodeId(1),
            partition: p,
            token: t,
            remaining: Duration::from_millis(5),
        };
        // What the node owns now; `still_owner` is true for exactly that token.
        let now: Cell<Option<OwnerToken>> = Cell::new(None);
        let mut held = BTreeMap::new();
        let mut poll = || {
            poll_ownership(
                &mut held,
                1,
                |q| now.get().filter(|_| q == p).map(owned),
                |q, t| q == p && now.get() == Some(t),
            )
        };
        assert_eq!(poll(), [], "nothing owned");
        now.set(Some(token(1, 1)));
        assert_eq!(poll(), [OwnershipSeen::Held(owned(token(1, 1)))]);
        assert_eq!(poll(), [], "held: no repeat");
        now.set(None);
        assert_eq!(poll(), [OwnershipSeen::Lapsed(p, token(1, 1))]);
        assert_eq!(poll(), [], "lapsed: no repeat");
        now.set(Some(token(1, 1)));
        assert_eq!(
            poll(),
            [OwnershipSeen::Held(owned(token(1, 1)))],
            "the same token again"
        );
        now.set(Some(token(2, 2)));
        assert_eq!(
            poll(),
            [
                OwnershipSeen::Lapsed(p, token(1, 1)),
                OwnershipSeen::Held(owned(token(2, 2)))
            ],
            "a higher token in one poll: the old lapses first"
        );
    }

    /// own84_10b: the watcher thread ends once `done` is set, so `serve`'s scope joins it at
    /// shutdown; within a few polls. Unit (~20 ms).
    #[test]
    fn own84_10b_the_watcher_stops_when_done_and_is_joined() {
        use super::{watch_ownership_with, OWNERSHIP_POLL};
        use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
        use std::time::Instant;

        let done = AtomicBool::new(false);
        let polls = AtomicU32::new(0);
        let started = Instant::now();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                watch_ownership_with(
                    &done,
                    || {
                        polls.fetch_add(1, Ordering::Relaxed);
                        4
                    },
                    |_| None,
                    |_, _| false,
                    |seen| panic!("nothing is owned: {seen:?}"),
                );
            });
            while polls.load(Ordering::Relaxed) < 2 {
                std::thread::yield_now();
            }
            done.store(true, Ordering::Relaxed);
        });
        assert!(
            started.elapsed() < OWNERSHIP_POLL * 50,
            "joined after {:?}",
            started.elapsed()
        );
    }

    /// V10 (#81), PR #171 review: `run`, the node process's whole `main` after logging starts,
    /// exits 75 (`EX_TEMPFAIL`) when its serve asks for a restart, whichever reason it gave; a
    /// stop file exits 0 and a failure 1. The row runs `run` itself, with a serve that ends as
    /// told, so what `run` returns for each outcome is what the process exits with. The serve
    /// really ending `Restart` is `own81_v10_frozen_returning_node_exits_after_the_sweep`'s.
    /// Unit (no store, no sockets).
    /// Cut: `run` returns 0 for a `Restart` serve; red at the first reason.
    #[config_log::retcd_test]
    fn own81_v10_a_restart_wanted_exits_75() {
        use std::process::ExitCode;
        let run = |ended: Result<Served, String>| {
            let mut served = 0;
            let code = super::run(
                ["--node", "1"].iter().map(|a| (*a).to_owned()),
                |_| Ok(None),
                |args, _| {
                    served += 1;
                    assert_eq!(args.node, Some(1), "run serves the node it parsed");
                    ended
                },
            );
            assert_eq!(served, 1, "run served once");
            code
        };
        for reason in [RestartReason::ReplaceStopped, RestartReason::HostFaulted] {
            assert_eq!(
                run(Ok(Served::Restart(reason))),
                ExitCode::from(75),
                "{reason}"
            );
        }
        assert_eq!(run(Ok(Served::Stopped)), ExitCode::SUCCESS);
        assert_eq!(run(Err("a failure".to_owned())), ExitCode::from(1));
    }

    /// #81 D1 (ADR-0019: off by default): a node process started without `--failover` runs no
    /// planner (`DbConfig::failover` is `None`); with it, the library's defaults. Unit.
    /// Cut: `failover` answers `Some` whatever `--failover` says; the first assertion is red.
    #[config_log::retcd_test]
    fn own81_d1_no_failover_flag_runs_no_planner() {
        let plain = parsed(&["--node", "1"]).expect("parses");
        assert_eq!(super::failover(&plain), None, "no --failover, no planner");
        let on = parsed(&["--node", "1", "--failover"]).expect("parses");
        assert_eq!(super::failover(&on), Some(rdb_api::Failover::default()));
    }

    /// The node owning each partition, and each one's members, read from `partitions/`.
    fn placement(
        rt: &tokio::runtime::Runtime,
        store: &dyn ConfigStore,
    ) -> Vec<(
        rdb_core::authority::partition::PartitionRecord,
        Vec<rdb_core::authority::partition::RecordMember>,
    )> {
        let prefix = rdb_core::contracts::control::ControlPrefix::Partitions.encode();
        rt.block_on(store.list(config_core::ListRequest {
            prefix: bytes::Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        }))
        .expect("the store answers")
        .records
        .iter()
        .map(|found| {
            (
                rdb_core::authority::partition::PartitionRecord::decode(&found.value)
                    .expect("a partition record"),
                rdb_core::authority::partition::RecordMember::decode_all(&found.value)
                    .unwrap_or_default(),
            )
        })
        .collect()
    }

    /// V10 (#81) over three node processes' start paths (`serve_on_ready`, one thread each) on
    /// a declared database of 12 partitions, each node with `--failover
    /// --failover-max-concurrent 1`, so the planner's sweep moves node 1's four partitions one
    /// at a time. Node 1 is stopped as `kill -STOP` stops it (its fault file: `mute` and
    /// `cut-control`), resumed once the first of its partitions has moved, and its serve
    /// returns `Restart` (exit 75) only once no `partitions/` record names it owner. Then it is
    /// started again on its folder, as a supervisor would, at a new boot: it says ready, the
    /// owners take it back as a regular copy of every partition, and at the stop file it
    /// ends `Stopped`, not `Restart` again.
    /// Cluster row (~30 s): one grant lapse (6 s), four promotes, the 5 s hold, a re-add. Every
    /// node thread runs in this row's span, so the nodes log into this row's own file: each of
    /// the four serves says `rdb_node_listening`, and each node's ownership watcher says
    /// `ownership_held` (PR #171 review). The library's own node and sync threads still log
    /// untagged from here: they enter the opener's span only under rdb-api's own `cfg(test)`.
    /// No cut is red here for the owns-none guard: the four promotes took 1.2 s in all
    /// (measured), well inside the 5 s hold, so the guard is pinned by the `restart_due` units
    /// (`rdb_api::db::restart`).
    /// Cut: `Db::restart_wanted` always answers `None` (no V10); node 1 never returns, red at
    /// "node 1's serve returns".
    #[config_log::retcd_test]
    fn own81_v10_frozen_returning_node_exits_after_the_sweep() {
        const METHOD: &str = "own81_v10_frozen_returning_node_exits_after_the_sweep";
        use rdb_api::NodeId;
        use rdb_core::contracts::ids::ReplicaRole;
        use std::time::{Duration, Instant};
        let scale = config_testkit::poll::deadline_scale();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        let root = config_testkit::fs::temp_dir();
        let certs = root.path().join("certs");
        let labels: Vec<rdb_api::net::wire::Label> = (1..=3)
            .map(|n| rdb_api::net::wire::Label::Node(NodeId(n)))
            .collect();
        rdb_api::net::certs::generate(&certs, &labels).expect("certificates");
        let mut handed: Vec<Option<tokio::net::TcpListener>> = Vec::new();
        let addrs: Vec<String> = (1..=3u8)
            .map(|n| {
                let host = std::net::Ipv4Addr::new(127, 0, 0, n);
                let (listener, addr) =
                    rt.block_on(config_testkit::ports::ephemeral_listener_on(host));
                handed.push(Some(listener));
                addr.to_string()
            })
            .collect();
        let peers: Vec<String> = addrs
            .iter()
            .zip(1..)
            .map(|(addr, n)| format!("{n}={addr}"))
            .collect();
        let peers = peers.join(",");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        declare_on(
            &parsed(&["--declare", "--peers", &peers, "--partitions", "12"]).expect("parses"),
            &store,
            &rt,
        )
        .expect("declared");
        let stop = root.path().join("stop");
        let path = |p: &std::path::Path| p.display().to_string();
        let faults: Vec<std::path::PathBuf> = (1..=3)
            .map(|n| root.path().join(format!("faults-{n}.txt")))
            .collect();
        for file in &faults {
            std::fs::write(file, b"").expect("a fault file");
        }
        let args_of = |n: usize| {
            parsed(&[
                "--node",
                &n.to_string(),
                "--dir",
                &path(&root.path().join(format!("node-{n}"))),
                "--listen",
                &addrs[n - 1],
                "--peers",
                &peers,
                "--control",
                "http://127.0.0.1:1",
                "--cert-dir",
                &path(&certs),
                "--faults",
                &path(&faults[n - 1]),
                "--stop-file",
                &path(&stop),
                "--ready-secs",
                "30",
                "--failover",
                "--failover-max-concurrent",
                "1",
            ])
            .expect("parses")
        };
        let ready = std::sync::Mutex::new(Vec::new());
        let on_ready = |node: NodeId| ready.lock().expect("the ready list").push(node.0);
        let ready_count = |n: u32| {
            ready
                .lock()
                .expect("the ready list")
                .iter()
                .filter(|r| **r == n)
                .count()
        };
        let wait = |what: &str, within: Duration, done: &mut dyn FnMut() -> bool| {
            let deadline = Instant::now() + within * scale;
            while !done() {
                assert!(Instant::now() < deadline, "not within {within:?}: {what}");
                std::thread::sleep(Duration::from_millis(50));
            }
        };
        let owned_by = |n: u32| -> Vec<u32> {
            placement(&rt, &*store)
                .iter()
                .filter(|(record, _)| record.owner == NodeId(n))
                .map(|(record, _)| record.partition.0)
                .collect()
        };
        /// Writes the stop file when dropped, so a failed assertion inside the scope ends every
        /// node thread and the scope's join returns the panic instead of waiting forever.
        struct StopOnDrop<'a>(&'a std::path::Path);
        impl Drop for StopOnDrop<'_> {
            fn drop(&mut self) {
                let _ = std::fs::write(self.0, b"");
            }
        }
        let span = tracing::Span::current();
        std::thread::scope(|scope| {
            let _stop_on_panic = StopOnDrop(&stop);
            let mut running: Vec<_> = (1..=3usize)
                .map(|n| {
                    let args = args_of(n);
                    let store = Arc::clone(&store);
                    let (rt, on_ready, span) = (&rt, &on_ready, &span);
                    let bound = handed[n - 1].take();
                    scope.spawn(move || {
                        span.in_scope(|| serve_on_ready(&args, rt, store, bound, on_ready))
                    })
                })
                .collect();
            wait("every node is ready", Duration::from_secs(30), &mut || {
                (1..=3).all(|n| ready_count(n) == 1)
            });
            let moving = owned_by(1);
            assert_eq!(moving.len(), 4, "node 1 owns a third of 12");

            std::fs::write(&faults[0], b"mute\ncut-control\n").expect("stop node 1");
            wait(
                "the first partition of node 1 moves",
                Duration::from_secs(60),
                &mut || owned_by(1).len() < moving.len(),
            );
            std::fs::write(&faults[0], b"").expect("resume node 1");
            let first = running.remove(0);
            wait(
                "node 1's serve returns",
                Duration::from_secs(90),
                &mut || first.is_finished(),
            );
            let still = owned_by(1);
            let served = first.join().expect("node 1's thread");
            assert!(
                matches!(
                    served,
                    Ok(Served::Restart(
                        RestartReason::ReplaceStopped | RestartReason::HostFaulted
                    ))
                ),
                "node 1 asks for a restart: {served:?}"
            );
            assert_eq!(
                still,
                Vec::<u32>::new(),
                "node 1 owns none when it asked for a restart"
            );
            println!("own81_v10 node 1 served {served:?}");

            // The supervisor's part: start node 1 again on its folder, at a new boot. The ended
            // serve ran `Net::shutdown`, so nothing answers the peers at the old boot (#81).
            // Its listener closes as its tasks wind down, so the address is bound again with a
            // bounded retry, as `Rig::restart` does, and handed to the new serve.
            let rebound = rt.block_on(async {
                let deadline = Instant::now() + Duration::from_secs(5) * scale;
                loop {
                    match tokio::net::TcpListener::bind(&addrs[0]).await {
                        Ok(listener) => break listener,
                        Err(e) if Instant::now() < deadline => {
                            tracing::debug!(error = %e, "own81_restart_bind_retry");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        Err(e) => panic!("node 1's address again: {e}"),
                    }
                }
            });
            let again = {
                let args = args_of(1);
                let store = Arc::clone(&store);
                let (rt, on_ready, span) = (&rt, &on_ready, &span);
                scope.spawn(move || {
                    span.in_scope(|| serve_on_ready(&args, rt, store, Some(rebound), on_ready))
                })
            };
            wait(
                "node 1 is ready again",
                Duration::from_secs(60),
                &mut || ready_count(1) == 2,
            );
            wait(
                "node 1 is a regular copy of every partition again",
                Duration::from_secs(90),
                &mut || {
                    placement(&rt, &*store).iter().all(|(_, members)| {
                        members.iter().any(|member| {
                            member.node == NodeId(1) && member.role == ReplicaRole::RegularSecondary
                        })
                    })
                },
            );
            std::fs::write(&stop, b"").expect("the stop file");
            assert_eq!(
                again.join().expect("node 1's second thread"),
                Ok(Served::Stopped),
                "node 1's new boot ends at the stop file, not with a second restart"
            );
            for (handle, n) in running.into_iter().zip(2..) {
                assert_eq!(
                    handle.join().expect("a node thread"),
                    Ok(Served::Stopped),
                    "node {n} ends cleanly"
                );
            }
        });
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let nodes_saying = |message: &str| -> Vec<u64> {
            let mut nodes: Vec<u64> = lines
                .iter()
                .filter(|line| line["@m"] == message)
                .filter_map(|line| line["node"].as_u64())
                .collect();
            nodes.sort_unstable();
            nodes
        };
        assert_eq!(
            nodes_saying("rdb_node_listening"),
            [1, 1, 2, 3],
            "each serve logs into this row's file"
        );
        let mut held = nodes_saying("ownership_held");
        held.dedup();
        assert_eq!(
            held,
            [1, 2, 3],
            "each node's ownership watcher logs into this row's file"
        );
    }

    /// The `--peers` value of three nodes on three private hosts (#80 S2).
    const REMOTE: &str = "1=10.9.9.1:7100,2=10.9.9.2:7100,3=10.9.9.3:7100";

    /// A control store that reports mutual TLS, as a `GrpcClient` under `--control-tls` does
    /// (#80 S2); every call goes to `inner`.
    struct TlsReported(Arc<dyn ConfigStore>);

    #[async_trait::async_trait]
    impl ConfigStore for TlsReported {
        async fn get(
            &self,
            request: config_core::GetRequest,
        ) -> Result<config_core::GetResponse, config_core::ConfigError> {
            self.0.get(request).await
        }
        async fn list(
            &self,
            request: config_core::ListRequest,
        ) -> Result<config_core::ListResponse, config_core::ConfigError> {
            self.0.list(request).await
        }
        async fn put(
            &self,
            request: config_core::PutRequest,
        ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
            self.0.put(request).await
        }
        async fn delete(
            &self,
            request: config_core::DeleteRequest,
        ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
            self.0.delete(request).await
        }
        fn capabilities(&self) -> config_core::Capabilities {
            config_core::Capabilities {
                transport_security: config_core::TransportSecurity::MutualTls,
                ..self.0.capabilities()
            }
        }
        async fn watch(
            &self,
            request: config_core::WatchRequest,
        ) -> Result<config_core::WatchStream, config_core::ConfigError> {
            self.0.watch(request).await
        }
        async fn watch_leader(
            &self,
            request: config_core::WatchRequest,
        ) -> Result<config_core::WatchStream, config_core::ConfigError> {
            self.0.watch_leader(request).await
        }
    }

    /// #80 S2 SC7 (ruling 1, AC2): `--declare` without `--control-tls` of peers on other
    /// machines is refused, naming node 1's address and `--control-tls`, with nothing written;
    /// the same peers over a TLS store declare. And a node process of that database, started
    /// over a plaintext store, is refused at its start check, before it writes its boot or
    /// binds. Unit (an in-memory store).
    /// Cut 1: `declare` skips `control_tls_refusal`: the plaintext declaration is written, red.
    /// Cut 2: `start_check_on` skips it: node 1's start check passes, red.
    #[config_log::retcd_test]
    fn s2_rdb_node_declare_without_control_tls_is_refused_for_remote_peers() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let args = parsed(&["--declare", "--peers", REMOTE]).expect("parses");
        let memory: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let refused = declare_on(&args, &memory, &rt);
        assert!(
            matches!(&refused, Err(text) if text.contains("node 1's address 10.9.9.1:7100")
                && text.contains("not TLS") && text.contains("--control-tls")
                && text.contains("nothing was written")),
            "{refused:?}"
        );
        let listed = rt
            .block_on(memory.list(config_core::ListRequest {
                prefix: bytes::Bytes::from_static(b""),
                max_items: 0,
                max_bytes: 0,
            }))
            .expect("the store answers");
        assert!(listed.records.is_empty(), "nothing written");

        let tls: Arc<dyn ConfigStore> = Arc::new(TlsReported(Arc::clone(&memory)));
        let line = declare_on(&args, &tls, &rt).expect("over TLS the peers declare");
        assert!(line.starts_with("rdb_node declared "), "{line}");

        let node = parsed(&[
            "--node",
            "1",
            "--listen",
            "10.9.9.1:7100",
            "--peers",
            REMOTE,
        ])
        .expect("parses");
        let refused =
            super::start_check_on(&node, rdb_api::NodeId(1), "10.9.9.1:7100", &*memory, &rt);
        assert!(
            matches!(&refused, Err(text) if text.contains("not TLS")
                && text.contains("nothing was started")),
            "{refused:?}"
        );
        super::start_check_on(&node, rdb_api::NodeId(1), "10.9.9.1:7100", &*tls, &rt)
            .expect("over TLS node 1's start check passes");
    }

    /// #80 S2: `--control-cluster` and `--control-voters` are read by `--gen-certs` only, and
    /// together; `--control-tls` parses for every mode. Unit.
    #[test]
    fn s2_control_cert_flags_parse_only_with_gen_certs_and_together() {
        const CID: &str = "80808080808080808080808080808080";
        for (argv, refused) in [
            (
                &["--control-cluster", CID, "--control-voters", "1=10.0.0.2"][..],
                "read only by --gen-certs",
            ),
            (
                &["--gen-certs", "--control-cluster", CID][..],
                "go together",
            ),
            (
                &["--gen-certs", "--control-voters", "1=10.0.0.2"][..],
                "go together",
            ),
            (
                &["--gen-certs", "--control-cluster", "80"][..],
                "is not a cluster id",
            ),
            (
                &[
                    "--gen-certs",
                    "--control-cluster",
                    CID,
                    "--control-voters",
                    "1",
                ][..],
                "is not N=HOST",
            ),
            (
                &[
                    "--gen-certs",
                    "--control-cluster",
                    CID,
                    "--control-voters",
                    "1=a,1=b",
                ][..],
                "names voter 1 twice",
            ),
        ] {
            assert!(
                matches!(parsed(argv), Err(text) if text.contains(refused)),
                "{argv:?}: {:?}",
                parsed(argv).map(|_| ())
            );
        }
        let args = parsed(&["--declare", "--peers", PEERS, "--control-tls", "t"]).expect("parses");
        assert_eq!(args.control_tls, Some(std::path::PathBuf::from("t")));
    }

    /// #80 S2 (lead decision: dev certificates come from `--gen-certs`): `--gen-certs` with
    /// `--control-cluster` and `--control-voters` exits 0 and writes `control/`, which
    /// `certs::load_control` reads as client `rdb` of that cluster, with one certificate per
    /// voter. Unit.
    #[config_log::retcd_test]
    fn s2_gen_certs_writes_the_control_folder() {
        const CID: &str = "80808080808080808080808080808080";
        let root = config_testkit::fs::temp_dir();
        let dir = root.path().join("certs");
        let argv: Vec<String> = [
            "--gen-certs",
            "--cert-dir",
            &dir.display().to_string(),
            "--nodes",
            "5",
            "--apps",
            "admin",
            "--control-cluster",
            CID,
            "--control-voters",
            "1=172.30.80.2,2=172.30.80.3,3=172.30.80.4",
        ]
        .iter()
        .map(|a| (*a).to_owned())
        .collect();
        let code = super::run(argv.into_iter(), |_| Ok(None), super::serve);
        assert_eq!(code, std::process::ExitCode::SUCCESS);
        let control = dir.join(rdb_api::net::certs::CONTROL_DIR);
        let loaded = rdb_api::net::certs::load_control(&control).expect("control/ loads");
        assert_eq!(loaded.cluster.to_string(), CID);
        assert_eq!(loaded.principal, "rdb");
        for n in 1..=3 {
            assert!(control.join(format!("voter-{n}.pem")).exists(), "voter {n}");
            assert!(control.join(format!("voter-{n}.key")).exists(), "voter {n}");
        }
        assert!(dir.join("node-5.pem").exists() && dir.join("app-admin.pem").exists());
    }

    /// A 3-voter mutual-TLS rEtcd of cluster `cluster` granting only the `rdb` client, a
    /// `--control-tls` folder of its fixture's client certificate, and its client endpoints
    /// followers first, so a client that cannot follow a leader hint fails (#80 S2 review r1).
    fn tls_control(
        rt: &tokio::runtime::Runtime,
        cluster: config_core::ClusterId,
        seed: u64,
    ) -> (
        config_testkit::Cluster,
        Vec<String>,
        std::path::PathBuf,
        Box<dyn std::any::Any>,
    ) {
        let voters = rt.block_on(
            config_testkit::Cluster::builder()
                .nodes(3)
                .cluster_id(cluster)
                .storage(config_testkit::cluster::StorageKind::Ephemeral)
                .mutual_tls(seed)
                .authz(config_testkit::cluster::AuthzKind::Static(
                    "[[grant]]\nprincipal = \"rdb\"\nprefix = \"\"\naccess = [\"read\", \"write\"]\n"
                        .to_owned(),
                ))
                .start(),
        );
        let leader = rt.block_on(voters.leader());
        let dir = config_testkit::fs::temp_dir();
        let pair = voters
            .fixture()
            .issue(config_testkit::CertProfile::client("rdb"));
        for (name, text) in [
            ("ca.pem", &pair.ca_pem),
            ("client.pem", &pair.cert_pem),
            ("client.key", &pair.key_pem),
        ] {
            std::fs::write(dir.path().join(name), text).expect("a control file");
        }
        let mut endpoints: Vec<String> = voters
            .followers()
            .into_iter()
            .map(|id| voters.client_endpoint(id))
            .collect();
        endpoints.push(voters.client_endpoint(leader));
        let path = dir.path().to_path_buf();
        (voters, endpoints, path, Box::new(dir))
    }

    /// #80 S2 review r1: `control(args)` under `--control-tls` reaches a mutual-TLS rEtcd
    /// whose cluster is not the default, starting at a follower: a create-only put and its
    /// exact read back succeed, and the store reports mutual TLS. Cluster row (~3 s): a
    /// 3-voter TLS rEtcd, the only way to prove the constructor's TLS and cluster wiring.
    /// Cut 1: `control` builds a plaintext client under `--control-tls`: it reports
    /// `Insecure`, red at the transport-security assertion. Cut 2: `control` drops
    /// `.with_cluster_id`: the put fails `NotLeader` with the hint unfollowed, red (both
    /// observed 2026-11-19).
    #[config_log::retcd_test]
    fn s2_control_constructor_puts_and_reads_back_over_tls_from_a_follower() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let cluster = config_core::ClusterId::from_bytes([0x81; 16]);
        let (voters, endpoints, path, _dir) = tls_control(&rt, cluster, 811);
        let tls = path.display().to_string();
        let args = parsed(&[
            "--declare",
            "--peers",
            PEERS,
            "--control",
            &endpoints.join(","),
            "--control-tls",
            &tls,
        ])
        .expect("parses");
        let store = {
            let _entered = rt.enter();
            super::control(&args).expect("the control client")
        };
        assert_eq!(
            store.capabilities().transport_security,
            config_core::TransportSecurity::MutualTls
        );
        let key = bytes::Bytes::from_static(b"s2-sentinel/rdb_node-control");
        let value = bytes::Bytes::from_static(b"over tls from a follower");
        rt.block_on(store.put(config_core::PutRequest {
            key: key.clone(),
            value: value.clone(),
            expected_mod_revision: Some(0),
            ..config_core::PutRequest::default()
        }))
        .expect("a create-only put over TLS");
        let read = rt
            .block_on(store.get(config_core::GetRequest { key: key.clone() }))
            .expect("the read back");
        assert_eq!(
            read.record.map(|record| (record.key, record.value)),
            Some((key, value))
        );
        drop(store);
        rt.block_on(voters.shutdown());
    }

    /// #80 S2 review r1: the node process's own start, `serve_on`, refuses a plaintext store
    /// for a database declared on other machines before it writes its boot or binds. The
    /// bind is observed on its own: `--listen` is the wildcard on a port this row already
    /// holds, so a start that bound first would fail with the bind error, not the TLS one.
    /// Over a TLS store the same start passes the check, writes its boot, and then meets the
    /// held port: the order is check, boot, bind. Unit (an in-memory store, one held port).
    /// Cut: `start_check_on` skips `control_tls_refusal`: the plaintext start writes its boot
    /// and fails at the bind instead, red.
    #[config_log::retcd_test]
    fn s2_serve_refuses_plaintext_control_before_boot_or_bind() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let held = std::net::TcpListener::bind("0.0.0.0:0").expect("a free port");
        let port = held.local_addr().expect("bound").port();
        let peers = format!("1=10.9.9.1:{port},2=10.9.9.2:{port},3=10.9.9.3:{port}");
        let memory: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let tls: Arc<dyn ConfigStore> = Arc::new(TlsReported(Arc::clone(&memory)));
        let declare = parsed(&["--declare", "--peers", &peers]).expect("parses");
        declare_on(&declare, &tls, &rt).expect("declared over TLS");
        let root = config_testkit::fs::temp_dir();
        let certs_dir = root.path().join("certs");
        rdb_api::net::certs::generate(
            &certs_dir,
            &(1..=3)
                .map(|n| rdb_api::net::wire::Label::Node(rdb_api::NodeId(n)))
                .collect::<Vec<_>>(),
        )
        .expect("certs");
        let path = |p: &std::path::Path| p.display().to_string();
        let start = |dir: &std::path::Path, store: &Arc<dyn ConfigStore>| {
            let args = parsed(&[
                "--node",
                "1",
                "--dir",
                &path(dir),
                "--listen",
                &format!("0.0.0.0:{port}"),
                "--peers",
                &peers,
                "--control",
                "127.0.0.1:1",
                "--cert-dir",
                &path(&certs_dir),
            ])
            .expect("parses");
            super::serve_on(&args, &rt, Arc::clone(store))
        };
        let plain_dir = root.path().join("plain");
        let refused = start(&plain_dir, &memory);
        assert!(
            matches!(&refused, Err(text) if text.contains("node 1's address 10.9.9.1")
                && text.contains("not TLS") && text.contains("nothing was started")),
            "{refused:?}"
        );
        assert!(!plain_dir.exists(), "no boot was written");
        let tls_dir = root.path().join("tls");
        let bind = start(&tls_dir, &tls);
        assert!(
            matches!(&bind, Err(text) if text.starts_with(&format!("--listen 0.0.0.0:{port}"))
                && !text.contains("not TLS")),
            "over TLS the start reaches the held port: {bind:?}"
        );
        assert!(
            tls_dir.exists(),
            "over TLS the boot is written before the bind"
        );
        drop(held);
    }
}
