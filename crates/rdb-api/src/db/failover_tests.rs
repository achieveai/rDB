//! #81 C1: the planner over real links. Each row runs three node "processes" in this test
//! process, as `clock_guard_tests` does: a `Db`, a `Net` and a `Links` each, real TLS links,
//! one shared control store that each process reaches through its own [`CutStore`].
//!
//! A process is stopped, as `kill -STOP` stops one, with the fault file's `mute` and
//! `cut-control` together: its links read nothing and write nothing, probes included, and its
//! grant stops renewing. A stop is a pause, as `resume` shows. An exit is `Net::shutdown` (or
//! dropping the last `Arc<Net>`): it ends every link, accepted ones too, and stops listening
//! (`own81_net_shutdown_leaves_no_link_answering`); [`Rig::restart`] uses it.

use super::reopen_tests::{config, fast, open_app, runtime};
use super::*;
use crate::net::faults::{CutStore, Faults};
use crate::net::{certs, wire::Label, Net, NetConfig, PROTOCOL_VERSION};
use crate::planner::Failover;
use crate::test_store::{ClockOnPut, CutCalls, HoldAt, PrefixCut, PutHold};
use rdb_core::authority::partition::PartitionLifecycle;

/// One node process.
struct Proc {
    db: Db,
    net: Arc<Net>,
    faults: Arc<Faults>,
    boot: BootId,
}

struct Rig {
    rt: tokio::runtime::Runtime,
    store: Arc<dyn ConfigStore>,
    _dirs: Vec<Box<dyn std::any::Any>>,
    cert_dir: std::path::PathBuf,
    /// The database folder every process opens (`nodes/{n}` each), for [`Rig::restart`].
    dir: std::path::PathBuf,
    addrs: BTreeMap<NodeId, String>,
    procs: Vec<Proc>,
    partitions: u32,
    /// The cut of each node [`opened_cut`] cut: its own `grants/{node}` writes, installed
    /// before that node opened.
    grant_cuts: BTreeMap<NodeId, Arc<PrefixCut>>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for proc in &mut self.procs {
            proc.db.shutdown();
        }
    }
}

const APP: &str = "own81-app";

/// A grant short enough for a row to wait out.
fn budgets() -> Budgets {
    Budgets {
        grant_millis: 3_000,
        ..fast()
    }
}

/// `n` of the rig's grant lifetimes ([`budgets`]), the unit a row's deadline is counted in:
/// README's "No fixed sleeps" asks for a multiple of the configured timeout, not a literal.
/// [`until`] and [`logged_at_least_within`] stretch it by `RETCD_TEST_DEADLINE_SCALE`.
fn lifetimes(n: u32) -> Duration {
    Duration::from_millis(budgets().grant_millis) * n
}

/// The planner the rows run: polls often, so the measured time is the rule's, not the poll's.
fn failover() -> Failover {
    Failover {
        poll: Duration::from_millis(200),
        ..Failover::default()
    }
}

/// A database of `partitions` partitions, formed by one all-nodes open, written once, closed.
fn formed(rt: &tokio::runtime::Runtime, dir: &Path, store: &Arc<dyn ConfigStore>, partitions: u32) {
    let mut db = rt
        .block_on(Db::open_with(
            DbConfig {
                partitions: Some(partitions),
                ..config(dir)
            },
            Arc::clone(store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the forming open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("the forming open is ready");
    db.shutdown();
}

/// Three node processes over a database of `partitions`, each running `failover`.
fn rig(partitions: u32, failover: Option<Failover>) -> Rig {
    rig_with(partitions, [failover; 3])
}

/// [`rig`], with node `n`'s process running `failovers[n - 1]`.
fn rig_with(partitions: u32, failovers: [Option<Failover>; 3]) -> Rig {
    let rig = opened(partitions, failovers);
    rig.wait_ready();
    rig.wait_for_owner_grants();
    rig
}

/// [`rig_with`] up to every process open, before any is ready: no node has recovered its
/// partitions, so `grants/{node}` still holds the forming open's record, at that open's boot.
fn opened(partitions: u32, failovers: [Option<Failover>; 3]) -> Rig {
    opened_cut(partitions, failovers, None)
}

/// [`opened`], with `cut`'s own `grants/{cut}` writes cut ([`PrefixCut`]) from before it
/// opens, so the node can take no grant of its own at its boot, however long the rest of the
/// setup takes, until the row lifts it; every other call passes, so it opens (PR #119 review).
/// Ordinary callers pass `None` and start without faults.
fn opened_cut(partitions: u32, failovers: [Option<Failover>; 3], cut: Option<NodeId>) -> Rig {
    let mut grant_cuts = BTreeMap::new();
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let cert_dir = config_testkit::fs::temp_dir();
    let labels: Vec<Label> = ALL_NODES
        .iter()
        .map(|node| Label::Node(*node))
        .chain([Label::App(APP.to_owned())])
        .collect();
    certs::generate(cert_dir.path(), &labels).expect("certs");
    formed(&rt, dir.path(), &store, partitions);
    let listeners: Vec<tokio::net::TcpListener> = ALL_NODES
        .iter()
        .map(|_| rt.block_on(config_testkit::ports::ephemeral_listener()).0)
        .collect();
    let addrs: BTreeMap<NodeId, String> = ALL_NODES
        .iter()
        .zip(&listeners)
        .map(|(node, listener)| (*node, listener.local_addr().expect("bound").to_string()))
        .collect();
    let procs = {
        let _entered = rt.enter();
        ALL_NODES
            .iter()
            .zip(listeners)
            .zip(failovers)
            .map(|((node, listener), failover)| {
                let node = *node;
                let grant_cuts = &mut grant_cuts;
                start_proc(
                    &rt,
                    dir.path(),
                    cert_dir.path(),
                    &addrs,
                    &store,
                    node,
                    listener,
                    failover,
                    |control| {
                        if cut != Some(node) {
                            return control;
                        }
                        let grant_cut = PrefixCut::new(
                            control,
                            &ControlKey::Grant(node).encode(),
                            CutCalls::Writes,
                        );
                        grant_cuts.insert(node, Arc::clone(&grant_cut));
                        grant_cut
                    },
                )
            })
            .collect()
    };
    let rig = Rig {
        rt,
        store,
        cert_dir: cert_dir.path().to_path_buf(),
        dir: dir.path().to_path_buf(),
        _dirs: vec![Box::new(dir), Box::new(cert_dir)],
        addrs,
        procs,
        partitions,
        grant_cuts,
    };
    rig
}

/// Start node `node`'s process on `listener`, at the next boot of `dir`: its `Net` and its
/// `Db`, the `Db` reaching `store` through a [`CutStore`] on the process's own faults, and
/// `wrap` around that (a row's extra cut).
#[allow(clippy::too_many_arguments)]
fn start_proc(
    rt: &tokio::runtime::Runtime,
    dir: &std::path::Path,
    cert_dir: &std::path::Path,
    addrs: &BTreeMap<NodeId, String>,
    store: &Arc<dyn ConfigStore>,
    node: NodeId,
    listener: tokio::net::TcpListener,
    failover: Option<Failover>,
    wrap: impl FnOnce(Arc<dyn ConfigStore>) -> Arc<dyn ConfigStore>,
) -> Proc {
    let _entered = rt.enter();
    let boot = next_boot(dir).expect("a boot");
    let label = Label::Node(node);
    let links = Links::new();
    let faults = Faults::new();
    let net = Net::start(
        NetConfig {
            tls: certs::load(cert_dir, &label).expect("node certs"),
            label,
            boot,
            peers: ALL_NODES.iter().copied().filter(|n| *n != node).collect(),
            dial: NetConfig::dial_lower(node, addrs),
            protocol_version: PROTOCOL_VERSION,
            admin_apps: BTreeSet::new(),
            faults: Arc::clone(&faults),
        },
        &links,
        Some(listener),
    )
    .expect("the node's net starts");
    let control = wrap(CutStore::new(Arc::clone(store), Arc::clone(&faults)));
    let db = rt
        .block_on(Db::open_with(
            DbConfig {
                local: vec![node],
                links: Some(links),
                boot: Some(boot),
                failover,
                ..config(dir)
            },
            control,
            rt.handle().clone(),
            budgets(),
        ))
        .unwrap_or_else(|e| panic!("node {}'s process opens: {e}", node.0));
    Proc {
        db,
        net,
        faults,
        boot,
    }
}

impl Rig {
    /// Stop process `i` for good, as a crash and a supervisor's start would, and start node
    /// `i + 1` again on its folder and address at a new boot, running `failover`.
    fn restart(&mut self, i: usize, failover: Option<Failover>) {
        let node = ALL_NODES[i];
        // `Db::shutdown` leaves the links up; `Net::shutdown` ends them, as the process's exit
        // would, so the peers meet the new boot when it dials or is dialled.
        self.procs[i].db.shutdown();
        self.procs[i].net.shutdown();
        drop(self.procs.remove(i));
        let listener = self.rt.block_on(async {
            let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(5));
            loop {
                match tokio::net::TcpListener::bind(&self.addrs[&node]).await {
                    Ok(listener) => break listener,
                    Err(e) if Instant::now() < deadline => {
                        tracing::debug!(error = %e, "own81_restart_bind_retry");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(e) => panic!("node {}'s address again: {e}", node.0),
                }
            }
        });
        let proc = start_proc(
            &self.rt,
            &self.dir,
            &self.cert_dir,
            &self.addrs,
            &self.store,
            node,
            listener,
            failover,
            |control| control,
        );
        self.procs.insert(i, proc);
        self.procs[i]
            .db
            .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
            .expect("the restarted process is ready");
    }

    /// The members `partitions/{partition}` pins.
    fn members(&self, partition: u32) -> Vec<rdb_core::authority::partition::RecordMember> {
        let key = ControlKey::Partition(PartitionId(partition)).encode();
        self.rt
            .block_on(self.store.get(config_core::GetRequest {
                key: Bytes::from(key),
            }))
            .expect("the store answers")
            .record
            .and_then(|found| {
                rdb_core::authority::partition::RecordMember::decode_all(&found.value)
            })
            .unwrap_or_default()
    }
}

impl Rig {
    fn wait_ready(&self) {
        for proc in &self.procs {
            proc.db
                .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
                .expect("every process is ready");
        }
    }

    /// `wait_ready` can answer before an owner has taken its grant: `grants/{node}` still holds
    /// the forming open's record, at that open's boot, until the node's recovery replaces it.
    /// Wait until each process that owns a partition holds a grant at its own boot. A node
    /// that owns none takes no grant.
    fn wait_for_owner_grants(&self) {
        until(
            "every owner holds a grant at its own boot",
            Duration::from_secs(30),
            || {
                let grants = self.grants();
                self.procs.iter().zip(ALL_NODES).all(|(proc, node)| {
                    self.owned_by(node).is_empty()
                        || grants
                            .get(&node)
                            .is_some_and(|grant| grant.boot == proc.boot && !grant.frozen)
                })
            },
        );
    }

    /// Stop process `i` as `kill -STOP` would, from outside: links silent, grant not renewed.
    fn stop(&self, i: usize) {
        self.procs[i].faults.apply("mute\ncut-control\n");
    }

    /// `kill -CONT`.
    fn resume(&self, i: usize) {
        self.procs[i].faults.apply("");
    }

    /// Let `node` write its own grant again ([`opened_cut`]).
    fn lift_grant_cut(&self, node: NodeId) {
        self.grant_cuts[&node].lift();
    }

    /// `planner/grant`, read from the store. Rows wait on the store, never by polling the log:
    /// each `logged` call flushes the test log and reads it whole, and polling it every 20 ms
    /// starved the nodes until their grants ran out.
    fn planner_grant(&self) -> Option<rdb_core::authority::grant::GrantRecord> {
        self.rt
            .block_on(self.store.get(config_core::GetRequest {
                key: Bytes::from(ControlKey::PlannerGrant.encode()),
            }))
            .expect("the store answers")
            .record
            .map(|found| {
                rdb_core::authority::grant::GrantRecord::decode(&found.value).expect("a grant")
            })
    }

    fn grants(&self) -> BTreeMap<NodeId, rdb_core::authority::grant::GrantRecord> {
        self.rt
            .block_on(self.store.list(config_core::ListRequest {
                prefix: Bytes::from_static(ControlPrefix::Grants.encode().as_bytes()),
                max_items: 0,
                max_bytes: 0,
            }))
            .expect("the store answers")
            .records
            .iter()
            .map(|found| {
                let grant =
                    rdb_core::authority::grant::GrantRecord::decode(&found.value).expect("a grant");
                (grant.node, grant)
            })
            .collect()
    }

    fn owner(&self, partition: u32) -> NodeId {
        self.record(partition).owner
    }

    fn record(&self, partition: u32) -> PartitionRecord {
        self.rt
            .block_on(admin::read_partition(&*self.store, PartitionId(partition)))
            .unwrap_or_else(|e| panic!("partitions/{partition}: {e}"))
            .0
    }

    fn owned_by(&self, node: NodeId) -> Vec<u32> {
        (1..=self.partitions)
            .filter(|p| self.owner(*p) == node)
            .collect()
    }

    /// An app-only `Db` linked to `peers`.
    fn app(&self, peers: &[NodeId]) -> (Db, Arc<Net>, Arc<Links>) {
        let _entered = self.rt.enter();
        let label = Label::App(APP.to_owned());
        let links = Links::new();
        let net = Net::start(
            NetConfig {
                tls: certs::load(&self.cert_dir, &label).expect("app certs"),
                label,
                boot: BootId(0),
                peers: peers.iter().copied().collect(),
                dial: peers
                    .iter()
                    .map(|node| (*node, self.addrs[node].clone()))
                    .collect(),
                protocol_version: PROTOCOL_VERSION,
                admin_apps: BTreeSet::new(),
                faults: Faults::new(),
            },
            &links,
            None,
        )
        .expect("the app's net starts");
        self.rt
            .block_on(net.wait_up(peers, crate::host::test_patience(Duration::from_secs(30))))
            .expect("the app reaches its peers");
        let db = open_app(&self.rt, &self.store, &links, ClientId(2));
        (db, net, links)
    }
}

/// An affinity group that maps to `partition` in a `Db` of that many partitions.
fn group_of(db: &Db, partition: u32) -> AffinityId {
    (1..1_000_000)
        .map(AffinityId)
        .find(|group| db.partition_for(*group) == PartitionId(partition))
        .expect("some group maps to every partition")
}

/// Put in `group` until one lands, or panic with the last refusal.
fn put_lands(db: &Db, group: AffinityId, within: Duration) -> PutOk {
    let deadline = Instant::now() + crate::host::test_patience(within);
    loop {
        match db.put_in(group, b"after", b"1", None) {
            Ok(put) => return put,
            Err(refused) => assert!(
                Instant::now() < deadline,
                "no put landed within {within:?}: {refused:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Wait on a condition a live system will reach: no fixed sleep decides the result.
fn until(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + crate::host::test_patience(within);
    while !done() {
        assert!(Instant::now() < deadline, "not within {within:?}: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `logged`, waited on until it holds `n` lines. Polls every 250 ms: each read flushes the log
/// and reads it whole, and polling faster starves the nodes.
fn logged_at_least(method: &str, message: &str, n: usize) -> Vec<serde_json::Value> {
    logged_at_least_within(method, message, n, Duration::from_secs(10))
}

fn logged_at_least_within(
    method: &str,
    message: &str,
    n: usize,
    within: Duration,
) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + crate::host::test_patience(within);
    loop {
        let lines = logged(method, message);
        if lines.len() >= n || Instant::now() >= deadline {
            return lines;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn logged(method: &str, message: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == message)
        .collect()
}

/// B1's allowance for one wave of promotes (`max_concurrent` at once): two looks, the
/// `Fencing` write, the target's landing and the `Serving` commit, stretched by
/// `RETCD_TEST_DEADLINE_SCALE` as a wait. Measured on this rig: see [`b1_bound`].
const T_PROMOTE: Duration = Duration::from_secs(2);

/// B1's stated upper bound on the time from a stop to the last of `moving` partitions moved,
/// under the rig's [`budgets`] and `failover`:
/// `max(G + eps + delta, silent_after) + 2 * poll + look_within + T_promote * ceil(moving / max_concurrent)`.
/// The stopped owner's grant and the planner grant (when the stopped node held it) both run out
/// by `G + eps + delta` after the stop, since both were renewed before it; the judge also wants
/// `silent_after` of link silence. One poll for a running planner to take `planner/grant`, one
/// for its next sweep; the sweep's look at the dead owner waits `look_within`, the read
/// timeout; then the promotes run `max_concurrent` at a time.
fn b1_bound(moving: usize, failover: &Failover) -> Duration {
    let b = budgets();
    let expired =
        Duration::from_millis(b.grant_millis + b.clock_error_millis + b.dispatch_margin_millis)
            .max(failover.silent_after);
    let waves = u32::try_from(moving.div_ceil(failover.max_concurrent)).expect("few waves");
    let look_within = Timeouts::default().read;
    expired + failover.poll * 2 + look_within + crate::host::test_patience(T_PROMOTE) * waves
}

/// V7 and B1: stop node 1's process for longer than G. Every partition it owned moves to a
/// node that runs, within [`b1_bound`], nothing else moves, and a put lands on each moved
/// partition through an app linked to the two that run. Prints and logs the time from the stop
/// to the last move.
fn stopped_owner_every_partition_moves(method: &str, partitions: u32) {
    let rig = rig(partitions, Some(failover()));
    let dead = NodeId(1);
    let moving = rig.owned_by(dead);
    assert!(!moving.is_empty(), "node 1 owns some partition");
    let staying: Vec<(u32, NodeId)> = (1..=partitions)
        .filter(|p| !moving.contains(p))
        .map(|p| (p, rig.owner(p)))
        .collect();
    until("a planner holds its grant", Duration::from_secs(20), || {
        rig.planner_grant().is_some()
    });

    // A cold start raises no alert (lead ruling 2), and the rig has waited for every owner's
    // own grant, so this is 0 in a passing run. Counted anyway so that only alerts after the
    // stop count against the running nodes.
    let setup_alerts = logged(method, "planner_owner_alive_not_serving").len();
    let bound = b1_bound(moving.len(), &failover());
    let stopped = Instant::now();
    rig.stop(0);
    // The wait outlasts the bound, so a slow move fails on the bound, with its time.
    until(
        "every partition of node 1 moves",
        Duration::from_secs(60).max(bound),
        || moving.iter().all(|p| rig.owner(*p) != dead),
    );
    let took = stopped.elapsed();
    println!(
        "own81_b1 partitions={partitions} moved={} time_to_new_primary_ms={} bound_ms={}",
        moving.len(),
        took.as_millis(),
        bound.as_millis()
    );
    let floor =
        budgets().grant_millis + budgets().clock_error_millis + budgets().dispatch_margin_millis;
    assert!(
        took >= Duration::from_millis(floor),
        "moved {took:?} after the stop, before the grant could have run out ({floor} ms)"
    );
    assert!(
        took <= bound,
        "{} partitions moved {took:?} after the stop, past B1's bound {bound:?}",
        moving.len()
    );

    for p in &moving {
        let record = rig.record(*p);
        assert!(
            matches!(record.owner, NodeId(2 | 3)),
            "partition {p} moved to a running node: {record:?}"
        );
    }
    for (p, owner) in &staying {
        assert_eq!(rig.owner(*p), *owner, "partition {p} did not move");
    }
    let silent = logged(method, "planner_node_silent");
    assert!(
        silent
            .iter()
            .any(|line| line["node"] == 1 && line["reason"] == "silent_and_expired"),
        "{silent:?}"
    );
    // Nodes 2 and 3 kept renewing: no planner ever saw their grants run out.
    let alerts = logged(method, "planner_owner_alive_not_serving");
    assert_eq!(
        alerts.len(),
        setup_alerts,
        "the running nodes stayed healthy: {alerts:?}"
    );
    // The record moves at the commit; `planner_promoted` follows when the promote returns.
    let promoted = logged_at_least(method, "planner_promoted", moving.len());
    let mut moved: Vec<u64> = promoted
        .iter()
        .filter_map(|line| line["partition"].as_u64())
        .collect();
    moved.sort_unstable();
    moved.dedup();
    assert_eq!(
        moved,
        moving.iter().map(|p| u64::from(*p)).collect::<Vec<_>>(),
        "one planner_promoted per moved partition: {promoted:?}"
    );
    let (app, _net, _links) = rig.app(&[NodeId(2), NodeId(3)]);
    for p in &moving {
        put_lands(&app, group_of(&app, *p), Duration::from_secs(30));
    }
    tracing::info!(
        partitions,
        moved = moving.len(),
        took_ms = took.as_millis(),
        bound_ms = bound.as_millis(),
        "own81_b1_time_to_new_primary"
    );
}

/// V7 and B1 at a small count: 6 partitions, node 1 owns 2. Integration (~10 s): waits out a
/// 3 s grant, its margins, a look and the promotes, then asserts B1's bound ([`b1_bound`]).
/// Cut: `judge` answers `Alive` for an expired grant; nothing moves, red at the first wait.
#[config_log::retcd_test]
fn own81_v7_b1_a_stopped_owner_has_every_partition_moved() {
    const METHOD: &str = "own81_v7_b1_a_stopped_owner_has_every_partition_moved";
    stopped_owner_every_partition_moves(METHOD, 6);
}

/// B1 at 512 partitions (acceptance: "within a stated bound"), node 1 owns about a third. The
/// same helper and bound as the 6-partition row; the measured time is printed and logged
/// (`own81_b1_time_to_new_primary`). Run alone, by hand, on a quiet host:
/// `cargo test -p rdb-api --lib -- --ignored --nocapture own81_b1_512`.
#[config_log::retcd_test]
#[ignore = "perf: 512 partitions, run alone by hand"]
fn own81_b1_512_partitions_a_stopped_owner_has_every_partition_moved() {
    const METHOD: &str = "own81_b1_512_partitions_a_stopped_owner_has_every_partition_moved";
    stopped_owner_every_partition_moves(METHOD, 512);
}

/// S1 and C4: after the failover, node 1's process resumes. A put sent to it answers
/// `NOT_PRIMARY` with `no_mutation`, and the app's retry lands on the new owner. Node 1's grant
/// is frozen, so it never replaces it and admits nothing.
/// Integration (~12 s): one failover, then the resumed node's refusal.
/// No C1 cut: the refusal is the existing owner check and frozen-grant fence. This row pins
/// that a planner's promote leaves the returning owner refusing, as an operator's does.
#[config_log::retcd_test]
fn own81_s1_c4_the_old_primary_returns_answering_not_primary() {
    let rig = rig(1, Some(failover()));
    until("a planner holds its grant", Duration::from_secs(20), || {
        rig.planner_grant().is_some()
    });
    rig.stop(0);
    until("partition 1 moves", Duration::from_secs(60), || {
        rig.owner(1) != NodeId(1)
    });
    let new_owner = rig.owner(1);
    rig.resume(0);

    let (app, _net, _links) = rig.app(&ALL_NODES);
    // Until node 1 has read rEtcd again it answers LEASE_EXPIRED (design C, S1); both answers
    // carry `no_mutation`, and every one is checked.
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(30));
    let refused = loop {
        app.route.owners[0].store(1, Ordering::Relaxed);
        let refused = app
            .put(b"s1", b"old", None)
            .expect_err("node 1 is no longer the owner");
        assert!(refused.error.no_mutation, "{refused}");
        if refused.error.kind != ErrorKind::LeaseExpired {
            break refused;
        }
        assert!(Instant::now() < deadline, "still LEASE_EXPIRED: {refused}");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(
        (
            refused.error.kind,
            refused.error.retry,
            refused.error.no_mutation
        ),
        (ErrorKind::NotPrimary, RetryRule::RefreshRoute, true),
        "{refused}"
    );
    // The retry lands on the new owner. How few refusals the route takes to get there is
    // S4's, not this row's: refreshes are rate-limited, and the loop above forced it back.
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(10));
    while let Err(refused) = app.put(b"s1", b"new", None) {
        assert!(refused.error.no_mutation, "{refused}");
        assert!(Instant::now() < deadline, "no retry landed: {refused}");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(app.route.owner(), new_owner);

    let (.., grant) = rig.grants().remove_entry(&NodeId(1)).expect("grants/1");
    assert!(grant.frozen, "the promote froze node 1's grant: {grant:?}");
    let status = rig.procs[0]
        .db
        .node_status()
        .pop()
        .expect("node 1's status");
    assert_ne!(status.admits, Some(true), "{status:?}");
}

/// PR #119 review (blocker on 34c712e): after one failover, a second failure is not failed over
/// while the old owner is still out. Node 1 stops and partition 1 moves to node 2 or 3; the
/// promote drops node 1, so the record pins only those two. Node 1 stays stopped, and then the
/// new owner stops too. The one running node is the only answering copy besides the dead owner,
/// so the planner refuses (`lone_survivor`) and writes nothing: no second decision, no second
/// promote, the generation and owner as the first failover left them, and the new owner's
/// grant not frozen.
/// Integration (~17 s): two grant lapses, one failover.
/// Cut: `choose_target` needs one candidate, not two; the running node decides to promote
/// itself, so no `planner_no_eligible_copy` is said, red. Observed under that cut: the promote
/// then failed before its `Fencing` write, because the new owner's expiry was not proven past
/// on a clock with no peer left to bound it, so the generation did not move either. That is a
/// second, timing-dependent guard; this row pins the first.
#[config_log::retcd_test]
fn own81_a_second_failure_before_the_old_owner_restarts_is_not_failed_over() {
    const METHOD: &str = "own81_a_second_failure_before_the_old_owner_restarts_is_not_failed_over";
    let rig = rig(1, Some(failover()));
    until("a planner holds its grant", lifetimes(7), || {
        rig.planner_grant().is_some()
    });
    rig.stop(0);
    until("partition 1 moves and serves", lifetimes(20), || {
        let record = rig.record(1);
        record.owner != NodeId(1) && record.lifecycle == PartitionLifecycle::Serving
    });
    let first = rig.record(1);
    let new_owner = first.owner;
    assert!(matches!(new_owner, NodeId(2 | 3)), "{first:?}");
    let promoted = logged_at_least(METHOD, "planner_promoted", 1);
    assert_eq!(promoted.len(), 1, "the first failover: {promoted:?}");
    // A node that owned nothing held no grant: the failover is complete once the new owner
    // holds one at its own boot.
    rig.wait_for_owner_grants();

    // Only refusals after the second stop count: one said during the first failover would
    // otherwise satisfy the wait before the second failure is judged at all (PR #119 review).
    let baseline = logged(METHOD, "planner_no_eligible_copy").len();
    // The old owner has not restarted: node 1 is still stopped.
    rig.stop(new_owner.0 as usize - 1);
    let refused = logged_at_least_within(
        METHOD,
        "planner_no_eligible_copy",
        baseline + 1,
        lifetimes(20),
    );
    let refused = &refused[baseline..];
    assert_eq!(
        refused.len(),
        1,
        "logged once after the second stop: {refused:?}"
    );
    assert_eq!(
        (&refused[0]["partition"], &refused[0]["reason"]),
        (&serde_json::json!(1), &serde_json::json!("lone_survivor")),
        "{refused:?}"
    );
    let after = rig.record(1);
    assert_eq!(
        (after.owner, after.generation, after.lifecycle),
        (new_owner, first.generation, PartitionLifecycle::Serving),
        "no second promotion and no generation advance: {first:?} -> {after:?}"
    );
    assert_eq!(after, first, "partitions/1 as the first failover left it");
    assert!(
        !rig.grants()[&new_owner].frozen,
        "nothing froze the new owner's grant"
    );
    let second: Vec<_> = logged(METHOD, "planner_promote_decision")
        .into_iter()
        .filter(|line| line["dead"] == new_owner.0)
        .collect();
    assert!(
        second.is_empty(),
        "no decision against the new owner: {second:?}"
    );
    let promoted = logged(METHOD, "planner_promoted");
    assert_eq!(promoted.len(), 1, "no second promote: {promoted:?}");
}

/// V3 over real links: nodes 1 and 3 both stop, so partition 1 has one answering copy besides
/// its dead owner (customer answer 1). Nothing is written to `partitions/1`, and the planner
/// says why once. Integration (~10 s): waits out the grant and both looks.
/// Cut: `choose_target` needs one candidate; partition 1 moves to node 2, red.
#[config_log::retcd_test]
fn own81_v3_a_lone_survivor_is_not_promoted() {
    const METHOD: &str = "own81_v3_a_lone_survivor_is_not_promoted";
    let rig = rig(1, Some(failover()));
    let before = rig.record(1);
    rig.stop(0);
    rig.stop(2);
    let refused = logged_at_least_within(
        METHOD,
        "planner_no_eligible_copy",
        1,
        Duration::from_secs(30),
    );
    assert_eq!(refused.len(), 1, "logged once: {refused:?}");
    assert_eq!(refused[0]["partition"], 1, "{refused:?}");
    assert_eq!(refused[0]["reason"], "lone_survivor", "{refused:?}");
    assert_eq!(rig.record(1), before, "partitions/1 unchanged");
    assert!(!rig.grants()[&NodeId(1)].frozen, "nothing froze grants/1");
    assert!(logged(METHOD, "planner_promote_decision").is_empty());
}

/// V4, V5 and V6 share one shape: node 1's process still answers on its links, so it is never
/// failed over (customer answers 2 and 3). With `alert`, its grant runs out and the planner
/// raises `planner_owner_alive_not_serving` naming node 1 (`heard_on_link`). Either way, two
/// grant periods later nothing has been written: no decision, no freeze, `partitions/1`
/// unchanged.
///
/// Only nodes 2 and 3 run a planner: a planner does not judge its own node while its grant
/// stands, so with node 1 holding the planner grant these rows would pass with nothing judged.
fn alive_owner_is_not_failed_over(method: &str, alert: bool, harm: impl FnOnce(&Rig)) {
    let rig = rig_with(1, [None, Some(failover()), Some(failover())]);
    let before = rig.record(1);
    let setup = logged(method, "planner_owner_alive_not_serving").len();
    harm(&rig);
    if alert {
        let alerts = logged_at_least_within(
            method,
            "planner_owner_alive_not_serving",
            setup + 1,
            Duration::from_secs(30),
        );
        assert!(
            alerts[setup..]
                .iter()
                .any(|line| line["node"] == 1 && line["evidence"] == "heard_on_link"),
            "{alerts:?}"
        );
    }
    // An absence is asserted, so nothing can be polled for: wait two grant lifetimes, the
    // configured timing, and look once.
    std::thread::sleep(lifetimes(2));
    assert_eq!(rig.record(1), before, "partitions/1 unchanged");
    assert!(!rig.grants()[&NodeId(1)].frozen, "nothing froze grants/1");
    assert!(logged(method, "planner_promote_decision").is_empty());
    assert!(logged(method, "planner_node_silent").is_empty());
}

/// V4: node 1 is cut from rEtcd (`cut-control`) and nothing else. Its grant runs out; its links
/// still answer. Integration (~13 s): waits out the grant, then two more.
/// Cut: skip `heard_within` in `judge`; the alert names `answered_look`, so the
/// `heard_on_link` evidence assertion goes red; no partition moves.
#[config_log::retcd_test]
fn own81_v4_an_owner_cut_from_retcd_but_answering_is_not_failed_over() {
    alive_owner_is_not_failed_over(
        "own81_v4_an_owner_cut_from_retcd_but_answering_is_not_failed_over",
        true,
        |rig| rig.procs[0].faults.apply("cut-control\n"),
    );
}

/// V5: node 1's host faults (`Msg::Fault`, as a failed control effect does), so A1 renews no
/// more. Its net tasks still answer. Integration (~13 s).
/// Cut: as V4; the `heard_on_link` evidence assertion goes red.
#[config_log::retcd_test]
fn own81_v5_a_faulted_owner_still_answering_is_not_failed_over() {
    alive_owner_is_not_failed_over(
        "own81_v5_a_faulted_owner_still_answering_is_not_failed_over",
        true,
        |rig| {
            let db = &rig.procs[0].db;
            let boot = db.boot_of(NodeId(1)).expect("this process runs node 1");
            db.send_to(
                NodeId(1),
                Msg::Fault {
                    boot,
                    kind: "own81_v5",
                    detail: "injected".to_owned(),
                },
            )
            .expect("node 1 takes the fault");
            until("node 1 faults", Duration::from_secs(10), || {
                rig.procs[0]
                    .db
                    .node_status()
                    .pop()
                    .is_some_and(|status| status.fault.is_some())
            });
        },
    );
}

/// V6: node 1's host thread stalls for 2 x G (as a sync stuck on a slow disk holds it) while
/// its net tasks run. No promote, whether its grant runs out or not (the design's words), so
/// no alert is required. Integration (~10 s).
/// Cut: as V4, when the grant runs out; otherwise no planner change reddens it.
#[config_log::retcd_test]
fn own81_v6_a_stalled_host_thread_does_not_fail_over() {
    alive_owner_is_not_failed_over(
        "own81_v6_a_stalled_host_thread_does_not_fail_over",
        false,
        |rig| {
            let (release, stall) = std::sync::mpsc::channel::<()>();
            rig.procs[0]
                .db
                .send_to(NodeId(1), Msg::Stall(stall))
                .expect("a stall");
            // Released on a timer, after the row's checks and however they end: a stalled
            // host never shuts down, so the rig's drop would wait on it for good.
            let held = Duration::from_millis(2 * budgets().grant_millis + 1_000);
            std::thread::spawn(move || {
                std::thread::sleep(held);
                let _ = release.send(());
            });
        },
    );
}

/// V8 (ruling C-1): 12 partitions, node 1 owns 4. Node 1 stops, one of its partitions is
/// promoted by hand, which freezes its grant, and node 1 resumes and answers again. Only then
/// does a planner start: it finds node 1 heard on the link but frozen, and still moves the
/// other 3. Node 1 admits nothing.
/// A resume inside one sweep cannot show this: a sweep judges each owner once and then fences
/// all of its partitions, so the frozen grant is read only by a later sweep. This row puts the
/// planner on that later sweep.
/// Integration (~15 s): waits out the grant, a hand promote, then a failover.
/// Cut: skip the frozen-grant test in `judge`; node 1 is heard, the planner raises
/// `planner_owner_alive_not_serving`, and the 3 stay on node 1, red.
#[config_log::retcd_test]
fn own81_v8_an_owner_resumed_after_one_fence_still_has_its_other_partitions_moved() {
    const METHOD: &str =
        "own81_v8_an_owner_resumed_after_one_fence_still_has_its_other_partitions_moved";
    let mut rig = rig(12, None);
    let moving = rig.owned_by(NodeId(1));
    assert_eq!(moving.len(), 4, "{moving:?}");
    rig.stop(0);
    let promoted = rig.procs[1]
        .db
        .promote_partition(PartitionId(moving[0]), NodeId(2), None)
        .unwrap_or_else(|error| panic!("node 1 is stopped: {error}"));
    assert_eq!(promoted.owner, NodeId(2), "{promoted:?}");
    assert!(
        rig.grants()[&NodeId(1)].frozen,
        "the promote froze grants/1"
    );
    rig.resume(0);
    // Long enough for node 1's echoes to be heard again, so the planner's link test says
    // "alive" and only the frozen grant can send it on.
    std::thread::sleep(Duration::from_millis(
        2 * failover().silent_after.as_millis() as u64 + 500,
    ));
    let rest = &moving[1..];
    assert!(rest.iter().all(|p| rig.owner(*p) == NodeId(1)), "{rest:?}");

    rig.procs[1].db.start_planner(Failover {
        max_concurrent: 2,
        ..failover()
    });
    until(
        "node 1's other partitions move",
        Duration::from_secs(60),
        || rest.iter().all(|p| rig.owner(*p) != NodeId(1)),
    );
    let silent = logged_at_least(METHOD, "planner_node_silent", 1);
    assert_eq!(silent[0]["node"], 1, "{silent:?}");
    assert_eq!(silent[0]["reason"], "grant_frozen", "{silent:?}");
    assert!(logged(METHOD, "planner_owner_alive_not_serving").is_empty());
    let status = rig.procs[0].db.node_status();
    assert!(
        status.iter().all(|status| status.admits != Some(true)),
        "{status:?}"
    );
}

/// V8 on the frozen node's own planner (PR #119 review, `frozen_self_v8`): as V8, but the planner
/// that starts runs on node 1, the frozen owner itself, and so holds `planner/grant`. A planner
/// skips its own node only while that node's grant is not frozen, so its other 3 partitions
/// still move, to the two healthy peers.
/// Integration (~15 s): waits out the grant, a hand promote, then a failover.
/// Cut: skip the planner's own node whatever its grant (`if owner == self.node`); the 3 stay on
/// node 1, red at the wait.
#[config_log::retcd_test]
fn own81_frozen_self_v8_a_frozen_planner_moves_its_own_remaining_partitions() {
    const METHOD: &str = "own81_frozen_self_v8_a_frozen_planner_moves_its_own_remaining_partitions";
    let mut rig = rig(12, None);
    let moving = rig.owned_by(NodeId(1));
    assert_eq!(moving.len(), 4, "{moving:?}");
    rig.stop(0);
    let promoted = rig.procs[1]
        .db
        .promote_partition(PartitionId(moving[0]), NodeId(2), None)
        .unwrap_or_else(|error| panic!("node 1 is stopped: {error}"));
    assert_eq!(promoted.owner, NodeId(2), "{promoted:?}");
    assert!(
        rig.grants()[&NodeId(1)].frozen,
        "the promote froze grants/1"
    );
    rig.resume(0);
    let rest = &moving[1..];
    assert!(rest.iter().all(|p| rig.owner(*p) == NodeId(1)), "{rest:?}");

    rig.procs[0].db.start_planner(Failover {
        max_concurrent: 2,
        ..failover()
    });
    until(
        "node 1's planner holds planner/grant",
        Duration::from_secs(30),
        || {
            rig.planner_grant()
                .is_some_and(|grant| grant.node == NodeId(1) && !grant.frozen)
        },
    );
    until(
        "node 1's other partitions move",
        Duration::from_secs(60),
        || rest.iter().all(|p| rig.owner(*p) != NodeId(1)),
    );
    for p in rest {
        assert!(
            matches!(rig.owner(*p), NodeId(2 | 3)),
            "partition {p} moved to a healthy peer"
        );
    }
    assert_eq!(
        rig.planner_grant().map(|grant| grant.node),
        Some(NodeId(1)),
        "the frozen node's planner did the moves"
    );
    let silent = logged_at_least(METHOD, "planner_node_silent", 1);
    assert_eq!(silent[0]["node"], 1, "{silent:?}");
    assert_eq!(silent[0]["reason"], "grant_frozen", "{silent:?}");
    assert!(rig.grants()[&NodeId(1)].frozen, "grants/1 stays frozen");
}

/// D1, then V9. With `failover: None` nothing promotes: node 1 stops, and after 3 x G nothing
/// has changed and no planner ran. Then a planner starts on node 2, which never heard node 1
/// at its start, and every partition node 1 owned moves.
/// Integration (~16 s): 9 s of D1 waiting, then a failover.
/// Cut (D1): start the planner when `failover` is `None`; partition 1 moves inside the 3 x G.
#[config_log::retcd_test]
fn own81_d1_v9_off_by_default_then_a_planner_started_after_the_owner_died_fails_it_over() {
    const METHOD: &str =
        "own81_d1_v9_off_by_default_then_a_planner_started_after_the_owner_died_fails_it_over";
    let mut rig = rig(6, None);
    let moving = rig.owned_by(NodeId(1));
    let before: Vec<PartitionRecord> = (1..=6).map(|p| rig.record(p)).collect();
    rig.stop(0);
    std::thread::sleep(lifetimes(3));
    let after: Vec<PartitionRecord> = (1..=6).map(|p| rig.record(p)).collect();
    assert_eq!(after, before, "D1: no record changed");
    assert_eq!(rig.planner_grant(), None, "D1: no planner ran");
    assert!(logged(METHOD, "planner_started").is_empty());

    rig.procs[1].db.start_planner(failover());
    until(
        "V9: every partition of node 1 moves",
        Duration::from_secs(60),
        || moving.iter().all(|p| rig.owner(*p) != NodeId(1)),
    );
}

/// R3 over real links: two planners that both believe they hold the grant sweep at once
/// after node 1 stops. Each partition commits once and the generation rises by one. One
/// sweep's promote lands. The other either reports it lost the race, or, when both promotes
/// dropped the same node, answers Ok for that same commit (A-R448 case 3: "both answer Ok,
/// each with the commit's generation and revision"). So the row counts the distinct
/// generations of the Ok answers, not Ok answers (seen once under load in a full `--lib` run:
/// two Ok answers, both at generation 3 and revision 51). Not the revision too: for one commit
/// `control_revision` can be a later write at the same generation, so two answers to one
/// commit may differ there. A second commit raises the generation again, so the distinct
/// generations, the record's generation and the +1 below each catch it.
/// Three guards stand between two sweeps and a second commit: the promote's `Fencing` CAS, F1's
/// commit CAS on `partitions/` at the fence revision, and the node's lineage, which refuses a
/// plan for a lineage it has moved past.
/// Integration (~10 s): waits out the grant, then two sweeps.
/// Cuts (PR #171 round 2): the `Fencing` CAS made unconditional: 3 of 3 green, one commit both
/// sweeps answered Ok for. That CAS and F1's commit CAS both made unconditional: 6 of 9 red, every
/// one at the record-equals-answers check (the late `Fencing` write put the record back to
/// generation 2 while the answers named 3). So that check catches a double write. No cut short
/// of the lineage refusal made a second commit. This row pins that the planner adds no second
/// write around them.
#[config_log::retcd_test]
fn own81_r3_two_planners_sweeping_at_once_commit_each_partition_once() {
    let rig = rig(3, None);
    let before = rig.record(1);
    let planners: Vec<crate::planner::Planner> = rig.procs[1..]
        .iter()
        .map(|proc| {
            proc.db
                .planner(failover())
                .expect("a node process's planner")
        })
        .collect();
    rig.stop(0);
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(60));
    let answers = loop {
        let (mut ma, mut mb) = (
            crate::planner::Memory::default(),
            crate::planner::Memory::default(),
        );
        ma.believe_held(&planners[0]);
        mb.believe_held(&planners[1]);
        let (a, b) = rig.rt.block_on(async {
            tokio::join!(planners[0].sweep(&mut ma), planners[1].sweep(&mut mb))
        });
        if !a.is_empty() || !b.is_empty() {
            break [a, b];
        }
        assert!(Instant::now() < deadline, "no sweep acted");
        std::thread::sleep(Duration::from_millis(200));
    };
    let commits: std::collections::BTreeSet<_> = answers
        .iter()
        .flatten()
        .filter(|(partition, _)| *partition == PartitionId(1))
        .filter_map(|(_, answer)| answer.as_ref().ok())
        .map(|promoted| promoted.generation)
        .collect();
    assert_eq!(commits.len(), 1, "one commit: {answers:?}");
    let after = rig.record(1);
    let generation = commits.first().copied().expect("one commit");
    assert_eq!(
        after.generation, generation,
        "the Ok answers name the commit: {after:?}"
    );
    assert_ne!(after.owner, NodeId(1), "{after:?}");
    assert_eq!(
        after.generation.0,
        before.generation.0 + 1,
        "one new generation: {before:?} -> {after:?}"
    );
}

/// Lead ruling 2 on PR #119: a cold start raises no `planner_owner_alive_not_serving`. A node
/// opens with no grant of its own: `grants/{node}` still holds the record of the boot before,
/// until the node's recovery replaces it, and that record runs out first. An owner whose only
/// grant is a past boot's is not an owner that stopped serving.
///
/// The row holds node 2 in that state on purpose: its own grant writes are cut ([`opened_cut`])
/// from before it opens, so its recovery cannot replace the old record, while its links still
/// answer. Left to the start's own timing the state lasts from none to a few hundred ms, and
/// the row saw it or not by chance (lead's gate at dc2b1b4). The setup is then delayed by one
/// grant lifetime before the first check, and the state still holds (PR #119 review: a cut
/// applied after the opens let node 2's first grant land first). Then node 2 is let go and
/// takes its grant, and still no alert. Only nodes 1 and 3 run a planner, so the one holding the
/// planner grant can judge node 2; the wait for `planner_owner_no_grant_yet` naming node 2 fails
/// the row if nothing did.
/// Integration (~18 s): the old grant runs out, the delay, a few sweeps, the owners' grants.
/// Cut: drop the "no grant of its own" check in `sweep`; node 2's old record runs out while it
/// answers, the alert is said for it, red. Or judge "of its own" by the record's presence, not
/// its boot (as before this row): the same, red.
#[config_log::retcd_test]
fn own81_cold_start_raises_no_alert_before_an_owners_first_grant() {
    const METHOD: &str = "own81_cold_start_raises_no_alert_before_an_owners_first_grant";
    // No planner on node 2: a planner does not judge its own node while its grant stands,
    // so with node 2 holding the planner grant nothing would judge it and the wait below
    // would time out (PR #119 round 6).
    let rig = opened_cut(
        3,
        [Some(failover()), None, Some(failover())],
        Some(NodeId(2)),
    );
    // Inside the grace: the row asserts no alert, and past the grace one is due.
    std::thread::sleep(lifetimes(1));
    let node2_boot = rig.procs[1].boot;
    assert_eq!(rig.owned_by(NodeId(2)), vec![2], "node 2 owns partition 2");
    assert!(
        rig.grants()
            .get(&NodeId(2))
            .is_none_or(|grant| grant.boot != node2_boot),
        "node 2 took its grant before it was cut: the row did not set up its state"
    );

    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(30));
    let held = loop {
        let lines: Vec<_> = logged(METHOD, "planner_owner_no_grant_yet")
            .into_iter()
            .filter(|line| line["node"] == 2)
            .collect();
        if !lines.is_empty() || !logged(METHOD, "planner_owner_alive_not_serving").is_empty() {
            break lines;
        }
        assert!(Instant::now() < deadline, "no planner judged node 2");
        std::thread::sleep(Duration::from_millis(250));
    };
    std::thread::sleep(5 * failover().poll);
    assert_eq!(
        logged(METHOD, "planner_owner_alive_not_serving"),
        Vec::<serde_json::Value>::new(),
        "while node 2 had no grant of its own"
    );
    assert_eq!(
        held.len(),
        1,
        "node 2 heard with no grant of its own, said once: {held:?}"
    );
    assert!(
        rig.grants()
            .get(&NodeId(2))
            .is_none_or(|grant| grant.boot != node2_boot),
        "node 2 stayed without a grant of its own while cut"
    );

    rig.lift_grant_cut(NodeId(2));
    rig.wait_ready();
    rig.wait_for_owner_grants();
    std::thread::sleep(5 * failover().poll);
    assert_eq!(
        logged(METHOD, "planner_owner_alive_not_serving"),
        Vec::<serde_json::Value>::new(),
        "after node 2 took its grant"
    );
}

/// Lead ruling on PR #119 item 9: an owner heard on its links that has had its chance to take a
/// grant and has none of its own is the Q2 case. Node 2's own grant writes are cut from before
/// it opens, as in the cold-start row, and stay cut. The setup is delayed past the ~6.3 s a
/// start takes to its own grant before the first check (PR #119 review), and node 2 still has
/// none. Past [`crate::planner::no_grant_grace`] the planner raises
/// `planner_owner_alive_not_serving` for node 2 once (`no_grant_of_own`), and moves nothing.
/// Integration (~20 s): the old record runs out, then the grace, 9 s at the rig's 3 s grants.
/// The grace follows the grant, and a shorter grant in this rig would also shorten every
/// host's lease and renewals, which the other rows here are tuned to.
/// Only nodes 1 and 3 run a planner, leaving the owner under test (node 2) without one,
/// just as [`alive_owner_is_not_failed_over`] leaves its owner (node 1) without one.
/// Otherwise node 2 can win the planner grant at start and, not judging itself while its
/// grant stands, raise no alert.
/// Cut: never escalate in `sweep` (the `waited < no_grant_grace` test always true); no alert
/// within 30 s, red.
#[config_log::retcd_test]
fn own81_an_owner_with_no_grant_of_its_own_past_the_grace_is_alerted_once_not_moved() {
    const METHOD: &str =
        "own81_an_owner_with_no_grant_of_its_own_past_the_grace_is_alerted_once_not_moved";
    // No planner on node 2: a planner does not judge its own node while its grant stands,
    // so with node 2 holding the planner grant no alert for it could come (seen once,
    // PR #119 round 6).
    let rig = opened_cut(
        3,
        [Some(failover()), None, Some(failover())],
        Some(NodeId(2)),
    );
    // Wait for the uncut owners to acquire their own grants; node 2's write cut was installed
    // before Db::open_with.
    until(
        "nodes 1 and 3 hold a grant at their own boots",
        Duration::from_secs(30),
        || {
            let grants = rig.grants();
            [0, 2].into_iter().all(|i| {
                grants
                    .get(&ALL_NODES[i])
                    .is_some_and(|grant| grant.boot == rig.procs[i].boot && !grant.frozen)
            })
        },
    );
    let node2_boot = rig.procs[1].boot;
    assert_eq!(rig.owned_by(NodeId(2)), vec![2], "node 2 owns partition 2");
    assert!(
        rig.grants()
            .get(&NodeId(2))
            .is_none_or(|grant| grant.boot != node2_boot),
        "node 2 took its grant before it was cut: the row did not set up its state"
    );
    let before = rig.record(2);

    let alerts = logged_at_least_within(
        METHOD,
        "planner_owner_alive_not_serving",
        1,
        Duration::from_secs(30),
    );
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    let alert = &alerts[0];
    assert_eq!(alert["node"], 2, "{alert:?}");
    assert_eq!(alert["no_grant_of_own"], true, "{alert:?}");
    let grace = crate::planner::no_grant_grace(&budgets());
    assert!(
        // A `u128` field is logged as a string.
        alert["since_ms"]
            .as_str()
            .and_then(|ms| ms.parse::<u128>().ok())
            .is_some_and(|ms| ms >= grace.as_millis()),
        "said only past the grace of {grace:?}: {alert:?}"
    );
    let entered = logged(METHOD, "planner_owner_no_grant_yet");
    assert!(
        entered.iter().any(|line| line["node"] == 2),
        "the planner saw node 2 with no grant of its own first: {entered:?}"
    );

    std::thread::sleep(10 * failover().poll);
    let alerts = logged(METHOD, "planner_owner_alive_not_serving");
    assert_eq!(alerts.len(), 1, "said once, not every poll: {alerts:?}");
    assert_eq!(rig.record(2), before, "partitions/2 unchanged");
    // Scoped to node 2: a process that opens late under load is rightly judged silent until
    // it does (observed for node 3, refused `lone_survivor`), and that is not this row's case.
    let decided: Vec<_> = logged(METHOD, "planner_promote_decision")
        .into_iter()
        .filter(|line| line["partition"] == 2 || line["dead"] == 2)
        .collect();
    assert!(decided.is_empty(), "{decided:?}");
    let silent: Vec<_> = logged(METHOD, "planner_node_silent")
        .into_iter()
        .filter(|line| line["node"] == 2)
        .collect();
    assert!(silent.is_empty(), "{silent:?}");
}

/// Lead ruling 1 on PR #119: the planner on node 1 holds `planner/grant`; node 1's host
/// faults, and its process stays up, on its links and on its rEtcd. The planner gives the grant
/// up (`planner_grant_released_host_faulted`, once), node 2's planner takes it once it lapses,
/// and node 2 is the one that judges node 1: still answering, so the Q2 alert and no move.
/// Then node 1 goes silent on its links and node 2 fails its partition over.
/// The first half is what the ruling changes. Once node 1 is muted its clock has no samples,
/// so its planner stops on `planner_clock_unbounded` with or without the ruling.
/// Integration (~20 s): a grant lapse, the alert, then a failover.
/// Cut: skip the own-host check in `hold`; node 1 keeps renewing, nobody judges node 1 (a
/// planner does not judge its own node while its grant stands), and node 2 never holds the
/// grant, red.
#[config_log::retcd_test]
fn own81_a_planner_on_a_faulted_host_gives_up_its_grant_and_another_fails_over() {
    const METHOD: &str =
        "own81_a_planner_on_a_faulted_host_gives_up_its_grant_and_another_fails_over";
    let mut rig = rig_with(3, [Some(failover()), None, None]);
    assert_eq!(rig.owner(1), NodeId(1));
    until(
        "node 1's planner holds its grant",
        Duration::from_secs(20),
        || {
            rig.planner_grant()
                .is_some_and(|grant| grant.node == NodeId(1))
        },
    );
    rig.procs[1].db.start_planner(failover());

    let db = &rig.procs[0].db;
    let boot = db.boot_of(NodeId(1)).expect("this process runs node 1");
    db.send_to(
        NodeId(1),
        Msg::Fault {
            boot,
            kind: "own81_planner_fault",
            detail: "injected".to_owned(),
        },
    )
    .expect("node 1 takes the fault");
    until(
        "node 2's planner takes the grant",
        Duration::from_secs(30),
        || {
            rig.planner_grant()
                .is_some_and(|grant| grant.node == NodeId(2))
        },
    );
    let alert = logged_at_least(METHOD, "planner_owner_alive_not_serving", 1);
    assert_eq!(alert.len(), 1, "{alert:?}");
    assert_eq!(alert[0]["node"], 1, "{alert:?}");
    assert_eq!(
        rig.owner(1),
        NodeId(1),
        "an answering owner keeps its partition"
    );

    rig.procs[0].faults.apply("mute\n");
    until("node 1's partition moves", Duration::from_secs(60), || {
        rig.owner(1) != NodeId(1)
    });
    let silent = logged_at_least(METHOD, "planner_node_silent", 1);
    assert_eq!(silent[0]["node"], 1, "{silent:?}");
    let released = logged(METHOD, "planner_grant_released_host_faulted");
    assert_eq!(released.len(), 1, "said once, not every poll: {released:?}");
    assert_eq!(released[0]["node"], 1, "{released:?}");
}

/// PR #119 review (`stopped_host_releases`): as the faulted-host row, but node 1's host thread
/// ends (`Msg::Stop`) while its process stays up, on its links and on its rEtcd. Its mailbox is
/// closed, so the planner gives the grant up (`planner_grant_released_host_stopped`, once), and
/// node 2's planner takes it once it lapses, before any mute. Node 1 is still heard, so it is
/// alerted and not moved. Then node 1 goes silent on its links and node 2 fails its partition
/// over.
/// Integration (~20 s): a grant lapse, the alert, then a failover.
/// Cut: treat a closed local mailbox as no answer in `hold`; node 1 keeps renewing and node 2
/// never holds the grant, red at the first wait.
#[config_log::retcd_test]
fn own81_stopped_host_releases_its_planner_grant_and_another_fails_over() {
    const METHOD: &str = "own81_stopped_host_releases_its_planner_grant_and_another_fails_over";
    let mut rig = rig_with(3, [Some(failover()), None, None]);
    assert_eq!(rig.owner(1), NodeId(1));
    until(
        "node 1's planner holds its grant",
        Duration::from_secs(20),
        || {
            rig.planner_grant()
                .is_some_and(|grant| grant.node == NodeId(1))
        },
    );
    rig.procs[1].db.start_planner(failover());

    rig.procs[0]
        .db
        .send_to(NodeId(1), Msg::Stop)
        .expect("node 1's host takes the stop");
    until(
        "node 2's planner takes the grant",
        Duration::from_secs(30),
        || {
            rig.planner_grant()
                .is_some_and(|grant| grant.node == NodeId(2))
        },
    );
    let released = logged(METHOD, "planner_grant_released_host_stopped");
    assert_eq!(released.len(), 1, "{released:?}");
    assert_eq!(released[0]["node"], 1, "{released:?}");
    let lost = logged(METHOD, "planner_grant_lost");
    assert!(
        lost.iter()
            .any(|line| line["node"] == 1 && line["reason"] == "host_stopped"),
        "{lost:?}"
    );
    let alert = logged_at_least(METHOD, "planner_owner_alive_not_serving", 1);
    assert_eq!(alert[0]["node"], 1, "{alert:?}");
    assert_eq!(
        rig.owner(1),
        NodeId(1),
        "an owner still heard keeps its partition"
    );

    rig.procs[0].faults.apply("mute\n");
    until("node 1's partition moves", Duration::from_secs(60), || {
        rig.owner(1) != NodeId(1)
    });
    let silent = logged_at_least(METHOD, "planner_node_silent", 1);
    assert_eq!(silent[0]["node"], 1, "{silent:?}");
    let released = logged(METHOD, "planner_grant_released_host_stopped");
    assert_eq!(released.len(), 1, "said once, not every poll: {released:?}");
}

/// `config` refused with [`OpenError::Config`] whose text starts with `starts`, by `check` and
/// by `open`, and no node created: `nodes/` does not exist after the open.
fn refused_before_creation(config: &DbConfig, starts: &str) {
    let refused = |what: &str, result: Result<(), OpenError>| match result {
        Err(OpenError::Config(text)) => assert!(
            text.starts_with(starts) && text.ends_with("; nothing was created"),
            "{what}: {text}"
        ),
        other => panic!("{what} of {:?}: {other:?}", config.failover),
    };
    refused("check", config.check());
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let opened = rt.block_on(Db::open(config.clone(), store, rt.handle().clone()));
    refused("open", opened.map(|_db| ()));
    assert!(
        !config.dir.join("nodes").exists(),
        "a refused open created nodes/: {:?}",
        config.failover
    );
}

/// A node process's config for node 1, running `failover`.
fn node_with(dir: &Path, failover: Failover) -> DbConfig {
    DbConfig {
        local: vec![NodeId(1)],
        failover: Some(failover),
        ..config(dir)
    }
}

/// PR #119 review: a zero poll is refused before anything is created, by `check` and `open`:
/// it would remove the spacing between the planner's control-store reads and its grant
/// writes. A 1 ms poll is taken.
/// Cut: drop the poll test in `failover_refusal`; `check` answers `Ok`, red.
#[test]
fn own81_zero_poll_is_refused() {
    let dir = config_testkit::fs::temp_dir();
    let poll = |millis| {
        node_with(
            dir.path(),
            Failover {
                poll: Duration::from_millis(millis),
                ..Failover::default()
            },
        )
    };
    refused_before_creation(&poll(0), "failover poll must be above 0 ms");
    assert!(poll(1).check().is_ok(), "a 1 ms poll is taken");
}

/// PR #119 review: a zero silence window is refused before anything is created: every link
/// would read as silent. The default 2 s is taken.
/// Cut: drop the silence test in `failover_refusal`; `check` answers `Ok`, red.
#[test]
fn own81_zero_silence_is_refused() {
    let dir = config_testkit::fs::temp_dir();
    refused_before_creation(
        &node_with(
            dir.path(),
            Failover {
                silent_after: Duration::ZERO,
                ..Failover::default()
            },
        ),
        "failover silent_after 0 ms is under one clock-probe period (1000 ms)",
    );
    assert_eq!(Failover::default().silent_after, Duration::from_secs(2));
    assert!(
        node_with(dir.path(), Failover::default()).check().is_ok(),
        "the default 2 s silence is taken"
    );
}

/// PR #119 review: a silence window under one clock-probe period is refused: a healthy link
/// echoes once a period, so it could read as silent between echoes. One period is the least
/// taken. Boundary rows at 999 ms and 1,000 ms.
/// Cut: compare against zero instead of the probe period; 999 ms is taken, red.
#[test]
fn own81_subprobe_silence_is_refused() {
    let dir = config_testkit::fs::temp_dir();
    let silent = |millis| {
        node_with(
            dir.path(),
            Failover {
                silent_after: Duration::from_millis(millis),
                ..Failover::default()
            },
        )
    };
    assert_eq!(crate::net::PROBE_PERIOD, Duration::from_millis(1_000));
    refused_before_creation(
        &silent(999),
        "failover silent_after 999 ms is under one clock-probe period (1000 ms)",
    );
    assert!(silent(1_000).check().is_ok(), "one probe period is taken");
}

/// PR #119 review: each invalid failover config is refused with `Config` before any node is
/// created, by `check` and by `open`, one row each: no local node (an app open), two distinct
/// nodes, a duplicated node, and zero promotes at once. Controls: failover off with all three
/// nodes, and a valid single-node failover, are taken.
/// Cut: accept any `local` in `failover_refusal`; the first row's `check` answers `Ok`, red.
#[test]
fn own81_invalid_failover_is_refused_before_creation() {
    let dir = config_testkit::fs::temp_dir();
    let local = |nodes: &[u32]| DbConfig {
        local: nodes.iter().copied().map(NodeId).collect(),
        ..node_with(dir.path(), Failover::default())
    };
    let one_node = "failover runs only in a node process, one node in DbConfig::local";
    for (nodes, names) in [(&[][..], 0), (&[1, 2][..], 2), (&[1, 1][..], 2)] {
        refused_before_creation(
            &local(nodes),
            &format!("{one_node}, and this open names {names}"),
        );
    }
    refused_before_creation(
        &node_with(
            dir.path(),
            Failover {
                max_concurrent: 0,
                ..Failover::default()
            },
        ),
        "failover max_concurrent 0 must be at least 1",
    );
    assert!(config(dir.path()).check().is_ok(), "failover off is taken");
    assert!(
        local(&[1]).check().is_ok(),
        "one node with failover is taken"
    );
}

/// PR #119 review (`promote_failure_warns_once`): node 1 stops, and every promote of its
/// partition is refused, stably: the planner's `partitions/` writes are cut, so each `Fencing`
/// CAS fails. Each sweep still retries, but the decision and the failure are said once for the
/// window (later decisions at debug). Once the cut is lifted the next sweep promotes,
/// `planner_promoted` is said, and `planner_warn_recovered` names the 3 failures held back.
/// Integration (~15 s): waits out the grant, then four sweeps whose looks wait the read
/// timeout.
/// Cut: `Repeats::note` says every occurrence; four failures, red.
#[config_log::retcd_test]
fn own81_promote_failure_warns_once() {
    const METHOD: &str = "own81_promote_failure_warns_once";
    let rig = rig(3, None);
    let moving = rig.owned_by(NodeId(1));
    let p = *moving.first().expect("node 1 owns a partition");
    let planner = rig.procs[1]
        .db
        .planner(failover())
        .expect("a node process's planner");
    let cut = PrefixCut::new(
        Arc::clone(&planner.store),
        ControlPrefix::Partitions.encode(),
        CutCalls::Writes,
    );
    let planner = crate::planner::Planner {
        store: Arc::clone(&cut) as Arc<dyn ConfigStore>,
        ..planner
    };
    rig.stop(0);
    let mut memory = crate::planner::Memory::default();
    let mut failed = 0;
    until("four promotes are tried", lifetimes(20), || {
        memory.believe_held(&planner);
        let answers = rig.rt.block_on(planner.sweep(&mut memory));
        for (partition, answer) in &answers {
            assert_eq!(partition.0, p, "{answers:?}");
            assert!(answer.is_err(), "the Fencing write is cut: {answers:?}");
            failed += 1;
        }
        failed >= 4
    });
    let said = |message: &str| -> Vec<serde_json::Value> {
        logged(METHOD, message)
            .into_iter()
            .filter(|line| line["partition"] == p && line["@l"] != "Debug")
            .collect()
    };
    let failures = said("planner_promote_failed");
    assert_eq!(failures.len(), 1, "said once in the window: {failures:?}");
    assert_eq!(failures[0]["suppressed"], 0, "{failures:?}");
    let decisions = said("planner_promote_decision");
    assert_eq!(decisions.len(), 1, "the pair, once: {decisions:?}");

    cut.lift();
    memory.believe_held(&planner);
    let answers = rig.rt.block_on(planner.sweep(&mut memory));
    assert!(
        answers
            .iter()
            .any(|(partition, answer)| partition.0 == p && answer.is_ok()),
        "{answers:?}"
    );
    assert_eq!(said("planner_promoted").len(), 1);
    let recovered: Vec<_> = logged(METHOD, "planner_warn_recovered")
        .into_iter()
        .filter(|line| line["warn"] == "planner_promote_failed")
        .collect();
    assert_eq!(recovered.len(), 1, "{recovered:?}");
    assert_eq!(recovered[0]["suppressed"], 3, "{recovered:?}");
    assert_ne!(rig.owner(p), NodeId(1));
}

/// `grants/{node}` and the revision it was written at.
fn grant_at(rig: &Rig, node: NodeId) -> Option<(rdb_core::authority::grant::GrantRecord, u64)> {
    rig.rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Grant(node).encode()),
        }))
        .expect("the store answers")
        .record
        .map(|found| {
            (
                rdb_core::authority::grant::GrantRecord::decode(&found.value).expect("a grant"),
                found.mod_revision,
            )
        })
}

/// Sweep with `planner` until a sweep acts, and return what it did.
fn sweep_until_acted(
    rig: &Rig,
    planner: &crate::planner::Planner,
    memory: &mut crate::planner::Memory,
) -> Vec<(
    PartitionId,
    Result<crate::admin::Promoted, crate::admin::PromoteError>,
)> {
    let mut answers = Vec::new();
    until("a sweep acts", lifetimes(20), || {
        memory.believe_held(planner);
        answers = rig.rt.block_on(planner.sweep(memory));
        !answers.is_empty()
    });
    answers
}

/// The interrupted-sweep rows (PR #119 review): node 1 stops and node 2's planner fails its
/// partition over, but that planner is interrupted at its freeze of `grants/1`, `at` the
/// point the hold names: its sweep is dropped there, as a stopped process leaves it. The next
/// attempt fails too (its `partitions/` writes are cut). Then the planner restarts with a
/// fresh `Memory` and nothing cut.
///
/// Asserted: no attempt before the restart commits (generation unchanged, the record left
/// `Fencing`); the restarted planner moves the partition once (one commit, one new
/// generation, and a further sweep does nothing); `grants/1` is frozen from the first applied
/// freeze on and never rewritten; and node 1's old boot, resumed, reads that grant, stops
/// replacing it (`replace_stopped`), and cannot write.
fn interrupted_sweep(method: &str, at: HoldAt) {
    let rig = rig(1, None);
    let before = rig.record(1);
    assert_eq!(before.owner, NodeId(1), "{before:?}");
    let planner = || {
        rig.procs[1]
            .db
            .planner(failover())
            .expect("a node process's planner")
    };
    let base = planner();
    let hold = PutHold::new(
        Arc::clone(&base.store),
        &ControlKey::Grant(NodeId(1)).encode(),
        at,
    );
    let interrupted = crate::planner::Planner {
        store: Arc::clone(&hold) as Arc<dyn ConfigStore>,
        ..base
    };
    rig.stop(0);
    let mut memory = crate::planner::Memory::default();
    until(
        "a promote reaches the freeze",
        Duration::from_secs(60),
        || {
            memory.believe_held(&interrupted);
            rig.rt.block_on(async {
                tokio::select! {
                    answers = interrupted.sweep(&mut memory) => {
                        assert!(answers.is_empty(), "the freeze is held: {answers:?}");
                        false
                    }
                    () = hold.arrived.notified() => true,
                }
            })
        },
    );
    let fenced = rig.record(1);
    assert_eq!(
        (fenced.generation, fenced.owner, fenced.lifecycle),
        (before.generation, NodeId(1), PartitionLifecycle::Fencing),
        "interrupted at the freeze, after Fencing: {fenced:?}"
    );
    let frozen = grant_at(&rig, NodeId(1));
    match at {
        HoldAt::BeforeApply => assert!(
            frozen.is_some_and(|(grant, _)| !grant.frozen),
            "the freeze never applied: {frozen:?}"
        ),
        HoldAt::BeforeReply => assert!(
            frozen.is_some_and(|(grant, _)| grant.frozen),
            "the freeze applied: {frozen:?}"
        ),
    }

    let cut = PrefixCut::new(
        Arc::clone(&planner().store),
        ControlPrefix::Partitions.encode(),
        CutCalls::Writes,
    );
    let failing = crate::planner::Planner {
        store: Arc::clone(&cut) as Arc<dyn ConfigStore>,
        ..planner()
    };
    let answers = sweep_until_acted(&rig, &failing, &mut memory);
    assert!(
        answers
            .iter()
            .all(|(partition, answer)| partition.0 == 1 && answer.is_err()),
        "the retry's Fencing write is cut: {answers:?}"
    );
    let retried = rig.record(1);
    assert_eq!(retried.generation, before.generation, "{retried:?}");
    if at == HoldAt::BeforeReply {
        assert_eq!(
            grant_at(&rig, NodeId(1)),
            frozen,
            "the frozen grant is not rewritten"
        );
    }

    let restarted = planner();
    let mut fresh = crate::planner::Memory::default();
    let answers = sweep_until_acted(&rig, &restarted, &mut fresh);
    let committed: Vec<_> = answers
        .iter()
        .filter(|(partition, answer)| partition.0 == 1 && answer.is_ok())
        .collect();
    assert_eq!(committed.len(), 1, "one commit: {answers:?}");
    let after = rig.record(1);
    assert_ne!(after.owner, NodeId(1), "{after:?}");
    assert_eq!(after.lifecycle, PartitionLifecycle::Serving, "{after:?}");
    assert_eq!(
        after.generation.0,
        before.generation.0 + 1,
        "one new generation: {before:?} -> {after:?}"
    );
    let promoted: Vec<_> = logged(method, "planner_promoted")
        .into_iter()
        .filter(|line| line["partition"] == 1)
        .collect();
    assert_eq!(promoted.len(), 1, "{promoted:?}");
    fresh.believe_held(&restarted);
    let again = rig.rt.block_on(restarted.sweep(&mut fresh));
    assert!(again.is_empty(), "moved once: {again:?}");
    assert_eq!(rig.record(1).generation, after.generation);

    let last = grant_at(&rig, NodeId(1));
    assert!(last.is_some_and(|(grant, _)| grant.frozen), "{last:?}");
    if at == HoldAt::BeforeReply {
        assert_eq!(last, frozen, "the first freeze's record, never rewritten");
    }

    rig.resume(0);
    // Node 1 has read rEtcd again once its own status says it found its grant frozen and
    // stopped replacing it: only then is its grant's state its own answer, not a refusal it
    // gave before it read, such as an out-of-reach UNAVAILABLE (PR #119 review). The grant is
    // checked unchanged on every poll.
    until(
        "node 1 finds its grant frozen and stops replacing it",
        lifetimes(10),
        || {
            assert_eq!(
                grant_at(&rig, NodeId(1)),
                last,
                "grants/1 stays as the promote left it"
            );
            rig.procs[0]
                .db
                .node_status()
                .pop()
                .is_some_and(|status| status.replace_stopped)
        },
    );
    let (app, _net, _links) = rig.app(&ALL_NODES);
    app.route.owners[0].store(1, Ordering::Relaxed);
    let refused = app
        .put(b"interrupted", b"old", None)
        .expect_err("node 1's old boot cannot write");
    assert!(refused.error.no_mutation, "{refused}");
    assert_eq!(
        grant_at(&rig, NodeId(1)),
        last,
        "grants/1 stays as the promote left it"
    );
}

/// PR #119 review (`sweep_restarts_before_freeze`): the planner is interrupted at `Fencing`,
/// before its freeze of `grants/1` applies ([`interrupted_sweep`]).
/// Integration (~15 s): waits out the grant, then three sweeps whose looks wait the read
/// timeout.
/// Cut: `promote_looked` refuses a record a stopped run left `Fencing` (A-R447); the restarted
/// planner never moves the partition, red.
#[config_log::retcd_test]
fn own81_sweep_restarts_before_freeze() {
    interrupted_sweep("own81_sweep_restarts_before_freeze", HoldAt::BeforeApply);
}

/// PR #119 review (`sweep_restarts_after_freeze`): the planner is interrupted after its freeze
/// of `grants/1` applied and before the reply came back ([`interrupted_sweep`]). The retry and
/// the restart find the record frozen and keep it as it is.
/// Integration (~15 s), as the row above.
/// Cut: `freeze` rewrites a record it finds frozen; the frozen grant's revision moves, red.
#[config_log::retcd_test]
fn own81_sweep_restarts_after_freeze() {
    interrupted_sweep("own81_sweep_restarts_after_freeze", HoldAt::BeforeReply);
}

/// #81 (lead, embedded restart): a process that ends its links goes silent to its peers. Node 1
/// ends them by `Net::shutdown`, with no mute or cut fault set; node 1 is the one that accepts
/// links (2 and 3 dial lower nodes), which a plain abort of `Net`'s own tasks left echoing
/// probes. Then node 2's process is dropped whole, with no shutdown call: the last `Arc<Net>`
/// goes, and `Drop` ends its links too. Each time, every peer drops its route and stops
/// hearing the node. Integration (~10 s).
/// Cut: `Net::shutdown` aborts only `Net`'s own tasks (no link tasks, no conns closed, plain
/// `serve_with_incoming`); red at "nodes 2 and 3 stop hearing node 1".
#[config_log::retcd_test]
fn own81_net_shutdown_leaves_no_link_answering() {
    let mut rig = rig(1, None);
    let hears = |rig: &Rig, i: usize, node: NodeId| {
        rig.procs[i].net.connection(node).is_some()
            && rig.procs[i].net.heard_within(node, Duration::from_secs(1))
    };
    until("every node hears every other", lifetimes(5), || {
        (0..3).all(|i| {
            ALL_NODES
                .iter()
                .filter(|n| **n != ALL_NODES[i])
                .all(|n| hears(&rig, i, *n))
        })
    });

    rig.procs[0].db.shutdown();
    rig.procs[0].net.shutdown();
    until("nodes 2 and 3 stop hearing node 1", lifetimes(3), || {
        (1..3).all(|i| rig.procs[i].net.connection(NodeId(1)).is_none())
            && (1..3).all(|i| {
                !rig.procs[i]
                    .net
                    .heard_within(NodeId(1), Duration::from_secs(1))
            })
    });

    let proc = rig.procs.remove(1);
    let net = Arc::downgrade(&proc.net);
    drop(proc);
    until("node 2's Net is gone", lifetimes(3), || {
        net.upgrade().is_none()
    });
    // `procs` is now nodes 1 and 3.
    until("node 3 stops hearing node 2", lifetimes(3), || {
        rig.procs[1].net.connection(NodeId(2)).is_none()
            && !rig.procs[1]
                .net
                .heard_within(NodeId(2), Duration::from_secs(1))
    });
}

/// #81 X2 (owner answer (a), 2026-11-19): a second failover after the first owner returns.
/// Node 1 stops and partition 1 moves to node 2 or 3, freezing `grants/1`. Node 1 restarts at a
/// new boot (`Rig::restart`), as its supervisor does on exit 75. The new owner re-adds it as a
/// regular copy, and the planner deletes the frozen `grants/1` of the gone boot
/// (`planner_frozen_grant_cleared`). Then the new owner stops, and partition 1 moves again with
/// no operator: two copies are eligible, node 1 among them, so no `lone_survivor` and no
/// `TargetFrozen`. A node 1 chosen holds a grant of its own new boot.
/// Integration (~40 s): two grant lapses, a restart and a re-add.
/// Cut: `sweep` skips the `clear_frozen` pass; `grants/1` stays frozen, red at "the frozen
/// grants/1 is cleared" (and, past that wait, the second failure ends `lone_survivor`).
#[config_log::retcd_test]
fn own81_x2_promoted_away_owner_returns_as_a_promotable_regular_copy() {
    const METHOD: &str = "own81_x2_promoted_away_owner_returns_as_a_promotable_regular_copy";
    let mut rig = rig(1, Some(failover()));
    until("a planner holds its grant", lifetimes(7), || {
        rig.planner_grant().is_some()
    });
    rig.stop(0);
    until("partition 1 moves and serves", lifetimes(20), || {
        let record = rig.record(1);
        record.owner != NodeId(1) && record.lifecycle == PartitionLifecycle::Serving
    });
    let first = rig.record(1);
    let new_owner = first.owner;
    assert!(
        rig.grants()[&NodeId(1)].frozen,
        "the promote froze grants/1"
    );
    let old_boot = rig.procs[0].boot;

    rig.restart(0, Some(failover()));
    let boot = rig.procs[0].boot;
    assert!(boot > old_boot, "a new boot: {old_boot:?} -> {boot:?}");
    until("node 1 is a regular copy again", lifetimes(20), || {
        rig.members(1).iter().any(|member| {
            member.node == NodeId(1)
                && member.role == rdb_core::contracts::ids::ReplicaRole::RegularSecondary
        })
    });
    until("the frozen grants/1 is cleared", lifetimes(10), || {
        rig.grants()
            .get(&NodeId(1))
            .is_none_or(|grant| !grant.frozen)
    });
    let cleared = logged_at_least(METHOD, "planner_frozen_grant_cleared", 1);
    assert!(
        cleared
            .iter()
            .any(|line| line["node"] == 1 && line["record_boot"] == old_boot.0),
        "{cleared:?}"
    );
    rig.wait_for_owner_grants();

    let refused_before = logged(METHOD, "planner_no_eligible_copy").len();
    let failed_before = logged(METHOD, "planner_promote_failed").len();
    rig.stop(new_owner.0 as usize - 1);
    until("partition 1 moves again and serves", lifetimes(20), || {
        let record = rig.record(1);
        record.owner != new_owner && record.lifecycle == PartitionLifecycle::Serving
    });
    let second = rig.record(1);
    assert!(
        second.generation > first.generation,
        "a second promotion: {first:?} -> {second:?}"
    );
    let refused = logged(METHOD, "planner_no_eligible_copy");
    assert_eq!(
        refused.len(),
        refused_before,
        "no lone_survivor after the return: {refused:?}"
    );
    let failed = logged(METHOD, "planner_promote_failed");
    assert!(
        failed[failed_before..]
            .iter()
            .all(|line| !line["error"].as_str().unwrap_or("").contains("frozen")),
        "no TargetFrozen: {failed:?}"
    );
    let decided: Vec<_> = logged(METHOD, "planner_promote_decision")
        .into_iter()
        .filter(|line| line["dead"] == new_owner.0)
        .collect();
    assert!(
        // `answering` is logged with `?`, as `[1, 3]`.
        decided
            .iter()
            .any(|line| line["answering"].as_str().is_some_and(|nodes| nodes
                .trim_matches(|c| c == '[' || c == ']')
                .split(", ")
                .any(|node| node == "1"))),
        "node 1 answered the look as a candidate: {decided:?}"
    );
    if second.owner == NodeId(1) {
        until("node 1 holds a grant of its new boot", lifetimes(5), || {
            rig.grants()
                .get(&NodeId(1))
                .is_some_and(|grant| grant.boot == boot && !grant.frozen)
        });
    }
    println!("own81_x2 first={first:?} second={second:?}");
}

/// `G + eps + delta` on `budgets`, plus one: how far a held planner clock is moved past the
/// moment node 1 stopped for node 1's grant expiry to be proven on it, whenever its last renewal
/// was.
fn past_node_grant(budgets: &Budgets) -> u64 {
    budgets.grant_millis + budgets.clock_error_millis + budgets.dispatch_margin_millis + 1
}

/// The planner rows of node 2's process over `store`, on a capped `clock` (#81, the grant
/// re-check). No planner runs in `rig`, so this one alone writes `planner/grant`.
fn held_planner(
    rig: &Rig,
    store: Arc<dyn ConfigStore>,
    clock: crate::clock::HostClock,
) -> crate::planner::Planner {
    crate::planner::Planner {
        store,
        clock,
        ..rig.procs[1]
            .db
            .planner(failover())
            .expect("a node process's planner")
    }
}

/// #81, the planner grant re-check, case (a): the planner holds `planner/grant` on a held
/// clock, node 1 stops, and the grant runs out before the sweep reaches the promote. The
/// promote is skipped: no `Fencing` write (partition 1 keeps its owner, generation and
/// lifecycle), `planner_promote_skipped_grant_expired` said exactly once, and the grant lost.
/// Integration (~5 s): waits for node 1 to be judged silent.
/// Cut: no re-check before the promote in `fail_over`; the promote lands, red.
#[config_log::retcd_test]
fn own81_a_grant_that_ran_out_before_the_promote_sends_no_fence() {
    const METHOD: &str = "own81_a_grant_that_ran_out_before_the_promote_sends_no_fence";
    let rig = rig(1, None);
    let before = rig.record(1);
    assert_eq!(before.owner, NodeId(1), "{before:?}");
    let clock = crate::clock::HostClock::capped(1);
    let planner = held_planner(&rig, Arc::clone(&rig.store), clock);
    rig.stop(0);
    let hold = clock.hold();
    hold.advance(past_node_grant(&planner.budgets));
    let mut memory = crate::planner::Memory::default();
    assert!(
        rig.rt.block_on(planner.hold(&mut memory)),
        "the planner holds"
    );
    hold.advance(planner.budgets.grant_millis);
    until("a promote is reached", lifetimes(20), || {
        let answers = rig.rt.block_on(planner.sweep(&mut memory));
        assert!(answers.is_empty(), "nothing is promoted: {answers:?}");
        !logged(METHOD, "planner_promote_skipped_grant_expired").is_empty()
    });
    let skipped = logged(METHOD, "planner_promote_skipped_grant_expired");
    assert_eq!(skipped.len(), 1, "said once: {skipped:?}");
    assert_eq!(
        (&skipped[0]["partition"], &skipped[0]["dead"]),
        (&serde_json::json!(1), &serde_json::json!(1)),
        "{skipped:?}"
    );
    let after = rig.record(1);
    assert_eq!(
        (after.owner, after.generation, after.lifecycle),
        (before.owner, before.generation, before.lifecycle),
        "no Fencing write: {before:?} -> {after:?}"
    );
    let lost: Vec<_> = logged(METHOD, "planner_grant_lost")
        .into_iter()
        .filter(|line| line["reason"] == "expired")
        .collect();
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert!(logged(METHOD, "planner_promoted").is_empty());
}

/// #81, the planner grant re-check, case (b): the planner holds `planner/grant` on a held
/// clock, node 1 stops, and the grant runs out during the promote: the clock moves past it as
/// the first `partitions/` write (the `Fencing` CAS) arrives. The promote still lands, and the
/// answer is unchanged (`Ok`), but it is logged `planner_promote_observed`, never
/// `planner_promoted`: the count of promotes made under a held grant stays honest.
/// Integration (~5 s): waits for node 1 to be judged silent.
/// Cut: no re-check after the promote answers; `planner_promoted` is said, red.
#[config_log::retcd_test]
fn own81_a_grant_that_runs_out_during_the_promote_is_logged_observed() {
    const METHOD: &str = "own81_a_grant_that_runs_out_during_the_promote_is_logged_observed";
    let rig = rig(1, None);
    let before = rig.record(1);
    let clock = crate::clock::HostClock::capped(1);
    let grant_millis = budgets().grant_millis;
    let store = ClockOnPut::new(
        Arc::clone(&rig.store),
        ControlPrefix::Partitions.encode(),
        clock.hold(),
        grant_millis,
    );
    let planner = held_planner(&rig, Arc::clone(&store) as Arc<dyn ConfigStore>, clock);
    assert_eq!(planner.budgets.grant_millis, grant_millis);
    rig.stop(0);
    store.hold.advance(past_node_grant(&planner.budgets));
    let mut memory = crate::planner::Memory::default();
    assert!(
        rig.rt.block_on(planner.hold(&mut memory)),
        "the planner holds"
    );
    let mut answers = Vec::new();
    until("a promote answers", lifetimes(20), || {
        answers = rig.rt.block_on(planner.sweep(&mut memory));
        !answers.is_empty()
    });
    assert!(
        answers
            .iter()
            .all(|(partition, answer)| partition.0 == 1 && answer.is_ok()),
        "the promote lands and its answer is kept: {answers:?}"
    );
    let after = rig.record(1);
    assert_ne!(after.owner, NodeId(1), "{after:?}");
    assert_eq!(after.generation.0, before.generation.0 + 1, "{after:?}");
    let observed = logged(METHOD, "planner_promote_observed");
    assert_eq!(observed.len(), 1, "{observed:?}");
    assert_eq!(observed[0]["partition"], 1, "{observed:?}");
    assert_eq!(logged(METHOD, "test_clock_moved_on_put").len(), 1);
    let promoted = logged(METHOD, "planner_promoted");
    assert!(promoted.is_empty(), "not counted as held: {promoted:?}");
}
