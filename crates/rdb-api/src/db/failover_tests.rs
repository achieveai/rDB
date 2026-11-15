//! #81 C1: the planner over real links. Each row runs three node "processes" in this test
//! process, as `clock_guard_tests` does: a `Db`, a `Net` and a `Links` each, real TLS links,
//! one shared control store that each process reaches through its own [`CutStore`].
//!
//! A process is stopped, as `kill -STOP` stops one, with the fault file's `mute` and
//! `cut-control` together: its links read nothing and write nothing, probes included, and its
//! grant stops renewing. Dropping an in-process `Net` is not a stop: the listener's
//! per-connection tasks are not its own and keep echoing probes.

use super::reopen_tests::{config, fast, open_app, runtime};
use super::*;
use crate::net::faults::{CutStore, Faults};
use crate::net::{certs, wire::Label, Net, NetConfig, PROTOCOL_VERSION};
use crate::planner::Failover;

/// One node process.
struct Proc {
    db: Db,
    _net: Arc<Net>,
    faults: Arc<Faults>,
    boot: BootId,
}

struct Rig {
    rt: tokio::runtime::Runtime,
    store: Arc<dyn ConfigStore>,
    _dirs: Vec<Box<dyn std::any::Any>>,
    cert_dir: std::path::PathBuf,
    addrs: BTreeMap<NodeId, String>,
    procs: Vec<Proc>,
    partitions: u32,
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
                let boot = next_boot(dir.path()).expect("a boot");
                let label = Label::Node(node);
                let links = Links::new();
                let faults = Faults::new();
                let net = Net::start(
                    NetConfig {
                        tls: certs::load(cert_dir.path(), &label).expect("node certs"),
                        label,
                        boot,
                        peers: ALL_NODES.iter().copied().filter(|n| *n != node).collect(),
                        dial: NetConfig::dial_lower(node, &addrs),
                        protocol_version: PROTOCOL_VERSION,
                        admin_apps: BTreeSet::new(),
                        faults: Arc::clone(&faults),
                    },
                    &links,
                    Some(listener),
                )
                .expect("the node's net starts");
                let cut: Arc<dyn ConfigStore> =
                    CutStore::new(Arc::clone(&store), Arc::clone(&faults));
                let db = rt
                    .block_on(Db::open_with(
                        DbConfig {
                            local: vec![node],
                            links: Some(links),
                            boot: Some(boot),
                            failover,
                            ..config(dir.path())
                        },
                        cut,
                        rt.handle().clone(),
                        budgets(),
                    ))
                    .unwrap_or_else(|e| panic!("node {}'s process opens: {e}", node.0));
                Proc {
                    db,
                    _net: net,
                    faults,
                    boot,
                }
            })
            .collect()
    };
    let rig = Rig {
        rt,
        store,
        cert_dir: cert_dir.path().to_path_buf(),
        _dirs: vec![Box::new(dir), Box::new(cert_dir)],
        addrs,
        procs,
        partitions,
    };
    rig
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

/// V7 and B1: stop node 1's process for longer than G. Every partition it owned moves to a
/// node that runs, nothing else moves, and a put lands on each moved partition through an
/// app linked to the two that run. Returns the time from the stop to the last move.
fn stopped_owner_every_partition_moves(method: &str, partitions: u32) -> Duration {
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

    // A cold start raises the alert while each node has no grant yet (heard, not serving):
    // only alerts after the stop count against the running nodes.
    let setup_alerts = logged(method, "planner_owner_alive_not_serving").len();
    let stopped = Instant::now();
    rig.stop(0);
    until(
        "every partition of node 1 moves",
        Duration::from_secs(60),
        || moving.iter().all(|p| rig.owner(*p) != dead),
    );
    let took = stopped.elapsed();

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
        "own81_b1_time_to_new_primary"
    );
    took
}

/// V7 and B1 at a small count: 6 partitions, node 1 owns 2. Integration (~10 s): waits out a
/// 3 s grant, its margins, a look and the promotes. The same row at 512 is the lead's to
/// schedule on a quiet host: `stopped_owner_every_partition_moves(METHOD, 512)`.
/// Cut: `judge` answers `Alive` for an expired grant; nothing moves, red at the first wait.
#[config_log::retcd_test]
fn own81_v7_b1_a_stopped_owner_has_every_partition_moved() {
    const METHOD: &str = "own81_v7_b1_a_stopped_owner_has_every_partition_moved";
    let took = stopped_owner_every_partition_moves(METHOD, 6);
    println!(
        "own81_b1 partitions=6 time_to_new_primary_ms={}",
        took.as_millis()
    );
    let bound =
        budgets().grant_millis + budgets().clock_error_millis + budgets().dispatch_margin_millis;
    assert!(
        took >= Duration::from_millis(bound),
        "moved {took:?} after the stop, before the grant could have run out ({bound} ms)"
    );
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
/// Only nodes 2 and 3 run a planner: a planner never judges its own node, so with node 1
/// holding the planner grant these rows would pass with nothing judged.
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
    std::thread::sleep(Duration::from_millis(2 * budgets().grant_millis));
    assert_eq!(rig.record(1), before, "partitions/1 unchanged");
    assert!(!rig.grants()[&NodeId(1)].frozen, "nothing froze grants/1");
    assert!(logged(method, "planner_promote_decision").is_empty());
    assert!(logged(method, "planner_node_silent").is_empty());
}

/// V4: node 1 is cut from rEtcd (`cut-control`) and nothing else. Its grant runs out; its links
/// still answer. Integration (~13 s): waits out the grant, then two more.
/// Cut: skip the `heard_within` test in `judge`; partition 1 moves, red.
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
/// Cut: as V4.
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
    std::thread::sleep(Duration::from_millis(3 * budgets().grant_millis));
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
/// after node 1 stops. Each partition commits once: one sweep's promote lands, the other's
/// reports it lost the race, and the generation rose by one.
/// Integration (~10 s): waits out the grant, then two sweeps.
/// Cut: none in C1 code: the `Fencing` CAS in `promote_looked` decides it, and its own rows pin
/// that. This row pins that the planner adds no second write around it.
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
        let (a, b) = rig.rt.block_on(async {
            tokio::join!(planners[0].sweep(&mut ma), planners[1].sweep(&mut mb))
        });
        if !a.is_empty() || !b.is_empty() {
            break [a, b];
        }
        assert!(Instant::now() < deadline, "no sweep acted");
        std::thread::sleep(Duration::from_millis(200));
    };
    let committed: Vec<_> = answers
        .iter()
        .flatten()
        .filter(|(partition, answer)| *partition == PartitionId(1) && answer.is_ok())
        .collect();
    assert_eq!(committed.len(), 1, "one commit: {answers:?}");
    let after = rig.record(1);
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
/// The row holds node 2 in that state on purpose: cut from rEtcd (`cut-control`) once every
/// process is open and before any is ready, so its recovery cannot replace the old record,
/// while its links still answer. Left to the start's own timing the state lasts from none to a
/// few hundred ms, and the row saw it or not by chance (lead's gate at dc2b1b4). Then node 2
/// is let go and takes its grant, and still no alert.
/// Integration (~15 s): the old grant runs out, a few sweeps, then the owners' grants.
/// Cut: drop the "no grant of its own" check in `sweep`; node 2's old record runs out while it
/// answers, the alert is said for it, red. Or judge "of its own" by the record's presence, not
/// its boot (as before this row): the same, red.
#[config_log::retcd_test]
fn own81_cold_start_raises_no_alert_before_an_owners_first_grant() {
    const METHOD: &str = "own81_cold_start_raises_no_alert_before_an_owners_first_grant";
    let rig = opened(3, [Some(failover()); 3]);
    rig.procs[1].faults.apply("cut-control\n");
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
    assert_eq!(held.len(), 1, "node 2 heard with no grant of its own, said once: {held:?}");
    assert!(
        rig.grants()
            .get(&NodeId(2))
            .is_none_or(|grant| grant.boot != node2_boot),
        "node 2 stayed without a grant of its own while cut"
    );

    rig.resume(1);
    rig.wait_ready();
    rig.wait_for_owner_grants();
    std::thread::sleep(5 * failover().poll);
    assert_eq!(
        logged(METHOD, "planner_owner_alive_not_serving"),
        Vec::<serde_json::Value>::new(),
        "after node 2 took its grant"
    );
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
/// planner never judges its own node), and node 2 never holds the grant, red.
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
