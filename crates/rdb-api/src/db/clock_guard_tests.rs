//! #80 S2c: the clock guard over real links. Each row runs real `Net`s (TLS, HTTP/2) and real
//! `Db`s, one `Links` and one clock per "process", in this test process. A process's clock is
//! made wrong with [`Links::with_clock_offset`] or `HostClock::step_back`, the same hooks
//! `rdb_node --clock-offset-ms` and `--clock-step-file` use.

use super::reopen_tests::{
    config, fast, open_app, partition_and_grant, runtime, written_and_closed,
};
use super::*;
use crate::clock::ClockGuard;
use crate::net::{certs, faults::Faults, wire::Label, Net, NetConfig, PROTOCOL_VERSION};
use rdb_core::contracts::authority::DenyReason;

/// One "process": a node's `Db`, its `Net`, its `Links`.
struct Proc {
    db: Db,
    _net: Arc<Net>,
    links: Arc<Links>,
    faults: Arc<Faults>,
}

impl Proc {
    fn guard(&self) -> &'static ClockGuard {
        self.links.clock().guard().expect("a guarded clock")
    }
}

struct Rig {
    rt: tokio::runtime::Runtime,
    store: Arc<dyn ConfigStore>,
    /// The temp dirs, kept until the rig drops.
    _dirs: Vec<Box<dyn std::any::Any>>,
    cert_dir: std::path::PathBuf,
    addrs: BTreeMap<NodeId, String>,
    procs: Vec<Proc>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for proc in &mut self.procs {
            proc.db.shutdown();
        }
    }
}

const APP: &str = "s2c-app";

/// Budgets with a grant short enough for a row to wait out.
fn short_grant() -> Budgets {
    Budgets {
        grant_millis: 3_000,
        ..fast()
    }
}

/// A formed database, then three node processes with these clock offsets.
fn rig(offsets: [i64; 3], budgets: Budgets) -> Rig {
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
    written_and_closed(&rt, dir.path(), &store);
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
            .zip(offsets)
            .map(|((node, listener), offset)| {
                let node = *node;
                let boot = next_boot(dir.path()).expect("a boot");
                let label = Label::Node(node);
                let links = Links::with_clock_offset(offset);
                let faults = Faults::new();
                let net = Net::start(
                    NetConfig {
                        tls: certs::load(cert_dir.path(), &label).expect("node certs"),
                        label,
                        boot,
                        peers: ALL_NODES.iter().copied().filter(|n| *n != node).collect(),
                        dial: NetConfig::dial_lower(node, &addrs),
                        protocol_version: PROTOCOL_VERSION,
                        faults: Arc::clone(&faults),
                    },
                    &links,
                    Some(listener),
                )
                .expect("the node's net starts");
                let db = rt
                    .block_on(Db::open_with(
                        DbConfig {
                            local: vec![node],
                            links: Some(Arc::clone(&links)),
                            boot: Some(boot),
                            ..config(dir.path())
                        },
                        Arc::clone(&store),
                        rt.handle().clone(),
                        budgets,
                    ))
                    .unwrap_or_else(|e| panic!("node {}'s process opens: {e}", node.0));
                Proc {
                    db,
                    _net: net,
                    links,
                    faults,
                }
            })
            .collect()
    };
    Rig {
        rt,
        store,
        cert_dir: cert_dir.path().to_path_buf(),
        _dirs: vec![Box::new(dir), Box::new(cert_dir)],
        addrs,
        procs,
    }
}

impl Rig {
    /// An app-only `Db` on a clock `offset` off, linked to `peers` only.
    fn app(&self, offset: i64, peers: &[NodeId]) -> (Db, Arc<Net>, Arc<Links>) {
        let _entered = self.rt.enter();
        let label = Label::App(APP.to_owned());
        let links = Links::with_clock_offset(offset);
        let dial = peers
            .iter()
            .map(|node| (*node, self.addrs[node].clone()))
            .collect();
        let net = Net::start(
            NetConfig {
                tls: certs::load(&self.cert_dir, &label).expect("app certs"),
                label,
                boot: BootId(0),
                peers: peers.iter().copied().collect(),
                dial,
                protocol_version: PROTOCOL_VERSION,
                faults: Faults::new(),
            },
            &links,
            None,
        )
        .expect("the app's net starts");
        let patience = crate::host::test_patience(Duration::from_secs(30));
        self.rt
            .block_on(net.wait_up(peers, patience))
            .expect("the app reaches its peers");
        let db = open_app(&self.rt, &self.store, &links, ClientId(2));
        (db, net, links)
    }

    fn ready(&self, node: usize) {
        self.procs[node]
            .db
            .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
            .expect("the owner is ready");
    }

    fn status(&self, node: usize) -> crate::host::NodeStatus {
        self.procs[node]
            .db
            .node_status()
            .pop()
            .expect("the process runs its node")
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

fn logged(method: &str, message: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == message)
        .collect()
}

/// Put until one lands, or panic with the last refusal.
fn put_lands(db: &Db, within: Duration) {
    let deadline = Instant::now() + crate::host::test_patience(within);
    loop {
        match db.put(b"after", b"1", None) {
            Ok(_) => return,
            Err(refused) => assert!(
                Instant::now() < deadline,
                "no put landed within {within:?}: {refused:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// 13c, the Net half at K=5: five real links-and-guards, node 3 at +500. Node 3's four
/// Breaches leave it lost, with no Agree ever; the other four outvote the one Breach against
/// them and stay established. Integration (~1 s).
#[config_log::retcd_test]
fn row_13c_five_nodes_with_node_3_at_plus_500_only_node_3_loses_the_bound() {
    const METHOD: &str = "row_13c_five_nodes_with_node_3_at_plus_500_only_node_3_loses_the_bound";
    let rt = runtime();
    let _entered = rt.enter();
    let cert_dir = config_testkit::fs::temp_dir();
    let nodes: Vec<NodeId> = (1..=5).map(NodeId).collect();
    let labels: Vec<Label> = nodes.iter().map(|node| Label::Node(*node)).collect();
    certs::generate(cert_dir.path(), &labels).expect("certs");
    let listeners: Vec<tokio::net::TcpListener> = nodes
        .iter()
        .map(|_| rt.block_on(config_testkit::ports::ephemeral_listener()).0)
        .collect();
    let addrs: BTreeMap<NodeId, String> = nodes
        .iter()
        .zip(&listeners)
        .map(|(node, listener)| (*node, listener.local_addr().expect("bound").to_string()))
        .collect();
    let mut inboxes = Vec::new();
    let running: Vec<(NodeId, Arc<Net>, Arc<Links>)> = nodes
        .iter()
        .zip(listeners)
        .map(|(node, listener)| {
            let node = *node;
            let label = Label::Node(node);
            let links = Links::with_clock_offset(if node == NodeId(3) { 500 } else { 0 });
            // A `Net` serves its node only once that node takes mail, as a `Db` arranges.
            let (mail, inbox) = mpsc::channel();
            links.register(node, mail, BootId(u64::from(node.0)));
            inboxes.push(inbox);
            let net = Net::start(
                NetConfig {
                    tls: certs::load(cert_dir.path(), &label).expect("node certs"),
                    label,
                    boot: BootId(u64::from(node.0)),
                    peers: nodes.iter().copied().filter(|n| *n != node).collect(),
                    dial: NetConfig::dial_lower(node, &addrs),
                    protocol_version: PROTOCOL_VERSION,
                    faults: Faults::new(),
                },
                &links,
                Some(listener),
            )
            .expect("the node's net starts");
            (node, net, links)
        })
        .collect();
    let guard = |node: NodeId| {
        running
            .iter()
            .find(|(n, ..)| *n == node)
            .and_then(|(.., links)| links.clock().guard())
            .expect("a guard")
    };
    let breaches_of_three = || {
        logged(METHOD, "clock_verdict")
            .iter()
            .filter(|line| line["peer"] == 3 && line["verdict"] == "Breach")
            .count()
    };
    until("the verdicts settle", Duration::from_secs(15), || {
        guard(NodeId(3)).lost()
            && nodes
                .iter()
                .filter(|node| **node != NodeId(3))
                .all(|node| guard(*node).established())
            && breaches_of_three() >= 4
    });
    assert!(!guard(NodeId(3)).established());
    assert_eq!(
        breaches_of_three(),
        4,
        "each of the other four says so once"
    );
}

/// 13c, the Host half, and 13j's "an ahead node is never re-anchored". Node 1, owner, jumps
/// 500 ms ahead while it holds its grant. Its two peers agree with each other and Breach it,
/// so node 1's guard reports the loss, it fences `ClockUnbounded`, its clock does not move
/// back, and nodes 2 and 3 stay established. Cut: `HostClock::sample` reports established
/// always, red at the fence.
/// Integration (~5 s): three real processes until node 1 holds.
#[config_log::retcd_test]
fn row_13c_an_owner_out_of_bound_ahead_fences_clock_unbounded_and_is_never_reanchored() {
    const METHOD: &str =
        "row_13c_an_owner_out_of_bound_ahead_fences_clock_unbounded_and_is_never_reanchored";
    let rig = rig([0, 0, 0], short_grant());
    rig.ready(0);
    rig.procs[0].db.put(b"b", b"2", None).expect("a put");
    until(
        "every node has an agreement",
        Duration::from_secs(10),
        || rig.procs.iter().all(|proc| proc.guard().established()),
    );

    let clock = rig.procs[0].links.clock();
    clock.step_forward(500);
    until("node 1 fences", Duration::from_secs(20), || {
        rig.status(0).fence == Some(DenyReason::ClockUnbounded)
    });
    assert!(rig.status(0).clock_bound_lost, "NodeStatus names the loss");
    until(
        "nodes 2 and 3 report node 1's Breach and hold their bound",
        Duration::from_secs(10),
        || {
            rig.procs[1..].iter().all(|proc| proc.guard().established())
                && logged(METHOD, "clock_verdict")
                    .iter()
                    .filter(|line| line["peer"] == 1 && line["verdict"] == "Breach")
                    .count()
                    >= 2
        },
    );
    assert!(
        !logged(METHOD, "clock_bound_lost").is_empty(),
        "the operator's line"
    );
    assert!(
        logged(METHOD, "clock_reanchored").is_empty(),
        "a node ahead of its peers is never moved"
    );
}

/// 13d, the guard's reason to exist on a promote. Node 2 (the target) and the app are both
/// 2500 ms ahead; the app reaches node 2 and 3 only, so its own bound holds and its own expiry
/// proof passes early. Node 2's guard is lost, so its `bind_routed_fence` refuses with
/// `clock_bound_lost`, nothing commits, node 1 keeps its grant, and the promote reports it.
/// Cut: drop the bound check in `Msg::FenceBound`, 13d goes red the way 13e passes.
/// Integration (~13 s): waits the promote's whole deadline for the report.
#[config_log::retcd_test]
fn row_13d_a_target_out_of_bound_refuses_the_fence_and_the_promote_says_so() {
    const METHOD: &str = "row_13d_a_target_out_of_bound_refuses_the_fence_and_the_promote_says_so";
    let rig = rig([0, 2_500, 0], short_grant());
    rig.ready(0);
    until("node 2 reports the loss", Duration::from_secs(10), || {
        rig.procs[1].guard().lost()
    });
    let (app, _net, _links) = rig.app(2_500, &[NodeId(2), NodeId(3)]);
    let promoted = app.promote(NodeId(2), None);
    let refused = promoted.expect_err("no commit while the target is out of bound");
    assert!(
        refused.detail.contains("routed_fence_refused"),
        "the promote names where to look: {refused:?}"
    );
    let reasons: Vec<_> = logged(METHOD, "routed_fence_refused")
        .iter()
        .map(|line| line["reason"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert!(
        reasons.iter().any(|reason| reason == "clock_bound_lost"),
        "{reasons:?}"
    );
    let (record, ..) = partition_and_grant(&rig.rt, &rig.store);
    assert_eq!(record.owner, NodeId(1), "no owner change: {record:?}");
}

/// 13e, the positive control for 13d: the same promote with node 2's guard off commits while
/// node 1's honest clock is still more than `epsilon + delta` short of the frozen expiry.
/// That is the double-primary window the guard closes. If this row stops committing early,
/// 13d no longer shows anything.
/// Integration (~4 s).
#[config_log::retcd_test]
fn row_13e_with_the_guard_off_the_same_promote_commits_early() {
    let budgets = short_grant();
    let rig = rig([0, 2_500, 0], budgets);
    rig.ready(0);
    rig.procs[1].guard().disable();
    let (app, _net, _links) = rig.app(2_500, &[NodeId(2), NodeId(3)]);
    let promoted = app.promote(NodeId(2), None).expect("the promote commits");
    let honest = rig.procs[0].links.clock().now().0;
    assert_eq!(promoted.owner, NodeId(2), "{promoted:?}");
    let (.., grant, _) = partition_and_grant(&rig.rt, &rig.store);
    assert!(grant.frozen, "the promote froze node 1's grant: {grant:?}");
    let expiry = u64::try_from(grant.expiry_utc_ms).expect("a real expiry");
    let margin = budgets.clock_error_millis + budgets.dispatch_margin_millis;
    assert!(
        honest + margin < expiry,
        "committed at {honest} on node 1's clock, expiry {expiry}: not early, so the control \
         shows nothing"
    );
}

/// 13h and its cut: the owner's clock steps back 800 ms under a live grant (a frozen-clock
/// pause). It fences `ClockUnbounded`, its peers prove it behind, it moves forward, takes a
/// new grant by self-replace, and writes resume with no restart.
/// Cut: `Host::reanchor_clock` returns at once; the node stays fenced and the put is refused
/// to the end of its patience.
/// Integration (~8 s).
#[config_log::retcd_test]
fn row_13h_a_paused_owner_fences_reanchors_replaces_its_grant_and_serves_again() {
    const METHOD: &str =
        "row_13h_a_paused_owner_fences_reanchors_replaces_its_grant_and_serves_again";
    let rig = rig([0, 0, 0], short_grant());
    rig.ready(0);
    rig.procs[0].db.put(b"b", b"2", None).expect("a put");
    until(
        "every node has an agreement",
        Duration::from_secs(10),
        || rig.procs.iter().all(|proc| proc.guard().established()),
    );
    let (.., before, _) = partition_and_grant(&rig.rt, &rig.store);

    rig.procs[0].links.clock().step_back(800);
    until("node 1 fences", Duration::from_secs(20), || {
        rig.status(0).fence == Some(DenyReason::ClockUnbounded)
    });
    put_lands(&rig.procs[0].db, Duration::from_secs(30));

    let moved = logged(METHOD, "clock_reanchored");
    assert_eq!(moved.len(), 1, "{moved:?}");
    let by = moved[0]["by_ms"].as_u64().expect("by_ms");
    assert!((680..=800).contains(&by), "by the proved lower bound: {by}");
    let status = rig.status(0);
    assert_eq!(status.fence, None, "{status:?}");
    let (.., after, _) = partition_and_grant(&rig.rt, &rig.store);
    assert!(
        after.grant > before.grant,
        "a new grant: {after:?} after {before:?}"
    );
    assert!(!logged(METHOD, "clock_stepped_back").is_empty());
}

/// 13i: the same pause with a promote frozen during it. Node 1 re-anchors but finds its grant
/// frozen: `SelfReplace::Never`, it stays fenced, and it serves nothing. `replace_stopped` with
/// `fence_polling` off is what the operator's line reads: `rdb_dev`'s "nothing in M9 replaces
/// it ... quit and start rdb_dev again", pinned by its own `ready_step` row.
/// Integration (~12 s).
#[config_log::retcd_test]
fn row_13i_a_paused_owner_whose_grant_was_frozen_stops_and_never_replaces() {
    let rig = rig([0, 0, 0], short_grant());
    rig.ready(0);
    rig.procs[0].db.put(b"b", b"2", None).expect("a put");
    until(
        "every node has an agreement",
        Duration::from_secs(10),
        || rig.procs.iter().all(|proc| proc.guard().established()),
    );

    rig.procs[0].links.clock().step_back(800);
    let (app, _net, _links) = rig.app(0, &[NodeId(2), NodeId(3)]);
    let promoted = app.promote(NodeId(2), None).expect("the promote commits");
    assert_eq!(promoted.owner, NodeId(2), "{promoted:?}");

    until(
        "node 1 gives up on its grant",
        Duration::from_secs(20),
        || rig.status(0).replace_stopped,
    );
    let status = rig.status(0);
    assert!(status.fence.is_some(), "still fenced: {status:?}");
    assert!(!status.fence_polling, "nothing will lift it: {status:?}");
    assert_ne!(status.admits, Some(true), "{status:?}");
    let refused = rig.procs[0]
        .db
        .put(b"c", b"3", None)
        .expect_err("a frozen node serves nothing");
    assert!(refused.error.no_mutation, "{refused:?}");
}

/// 13j: after a re-anchor the grant is new, and the first admission is proved against the
/// peers' clocks: the old expiry, `epsilon` and `delta` are past on node 2's clock before node
/// 1 admits again. Runs the 13h flow and reads node 2's clock at the first accepted put.
/// Integration (~8 s).
#[config_log::retcd_test]
fn row_13j_the_first_admission_after_a_reanchor_is_past_the_old_expiry_on_a_peers_clock() {
    let budgets = short_grant();
    let rig = rig([0, 0, 0], budgets);
    rig.ready(0);
    rig.procs[0].db.put(b"b", b"2", None).expect("a put");
    until(
        "every node has an agreement",
        Duration::from_secs(10),
        || rig.procs.iter().all(|proc| proc.guard().established()),
    );

    rig.procs[0].links.clock().step_back(800);
    // Read at the fence, before the self-replace overwrites it: the last expiry node 1 renewed.
    until("node 1 fences", Duration::from_secs(20), || {
        rig.status(0).fence.is_some()
    });
    let (.., grant, _) = partition_and_grant(&rig.rt, &rig.store);
    put_lands(&rig.procs[0].db, Duration::from_secs(30));
    let peer = rig.procs[1].links.clock().now().0;
    let margin =
        i64::try_from(budgets.clock_error_millis + budgets.dispatch_margin_millis).expect("small");
    let peer = i64::try_from(peer).expect("a tick");
    assert!(
        peer > grant.expiry_utc_ms + margin,
        "admitted at {peer} on node 2's clock, old expiry {} + {margin}",
        grant.expiry_utc_ms
    );
}

/// Row 20's skew half (S2d's row carries the arithmetic). Nodes 1 and 2 are 180 ms apart, just
/// under 2-epsilon, each within epsilon of node 3, so the guard lets every one of them keep its
/// bound. A promote from the 90-ms-fast side must still wait until the frozen expiry is past on
/// node 1's own clock, the one that stopped admitting at `E - epsilon - delta`. Row 13e is the
/// positive control: past `2(epsilon + delta)` with the guard off, it commits early.
/// Integration (~6 s): waits out a 3 s grant.
#[config_log::retcd_test]
fn row_20_skew_half_a_pair_just_under_two_epsilon_keeps_its_bound_and_never_overlaps() {
    let rig = rig([-90, 90, 0], short_grant());
    rig.ready(0);
    until(
        "every node has an agreement",
        Duration::from_secs(10),
        || rig.procs.iter().all(|proc| proc.guard().established()),
    );
    let (app, _net, _links) = rig.app(90, &[NodeId(2), NodeId(3)]);
    let promoted = app.promote(NodeId(2), None).expect("the promote commits");
    assert_eq!(promoted.owner, NodeId(2), "{promoted:?}");
    let honest = rig.procs[0].links.clock().now().0;
    let (.., grant, _) = partition_and_grant(&rig.rt, &rig.store);
    assert!(grant.frozen, "{grant:?}");
    let expiry = u64::try_from(grant.expiry_utc_ms).expect("a real expiry");
    assert!(
        honest > expiry,
        "committed at {honest} on node 1's clock, expiry {expiry}: an overlap"
    );
    assert!(
        rig.procs.iter().all(|proc| proc.guard().established()),
        "a skew under 2-epsilon loses no bound"
    );
}

/// Design 5.5: `NodeStatus.qualifying_copy` tells a cut-off primary from a busy one. Node 1's
/// links to both copies are held under a live write, so no copy can acknowledge: the field
/// reads `Some(false)` where it read `Some(true)`. Integration (~6 s): the put's own timeout.
#[config_log::retcd_test]
fn row_5_5_qualifying_copy_goes_false_when_no_copy_can_acknowledge() {
    let rig = rig([0, 0, 0], short_grant());
    rig.ready(0);
    rig.procs[0].db.put(b"b", b"2", None).expect("a put");
    assert_eq!(rig.status(0).qualifying_copy, Some(true));
    rig.procs[0].faults.apply("hold 2\nhold 3\n");
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| rig.procs[0].db.put(b"c", b"3", None));
        until("no copy qualifies", Duration::from_secs(20), || {
            rig.status(0).qualifying_copy == Some(false)
        });
        rig.procs[0].faults.apply("");
        drop(writer.join().expect("the writer thread"));
    });
}
