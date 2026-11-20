//! #80 S4 T11a: a data node added at runtime is admitted without restarting the others. Five
//! declared node "processes" in this test process, as `formation_tests` runs them (a `Db`, a
//! `Net` and a `Links` each, real TLS links, node N on 127.0.0.N, each with its own folder),
//! then `admin::add_node` writes `nodes/6`, `certs::issue_node` signs node 6 with the same CA,
//! and node 6's process opens with peer addresses for nodes 1 to 5. Every node's `nodes/`
//! watcher (`admin::watch_members`) admits node 6 on its links (`Net::set_members`).

use super::reopen_tests::{config, fast, runtime};
use super::*;
use crate::net::faults::Faults;
use crate::net::{certs, wire::Label, LinkState, Net, NetConfig, PROTOCOL_VERSION};
use crate::test_store::{CutCalls, PrefixCut};
use rdb_core::contracts::ids::ReplicaRole;

/// Partitions the rows declare: more than one per node of five.
const PARTITIONS: u32 = 10;

/// The node the rows hold: its `nodes/` watcher is cut until node 6 has dialled it.
const HELD: NodeId = NodeId(3);

/// The node the rows add.
const ADDED: NodeId = NodeId(6);

/// One node process, its own folder kept alive with it.
struct Proc {
    node: NodeId,
    db: Db,
    net: Arc<Net>,
    boot: BootId,
    _dir: Box<dyn std::any::Any>,
}

struct Rig {
    rt: tokio::runtime::Runtime,
    store: Arc<dyn ConfigStore>,
    cert_dir: PathBuf,
    _cert_dir: Box<dyn std::any::Any>,
    /// Where each node listens, node 6's included: node N on 127.0.0.N.
    addrs: BTreeMap<NodeId, String>,
    procs: Vec<Proc>,
    /// [`HELD`]'s `nodes/` lists, cut and lifted by the row.
    held: Arc<PrefixCut>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for proc in &mut self.procs {
            proc.db.shutdown();
        }
    }
}

impl Rig {
    fn of(&self, node: NodeId) -> &Proc {
        self.procs
            .iter()
            .find(|proc| proc.node == node)
            .unwrap_or_else(|| panic!("node {} runs", node.0))
    }

    fn record(&self, partition: PartitionId) -> (PartitionRecord, Vec<(NodeId, ReplicaRole)>) {
        let (record, _, members, ..) = self
            .rt
            .block_on(admin::read_partition(&*self.store, partition))
            .unwrap_or_else(|e| panic!("partitions/{}: {e}", partition.0));
        let members = members
            .unwrap_or_default()
            .iter()
            .map(|member| (member.node, member.role))
            .collect();
        (record, members)
    }
}

/// Open `node`'s process on a new folder, listening on `listener`, linked to `peers` (dialling
/// the lower ids), its control calls through `store`.
fn open_node(
    rig: &Rig,
    store: Arc<dyn ConfigStore>,
    node: NodeId,
    listener: tokio::net::TcpListener,
    peers: &BTreeMap<NodeId, String>,
) -> Proc {
    let _entered = rig.rt.enter();
    let dir = config_testkit::fs::temp_dir();
    let boot = next_boot(dir.path()).expect("a boot");
    let label = Label::Node(node);
    let links = Links::new();
    let net = Net::start(
        NetConfig {
            tls: certs::load(&rig.cert_dir, &label).expect("node certs"),
            label,
            boot,
            peers: peers.keys().copied().filter(|n| *n != node).collect(),
            dial: NetConfig::dial_lower(node, peers),
            protocol_version: PROTOCOL_VERSION,
            admin_apps: BTreeSet::new(),
            faults: Faults::new(),
        },
        &links,
        Some(listener),
    )
    .expect("the node's net starts");
    let db = rig
        .rt
        .block_on(Db::open_with(
            DbConfig {
                local: vec![node],
                links: Some(links),
                boot: Some(boot),
                addresses: Some(NodeAddresses {
                    listen: rig.addrs[&node].clone(),
                    peers: peers.clone(),
                    app: None,
                }),
                partitions: Some(PARTITIONS),
                ..config(dir.path())
            },
            store,
            rig.rt.handle().clone(),
            fast(),
        ))
        .unwrap_or_else(|e| panic!("node {}'s process opens: {e}", node.0));
    Proc {
        node,
        db,
        net,
        boot,
        _dir: Box::new(dir),
    }
}

/// Nodes 1 to 5 declared and ready, node 6's listener bound on 127.0.0.6 and not yet used.
fn five_ready() -> (Rig, tokio::net::TcpListener) {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let cert_dir = config_testkit::fs::temp_dir();
    let labels: Vec<Label> = (1..=5).map(|n| Label::Node(NodeId(n))).collect();
    certs::generate(cert_dir.path(), &labels).expect("certs");
    let mut listeners: BTreeMap<NodeId, tokio::net::TcpListener> = (1..=6)
        .map(|n| {
            let host = std::net::Ipv4Addr::new(127, 0, 0, u8::try_from(n).expect("an id"));
            (
                NodeId(n),
                rt.block_on(config_testkit::ports::ephemeral_listener_on(host))
                    .0,
            )
        })
        .collect();
    let addrs: BTreeMap<NodeId, String> = listeners
        .iter()
        .map(|(node, listener)| (*node, listener.local_addr().expect("bound").to_string()))
        .collect();
    let declaration = admin::Declaration {
        nodes: (1..=5)
            .map(|n| admin::NodeDecl {
                node: NodeId(n),
                addr: addrs[&NodeId(n)].clone(),
                machine: None,
            })
            .collect(),
        partitions: PARTITIONS,
    };
    match rt.block_on(admin::declare(&store, &declaration)) {
        Ok(admin::Declared::Created(_)) => {}
        other => panic!("the declaration is written: {other:?}"),
    }
    let held = PrefixCut::new(
        Arc::clone(&store),
        ControlPrefix::Nodes.encode(),
        CutCalls::Lists,
    );
    held.lift();
    let mut rig = Rig {
        rt,
        store,
        cert_dir: cert_dir.path().to_path_buf(),
        _cert_dir: Box::new(cert_dir),
        addrs: addrs.clone(),
        procs: Vec::new(),
        held,
    };
    let five: BTreeMap<NodeId, String> = addrs.into_iter().filter(|(n, _)| n.0 <= 5).collect();
    for n in 1..=5 {
        let node = NodeId(n);
        let store: Arc<dyn ConfigStore> = if node == HELD {
            rig.held.clone()
        } else {
            Arc::clone(&rig.store)
        };
        let listener = listeners.remove(&node).expect("a listener");
        let proc = open_node(&rig, store, node, listener, &five);
        rig.procs.push(proc);
    }
    for proc in &rig.procs {
        proc.db
            .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
            .unwrap_or_else(|e| panic!("node {} is ready: {e}", proc.node.0));
    }
    let six = listeners.remove(&ADDED).expect("node 6's listener");
    (rig, six)
}

/// `true` within `within` (scaled by `RETCD_TEST_DEADLINE_SCALE`), polled every 20 ms.
fn until(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + crate::host::test_patience(within);
    while !done() {
        assert!(Instant::now() < deadline, "not within {within:?}: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A group that maps to `partition` of `db`.
fn group_in(db: &Db, partition: PartitionId) -> AffinityId {
    (1..10_000)
        .map(AffinityId)
        .find(|group| db.partition_for(*group) == partition)
        .expect("some group maps to every partition")
}

/// Put `object` in `group` through `db` until it lands. Just after a commit re-plans the
/// partition, its owner refuses with nothing written (`UNAVAILABLE` or `PROTECTION_PAUSED`,
/// `no_mutation`) until the new generation serves, and the retry rule says send again; as
/// `reopen_tests` does after a promote. Any other refusal, or one past 10 s, fails the row.
fn put_landing(db: &Db, group: AffinityId, object: &[u8], value: &[u8]) {
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(10));
    loop {
        match db.put_in(group, object, value, None) {
            Ok(_) => return,
            Err(refused)
                if refused.error.no_mutation
                    && matches!(
                        refused.error.kind,
                        ErrorKind::Unavailable | ErrorKind::ProtectionPaused
                    )
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(refused) => panic!("a put to the partition: {refused:?}"),
        }
    }
}

/// What [`admitted`] did: the partition it moved a copy of onto node 6, and from which node.
struct Admitted {
    rig: Rig,
    partition: PartitionId,
    from: NodeId,
}

/// The M7 scenario up to the move landing, asserting as it goes:
///
/// 1. Five nodes run; node 3's `nodes/` lists are cut, so its watcher is held.
/// 2. `admin::add_node` writes `nodes/6`; nodes 1, 2, 4 and 5 admit it.
/// 3. Node 6 is issued a certificate from the same CA and opens, dialling nodes 1 to 5. Its
///    links to 1, 2, 4 and 5 come up; its dial to node 3 is refused "not a live member".
/// 4. Node 3's cut is lifted: its watcher admits node 6 and the link comes up within three dial
///    backoffs (each at most 1 s). Nodes 1 to 5 run at the boots they opened at.
/// 5. Node 3's clock guard takes K = 6.
/// 6. `move_copy_in` moves partition p's (p != 1) copy on a regular secondary x to node 6, until
///    `done`; a put to p lands, and `partitions/p` pins node 6 regular and leaves x out.
fn admitted() -> Admitted {
    let (mut rig, six) = five_ready();
    rig.held.recut();
    let added = rig
        .rt
        .block_on(admin::add_node(&rig.store, &rig.addrs[&ADDED], None))
        .expect("node 6 is added");
    assert_eq!(added.node, ADDED);
    for n in [1, 2, 4, 5] {
        let net = Arc::clone(&rig.of(NodeId(n)).net);
        until(
            &format!("node {n} admits node 6"),
            Duration::from_secs(10),
            || net.members().contains(&ADDED),
        );
    }
    assert!(
        !rig.of(HELD).net.members().contains(&ADDED),
        "node 3's watcher is held"
    );

    certs::issue_node(&rig.cert_dir, ADDED).expect("node 6's certificate");
    let all = rig.addrs.clone();
    let store = Arc::clone(&rig.store);
    let six = open_node(&rig, store, ADDED, six, &all);
    rig.procs.push(six);
    let six_net = Arc::clone(&rig.of(ADDED).net);
    rig.rt
        .block_on(six_net.wait_up(
            &[NodeId(1), NodeId(2), NodeId(4), NodeId(5)],
            crate::host::test_patience(Duration::from_secs(10)),
        ))
        .expect("node 6 links to the nodes that admitted it");
    until(
        "node 6's dial to node 3 is refused as not a live member",
        Duration::from_secs(10),
        || {
            matches!(
                six_net.states().get(&HELD),
                Some(LinkState::Refused(reason)) if reason.contains("not a live member")
            )
        },
    );

    rig.held.lift();
    // Polled, not `Net::wait_up`: that gives up at the first refusal, and this one heals.
    until(
        "node 6's link to node 3 is up within three dial backoffs of the release",
        Duration::from_secs(3),
        || matches!(six_net.states().get(&HELD), Some(LinkState::Up(_))),
    );
    for n in 1..=5 {
        let proc = rig.of(NodeId(n));
        assert_eq!(
            six_net.boot_of(NodeId(n)),
            Some(proc.boot),
            "node {n} runs at the boot it opened at"
        );
    }
    let k = rig
        .of(HELD)
        .db
        .clock
        .guard()
        .expect("a node's clock has a guard")
        .cluster_k();
    assert_eq!(k, 6, "node 3's clock guard counts node 6");

    let partition = (2..=PARTITIONS)
        .map(PartitionId)
        .find(|p| {
            let (record, members) = rig.record(*p);
            record.owner != NodeId(1)
                && members
                    .iter()
                    .any(|(_, role)| *role == ReplicaRole::RegularSecondary)
        })
        .expect("a partition other than 1, not owned by node 1, with a regular secondary");
    let (record, members) = rig.record(partition);
    let from = members
        .iter()
        .find(|(node, role)| *role == ReplicaRole::RegularSecondary && *node != record.owner)
        .map(|(node, _)| *node)
        .expect("a regular secondary");
    let asker = &rig.of(NodeId(1)).db;
    let mut last = None;
    until(
        "the move of a copy onto node 6 is done",
        Duration::from_secs(60),
        || {
            let answer = asker.move_copy_in(partition, from, ADDED);
            let done = matches!(&answer, Ok(moved) if moved.done);
            last = Some(format!("{answer:?}"));
            if !done {
                std::thread::sleep(Duration::from_millis(200));
            }
            done
        },
    );
    tracing::info!(partition = partition.0, from = from.0, last = ?last, "s80s4_move_done");
    let group = group_in(asker, partition);
    put_landing(asker, group, b"m7", b"after-the-add");
    let (_, members) = rig.record(partition);
    assert!(
        members.contains(&(ADDED, ReplicaRole::RegularSecondary)),
        "partitions/{} pins node 6 regular: {members:?}",
        partition.0
    );
    assert!(
        !members.iter().any(|(node, _)| *node == from),
        "and leaves node {} out: {members:?}",
        from.0
    );
    Admitted {
        rig,
        partition,
        from,
    }
}

/// #80 S4 M7: a node added at runtime links and takes a copy without restarting the others.
/// The steps are [`admitted`]'s. Integration (six node processes, real TLS).
/// Cut (a): `watch_members` never calls `Net::set_members`: no node admits node 6 ("not within
/// 10s: node 1 admits node 6"), red, observed 2026-11-20.
/// Cut (b): `refusal()` has no member check: node 6's held dial to node 3 is welcomed (and
/// orphaned, node 3 having no route to it), so it is never refused ("not within 10s: node 6's
/// dial to node 3 is refused as not a live member"), red, observed 2026-11-20.
#[config_log::retcd_test]
fn s80s4_a_node_added_at_runtime_links_and_takes_a_copy_without_restarting_the_others() {
    let Admitted {
        rig,
        partition,
        from,
    } = admitted();
    let lines = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "s80s4_a_node_added_at_runtime_links_and_takes_a_copy_without_restarting_the_others",
    );
    assert!(
        lines
            .iter()
            .any(|line| line.get("@m").and_then(|m| m.as_str()) == Some("net_members_set")),
        "the admissions are logged"
    );
    tracing::info!(
        partition = partition.0,
        from = from.0,
        nodes = rig.procs.len(),
        "s80s4_m7_done"
    );
}

/// #80 S4 M7b (S-6): after M7, partition p's owner (never node 1) is stopped, as an operator
/// stops a node before a promote of one of its partitions, and p is promoted to node 6. Node 1's
/// own embedded `Db`, opened before node 6 existed and never restarted, then writes to p and the
/// put lands, and `route` names node 6 as p's owner. Integration. Stopping the owner is the
/// row's scope, not a gap: it proves an embedded `Db` writes to a node added at runtime with no
/// restart. A handoff that keeps the owner running is T12's drain (M10). Not asserted here:
/// node 6's app address in the route (`RouteInfo` has none; gated on t2's #83 B2).
/// Cut: no `nodes/` watcher in `Db::open` (`start_members` not called): no node admits node 6,
/// so the row stops at M7's admission step ("not within 10s: node 1 admits node 6") before the
/// promote, red, observed 2026-11-20.
#[config_log::retcd_test]
fn s80s4_an_embedded_node_db_writes_to_a_new_owner_without_restart() {
    let Admitted {
        mut rig, partition, ..
    } = admitted();
    let owner = rig.record(partition).0.owner;
    rig.procs
        .iter_mut()
        .find(|proc| proc.node == owner)
        .expect("the owner runs")
        .db
        .shutdown();
    let one = &rig.of(NodeId(1)).db;
    let promoted = one
        .promote_partition(partition, ADDED, Some(owner))
        .unwrap_or_else(|e| panic!("partition {} is promoted to node 6: {e}", partition.0));
    tracing::info!(?promoted, "s80s4_promoted");
    let group = group_in(one, partition);
    put_landing(one, group, b"m7b", b"on-node-6");
    assert_eq!(rig.record(partition).0.owner, ADDED);
    let route = one.route(group).expect("a route");
    assert_eq!(route.owner, ADDED, "the route names node 6");
}
