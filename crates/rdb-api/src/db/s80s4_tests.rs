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

/// #80 S4 M6, layout 3's half (T8): node 1's record is removed (layout 3, keeping its app
/// address). An open of node 1 that would write its app address is refused with "node 1 was
/// removed; ids are never reused", and writes nothing: the record's bytes and revision are
/// unchanged and no app-address marker is left in the folder. `route`'s address read still
/// reads the removed record's app address. Unit (one store, no node process).
/// Cut: `register_app_addr` without its removed check: the open is refused as "not a node
/// record this build reads" instead, red at the removal text, observed 2026-11-20.
#[config_log::retcd_test]
fn s80s4_an_open_on_a_removed_record_is_refused_and_route_keeps_its_app_address() {
    const APP: &str = "https://gw-1.example:8443";
    let rt = runtime();
    let store = config_testkit::MemStore::new();
    let key = ControlKey::Node(NodeId(1)).encode();
    let removed = rdb_core::authority::node::NodeRecord {
        node: NodeId(1),
        addr: "127.0.0.1:7100".to_owned(),
        machine: "rack-1".to_owned(),
    }
    .encode_removed(Some(APP));
    let put = rt
        .block_on(store.put(config_core::PutRequest {
            key: Bytes::from(key.clone()),
            value: removed.clone(),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("the removed record is written");
    assert_eq!(put.outcome, config_core::MutationOutcome::Applied);
    let read = || {
        rt.block_on(store.get(config_core::GetRequest {
            key: Bytes::from(key.clone()),
        }))
        .expect("a read")
        .record
        .expect("nodes/1")
    };
    let before = read();

    let dir = config_testkit::fs::temp_dir();
    let refused = rt
        .block_on(register_app_addr(
            &store,
            dir.path(),
            NodeId(1),
            "https://gw-1.b.example:9443",
        ))
        .expect_err("an open of a removed node is refused");
    assert!(
        refused
            .to_string()
            .contains("node 1 was removed; ids are never reused"),
        "{refused}"
    );
    let after = read();
    assert_eq!(after.value, removed, "nothing was written");
    assert_eq!(
        after.mod_revision, before.mod_revision,
        "nothing was written"
    );
    assert!(
        !dir.path().join(APP_ADDR_PENDING_FILE).exists(),
        "no app-address marker is left"
    );

    assert_eq!(
        rt.block_on(read_app_addr(&store, NodeId(1)))
            .expect("route reads the removed record"),
        Some(APP.to_owned())
    );
}

/// #80 S4 T8, the removed record's readers: five nodes run; node 5 stops and its record is
/// written removed (layout 3, a CAS at the revision read, as `remove_node` will write it). One
/// removed record beside live ones fails no list:
/// - every running node's `nodes/` watcher lists again and drops node 5 from its members;
/// - `admin::read_nodes` lists the four live records, and `read_removed_nodes` names node 5;
/// - node 1's declared open check, on peers 1 to 4, passes; node 5's, with no app address, is
///   refused "node 5 was removed; ids are never reused" (M6 removed half);
/// - node 1's `route` answers for a group of every partition, node 5's included;
/// - `admin::add_node` takes id 6, not the removed 5.
///
/// Integration (five node processes). Cut: `read_nodes_at` refuses a removed record as
/// undecodable (its old reader), so every watcher's list fails and node 1 keeps node 5 ("not
/// within 10s: node 1 drops removed node 5"), red, observed 2026-11-20. Cut 2:
/// `admin::own_removed_refusal` always answers `None`: node 5's open is refused "nodes/5 is
/// absent, so node 5 is not a declared node", red, observed 2026-11-21 at load 17-22.
#[config_log::retcd_test]
fn s80s4_a_removed_record_beside_live_ones_leaves_members_open_and_route_working() {
    const REMOVED: NodeId = NodeId(5);
    let (mut rig, _six) = five_ready();
    rig.procs
        .iter_mut()
        .find(|proc| proc.node == REMOVED)
        .expect("node 5 runs")
        .db
        .shutdown();
    let key = ControlKey::Node(REMOVED).encode();
    let found = rig
        .rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(key.clone()),
        }))
        .expect("a read")
        .record
        .expect("nodes/5");
    let (record, app) = rdb_core::authority::node::NodeRecord::decode_with_app(&found.value)
        .expect("node 5's live record");
    let written = rig
        .rt
        .block_on(rig.store.put(config_core::PutRequest {
            key: Bytes::from(key),
            value: record.encode_removed(app.as_deref()),
            expected_mod_revision: Some(found.mod_revision),
            dedup: None,
        }))
        .expect("the removal is written");
    assert_eq!(written.outcome, config_core::MutationOutcome::Applied);

    let four: BTreeSet<NodeId> = (1..=4).map(NodeId).collect();
    for n in 1..=4 {
        let net = Arc::clone(&rig.of(NodeId(n)).net);
        until(
            &format!("node {n} drops removed node 5"),
            Duration::from_secs(10),
            || net.members() == four,
        );
    }

    let removed = rig
        .rt
        .block_on(admin::read_removed_nodes(&*rig.store))
        .expect("the removed nodes list");
    assert_eq!(removed, BTreeSet::from([REMOVED]));
    let live = rig
        .rt
        .block_on(admin::read_nodes(&*rig.store))
        .expect("the live records list");
    assert_eq!(live.keys().copied().collect::<BTreeSet<_>>(), four);

    let formation = rig
        .rt
        .block_on(admin::read_formation(&*rig.store))
        .expect("a formation read")
        .expect("the database is declared");
    let peers: BTreeMap<NodeId, String> = rig
        .addrs
        .iter()
        .filter(|(node, _)| four.contains(node))
        .map(|(node, addr)| (*node, addr.clone()))
        .collect();
    rig.rt
        .block_on(declared_open_refusal(
            &[NodeId(1)],
            Some(&NodeAddresses {
                listen: rig.addrs[&NodeId(1)].clone(),
                peers,
                app: None,
            }),
            &formation,
            &*rig.store,
        ))
        .expect("node 1's open check passes beside a removed record");
    let refused = rig
        .rt
        .block_on(declared_open_refusal(
            &[REMOVED],
            Some(&NodeAddresses {
                listen: rig.addrs[&REMOVED].clone(),
                peers: rig.addrs.clone(),
                app: None,
            }),
            &formation,
            &*rig.store,
        ))
        .expect_err("node 5's open is refused");
    assert!(
        refused
            .to_string()
            .contains("node 5 was removed; ids are never reused"),
        "{refused}"
    );

    let one = &rig.of(NodeId(1)).db;
    for p in 1..=PARTITIONS {
        let group = group_in(one, PartitionId(p));
        one.route(group)
            .unwrap_or_else(|e| panic!("route answers for partition {p}: {e:?}"));
    }

    let added = rig
        .rt
        .block_on(admin::add_node(&rig.store, &rig.addrs[&ADDED], None))
        .expect("a node is added");
    assert_eq!(added.node, ADDED, "the removed id 5 is not reused");
}

/// #80 S4 T12, `members_revision` on the wire: five nodes run. Node 1 looks at node 2 over its
/// link, and node 2's status names the `nodes/` revision its watcher last applied, the one its
/// own `Net` holds and below `nodes/6`'s. `admin::add_node` writes `nodes/6`; node 1's look at
/// node 2 then reports a `members_revision` at or above `nodes/6`'s mod revision. This is what
/// T11b's remove gate reads from each live member.
///
/// Integration (five node processes). Cut: `WireNodeStatus::from` sends `members_revision:
/// None`, so node 1's look carries none ("node 2's status over the wire names its members
/// revision"), red, observed 2026-11-21.
#[config_log::retcd_test]
fn s80s4_a_nodes_status_reports_its_members_revision_and_it_moves_after_an_add() {
    const LOOKED: NodeId = NodeId(2);
    let (rig, _six) = five_ready();
    let one = &rig.of(NodeId(1)).db;
    let local = rig.of(LOOKED).net.members_revision();
    let mut before = None;
    until(
        "node 2's status over the wire names its members revision",
        Duration::from_secs(10),
        || {
            before = one
                .inspect(LOOKED)
                .and_then(|status| status.members_revision);
            before.is_some()
        },
    );
    let before = before.expect("a members revision");
    assert!(
        before >= local && local > 0,
        "node 2's look carries at least the revision its Net held before it: {before} vs {local}"
    );

    let added = rig
        .rt
        .block_on(admin::add_node(&rig.store, &rig.addrs[&ADDED], None))
        .expect("node 6 is added");
    let six = rig
        .rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Node(added.node).encode()),
        }))
        .expect("a read")
        .record
        .expect("nodes/6")
        .mod_revision;
    assert!(
        before < six,
        "nodes/6 is written after the first look: {before} vs {six}"
    );
    let mut after = None;
    until(
        "node 2's status reports a members revision at or above nodes/6's",
        Duration::from_secs(10),
        || {
            after = one
                .inspect(LOOKED)
                .and_then(|status| status.members_revision);
            after.is_some_and(|revision| revision >= six)
        },
    );
    tracing::info!(before, six, after = ?after, "s80s4_members_revision_moved");
}

/// The `nodes/{node}` record's mod revision.
fn node_revision(rig: &Rig, node: NodeId) -> u64 {
    rig.rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Node(node).encode()),
        }))
        .expect("a read")
        .record
        .expect("the node's record")
        .mod_revision
}

/// M5c (#80 S4, S-3: one membership change at a time). Five nodes run and node 3's `nodes/`
/// lists are cut, so its watcher is held. `admin::add_node` writes `nodes/6` (node 6 does not
/// start, so it is named in `unreachable_ok`). `membership_settled` then refuses, naming live
/// member 3 and its older revision; once node 3's cut is lifted it settles, every other member
/// having applied `nodes/6`. Integration (five node processes).
///
/// Cut: `membership_settled` takes any reported revision as current, so it settles while node
/// 3 is held ("node 3's held watcher blocks"), red, observed 2026-11-21.
#[config_log::retcd_test]
fn s80s4_membership_changes_refuse_until_every_live_node_has_applied_the_last() {
    let within = Duration::from_millis(500);
    let (rig, _six) = five_ready();
    rig.held.recut();
    let added = rig
        .rt
        .block_on(admin::add_node(&rig.store, &rig.addrs[&ADDED], None))
        .expect("node 6 is added");
    let six = node_revision(&rig, added.node);
    let one = &rig.of(NodeId(1)).db;
    for n in [1, 2, 4, 5] {
        let net = Arc::clone(&rig.of(NodeId(n)).net);
        until(
            &format!("node {n} applies nodes/6"),
            Duration::from_secs(10),
            || net.members_revision() >= six,
        );
    }
    let why = admin::membership_settled(one, rig.rt.handle(), &*rig.store, &[ADDED], within)
        .expect_err("node 3's held watcher blocks");
    assert!(
        why.contains("live member 3 has applied nodes/ revision")
            && why.contains("below the newest"),
        "{why}"
    );
    rig.held.lift();
    let mut last = Ok(());
    until(
        "membership settles once node 3 applies nodes/6",
        Duration::from_secs(10),
        || {
            last = admin::membership_settled(one, rig.rt.handle(), &*rig.store, &[ADDED], within);
            last.is_ok()
        },
    );
}

/// #80 S4 T11b, the mixed-version guard: five nodes run, node 6 is added (pinned by nothing,
/// named in `unreachable_ok` since it never starts), every running node applies `nodes/6`, and
/// node 4 stops. `admin::remove_member`
/// of node 6, its fresh status requests answered within 500 ms, is refused naming live member
/// 4 ("has sent no status since its boot", "500ms") and writes nothing: a member that has not
/// answered at this build's protocol version may run a build that cannot read a removed
/// record. With node 4 also named in `unreachable_ok`, the same call writes `nodes/6` removed.
/// Integration (five node processes).
///
/// Cut: `membership_settled` takes an unanswered request as current, so the first remove
/// writes ("node 4's silence refuses the remove"), red, observed 2026-11-21.
#[config_log::retcd_test]
fn s80s4_a_remove_is_refused_while_a_live_member_has_sent_no_status_since_its_boot() {
    const STOPPED: NodeId = NodeId(4);
    let within = Duration::from_millis(500);
    let (mut rig, _six) = five_ready();
    rig.rt
        .block_on(admin::add_node(&rig.store, &rig.addrs[&ADDED], None))
        .expect("node 6 is added");
    let six = node_revision(&rig, ADDED);
    for n in 1..=5 {
        let net = Arc::clone(&rig.of(NodeId(n)).net);
        until(
            &format!("node {n} applies nodes/6"),
            Duration::from_secs(10),
            || net.members_revision() >= six,
        );
    }
    rig.procs
        .iter_mut()
        .find(|proc| proc.node == STOPPED)
        .expect("node 4 runs")
        .db
        .shutdown();
    let before = node_revision(&rig, ADDED);
    let one = &rig.of(NodeId(1)).db;
    let answer = admin::remove_member(one, rig.rt.handle(), &rig.store, ADDED, &[ADDED], within);
    let why = match answer {
        Err(admin::RemoveNodeError::Refused(why)) => why,
        other => panic!("node 4's silence refuses the remove, not {other:?}"),
    };
    assert!(
        why.contains("live member 4 has sent no status since its boot") && why.contains("500ms"),
        "{why}"
    );
    assert_eq!(node_revision(&rig, ADDED), before, "nothing is written");

    let removed = admin::remove_member(
        one,
        rig.rt.handle(),
        &rig.store,
        ADDED,
        &[ADDED, STOPPED],
        within,
    )
    .expect("node 6 is removed with node 4 named unreachable");
    assert_eq!(removed.node, ADDED);
    let found = rig
        .rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Node(ADDED).encode()),
        }))
        .expect("a read")
        .record
        .expect("nodes/6");
    assert!(
        rdb_core::authority::node::NodeRecord::decode_removed(&found.value).is_some(),
        "nodes/6 is layout 3"
    );
}

/// M8 links half and M9 shrink (#80 S4, B-2): five nodes run; node 6 is added, opens and links
/// to all five, and K is 6. `admin::remove_node(6)` writes `nodes/6` removed (node 6 holds no
/// copy). Then every node of 1 to 5 drops node 6 from its members, holds no link up to it, and
/// its clock guard's K is 5 again; and node 6's link to node 1 ends and its dials back are
/// refused "not a live member". Integration (six node processes).
///
/// Cut: `Net::set_members` leaves a retired node's links open: "not within 5s: node 1 holds no
/// link to removed node 6" (its connection, `Net::linked_peers`, not its route), red, observed
/// 2026-11-21 at load 40-44.
#[config_log::retcd_test]
fn s80s4_a_removed_node_loses_its_links_and_k_shrinks() {
    let (mut rig, six) = five_ready();
    rig.rt
        .block_on(admin::add_node(&rig.store, &rig.addrs[&ADDED], None))
        .expect("node 6 is added");
    for n in 1..=5 {
        let net = Arc::clone(&rig.of(NodeId(n)).net);
        until(
            &format!("node {n} admits node 6"),
            Duration::from_secs(10),
            || net.members().contains(&ADDED),
        );
    }
    certs::issue_node(&rig.cert_dir, ADDED).expect("node 6's certificate");
    let all = rig.addrs.clone();
    let store = Arc::clone(&rig.store);
    let proc = open_node(&rig, store, ADDED, six, &all);
    rig.procs.push(proc);
    let six_net = Arc::clone(&rig.of(ADDED).net);
    rig.rt
        .block_on(six_net.wait_up(
            &(1..=5).map(NodeId).collect::<Vec<_>>(),
            crate::host::test_patience(Duration::from_secs(10)),
        ))
        .expect("node 6 links to nodes 1 to 5");
    let guard = |n: u32| {
        rig.of(NodeId(n))
            .db
            .clock
            .guard()
            .expect("a node's clock has a guard")
            .cluster_k()
    };
    until("node 1's K counts node 6", Duration::from_secs(10), || {
        guard(1) == 6
    });

    rig.rt
        .block_on(admin::remove_node(&rig.store, ADDED))
        .expect("node 6 is removed");
    let five: BTreeSet<NodeId> = (1..=5).map(NodeId).collect();
    for n in 1..=5 {
        let net = Arc::clone(&rig.of(NodeId(n)).net);
        until(
            &format!("node {n} drops removed node 6"),
            Duration::from_secs(10),
            || net.members() == five,
        );
        until(
            &format!("node {n} holds no link to removed node 6"),
            Duration::from_secs(5),
            || {
                !matches!(net.states().get(&ADDED), Some(LinkState::Up(_)))
                    && !net.linked_peers().contains(&ADDED)
            },
        );
        until(
            &format!("node {n}'s K is 5 again"),
            Duration::from_secs(5),
            || guard(n) == 5,
        );
    }
    until(
        "node 6's dial back to node 1 is refused as not a live member",
        Duration::from_secs(10),
        || {
            matches!(
                six_net.states().get(&NodeId(1)),
                Some(LinkState::Refused(reason)) if reason.contains("not a live member")
            )
        },
    );
}

/// M8 embedded-`Db` half (#80 S4, S-5): five nodes run; node 6 is added, opens and links to all
/// five, and its own host answers. `admin::remove_node(6)` writes `nodes/6` removed. Then node
/// 6's `Db` reports `removed()`, its host no longer answers, and every kind of call through it
/// is refused `INVALID_ARGUMENT`, `no_mutation`, naming the removal: a put, a get, a route, a
/// restart of node 6 and a promote. Integration (six node processes).
///
/// Cuts: (a) `admin::watch_members` never marks its own node removed: "not within 10s: node
/// 6's Db reports removed"; (b) `Db::removed` stops no node: "node 6's host has stopped". Both
/// red, observed 2026-11-21 at load 28-34.
#[config_log::retcd_test]
fn s80s4_a_removed_nodes_db_stops_its_host_and_refuses_every_call() {
    let (mut rig, six) = five_ready();
    rig.rt
        .block_on(admin::add_node(&rig.store, &rig.addrs[&ADDED], None))
        .expect("node 6 is added");
    for n in 1..=5 {
        let net = Arc::clone(&rig.of(NodeId(n)).net);
        until(
            &format!("node {n} admits node 6"),
            Duration::from_secs(10),
            || net.members().contains(&ADDED),
        );
    }
    certs::issue_node(&rig.cert_dir, ADDED).expect("node 6's certificate");
    let all = rig.addrs.clone();
    let store = Arc::clone(&rig.store);
    let proc = open_node(&rig, store, ADDED, six, &all);
    rig.procs.push(proc);
    rig.rt
        .block_on(rig.of(ADDED).net.wait_up(
            &(1..=5).map(NodeId).collect::<Vec<_>>(),
            crate::host::test_patience(Duration::from_secs(10)),
        ))
        .expect("node 6 links to nodes 1 to 5");
    let db = &rig.of(ADDED).db;
    assert!(!db.removed(), "node 6 is live");
    assert!(db.inspect(ADDED).is_some(), "node 6's own host answers");

    rig.rt
        .block_on(admin::remove_node(&rig.store, ADDED))
        .expect("node 6 is removed");
    let db = &rig.of(ADDED).db;
    until(
        "node 6's Db reports removed",
        Duration::from_secs(10),
        || db.removed(),
    );
    assert!(db.inspect(ADDED).is_none(), "node 6's host has stopped");
    let refused = |what: &str, error: &ApiError| {
        assert_eq!(error.kind, ErrorKind::InvalidArgument, "{what}: {error:?}");
        assert!(error.no_mutation, "{what}: {error:?}");
        assert!(
            error
                .detail
                .contains("node 6 was removed; ids are never reused"),
            "{what}: {error:?}"
        );
    };
    let group = AffinityId(1);
    refused(
        "a put",
        &db.put_in(group, b"k", b"v", None)
            .expect_err("a removed Db writes nothing")
            .error,
    );
    refused(
        "a get",
        &db.get_in(group, b"k")
            .expect_err("a removed Db reads nothing"),
    );
    refused(
        "a route",
        &db.route(group).expect_err("a removed Db routes nothing"),
    );
    refused(
        "a restart",
        &db.restart_node(ADDED)
            .expect_err("a removed Db restarts nothing"),
    );
    let promote = db
        .promote_partition(PartitionId(1), NodeId(1), None)
        .expect_err("a removed Db promotes nothing");
    assert!(promote.no_mutation(), "a promote: {promote:?}");
    assert!(
        promote
            .to_string()
            .contains("node 6 was removed; ids are never reused"),
        "a promote: {promote:?}"
    );
}
