//! #80 S1a (ADR-rdb-0022): a database formed by declaration, three node "processes" in this test
//! process, as `failover_tests` runs them (a `Db`, a `Net` and a `Links` each, real TLS links),
//! but **each with its own folder** and no forming open: `admin::declare` writes the control
//! records, and each owner forms its own partitions from an empty copy.

use super::reopen_tests::{config, fast, runtime};
use super::*;
use crate::net::faults::Faults;
use crate::net::{certs, wire::Label, Net, NetConfig, PROTOCOL_VERSION};
use crate::test_store::{HoldAt, PutHold, Script, Scripted, TlsReported};

/// The partition count the rows declare: more than one per node, so each owner forms several.
const PARTITIONS: u32 = 8;

/// The `nodes/{id}` puts a declaration of three nodes makes after `routes/0` and before its
/// partition records (#80 S1b). Rows that stop a declaration after a count of puts add them.
const NODE_PUTS: u32 = 3;

/// One node process, its own folder kept alive with it.
struct Proc {
    node: NodeId,
    /// `None` once the process ended ([`Rig::stop`], [`Rig::shutdown_all`]): its `Links` held
    /// its net, so only dropping it lets go of its listener, as a process exit does.
    db: Option<Db>,
    /// `None` once [`Rig::stop`] took the node down: its listener and links are gone.
    _net: Option<Arc<Net>>,
    dir: PathBuf,
    /// The `TempDir` that removes `dir` when dropped.
    _dir: Box<dyn std::any::Any>,
    /// This process's own runtime when the rig gives each one its own (#80 S1b): dropped,
    /// it ends every task the process ran, its peers' accepted links included, as a process
    /// exit does. Last, so it outlives the `Db` and net that run on it.
    own_rt: Option<tokio::runtime::Runtime>,
}

struct Rig {
    rt: tokio::runtime::Runtime,
    store: Arc<dyn ConfigStore>,
    cert_dir: PathBuf,
    _cert_dir: Box<dyn std::any::Any>,
    procs: Vec<Proc>,
    /// How many nodes the rig runs: nodes 1 to `k` (#80 S1b).
    k: u32,
    /// The timeouts [`Rig::open_all`] opens every node with.
    timeouts: Timeouts,
    /// Whether [`Rig::open_all`] runs each process on a runtime of its own, so that
    /// [`Rig::stop`] and [`Rig::restart`] end it as a process exit would (#80 S1b).
    own_runtimes: bool,
    /// Where each node listens: node N on 127.0.0.N, so every node of the rig is a different
    /// host on one machine (#80 S1b ruling 1). Bound when the rig is made, before anything is
    /// declared, so [`Rig::declaration`] records the ports the nodes listen on.
    addrs: BTreeMap<NodeId, String>,
    /// The listeners bound when the rig was made, each until its node's first open takes it.
    reserved: std::sync::Mutex<BTreeMap<NodeId, tokio::net::TcpListener>>,
    /// The app address each node registers when [`Rig::open_all`] or [`Rig::restart`] opens it
    /// (#83 B2); a node not named registers none.
    apps: BTreeMap<NodeId, String>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for proc in &mut self.procs {
            if let Some(db) = &mut proc.db {
                db.shutdown();
            }
        }
    }
}

fn declaration() -> admin::Declaration {
    declaration_of(3, PARTITIONS)
}

/// A declaration of nodes 1 to `k` and `partitions` partitions (#80 S1b), node N at
/// `127.0.0.N:7100` on machine `127.0.0.N`: for rows that only declare. A row that opens a node
/// declares [`Rig::declaration`], the addresses its listeners are bound to, since a declared
/// open checks them against the records.
fn declaration_of(k: u32, partitions: u32) -> admin::Declaration {
    declaration_at(
        &(1..=k)
            .map(|n| (NodeId(n), format!("127.0.0.{n}:7100")))
            .collect(),
        partitions,
    )
}

/// A declaration of the nodes of `addrs`, each at its address, and `partitions` partitions.
fn declaration_at(addrs: &BTreeMap<NodeId, String>, partitions: u32) -> admin::Declaration {
    admin::Declaration {
        nodes: addrs
            .iter()
            .map(|(node, addr)| admin::NodeDecl {
                node: *node,
                addr: addr.clone(),
                machine: None,
            })
            .collect(),
        partitions,
    }
}

/// Bind `addr` again once whatever held it let go: a restarted process listens where its
/// record says. Bounded at 5 s.
fn rebind(rt: &tokio::runtime::Runtime, addr: &str) -> tokio::net::TcpListener {
    let addr: std::net::SocketAddr = addr.parse().expect("an address");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match rt.block_on(tokio::net::TcpListener::bind(addr)) {
            Ok(listener) => return listener,
            Err(e) => {
                assert!(Instant::now() < deadline, "rebind {addr}: {e}");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Open `node`'s process on `dir` alone, linked to every other node of `addrs`.
fn open_node(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    cert_dir: &Path,
    node: NodeId,
    dir: &Path,
    listener: tokio::net::TcpListener,
    addrs: &BTreeMap<NodeId, String>,
) -> Result<(Db, Arc<Net>), OpenError> {
    open_node_with(
        rt,
        store,
        cert_dir,
        node,
        dir,
        listener,
        addrs,
        Timeouts::default(),
        None,
    )
}

/// [`open_node`] with `timeouts` (#80 S1b: a shorter unreachable drop) and the app address the
/// node registers (#83 B2).
#[allow(clippy::too_many_arguments)]
fn open_node_with(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    cert_dir: &Path,
    node: NodeId,
    dir: &Path,
    listener: tokio::net::TcpListener,
    addrs: &BTreeMap<NodeId, String>,
    timeouts: Timeouts,
    app: Option<&str>,
) -> Result<(Db, Arc<Net>), OpenError> {
    let _entered = rt.enter();
    let (config, net) = node_config(cert_dir, node, dir, listener, addrs, timeouts, app);
    let db = rt.block_on(Db::open_with(
        config,
        Arc::clone(store),
        rt.handle().clone(),
        fast(),
    ))?;
    Ok((db, net))
}

/// What [`open_node_with`] opens `node` with, and its started net, inside a runtime context:
/// for a row that drives the open's future itself (#83 B2: an open dropped mid-write).
fn node_config(
    cert_dir: &Path,
    node: NodeId,
    dir: &Path,
    listener: tokio::net::TcpListener,
    addrs: &BTreeMap<NodeId, String>,
    timeouts: Timeouts,
    app: Option<&str>,
) -> (DbConfig, Arc<Net>) {
    let boot = next_boot(dir).expect("a boot");
    let label = Label::Node(node);
    let links = Links::new();
    let net = Net::start(
        NetConfig {
            tls: certs::load(cert_dir, &label).expect("node certs"),
            label,
            boot,
            peers: addrs.keys().copied().filter(|n| *n != node).collect(),
            dial: NetConfig::dial_lower(node, addrs),
            protocol_version: PROTOCOL_VERSION,
            admin_apps: BTreeSet::new(),
            faults: Faults::new(),
        },
        &links,
        Some(listener),
    )
    .expect("the node's net starts");
    let config = DbConfig {
        local: vec![node],
        links: Some(links),
        boot: Some(boot),
        timeouts,
        addresses: Some(NodeAddresses {
            listen: addrs[&node].clone(),
            peers: addrs.clone(),
            app: app.map(str::to_owned),
        }),
        ..config(dir)
    };
    (config, net)
}

/// Listeners for nodes 1 to `k`, node N on 127.0.0.N at a port from the test range, and the
/// address each is reached at (#80 S1b: one machine, `k` hosts).
fn listeners(
    rt: &tokio::runtime::Runtime,
    k: u32,
) -> (Vec<tokio::net::TcpListener>, BTreeMap<NodeId, String>) {
    let listeners: Vec<tokio::net::TcpListener> = (1..=k)
        .map(|n| {
            let host = std::net::Ipv4Addr::new(127, 0, 0, u8::try_from(n).expect("a node id"));
            rt.block_on(config_testkit::ports::ephemeral_listener_on(host))
                .0
        })
        .collect();
    let addrs = (1..=k)
        .map(NodeId)
        .zip(&listeners)
        .map(|(node, listener)| (node, listener.local_addr().expect("bound").to_string()))
        .collect();
    (listeners, addrs)
}

/// A runtime, `store` and certificates for nodes 1 to 3; no node process yet.
fn bare_rig(store: Arc<dyn ConfigStore>) -> Rig {
    bare_rig_of(store, 3)
}

/// A runtime, `store` and certificates for nodes 1 to `k`; no node process yet.
fn bare_rig_of(store: Arc<dyn ConfigStore>, k: u32) -> Rig {
    let rt = runtime();
    let cert_dir = config_testkit::fs::temp_dir();
    let labels: Vec<Label> = (1..=k).map(|node| Label::Node(NodeId(node))).collect();
    certs::generate(cert_dir.path(), &labels).expect("certs");
    let (held, addrs) = listeners(&rt, k);
    let reserved = addrs.keys().copied().zip(held).collect();
    Rig {
        rt,
        store,
        cert_dir: cert_dir.path().to_path_buf(),
        _cert_dir: Box::new(cert_dir),
        procs: Vec::new(),
        k,
        timeouts: Timeouts::default(),
        own_runtimes: false,
        addrs,
        reserved: std::sync::Mutex::new(reserved),
        apps: BTreeMap::new(),
    }
}

/// Declare, then open nodes 1 to 3 each on a new empty folder of its own; none waited on yet.
fn declared_rig() -> Rig {
    let mut rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    rig.declare_created();
    rig.open_all(fresh_dirs());
    rig
}

/// A folder's path, and the `TempDir` that removes it when dropped.
type Folder = (PathBuf, Box<dyn std::any::Any>);

/// A new empty folder.
fn folder() -> Folder {
    let dir = config_testkit::fs::temp_dir();
    (dir.path().to_path_buf(), Box::new(dir))
}

/// Three new empty folders, for nodes 1 to 3.
fn fresh_dirs() -> Vec<Folder> {
    fresh_dirs_of(3)
}

/// `k` new empty folders, for nodes 1 to `k`.
fn fresh_dirs_of(k: u32) -> Vec<Folder> {
    (1..=k).map(|_| folder()).collect()
}

/// Every record in `store`: key, value, create and mod revision, in key order. Equal snapshots
/// mean nothing was written between them, an identical-byte rewrite included.
fn snapshot(
    rt: &tokio::runtime::Runtime,
    store: &dyn ConfigStore,
) -> Vec<(Bytes, Bytes, u64, u64)> {
    let listed = rt
        .block_on(store.list(config_core::ListRequest {
            prefix: Bytes::from_static(b""),
            max_items: 0,
            max_bytes: 0,
        }))
        .expect("the store answers");
    assert!(!listed.truncated, "a whole list");
    listed
        .records
        .into_iter()
        .map(|record| {
            (
                record.key,
                record.value,
                record.create_revision,
                record.mod_revision,
            )
        })
        .collect()
}

/// The keys of `snap`, as text.
fn keys(snap: &[(Bytes, Bytes, u64, u64)]) -> Vec<String> {
    snap.iter()
        .map(|(key, ..)| String::from_utf8_lossy(key).into_owned())
        .collect()
}

/// The keys a whole declaration of `partitions` (more than one) over nodes 1 to 3 writes, its
/// reservation first, then `routes/0`, the node records (#80 S1b), the partition records and
/// its open counter (`counters`: the counter keys expected), and the formation record last.
fn declared_keys(partitions: u32, counters: &[&str], marker: bool) -> Vec<String> {
    let mut keys: Vec<String> = (1..=partitions)
        .map(|id| ControlKey::Partition(PartitionId(id)).encode())
        .collect();
    keys.extend(node_keys(3));
    keys.push(ControlKey::Route(RangeId(0)).encode());
    keys.push(ControlKey::Operation(admin::RESERVATION_OPERATION).encode());
    keys.extend(counters.iter().map(|key| (*key).to_owned()));
    if marker {
        keys.push(ControlKey::Operation(admin::FORMATION_OPERATION).encode());
    }
    keys.sort();
    keys
}

/// The `nodes/` keys a declaration of nodes 1 to `k` writes (#80 S1b).
fn node_keys(k: u32) -> Vec<String> {
    (1..=k)
        .map(|n| ControlKey::Node(NodeId(n)).encode())
        .collect()
}

/// Whether every record of `before` is in `after`, byte for byte, at the same revisions.
fn kept(before: &[(Bytes, Bytes, u64, u64)], after: &[(Bytes, Bytes, u64, u64)]) -> bool {
    before.iter().all(|record| after.contains(record))
}

/// Whether `dir` holds a `nodes/` folder: an open that created one and was refused removes it.
fn has_copies(dir: &Path) -> bool {
    dir.join("nodes").exists()
}

impl Rig {
    /// The rig's declaration: nodes 1 to `k` at the addresses its listeners are bound to, and
    /// [`PARTITIONS`] partitions.
    fn declaration(&self) -> admin::Declaration {
        self.declaration_of(PARTITIONS)
    }

    /// [`Rig::declaration`] with `partitions` partitions.
    fn declaration_of(&self, partitions: u32) -> admin::Declaration {
        declaration_at(&self.addrs, partitions)
    }

    /// The listener `node` opens on: the one bound when the rig was made, or its address
    /// bound again once that one is gone.
    fn listen(&self, rt: &tokio::runtime::Runtime, node: NodeId) -> tokio::net::TcpListener {
        let held = self
            .reserved
            .lock()
            .expect("the reserved listeners")
            .remove(&node);
        held.unwrap_or_else(|| rebind(rt, &self.addrs[&node]))
    }

    /// [`Rig::declaration`] on this rig's store, which must write it.
    fn declare_created(&self) -> admin::Formation {
        match self
            .rt
            .block_on(admin::declare(&self.store, &self.declaration()))
        {
            Ok(admin::Declared::Created(formation)) => formation,
            other => panic!("the declaration is written: {other:?}"),
        }
    }

    /// Open nodes 1 to `k`, each on its folder in `dirs`, in that order; none waited on yet.
    fn open_all(&mut self, dirs: Vec<Folder>) {
        let addrs = self.addrs.clone();
        let nodes: Vec<NodeId> = addrs.keys().copied().collect();
        for (node, (dir, keep)) in nodes.iter().zip(dirs) {
            let listener = self.listen(&self.rt, *node);
            let own_rt = self.own_runtimes.then(runtime);
            let (db, net) = open_node_with(
                own_rt.as_ref().unwrap_or(&self.rt),
                &self.store,
                &self.cert_dir,
                *node,
                &dir,
                listener,
                &addrs,
                self.timeouts,
                self.apps.get(node).map(String::as_str),
            )
            .unwrap_or_else(|e| panic!("node {}'s process opens its own folder: {e}", node.0));
            self.procs.push(Proc {
                node: *node,
                db: Some(db),
                _net: Some(net),
                dir,
                _dir: keep,
                own_rt,
            });
        }
    }

    /// Open `node`'s process once more on its folder, alone, the others down.
    fn reopen(&self, node: NodeId) -> Result<(Db, Arc<Net>), OpenError> {
        open_node(
            &self.rt,
            &self.store,
            &self.cert_dir,
            node,
            &self.procs[node.0 as usize - 1].dir,
            self.listen(&self.rt, node),
            &self.addrs,
        )
    }

    /// End every process as an exit does: shut its `Db` down and drop it, its net and its
    /// runtime, so its listener is free for a reopen at its recorded address. Its folder stays.
    fn shutdown_all(&mut self) {
        for proc in &mut self.procs {
            if let Some(mut db) = proc.db.take() {
                db.shutdown();
            }
            proc._net = None;
            if let Some(rt) = proc.own_rt.take() {
                rt.shutdown_background();
            }
        }
    }

    /// Lose `node`'s whole folder, as a wiped disk does, leaving an empty one.
    fn wipe(&self, node: NodeId) {
        let dir = &self.procs[node.0 as usize - 1].dir;
        std::fs::remove_dir_all(dir).expect("remove the folder");
        std::fs::create_dir_all(dir).expect("an empty folder");
    }

    fn wait_ready(&self) {
        for proc in &self.procs {
            proc.db
                .as_ref()
                .expect("a running process")
                .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
                .unwrap_or_else(|e| panic!("node {} is ready: {e}", proc.node.0));
        }
    }

    /// The process that runs `node`.
    fn of(&self, node: NodeId) -> &Db {
        self.procs[node.0 as usize - 1]
            .db
            .as_ref()
            .expect("a running process")
    }

    /// Shut every process down and open each again on its own folder, at a new boot.
    fn restart_all(&mut self) {
        // Each process listens again where its record says, once the old one let go.
        self.shutdown_all();
        let addrs = self.addrs.clone();
        for proc in &mut self.procs {
            let listener = rebind(&self.rt, &addrs[&proc.node]);
            let (db, net) = open_node(
                &self.rt,
                &self.store,
                &self.cert_dir,
                proc.node,
                &proc.dir,
                listener,
                &addrs,
            )
            .unwrap_or_else(|e| panic!("node {} reopens its own folder: {e}", proc.node.0));
            proc.db = Some(db);
            proc._net = Some(net);
        }
    }
}

/// An affinity group that maps to `partition`.
fn group_of(db: &Db, partition: u32) -> AffinityId {
    (1..1_000_000)
        .map(AffinityId)
        .find(|group| db.partition_for(*group) == PartitionId(partition))
        .expect("some group maps to every partition")
}

/// Put in `group` through `db` until one lands; a partition still recovering refuses first.
fn put_lands(db: &Db, group: AffinityId, key: &[u8], value: &[u8]) -> PutOk {
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(20));
    loop {
        match db.put_in(group, key, value, None) {
            Ok(put) => return put,
            Err(refused) => assert!(
                Instant::now() < deadline,
                "no put landed in {group:?}: {refused:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The node folders under `dir/nodes`.
fn copies_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir.join("nodes"))
        .expect("nodes/ exists")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/// S1a acceptance: declared, then three node processes each on its own empty folder become
/// ready; every partition's owner is the one `initial_owner` places, `Db::route` names it, and a
/// put through it reads back; and no folder holds another node's copy. Integration (~3 s): one
/// formation of 8 partitions over real links.
/// Cut 1: `open_spawning` reads no formation record (`declared` always `None`): node 1's open is
/// refused "database not formed yet: partitions/1 names no instance", red at the first open.
/// Cut 2: the heal's bootstrap plans pin every member at this process's boot (`members_for(p,
/// ctx.boot)`): the copies in other processes answer at other boots and node 1's recovery stops
/// `BarrierIncomplete`, red at `wait_ready`.
#[config_log::retcd_test]
fn s1a_declared_database_forms_with_no_shared_folder() {
    let rig = declared_rig();
    rig.wait_ready();
    for partition in 1..=PARTITIONS {
        let owner = admin::initial_owner(PartitionId(partition));
        let db = rig.of(owner);
        let group = group_of(db, partition);
        let route = db.route(group).expect("a route");
        assert_eq!(
            (route.partition, route.owner),
            (PartitionId(partition), owner),
            "{route:?}"
        );
        let key = format!("k{partition}");
        let put = put_lands(db, group, key.as_bytes(), b"v");
        assert_eq!(put.partition, PartitionId(partition), "{put:?}");
        let got = db.get_in(group, key.as_bytes()).expect("a get");
        assert_eq!(
            got.value.as_ref().map(|(_, value)| &value[..]),
            Some(&b"v"[..]),
            "{got:?}"
        );
    }
    for proc in &rig.procs {
        assert_eq!(
            copies_in(&proc.dir),
            [proc.node.0.to_string()],
            "node {}'s folder holds only its own copy",
            proc.node.0
        );
    }
}

/// S1a: each node process shut down and opened again on its own folder reopens its copy (no
/// declared forming again) and serves what was written before. Integration (~5 s): a
/// formation, then a restart of all three.
#[config_log::retcd_test]
fn s1a_restart_of_each_node_reopens_its_own_folder() {
    let mut rig = declared_rig();
    rig.wait_ready();
    let groups: Vec<(u32, AffinityId)> = (1..=PARTITIONS)
        .map(|partition| (partition, group_of(rig.of(NodeId(1)), partition)))
        .collect();
    for (partition, group) in &groups {
        let owner = rig.of(admin::initial_owner(PartitionId(*partition)));
        put_lands(owner, *group, b"before", b"1");
    }
    rig.restart_all();
    rig.wait_ready();
    for (partition, group) in &groups {
        let owner = rig.of(admin::initial_owner(PartitionId(*partition)));
        let got = owner.get_in(*group, b"before").expect("a get");
        assert_eq!(
            got.value.as_ref().map(|(_, value)| &value[..]),
            Some(&b"1"[..]),
            "partition {partition}"
        );
        put_lands(owner, *group, b"after", b"2");
    }
    for proc in &rig.procs {
        assert_eq!(copies_in(&proc.dir), [proc.node.0.to_string()]);
    }
}

/// S1a: `declare` is write-once. The same declaration again answers `Already` with the same
/// instance; another partition count, or another node set, is refused, before and after the
/// declaration. None of the later calls writes anything: every record's bytes and create and
/// mod revisions are as they were (PR #138 review: an identical-byte rewrite would pass a
/// compare of the decoded record). Unit (<100 ms, an in-memory store).
/// #80 S1b changed what this row pins: four nodes are now a valid declaration, so before the
/// declaration a node set with a gap (1, 2, 4) is the refused one, and after it four nodes are
/// refused because they are not the declared three.
#[config_log::retcd_test]
fn s1a_declare_is_idempotent_and_refuses_a_changed_count() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let mut gap = declaration_of(4, PARTITIONS);
    gap.nodes.remove(2);
    let refused = rt.block_on(admin::declare(&store, &gap));
    assert!(
        matches!(&refused, Err(admin::DeclareError::Refused(text)) if text.contains("nodes [1, 2, 4]")),
        "{refused:?}"
    );
    assert!(snapshot(&rt, &*store).is_empty(), "nothing was written");
    let Ok(admin::Declared::Created(first)) = rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("the first declaration is written");
    };
    assert_eq!(first.partitions, PARTITIONS);
    let declared = snapshot(&rt, &*store);
    let counter = admin::instance_key(&first.instance);
    assert_eq!(
        keys(&declared),
        declared_keys(PARTITIONS, &[&counter], true)
    );

    let again = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(&again, Ok(admin::Declared::Already(found)) if *found == first),
        "{again:?}"
    );
    assert_eq!(
        snapshot(&rt, &*store),
        declared,
        "the same declaration wrote nothing"
    );

    let changed = rt.block_on(admin::declare(
        &store,
        &admin::Declaration {
            partitions: PARTITIONS + 1,
            ..declaration()
        },
    ));
    assert!(
        matches!(&changed, Err(admin::DeclareError::Refused(text))
            if text.contains(&format!("already declared with {PARTITIONS} partitions"))),
        "{changed:?}"
    );
    assert_eq!(
        snapshot(&rt, &*store),
        declared,
        "another count wrote nothing"
    );

    let four = declaration_of(4, PARTITIONS);
    let refused = rt.block_on(admin::declare(&store, &four));
    assert!(
        matches!(&refused, Err(admin::DeclareError::Refused(text))
            if text.contains("already declared with 8 partitions and nodes [1@127.0.0.1:7100/127.0.0.1, ")),
        "{refused:?}"
    );
    assert_eq!(snapshot(&rt, &*store), declared, "four nodes wrote nothing");
}

/// S1a keeps A-R231(a): with no declaration, a node process on an empty folder is refused with
/// today's text, and forms nothing. PR #138 review: that same folder then opens once the
/// database is declared, binds the declared instance, becomes ready and holds only its own
/// copy. Integration (~3 s): a refused open, then a declared formation of 8 partitions.
#[config_log::retcd_test]
fn s1a_undeclared_partial_open_is_refused_as_before() {
    let mut rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    let (kept_dir, keep) = folder();
    let refused = rig
        .rt
        .block_on(Db::open_with(
            DbConfig {
                local: vec![NodeId(2)],
                boot: Some(next_boot(&kept_dir).expect("a boot")),
                ..config(&kept_dir)
            },
            Arc::clone(&rig.store),
            rig.rt.handle().clone(),
            fast(),
        ))
        .map(|_| ())
        .expect_err("an undeclared database is not formed by a node process");
    assert!(
        refused
            .to_string()
            .contains("database not formed: the control store holds no partitions/1"),
        "{refused}"
    );
    assert!(
        snapshot(&rig.rt, &*rig.store).is_empty(),
        "nothing was written"
    );
    assert!(!has_copies(&kept_dir), "no copy was made");

    let formation = rig.declare_created();
    let mut dirs = fresh_dirs();
    dirs[1] = (kept_dir.clone(), keep);
    rig.open_all(dirs);
    rig.wait_ready();
    assert_eq!(
        rig.of(NodeId(2)).opened.instance,
        Some(formation.instance),
        "the kept folder binds the declared instance"
    );
    assert_eq!(copies_in(&kept_dir), ["2"]);
}

/// PR #138 review (blocker): a store formed with one partition the old way holds a rooted
/// `partitions/1` and no `routes/0`. A declaration of 8 is refused there, twice, before it
/// writes anything: no `routes/0` that would make later opens expect 8 partitions. The
/// database then reopens at its own count. Integration (~3 s): a whole-database formation and
/// a reopen.
/// Cut: drop `declare_preflight` from `admin::declare`; the declaration writes its reservation
/// and `routes/0`, then stops at the rooted record with `Records`, red at the first refusal.
#[config_log::retcd_test]
fn s1a_declare_on_a_formed_store_is_refused_and_writes_nothing() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let open = || {
        rt.block_on(Db::open_with(
            config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the whole database opens")
    };
    let mut db = open();
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("formed with one partition");
    db.shutdown();
    drop(db);
    let formed = snapshot(&rt, &*store);
    for attempt in 1..=2 {
        let refused = rt.block_on(admin::declare(&store, &declaration()));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains("partitions/1 is not the untouched generation-0 record")
                    && text.contains("nothing was written")),
            "attempt {attempt}: {refused:?}"
        );
        assert_eq!(
            snapshot(&rt, &*store),
            formed,
            "attempt {attempt} wrote nothing"
        );
    }
    let mut db = open();
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("the database reopens at its own count");
    assert_eq!(db.partition_for(AffinityId(1)), PartitionId(1));
    db.shutdown();
}

/// PR #138 review (blocker, and the missing interrupted-retry row): a declaration of 8 stops
/// right after its reservation; a declaration of 1 is then refused at that reservation and
/// writes nothing. The declaration of 8 runs again, adopts its reservation and stops after
/// `routes/0` and three records; a declaration of 1 is refused by the preflight and writes
/// nothing (it would publish count 1 beside `routes/0` = 8). Running the declaration of 8 again
/// stops after a longer prefix, then once more right after its counter (r5), and a last run
/// finishes it under a new instance with a counter of its own at N = 0; the stopped run's
/// counter stays, named by no formation record. Every record written by an earlier run keeps
/// its bytes and revisions, and the formation record exists only after the run that finished.
/// Unit (<200 ms, an in-memory store).
/// Cut: drop `declare_preflight` from `admin::declare`; the second declaration of 1 is refused
/// at the reservation instead, with another text, red. Drop `reserve` too and it adopts
/// `partitions/1` and writes a formation record of 1 partition.
#[config_log::retcd_test]
fn s1a_an_interrupted_declaration_is_finished_by_its_own_count_only() {
    let rt = runtime();
    let scripted = Arc::new(Scripted::new(Script::default()));
    let store: Arc<dyn ConfigStore> = scripted.clone();
    let stop_after = |puts: u32| {
        scripted.rescript(Script {
            pass_puts: puts,
            fail_put_with: Some(config_core::ConfigError::Unavailable {
                reason: "scripted: the declaring process stopped".to_owned(),
            }),
            ..Script::default()
        });
    };

    let one = || {
        rt.block_on(admin::declare(
            &store,
            &admin::Declaration {
                partitions: 1,
                ..declaration()
            },
        ))
    };
    stop_after(1);
    let stopped = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(stopped, Err(admin::DeclareError::Records(_))),
        "{stopped:?}"
    );
    let reserved = snapshot(&rt, &*store);
    assert_eq!(
        keys(&reserved),
        [ControlKey::Operation(admin::RESERVATION_OPERATION).encode()]
    );
    scripted.rescript(Script::default());
    let refused = one();
    assert!(
        matches!(&refused, Err(admin::DeclareError::Refused(text))
            if text.contains("reserves the store for a declaration of 8 partitions")),
        "{refused:?}"
    );
    assert_eq!(
        snapshot(&rt, &*store),
        reserved,
        "the declaration of 1 wrote nothing at the reservation"
    );

    stop_after(2 + NODE_PUTS + 3);
    let stopped = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(stopped, Err(admin::DeclareError::Records(_))),
        "{stopped:?}"
    );
    let first = snapshot(&rt, &*store);
    assert!(kept(&reserved, &first), "{reserved:?} -> {first:?}");
    let mut expected = vec![
        ControlKey::Operation(admin::RESERVATION_OPERATION).encode(),
        "partitions/1".to_owned(),
        "partitions/2".to_owned(),
        "partitions/3".to_owned(),
        ControlKey::Route(RangeId(0)).encode(),
    ];
    expected.extend(node_keys(3));
    expected.sort();
    assert_eq!(keys(&first), expected);

    scripted.rescript(Script::default());
    let one = one();
    assert!(
        matches!(&one, Err(admin::DeclareError::Refused(text))
            if text.contains("says 8 partitions and this declaration asks for 1")),
        "{one:?}"
    );
    assert_eq!(
        snapshot(&rt, &*store),
        first,
        "the declaration of 1 wrote nothing"
    );

    stop_after(2 + NODE_PUTS + 5);
    let stopped = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(stopped, Err(admin::DeclareError::Records(_))),
        "{stopped:?}"
    );
    let second = snapshot(&rt, &*store);
    assert!(kept(&first, &second), "{first:?} -> {second:?}");
    assert_eq!(keys(&second).len(), 10, "{:?}", keys(&second));
    assert_eq!(
        rt.block_on(admin::read_formation(&*store)).expect("reads"),
        None
    );

    // PR #138 review r5: stopped after its counter, at the formation record. The run that
    // finishes names a new instance and a counter of its own; the stopped run's counter stays at
    // no opens, for an instance no formation record names.
    stop_after(2 + NODE_PUTS + PARTITIONS + 1);
    let stopped = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(stopped, Err(admin::DeclareError::Records(_))),
        "{stopped:?}"
    );
    let third = snapshot(&rt, &*store);
    assert!(kept(&second, &third), "{second:?} -> {third:?}");
    let orphan: Vec<String> = keys(&third)
        .into_iter()
        .filter(|key| key.starts_with("rdb/instances/"))
        .collect();
    assert_eq!(orphan.len(), 1, "{:?}", keys(&third));
    assert_eq!(
        keys(&third),
        declared_keys(PARTITIONS, &[&orphan[0]], false)
    );
    assert_eq!(
        rt.block_on(admin::read_formation(&*store)).expect("reads"),
        None
    );

    scripted.rescript(Script::default());
    let done = rt.block_on(admin::declare(&store, &declaration()));
    let Ok(admin::Declared::Created(formation)) = done else {
        panic!("the last run finishes: {done:?}");
    };
    let fourth = snapshot(&rt, &*store);
    assert!(kept(&third, &fourth), "{third:?} -> {fourth:?}");
    let counter = admin::instance_key(&formation.instance);
    assert_ne!(
        counter, orphan[0],
        "the finishing run names its own instance"
    );
    assert_eq!(
        keys(&fourth),
        declared_keys(PARTITIONS, &[&orphan[0], &counter], true)
    );
    let (count, _) = rt
        .block_on(admin::read_count(&*store, &formation.instance))
        .expect("the declared counter reads");
    assert_eq!(count.n, 0, "the declared counter counts no opens yet");
}

/// One declaration's view of a store shared with another, for the race row. Its `list` of
/// `partitions/` (the preflight's last read) returns only once both declarations have made it,
/// so both have passed the preflight, then waits at `go`. When `hold_marker`, its put of the
/// formation record notifies `at_marker` and waits at `marker`. Every other call passes.
struct DeclareRace {
    inner: Arc<dyn ConfigStore>,
    both: Arc<tokio::sync::Barrier>,
    go: tokio::sync::Semaphore,
    hold_marker: bool,
    marker: tokio::sync::Semaphore,
    at_marker: tokio::sync::Notify,
}

impl DeclareRace {
    fn new(inner: Arc<dyn ConfigStore>, both: Arc<tokio::sync::Barrier>, winner: bool) -> Self {
        Self {
            inner,
            both,
            go: tokio::sync::Semaphore::new(if winner { 1 } else { 0 }),
            hold_marker: winner,
            marker: tokio::sync::Semaphore::new(0),
            at_marker: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl ConfigStore for DeclareRace {
    async fn get(
        &self,
        request: config_core::GetRequest,
    ) -> Result<config_core::GetResponse, config_core::ConfigError> {
        self.inner.get(request).await
    }

    async fn list(
        &self,
        request: config_core::ListRequest,
    ) -> Result<config_core::ListResponse, config_core::ConfigError> {
        let preflight = request.prefix.as_ref() == ControlPrefix::Partitions.encode().as_bytes();
        let listed = self.inner.list(request).await;
        if preflight {
            self.both.wait().await;
            let _permit = self.go.acquire().await.expect("never closed");
        }
        listed
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, config_core::ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(
        &self,
        request: config_core::PutRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        let marker = ControlKey::Operation(admin::FORMATION_OPERATION).encode();
        if self.hold_marker && request.key.as_ref() == marker.as_bytes() {
            self.at_marker.notify_one();
            let _permit = self.marker.acquire().await.expect("never closed");
        }
        self.inner.put(request).await
    }

    async fn delete(
        &self,
        request: config_core::DeleteRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> config_core::Capabilities {
        self.inner.capabilities()
    }

    async fn watch(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// PR #138 review (blocker): declarations of 1 and of 8 partitions run at once and both pass
/// the preflight before either writes. The winner then writes up to its formation record and is
/// held there; the loser runs to its end. In both winner orders the loser is refused at the
/// reservation, and a full snapshot of the store (bytes and revisions) is the same before and
/// after it: it wrote nothing. Released, the winner publishes its own count, and `routes/0`
/// agrees with it. Unit (<200 ms, an in-memory store).
/// Cut: drop `reserve` from `admin::declare`. With 1 winning, the 8 writes `routes/0` and seven
/// records and publishes 8; with 8 winning, the 1 publishes 1 beside `routes/0` = 8. Red in both
/// orders, at the loser's outcome.
#[config_log::retcd_test]
fn s1a_concurrent_declarations_of_one_and_eight_partitions_publish_one_count() {
    let rt = runtime();
    for (won, lost) in [(1, PARTITIONS), (PARTITIONS, 1)] {
        let mem: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let both = Arc::new(tokio::sync::Barrier::new(2));
        let winner = Arc::new(DeclareRace::new(Arc::clone(&mem), Arc::clone(&both), true));
        let loser = Arc::new(DeclareRace::new(Arc::clone(&mem), both, false));
        let declare = |store: Arc<DeclareRace>, partitions: u32| {
            let store: Arc<dyn ConfigStore> = store;
            rt.spawn(async move {
                admin::declare(
                    &store,
                    &admin::Declaration {
                        partitions,
                        ..declaration()
                    },
                )
                .await
            })
        };
        let winning = declare(Arc::clone(&winner), won);
        let losing = declare(Arc::clone(&loser), lost);
        let within = Duration::from_secs(10);
        rt.block_on(async {
            tokio::time::timeout(within, winner.at_marker.notified())
                .await
                .unwrap_or_else(|_| panic!("{won} wins: its formation record's put arrives"));
        });
        let before = snapshot(&rt, &*mem);
        loser.go.add_permits(1);
        let outcome = rt
            .block_on(async { tokio::time::timeout(within, losing).await })
            .unwrap_or_else(|_| panic!("{won} wins: the {lost} ends"))
            .expect("the task ends");
        assert!(
            matches!(&outcome, Err(admin::DeclareError::Refused(text))
                if text.contains(&format!("reserves the store for a declaration of {won} partitions"))
                    && text.contains("nothing was written")),
            "{won} wins: the {lost} is refused at the reservation: {outcome:?}"
        );
        assert_eq!(
            snapshot(&rt, &*mem),
            before,
            "{won} wins: the {lost} wrote nothing"
        );
        winner.marker.add_permits(1);
        let outcome = rt
            .block_on(async { tokio::time::timeout(within, winning).await })
            .unwrap_or_else(|_| panic!("{won} wins: it ends"))
            .expect("the task ends");
        assert!(
            matches!(&outcome, Ok(admin::Declared::Created(formation)) if formation.partitions == won),
            "{won} wins: {outcome:?}"
        );
        assert_eq!(
            rt.block_on(read_partition_count(&mem))
                .expect("routes/0 reads"),
            (won > 1).then_some(won),
            "{won} wins: routes/0 agrees with the published count"
        );
    }
}

/// PR #138 review r5: two identical declarations at once. Both pass the preflight and adopt one
/// reservation; one is held at its formation record while the other runs to its end and
/// publishes. Released, the held one finds that record and answers `Already` with it. Each
/// wrote a counter of its own instance at no opens; the published formation record names one
/// of them, and the other is never named. Unit (<200 ms, an in-memory store).
#[config_log::retcd_test]
fn s1a_concurrent_identical_declarations_publish_one_formation_with_its_counter() {
    let rt = runtime();
    let mem: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let both = Arc::new(tokio::sync::Barrier::new(2));
    let held = Arc::new(DeclareRace::new(Arc::clone(&mem), Arc::clone(&both), true));
    let other = Arc::new(DeclareRace::new(Arc::clone(&mem), both, false));
    let declare = |store: Arc<DeclareRace>| {
        let store: Arc<dyn ConfigStore> = store;
        rt.spawn(async move { admin::declare(&store, &declaration()).await })
    };
    let holding = declare(Arc::clone(&held));
    let running = declare(Arc::clone(&other));
    let within = Duration::from_secs(10);
    rt.block_on(async {
        tokio::time::timeout(within, held.at_marker.notified())
            .await
            .expect("the held declaration reaches its formation record");
    });
    other.go.add_permits(1);
    let ran = rt
        .block_on(async { tokio::time::timeout(within, running).await })
        .expect("the other declaration ends")
        .expect("the task ends");
    let Ok(admin::Declared::Created(published)) = ran else {
        panic!("the declaration that ran publishes: {ran:?}");
    };
    held.marker.add_permits(1);
    let late = rt
        .block_on(async { tokio::time::timeout(within, holding).await })
        .expect("the held declaration ends")
        .expect("the task ends");
    assert!(
        matches!(&late, Ok(admin::Declared::Already(found)) if *found == published),
        "{late:?}"
    );
    assert_eq!(
        rt.block_on(admin::read_formation(&*mem)).expect("reads"),
        Some(published.clone())
    );
    let (count, _) = rt
        .block_on(admin::read_count(&*mem, &published.instance))
        .expect("the published instance's counter reads");
    assert_eq!(count.n, 0, "no opens yet");
    let counters: Vec<String> = keys(&snapshot(&rt, &*mem))
        .into_iter()
        .filter(|key| key.starts_with("rdb/instances/"))
        .collect();
    assert_eq!(counters.len(), 2, "one counter each: {counters:?}");
    assert!(counters.contains(&admin::instance_key(&published.instance)));
}

/// PR #138 review: the formation record is published last. Held at its put, the store
/// already holds `routes/0` and all 8 untouched generation-0 records, and no formation record.
/// A declaration stopped before its last record writes no formation record; a node process on
/// a fresh folder is then refused, makes no copy and writes nothing; once the declaration is
/// run again, that same folder opens and the database becomes ready. Integration (~3 s).
/// Cut: `declare` writes the formation record before `write_records`; the held put finds no
/// partition records, red.
#[config_log::retcd_test]
fn s1a_the_formation_record_is_published_after_every_prerequisite() {
    let rt = runtime();
    let mem: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let marker = ControlKey::Operation(admin::FORMATION_OPERATION).encode();
    let held = PutHold::new(Arc::clone(&mem), &marker, HoldAt::BeforeApply);
    let held_store: Arc<dyn ConfigStore> = held.clone();
    let declaring = rt.spawn(async move { admin::declare(&held_store, &declaration()).await });
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), held.arrived.notified())
            .await
            .expect("the formation record's put arrives");
    });
    let at_marker = snapshot(&rt, &*mem);
    let counter: Vec<String> = keys(&at_marker)
        .into_iter()
        .filter(|key| key.starts_with("rdb/instances/"))
        .collect();
    assert_eq!(
        counter.len(),
        1,
        "the counter is written before the formation record"
    );
    assert_eq!(
        keys(&at_marker),
        declared_keys(PARTITIONS, &[&counter[0]], false)
    );
    for (key, value, create, modified) in &at_marker {
        assert_eq!(create, modified, "{key:?} is untouched");
        if key.starts_with(b"partitions/") {
            assert!(
                PartitionRoot::decode(value).is_none(),
                "{key:?} names no instance yet"
            );
        }
    }
    declaring.abort();
    drop(rt);

    let scripted = Arc::new(Scripted::new(Script {
        pass_puts: 2 + NODE_PUTS + (PARTITIONS - 1),
        fail_put_with: Some(config_core::ConfigError::Unavailable {
            reason: "scripted: the declaring process stopped".to_owned(),
        }),
        ..Script::default()
    }));
    let mut rig = bare_rig(scripted.clone());
    let stopped = rig
        .rt
        .block_on(admin::declare(&rig.store, &rig.declaration()));
    assert!(
        matches!(stopped, Err(admin::DeclareError::Records(_))),
        "{stopped:?}"
    );
    let prefix = snapshot(&rig.rt, &*rig.store);
    assert!(
        !keys(&prefix).contains(&marker),
        "no formation record before the last partition record: {:?}",
        keys(&prefix)
    );
    assert_eq!(
        keys(&prefix).len(),
        PARTITIONS as usize + 1 + NODE_PUTS as usize,
        "{:?}",
        keys(&prefix)
    );

    let (kept_dir, keep) = folder();
    let refused = open_node(
        &rig.rt,
        &rig.store,
        &rig.cert_dir,
        NodeId(1),
        &kept_dir,
        rig.listen(&rig.rt, NodeId(1)),
        &rig.addrs,
    )
    .map(|_| ())
    .expect_err("an unfinished declaration forms nothing");
    assert!(
        refused.to_string().contains("database not formed yet"),
        "{refused}"
    );
    assert!(!has_copies(&kept_dir), "no copy was made");
    assert_eq!(
        snapshot(&rig.rt, &*rig.store),
        prefix,
        "the open wrote nothing"
    );

    scripted.rescript(Script::default());
    rig.declare_created();
    let mut dirs = fresh_dirs();
    dirs[0] = (kept_dir.clone(), keep);
    rig.open_all(dirs);
    rig.wait_ready();
    assert_eq!(copies_in(&kept_dir), ["1"]);
}

/// A control store whose puts under `partitions/` wait at a gate until [`PartitionGate::open`],
/// after notifying [`PartitionGate::arrived`]; every other call passes. When `refuse` is set by
/// then, a put let through fails `Unavailable` instead, as if the opener had stopped there.
struct PartitionGate {
    inner: Arc<dyn ConfigStore>,
    gate: tokio::sync::Semaphore,
    arrived: tokio::sync::Notify,
    refuse: std::sync::atomic::AtomicBool,
}

impl PartitionGate {
    fn open(&self) {
        self.gate
            .add_permits(tokio::sync::Semaphore::MAX_PERMITS / 2);
    }
}

/// Opens a [`PartitionGate`] when dropped, so a row that panics while a put waits at the gate
/// still lets that put's thread finish and its scope end.
struct OpenOnDrop<'g>(&'g PartitionGate);

impl Drop for OpenOnDrop<'_> {
    fn drop(&mut self) {
        self.0.open();
    }
}

#[async_trait::async_trait]
impl ConfigStore for PartitionGate {
    async fn get(
        &self,
        request: config_core::GetRequest,
    ) -> Result<config_core::GetResponse, config_core::ConfigError> {
        self.inner.get(request).await
    }

    async fn list(
        &self,
        request: config_core::ListRequest,
    ) -> Result<config_core::ListResponse, config_core::ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, config_core::ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(
        &self,
        request: config_core::PutRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        if request.key.starts_with(b"partitions/") {
            tracing::info!(key = %String::from_utf8_lossy(&request.key), "test_partition_put_gated");
            self.arrived.notify_one();
            let _permit = self.gate.acquire().await.expect("never closed");
            if self.refuse.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(config_core::ConfigError::Unavailable {
                    reason: "scripted: the opener stopped at its partition record".to_owned(),
                });
            }
        }
        self.inner.put(request).await
    }

    async fn delete(
        &self,
        request: config_core::DeleteRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> config_core::Capabilities {
        self.inner.capabilities()
    }

    async fn watch(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// PR #138 review (blocker): node 2's open is delayed after its copy starts, at its first
/// partition record write, while nodes 1 and 3 open and node 1 roots partition 1. Released,
/// node 2's open still succeeds: it writes only the records of the partitions it owns, so it
/// never meets node 1's root. Its folder keeps its copy, and all three become ready.
/// Integration (~4 s): a declared formation with one open held.
/// Cut: drop the owner filter on `forming` in `open_spawning`; node 2 adopts `partitions/1`,
/// finds it rooted, and its open is refused `AlreadyExists` and removes its `nodes/`, red.
#[config_log::retcd_test]
fn s1a_a_delayed_declared_opener_does_not_write_another_owners_partition() {
    let mut rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    rig.declare_created();
    let gated = Arc::new(PartitionGate {
        inner: Arc::clone(&rig.store),
        gate: tokio::sync::Semaphore::new(0),
        arrived: tokio::sync::Notify::new(),
        refuse: std::sync::atomic::AtomicBool::new(false),
    });
    let gated_store: Arc<dyn ConfigStore> = gated.clone();
    let addrs = rig.addrs.clone();
    let mut listeners: Vec<Option<tokio::net::TcpListener>> = (1..=rig.k)
        .map(|n| Some(rig.listen(&rig.rt, NodeId(n))))
        .collect();
    let dirs = fresh_dirs();
    let rooted_first = |rig: &Rig| {
        let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(20));
        loop {
            let read = rig
                .rt
                .block_on(rig.store.get(config_core::GetRequest {
                    key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                }))
                .expect("the store answers");
            if read
                .record
                .is_some_and(|found| PartitionRoot::decode(&found.value).is_some())
            {
                return;
            }
            assert!(Instant::now() < deadline, "node 1 never rooted partition 1");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let opened: Vec<(NodeId, Db, Arc<Net>)> = std::thread::scope(|scope| {
        let _release = OpenOnDrop(&gated);
        let late_listener = listeners[1].take().expect("node 2's listener");
        let late_dir = dirs[1].0.clone();
        let (rt, cert_dir, addrs) = (&rig.rt, &rig.cert_dir, &addrs);
        let gated_store = &gated_store;
        let late = scope.spawn(move || {
            open_node(
                rt,
                gated_store,
                cert_dir,
                NodeId(2),
                &late_dir,
                late_listener,
                addrs,
            )
        });
        rig.rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(20), gated.arrived.notified())
                .await
                .expect("node 2's open reaches its first partition record write");
        });
        let mut opened = Vec::new();
        for node in [NodeId(1), NodeId(3)] {
            let (db, net) = open_node(
                &rig.rt,
                &rig.store,
                &rig.cert_dir,
                node,
                &dirs[node.0 as usize - 1].0,
                listeners[node.0 as usize - 1].take().expect("a listener"),
                addrs,
            )
            .unwrap_or_else(|e| panic!("node {} opens: {e}", node.0));
            opened.push((node, db, net));
        }
        rooted_first(&rig);
        gated.open();
        let (db, net) = late
            .join()
            .expect("node 2's thread")
            .unwrap_or_else(|e| panic!("node 2's delayed open succeeds: {e}"));
        opened.insert(1, (NodeId(2), db, net));
        opened
    });
    assert_eq!(
        copies_in(&dirs[1].0),
        ["2"],
        "node 2's folder keeps its copy"
    );
    for ((node, db, net), (dir, keep)) in opened.into_iter().zip(dirs) {
        rig.procs.push(Proc {
            node,
            db: Some(db),
            _net: Some(net),
            dir,
            _dir: keep,
            own_rt: None,
        });
    }
    rig.wait_ready();
}

/// PR #138 review (blocker): a whole-database open (`--form`, `lr up`) of a declared store on
/// an empty folder would bind an instance of its own beside the declared one. It is refused
/// before anything is bound or started: no `nodes/` is left and the store is unchanged. Unit
/// (<1 s, an in-memory store).
/// Cut: drop the declared refusal in `Db::check`; the open binds a fresh instance and forms,
/// red at `expect_err`.
#[config_log::retcd_test]
fn s1a_a_whole_database_open_of_a_declared_store_is_refused() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let Ok(admin::Declared::Created(formation)) =
        rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("declared");
    };
    let declared = snapshot(&rt, &*store);
    let dir = config_testkit::fs::temp_dir();
    let refused = rt
        .block_on(Db::open_with(
            config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .map(|_| ())
        .expect_err("a whole-database open does not form a declared database");
    let text = refused.to_string();
    assert!(
        matches!(refused, OpenError::Config(_))
            && text.contains(&admin::instance_hex(&formation.instance))
            && text.contains("nothing was created"),
        "{text}"
    );
    assert!(!has_copies(dir.path()), "nothing was bound");
    assert_eq!(snapshot(&rt, &*store), declared, "nothing was written");
}

/// PR #138 review r6 (blocker): the declared refusal does not rest on `partitions/1`. An empty
/// declared store with `partitions/1` and the counter both deleted by hand still holds its
/// formation record, so a whole-database open is refused before anything is bound: it does
/// not form a database of its own, or count N = 1 on a counter of its own. No `nodes/` is left
/// and the store is unchanged. Unit (<1 s, an in-memory store).
/// Cut: refuse only over an untouched `partitions/1` (b53bd7a's condition in `Db::check`); the
/// open forms a fresh instance and creates its counter, red at `expect_err`.
#[config_log::retcd_test]
fn s1a_a_whole_open_of_a_declared_store_without_partitions_1_or_its_counter_is_refused() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let Ok(admin::Declared::Created(formation)) =
        rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("declared");
    };
    for key in [
        ControlKey::Partition(PARTITION).encode(),
        admin::instance_key(&formation.instance),
    ] {
        rt.block_on(store.delete(config_core::DeleteRequest {
            key: Bytes::from(key.clone()),
            expected_mod_revision: None,
            dedup: None,
        }))
        .unwrap_or_else(|error| panic!("{key} is deleted: {error}"));
    }
    let before = snapshot(&rt, &*store);
    assert_eq!(
        keys(&before),
        declared_keys(PARTITIONS, &[], true)
            .into_iter()
            .filter(|key| *key != ControlKey::Partition(PARTITION).encode())
            .collect::<Vec<_>>(),
        "partitions/1 and the counter are gone"
    );
    let dir = config_testkit::fs::temp_dir();
    let refused = rt
        .block_on(Db::open_with(
            config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .map(|_| ())
        .expect_err("a whole-database open does not form a declared database");
    let text = refused.to_string();
    assert!(
        matches!(refused, OpenError::Config(_))
            && text.contains(&admin::instance_hex(&formation.instance))
            && text.contains("nothing was created"),
        "{text}"
    );
    assert!(!has_copies(dir.path()), "nothing was bound");
    assert_eq!(snapshot(&rt, &*store), before, "nothing was written");
}

/// PR #138 review r6 (sweep): a node process of a declared store does not read a missing record
/// as one still to write. With `routes/0` deleted by hand, node 1 does not open the 8-partition
/// declaration as a 1-partition database; with `partitions/2` deleted, it does not mark that
/// partition as one to form from nothing: the declaration wrote every partition record. Each is
/// refused, naming the formation record's count or the missing record: nothing is written and
/// no `nodes/` is left. Unit (<1 s per case, one node process refused before it starts).
/// Cut: drop the count comparison in `open_spawning` (case `routes/0`) or the `declared` guard
/// on `PartitionState::Absent` in `check_others` (case `partitions/2`); node 1 opens, red.
#[config_log::retcd_test]
fn s1a_a_declared_node_process_missing_routes_or_a_partition_record_is_refused() {
    for gone in [
        ControlKey::Route(RangeId(0)).encode(),
        ControlKey::Partition(PartitionId(2)).encode(),
    ] {
        let rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
        rig.declare_created();
        rig.rt
            .block_on(rig.store.delete(config_core::DeleteRequest {
                key: Bytes::from(gone.clone()),
                expected_mod_revision: None,
                dedup: None,
            }))
            .unwrap_or_else(|error| panic!("{gone} is deleted: {error}"));
        let before = snapshot(&rig.rt, &*rig.store);
        assert!(!keys(&before).contains(&gone), "{gone} is gone");
        let (dir, _keep) = folder();
        let opened = open_node(
            &rig.rt,
            &rig.store,
            &rig.cert_dir,
            NodeId(1),
            &dir,
            rig.listen(&rig.rt, NodeId(1)),
            &rig.addrs,
        );
        let Err(refused) = opened.map(|(mut db, _net)| db.shutdown()) else {
            panic!("{gone}: node 1 opened a declared store missing it");
        };
        let text = refused.to_string();
        assert!(
            matches!(refused, OpenError::Config(_))
                && text.contains(&gone)
                && text.contains(&format!("declared with {PARTITIONS} partitions"))
                && !text.contains("  "),
            "{gone}: {text}"
        );
        assert_eq!(
            snapshot(&rig.rt, &*rig.store),
            before,
            "{gone}: nothing written"
        );
        assert!(!has_copies(&dir), "{gone}: no copy was made");
    }
}

/// PR #138 review: a node process's first read, the formation record, failing `Unavailable`
/// says `control store:` once and gives the step to take. Unit (<100 ms).
/// Cut: `formation_refused` maps `Store` through `to_string` as before; the text says
/// `control store:` twice and has no step, red.
#[config_log::retcd_test]
fn s1a_an_unreachable_store_at_the_formation_read_says_what_to_do() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
        fail_gets: 1,
        ..Script::default()
    }));
    let dir = config_testkit::fs::temp_dir();
    let refused = rt
        .block_on(Db::open_with(
            DbConfig {
                local: vec![NodeId(2)],
                boot: Some(next_boot(dir.path()).expect("a boot")),
                ..config(dir.path())
            },
            store,
            rt.handle().clone(),
            fast(),
        ))
        .map(|_| ())
        .expect_err("the store is unreachable");
    let text = refused.to_string();
    assert!(
        matches!(refused, OpenError::Control(_))
            && text.matches("control store:").count() == 1
            && text.contains("get operations/")
            && text.ends_with(RETCD_STEP),
        "{text}"
    );
}

/// The partitions of `partitions` that `wiped` owns and that are rooted in the store: each with
/// its record's owner and generation.
fn rooted_of_wiped(
    rig: &Rig,
    partitions: u32,
    wiped: NodeId,
) -> Vec<(PartitionId, NodeId, Generation)> {
    rig.rt
        .block_on(partition_states(&rig.store, partitions))
        .expect("the store answers")
        .into_iter()
        .filter_map(|(partition, state)| match state {
            PartitionState::Rooted(record, _)
                if admin::owner_in(&ALL_NODES, partition) == wiped =>
            {
                Some((partition, record.owner, record.generation))
            }
            _ => None,
        })
        .collect()
}

/// #137's state: declare `partitions` over nodes 1 to 3 in `store` and open them, `wiped`'s
/// commit hook faulting it right after its first root commits, before that generation is linked
/// or activated, so no write is ever admitted; wait until one of its partitions is rooted, then
/// end every process and lose `wiped`'s whole folder. Answers the rig, every process ended, and
/// the partitions of `wiped` that were rooted.
fn wiped_before_any_write(
    store: Arc<dyn ConfigStore>,
    partitions: u32,
    wiped: NodeId,
) -> (Rig, Vec<(PartitionId, NodeId, Generation)>) {
    let mut rig = bare_rig(store);
    match rig
        .rt
        .block_on(admin::declare(&rig.store, &rig.declaration_of(partitions)))
    {
        Ok(admin::Declared::Created(_)) => {}
        other => panic!("the declaration is written: {other:?}"),
    }
    let dirs = fresh_dirs();
    crate::host::set_after_commit(
        &dirs[wiped.0 as usize - 1].0,
        Some(crate::host::AfterCommit::Fault),
    );
    rig.open_all(dirs);
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(20));
    while rooted_of_wiped(&rig, partitions, wiped).is_empty() {
        assert!(
            Instant::now() < deadline,
            "node {} never rooted a partition",
            wiped.0
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    rig.shutdown_all();
    crate::host::set_after_commit(&rig.procs[wiped.0 as usize - 1].dir, None);
    let rooted = rooted_of_wiped(&rig, partitions, wiped);
    rig.wipe(wiped);
    (rig, rooted)
}

/// #137: [`wiped_before_any_write`], then every process reopens: the wiped owner's open rolls
/// each rooted partition it owns forward onto its own empty copy (no promote, no re-form),
/// every partition takes a write, and after a restart of all three every write reads back.
/// Returns the partitions that were rolled forward.
fn wiped_owner_before_any_write_rolls_forward(
    method: &str,
    partitions: u32,
    wiped: NodeId,
) -> Vec<PartitionId> {
    let owner_of = |partition: u32| admin::owner_in(&ALL_NODES, PartitionId(partition));
    let (mut rig, rooted) =
        wiped_before_any_write(Arc::new(config_testkit::MemStore::new()), partitions, wiped);
    rig.restart_all();
    rig.wait_ready();
    // The roll-forward names the root's generation (below); the reopen's heal may commit a
    // later one, but the owner stays: nothing promoted the partition away.
    let after = rooted_of_wiped(&rig, partitions, wiped);
    for (partition, owner, generation) in &rooted {
        assert!(
            after
                .iter()
                .any(|(p, o, g)| p == partition && o == owner && g >= generation),
            "partition {} keeps owner {}: {after:?}",
            partition.0,
            owner.0
        );
    }
    let rolled: Vec<serde_json::Value> =
        config_testkit::logs::lines_for_current_test(module_path!(), method)
            .into_iter()
            .filter(|line| line["@m"] == "db_open_rolled_forward")
            .collect();
    for (partition, _, generation) in &rooted {
        // Partition 1's line names no partition; a later partition's names it. `copies` is
        // logged as its debug text.
        let names = |line: &serde_json::Value| {
            line["generation"] == generation.0
                && line["copies"] == format!("[{}]", wiped.0)
                && match partition.0 {
                    1 => line["partition"].is_null(),
                    n => line["partition"] == n,
                }
        };
        assert!(
            rolled.iter().any(names),
            "partition {} rolled forward onto node {}'s copy: {rolled:?}",
            partition.0,
            wiped.0
        );
    }
    let groups: Vec<(u32, AffinityId)> = (1..=partitions)
        .map(|partition| (partition, group_of(rig.of(NodeId(1)), partition)))
        .collect();
    for (partition, group) in &groups {
        let put = put_lands(rig.of(owner_of(*partition)), *group, b"k", b"v");
        assert_eq!(put.partition, PartitionId(*partition), "{put:?}");
    }
    rig.restart_all();
    rig.wait_ready();
    for (partition, group) in &groups {
        let got = rig
            .of(owner_of(*partition))
            .get_in(*group, b"k")
            .expect("a get");
        assert_eq!(
            got.value.as_ref().map(|(_, value)| &value[..]),
            Some(&b"v"[..]),
            "partition {partition} keeps its write"
        );
    }
    rooted
        .into_iter()
        .map(|(partition, ..)| partition)
        .collect()
}

/// #137, a later partition: node 2 owns no partition 1, so its reopen reaches the owner's empty
/// copy in `Db::decide_partition` (from `Db::check_others`). Integration (~8 s).
/// Cut: `Db::decide_partition` refuses the owner's empty copy unconditionally (its
/// `empty_owner` call answers the old refusal); `restart_all` panics "has not finished forming".
#[config_log::retcd_test]
fn s1a_a_declared_owner_wiped_before_any_write_rolls_forward_a_later_partition() {
    const METHOD: &str =
        "s1a_a_declared_owner_wiped_before_any_write_rolls_forward_a_later_partition";
    assert_ne!(admin::owner_in(&ALL_NODES, PARTITION), NodeId(2));
    let rolled = wiped_owner_before_any_write_rolls_forward(METHOD, PARTITIONS, NodeId(2));
    assert!(
        !rolled.is_empty() && !rolled.contains(&PARTITION),
        "{rolled:?}"
    );
}

/// #137, partition 1: one partition, owned by node 1, so the reopen reaches the owner's empty
/// copy in `Db::check`. Integration (~6 s).
/// Cut: `Db::check`'s owner-local branch refuses unconditionally (its `empty_owner` call
/// answers the old refusal); `restart_all` panics "has not finished forming".
#[config_log::retcd_test]
fn s1a_a_declared_owner_wiped_before_any_write_rolls_forward_partition_1() {
    const METHOD: &str = "s1a_a_declared_owner_wiped_before_any_write_rolls_forward_partition_1";
    assert_eq!(admin::owner_in(&ALL_NODES, PARTITION), NodeId(1));
    let rolled = wiped_owner_before_any_write_rolls_forward(METHOD, 1, NodeId(1));
    assert_eq!(rolled, [PARTITION]);
}

/// PR #156 review (question): `partitions/1` is read twice by a reopen of the owner's empty
/// copy, `formed_member`'s `get` (the instance) and `partition_states`' `list` (the record and
/// root the roll uses). Replacing the control-store namespace while an open runs is not
/// supported, but a root of another instance on the second read must not be rolled forward
/// under the first read's instance. #137's state, then the store's `list` answers
/// `partitions/1` naming another instance: node 1's reopen is refused naming both instances,
/// binds nothing and writes nothing. Integration (~4 s).
/// Cut: drop the instance comparison in `Db::check`'s owner-local branch; the reopen rolls the
/// other instance's root forward and opens, red.
#[config_log::retcd_test]
fn pr156_partition_1_read_again_under_another_instance_is_refused() {
    let swapping =
        crate::test_store::SwapPartitionOneInstance::new(Arc::new(config_testkit::MemStore::new()));
    let (rig, rooted) =
        wiped_before_any_write(Arc::clone(&swapping) as Arc<dyn ConfigStore>, 1, NodeId(1));
    assert_eq!(rooted.len(), 1, "partition 1 is rooted: {rooted:?}");
    let Some((_, PartitionState::Rooted(_, root))) = rig
        .rt
        .block_on(partition_states(&rig.store, 1))
        .expect("the store answers")
        .pop()
    else {
        panic!("partition 1 has a root");
    };
    let mut other = root.instance;
    for byte in &mut other {
        *byte = !*byte;
    }
    swapping.arm(&root.instance, &other);
    let before = snapshot(&rig.rt, &*rig.store);
    let refused = rig
        .reopen(NodeId(1))
        .map(|_| ())
        .expect_err("a root of another instance is not rolled forward");
    let text = refused.to_string();
    assert!(
        matches!(refused, OpenError::Mismatch(_))
            && text.contains(&format!(
                "partitions/1 named instance {} and then {} while this open read it",
                admin::instance_hex(&root.instance),
                admin::instance_hex(&other)
            )),
        "{text}"
    );
    assert!(!has_copies(&rig.procs[0].dir), "nothing bound");
    assert_eq!(snapshot(&rig.rt, &*rig.store), before, "nothing written");
}

/// PR #138 review: after acknowledged writes, node 1 loses its whole folder. With the
/// formation record present, its reopen is refused with the promote step: it never forms
/// again, binds nothing, starts nothing and writes nothing. Integration (~5 s).
#[config_log::retcd_test]
fn s1a_a_declared_owner_wiped_after_acknowledged_writes_is_told_to_promote() {
    let mut rig = declared_rig();
    rig.wait_ready();
    for partition in (1..=PARTITIONS).filter(|p| admin::initial_owner(PartitionId(*p)) == NodeId(1))
    {
        let db = rig.of(NodeId(1));
        put_lands(db, group_of(db, partition), b"k", b"v");
    }
    rig.shutdown_all();
    let before = snapshot(&rig.rt, &*rig.store);
    rig.wipe(NodeId(1));
    let refused = rig
        .reopen(NodeId(1))
        .map(|_| ())
        .expect_err("the wiped owner is refused");
    assert!(refused.to_string().contains("admin promote"), "{refused}");
    assert!(!has_copies(&rig.procs[0].dir), "nothing bound");
    assert_eq!(snapshot(&rig.rt, &*rig.store), before, "nothing written");
}

/// PR #138 review (author question): the open counter is its own key, `rdb/instances/<hex>`, so
/// a manual delete, or a restore of some prefixes and not others, can leave it absent beside
/// rooted partitions and copies with data. A declared node process reopening its folder then
/// is refused, naming the absent counter: it does not create it again at N = 1 and repeat the
/// request ids N derives. Nothing is written and the folder keeps its copy. Integration (~3 s).
/// Cut: count with `create` whenever the database is declared (`found.fresh ||
/// declared.is_some()` in `open_spawning`); the reopen counts N = 1 and opens, red.
#[config_log::retcd_test]
fn s1a_a_declared_reopen_with_its_counter_gone_is_refused() {
    let mut rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    let formation = rig.declare_created();
    rig.open_all(fresh_dirs());
    rig.wait_ready();
    rig.shutdown_all();
    let counter = admin::instance_key(&formation.instance);
    rig.rt
        .block_on(rig.store.delete(config_core::DeleteRequest {
            key: Bytes::from(counter.clone()),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("the counter is deleted");
    let before = snapshot(&rig.rt, &*rig.store);
    assert!(
        !keys(&before).contains(&counter),
        "{counter} is gone: {:?}",
        keys(&before)
    );
    for node in ALL_NODES {
        let refused = rig
            .reopen(node)
            .map(|_| ())
            .expect_err("a reopen with no counter is refused");
        assert!(
            refused
                .to_string()
                .contains(&format!("{counter} is absent")),
            "node {}: {refused}",
            node.0
        );
        assert_eq!(
            snapshot(&rig.rt, &*rig.store),
            before,
            "node {}: nothing written",
            node.0
        );
        assert_eq!(
            copies_in(&rig.procs[node.0 as usize - 1].dir),
            [node.0.to_string()],
            "node {}: the folder keeps its copy",
            node.0
        );
    }
}

/// PR #138 review r3 (blocker): the counter-gone case after a partial formation. Node 1's open
/// is held at its `partitions/1` write while nodes 2 and 3 open, so partition 2 (node 2's) is
/// rooted and takes a write; node 1's open then stops there, leaving partition 1 untouched. With the counter then deleted, node 2 and node 3 reopening their folders,
/// and node 1 opening its first, empty one, are each refused, naming the absent counter: none
/// creates it again at N = 1. Nothing is written, nodes 2 and 3 keep their copies, and node 1's
/// open leaves no `nodes/`. Integration (~4 s): a partial formation over real links.
/// Cut: count with `create` whenever partition 1 is still to form (`found.fresh ||
/// found.forms_first`, as at 54db485); node 2's reopen counts N = 1 and opens, red. Since r5 a
/// declared open never counts with `create` (`admin::declare` wrote the counter), so the r3 cut
/// of the "no other partition rooted" clause has no code left to cut.
#[config_log::retcd_test]
fn s1a_a_partially_formed_declared_reopen_with_its_counter_gone_is_refused() {
    let rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    let formation = rig.declare_created();
    let gated = Arc::new(PartitionGate {
        inner: Arc::clone(&rig.store),
        gate: tokio::sync::Semaphore::new(0),
        arrived: tokio::sync::Notify::new(),
        refuse: std::sync::atomic::AtomicBool::new(true),
    });
    let gated_store: Arc<dyn ConfigStore> = gated.clone();
    let dirs = fresh_dirs();
    let addrs = rig.addrs.clone();
    let mut listeners: Vec<Option<tokio::net::TcpListener>> = (1..=rig.k)
        .map(|n| Some(rig.listen(&rig.rt, NodeId(n))))
        .collect();
    assert_eq!(admin::initial_owner(PartitionId(2)), NodeId(2));
    std::thread::scope(|scope| {
        let _release = OpenOnDrop(&gated);
        let first_listener = listeners[0].take().expect("node 1's listener");
        let first_dir = dirs[0].0.clone();
        let (rt, cert_dir, addrs) = (&rig.rt, &rig.cert_dir, &addrs);
        let gated_store = &gated_store;
        let held = scope.spawn(move || {
            open_node(
                rt,
                gated_store,
                cert_dir,
                NodeId(1),
                &first_dir,
                first_listener,
                addrs,
            )
            .map(|_| ())
        });
        rig.rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(20), gated.arrived.notified())
                .await
                .expect("node 1's open reaches its partition 1 record write");
        });
        let mut opened = Vec::new();
        for node in [NodeId(2), NodeId(3)] {
            let (db, net) = open_node(
                &rig.rt,
                &rig.store,
                &rig.cert_dir,
                node,
                &dirs[node.0 as usize - 1].0,
                listeners[node.0 as usize - 1].take().expect("a listener"),
                addrs,
            )
            .unwrap_or_else(|e| panic!("node {} opens: {e}", node.0));
            opened.push((db, net));
        }
        put_lands(&opened[0].0, group_of(&opened[0].0, 2), b"k", b"v");
        for (db, _) in &mut opened {
            db.shutdown();
        }
        gated.open();
        let stopped = held.join().expect("node 1's thread");
        assert!(stopped.is_err(), "node 1's open stopped before partition 1");
    });
    let first = rig
        .rt
        .block_on(partition_states(&rig.store, PARTITION.0))
        .expect("partitions/1 reads")
        .pop();
    assert!(
        matches!(
            first,
            Some((_, PartitionState::Unrooted { untouched: true }))
        ),
        "partition 1 is still untouched"
    );
    let counter = admin::instance_key(&formation.instance);
    rig.rt
        .block_on(rig.store.delete(config_core::DeleteRequest {
            key: Bytes::from(counter.clone()),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("the counter is deleted");
    let before = snapshot(&rig.rt, &*rig.store);
    assert!(!keys(&before).contains(&counter), "{counter} is gone");
    for node in [NodeId(2), NodeId(3), NodeId(1)] {
        let dir = &dirs[node.0 as usize - 1].0;
        let opened = open_node(
            &rig.rt,
            &rig.store,
            &rig.cert_dir,
            node,
            dir,
            rig.listen(&rig.rt, node),
            &rig.addrs,
        );
        let Err(refused) = opened else {
            panic!("node {}: an open with no counter opened", node.0);
        };
        assert!(
            refused
                .to_string()
                .contains(&format!("{counter} is absent")),
            "node {}: {refused}",
            node.0
        );
        assert_eq!(
            snapshot(&rig.rt, &*rig.store),
            before,
            "node {}: nothing written",
            node.0
        );
        if node == NodeId(1) {
            assert!(!has_copies(dir), "node 1: no copy was made");
        } else {
            assert_eq!(
                copies_in(dir),
                [node.0.to_string()],
                "node {}: the folder keeps its copy",
                node.0
            );
        }
    }
}

/// PR #138 review r3 (ADR question): a declaration stopped after every partition record and
/// before its formation record has declared nothing yet, so a whole-database open (`--form`,
/// `lr up`) of the same count may take the store over: it forms all three copies in its one
/// folder over the untouched records. The database is then a `--form` database, and running
/// the stopped declaration again is refused (partition 1 is rooted) and writes nothing; no
/// formation record is ever published. Integration (~2 s): one whole-database formation.
#[config_log::retcd_test]
fn s1a_a_form_takes_over_a_declaration_stopped_before_its_formation_record() {
    let rt = runtime();
    let scripted = Arc::new(Scripted::new(Script {
        pass_puts: 2 + NODE_PUTS + PARTITIONS,
        fail_put_with: Some(config_core::ConfigError::Unavailable {
            reason: "scripted: the declaring process stopped".to_owned(),
        }),
        ..Script::default()
    }));
    let store: Arc<dyn ConfigStore> = scripted.clone();
    let stopped = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(stopped, Err(admin::DeclareError::Records(_))),
        "{stopped:?}"
    );
    assert_eq!(
        keys(&snapshot(&rt, &*store)),
        declared_keys(PARTITIONS, &[], false)
    );
    scripted.rescript(Script::default());
    let dir = config_testkit::fs::temp_dir();
    let mut db = rt
        .block_on(Db::open_with(
            DbConfig {
                partitions: Some(PARTITIONS),
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .unwrap_or_else(|e| panic!("the whole-database open forms: {e}"));
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .unwrap_or_else(|e| panic!("ready: {e}"));
    assert_eq!(copies_in(dir.path()), ["1", "2", "3"]);
    db.shutdown();
    let formed = snapshot(&rt, &*store);
    let again = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(&again, Err(admin::DeclareError::Refused(_))),
        "{again:?}"
    );
    assert_eq!(snapshot(&rt, &*store), formed, "the retry wrote nothing");
    assert_eq!(
        rt.block_on(admin::read_formation(&*store)).expect("reads"),
        None
    );
}

/// PR #138 review r3: the reservation's unknown outcome. The reservation put answers
/// `DeadlineExceededUnknownOutcome`, once having applied (a lost reply) and once not (a dropped
/// put), and the read that resolves it fails, on two attempts in a row. Each attempt fails
/// naming that read, and leaves only what landed: the reservation in the applied case, nothing
/// in the other. A third attempt, with the store whole again, adopts or creates the reservation
/// and finishes: every record written before keeps its bytes and revisions. A declaration of
/// another count is then refused and writes nothing. Unit (<100 ms, an in-memory store).
/// Cut: `create_or_adopt` takes an unknown outcome as created; the first attempt goes on and
/// publishes, red.
#[config_log::retcd_test]
fn s1a_a_reservation_of_unknown_outcome_whose_read_fails_is_finished_by_a_retry() {
    let rt = runtime();
    for applied in [true, false] {
        let scripted = Arc::new(Scripted::new(Script::default()));
        let store: Arc<dyn ConfigStore> = scripted.clone();
        let reservation = ControlKey::Operation(admin::RESERVATION_OPERATION).encode();
        let mut before = snapshot(&rt, &*store);
        for attempt in 1..=2 {
            // The formation record, reservation (#80 S1b preflight, PR #144 review) and
            // `routes/0` reads pass; the read after the put fails.
            scripted.rescript(Script {
                pass_gets: 3,
                fail_gets: 1,
                lose_put_replies: u32::from(applied),
                drop_puts: u32::from(!applied),
                ..Script::default()
            });
            let failed = rt.block_on(admin::declare(&store, &declaration()));
            assert!(
                matches!(&failed, Err(admin::DeclareError::Records(error))
                    if error.to_string().contains("scripted: the read failed")),
                "applied {applied}, attempt {attempt}: {failed:?}"
            );
            let after = snapshot(&rt, &*store);
            let expected: Vec<String> = if applied {
                vec![reservation.clone()]
            } else {
                Vec::new()
            };
            assert_eq!(
                keys(&after),
                expected,
                "applied {applied}, attempt {attempt}"
            );
            assert!(
                kept(&before, &after),
                "applied {applied}, attempt {attempt}"
            );
            before = after;
        }
        scripted.rescript(Script::default());
        let done = rt.block_on(admin::declare(&store, &declaration()));
        let Ok(admin::Declared::Created(formation)) = &done else {
            panic!("applied {applied}: {done:?}");
        };
        assert_eq!(formation.partitions, PARTITIONS, "applied {applied}");
        let finished = snapshot(&rt, &*store);
        assert!(
            kept(&before, &finished),
            "applied {applied}: {before:?} -> {finished:?}"
        );
        let counter = admin::instance_key(&formation.instance);
        assert_eq!(
            keys(&finished),
            declared_keys(PARTITIONS, &[&counter], true)
        );
        let other = rt.block_on(admin::declare(
            &store,
            &admin::Declaration {
                partitions: 1,
                ..declaration()
            },
        ));
        assert!(
            matches!(&other, Err(admin::DeclareError::Refused(_))),
            "applied {applied}: {other:?}"
        );
        assert_eq!(
            snapshot(&rt, &*store),
            finished,
            "applied {applied}: the other count wrote nothing"
        );
    }
}

/// PR #138 review r3: a reservation that does not decode (another build's layout, or damage)
/// refuses a declaration as `Undecodable`, naming the reservation's key, and the whole store,
/// bytes and revisions, is unchanged. Unit (<100 ms, an in-memory store).
/// Cut: `adopt` adopts a record whatever its bytes; the declaration adopts the seeded bytes
/// and publishes, red.
#[config_log::retcd_test]
fn s1a_an_undecodable_reservation_refuses_the_declaration_and_writes_nothing() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let reservation = ControlKey::Operation(admin::RESERVATION_OPERATION).encode();
    rt.block_on(store.put(config_core::PutRequest {
        key: Bytes::from(reservation.clone()),
        value: Bytes::from_static(&[9, 0, 0]),
        expected_mod_revision: Some(0),
        dedup: None,
    }))
    .expect("seed an undecodable reservation");
    let before = snapshot(&rt, &*store);
    let refused = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(&refused, Err(admin::DeclareError::Undecodable { key }) if *key == reservation),
        "{refused:?}"
    );
    assert_eq!(snapshot(&rt, &*store), before, "nothing was written");
}

/// A store whose counter puts race other opens, for the counter rows. Each of the first
/// `conflicts` puts on the counter first lets another open count (`count_open` straight on
/// `inner`, which creates the counter if it is absent), so the put it then forwards conflicts.
/// When `vanish`, the counter is deleted once the last of them has conflicted, whether it was a
/// create or a CAS, and every counter put after that is counted in `late_puts`.
struct CounterRace {
    inner: Arc<dyn ConfigStore>,
    key: Bytes,
    instance: [u8; INSTANCE_LEN],
    conflicts: std::sync::atomic::AtomicU32,
    vanish: bool,
    vanished: std::sync::atomic::AtomicBool,
    late_puts: std::sync::atomic::AtomicU32,
}

#[async_trait::async_trait]
impl ConfigStore for CounterRace {
    async fn get(
        &self,
        request: config_core::GetRequest,
    ) -> Result<config_core::GetResponse, config_core::ConfigError> {
        self.inner.get(request).await
    }

    async fn list(
        &self,
        request: config_core::ListRequest,
    ) -> Result<config_core::ListResponse, config_core::ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, config_core::ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(
        &self,
        request: config_core::PutRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        use std::sync::atomic::Ordering;
        if request.key != self.key {
            return self.inner.put(request).await;
        }
        if self.vanished.load(Ordering::SeqCst) {
            self.late_puts.fetch_add(1, Ordering::SeqCst);
            return self.inner.put(request).await;
        }
        let left = self.conflicts.load(Ordering::SeqCst);
        if left > 0 {
            self.conflicts.store(left - 1, Ordering::SeqCst);
            admin::count_open(&*self.inner, &self.instance, true, admin::COUNT_WITHIN)
                .await
                .expect("the other open counts");
            if left == 1 && self.vanish {
                let answer = self.inner.put(request).await;
                let deleted = self
                    .inner
                    .delete(config_core::DeleteRequest {
                        key: self.key.clone(),
                        expected_mod_revision: None,
                        dedup: None,
                    })
                    .await
                    .expect("delete the counter");
                assert_eq!(deleted.outcome, config_core::MutationOutcome::Applied);
                self.vanished.store(true, Ordering::SeqCst);
                return answer;
            }
        }
        self.inner.put(request).await
    }

    async fn delete(
        &self,
        request: config_core::DeleteRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> config_core::Capabilities {
        self.inner.capabilities()
    }

    async fn watch(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// PR #138 review r4 (blocker): an open allowed to create the counter (`fresh`) reads it present,
/// loses its CAS to other opens one or more times, and the counter is then deleted before it
/// reads again. It is refused `Absent` and writes nothing: a counter it has seen is never
/// recreated, since a new N = 1 would overlap the request range of the open that already holds
/// 1. Started on an empty store too, where its own create loses to another open's, and the
/// counter may be deleted right after that lost create (r5). Against an intact counter the same
/// conflicts are retried and counted after the others, and a first creation with no race is
/// N = 1. Unit (<1 s, an in-memory store; each conflict waits 50 ms).
/// Cut 1: `count_open` keeps `fresh` across a read that found the counter; the retry recreates
/// N = 1, red. Cut 2: a conflicting create leaves `may_create` set; the unseeded 1-conflict case
/// recreates N = 1, red.
#[config_log::retcd_test]
fn s1a_a_counter_deleted_after_a_lost_cas_is_never_recreated() {
    let rt = runtime();
    let instance = [0x5a; INSTANCE_LEN];
    let race = |vanish: bool, conflicts: u32, seeded: bool| {
        let inner: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        if seeded {
            assert_eq!(
                rt.block_on(admin::count_open(
                    &*inner,
                    &instance,
                    true,
                    admin::COUNT_WITHIN
                )),
                Ok(1),
                "the first creation"
            );
        }
        CounterRace {
            inner,
            key: Bytes::from(admin::instance_key(&instance)),
            instance,
            conflicts: std::sync::atomic::AtomicU32::new(conflicts),
            vanish,
            vanished: std::sync::atomic::AtomicBool::new(false),
            late_puts: std::sync::atomic::AtomicU32::new(0),
        }
    };
    for seeded in [true, false] {
        // Unseeded, the first conflict is on the open's own create: with 1, the counter is
        // deleted right after that lost create (PR #138 review r5).
        let lost: &[u32] = if seeded { &[1, 3] } else { &[1, 2, 3] };
        for &conflicts in lost {
            let case = format!("seeded {seeded}, {conflicts} conflicts, then deleted");
            let store = race(true, conflicts, seeded);
            let counted = rt.block_on(admin::count_open(
                &store,
                &instance,
                true,
                admin::COUNT_WITHIN,
            ));
            assert_eq!(
                counted,
                Err(admin::CounterError::Absent {
                    key: admin::instance_key(&instance)
                }),
                "{case}"
            );
            assert!(
                store.vanished.load(std::sync::atomic::Ordering::SeqCst),
                "{case}: the counter was deleted"
            );
            assert_eq!(
                store.late_puts.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "{case}: no put after the delete"
            );
            assert_eq!(
                snapshot(&rt, &*store.inner),
                Vec::new(),
                "{case}: nothing written"
            );
        }
        for conflicts in [0, 3] {
            let case = format!("seeded {seeded}, {conflicts} conflicts, counter intact");
            let store = race(false, conflicts, seeded);
            let counted = rt.block_on(admin::count_open(
                &store,
                &instance,
                true,
                admin::COUNT_WITHIN,
            ));
            assert_eq!(
                counted,
                Ok(u64::from(seeded) + u64::from(conflicts) + 1),
                "{case}: counted after the others"
            );
        }
    }
}

/// The declared counter of `formation`, deleted by hand, and the store as it was before the
/// delete, less that one record.
fn delete_counter(
    rig: &Rig,
    formation: &admin::Formation,
) -> (String, Vec<(Bytes, Bytes, u64, u64)>) {
    let counter = admin::instance_key(&formation.instance);
    rig.rt
        .block_on(rig.store.delete(config_core::DeleteRequest {
            key: Bytes::from(counter.clone()),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("the counter is deleted");
    let after = snapshot(&rig.rt, &*rig.store);
    assert!(!keys(&after).contains(&counter), "{counter} is gone");
    (counter, after)
}

/// PR #138 review r5 (blocker): on an untouched declared store, node 1's open meets the counter
/// moving under it. Its first counter write loses to node 2's open, which takes N = 1 and has
/// rooted nothing yet, and the counter is then deleted before node 1 reads it again. Node 1 is
/// refused, naming the absent counter: it sends no counter put after the delete, the store is
/// what it was less the counter, and no `nodes/` is left. A declared open never creates the
/// counter, since `admin::declare` wrote it. Unit (<1 s, one node process refused before it
/// spawns anything).
/// Cut: `declare` writes no counter and a declared open with partition 1 untouched may create
/// it (as at 168f56e); node 1's create loses, and its retry creates N = 1 again and opens, red.
#[config_log::retcd_test]
fn s1a_counter_deleted_after_lost_create_is_not_recreated() {
    let rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    let formation = rig.declare_created();
    let counter = admin::instance_key(&formation.instance);
    let before: Vec<_> = snapshot(&rig.rt, &*rig.store)
        .into_iter()
        .filter(|(key, ..)| key.as_ref() != counter.as_bytes())
        .collect();
    let race = Arc::new(CounterRace {
        inner: Arc::clone(&rig.store),
        key: Bytes::from(counter.clone()),
        instance: formation.instance,
        conflicts: std::sync::atomic::AtomicU32::new(1),
        vanish: true,
        vanished: std::sync::atomic::AtomicBool::new(false),
        late_puts: std::sync::atomic::AtomicU32::new(0),
    });
    let raced: Arc<dyn ConfigStore> = race.clone();
    let (dir, _keep) = folder();
    let opened = open_node(
        &rig.rt,
        &raced,
        &rig.cert_dir,
        NodeId(1),
        &dir,
        rig.listen(&rig.rt, NodeId(1)),
        &rig.addrs,
    );
    let Err(refused) = opened.map(|_| ()) else {
        panic!("node 1 opened over a counter deleted under it");
    };
    assert!(
        refused
            .to_string()
            .contains(&format!("{counter} is absent")),
        "{refused}"
    );
    assert!(
        race.vanished.load(std::sync::atomic::Ordering::SeqCst),
        "the counter was deleted"
    );
    assert_eq!(
        race.late_puts.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no counter put after the delete"
    );
    assert_eq!(snapshot(&rig.rt, &*rig.store), before, "nothing written");
    assert!(!has_copies(&dir), "no copy was made");
}

/// PR #138 review r5 (restart variant): node 2's first open of an untouched declared store
/// counts N = 1 and stops at its first partition record, so nothing is rooted and no copy holds
/// data. The counter is then deleted by hand. Node 2 restarting on its folder, and nodes 1 and
/// 3 opening theirs for the first time, are each refused, naming the absent counter: nothing
/// is written and no `nodes/` is left. The declaration's counter is the evidence of the
/// database's first open, and no open writes it from absent. Unit (<1 s, an in-memory store).
/// Cut: as `s1a_counter_deleted_after_lost_create_is_not_recreated`; node 2's restart creates
/// N = 1 again and opens, red.
#[config_log::retcd_test]
fn s1a_a_declared_restart_with_its_counter_gone_before_any_root_is_refused() {
    let rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    let formation = rig.declare_created();
    let gated = Arc::new(PartitionGate {
        inner: Arc::clone(&rig.store),
        gate: tokio::sync::Semaphore::new(tokio::sync::Semaphore::MAX_PERMITS / 2),
        arrived: tokio::sync::Notify::new(),
        refuse: std::sync::atomic::AtomicBool::new(true),
    });
    let gated_store: Arc<dyn ConfigStore> = gated;
    let dirs = fresh_dirs();
    let open = |store: &Arc<dyn ConfigStore>, node: NodeId| {
        open_node(
            &rig.rt,
            store,
            &rig.cert_dir,
            node,
            &dirs[node.0 as usize - 1].0,
            rig.listen(&rig.rt, node),
            &rig.addrs,
        )
        .map(|_| ())
    };
    let stopped = open(&gated_store, NodeId(2));
    assert!(
        matches!(&stopped, Err(error) if error.to_string().contains("the opener stopped")),
        "node 2's first open stops at its partition record: {stopped:?}"
    );
    let count = rig
        .rt
        .block_on(admin::read_count(&*rig.store, &formation.instance))
        .expect("the counter reads");
    assert_eq!(count.0.n, 1, "node 2's first open took N = 1");
    let (counter, before) = delete_counter(&rig, &formation);
    for node in [NodeId(2), NodeId(1), NodeId(3)] {
        let refused = open(&rig.store, node).expect_err("an open with no counter is refused");
        assert!(
            refused
                .to_string()
                .contains(&format!("{counter} is absent")),
            "node {}: {refused}",
            node.0
        );
        assert_eq!(
            snapshot(&rig.rt, &*rig.store),
            before,
            "node {}: nothing written",
            node.0
        );
        assert!(
            !has_copies(&dirs[node.0 as usize - 1].0),
            "node {}: no copy was made",
            node.0
        );
    }
}

/// PR #138 review r5 (author question): an open refused because a declared database is still
/// forming names what forms it, its node processes, and never `--form`, `lr up` or `--declare`
/// as the step, since `--form` is refused on a declared store and `--declare` answers that it is
/// already declared. On a store with no formation record the step still names `--form`, and
/// `--declare` only for a declaration stopped before its formation record. Unit (<100 ms, an
/// in-memory store and the refusal texts).
#[config_log::retcd_test]
fn s1a_a_declared_store_still_forming_names_its_node_processes_never_form() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let Ok(admin::Declared::Created(_)) = rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("declared");
    };
    let declared_texts = [
        rt.block_on(formed_member(&store, &[NodeId(2)], true))
            .map(|_| ())
            .expect_err("partitions/1 names no instance yet")
            .to_string(),
        unformed_refusal(PartitionId(2), PARTITIONS, &PartitionState::Absent, true)
            .expect("refused")
            .to_string(),
        unformed_refusal(
            PartitionId(2),
            PARTITIONS,
            &PartitionState::Unrooted { untouched: true },
            true,
        )
        .expect("refused")
        .to_string(),
    ];
    for text in &declared_texts {
        assert!(
            text.contains("its node processes form it") && text.contains("rdb_node --node N"),
            "{text}"
        );
        assert!(
            !text.contains("lr up")
                && !text.contains("--form")
                && !text.contains("--declare")
                && !text.contains("again with the same"),
            "{text}"
        );
    }
    let undeclared = unformed_refusal(PartitionId(2), PARTITIONS, &PartitionState::Absent, false)
        .expect("refused")
        .to_string();
    assert!(
        undeclared.contains("rdb_node --form (what lr up runs) with the same --dir")
            && undeclared.contains(
                "if a declaration of it stopped before its formation record, that rdb_node \
                 --declare again"
            ),
        "{undeclared}"
    );
}

// ---- #80 S1b: more than three nodes, placed on a ring (ADR-rdb-0023) ----

/// Declare nodes 1 to `k` and `partitions`, then open each on a new empty folder of its own,
/// node N listening on 127.0.0.N; none waited on yet.
fn declared_rig_of(k: u32, partitions: u32) -> Rig {
    declared_rig_with(k, partitions, false)
}

/// [`declared_rig_of`], each process on a runtime of its own when `own_runtimes`.
fn declared_rig_with(k: u32, partitions: u32, own_runtimes: bool) -> Rig {
    let mut rig = bare_rig_of(Arc::new(config_testkit::MemStore::new()), k);
    rig.own_runtimes = own_runtimes;
    match rig
        .rt
        .block_on(admin::declare(&rig.store, &rig.declaration_of(partitions)))
    {
        Ok(admin::Declared::Created(_)) => {}
        other => panic!("the declaration of {k} nodes is written: {other:?}"),
    }
    rig.open_all(fresh_dirs_of(k));
    rig
}

/// Where the ring places `partition` over nodes 1 to `k`.
fn ring(k: u32, partition: u32) -> rdb_core::placement::Partition {
    let nodes: Vec<u32> = (1..=k).collect();
    rdb_core::placement::initial(&nodes, partition)
}

/// The partitions `node`'s copy in `dir` holds a lineage of, read from its engine after the
/// process is shut down.
fn held_partitions(dir: &Path, node: NodeId) -> BTreeSet<u32> {
    let engine = RocksEngine::open_with(
        dir.join("nodes").join(node.0.to_string()),
        crate::StorageOptions::default(),
    )
    .unwrap_or_else(|e| panic!("node {}'s copy opens: {e}", node.0));
    engine
        .lineages()
        .into_iter()
        .map(|(partition, _)| partition.0)
        .collect()
}

impl Rig {
    /// Take `node`'s process down, the others running: shut its `Db` down and drop its net, so
    /// its listener and every link to it are gone. [`Rig::restart`] brings it back.
    fn stop(&mut self, node: NodeId) {
        let proc = &mut self.procs[node.0 as usize - 1];
        if let Some(mut db) = proc.db.take() {
            db.shutdown();
        }
        proc._net = None;
        if let Some(rt) = proc.own_rt.take() {
            rt.shutdown_background();
        }
    }

    /// Restart `node`'s process on its own folder and its own address, the others running:
    /// shut it down, let go of its listener, bind the same address again, open again.
    fn restart(&mut self, node: NodeId, timeouts: Timeouts) {
        let at = node.0 as usize - 1;
        let Proc {
            node,
            db,
            _net,
            dir,
            _dir,
            own_rt,
        } = self.procs.remove(at);
        if let Some(mut db) = db {
            db.shutdown();
        }
        // The old net holds the listener until it is dropped and its tasks end; its own
        // runtime, dropped, ends every task it ran, as the process's exit would.
        drop(_net);
        if let Some(rt) = own_rt {
            rt.shutdown_background();
        }
        let own_rt = self.own_runtimes.then(runtime);
        let rt = own_rt.as_ref().unwrap_or(&self.rt);
        let listener = rebind(rt, &self.addrs[&node]);
        let (db, net) = open_node_with(
            rt,
            &self.store,
            &self.cert_dir,
            node,
            &dir,
            listener,
            &self.addrs,
            timeouts,
            self.apps.get(&node).map(String::as_str),
        )
        .unwrap_or_else(|e| panic!("node {} reopens its own folder: {e}", node.0));
        self.procs.insert(
            at,
            Proc {
                node,
                db: Some(db),
                _net: Some(net),
                dir,
                _dir,
                own_rt,
            },
        );
    }
}

/// The node count and partition count the S1b integration rows declare.
const S1B_K: u32 = 5;
const S1B_P: u32 = 16;

/// #80 S1b row 11, acceptance: five nodes declared, each node process on its own empty folder
/// and its own host on this machine (127.0.0.1 to 127.0.0.5), sixteen partitions. Every node
/// becomes ready; each partition's route names the ring's primary, and a put through it reads
/// back; each node's copy holds exactly the partitions the ring places on it and no other, so
/// nodes 4 and 5 hold partitions and no node holds every partition; and each folder holds only
/// its own copy. Integration (~5 s): one formation of 16 partitions over five processes' links.
/// Red before S1b: `declare` refuses five nodes. Cut: the heal's forming members back to
/// `members_for` (nodes 1 to 3): partition 4's plan pins node 1 primary, and its route names
/// node 1, not node 4, red at the route.
#[config_log::retcd_test]
fn s1b_five_declared_nodes_form_sixteen_partitions_each_from_its_own_folder() {
    forms_each_from_its_own_folder(S1B_K);
}

/// #80 S1b, PR #144 review: row 11 at six, seven and eight nodes, the most a declaration takes,
/// each node process on its own folder and its own host (127.0.0.1 to 127.0.0.K). Integration
/// (~1 s each). Cut: `admin::placed` and `admin::owner_in` ring over the first five declared
/// nodes only; partition 6's route names node 1, not node 6, red at 6, 7 and 8 (row 11 green).
#[config_log::retcd_test]
fn s1b_six_declared_nodes_form_sixteen_partitions_each_from_its_own_folder() {
    forms_each_from_its_own_folder(6);
}

/// Row 11 at seven nodes; see the six-node row.
#[config_log::retcd_test]
fn s1b_seven_declared_nodes_form_sixteen_partitions_each_from_its_own_folder() {
    forms_each_from_its_own_folder(7);
}

/// Row 11 at eight nodes, `db::MAX_NODES`; see the six-node row.
#[config_log::retcd_test]
fn s1b_eight_declared_nodes_form_sixteen_partitions_each_from_its_own_folder() {
    forms_each_from_its_own_folder(8);
}

/// Row 11's body over `k` declared nodes and [`S1B_P`] partitions.
fn forms_each_from_its_own_folder(k: u32) {
    let mut rig = declared_rig_of(k, S1B_P);
    rig.wait_ready();
    for partition in 1..=S1B_P {
        let owner = NodeId(ring(k, partition).primary);
        let db = rig.of(owner);
        let group = group_of(db, partition);
        let route = db.route(group).expect("a route");
        assert_eq!(
            (route.partition, route.owner),
            (PartitionId(partition), owner),
            "{route:?}"
        );
        let key = format!("k{partition}");
        let put = put_lands(db, group, key.as_bytes(), b"v");
        assert_eq!(put.partition, PartitionId(partition), "{put:?}");
        let got = db.get_in(group, key.as_bytes()).expect("a get");
        assert_eq!(
            got.value.as_ref().map(|(_, value)| &value[..]),
            Some(&b"v"[..]),
            "{got:?}"
        );
    }
    rig.shutdown_all();
    for proc in &rig.procs {
        assert_eq!(
            copies_in(&proc.dir),
            [proc.node.0.to_string()],
            "node {}'s folder holds only its own copy",
            proc.node.0
        );
        let placed: BTreeSet<u32> = (1..=S1B_P)
            .filter(|p| ring(k, *p).copies.contains(&proc.node.0))
            .collect();
        assert!(!placed.is_empty(), "node {} is placed", proc.node.0);
        assert!(placed.len() < S1B_P as usize, "node {}", proc.node.0);
        assert_eq!(
            held_partitions(&proc.dir, proc.node),
            placed,
            "node {} holds exactly what the ring places on it",
            proc.node.0
        );
    }
}

/// #80 S1b row 12: each of five node processes restarted on its own folder, the others
/// running, reopens its copy and serves what was written before; nodes 4 and 5 hold no copy of
/// partition 1, so their reopen decides partition 1 from the record alone. New puts land after.
/// A rolling restart: each node is ready again before the next stops, since a recovery that
/// loses a copy mid-barrier stops for that open by design. Integration (~10 s): a formation,
/// then five restarts one by one.
/// Cut: `Db::check`'s owner of a still-forming partition 1 is not the ring's: it holds as at
/// S1a for partition 1 (node 1 at every K), so this row is red only through the cuts it shares
/// with row 11; what it adds is the reopen of a node holding no partition 1.
#[config_log::retcd_test]
fn s1b_restart_of_each_of_five_nodes_reopens_its_own_folder() {
    let mut rig = declared_rig_with(S1B_K, S1B_P, true);
    rig.wait_ready();
    let groups: Vec<(u32, AffinityId)> = (1..=S1B_P)
        .map(|partition| (partition, group_of(rig.of(NodeId(1)), partition)))
        .collect();
    for (partition, group) in &groups {
        let owner = rig.of(NodeId(ring(S1B_K, *partition).primary));
        put_lands(owner, *group, b"before", b"1");
    }
    for node in (1..=S1B_K).map(NodeId) {
        rig.restart(node, Timeouts::default());
        rig.wait_ready();
    }
    for (partition, group) in &groups {
        let owner = rig.of(NodeId(ring(S1B_K, *partition).primary));
        let got = owner.get_in(*group, b"before").expect("a get");
        assert_eq!(
            got.value.as_ref().map(|(_, value)| &value[..]),
            Some(&b"1"[..]),
            "partition {partition}"
        );
        put_lands(owner, *group, b"after", b"2");
    }
    rig.shutdown_all();
    for node in [NodeId(4), NodeId(5)] {
        let held = held_partitions(&rig.procs[node.0 as usize - 1].dir, node);
        assert!(!held.contains(&1), "node {} holds {held:?}", node.0);
    }
    for proc in &rig.procs {
        assert_eq!(copies_in(&proc.dir), [proc.node.0.to_string()]);
    }
}

/// The nodes `partitions/{partition}` pins now, from its layout-4 members; empty before any.
fn pinned_nodes(rig: &Rig, partition: u32) -> BTreeSet<u32> {
    let read = rig
        .rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Partition(PartitionId(partition)).encode()),
        }))
        .expect("the store answers");
    read.record
        .and_then(|found| RecordMember::decode_all(&found.value))
        .map(|members| members.iter().map(|member| member.node.0).collect())
        .unwrap_or_default()
}

/// Poll `partitions/{partition}`'s pinned nodes until `done` holds of them, and answer every
/// set seen on the way, the last one included. Panics after `within`.
fn pins_until(
    rig: &Rig,
    partition: u32,
    within: Duration,
    what: &str,
    done: impl Fn(&BTreeSet<u32>) -> bool,
) -> Vec<BTreeSet<u32>> {
    let deadline = Instant::now() + crate::host::test_patience(within);
    let mut seen = Vec::new();
    loop {
        let now = pinned_nodes(rig, partition);
        if seen.last() != Some(&now) {
            seen.push(now.clone());
        }
        if done(&now) {
            return seen;
        }
        assert!(Instant::now() < deadline, "{what}: pins seen {seen:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// #80 S1b row 13: partition 2 is placed on nodes 2, 3 and 4. Node 4 goes down and its owner,
/// node 2, drops it after the unreachable clock. Node 2 then restarts, which empties what it
/// remembered was left out, while node 4 is still down. Node 4 comes back (owner 2 relearns it
/// at that boot, the seat the ring placed it on) and restarts once more, and owner 2 pins it
/// again. Node 1, which the ring never placed partition 2 on, is never pinned. Each process
/// runs on its own runtime, so a stop ends its links as a process exit does. Integration
/// (~15 s): a formation, a 2 s drop clock, three restarts.
/// Cut: `Host::restore_left_out`'s seats back to `admin::members()` (nodes 1 to 3): owner 2
/// relearns node 1, whose copy slot 0 is free, and never node 4: red at the wait for
/// `left_out_restored` of node 4.
#[config_log::retcd_test]
fn s1b_a_dropped_copy_outside_nodes_one_to_three_is_restored_after_an_owner_restart() {
    const PARTITION_2: u32 = 2;
    let fast_drop = Timeouts {
        unreachable_drop_after: Duration::from_secs(2),
        ..Timeouts::default()
    };
    assert_eq!(ring(S1B_K, PARTITION_2).copies, [2, 3, 4]);
    let mut rig = bare_rig_of(Arc::new(config_testkit::MemStore::new()), S1B_K);
    rig.timeouts = fast_drop;
    rig.own_runtimes = true;
    match rig
        .rt
        .block_on(admin::declare(&rig.store, &rig.declaration_of(S1B_P)))
    {
        Ok(admin::Declared::Created(_)) => {}
        other => panic!("declared: {other:?}"),
    }
    rig.open_all(fresh_dirs_of(S1B_K));
    rig.wait_ready();
    let group = group_of(rig.of(NodeId(2)), PARTITION_2);
    put_lands(rig.of(NodeId(2)), group, b"before", b"1");
    let mut seen = pins_until(
        &rig,
        PARTITION_2,
        Duration::from_secs(10),
        "formed",
        |pins| *pins == BTreeSet::from([2, 3, 4]),
    );

    rig.stop(NodeId(4));
    seen.extend(pins_until(
        &rig,
        PARTITION_2,
        Duration::from_secs(30),
        "owner 2 drops node 4",
        |pins| *pins == BTreeSet::from([2, 3]),
    ));
    // Owner 2's other partitions are on nodes 2, 3 and 4 too: its heal after the restart waits
    // for every member it pins, so each must have dropped node 4 first.
    for other in (1..=S1B_P).filter(|p| *p != PARTITION_2 && ring(S1B_K, *p).primary == 2) {
        assert_eq!(ring(S1B_K, other).copies, [2, 3, 4], "partition {other}");
        pins_until(
            &rig,
            other,
            Duration::from_secs(30),
            "owner 2 drops node 4",
            |pins| *pins == BTreeSet::from([2, 3]),
        );
    }
    rig.restart(NodeId(2), fast_drop);
    rig.of(NodeId(2))
        .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("owner 2 is ready again on nodes 2 and 3");
    rig.restart(NodeId(4), fast_drop);
    let relearned = Instant::now() + crate::host::test_patience(Duration::from_secs(20));
    while !config_testkit::logs::lines_for_current_test(
        module_path!(),
        "s1b_a_dropped_copy_outside_nodes_one_to_three_is_restored_after_an_owner_restart",
    )
    .iter()
    .any(|line| line["@m"] == "left_out_restored" && line["left_out"] == 4)
    {
        assert!(Instant::now() < relearned, "owner 2 never relearned node 4");
        std::thread::sleep(Duration::from_millis(50));
    }
    rig.restart(NodeId(4), fast_drop);
    seen.extend(pins_until(
        &rig,
        PARTITION_2,
        Duration::from_secs(30),
        "owner 2 pins node 4 again",
        |pins| pins.contains(&4),
    ));
    assert!(
        seen.iter().all(|pins| !pins.contains(&1)),
        "node 1 is never pinned in partition 2: {seen:?}"
    );
    rig.wait_ready();
    let got = rig.of(NodeId(2)).get_in(group, b"before").expect("a get");
    assert_eq!(
        got.value.as_ref().map(|(_, value)| &value[..]),
        Some(&b"1"[..]),
        "{got:?}"
    );
}

/// `declaration()` with node `node`'s address and machine label replaced.
fn with_node(node: u32, addr: &str, machine: Option<&str>) -> admin::Declaration {
    let mut declared = declaration();
    let decl = &mut declared.nodes[node as usize - 1];
    decl.addr = addr.to_owned();
    decl.machine = machine.map(str::to_owned);
    declared
}

/// #80 S1b row 5 (ruling 1): two nodes on one machine are refused, whatever their labels say,
/// and nothing is written: the same label, a label differing only in case, one host on two
/// ports, and distinct labels on one host. Distinct loopback addresses are distinct hosts, so
/// the rig's 127.0.0.1 to .3 declare. Unit (<100 ms, an in-memory store).
/// Cut: `Declaration::records` skips `machine_clash`; each case declares, red at the first.
#[config_log::retcd_test]
fn s1b_declare_refuses_two_nodes_on_one_machine_and_writes_nothing() {
    let rt = runtime();
    let labelled = |labels: [&str; 3]| {
        let mut declared = declaration();
        for (decl, label) in declared.nodes.iter_mut().zip(labels) {
            decl.machine = Some(label.to_owned());
        }
        declared
    };
    for (what, declared, names) in [
        ("the same label", labelled(["a", "b", "b"]), "machine \"b\""),
        (
            "a case variant",
            labelled(["a", "HostB", "hostb"]),
            "machine \"hostb\"",
        ),
        (
            "one host on two ports",
            with_node(2, "127.0.0.1:7101", None),
            "the same host",
        ),
        (
            "distinct labels on one host",
            {
                let mut declared = with_node(2, "127.0.0.1:7101", Some("b"));
                declared.nodes[0].machine = Some("a".to_owned());
                declared
            },
            "the same host",
        ),
    ] {
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let refused = rt.block_on(admin::declare(&store, &declared));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains("one data node per machine") && text.contains(names)
                    && text.contains("nothing was written")),
            "{what}: {refused:?}"
        );
        assert!(snapshot(&rt, &*store).is_empty(), "{what}: nothing written");
    }
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    assert!(
        matches!(
            rt.block_on(admin::declare(&store, &declaration())),
            Ok(admin::Declared::Created(_))
        ),
        "127.0.0.1, .2 and .3 are three machines"
    );
}

/// #80 S1b, PR #144 review: two spellings of one IP are one host, so their nodes are refused
/// as on one machine, with default labels and with distinct explicit ones, and nothing is
/// written: `[::1]` and `[0:0:0:0:0:0:0:1]`, and `127.0.0.1` and `[::ffff:127.0.0.1]`. A host
/// name differing only in case or a trailing dot is one host too. Unit (<100 ms).
/// Cut: `machine_clash` compares the address text, lowercase; every IP case declares, red at
/// the first.
#[config_log::retcd_test]
fn s1b_declare_refuses_two_spellings_of_one_ip_as_one_machine() {
    let rt = runtime();
    let pair = |a: &str, b: &str, labels: Option<[&str; 2]>| {
        let mut declared = with_node(1, a, labels.map(|l| l[0]));
        declared.nodes[1].addr = b.to_owned();
        declared.nodes[1].machine = labels.map(|l| l[1].to_owned());
        declared
    };
    for labels in [None, Some(["a", "b"])] {
        for (a, b) in [
            ("[::1]:7100", "[0:0:0:0:0:0:0:1]:7100"),
            ("[::1]:7100", "[0:0:0:0:0:0:0:1]:7101"),
            ("127.0.0.1:7100", "[::ffff:127.0.0.1]:7101"),
            ("DB-A.example:7100", "db-a.example.:7101"),
        ] {
            let what = format!("{a} and {b}, labels {labels:?}");
            let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
            let refused = rt.block_on(admin::declare(&store, &pair(a, b, labels)));
            assert!(
                matches!(&refused, Err(admin::DeclareError::Refused(text))
                    if text.contains("the same host") && text.contains("nodes 1 (")
                        && text.contains("nothing was written")),
                "{what}: {refused:?}"
            );
            assert!(snapshot(&rt, &*store).is_empty(), "{what}: nothing written");
        }
    }
    // #80 S2: `[::2]` is not a loopback IP, so the declaration needs a TLS control store.
    let store: Arc<dyn ConfigStore> = TlsReported::new(Arc::new(config_testkit::MemStore::new()));
    let declared = rt.block_on(admin::declare(
        &store,
        &pair("[::1]:7100", "[::2]:7100", None),
    ));
    assert!(
        matches!(&declared, Ok(admin::Declared::Created(_))),
        "::1 and ::2 are two hosts: {declared:?}"
    );
}

/// #80 S1b, PR #144 review: a declared address no peer can dial is refused before anything is
/// written, since a node record is never rewritten: port 0, a bracketed host that is not an IPv6
/// address (`[::gg]`), and (PR #144 review r2) an unspecified, broadcast or multicast host
/// (`0.0.0.0`, `[::]`, `[::ffff:0.0.0.0]`, `255.255.255.255`, `224.0.0.1`, `[ff02::1]`). The same
/// store then takes the corrected addresses. Unit (<100 ms). Cut: `parse_addr` accepts port 0 and
/// parses no bracketed host; both declare, red at the first. Cut: `dialable` refuses nothing; red
/// at `255.255.255.255`. Cut: `dialable` skips `is_unspecified`; red at `0.0.0.0`.
#[config_log::retcd_test]
fn s1b_declare_refuses_an_undialable_address_then_takes_the_corrected_one() {
    let rt = runtime();
    for (bad, names, corrected) in [
        (
            "127.0.0.3:0",
            "has port 0, which no peer can dial",
            "127.0.0.3:7100",
        ),
        (
            "[::gg]:7100",
            "a bracketed host must be an IPv6 address",
            "[::3]:7100",
        ),
        (
            "255.255.255.255:7100",
            "broadcast or multicast address",
            "127.0.0.3:7100",
        ),
        (
            "224.0.0.1:7100",
            "broadcast or multicast address",
            "127.0.0.3:7100",
        ),
        (
            "[ff02::1]:7100",
            "broadcast or multicast address",
            "[::3]:7100",
        ),
        ("0.0.0.0:7100", "an unspecified", "127.0.0.3:7100"),
        ("[::]:7100", "an unspecified", "[::3]:7100"),
        ("[::ffff:0.0.0.0]:7100", "an unspecified", "127.0.0.3:7100"),
    ] {
        // #80 S2: a TLS control store, since a corrected `[::3]` is not a loopback IP. Every
        // refusal here comes before the control-TLS check.
        let store: Arc<dyn ConfigStore> =
            TlsReported::new(Arc::new(config_testkit::MemStore::new()));
        let refused = rt.block_on(admin::declare(&store, &with_node(3, bad, None)));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains("node 3") && text.contains(bad) && text.contains(names)
                    && text.contains("nothing was written")),
            "{bad}: {refused:?}"
        );
        assert!(snapshot(&rt, &*store).is_empty(), "{bad}: nothing written");
        let declared = rt.block_on(admin::declare(&store, &with_node(3, corrected, None)));
        assert!(
            matches!(&declared, Ok(admin::Declared::Created(formation))
                if formation.nodes.len() == 3),
            "{corrected} on the store {bad} left: {declared:?}"
        );
    }
}

/// #80 S1b, PR #144 review r3: a host that is not an IP address must be a whitelisted host name,
/// and a machine label is `[a-z0-9._:-]`, 1 to 63 bytes, so nothing a peer cannot resolve and
/// no control byte is stored forever. Each case is declared with an explicit machine label, so
/// the label cannot be what refuses a bad host: a host holding a space, tab, CR, LF or NUL,
/// hosts of only dots, and the hex and short numeric spellings of an IPv4 address. Then an
/// explicit label holding a space, tab, CR, LF, NUL or DEL. Each is refused naming node 3, with
/// nothing written; the same store then takes `db-c.example` with label `m3`. Unit (<100 ms).
/// Cut: `name_refusal` answers `None`; `db c.example` declares, red at the first.
#[config_log::retcd_test]
fn s1b_declare_refuses_a_host_or_label_outside_the_whitelist() {
    let rt = runtime();
    // #80 S2: a TLS control store, since the accepted `db-c.example` is a host name. Every
    // refusal here comes before the control-TLS check.
    let store: Arc<dyn ConfigStore> = TlsReported::new(Arc::new(config_testkit::MemStore::new()));
    let hosts = [
        "db c.example:7100",
        "db\tc.example:7100",
        "db\rc.example:7100",
        "db\nc.example:7100",
        "db\0c.example:7100",
        "..:7100",
        "...:7100",
        "0xffffffff:7100",
        "0xe0000001:7100",
        "0x7f.1:7100",
        "127.1:7100",
    ];
    for bad in hosts {
        let refused = rt.block_on(admin::declare(&store, &with_node(3, bad, Some("m3"))));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains("node 3") && text.contains(&format!("{bad:?}"))
                    && text.contains("host name") && text.contains("nothing was written")),
            "{bad:?}: {refused:?}"
        );
        assert!(
            snapshot(&rt, &*store).is_empty(),
            "{bad:?}: nothing written"
        );
    }
    for label in ["m 3", "m\t3", "m\r3", "m\n3", "m\0x", "m\u{7f}3"] {
        let refused = rt.block_on(admin::declare(
            &store,
            &with_node(3, "db-c.example:7100", Some(label)),
        ));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains("node 3") && text.contains("is not one of a-z")
                    && text.contains("nothing was written")),
            "{label:?}: {refused:?}"
        );
        assert!(
            snapshot(&rt, &*store).is_empty(),
            "{label:?}: nothing written"
        );
    }
    let declared = rt.block_on(admin::declare(
        &store,
        &with_node(3, "db-c.example:7100", Some("m3")),
    ));
    assert!(
        matches!(&declared, Ok(admin::Declared::Created(formation)) if formation.nodes.len() == 3),
        "{declared:?}"
    );
}

/// #80 S1b row 6: the node sets and addresses a declaration refuses, each with nothing
/// written: 2 nodes, 9 nodes, a gap (1, 2, 4), a node named twice, one address for two nodes,
/// and an address with no port. Four nodes, refused at S1a, now declare. This replaces the S1a
/// `rdb_node` row that declared four nodes to reach a refusal: K = 4 is legal. Unit (<100 ms).
/// Cut: `Declaration::records` skips its duplicate-address check; that case is then refused by
/// `machine_clash` as one host instead, red at the text naming the address twice.
#[config_log::retcd_test]
fn s1b_declare_refuses_node_sets_outside_three_to_eight_or_with_gaps() {
    let rt = runtime();
    let mut gap = declaration_of(4, PARTITIONS);
    gap.nodes.remove(2);
    let mut twice = declaration_of(3, PARTITIONS);
    twice.nodes[2].node = NodeId(2);
    for (what, declared, names) in [
        ("two nodes", declaration_of(2, PARTITIONS), "3 to 8 nodes"),
        ("nine nodes", declaration_of(9, PARTITIONS), "3 to 8 nodes"),
        ("a gap", gap, "nodes [1, 2, 4]"),
        ("a node twice", twice, "nodes [1, 2, 2]"),
        (
            "one address twice",
            with_node(3, "127.0.0.2:7100", None),
            "nodes 2 and 3 both have address 127.0.0.2:7100",
        ),
        (
            "no port",
            with_node(3, "127.0.0.3", None),
            "address \"127.0.0.3\" is not host:port",
        ),
    ] {
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let refused = rt.block_on(admin::declare(&store, &declared));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains(names) && text.contains("nothing was written")),
            "{what}: {refused:?}"
        );
        assert!(snapshot(&rt, &*store).is_empty(), "{what}: nothing written");
    }
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let four = rt.block_on(admin::declare(&store, &declaration_of(4, PARTITIONS)));
    assert!(
        matches!(&four, Ok(admin::Declared::Created(formation)) if formation.nodes.len() == 4),
        "{four:?}"
    );
}

/// #80 S1b row 7: a declaration writes every `nodes/{id}` record after `routes/0` and before
/// any partition record, so the formation record, published last, is published after them
/// too. Held at its first partition record, the store holds the reservation, `routes/0` and
/// the three node records, each the bytes its `NodeRecord` encodes; held at the formation
/// record, all of them and every partition record. Unit (<200 ms, an in-memory store).
/// Cut: write the node records after the counter, before the formation record; the first hold
/// finds none, red.
#[config_log::retcd_test]
fn s1b_declare_writes_every_node_record_before_the_formation_record() {
    let rt = runtime();
    let records = declaration().records().expect("the rig's declaration");
    for (held_at, expected) in [
        (
            ControlKey::Partition(PartitionId(1)).encode(),
            vec![
                ControlKey::Operation(admin::RESERVATION_OPERATION).encode(),
                ControlKey::Route(RangeId(0)).encode(),
            ],
        ),
        (
            ControlKey::Operation(admin::FORMATION_OPERATION).encode(),
            Vec::new(),
        ),
    ] {
        let mem: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let held = PutHold::new(Arc::clone(&mem), &held_at, HoldAt::BeforeApply);
        let held_store: Arc<dyn ConfigStore> = held.clone();
        let declaring = rt.spawn(async move { admin::declare(&held_store, &declaration()).await });
        rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), held.arrived.notified())
                .await
                .unwrap_or_else(|_| panic!("the put of {held_at} arrives"));
        });
        let at = snapshot(&rt, &*mem);
        for record in &records {
            let key = ControlKey::Node(record.node).encode();
            let found = at.iter().find(|(k, ..)| k.as_ref() == key.as_bytes());
            assert_eq!(
                found.map(|(_, value, ..)| value.clone()),
                Some(record.encode()),
                "held at {held_at}: {key}"
            );
        }
        if expected.is_empty() {
            let counter: Vec<String> = keys(&at)
                .into_iter()
                .filter(|key| key.starts_with("rdb/instances/"))
                .collect();
            assert_eq!(keys(&at), declared_keys(PARTITIONS, &[&counter[0]], false));
        } else {
            let mut expected = expected;
            expected.extend(node_keys(3));
            expected.sort();
            assert_eq!(keys(&at), expected, "held at {held_at}");
        }
        declaring.abort();
    }
}

/// #80 S1b row 8: two declarations that differ only in node 3's address run at once and both
/// pass the preflight before either writes. In both winner orders the loser is refused at the
/// reservation, naming both node lists, and writes nothing (a full snapshot is the same before
/// and after it); the winner publishes, and `nodes/3` holds the winner's address. Unit
/// (<200 ms, an in-memory store).
/// Cut: the reservation without its node records (a count of 0); the loser adopts it, then
/// fails at `nodes/3` with the winner's bytes there, red at the loser's outcome.
#[config_log::retcd_test]
fn s1b_concurrent_declarations_differing_only_in_an_address_publish_one() {
    let rt = runtime();
    let near = declaration();
    let far = with_node(3, "127.0.0.33:7100", None);
    for (won, lost) in [(&near, &far), (&far, &near)] {
        let mem: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let both = Arc::new(tokio::sync::Barrier::new(2));
        let winner = Arc::new(DeclareRace::new(Arc::clone(&mem), Arc::clone(&both), true));
        let loser = Arc::new(DeclareRace::new(Arc::clone(&mem), both, false));
        let declare = |store: Arc<DeclareRace>, declared: admin::Declaration| {
            let store: Arc<dyn ConfigStore> = store;
            rt.spawn(async move { admin::declare(&store, &declared).await })
        };
        let won_addr = won.nodes[2].addr.clone();
        let winning = declare(Arc::clone(&winner), won.clone());
        let losing = declare(Arc::clone(&loser), lost.clone());
        let within = Duration::from_secs(10);
        rt.block_on(async {
            tokio::time::timeout(within, winner.at_marker.notified())
                .await
                .unwrap_or_else(|_| panic!("{won_addr} wins: its formation put arrives"));
        });
        let before = snapshot(&rt, &*mem);
        loser.go.add_permits(1);
        let outcome = rt
            .block_on(async { tokio::time::timeout(within, losing).await })
            .unwrap_or_else(|_| panic!("{won_addr} wins: the loser ends"))
            .expect("the task ends");
        assert!(
            matches!(&outcome, Err(admin::DeclareError::Refused(text))
                if text.contains("reserves the store for a declaration of 8 partitions")
                    && text.contains(&format!("3@{won_addr}/"))
                    && text.contains(&format!("3@{}/", lost.nodes[2].addr))
                    && text.contains("nothing was written")),
            "{won_addr} wins: the loser is refused at the reservation: {outcome:?}"
        );
        assert_eq!(
            snapshot(&rt, &*mem),
            before,
            "{won_addr} wins: the loser wrote nothing"
        );
        winner.marker.add_permits(1);
        let outcome = rt
            .block_on(async { tokio::time::timeout(within, winning).await })
            .unwrap_or_else(|_| panic!("{won_addr} wins: it ends"))
            .expect("the task ends");
        assert!(
            matches!(&outcome, Ok(admin::Declared::Created(_))),
            "{won_addr} wins: {outcome:?}"
        );
        let nodes = rt
            .block_on(admin::read_nodes(&*mem))
            .expect("the node records read");
        assert_eq!(nodes[&NodeId(3)].addr, won_addr);
    }
}

/// #80 S1b row 9: a declaration stopped after `nodes/2` is finished only by itself. Another
/// declaration whose node 2 differs is refused by the preflight, naming `nodes/2` and both
/// values; one whose node 3 differs (no record yet) is refused at the reservation. Neither
/// writes. The same declaration run again adopts the reservation, `routes/0` and both node
/// records, unchanged at their revisions, and publishes. Unit (<200 ms, an in-memory store).
/// Cut: the preflight skips `nodes/`; the node-2 variant passes it and is refused at the
/// reservation instead, red at the text.
#[config_log::retcd_test]
fn s1b_an_interrupted_declaration_is_finished_by_its_own_node_records_only() {
    let rt = runtime();
    let scripted = Arc::new(Scripted::new(Script {
        pass_puts: 2 + 2,
        fail_put_with: Some(config_core::ConfigError::Unavailable {
            reason: "scripted: the declaring process stopped".to_owned(),
        }),
        ..Script::default()
    }));
    let store: Arc<dyn ConfigStore> = scripted.clone();
    let stopped = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(stopped, Err(admin::DeclareError::Records(_))),
        "{stopped:?}"
    );
    let stopped_at = snapshot(&rt, &*store);
    let mut expected = vec![
        ControlKey::Operation(admin::RESERVATION_OPERATION).encode(),
        ControlKey::Route(RangeId(0)).encode(),
        ControlKey::Node(NodeId(1)).encode(),
        ControlKey::Node(NodeId(2)).encode(),
    ];
    expected.sort();
    assert_eq!(keys(&stopped_at), expected);
    scripted.rescript(Script::default());

    for (what, other, names) in [
        (
            "node 2 elsewhere",
            with_node(2, "127.0.0.22:7100", None),
            "nodes/2 records 2@127.0.0.2:7100/127.0.0.2 and this declaration says \
             2@127.0.0.22:7100/127.0.0.22",
        ),
        (
            "node 3 elsewhere",
            with_node(3, "127.0.0.33:7100", None),
            "reserves the store for a declaration of 8 partitions and nodes [1@127.0.0.1:7100",
        ),
    ] {
        let refused = rt.block_on(admin::declare(&store, &other));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains(names) && text.contains("nothing was written")),
            "{what}: {refused:?}"
        );
        assert_eq!(
            snapshot(&rt, &*store),
            stopped_at,
            "{what}: nothing written"
        );
    }

    let done = rt.block_on(admin::declare(&store, &declaration()));
    let Ok(admin::Declared::Created(formation)) = done else {
        panic!("the same declaration finishes: {done:?}");
    };
    let finished = snapshot(&rt, &*store);
    assert!(
        kept(&stopped_at, &finished),
        "{stopped_at:?} -> {finished:?}"
    );
    let counter = admin::instance_key(&formation.instance);
    assert_eq!(
        keys(&finished),
        declared_keys(PARTITIONS, &[&counter], true)
    );
}

/// #80 S1b row 14 (ADR-rdb-0023 Decision 4): a declared open of two local nodes is refused: one
/// process is one machine, so it would run two data nodes on one. It is refused before
/// anything is created or read past the records: the store is unchanged and no `nodes/` folder
/// is left. Unit (<1 s, an in-memory store).
/// Cut: drop the `local.len() != 1` arm of `declared_open_refusal`; the open goes on and forms
/// copies 1 and 2, red at `expect_err`.
#[config_log::retcd_test]
fn s1b_a_declared_open_of_two_local_nodes_is_refused() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let Ok(admin::Declared::Created(_)) = rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("declared");
    };
    let declared = snapshot(&rt, &*store);
    let dir = config_testkit::fs::temp_dir();
    let refused = rt
        .block_on(Db::open_with(
            DbConfig {
                local: vec![NodeId(1), NodeId(2)],
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .map(|_| ())
        .expect_err("two declared nodes do not open in one process");
    let text = refused.to_string();
    assert!(
        matches!(refused, OpenError::Config(_))
            && text.contains("runs nodes [1, 2] of a declared database")
            && text.contains("one data node per process")
            && text.contains("nothing but its folder was created"),
        "{text}"
    );
    assert!(!dir.path().join("nodes").exists(), "no nodes/ folder");
    assert_eq!(snapshot(&rt, &*store), declared, "nothing was written");
}

/// Node 1 of `declaration()` opened alone on `dir` with `addresses` (#80 S1b ruling 2): no
/// net, since the address check comes before anything an open links or creates.
fn open_declared_one(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    dir: &Path,
    addresses: Option<NodeAddresses>,
) -> Result<Db, OpenError> {
    rt.block_on(Db::open_with(
        DbConfig {
            local: vec![NodeId(1)],
            addresses,
            ..config(dir)
        },
        Arc::clone(store),
        rt.handle().clone(),
        fast(),
    ))
}

/// `declaration()`'s addresses as node 1 was given them: its own and every node's.
fn declared_addresses() -> NodeAddresses {
    NodeAddresses {
        listen: "127.0.0.1:7100".to_owned(),
        peers: (1..=3)
            .map(|n| (NodeId(n), format!("127.0.0.{n}:7100")))
            .collect(),
        app: None,
    }
}

/// The names in a folder's `nodes/`, or none when it has no `nodes/`.
fn folder_listing(dir: &Path) -> Vec<String> {
    if dir.join("nodes").exists() {
        copies_in(dir)
    } else {
        Vec::new()
    }
}

/// #80 S1b ruling 2: node 1 opened with `wrong` addresses is refused, naming every one of
/// `names`, on its first open, a retry of it, and a restart after an open with the right
/// addresses. Each refusal leaves the store and the folder as they were.
fn refused_on_every_open(wrong: Option<NodeAddresses>, names: &[&str]) {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let Ok(admin::Declared::Created(_)) = rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("declared");
    };
    let dir = config_testkit::fs::temp_dir();
    let refused_here = |what: &str| {
        let before = snapshot(&rt, &*store);
        let listing = folder_listing(dir.path());
        let refused = open_declared_one(&rt, &store, dir.path(), wrong.clone())
            .map(|mut db| db.shutdown())
            .expect_err("a declared node whose addresses disagree is refused");
        let text = refused.to_string();
        assert!(matches!(refused, OpenError::Config(_)), "{what}: {text}");
        for name in names {
            assert!(text.contains(name), "{what}: {name:?} in {text}");
        }
        assert_eq!(snapshot(&rt, &*store), before, "{what}: nothing written");
        assert_eq!(
            folder_listing(dir.path()),
            listing,
            "{what}: the folder is as it was"
        );
    };
    refused_here("the first open");
    refused_here("a retry");
    let mut db = open_declared_one(&rt, &store, dir.path(), Some(declared_addresses()))
        .expect("node 1 opens with its recorded addresses");
    db.shutdown();
    drop(db);
    assert_eq!(
        folder_listing(dir.path()),
        ["1"],
        "the right open made node 1's copy"
    );
    refused_here("a restart");
}

/// #80 S1b ruling 2 (ADR-rdb-0023 Decision 5): an embedded node of a declared database whose
/// `DbConfig::addresses.listen` is not its `nodes/1` record's address is refused, naming the
/// record and both addresses, on the first open, a retry and a restart; nothing is written and
/// the folder is as it was. Unit (<1 s, an in-memory store, no net).
/// Cut: `declared_open_refusal` skips its `node_start_refusal` call; the first open opens, red
/// at `expect_err`.
#[config_log::retcd_test]
fn s1b_a_declared_open_whose_listen_address_disagrees_with_its_record_is_refused() {
    refused_on_every_open(
        Some(NodeAddresses {
            listen: "127.0.0.1:7101".to_owned(),
            ..declared_addresses()
        }),
        &[
            "listen address 127.0.0.1:7101",
            "nodes/1",
            "records 127.0.0.1:7100",
        ],
    );
}

/// #80 S1b ruling 2 (ADR-rdb-0023 Decision 5): an embedded node given an address for one peer
/// that its `nodes/` record does not hold is refused naming that entry and the record, on the
/// first open, a retry and a restart; nothing is written. Unit (<1 s).
/// Cut: as the listen row; red at `expect_err`.
#[config_log::retcd_test]
fn s1b_a_declared_open_given_a_peer_address_that_disagrees_is_refused() {
    let mut wrong = declared_addresses();
    wrong.peers.insert(NodeId(3), "127.0.0.33:7100".to_owned());
    refused_on_every_open(
        Some(wrong),
        &[
            "peer address 3=127.0.0.33:7100",
            "nodes/3",
            "records 127.0.0.3:7100",
        ],
    );
}

/// #80 S1b ruling 2: a declared open with no `DbConfig::addresses` is refused, so the check
/// cannot be skipped by leaving them out; first open, retry and restart. Unit (<1 s).
/// Cut: `declared_open_refusal` lets `None` through; the first open opens, red at `expect_err`.
#[config_log::retcd_test]
fn s1b_a_declared_open_with_no_addresses_is_refused() {
    refused_on_every_open(
        None,
        &[
            "no DbConfig::addresses",
            "nothing but its folder was created",
        ],
    );
}

/// #80 S1b, PR #144 review r2: the preflight's reservation read failing is a refusal naming
/// that read, not a reservation taken as absent: the store's error stays typed
/// (`DeclareError::Store`, step `get operations/<reservation>`) and nothing is written. The same
/// declaration then declares once the store answers. Unit (<100 ms, an in-memory store).
/// Cut: `declare_preflight` takes a failed reservation read as no reservation; the declaration
/// goes on and creates, red at the first `matches!`.
#[config_log::retcd_test]
fn s1b_a_failing_reservation_read_in_the_preflight_is_refused() {
    let rt = runtime();
    // The formation record's read passes; the next get, the preflight's reservation read, fails.
    let scripted = Arc::new(Scripted::new(Script {
        pass_gets: 1,
        fail_gets: 1,
        ..Script::default()
    }));
    let store: Arc<dyn ConfigStore> = scripted.clone();
    let reservation = ControlKey::Operation(admin::RESERVATION_OPERATION).encode();
    let failed = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(&failed, Err(admin::DeclareError::Store { step, error })
            if *step == format!("get {reservation}")
                && error.to_string().contains("scripted: the read failed")),
        "{failed:?}"
    );
    assert!(snapshot(&rt, &*store).is_empty(), "nothing written");
    scripted.rescript(Script::default());
    let declared = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(&declared, Ok(admin::Declared::Created(_))),
        "{declared:?}"
    );
}

/// #80 S1b ruling 4, PR #144 review: a store an S1a build began declaring and did not finish
/// holds its layout-1 reservation (partitions and node numbers, no addresses), `routes/0` and
/// partition records, and no formation or `nodes/` record. Declaring it again is refused with
/// what to do, whatever the new declaration says: five nodes, whose partition 4 differs from
/// the S1a record, and the same three nodes, whose records match it byte for byte so only the
/// reservation can refuse it. Nothing is written. Unit (<100 ms).
/// Cut: `declare_preflight` skips its reservation read; five nodes are refused at
/// `partitions/4` without what to do, red at the text.
#[config_log::retcd_test]
fn s1b_a_half_written_s1a_declaration_is_refused_with_how_to_redeclare() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let Ok(admin::Declared::Created(formation)) =
        rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("declared");
    };
    // What an S1a build left when it stopped before its formation record.
    let mut gone: Vec<String> = (1..=3)
        .map(|n| ControlKey::Node(NodeId(n)).encode())
        .collect();
    gone.push(ControlKey::Operation(admin::FORMATION_OPERATION).encode());
    gone.push(admin::instance_key(&formation.instance));
    for key in gone {
        rt.block_on(store.delete(config_core::DeleteRequest {
            key: Bytes::from(key),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("delete");
    }
    let mut layout_1 = vec![1];
    layout_1.extend_from_slice(&PARTITIONS.to_le_bytes());
    for n in 1..=3u32 {
        layout_1.extend_from_slice(&n.to_le_bytes());
    }
    rt.block_on(store.put(config_core::PutRequest {
        key: Bytes::from(ControlKey::Operation(admin::RESERVATION_OPERATION).encode()),
        value: Bytes::from(layout_1),
        expected_mod_revision: None,
        dedup: None,
    }))
    .expect("the S1a reservation");
    let s1a = snapshot(&rt, &*store);
    for (what, declared) in [
        ("five nodes", declaration_of(5, PARTITIONS)),
        ("the same three nodes", declaration()),
    ] {
        let refused = rt.block_on(admin::declare(&store, &declared));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains("by a build before #80 S1b")
                    && text.contains("declare the database again, with --peers, on a new empty \
                                      control store")
                    && text.contains("nothing was written")),
            "{what}: {refused:?}"
        );
        assert_eq!(snapshot(&rt, &*store), s1a, "{what}: nothing was written");
    }
}

/// #80 S1b ruling 4 (ADR-rdb-0023): a store an S1a build declared holds a formation record and
/// no `nodes/` record. A node process opening it is refused with what to do (declare again on
/// a new store), before any folder is claimed; `admin::declare` of it again is refused the same
/// way rather than answering `Already`. Nothing is written by either. Unit (<1 s).
/// Cut: `declared_open_refusal` skips its empty-records check; the open is refused by the next
/// check instead (`nodes/2 is absent`), without what to do, red at the text.
#[config_log::retcd_test]
fn s1b_a_store_declared_by_an_s1a_build_is_refused_with_how_to_redeclare() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let Ok(admin::Declared::Created(_)) = rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("declared");
    };
    // What an S1a build left: every record but `nodes/`.
    for n in 1..=3 {
        rt.block_on(store.delete(config_core::DeleteRequest {
            key: Bytes::from(ControlKey::Node(NodeId(n)).encode()),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("delete");
    }
    let s1a = snapshot(&rt, &*store);
    let dir = config_testkit::fs::temp_dir();
    let refused = rt
        .block_on(Db::open_with(
            DbConfig {
                local: vec![NodeId(2)],
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .map(|_| ())
        .expect_err("an S1a store does not open");
    let text = refused.to_string();
    assert!(
        text.contains(admin::S1A_STORE) && text.contains("nothing but its folder was created"),
        "{text}"
    );
    assert!(!dir.path().join("nodes").exists(), "no nodes/ folder");
    let again = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(&again, Err(admin::DeclareError::Refused(text)) if text.contains(admin::S1A_STORE)),
        "{again:?}"
    );
    assert_eq!(snapshot(&rt, &*store), s1a, "nothing was written");
}

/// Two groups that both map to `partition`: two workspaces that move together.
fn two_groups_of(db: &Db, partition: u32) -> (AffinityId, AffinityId) {
    let mut groups = (1..1_000_000)
        .map(AffinityId)
        .filter(|group| db.partition_of(*group) == PartitionId(partition));
    let first = groups.next().expect("a group");
    (first, groups.next().expect("a second group"))
}

/// `route(group)` on `db` until it names `owner`, bounded: the gateway's node reads the new
/// record once its `partitions/` watch delivers the change.
fn route_names(db: &Db, group: AffinityId, owner: NodeId) -> crate::RouteInfo {
    let until = Instant::now() + crate::host::test_patience(Duration::from_secs(10));
    loop {
        let route = db.route(group).expect("a route");
        if route.owner == owner {
            return route;
        }
        assert!(Instant::now() < until, "route still names {route:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The app address node N registers in the B2 rows: SBX's gateway port beside it.
fn app_of(node: NodeId) -> String {
    format!("https://gw-{}.example:8443", node.0)
}

/// [`declared_rig_of`] with every node registering [`app_of`] at open (#83 B2).
fn declared_rig_with_apps(k: u32, partitions: u32) -> Rig {
    let mut rig = bare_rig_of(Arc::new(config_testkit::MemStore::new()), k);
    match rig
        .rt
        .block_on(admin::declare(&rig.store, &rig.declaration_of(partitions)))
    {
        Ok(admin::Declared::Created(_)) => {}
        other => panic!("the declaration of {k} nodes is written: {other:?}"),
    }
    rig.apps = (1..=k).map(|n| (NodeId(n), app_of(NodeId(n)))).collect();
    rig.open_all(fresh_dirs_of(k));
    rig
}

/// own83_b2_1 (#83 B2 acceptance; SBX's scenarios "forwarding to the owner" and "a partition's
/// workspaces move together"; #84's epoch): five declared node processes, each on its own host,
/// each registering its app address at open. The gateway in node 2's process routes two
/// workspaces of partition 4, which the ring places on node 4: both name node 4, node 4's app
/// address and one token, and `leader_of(4)` says the same. In node 4's process,
/// `ownership(4)` holds that token, the epoch SBX stamps on its side effects. Node 4 stops and
/// partition 4 is promoted to node 5 from node 2's process: both workspaces now route to node 5
/// at node 5's app address, under a higher token, which node 5's `ownership` holds; node 5 does
/// not own the old token; a put through the gateway lands in partition 4 and the write made
/// before the move reads back. Cuts: the open's `register_app_addr` call removed, red at the
/// first route (no address); the address read for a fixed node (node 1), red at the first
/// route (node 1's address). Integration (~10 s): five processes' formation and a promote that
/// waits out node 4's grant.
#[config_log::retcd_test]
fn own83_b2_1_route_forwards_to_the_owners_app_address_across_a_promote() {
    const P: u32 = 4;
    let mut rig = declared_rig_with_apps(S1B_K, S1B_P);
    rig.wait_ready();
    let owner = NodeId(ring(S1B_K, P).primary);
    assert_eq!(owner, NodeId(4), "the ring places partition 4 on node 4");
    let gateway = rig.of(NodeId(2));
    let (a, b) = two_groups_of(gateway, P);
    let before = gateway.route(a).expect("a route");
    assert_eq!(
        (before.partition, before.owner, before.app_address()),
        (PartitionId(P), owner, Some(app_of(owner).as_str())),
        "{before:?}"
    );
    assert_eq!(
        gateway.route(b).expect("a route"),
        before,
        "workspaces move together"
    );
    assert_eq!(
        gateway.leader_of(PartitionId(P)).expect("leader_of"),
        before
    );
    let held = rig
        .of(owner)
        .ownership(PartitionId(P))
        .expect("node 4 leads partition 4");
    assert_eq!(
        held.token, before.token,
        "the epoch the owner stamps is route's"
    );
    put_lands(gateway, a, b"before", b"1");

    rig.stop(owner);
    let to = NodeId(5);
    rig.of(NodeId(2))
        .promote_partition(PartitionId(P), to, None)
        .unwrap_or_else(|e| panic!("node 4 is stopped: {e}"));
    let gateway = rig.of(NodeId(2));
    let after = route_names(gateway, a, to);
    assert_eq!(
        (after.partition, after.app_address()),
        (PartitionId(P), Some(app_of(to).as_str())),
        "{after:?}"
    );
    assert!(after.token > before.token, "{after:?} after {before:?}");
    assert_eq!(
        route_names(gateway, b, to),
        after,
        "workspaces move together"
    );
    let until = Instant::now() + crate::host::test_patience(Duration::from_secs(20));
    let now_held = loop {
        if let Some(held) = rig.of(to).ownership(PartitionId(P)) {
            break held;
        }
        assert!(Instant::now() < until, "node 5 never led partition 4");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        now_held.token, after.token,
        "the new owner stamps route's token"
    );
    assert!(!rig.of(to).still_owner(PartitionId(P), before.token));
    let put = put_lands(gateway, b, b"after", b"2");
    assert_eq!(put.partition, PartitionId(P), "{put:?}");
    let got = gateway.get_in(a, b"before").expect("a get");
    assert_eq!(
        got.value.as_ref().map(|(_, value)| &value[..]),
        Some(&b"1"[..]),
        "the write made before the move reads back: {got:?}"
    );
}

/// The `nodes/{id}` record's body and its mod revision.
fn node_record(rig: &Rig, node: NodeId) -> (Bytes, u64) {
    let read = rig
        .rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Node(node).encode()),
        }))
        .expect("a read");
    let record = read.record.expect("the record");
    (record.value, record.mod_revision)
}

/// own83_b2_2 (#83 B2, owner ruling 2026-11-19): a declared open writes its app address into
/// its own `nodes/{id}` record (layout 2), changing no declared field: `read_nodes` reads the
/// records the declaration wrote, and declaring the same again is `Already`. A restart with the
/// same address writes nothing (the mod revision stays); a restart with another replaces it;
/// a restart with none keeps the stored one. Cut: the "already holds it" check removed, red at
/// "same address". Integration (~3 s).
#[config_log::retcd_test]
fn own83_b2_2_a_declared_open_registers_its_app_address_once_per_change() {
    let mut rig = declared_rig_with_apps(3, PARTITIONS);
    rig.wait_ready();
    let node = NodeId(2);
    let (body, first) = node_record(&rig, node);
    assert_eq!(
        NodeRecord::decode_with_app(&body).and_then(|(_, app)| app),
        Some(app_of(node)),
        "layout 2 holds the app address"
    );
    let declared = rig.declaration();
    let read = rig
        .rt
        .block_on(admin::read_nodes(&*rig.store))
        .expect("records");
    for decl in &declared.nodes {
        assert_eq!(
            read[&decl.node].addr, decl.addr,
            "the declared fields are unchanged"
        );
    }
    assert!(
        matches!(
            rig.rt.block_on(admin::declare(&rig.store, &declared)),
            Ok(admin::Declared::Already(_))
        ),
        "the same declaration is adopted"
    );

    rig.restart(node, Timeouts::default());
    assert_eq!(
        node_record(&rig, node).1,
        first,
        "same address: nothing written"
    );

    rig.apps.insert(node, "gw-2.other:9000".to_owned());
    rig.restart(node, Timeouts::default());
    let (body, moved) = node_record(&rig, node);
    assert!(moved > first, "another address is written");
    assert_eq!(
        NodeRecord::decode_with_app(&body)
            .and_then(|(_, app)| app)
            .as_deref(),
        Some("gw-2.other:9000")
    );

    rig.apps.remove(&node);
    rig.restart(node, Timeouts::default());
    let (body, kept) = node_record(&rig, node);
    assert_eq!(kept, moved, "no address: nothing written");
    assert_eq!(
        NodeRecord::decode_with_app(&body)
            .and_then(|(_, app)| app)
            .as_deref(),
        Some("gw-2.other:9000"),
        "kept"
    );
}

/// own83_b2_3 (#83 B2): an app address is refused where no record can hold it, or out of
/// bounds, before anything is written: an empty one and one of 513 bytes on a declared open,
/// and any on an undeclared one. Each leaves the store as it was. Cut: the undeclared refusal
/// removed, red: the open goes on and is refused for another reason (no `partitions/1`).
/// Unit (<1 s, an in-memory store, no net).
#[config_log::retcd_test]
fn own83_b2_3_an_app_address_is_refused_where_no_record_can_hold_it() {
    let rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    let empty = snapshot(&rig.rt, &*rig.store);
    let dir = folder();
    let undeclared = rig.rt.block_on(Db::open_with(
        DbConfig {
            local: vec![NodeId(1)],
            addresses: Some(NodeAddresses {
                app: Some("gw-1:8443".to_owned()),
                ..declared_addresses()
            }),
            ..config(&dir.0)
        },
        Arc::clone(&rig.store),
        rig.rt.handle().clone(),
        fast(),
    ));
    let refused = undeclared.expect_err("undeclared");
    assert!(
        refused.to_string().contains("only a declared database"),
        "{refused}"
    );
    assert_eq!(snapshot(&rig.rt, &*rig.store), empty, "nothing was written");

    rig.declare_created();
    let declared = snapshot(&rig.rt, &*rig.store);
    for app in [
        String::new(),
        "a".repeat(rdb_core::authority::node::MAX_APP_ADDR_LEN + 1),
    ] {
        let dir = folder();
        let opened = rig.rt.block_on(Db::open_with(
            DbConfig {
                local: vec![NodeId(1)],
                addresses: Some(NodeAddresses {
                    app: Some(app.clone()),
                    ..declared_addresses()
                }),
                ..config(&dir.0)
            },
            Arc::clone(&rig.store),
            rig.rt.handle().clone(),
            fast(),
        ));
        let refused = opened.expect_err("out of bounds");
        assert!(
            refused.to_string().contains("app address"),
            "{} bytes: {refused}",
            app.len()
        );
        assert_eq!(
            snapshot(&rig.rt, &*rig.store),
            declared,
            "nothing was written"
        );
    }
}

// ---- #83 B2, PR #159 review: the app-address write's fence, and its unknown outcomes ----

/// What an [`AddrPuts`] store does to the next `nodes/` put.
#[derive(Debug, Clone)]
enum NodePut {
    /// Through to the store.
    Pass,
    /// Kept to apply later ([`AddrPuts::deliver`]), answered `DeadlineExceededUnknownOutcome`
    /// at once: a write sent whose reply was lost and which has not applied yet.
    Late,
    /// [`NodePut::Late`], and the next `nodes/` read fails: the read back fails.
    LateUnread,
    /// Kept to apply later, never answered: the caller's future is dropped there, as a
    /// process stopped mid-write, after its marker and before the write applied.
    Park,
    /// Applied, answered `DeadlineExceededUnknownOutcome`.
    AppliedLost,
    /// [`NodePut::AppliedLost`], and the next `nodes/` read fails: applied, and the read back
    /// fails.
    AppliedUnread,
    /// These bytes written over the record first, then the put sent: it meets a record that
    /// moved after the open read it.
    Rival(Bytes),
    /// These bytes written over the record first, the put not applied, answered
    /// `DeadlineExceededUnknownOutcome`.
    RivalLost(Bytes),
}

/// A memory store whose `nodes/` puts follow a script (#83 B2): each scripted put records
/// whether the folder's pending marker was on disk when it arrived.
struct AddrPuts {
    inner: config_testkit::MemStore,
    script: std::sync::Mutex<std::collections::VecDeque<NodePut>>,
    kept: std::sync::Mutex<Vec<config_core::PutRequest>>,
    failing_reads: std::sync::atomic::AtomicU32,
    /// The marker a scripted put looks for, and what each one found.
    marker: std::sync::Mutex<Option<PathBuf>>,
    marked: std::sync::Mutex<Vec<bool>>,
    /// The revision each rival write made.
    rivals: std::sync::Mutex<Vec<u64>>,
    parked: tokio::sync::Notify,
}

impl AddrPuts {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: config_testkit::MemStore::new(),
            script: std::sync::Mutex::new(std::collections::VecDeque::new()),
            kept: std::sync::Mutex::new(Vec::new()),
            failing_reads: std::sync::atomic::AtomicU32::new(0),
            marker: std::sync::Mutex::new(None),
            marked: std::sync::Mutex::new(Vec::new()),
            rivals: std::sync::Mutex::new(Vec::new()),
            parked: tokio::sync::Notify::new(),
        })
    }

    fn next(&self, step: NodePut) {
        self.script.lock().expect("script").push_back(step);
    }

    /// Apply every kept put now, as its late arrival: each one's outcome.
    fn deliver(&self, rt: &tokio::runtime::Runtime) -> Vec<config_core::MutationOutcome> {
        let kept: Vec<_> = self.kept.lock().expect("kept").drain(..).collect();
        kept.into_iter()
            .map(|put| {
                rt.block_on(self.inner.put(put))
                    .expect("applied or refused")
                    .outcome
            })
            .collect()
    }

    fn keep(&self, request: config_core::PutRequest) {
        self.kept.lock().expect("kept").push(request);
    }

    async fn overwrite(&self, key: &Bytes, value: Bytes) -> Result<(), config_core::ConfigError> {
        self.inner
            .put(config_core::PutRequest {
                key: key.clone(),
                value,
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .map(|written| self.rivals.lock().expect("rivals").push(written.revision))
    }
}

#[async_trait::async_trait]
impl ConfigStore for AddrPuts {
    async fn get(
        &self,
        request: config_core::GetRequest,
    ) -> Result<config_core::GetResponse, config_core::ConfigError> {
        if request.key.starts_with(b"nodes/")
            && self
                .failing_reads
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(config_core::ConfigError::Unavailable {
                reason: "AddrPuts: this read back is scripted to fail".to_owned(),
            });
        }
        self.inner.get(request).await
    }

    async fn list(
        &self,
        request: config_core::ListRequest,
    ) -> Result<config_core::ListResponse, config_core::ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, config_core::ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(
        &self,
        request: config_core::PutRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        use config_core::ConfigError::DeadlineExceededUnknownOutcome as Unknown;
        if !request.key.starts_with(b"nodes/") {
            return self.inner.put(request).await;
        }
        let step = self
            .script
            .lock()
            .expect("script")
            .pop_front()
            .unwrap_or(NodePut::Pass);
        if let Some(marker) = self.marker.lock().expect("marker").as_ref() {
            self.marked.lock().expect("marked").push(marker.exists());
        }
        tracing::info!(?step, "test_addr_put");
        match step {
            NodePut::Pass => self.inner.put(request).await,
            NodePut::Late => {
                self.keep(request);
                Err(Unknown)
            }
            NodePut::LateUnread => {
                self.keep(request);
                self.failing_reads.store(1, Ordering::SeqCst);
                Err(Unknown)
            }
            NodePut::Park => {
                self.keep(request);
                self.parked.notify_one();
                std::future::pending().await
            }
            NodePut::AppliedLost => {
                self.inner.put(request).await?;
                Err(Unknown)
            }
            NodePut::AppliedUnread => {
                self.inner.put(request).await?;
                self.failing_reads.store(1, Ordering::SeqCst);
                Err(Unknown)
            }
            NodePut::Rival(value) => {
                self.overwrite(&request.key, value).await?;
                self.inner.put(request).await
            }
            NodePut::RivalLost(value) => {
                self.overwrite(&request.key, value).await?;
                Err(Unknown)
            }
        }
    }

    async fn delete(
        &self,
        request: config_core::DeleteRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> config_core::Capabilities {
        self.inner.capabilities()
    }

    async fn watch(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// A declared three-node store behind an [`AddrPuts`], and node 1's folder, where node 1 has
/// opened once with `first` and stopped: its record holds `first`. Only node 1 ever runs.
///
/// Each open counts the calls to its `open_spawning` spawn, one per host started (PR #159
/// review r2): a refused open must have made none, and an open that succeeds at least one, the
/// control that the count sees a start at all. [`Fence::open_on`] asserts both, so every
/// refusal a row meets is checked to be before any host starts.
struct Fence {
    rig: Rig,
    store: Arc<AddrPuts>,
    dir: Folder,
}

impl Fence {
    fn new(first: &str) -> Self {
        let store = AddrPuts::new();
        let rig = bare_rig_of(Arc::clone(&store) as Arc<dyn ConfigStore>, 3);
        rig.declare_created();
        let dir = folder();
        *store.marker.lock().expect("marker") = Some(dir.0.join(APP_ADDR_PENDING_FILE));
        let fence = Self { rig, store, dir };
        fence.open(first).expect("the first open registers");
        fence
    }

    /// Open node 1 on its folder with `app`; stopped again at once when it opens.
    fn open(&self, app: &str) -> Result<(), OpenError> {
        self.open_on(&self.rig.rt, Some(app))
    }

    /// [`Fence::open`] on `rt` with `app`, or with no app address: a fresh runtime is a new
    /// process. Opened through `open_spawning`, whose spawn is counted: refused, no host
    /// started; opened, at least one did.
    fn open_on(&self, rt: &tokio::runtime::Runtime, app: Option<&str>) -> Result<(), OpenError> {
        let listener = self.rig.listen(rt, NodeId(1));
        let _entered = rt.enter();
        let (config, net) = node_config(
            &self.rig.cert_dir,
            NodeId(1),
            &self.dir.0,
            listener,
            &self.rig.addrs,
            Timeouts::default(),
            app,
        );
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&starts);
        let spawn = move |node, engine, links, control, clock, boot, partitions, nodes| {
            counted.fetch_add(1, Ordering::SeqCst);
            host::spawn_on(
                node,
                engine,
                links,
                control,
                clock,
                fast(),
                Limits::default(),
                host::UNREACHABLE_DROP_AFTER_MILLIS,
                false,
                boot,
                partitions,
                nodes,
            )
        };
        let opened = rt.block_on(Db::open_spawning(
            config,
            Arc::clone(&self.rig.store),
            rt.handle().clone(),
            fast(),
            spawn,
        ));
        let starts = starts.load(Ordering::SeqCst);
        match opened {
            Ok(mut db) => {
                assert!(
                    starts > 0,
                    "the control: an open that succeeds starts its host"
                );
                db.shutdown();
                drop((db, net));
                Ok(())
            }
            Err(refused) => {
                assert_eq!(starts, 0, "refused before any host starts: {refused}");
                Err(refused)
            }
        }
    }

    /// Node 1's record: its app address and mod revision.
    fn record(&self) -> (Option<String>, u64) {
        let (body, revision) = node_record(&self.rig, NodeId(1));
        let (_, app) = NodeRecord::decode_with_app(&body).expect("node 1's record decodes");
        (app, revision)
    }

    /// The record's bytes as node 1 writes them with `app`.
    fn bytes_with(&self, app: &str) -> Bytes {
        let (body, _) = node_record(&self.rig, NodeId(1));
        NodeRecord::decode(&body)
            .expect("node 1's record decodes")
            .encode_with_app(app)
    }

    fn marked(&self) -> bool {
        self.dir.0.join(APP_ADDR_PENDING_FILE).exists()
    }

    /// How many `nodes/` puts reached the store so far.
    fn node_puts(&self) -> usize {
        self.store.marked.lock().expect("marked").len()
    }

    /// Whether every scripted put found the marker on disk when it arrived.
    fn marker_before_every_put(&self) -> bool {
        let marked = self.store.marked.lock().expect("marked");
        !marked.is_empty() && marked.iter().all(|found| *found)
    }
}

/// own83_b2_4 (#83 B2, PR #159 review, the fence): node 1's record holds A. An open with B sends
/// its write at revision r, and the reply is lost before the write applies: refused, no host
/// started (its spawn counted zero, [`Fence`]), the marker left. The retry with A finds the record already holding A, but the
/// marker says a write may still apply at r, so it writes A again (the fence): the revision
/// moves past r. The late write of B then arrives and is refused, and the record holds A. A
/// further open with A, no marker now, writes nothing. Every scripted put found the marker on
/// disk. The address carries a synthetic credential: no log line of the changed, fenced or
/// unchanged registration holds it. Cut: the fence skipped (`held == app` returns before the
/// marker is read), red at "B never replaces A" (the late write applies). Cut:
/// `db_app_addr_registered` logs `app = %app`, red at the log check. Integration (~2 s): one
/// node, opened four times.
#[config_log::retcd_test]
fn own83_b2_4_a_late_write_cannot_replace_a_successful_retry() {
    const A: &str = "https://svc:pw-SYNTHETIC-51ce@gw-1.example:8443/sbx";
    let fence = Fence::new(A);
    let (held, r) = fence.record();
    assert_eq!(held.as_deref(), Some(A));

    fence.store.next(NodePut::Late);
    let refused = fence
        .open("gw-1.b:9443")
        .expect_err("the outcome is unknown");
    assert!(
        refused.to_string().contains("has not applied yet")
            && refused.to_string().contains("nothing was started"),
        "{refused}"
    );
    assert!(fence.marked(), "the marker stays while the write may apply");
    assert_eq!(fence.record(), (Some(A.to_owned()), r), "B has not applied");

    fence.open(A).expect("the retry with A opens");
    let (_, fenced) = fence.record();
    let late = fence.store.deliver(&fence.rig.rt);
    assert_eq!(fence.record().0.as_deref(), Some(A), "B never replaces A");
    assert!(!fence.marked(), "the fence resolved the marker");
    assert_eq!(
        late,
        [config_core::MutationOutcome::Conflict],
        "the late write at r is refused"
    );
    assert!(fenced > r, "the retry wrote A again, past r");
    assert_eq!(fence.record(), (Some(A.to_owned()), fenced));

    fence.open(A).expect("an unchanged open");
    assert_eq!(fence.record().1, fenced, "no marker: nothing written");
    assert!(fence.marker_before_every_put());

    let lines = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "own83_b2_4_a_late_write_cannot_replace_a_successful_retry",
    );
    let registered: Vec<_> = lines
        .iter()
        .filter(|line| line["@m"] == "db_app_addr_registered")
        .collect();
    assert_eq!(
        registered.len(),
        3,
        "first, fenced, unchanged: {registered:?}"
    );
    assert!(registered.iter().all(|line| line["app_len"] == A.len()));
    for line in &lines {
        assert!(!line.to_string().contains("pw-SYNTHETIC-51ce"), "{line}");
    }
}

/// own83_b2_5 (#83 B2, PR #159 review, lead's crash row): the open with B is dropped after its
/// marker is durable and before its write applies, as a crash there leaves it. The process
/// starts again (a new runtime) and opens with A, which the record already holds: the marker
/// makes it write A again. Then both the parked write at the old revision and a stale retry
/// of B at that revision are refused, and the record holds A. Cut: the fence skipped, red at
/// "B never replaces A". Cut: the marker written after the put is sent, red at "the marker
/// was on disk". Integration (~2 s).
#[config_log::retcd_test]
fn own83_b2_5_an_open_dropped_mid_write_is_fenced_by_the_next() {
    const A: &str = "gw-1.a:8443";
    let fence = Fence::new(A);
    let (_, r) = fence.record();
    fence.store.next(NodePut::Park);
    let rt = &fence.rig.rt;
    let listener = fence.rig.listen(rt, NodeId(1));
    {
        let _entered = rt.enter();
        let (config, net) = node_config(
            &fence.rig.cert_dir,
            NodeId(1),
            &fence.dir.0,
            listener,
            &fence.rig.addrs,
            Timeouts::default(),
            Some("gw-1.b:9443"),
        );
        let store = Arc::clone(&fence.rig.store);
        let dropped = rt.block_on(async {
            tokio::select! {
                opened = Db::open_with(config, store, rt.handle().clone(), fast()) => {
                    Err(format!("the open finished: {:?}", opened.map(|_| ())))
                }
                () = fence.store.parked.notified() => Ok(()),
            }
        });
        dropped.expect("the open is dropped at its write");
        drop(net);
    }
    assert!(fence.marked(), "the marker outlives the dropped open");
    assert!(
        fence.marker_before_every_put(),
        "the marker was on disk before the write went out"
    );
    assert_eq!(fence.record(), (Some(A.to_owned()), r));

    let restarted = runtime();
    fence
        .open_on(&restarted, Some(A))
        .expect("the restarted open");
    let (_, fenced) = fence.record();

    let stale = config_core::PutRequest {
        key: Bytes::from(ControlKey::Node(NodeId(1)).encode()),
        value: fence.bytes_with("gw-1.b:9443"),
        expected_mod_revision: Some(r),
        dedup: None,
    };
    fence.store.keep(stale);
    let late = fence.store.deliver(rt);
    assert_eq!(fence.record().0.as_deref(), Some(A), "B never replaces A");
    assert_eq!(
        late,
        [
            config_core::MutationOutcome::Conflict,
            config_core::MutationOutcome::Conflict
        ],
        "the parked write and a stale retry, both at r"
    );
    assert!(fenced > r, "written again past r");
    assert!(!fence.marked(), "the fence resolved the marker");
    assert_eq!(fence.record(), (Some(A.to_owned()), fenced));
}

/// own83_b2_6 (#83 B2, PR #159 review, the unknown-outcome table): a registration write
/// answered `DeadlineExceededUnknownOutcome` is read back. Applied, and the read back matches:
/// the open goes on, the marker removed. Not applied, and a rival's bytes are there instead:
/// refused, naming the rival's revision, the rival's record kept, the marker removed (no
/// write at the old revision can apply). The read back fails, twice, on two opens: each is
/// refused naming both the lost reply and the failed read, the marker kept, the record as it
/// was. Every refusal starts no host (its spawn counted zero, [`Fence`]). Then the store is healthy: the next open writes (the fence) and goes on. Cut: the read
/// back's error dropped (`.ok()`, as before the review), red at "read back failed". Cut: the
/// marker removed on a failed read back, red at "kept". Integration (~3 s): one node, six
/// opens.
#[config_log::retcd_test]
fn own83_b2_6_an_unknown_registration_outcome_is_read_back() {
    let fence = Fence::new("gw-1.a:8443");

    fence.store.next(NodePut::AppliedLost);
    let (_, r) = fence.record();
    fence
        .open("gw-1.b:9443")
        .expect("applied: the open goes on");
    let (held, applied) = fence.record();
    assert_eq!(held.as_deref(), Some("gw-1.b:9443"));
    assert!(applied > r);
    assert!(!fence.marked(), "applied: resolved");

    let rival = fence.bytes_with("gw-1.rival:1");
    fence.store.next(NodePut::RivalLost(rival));
    let refused = fence.open("gw-1.c:9443").expect_err("a rival wrote");
    let (held, rivals) = fence.record();
    assert!(
        refused.to_string().contains("other bytes")
            && refused.to_string().contains(&format!("revision {rivals}")),
        "{refused}"
    );
    assert_eq!(held.as_deref(), Some("gw-1.rival:1"), "the rival's record");
    assert!(!fence.marked(), "the record moved: resolved");

    for attempt in 1..=2 {
        fence.store.next(NodePut::LateUnread);
        let refused = fence.open("gw-1.d:9443").expect_err("unknown");
        let text = refused.to_string();
        assert!(
            text.contains("outcome is unknown")
                && text.contains("read back failed")
                && text.contains("scripted to fail"),
            "{attempt}: {text}"
        );
        assert!(fence.marked(), "{attempt}: kept");
        assert_eq!(
            fence.record(),
            (Some("gw-1.rival:1".to_owned()), rivals),
            "{attempt}: as it was"
        );
    }
    fence.open("gw-1.d:9443").expect("a healthy store");
    let (held, recovered) = fence.record();
    assert_eq!(held.as_deref(), Some("gw-1.d:9443"));
    assert!(recovered > rivals);
    assert!(!fence.marked());
    // The two late writes were sent at the rival's revision, which has moved on.
    assert_eq!(
        fence.store.deliver(&fence.rig.rt),
        [
            config_core::MutationOutcome::Conflict,
            config_core::MutationOutcome::Conflict
        ]
    );
    assert_eq!(fence.record().0.as_deref(), Some("gw-1.d:9443"));
}

/// own83_b2_11 (#83 B2, PR #159 review r2, T5): a registration write that applied, whose reply
/// was lost and whose read back failed. Twice: the open with B is refused naming both, no host
/// started, the marker kept, and the record holds B at a new revision (the second attempt finds
/// B already there, and the marker makes it write again). Then the store is healthy and the
/// open with B, an equal-address retry, writes once more (the fence): the open goes on, the
/// marker is resolved and the revision moves on. A further open writes nothing. Cut: the
/// marker removed on a failed read back, red at "kept". Integration (~3 s): one node, five
/// opens.
#[config_log::retcd_test]
fn own83_b2_11_an_applied_write_whose_read_back_fails_is_settled_by_a_retry() {
    const B: &str = "gw-1.b:9443";
    let fence = Fence::new("gw-1.a:8443");
    let (_, mut last) = fence.record();
    for attempt in 1..=2 {
        fence.store.next(NodePut::AppliedUnread);
        let refused = fence.open(B).expect_err("the outcome is unknown");
        let text = refused.to_string();
        assert!(
            text.contains("outcome is unknown")
                && text.contains("read back failed")
                && text.contains("scripted to fail"),
            "{attempt}: {text}"
        );
        assert!(fence.marked(), "{attempt}: kept");
        let (held, revision) = fence.record();
        assert_eq!(held.as_deref(), Some(B), "{attempt}: the write applied");
        assert!(revision > last, "{attempt}: at a new revision");
        last = revision;
    }
    fence.open(B).expect("a healthy store: the retry opens");
    let (held, settled) = fence.record();
    assert_eq!(held.as_deref(), Some(B));
    assert!(settled > last, "the retry fenced: the revision moved on");
    assert!(!fence.marked(), "and the marker is resolved");
    fence.open(B).expect("an unchanged open");
    assert_eq!(fence.record().1, settled, "no marker: nothing written");
}

/// own83_b2_7 (#83 B2, PR #159 review, the revision race): the record changes after the open
/// reads it and before its write. A rival address, and a rival write of exactly the bytes this
/// open wants at a newer revision: each refuses the open, no host started (its spawn counted
/// zero, [`Fence`]), the rival's bytes and revision kept, the marker removed. Cut: the put sent without `expected_mod_revision`,
/// red at "the rival's record" (the open overwrites it). Cut: a conflict whose stored bytes
/// equal this open's taken as success, red at the identical rival. Integration (~2 s).
#[config_log::retcd_test]
fn own83_b2_7_a_record_changed_mid_registration_refuses_the_open() {
    let fence = Fence::new("gw-1.a:8443");
    for (what, rival) in [
        ("a rival address", fence.bytes_with("gw-1.rival:1")),
        ("identical bytes", fence.bytes_with("gw-1.b:9443")),
    ] {
        fence.store.next(NodePut::Rival(rival.clone()));
        let refused = fence.open("gw-1.b:9443").expect_err(what);
        let text = refused.to_string();
        assert!(
            text.contains("changed after this open read it")
                && text.contains("nothing was started"),
            "{what}: {text}"
        );
        let (body, revision) = node_record(&fence.rig, NodeId(1));
        assert_eq!(body, rival, "{what}: the rival's record");
        let rival_at = fence.store.rivals.lock().expect("rivals").pop();
        assert_eq!(Some(revision), rival_at, "{what}: the rival's revision");
        assert!(!fence.marked(), "{what}: resolved");
    }
}

/// Node 1's record holds A, and an open with B sent its write at revision r, whose reply was
/// lost before it applied: refused, the marker left, the write of B still to arrive (#83 B2,
/// PR #159 review r2). Returns the fence and r.
fn fence_with_late_b(a: &str) -> (Fence, u64) {
    let fence = Fence::new(a);
    let (_, r) = fence.record();
    fence.store.next(NodePut::Late);
    let refused = fence
        .open("gw-1.b:9443")
        .expect_err("the outcome is unknown");
    assert!(
        refused.to_string().contains("has not applied yet"),
        "{refused}"
    );
    assert!(fence.marked(), "the marker stays while B may apply");
    assert_eq!(fence.record(), (Some(a.to_owned()), r), "B has not applied");
    (fence, r)
}

/// The late write of B arrives: refused at r, and the record still holds A.
fn late_b_is_refused(fence: &Fence, a: &str, r: u64) {
    let (_, fenced) = fence.record();
    assert!(fenced > r, "the recovery wrote A again, past r");
    assert_eq!(
        fence.store.deliver(&fence.rig.rt),
        [config_core::MutationOutcome::Conflict],
        "the late write at r is refused"
    );
    assert_eq!(
        fence.record(),
        (Some(a.to_owned()), fenced),
        "B never replaces A"
    );
    assert!(!fence.marked(), "the fence resolved the marker");
}

/// own83_b2_8 (#83 B2, PR #159 review r2, T1): a restart given no app address, while B's write
/// may still apply, is refused before any host starts, naming the step: open again with the
/// address the node should hold. It writes nothing and keeps the marker, since an open without
/// an address cannot fence. The open with A then settles it: it writes A again, and the late B
/// is refused. A later open with no address opens and writes nothing. Cut: `app_addr_settled`
/// skipped (the `None` arm does nothing), red at "a no-address open while B may apply" (it
/// opens). Integration (~2 s).
#[config_log::retcd_test]
fn own83_b2_8_a_no_address_restart_is_refused_while_a_write_may_apply() {
    const A: &str = "gw-1.a:8443";
    let (fence, r) = fence_with_late_b(A);
    let rt = &fence.rig.rt;
    for attempt in 1..=2 {
        let refused = fence
            .open_on(rt, None)
            .expect_err("a no-address open while B may apply");
        let text = refused.to_string();
        assert!(
            text.contains("was given no app address")
                && text.contains("Open this node again with the app address it should hold")
                && text.contains("Leave app-addr-pending and app-addr-pending.new in place")
                && text.contains("Nothing was started"),
            "{attempt}: {text}"
        );
        assert!(fence.marked(), "{attempt}: the marker is kept");
        assert_eq!(
            fence.record(),
            (Some(A.to_owned()), r),
            "{attempt}: nothing written"
        );
    }
    fence.open(A).expect("the explicit-address open settles it");
    late_b_is_refused(&fence, A, r);
    let (_, settled) = fence.record();
    fence
        .open_on(rt, None)
        .expect("settled: a no-address open opens");
    assert_eq!(
        fence.record(),
        (Some(A.to_owned()), settled),
        "and writes nothing"
    );
}

/// own83_b2_9 (#83 B2, PR #159 review r2, T28): B's write may still apply, and node 1's folder
/// is then emptied, its marker with it, as an operator empties a node's folder for it to
/// rejoin. The open with A on the empty folder finds the record holding A and no marker, but
/// the folder had no data, so it writes A again (the fence), and the late B is refused. A
/// restart on that folder, now holding data, writes nothing. Cut: `new_folder` ignored
/// (`fenced` from the marker only), red at "the recovery wrote A again" (no write, and the late
/// B then applies). Integration (~2 s).
#[config_log::retcd_test]
fn own83_b2_9_an_emptied_folder_fences_an_equal_address_rejoin() {
    const A: &str = "gw-1.a:8443";
    let (fence, r) = fence_with_late_b(A);
    std::fs::remove_dir_all(&fence.dir.0).expect("the folder is emptied");
    std::fs::create_dir(&fence.dir.0).expect("the empty folder");
    assert!(!fence.marked());
    fence.open(A).expect("the rejoin with A opens");
    late_b_is_refused(&fence, A, r);
    let (_, rejoined) = fence.record();
    fence.open(A).expect("a restart on the rejoined folder");
    assert_eq!(
        fence.record().1,
        rejoined,
        "data and no marker: nothing written"
    );
}

/// own83_b2_10 (#83 B2, PR #159 review r2, T32): the marker's presence is what fences. With B's
/// write pending, the marker's text is damaged: the open with A still writes A again, and the
/// late B is refused. A marker that is there but cannot be read (a folder in its place, so the
/// read fails for an I/O reason) refuses opens with an address and without one, before any
/// host starts, naming a step that keeps the marker; the marker stays. Made readable again,
/// the open with A goes on and resolves it. Cut: a damaged marker refused as before r2 (`Ok`
/// only for a parsed revision), red at "a damaged marker fences". Cut: the I/O arm answers
/// `Ok(false)`, red at "an unreadable marker" (the open goes on). Integration (~3 s).
#[config_log::retcd_test]
fn own83_b2_10_a_damaged_marker_fences_and_an_unreadable_one_refuses() {
    const A: &str = "gw-1.a:8443";
    let (fence, r) = fence_with_late_b(A);
    let marker = fence.dir.0.join(APP_ADDR_PENDING_FILE);
    std::fs::write(&marker, b"\xff not a revision\n").expect("the marker is damaged");
    fence.open(A).expect("a damaged marker fences");
    late_b_is_refused(&fence, A, r);

    let (_, before) = fence.record();
    std::fs::create_dir(&marker).expect("a folder where the marker is");
    for app in [Some(A), None] {
        let refused = fence
            .open_on(&fence.rig.rt, app)
            .expect_err("an unreadable marker");
        let text = refused.to_string();
        assert!(
            text.contains("is there but could not be read")
                && text.contains("Leave it and app-addr-pending.new in place")
                && text.contains("Nothing was started"),
            "{app:?}: {text}"
        );
        assert!(marker.is_dir(), "{app:?}: the marker is kept");
        assert_eq!(fence.record().1, before, "{app:?}: nothing written");
    }
    std::fs::remove_dir(&marker).expect("the folder is removed");
    std::fs::write(&marker, format!("{before}\n")).expect("a readable marker");
    fence.open(A).expect("readable: the open goes on");
    assert!(fence.record().1 > before, "it fenced");
    assert!(!fence.marked(), "and resolved the marker");
}

/// own83_b2_12 (#83 B2, PR #159 review r3, T47): the marker cannot be staged while an earlier
/// marker already says B may apply. With B pending, `app-addr-pending.new` is a folder, so
/// staging the next marker fails: the open with A is refused twice, before any host starts,
/// with no `nodes/` put sent, the marker and the record as they were. The folder removed, the
/// open with A fences, and the late B is refused. Cut: a staging failure ignored (the put sent
/// without a fresh marker), red at "refused". Integration (~3 s).
#[config_log::retcd_test]
fn own83_b2_12_a_marker_that_cannot_be_staged_refuses_until_it_can() {
    const A: &str = "gw-1.a:8443";
    let (fence, r) = fence_with_late_b(A);
    let marker = fence.dir.0.join(APP_ADDR_PENDING_FILE);
    let staged = fence.dir.0.join(format!("{APP_ADDR_PENDING_FILE}.new"));
    let held = std::fs::read(&marker).expect("the marker B left");
    std::fs::create_dir(&staged).expect("a folder where the staged file goes");
    let puts = fence.node_puts();
    for attempt in 1..=2 {
        let refused = fence
            .open(A)
            .expect_err("refused: the marker cannot be staged");
        assert!(
            matches!(refused, OpenError::Io(_)),
            "{attempt}: {refused:?}"
        );
        assert_eq!(fence.node_puts(), puts, "{attempt}: no put sent");
        assert_eq!(
            std::fs::read(&marker).expect("the marker is kept"),
            held,
            "{attempt}: the marker as it was"
        );
        assert_eq!(
            fence.record(),
            (Some(A.to_owned()), r),
            "{attempt}: the record as it was"
        );
    }
    std::fs::remove_dir(&staged).expect("the folder is removed");
    fence
        .open(A)
        .expect("stageable again: the open with A fences");
    late_b_is_refused(&fence, A, r);
}

/// own83_b2_13 (#83 B2, PR #159 review r3, T48): a lone `app-addr-pending.new`, as a crash
/// between staging and the rename leaves it, says nothing: the rename never happened, so no
/// write was sent after it. On a settled folder whose record holds A, with only the staged file
/// there, an open with A succeeds and writes nothing: the record and its revision as they were,
/// no `nodes/` put, no final marker, and the staged file left in place. Cut: the staged file
/// read as a marker (`pending_app_addr` true when it is there), red at "nothing written".
/// Integration (~2 s).
#[config_log::retcd_test]
fn own83_b2_13_a_lone_staged_marker_is_not_a_marker() {
    const A: &str = "gw-1.a:8443";
    let fence = Fence::new(A);
    fence.open(A).expect("settled: a restart with A");
    let settled = fence.record();
    let staged = fence.dir.0.join(format!("{APP_ADDR_PENDING_FILE}.new"));
    std::fs::write(&staged, format!("{}\n", settled.1)).expect("a staged file, no rename");
    let puts = fence.node_puts();
    fence
        .open(A)
        .expect("an open with A beside a lone staged file");
    assert_eq!(fence.record(), settled, "nothing written");
    assert_eq!(fence.node_puts(), puts, "no put");
    assert!(!fence.marked(), "no final marker");
    assert!(staged.exists(), "the staged file is left in place");
}

/// own83_b2_14 (#83 B2, PR #159 review r3, T42): B's write may still apply, and node 1's
/// folder is emptied, its marker with it. A rejoin given no app address on the emptied folder
/// cannot write an address, so it fences by writing the record's own bytes back: it opens, the
/// record still holds A at a later revision, and the late B is refused. The fence file is gone
/// once it has fenced. A restart with no address and one with A, on the rejoined folder, write
/// nothing. Cut: `fence_app_addr` skipped before the hosts start, red at "the recovery wrote A
/// again, past r" (no write, and the late B then applies). Integration (~3 s).
#[config_log::retcd_test]
fn own83_b2_14_a_no_address_rejoin_on_an_emptied_folder_fences() {
    const A: &str = "gw-1.a:8443";
    let (fence, r) = fence_with_late_b(A);
    std::fs::remove_dir_all(&fence.dir.0).expect("the folder is emptied");
    std::fs::create_dir(&fence.dir.0).expect("the empty folder");
    fence
        .open_on(&fence.rig.rt, None)
        .expect("the no-address rejoin opens");
    late_b_is_refused(&fence, A, r);
    assert!(
        !fence.dir.0.join(APP_ADDR_FENCE_FILE).exists(),
        "fenced: the fence file is removed"
    );
    let (_, rejoined) = fence.record();
    fence
        .open_on(&fence.rig.rt, None)
        .expect("a no-address restart");
    fence.open(A).expect("a restart with A");
    assert_eq!(
        fence.record(),
        (Some(A.to_owned()), rejoined),
        "settled: nothing written"
    );
}

/// own83_b2_15 (#83 B2, PR #159 review r3, T43): the new-folder fence survives a first open that
/// fails after the engine has created data. B's write may still apply; node 1's folder is
/// emptied but keeps an empty `nodes/`, so a failed open does not remove what its engine
/// created. Two opens with A on it fail at the read before the address is written, before any
/// host starts, each leaving `CURRENT` behind. The next open with A still fences (the fence file
/// says so, not `CURRENT`), and the late B is refused. A restart then writes nothing. Cut: the
/// fence file not consulted once `CURRENT` exists, red at "the recovery wrote A again, past r".
/// The fence file's name is pinned: it is on disk, so renaming it would miss a fence an older
/// build left owed. Integration (~3 s).
#[config_log::retcd_test]
fn own83_b2_15_a_failed_first_open_keeps_the_new_folder_fence() {
    const A: &str = "gw-1.a:8443";
    // The name is persisted: a folder an older build left owes its fence under it (T53).
    assert_eq!(APP_ADDR_FENCE_FILE, "app-addr-fence");
    let (fence, r) = fence_with_late_b(A);
    std::fs::remove_dir_all(&fence.dir.0).expect("the folder is emptied");
    std::fs::create_dir_all(fence.dir.0.join("nodes")).expect("an empty nodes/");
    let current = fence.dir.0.join("nodes").join("1").join("CURRENT");
    for attempt in 1..=2 {
        fence.store.failing_reads.store(1, Ordering::SeqCst);
        let refused = fence.open(A).expect_err("the read before the write fails");
        assert!(
            refused
                .to_string()
                .contains("read before the app address is written"),
            "{attempt}: {refused}"
        );
        assert!(current.exists(), "{attempt}: the engine left CURRENT");
        assert!(
            fence.dir.0.join(APP_ADDR_FENCE_FILE).exists(),
            "{attempt}: the fence is still owed"
        );
        assert_eq!(
            fence.record(),
            (Some(A.to_owned()), r),
            "{attempt}: nothing written"
        );
    }
    fence.open(A).expect("the healthy retry opens");
    late_b_is_refused(&fence, A, r);
    let (_, settled) = fence.record();
    fence.open(A).expect("a restart");
    assert_eq!(fence.record().1, settled, "settled: nothing written");
}

/// What meets the no-address fence's write in [`none_fence_refuses_until_it_fences`].
enum FenceMeets {
    /// Another write lands first: the fence meets a record that moved.
    Rival,
    /// Its reply is lost and it has not applied: it may still apply later.
    Late,
    /// It applies and its reply is lost.
    AppliedLost,
}

/// The no-address fence's refusals (PR #159 review r4, T28): B's write may still apply and node
/// 1's folder is emptied, its marker with it, so an open given no app address owes the fence.
/// Twice that fence's write meets `meets`: each open is refused `OpenError::Control`, before any
/// host starts ([`Fence::open_on`] counts them), its write having reached the store, and the
/// fence file is kept, so the next open fences again. Then a no-address open whose fence
/// applies opens: the fence file is gone, and every write still delayed (B, and any fence write
/// kept back) is refused when it arrives. A restart with no address writes nothing.
fn none_fence_refuses_until_it_fences(meets: FenceMeets) {
    const A: &str = "gw-1.a:8443";
    const C: &str = "gw-1.c:7443";
    let (fence, r) = fence_with_late_b(A);
    std::fs::remove_dir_all(&fence.dir.0).expect("the folder is emptied");
    std::fs::create_dir(&fence.dir.0).expect("the empty folder");
    let owed = fence.dir.0.join(APP_ADDR_FENCE_FILE);
    let rt = &fence.rig.rt;
    let mut held = (Some(A.to_owned()), r);
    for attempt in 1..=2 {
        let (step, why) = match meets {
            FenceMeets::Rival => (
                NodePut::Rival(fence.bytes_with(C)),
                "changed after this open read it",
            ),
            FenceMeets::Late => (NodePut::Late, "the fence's outcome is unknown"),
            FenceMeets::AppliedLost => (NodePut::AppliedLost, "the fence's outcome is unknown"),
        };
        fence.store.next(step);
        let puts = fence.node_puts();
        let refused = fence
            .open_on(rt, None)
            .expect_err("the fence did not settle");
        let text = refused.to_string();
        assert!(
            matches!(refused, OpenError::Control(_))
                && text.contains(why)
                && text.contains("nothing was started"),
            "{attempt}: {text}"
        );
        assert_eq!(fence.node_puts(), puts + 1, "{attempt}: the fence was sent");
        assert!(owed.exists(), "{attempt}: the fence is still owed");
        let now = fence.record();
        match meets {
            FenceMeets::Late => assert_eq!(now, held, "{attempt}: not applied"),
            FenceMeets::Rival => {
                assert_eq!(now.0.as_deref(), Some(C), "{attempt}: the rival's bytes");
                assert!(now.1 > held.1, "{attempt}: the rival moved it");
            }
            FenceMeets::AppliedLost => {
                assert_eq!(
                    now.0.as_deref(),
                    Some(A),
                    "{attempt}: the record's own bytes"
                );
                assert!(now.1 > held.1, "{attempt}: the fence applied");
            }
        }
        held = now;
    }
    fence
        .open_on(rt, None)
        .expect("a no-address open whose fence applies");
    assert!(!owed.exists(), "fenced: the debt is cleared");
    let (app, fenced) = fence.record();
    assert_eq!(app, held.0, "the fence wrote the record's own bytes");
    assert!(fenced > held.1, "the fence moved the revision on");
    let delayed = match meets {
        FenceMeets::Late => 3,
        FenceMeets::Rival | FenceMeets::AppliedLost => 1,
    };
    assert_eq!(
        fence.store.deliver(rt),
        vec![config_core::MutationOutcome::Conflict; delayed],
        "every delayed write is refused"
    );
    assert_eq!(
        fence.record(),
        (app.clone(), fenced),
        "nothing delayed applied"
    );
    fence.open_on(rt, None).expect("a no-address restart");
    assert_eq!(fence.record(), (app, fenced), "settled: nothing written");
}

/// own83_b2_16 (#83 B2, PR #159 review r4, T28): [`none_fence_refuses_until_it_fences`], the
/// fence meeting a rival write that moved the record. Cut: the fence's `Conflict` accepted
/// (the outcome check skipped), red at "the fence did not settle". Integration (~4 s).
#[config_log::retcd_test]
fn own83_b2_16_a_no_address_fence_met_by_a_rival_refuses_until_it_fences() {
    none_fence_refuses_until_it_fences(FenceMeets::Rival);
}

/// own83_b2_17 (#83 B2, PR #159 review r4, T28): [`none_fence_refuses_until_it_fences`], the
/// fence's reply lost before it applied. Accepting that unknown outcome would let an older
/// write land after startup. Cut: an unknown outcome accepted as fenced, red at "the fence did
/// not settle". Integration (~4 s).
#[config_log::retcd_test]
fn own83_b2_17_a_no_address_fence_whose_outcome_is_unknown_refuses_until_it_fences() {
    none_fence_refuses_until_it_fences(FenceMeets::Late);
}

/// own83_b2_18 (#83 B2, PR #159 review r4, T28): [`none_fence_refuses_until_it_fences`], the
/// fence applied and its reply lost. Cut: the fence file removed before the write is sent, red
/// at "the fence is still owed". Integration (~4 s).
#[config_log::retcd_test]
fn own83_b2_18_a_no_address_fence_applied_and_unanswered_refuses_until_it_fences() {
    none_fence_refuses_until_it_fences(FenceMeets::AppliedLost);
}

/// own83_b2_19 (#83 B2, PR #159 review r4, T28): the new-folder fence file cannot be put in
/// place. B's write may still apply and node 1's folder is emptied; a non-empty folder stands
/// where `app-addr-fence` goes, so its rename fails. Two opens given no app address are each
/// refused `OpenError::Io` before any host starts, with no `nodes/` put and no `CURRENT`
/// created. With the path repaired, a no-address open fences and opens, and the late B is
/// refused. Cut: the fence file's write failure ignored (`let _ =`), red at "no nodes/ put"
/// (the open goes on and sends the fence before it fails). Integration (~3 s).
#[config_log::retcd_test]
fn own83_b2_19_a_new_folder_fence_that_cannot_be_written_refuses_until_it_can() {
    const A: &str = "gw-1.a:8443";
    let (fence, r) = fence_with_late_b(A);
    std::fs::remove_dir_all(&fence.dir.0).expect("the folder is emptied");
    std::fs::create_dir(&fence.dir.0).expect("the empty folder");
    let blocker = fence.dir.0.join(APP_ADDR_FENCE_FILE);
    std::fs::create_dir(&blocker).expect("a folder where the fence file goes");
    std::fs::write(blocker.join("keep"), b"x").expect("it is not empty");
    let current = fence.dir.0.join("nodes").join("1").join("CURRENT");
    let rt = &fence.rig.rt;
    for attempt in 1..=2 {
        let puts = fence.node_puts();
        let refused = fence
            .open_on(rt, None)
            .expect_err("the fence file cannot be put in place");
        assert!(matches!(refused, OpenError::Io(_)), "{attempt}: {refused}");
        assert_eq!(fence.node_puts(), puts, "{attempt}: no nodes/ put");
        assert!(!current.exists(), "{attempt}: no CURRENT");
        assert_eq!(
            fence.record(),
            (Some(A.to_owned()), r),
            "{attempt}: nothing written"
        );
    }
    std::fs::remove_dir_all(&blocker).expect("the path is repaired");
    fence
        .open_on(rt, None)
        .expect("the repaired no-address open fences");
    late_b_is_refused(&fence, A, r);
    assert!(!blocker.exists(), "fenced: the fence file is removed");
}

/// The rEtcd cluster the #80 S2 control-TLS rows run against.
const S2_CLUSTER: config_core::ClusterId = config_core::ClusterId::from_bytes([0x80; 16]);

/// A three-voter rEtcd serving both planes under mutual TLS, on `rt` (#80 S2): its static
/// policy grants only the shared rDB client ([`certs::CONTROL_CLIENT`]) read and write on every
/// key, so rDB's control traffic (CAS, list, watch) is held to a real principal. Returned with
/// a client built from files in the `--control-tls` layout by [`certs::load_control`], with
/// the cluster id read from its certificate; its endpoints list the followers first, so its
/// first call meets a follower and must follow an authenticated leader hint. The `Box` keeps
/// the certificate folder.
fn mtls_control(
    rt: &tokio::runtime::Runtime,
    seed: u64,
) -> (
    config_testkit::Cluster,
    config_client::GrpcClient,
    Box<dyn std::any::Any>,
) {
    let policy = format!(
        "[[grant]]\nprincipal = \"{}\"\nprefix = \"\"\naccess = [\"read\", \"write\"]\n",
        certs::CONTROL_CLIENT
    );
    let cluster = rt.block_on(
        config_testkit::Cluster::builder()
            .nodes(3)
            .cluster_id(S2_CLUSTER)
            .storage(config_testkit::cluster::StorageKind::Ephemeral)
            .mutual_tls(seed)
            .authz(config_testkit::cluster::AuthzKind::Static(policy))
            .start(),
    );
    let leader = rt.block_on(cluster.leader());
    let dir = config_testkit::fs::temp_dir();
    let pair = cluster
        .fixture()
        .issue(config_testkit::CertProfile::client(certs::CONTROL_CLIENT));
    for (name, text) in [
        ("ca.pem", &pair.ca_pem),
        ("client.pem", &pair.cert_pem),
        ("client.key", &pair.key_pem),
    ] {
        std::fs::write(dir.path().join(name), text).expect("a control certificate file");
    }
    let control = certs::load_control(dir.path()).expect("the control folder loads");
    assert_eq!(
        control.cluster, S2_CLUSTER,
        "the cluster id comes from client.pem"
    );
    let mut endpoints: Vec<String> = cluster
        .followers()
        .into_iter()
        .map(|id| cluster.client_endpoint(id))
        .collect();
    endpoints.push(cluster.client_endpoint(leader));
    let _entered = rt.enter();
    let client = config_client::GrpcClient::connect(
        endpoints,
        config_client::GrpcClientOptions {
            tls: config_client::TlsMode::MutualTls(control.mtls),
            ..config_client::GrpcClientOptions::default()
        },
    )
    .expect("a control client")
    .with_cluster_id(control.cluster);
    (cluster, client, Box::new(dir))
}

/// Put `k<p>` in every partition through its owner and read it back (#80 S2): each partition
/// takes writes.
fn writes_in_every_partition(rig: &Rig, round: &str) {
    for partition in 1..=PARTITIONS {
        let owner = NodeId(ring(rig.k, partition).primary);
        let db = rig.of(owner);
        let group = group_of(db, partition);
        let key = format!("{round}-k{partition}");
        let put = put_lands(db, group, key.as_bytes(), b"v");
        assert_eq!(put.partition, PartitionId(partition), "{put:?}");
        let got = db.get_in(group, key.as_bytes()).expect("a get");
        assert_eq!(
            got.value.as_ref().map(|(_, value)| &value[..]),
            Some(&b"v"[..]),
            "{round}: {got:?}"
        );
    }
}

/// A rig of nodes 1 to 3 on 127.0.0.1 to .3, declared and opened over [`mtls_control`]'s
/// client, every node ready; with the rEtcd cluster and the client.
fn mtls_rig(
    seed: u64,
) -> (
    Rig,
    config_testkit::Cluster,
    config_client::GrpcClient,
    Box<dyn std::any::Any>,
) {
    let mut rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    let (cluster, client, certs) = mtls_control(&rig.rt, seed);
    assert_eq!(
        client.capabilities().transport_security,
        config_core::TransportSecurity::MutualTls,
        "the control store reports mutual TLS"
    );
    rig.store = Arc::new(client.clone());
    rig.declare_created();
    rig.open_all(fresh_dirs());
    rig.wait_ready();
    (rig, cluster, client, certs)
}

/// #80 S2 SC1 (AC1, ADR-rdb-0024): a database declared over a mutual-TLS control link, from a
/// `--control-tls` folder read by `certs::load_control`, forms: three node processes become
/// ready, and every partition takes a put and reads it back. rEtcd's static policy grants only
/// the shared `rdb` client, so the declaration's CAS writes, the opens' lists and the hosts'
/// watches all pass as that principal. The control client starts at a follower, so the cluster
/// id taken from the certificate is what lets it follow the leader hint. Integration (~5 s): a
/// 3-voter TLS rEtcd and one formation of 8 partitions.
/// Cut: the client is built with `TlsMode::Insecure`: it reports `Insecure`, red at the
/// transport-security assertion (observed 2026-11-19).
#[config_log::retcd_test]
fn s2_declared_database_forms_and_writes_over_mtls_control() {
    let (mut rig, cluster, client, _certs) = mtls_rig(801);
    writes_in_every_partition(&rig, "s2-1");
    assert!(
        client.stats().hint_follows >= 1,
        "a leader hint was followed: {:?}",
        client.stats()
    );
    rig.shutdown_all();
    rig.rt.block_on(cluster.shutdown());
}

/// #80 S2 SC2 (AC4): with the database of SC1 running, the voter leader stops; every partition
/// still takes writes within the put budget, the client following the new leader's hint under
/// mutual TLS. No `hint_identity_unverified` is logged. Integration (~8 s).
/// Cut 1: `mtls_control` drops `.with_cluster_id`: hints are refused unfollowed
/// (`hint_identity_unverified`) and the client, starting at a follower, never reaches the
/// leader: red at the declaration. Observed 2026-11-19: `NotLeader` with an unfollowed hint.
/// Review r1: after the stop, a fresh control sentinel (not an rDB key) is put create-only and
/// read back exactly, so a control plane that takes no mutation after the stop is red even
/// when the data plane writes on grants it holds. Cut 2: config-grpc's `put` handler refuses
/// every put on a voter other than the first that took one (a new leader takes no control
/// mutation): the sentinel put fails `DeadlineExceededUnknownOutcome`, red (observed
/// 2026-11-19).
#[config_log::retcd_test]
fn s2_control_writes_go_on_after_the_voter_leader_stops() {
    let (mut rig, cluster, client, _certs) = mtls_rig(802);
    writes_in_every_partition(&rig, "before");
    let leader = rig.rt.block_on(cluster.leader());
    rig.rt.block_on(cluster.stop_node(leader));
    let follows = client.stats().hint_follows;
    // The voters elect a new leader first: a call made while none leads is `Unavailable`
    // (observed in review r1's first run), which no client can answer before an election.
    let after = rig.rt.block_on(cluster.leader());
    // Review r1: a control mutation made after the leader stopped, not one the data plane can
    // finish on grants it already holds. Create-only, so it is fresh, and read back exactly;
    // the client last talked to the stopped leader, so it must follow the new one's hint.
    let key = bytes::Bytes::from(format!("s2-sentinel/after-leader-{}-stopped", leader.0));
    let value = bytes::Bytes::from(format!("sentinel stopped={} seed=802", leader.0));
    rig.rt
        .block_on(client.put(config_core::PutRequest {
            key: key.clone(),
            value: value.clone(),
            expected_mod_revision: Some(0),
            ..config_core::PutRequest::default()
        }))
        .expect("a fresh control put after the leader stopped");
    let read = rig
        .rt
        .block_on(client.get(config_core::GetRequest { key: key.clone() }))
        .expect("the control sentinel reads back");
    assert_eq!(
        read.record.map(|record| (record.key, record.value)),
        Some((key, value)),
        "the sentinel reads back exactly"
    );
    writes_in_every_partition(&rig, "after");
    assert_ne!(after, leader, "a new voter leads");
    let lines = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "s2_control_writes_go_on_after_the_voter_leader_stops",
    );
    assert!(
        !lines
            .iter()
            .any(|line| line.to_string().contains("hint_identity_unverified")),
        "no unverified hint"
    );
    tracing::info!(
        stopped = leader.0,
        new_leader = after.0,
        follows_before = follows,
        follows_after = client.stats().hint_follows,
        "s2_voter_leader_stopped"
    );
    rig.shutdown_all();
    rig.rt.block_on(cluster.shutdown());
}

/// The rig's 3-node declaration with its nodes at `hosts`, port 7100.
fn declaration_on(hosts: [&str; 3]) -> admin::Declaration {
    declaration_at(
        &(1..=3)
            .zip(hosts)
            .map(|(n, host)| (NodeId(n), format!("{host}:7100")))
            .collect(),
        PARTITIONS,
    )
}

/// #80 S2 SC3 (ruling 1, AC2, AC3): a declaration whose nodes are not all on loopback IPs is
/// refused over a plaintext control store, naming the first such node, its address and
/// `--control-tls`, and nothing is written: private IPs, a host name among loopback IPs, and
/// `localhost` (a name says nothing about where it resolves). The same declarations over a
/// store that reports mutual TLS are written, and 127.0.0.1 to .3 over the plaintext store are
/// written too. Unit (<100 ms, an in-memory store).
/// Cut: `declare` skips `control_tls_refusal`: the first case is written, red.
#[config_log::retcd_test]
fn s2_declare_refuses_plaintext_control_for_nodes_on_other_machines() {
    let rt = runtime();
    for (what, hosts, names) in [
        (
            "private IPs",
            ["10.0.0.1", "10.0.0.2", "10.0.0.3"],
            "node 1's address 10.0.0.1:7100",
        ),
        (
            "a name among loopback IPs",
            ["127.0.0.1", "db-2.example", "127.0.0.3"],
            "node 2's address db-2.example:7100",
        ),
        (
            "localhost",
            ["127.0.0.1", "127.0.0.2", "localhost"],
            "node 3's address localhost:7100",
        ),
        (
            "a private IP between loopback IPs",
            ["127.0.0.1", "10.0.0.2", "127.0.0.3"],
            "node 2's address 10.0.0.2:7100",
        ),
        (
            "an uppercase name after a loopback IP",
            ["127.0.0.1", "DB-2.Example", "127.0.0.3"],
            "node 2's address DB-2.Example:7100",
        ),
    ] {
        let declared = declaration_on(hosts);
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let refused = rt.block_on(admin::declare(&store, &declared));
        assert!(
            matches!(&refused, Err(admin::DeclareError::Refused(text))
                if text.contains(names) && text.contains("not TLS")
                    && text.contains("--control-tls") && text.contains("nothing was written")),
            "{what}: {refused:?}"
        );
        assert!(snapshot(&rt, &*store).is_empty(), "{what}: nothing written");
        let tls: Arc<dyn ConfigStore> = TlsReported::new(Arc::new(config_testkit::MemStore::new()));
        assert!(
            matches!(
                rt.block_on(admin::declare(&tls, &declared)),
                Ok(admin::Declared::Created(_))
            ),
            "{what}: written over a TLS store"
        );
    }
    for hosts in [
        ["127.0.0.1", "127.0.0.2", "[::1]"],
        ["127.0.0.2", "127.0.0.3", "[::ffff:127.0.0.1]"],
    ] {
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let declared = rt.block_on(admin::declare(&store, &declaration_on(hosts)));
        assert!(
            matches!(declared, Ok(admin::Declared::Created(_))),
            "{hosts:?}: loopback IPs need no TLS: {declared:?}"
        );
    }
}

/// #80 S2 SC4 (ruling 1, AC2, AC6): a database declared at 10.0.0.1 to .3 (over a store that
/// reports mutual TLS) is refused when node 1 opens it over the same records with a plaintext
/// store: `OpenError::Config` naming node 1's address and `--control-tls`, nothing written, no
/// `nodes/` folder. Over the TLS-reporting store the same open passes the check and opens.
/// Unit (<1 s, an in-memory store, no net).
/// Cut: `declared_open_refusal` skips `control_tls_refusal`: the plaintext open opens, red.
#[config_log::retcd_test]
fn s2_open_of_a_declared_database_refuses_plaintext_control() {
    let rt = runtime();
    let memory = Arc::new(config_testkit::MemStore::new());
    let plain: Arc<dyn ConfigStore> = memory.clone();
    let tls: Arc<dyn ConfigStore> = TlsReported::new(memory);
    let hosts = ["10.0.0.1", "10.0.0.2", "10.0.0.3"];
    let Ok(admin::Declared::Created(_)) = rt.block_on(admin::declare(&tls, &declaration_on(hosts)))
    else {
        panic!("declared over TLS");
    };
    let addresses = NodeAddresses {
        listen: "10.0.0.1:7100".to_owned(),
        peers: (1..=3)
            .zip(hosts)
            .map(|(n, host)| (NodeId(n), format!("{host}:7100")))
            .collect(),
        app: None,
    };
    let dir = config_testkit::fs::temp_dir();
    let before = snapshot(&rt, &*plain);
    let refused = open_declared_one(&rt, &plain, dir.path(), Some(addresses.clone()))
        .map(|mut db| db.shutdown())
        .expect_err("a plaintext open of a database on other machines is refused");
    let text = refused.to_string();
    assert!(matches!(refused, OpenError::Config(_)), "{text}");
    for name in [
        "node 1's address 10.0.0.1:7100",
        "not TLS",
        "--control-tls",
        "nothing but its folder was created",
    ] {
        assert!(text.contains(name), "{name:?} in {text}");
    }
    assert_eq!(snapshot(&rt, &*plain), before, "nothing written");
    assert!(!dir.path().join("nodes").exists(), "no nodes/ folder");
    let mut db = open_declared_one(&rt, &tls, dir.path(), Some(addresses))
        .unwrap_or_else(|e| panic!("over a TLS store node 1 opens: {e}"));
    db.shutdown();
}

/// #80 S2 review r1, owner answer 1: an app-only open (`DbConfig::local` empty) of a database
/// declared on other machines, over a plaintext store, is refused by the same `nodes/` rule,
/// naming node 1 and `--control-tls`, before it reads the owner or counts an open: the store
/// is unchanged. Over a TLS store the open passes that check (it is then refused for another
/// reason: no node has formed partition 1). Unit (an in-memory store).
/// Cut: `open_app` skips `control_tls_refusal`: the plaintext open is refused for the
/// unformed partition instead, red.
#[config_log::retcd_test]
fn s2_app_open_of_a_declared_database_refuses_plaintext_control() {
    let rt = runtime();
    let memory = Arc::new(config_testkit::MemStore::new());
    let plain: Arc<dyn ConfigStore> = memory.clone();
    let tls: Arc<dyn ConfigStore> = TlsReported::new(memory);
    let Ok(admin::Declared::Created(_)) = rt.block_on(admin::declare(
        &tls,
        &declaration_on(["10.0.0.1", "10.0.0.2", "10.0.0.3"]),
    )) else {
        panic!("declared over TLS");
    };
    let open_app = |store: &Arc<dyn ConfigStore>| {
        let dir = config_testkit::fs::temp_dir();
        rt.block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::new(crate::transport::Links::default())),
                ..config(dir.path())
            },
            Arc::clone(store),
            rt.handle().clone(),
            fast(),
        ))
        .map(|mut db| db.shutdown())
    };
    let before = snapshot(&rt, &*plain);
    let refused = open_app(&plain).expect_err("a plaintext app open is refused");
    let text = refused.to_string();
    assert!(matches!(refused, OpenError::Config(_)), "{text}");
    for name in [
        "node 1's address 10.0.0.1:7100",
        "not TLS",
        "--control-tls",
        "nothing was written",
    ] {
        assert!(text.contains(name), "{name:?} in {text}");
    }
    assert_eq!(snapshot(&rt, &*plain), before, "nothing written");
    let over_tls = open_app(&tls).expect_err("no node formed partition 1 yet");
    assert!(
        !over_tls.to_string().contains("not TLS"),
        "over TLS the app open passes the TLS check: {over_tls}"
    );
}
