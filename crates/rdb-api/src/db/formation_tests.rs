//! #80 S1a (ADR-rdb-0022): a database formed by declaration, three node "processes" in this test
//! process, as `failover_tests` runs them (a `Db`, a `Net` and a `Links` each, real TLS links),
//! but **each with its own folder** and no forming open: `admin::declare` writes the control
//! records, and each owner forms its own partitions from an empty copy.

use super::reopen_tests::{config, fast, runtime};
use super::*;
use crate::net::faults::Faults;
use crate::net::{certs, wire::Label, Net, NetConfig, PROTOCOL_VERSION};

/// The partition count the rows declare: more than one per node, so each owner forms several.
const PARTITIONS: u32 = 8;

/// One node process, its own folder kept alive with it.
struct Proc {
    node: NodeId,
    db: Db,
    _net: Arc<Net>,
    dir: PathBuf,
    /// The `TempDir` that removes `dir` when dropped.
    _dir: Box<dyn std::any::Any>,
}

struct Rig {
    rt: tokio::runtime::Runtime,
    store: Arc<dyn ConfigStore>,
    cert_dir: PathBuf,
    _cert_dir: Box<dyn std::any::Any>,
    procs: Vec<Proc>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for proc in &mut self.procs {
            proc.db.shutdown();
        }
    }
}

fn declaration() -> admin::Declaration {
    admin::Declaration {
        nodes: ALL_NODES.to_vec(),
        partitions: PARTITIONS,
    }
}

/// Open `node`'s process on `dir` alone, linked to the other two at `addrs`.
fn open_node(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    cert_dir: &Path,
    node: NodeId,
    dir: &Path,
    listener: tokio::net::TcpListener,
    addrs: &BTreeMap<NodeId, String>,
) -> Result<(Db, Arc<Net>), OpenError> {
    let _entered = rt.enter();
    let boot = next_boot(dir).expect("a boot");
    let label = Label::Node(node);
    let links = Links::new();
    let net = Net::start(
        NetConfig {
            tls: certs::load(cert_dir, &label).expect("node certs"),
            label,
            boot,
            peers: ALL_NODES.iter().copied().filter(|n| *n != node).collect(),
            dial: NetConfig::dial_lower(node, addrs),
            protocol_version: PROTOCOL_VERSION,
            admin_apps: BTreeSet::new(),
            faults: Faults::new(),
        },
        &links,
        Some(listener),
    )
    .expect("the node's net starts");
    let db = rt.block_on(Db::open_with(
        DbConfig {
            local: vec![node],
            links: Some(links),
            boot: Some(boot),
            ..config(dir)
        },
        Arc::clone(store),
        rt.handle().clone(),
        fast(),
    ))?;
    Ok((db, net))
}

/// Listeners for nodes 1 to 3, and the address each is reached at.
fn listeners(
    rt: &tokio::runtime::Runtime,
) -> (Vec<tokio::net::TcpListener>, BTreeMap<NodeId, String>) {
    let listeners: Vec<tokio::net::TcpListener> = ALL_NODES
        .iter()
        .map(|_| rt.block_on(config_testkit::ports::ephemeral_listener()).0)
        .collect();
    let addrs = ALL_NODES
        .iter()
        .zip(&listeners)
        .map(|(node, listener)| (*node, listener.local_addr().expect("bound").to_string()))
        .collect();
    (listeners, addrs)
}

/// Declare, then open nodes 1 to 3 each on a new empty folder of its own; none waited on yet.
fn declared_rig() -> Rig {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let cert_dir = config_testkit::fs::temp_dir();
    let labels: Vec<Label> = ALL_NODES.iter().map(|node| Label::Node(*node)).collect();
    certs::generate(cert_dir.path(), &labels).expect("certs");
    let declared = rt
        .block_on(admin::declare(&store, &declaration()))
        .expect("the declaration is written");
    assert!(
        matches!(declared, admin::Declared::Created(_)),
        "{declared:?}"
    );
    let (listeners, addrs) = listeners(&rt);
    let procs = ALL_NODES
        .iter()
        .zip(listeners)
        .map(|(node, listener)| {
            let dir = config_testkit::fs::temp_dir();
            let (db, net) = open_node(
                &rt,
                &store,
                cert_dir.path(),
                *node,
                dir.path(),
                listener,
                &addrs,
            )
            .unwrap_or_else(|e| panic!("node {}'s process opens its own folder: {e}", node.0));
            Proc {
                node: *node,
                db,
                _net: net,
                dir: dir.path().to_path_buf(),
                _dir: Box::new(dir),
            }
        })
        .collect();
    Rig {
        rt,
        store,
        cert_dir: cert_dir.path().to_path_buf(),
        _cert_dir: Box::new(cert_dir),
        procs,
    }
}

impl Rig {
    fn wait_ready(&self) {
        for proc in &self.procs {
            proc.db
                .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
                .unwrap_or_else(|e| panic!("node {} is ready: {e}", proc.node.0));
        }
    }

    /// The process that runs `node`.
    fn of(&self, node: NodeId) -> &Db {
        &self.procs[node.0 as usize - 1].db
    }

    /// Shut every process down and open each again on its own folder, at a new boot.
    fn restart_all(&mut self) {
        for proc in &mut self.procs {
            proc.db.shutdown();
        }
        let (listeners, addrs) = listeners(&self.rt);
        for (proc, listener) in self.procs.iter_mut().zip(listeners) {
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
            proc.db = db;
            proc._net = net;
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
/// instance; another partition count, or nodes other than 1 to 3, is refused and writes nothing.
/// Unit (<100 ms, an in-memory store).
#[config_log::retcd_test]
fn s1a_declare_is_idempotent_and_refuses_a_changed_count() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let refused = rt.block_on(admin::declare(
        &store,
        &admin::Declaration {
            nodes: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
            partitions: PARTITIONS,
        },
    ));
    assert!(
        matches!(&refused, Err(admin::DeclareError::Refused(text)) if text.contains("nodes 1, 2 and 3 only")),
        "{refused:?}"
    );
    let Ok(admin::Declared::Created(first)) = rt.block_on(admin::declare(&store, &declaration()))
    else {
        panic!("the first declaration is written");
    };
    assert_eq!(first.partitions, PARTITIONS);
    let again = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(&again, Ok(admin::Declared::Already(found)) if *found == first),
        "{again:?}"
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
        rt.block_on(admin::read_formation(&*store))
            .expect("the store answers"),
        Some(first),
        "the formation record is unchanged"
    );
}

/// S1a keeps A-R231(a): with no declaration, a node process on an empty folder is refused with
/// today's text, and forms nothing. Unit (<1 s, an in-memory store, no links).
#[config_log::retcd_test]
fn s1a_undeclared_partial_open_is_refused_as_before() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let refused = rt
        .block_on(Db::open_with(
            DbConfig {
                local: vec![NodeId(2)],
                boot: Some(next_boot(dir.path()).expect("a boot")),
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
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
    let listed = rt
        .block_on(store.list(config_core::ListRequest {
            prefix: Bytes::from_static(b""),
            max_items: 0,
            max_bytes: 0,
        }))
        .expect("the store answers");
    assert!(
        listed.records.is_empty(),
        "nothing was written: {:?}",
        listed.records
    );
}
