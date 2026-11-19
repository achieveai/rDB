//! #80 S1a (ADR-rdb-0022): a database formed by declaration, three node "processes" in this test
//! process, as `failover_tests` runs them (a `Db`, a `Net` and a `Links` each, real TLS links),
//! but **each with its own folder** and no forming open: `admin::declare` writes the control
//! records, and each owner forms its own partitions from an empty copy.

use super::reopen_tests::{config, fast, runtime};
use super::*;
use crate::net::faults::Faults;
use crate::net::{certs, wire::Label, Net, NetConfig, PROTOCOL_VERSION};
use crate::test_store::{HoldAt, PutHold, Script, Scripted};

/// The partition count the rows declare: more than one per node, so each owner forms several.
const PARTITIONS: u32 = 8;

/// One node process, its own folder kept alive with it.
struct Proc {
    node: NodeId,
    db: Db,
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
    /// Where each node listens once [`Rig::open_all`] has bound them: node N on 127.0.0.N, so
    /// every node of the rig is a different host on one machine (#80 S1b ruling 1).
    addrs: BTreeMap<NodeId, String>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for proc in &mut self.procs {
            proc.db.shutdown();
        }
    }
}

fn declaration() -> admin::Declaration {
    declaration_of(3, PARTITIONS)
}

/// A declaration of nodes 1 to `k` and `partitions` partitions (#80 S1b).
fn declaration_of(k: u32, partitions: u32) -> admin::Declaration {
    admin::Declaration {
        nodes: (1..=k).map(NodeId).collect(),
        partitions,
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
    )
}

/// [`open_node`] with `timeouts` (#80 S1b: a shorter unreachable drop).
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
    let db = rt.block_on(Db::open_with(
        DbConfig {
            local: vec![node],
            links: Some(links),
            boot: Some(boot),
            timeouts,
            ..config(dir)
        },
        Arc::clone(store),
        rt.handle().clone(),
        fast(),
    ))?;
    Ok((db, net))
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
    Rig {
        rt,
        store,
        cert_dir: cert_dir.path().to_path_buf(),
        _cert_dir: Box::new(cert_dir),
        procs: Vec::new(),
        k,
        timeouts: Timeouts::default(),
        own_runtimes: false,
        addrs: BTreeMap::new(),
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

/// The keys a whole declaration of `partitions` (more than one) writes, its reservation first,
/// then `routes/0`, the partition records and its open counter (`counters`: the counter keys
/// expected), and the formation record last.
fn declared_keys(partitions: u32, counters: &[&str], marker: bool) -> Vec<String> {
    let mut keys: Vec<String> = (1..=partitions)
        .map(|id| ControlKey::Partition(PartitionId(id)).encode())
        .collect();
    keys.push(ControlKey::Route(RangeId(0)).encode());
    keys.push(ControlKey::Operation(admin::RESERVATION_OPERATION).encode());
    keys.extend(counters.iter().map(|key| (*key).to_owned()));
    if marker {
        keys.push(ControlKey::Operation(admin::FORMATION_OPERATION).encode());
    }
    keys.sort();
    keys
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
    /// `declaration()` on this rig's store, which must write it.
    fn declare_created(&self) -> admin::Formation {
        match self
            .rt
            .block_on(admin::declare(&self.store, &declaration()))
        {
            Ok(admin::Declared::Created(formation)) => formation,
            other => panic!("the declaration is written: {other:?}"),
        }
    }

    /// Open nodes 1 to `k`, each on its folder in `dirs`, in that order; none waited on yet.
    fn open_all(&mut self, dirs: Vec<Folder>) {
        let (listeners, addrs) = listeners(&self.rt, self.k);
        let nodes: Vec<NodeId> = addrs.keys().copied().collect();
        for ((node, listener), (dir, keep)) in nodes.iter().zip(listeners).zip(dirs) {
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
            )
            .unwrap_or_else(|e| panic!("node {}'s process opens its own folder: {e}", node.0));
            self.procs.push(Proc {
                node: *node,
                db,
                _net: Some(net),
                dir,
                _dir: keep,
                own_rt,
            });
        }
        self.addrs = addrs;
    }

    /// Open `node`'s process once more on its folder, alone, the others down.
    fn reopen(&self, node: NodeId) -> Result<(Db, Arc<Net>), OpenError> {
        let (mut listeners, addrs) = listeners(&self.rt, self.k);
        open_node(
            &self.rt,
            &self.store,
            &self.cert_dir,
            node,
            &self.procs[node.0 as usize - 1].dir,
            listeners.remove(node.0 as usize - 1),
            &addrs,
        )
    }

    /// Shut every process down.
    fn shutdown_all(&mut self) {
        for proc in &mut self.procs {
            proc.db.shutdown();
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
        let (listeners, addrs) = listeners(&self.rt, self.k);
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
    let gap = admin::Declaration {
        nodes: vec![NodeId(1), NodeId(2), NodeId(4)],
        partitions: PARTITIONS,
    };
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

    let four = admin::Declaration {
        nodes: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        partitions: PARTITIONS,
    };
    let refused = rt.block_on(admin::declare(&store, &four));
    assert!(
        matches!(&refused, Err(admin::DeclareError::Refused(text))
            if text.contains("already declared with 8 partitions and nodes [1, 2, 3]")),
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

    stop_after(2 + 3);
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

    stop_after(2 + 5);
    let stopped = rt.block_on(admin::declare(&store, &declaration()));
    assert!(
        matches!(stopped, Err(admin::DeclareError::Records(_))),
        "{stopped:?}"
    );
    let second = snapshot(&rt, &*store);
    assert!(kept(&first, &second), "{first:?} -> {second:?}");
    assert_eq!(keys(&second).len(), 7, "{:?}", keys(&second));
    assert_eq!(
        rt.block_on(admin::read_formation(&*store)).expect("reads"),
        None
    );

    // PR #138 review r5: stopped after its counter, at the formation record. The run that
    // finishes names a new instance and a counter of its own; the stopped run's counter stays at
    // no opens, for an instance no formation record names.
    stop_after(2 + PARTITIONS + 1);
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
        pass_puts: 2 + (PARTITIONS - 1),
        fail_put_with: Some(config_core::ConfigError::Unavailable {
            reason: "scripted: the declaring process stopped".to_owned(),
        }),
        ..Script::default()
    }));
    let mut rig = bare_rig(scripted.clone());
    let stopped = rig.rt.block_on(admin::declare(&rig.store, &declaration()));
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
        PARTITIONS as usize + 1,
        "{:?}",
        keys(&prefix)
    );

    let (kept_dir, keep) = folder();
    let (listeners, addrs) = listeners(&rig.rt, rig.k);
    let refused = open_node(
        &rig.rt,
        &rig.store,
        &rig.cert_dir,
        NodeId(1),
        &kept_dir,
        listeners.into_iter().next().expect("a listener"),
        &addrs,
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
    let (listeners, addrs) = listeners(&rig.rt, rig.k);
    let mut listeners: Vec<Option<tokio::net::TcpListener>> =
        listeners.into_iter().map(Some).collect();
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
            db,
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
        let (listeners, addrs) = listeners(&rig.rt, rig.k);
        let opened = open_node(
            &rig.rt,
            &rig.store,
            &rig.cert_dir,
            NodeId(1),
            &dir,
            listeners.into_iter().next().expect("a listener"),
            &addrs,
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

/// PR #138 review, and #137: node 1 roots its first partition and stops before any write is
/// admitted (the commit hook faults it before activation), then loses its whole folder. With
/// the formation record present, its reopen is refused "has not finished forming", and running
/// the same declaration again (`Already`) does not change that: today's limit, which #137
/// owns. The refusal says so and does not tell the operator to run the formation again. The
/// refused opens bind nothing, start nothing and write nothing. Integration (~4 s).
/// Cut: `empty_owner_refusal` answers the shared `RERUN_FORM` step for a declared database
/// too; red at the text.
#[config_log::retcd_test]
fn s1a_a_declared_owner_wiped_before_any_write_stays_refused() {
    let mut rig = bare_rig(Arc::new(config_testkit::MemStore::new()));
    rig.declare_created();
    let dirs = fresh_dirs();
    crate::host::set_after_commit(&dirs[0].0, Some(crate::host::AfterCommit::Fault));
    rig.open_all(dirs);
    let owned: Vec<PartitionId> = (1..=PARTITIONS)
        .map(PartitionId)
        .filter(|partition| admin::initial_owner(*partition) == NodeId(1))
        .collect();
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(20));
    while !rig.rt.block_on(async {
        partition_states(&rig.store, PARTITIONS)
            .await
            .expect("the store answers")
            .iter()
            .any(|(partition, state)| {
                owned.contains(partition) && matches!(state, PartitionState::Rooted(..))
            })
    }) {
        assert!(Instant::now() < deadline, "node 1 never rooted a partition");
        std::thread::sleep(Duration::from_millis(20));
    }
    rig.shutdown_all();
    crate::host::set_after_commit(&rig.procs[0].dir, None);
    let before = snapshot(&rig.rt, &*rig.store);
    rig.wipe(NodeId(1));
    for attempt in ["first", "after declaring again"] {
        let refused = rig
            .reopen(NodeId(1))
            .map(|_| ())
            .expect_err("the wiped owner is refused");
        assert!(
            refused.to_string().contains("has not finished forming")
                && refused
                    .to_string()
                    .contains("does not finish it (issue #137)")
                && !refused
                    .to_string()
                    .contains("run the command that formed it again"),
            "{attempt}: {refused}"
        );
        assert!(!has_copies(&rig.procs[0].dir), "{attempt}: nothing bound");
        assert_eq!(
            snapshot(&rig.rt, &*rig.store),
            before,
            "{attempt}: nothing written"
        );
        let again = rig.rt.block_on(admin::declare(&rig.store, &declaration()));
        assert!(
            matches!(again, Ok(admin::Declared::Already(_))),
            "{again:?}"
        );
    }
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
    let (listeners, addrs) = listeners(&rig.rt, rig.k);
    let mut listeners: Vec<Option<tokio::net::TcpListener>> =
        listeners.into_iter().map(Some).collect();
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
        let (listeners, addrs) = self::listeners(&rig.rt, rig.k);
        let opened = open_node(
            &rig.rt,
            &rig.store,
            &rig.cert_dir,
            node,
            dir,
            listeners
                .into_iter()
                .nth(node.0 as usize - 1)
                .expect("a listener"),
            &addrs,
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
        pass_puts: 2 + PARTITIONS,
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
            // The formation record and `routes/0` reads pass; the read after the put fails.
            scripted.rescript(Script {
                pass_gets: 2,
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
    let (listeners, addrs) = listeners(&rig.rt, rig.k);
    let opened = open_node(
        &rig.rt,
        &raced,
        &rig.cert_dir,
        NodeId(1),
        &dir,
        listeners.into_iter().next().expect("a listener"),
        &addrs,
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
        let (listeners, addrs) = listeners(&rig.rt, rig.k);
        open_node(
            &rig.rt,
            store,
            &rig.cert_dir,
            node,
            &dirs[node.0 as usize - 1].0,
            listeners
                .into_iter()
                .nth(node.0 as usize - 1)
                .expect("a listener"),
            &addrs,
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
        .block_on(admin::declare(&rig.store, &declaration_of(k, partitions)))
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
        proc.db.shutdown();
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
            mut db,
            _net,
            dir,
            _dir,
            own_rt,
        } = self.procs.remove(at);
        db.shutdown();
        drop(db);
        // The old net holds the listener until it is dropped and its tasks end; its own
        // runtime, dropped, ends every task it ran, as the process's exit would.
        drop(_net);
        if let Some(rt) = own_rt {
            rt.shutdown_background();
        }
        let own_rt = self.own_runtimes.then(runtime);
        let rt = own_rt.as_ref().unwrap_or(&self.rt);
        let addr: std::net::SocketAddr = self.addrs[&node].parse().expect("an address");
        let deadline = Instant::now() + Duration::from_secs(5);
        let listener = loop {
            match rt.block_on(tokio::net::TcpListener::bind(addr)) {
                Ok(listener) => break listener,
                Err(e) => {
                    assert!(Instant::now() < deadline, "rebind {addr}: {e}");
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        };
        let (db, net) = open_node_with(
            rt,
            &self.store,
            &self.cert_dir,
            node,
            &dir,
            listener,
            &self.addrs,
            timeouts,
        )
        .unwrap_or_else(|e| panic!("node {} reopens its own folder: {e}", node.0));
        self.procs.insert(
            at,
            Proc {
                node,
                db,
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
    let mut rig = declared_rig_of(S1B_K, S1B_P);
    rig.wait_ready();
    for partition in 1..=S1B_P {
        let owner = NodeId(ring(S1B_K, partition).primary);
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
            .filter(|p| ring(S1B_K, *p).copies.contains(&proc.node.0))
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
        .block_on(admin::declare(&rig.store, &declaration_of(S1B_K, S1B_P)))
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
