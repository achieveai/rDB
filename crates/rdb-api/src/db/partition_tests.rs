//! Issue #79, slice S1: a database of more than one partition, in one process. Each row opens a
//! real three-node `Db` over the memory store; what they pin is the key map's effect on calls,
//! not the map (that is `rdb-core`'s `partition_map` rows).

use super::*;

use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::PartitionLifecycle;
use rdb_core::contracts::authority::DenyReason;

/// Group 2 maps to partition 1, primary node 1. Group 1 maps to partition 2, primary node 2.
const G1: AffinityId = AffinityId(2);
const G2: AffinityId = AffinityId(1);
const P2: PartitionId = PartitionId(2);

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

fn config(dir: &Path, partitions: Option<u32>) -> DbConfig {
    DbConfig {
        partitions,
        ack_floor: true,
        dir: dir.to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: ALL_NODES.to_vec(),
        links: None,
        client: DEFAULT_CLIENT,
        boot: None,
        storage: crate::StorageOptions::default(),
    }
}

fn store() -> Arc<dyn ConfigStore> {
    Arc::new(config_testkit::MemStore::new())
}

fn open(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    partitions: Option<u32>,
) -> Result<Db, OpenError> {
    rt.block_on(Db::open_with(
        config(dir, partitions),
        Arc::clone(store),
        rt.handle().clone(),
        crate::host::test_budgets(),
    ))
}

fn ready(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    partitions: Option<u32>,
) -> Db {
    let db = open(rt, dir, store, partitions).expect("a fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("every partition is ready");
    db
}

fn routes_root(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>) -> Option<u32> {
    rt.block_on(read_partition_count(store))
        .expect("routes/0 reads")
}

fn compile(request: u64, group: AffinityId) -> ClientCall {
    ClientCall::Compile {
        identity: RequestIdentity {
            tenant: TENANT,
            client: DEFAULT_CLIENT,
            request: RequestId(request),
        },
        puts: vec![PutOp {
            group,
            object: Bytes::from_static(b"misrouted"),
            change: byte_doc(b"x"),
            expect: None,
        }],
        remaining_millis: 5_000,
    }
}

/// Each group's write and read land on the partition that group maps to, on that partition's
/// own primary, and a group sees only its own partition's objects.
#[test]
fn a_group_writes_and_reads_on_its_own_partitions_primary() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));

    assert_eq!(db.partition_count(), 2);
    assert_eq!(db.owner_of(PARTITION), Some(NodeId(1)));
    assert_eq!(db.owner_of(P2), Some(NodeId(2)));
    assert_eq!(db.owner_of(PartitionId(3)), None);
    for (partition, primary, secondary) in [
        (PARTITION, NodeId(1), NodeId(2)),
        (P2, NodeId(2), NodeId(1)),
    ] {
        let on = |node| db.look_in(partition, node).expect("the node answers");
        assert_eq!(
            on(primary).admits,
            Some(true),
            "{partition:?} on its primary"
        );
        assert_ne!(on(secondary).admits, Some(true), "{partition:?} on a copy");
    }

    let first = db.put_in(G1, b"a", b"1", None).expect("group 2's put");
    assert_eq!(first.partition, PARTITION);
    let put = |object| TxnPut {
        group: G2,
        object,
        value: b"2",
        if_version: None,
    };
    let second = db
        .txn(&[put(b"b")], Duration::from_secs(5))
        .expect("group 1's txn");
    assert_eq!(second.partition, P2);

    let value = |group, object: &[u8]| {
        let found = db.get_in(group, object).expect("a read").value;
        found.map(|(_, bytes)| bytes.to_vec())
    };
    assert_eq!(value(G1, b"a"), Some(b"1".to_vec()));
    assert_eq!(value(G2, b"b"), Some(b"2".to_vec()));
    assert_eq!(value(G2, b"a"), None, "group 1 is another partition");
    assert_eq!(value(G1, b"b"), None, "group 2 is another partition");

    let status = db
        .status_in(G2, second.request, Some(second.generation))
        .expect("status where it was written");
    assert!(matches!(status, TxnStatus::Resolved(result) if result.partition == P2));

    // After the status read: this write raises the ack floor, which releases that status.
    let third = db
        .write_in(G2, b"c", byte_doc(b"3"), None)
        .expect("group 1's write")
        .expect("the write changed the object");
    assert_eq!(third.partition, P2);
    db.shutdown();
}

/// Issue #79 manual walk at N=4: node 1 primaries partitions 1 and 4, both F1s armed the one
/// discovery timer id at version 1, and the host faulted node 1. Each partition must recover.
#[test]
fn two_partitions_with_one_primary_both_recover_and_serve() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(4));
    let (first, fourth) = (AffinityId(2), AffinityId(7));
    assert_eq!(db.partition_for(first), PARTITION);
    assert_eq!(db.partition_for(fourth), PartitionId(4));
    assert_eq!(db.owner_of(PartitionId(4)), Some(NodeId(1)));

    let wrote = db
        .put_in(first, b"a", b"1", None)
        .expect("partition 1's put");
    assert_eq!(wrote.partition, PARTITION);
    let wrote = db
        .put_in(fourth, b"b", b"2", None)
        .expect("partition 4's put");
    assert_eq!(wrote.partition, PartitionId(4));
    let value = |group, object: &[u8]| {
        let found = db.get_in(group, object).expect("a read").value;
        found.map(|(_, bytes)| bytes.to_vec())
    };
    assert_eq!(value(first, b"a"), Some(b"1".to_vec()));
    assert_eq!(value(fourth, b"b"), Some(b"2".to_vec()));
    db.shutdown();
}

/// `write_as_in` under one id, sent twice: the first send applied, the repeat finds its entry
/// in the same partition and applies nothing. A resend goes to the partition its request's
/// group maps to and replays.
#[test]
fn a_repeat_of_write_as_in_finds_its_own_partition_and_applies_nothing() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));

    let id = app_request_id(900).expect("an app id");
    let first = db
        .write_as_in(G2, id, b"m", byte_doc(b"v"), None)
        .expect("the write")
        .expect("it sends");
    assert_eq!(first.partition, P2);
    let version = || db.read_in(G2, b"m", Look::Doc).expect("a read").seen;
    let before = version();

    let repeat = db
        .write_as_in(G2, id, b"m", byte_doc(b"v"), None)
        .expect_err("a fresh compile of the used id");
    assert_eq!(repeat.error.kind, ErrorKind::RequestIdReuse, "{repeat:?}");
    assert_eq!(version(), before, "the repeat applied nothing");

    let resent = db
        .resend((*first.sent).clone())
        .expect("the resend replays");
    assert_eq!(resent.seq, first.seq, "the same entry answered");
    assert_eq!(version(), before, "the resend applied nothing");
    db.shutdown();
}

/// The owner-side check: a call addressed to a partition its group does not map to is refused
/// there, sends nothing, and writes nothing.
#[test]
fn a_call_addressed_to_the_wrong_partition_is_route_changed_and_changes_nothing() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    let wait = Duration::from_secs(5);

    match db.call(P2, compile(1, G1), wait) {
        Some(Answer::Error { error, .. }) => {
            assert_eq!(error.kind, ErrorKind::RouteChanged, "{error:?}");
            assert!(error.no_mutation, "{error:?}");
            assert!(error.detail.contains("partition 1"), "{error:?}");
        }
        other => panic!("a compile for the wrong partition: {other:?}"),
    }
    let get = ClientCall::Get {
        identity: db.identity(),
        group: G1,
        object: Bytes::from_static(b"misrouted"),
        look: Look::Doc,
    };
    match db.call(P2, get, wait) {
        Some(Answer::Error { error, .. }) => {
            assert_eq!(error.kind, ErrorKind::RouteChanged, "{error:?}");
        }
        other => panic!("a read for the wrong partition: {other:?}"),
    }
    let seen = db.read_in(G1, b"misrouted", Look::Doc).expect("a read");
    assert_eq!(seen.seen, Seen::Absent, "nothing was written");
    db.shutdown();
}

fn refused(error: &ApiError, call: &str, instead: &str) {
    assert_eq!(error.kind, ErrorKind::InvalidArgument, "{call}: {error:?}");
    assert!(error.no_mutation, "{call}: {error:?}");
    assert!(error.detail.contains(call), "{call}: {error:?}");
    assert!(error.detail.contains(instead), "{call}: {error:?}");
}

/// Every call that cannot say which partition it means is refused above one partition, naming
/// the call and what to use, and nothing is sent.
#[test]
fn a_groupless_call_above_one_partition_is_refused_by_name() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    let as_id = app_request_id(1).expect("an app id");
    let deadline = Duration::from_secs(5);
    let put_refused = |answer: Result<PutOk, PutError>, call, instead| {
        refused(&answer.expect_err(call).error, call, instead);
    };
    let write_refused = |answer: Result<Option<PutOk>, PutError>, call, instead| {
        refused(&answer.expect_err(call).error, call, instead);
    };

    put_refused(db.put(b"o", b"v", None), "put", "put_in");
    put_refused(db.put_as(as_id, b"o", b"v", None), "put_as", "txn_as");
    write_refused(db.write(b"o", byte_doc(b"v"), None), "write", "write_in");
    write_refused(
        db.write_as(as_id, b"o", byte_doc(b"v"), None),
        "write_as",
        "write_as_in",
    );
    put_refused(
        db.txn(&[], deadline),
        "txn with no puts",
        "at least one put",
    );
    put_refused(
        db.txn_as(as_id, &[], deadline),
        "txn with no puts",
        "at least one put",
    );
    refused(&db.get(b"o").expect_err("get"), "get", "get_in");
    refused(
        &db.read(b"o", Look::Doc).expect_err("read"),
        "read",
        "read_in",
    );
    refused(
        &db.get_previous(b"o").expect_err("get_previous"),
        "get_previous",
        "no group form",
    );
    refused(
        &db.status(RequestId(1), None).expect_err("status"),
        "status",
        "status_in",
    );
    refused(
        &db.move_copy(NodeId(1), NodeId(2)).expect_err("move_copy"),
        "move_copy",
        "one partition",
    );
    refused(
        &db.pinned_members().expect_err("pinned_members"),
        "pinned_members",
        "one partition",
    );
    refused(
        &db.rebuild(NodeId(3), false).expect_err("rebuild"),
        "rebuild",
        "one partition",
    );
    #[cfg(debug_assertions)]
    {
        let ns = rdb_core::contracts::storage::Namespace::User;
        let damaged = db.damage_record(NodeId(1), Generation(1), ns, b"o");
        assert!(damaged.expect_err("damage").contains("damage_record"));
        let forked = db.fork_record(NodeId(1), Generation(1));
        assert!(forked.expect_err("fork").contains("fork_record"));
    }
    db.shutdown();
}

/// Above one partition `Db::promote` cannot say which partition moves, so it is refused naming
/// the call that can, and it freezes nothing (issue #79, ruling Q6).
#[test]
fn a_promote_above_one_partition_is_refused_and_the_next_open_heals() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    refused(
        &db.promote(NodeId(2), None).expect_err("promote"),
        "promote",
        "promote_partition",
    );
    db.shutdown();
    drop(db);
    let mut again = ready(&rt, dir.path(), &store, None);
    assert_eq!(again.owner_of(PARTITION), Some(NodeId(1)), "nothing moved");
    again.shutdown();
}

/// One partition stays as it was: no `routes/0`, and the group-less calls work.
#[test]
fn one_partition_writes_no_root_and_keeps_the_groupless_calls() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, None);

    assert_eq!(db.partition_count(), 1);
    assert_eq!(routes_root(&rt, &store), None, "N=1 writes no routes/0");
    let ok = db.put(b"o", b"v", None).expect("a group-less put");
    assert_eq!(ok.partition, PARTITION);
    let found = db.get(b"o").expect("a read").value;
    assert_eq!(found.map(|(_, bytes)| bytes.to_vec()), Some(b"v".to_vec()));
    db.shutdown();
}

/// A count no database can have is refused before anything exists: no `nodes/`, no root.
#[test]
fn a_partition_count_out_of_range_is_refused_before_anything_is_written() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let inner = dir.path().join("db");
    for asked in [0, MAX_PARTITIONS + 1] {
        let opened = open(&rt, &inner, &store, Some(asked));
        match opened.expect_err("refused") {
            OpenError::Config(text) => assert!(text.contains("out of range"), "{text}"),
            other => panic!("{other:?}"),
        }
        assert!(
            !inner.join("nodes").exists(),
            "nothing was created at {asked}"
        );
        assert_eq!(routes_root(&rt, &store), None, "no root at {asked}");
        assert!(
            config(&inner, Some(asked)).check().is_err(),
            "check at {asked}"
        );
    }
}

/// The count is fixed at formation: a reopen reads it from the root and cannot change it.
#[test]
fn a_formed_count_is_fixed_and_a_reopen_cannot_change_it() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    assert_eq!(
        routes_root(&rt, &store),
        Some(2),
        "the root names the count"
    );
    db.shutdown();
    drop(db);

    match open(&rt, dir.path(), &store, Some(3)).expect_err("refused") {
        OpenError::Mismatch(text) => assert!(text.contains("says 2"), "{text}"),
        other => panic!("{other:?}"),
    }
    let mut again = ready(&rt, dir.path(), &store, None);
    assert_eq!(again.partition_count(), 2, "the count comes from the root");
    again.shutdown();
    drop(again);

    let once = config_testkit::fs::temp_dir();
    let store = self::store();
    let mut single = ready(&rt, once.path(), &store, None);
    single.shutdown();
    drop(single);
    match open(&rt, once.path(), &store, Some(2)).expect_err("refused") {
        OpenError::Mismatch(text) => assert!(text.contains("formed with one"), "{text}"),
        other => panic!("{other:?}"),
    }
}

fn partition_value(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    partition: PartitionId,
) -> config_core::Record {
    rt.block_on(store.get(config_core::GetRequest {
        key: Bytes::from(ControlKey::Partition(partition).encode()),
    }))
    .expect("the record reads")
    .record
    .expect("the record is there")
}

fn generation_of(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    partition: PartitionId,
) -> Generation {
    PartitionRecord::decode(&partition_value(rt, store, partition).value)
        .expect("the record decodes")
        .generation
}

fn instance_of(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>) -> [u8; INSTANCE_LEN] {
    PartitionRoot::decode(&partition_value(rt, store, PARTITION).value)
        .expect("the root decodes")
        .instance
}

fn put_partition(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    partition: PartitionId,
    value: Bytes,
) {
    let revision = partition_value(rt, store, partition).mod_revision;
    rt.block_on(store.put(config_core::PutRequest {
        key: Bytes::from(ControlKey::Partition(partition).encode()),
        value,
        expected_mod_revision: Some(revision),
        dedup: None,
    }))
    .expect("the record is written");
}

/// Rewrite `partition`'s record through `change`, and return its bytes as they were.
fn tamper(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    partition: PartitionId,
    change: impl FnOnce(&mut PartitionRecord, &mut PartitionRoot),
) -> Bytes {
    let was = partition_value(rt, store, partition).value;
    let mut record = PartitionRecord::decode(&was).expect("the record decodes");
    let mut root = PartitionRoot::decode(&was).expect("the root decodes");
    change(&mut record, &mut root);
    put_partition(rt, store, partition, record.encode_with(&root));
    was
}

fn delete_key(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>, key: String) {
    rt.block_on(store.delete(config_core::DeleteRequest {
        key: Bytes::from(key),
        expected_mod_revision: None,
        dedup: None,
    }))
    .expect("the key is deleted");
}

/// Each partition's record is checked on a reopen, not only the first: a later partition ahead
/// of its copies, naming another instance, or missing is refused by name, and a copy holding a
/// generation of partition 1 does not count as holding it for partition 2. Cuts: `check_others`
/// answers nothing; `serves` looks at partition 1. Slower than most (~2 s): a real form, then
/// a real reopen.
#[test]
fn a_reopen_checks_every_partition_not_only_the_first() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    db.put_in(G2, b"k", b"v", None).expect("partition 2's put");
    db.shutdown();
    drop(db);

    let was = tamper(&rt, &store, P2, |record, _| {
        record.generation = Generation(record.generation.0 + 1);
    });
    match open(&rt, dir.path(), &store, None).expect_err("ahead of its copies") {
        OpenError::Stale { detail, .. } => assert!(detail.contains("partitions/2"), "{detail}"),
        other => panic!("{other:?}"),
    }
    put_partition(&rt, &store, P2, was);

    let was = tamper(&rt, &store, P2, |_, root| root.instance = [9; INSTANCE_LEN]);
    match open(&rt, dir.path(), &store, None).expect_err("another instance") {
        OpenError::Mismatch(text) => assert!(text.contains("partitions/2"), "{text}"),
        other => panic!("{other:?}"),
    }
    put_partition(&rt, &store, P2, was);

    let was = partition_value(&rt, &store, P2).value;
    delete_key(&rt, &store, ControlKey::Partition(P2).encode());
    match open(&rt, dir.path(), &store, None).expect_err("no record") {
        OpenError::Mismatch(text) => assert!(text.contains("partitions/2"), "{text}"),
        other => panic!("{other:?}"),
    }
    rt.block_on(store.put(config_core::PutRequest {
        key: Bytes::from(ControlKey::Partition(P2).encode()),
        value: was,
        expected_mod_revision: Some(0),
        dedup: None,
    }))
    .expect("the record is back");

    let mut db = ready(&rt, dir.path(), &store, None);
    let found = db.get_in(G2, b"k").expect("a read").value;
    assert_eq!(found.map(|(_, bytes)| bytes.to_vec()), Some(b"v".to_vec()));
    db.shutdown();
    drop(db);

    let held = generation_of(&rt, &store, PARTITION);
    let next = Generation(held.0 + 1);
    let mut engine = RocksEngine::open(dir.path().join("nodes").join("1")).expect("the copy");
    let base = Seq(engine.buffered_applied(PARTITION, held).0);
    engine
        .inherit(PARTITION, held, next, base)
        .expect("partition 1 alone moves on");
    assert!(serves(&engine, PARTITION, next));
    assert!(!serves(&engine, P2, next), "partition 1's generation only");
}

/// Above one partition, a later partition's activation is its own mark beside the open counter,
/// which stays partition 1's. Cut: `activate_in` writes the counter for every partition.
#[test]
fn a_later_partition_marks_its_own_activation() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    let instance = instance_of(&rt, &store);
    let (marked, revision) = rt
        .block_on(admin::read_activated(&*store, &instance, P2))
        .expect("the mark reads");
    assert_eq!(marked, generation_of(&rt, &store, P2));
    assert!(revision > 0, "partition 2 wrote its own mark");
    let (count, _) = rt
        .block_on(admin::read_count(&*store, &instance))
        .expect("the counter reads");
    assert_eq!(count.activated, generation_of(&rt, &store, PARTITION));
    db.shutdown();
}

/// A later partition's roll-forward is claimed on its own mark: a claim at a read that has moved
/// loses. Cut: `claim_roll` writes with no expected revision.
#[test]
fn a_roll_claim_at_a_moved_read_loses() {
    let rt = runtime();
    let store = store();
    let instance = [7; INSTANCE_LEN];
    rt.block_on(admin::claim_roll(&*store, &instance, P2, 0, Generation(5)))
        .expect("the first claim");
    let moved = rt.block_on(admin::claim_roll(&*store, &instance, P2, 0, Generation(6)));
    assert!(
        matches!(moved, Err(admin::CounterError::Moved { .. })),
        "{moved:?}"
    );
    let (marked, revision) = rt
        .block_on(admin::read_activated(&*store, &instance, P2))
        .expect("the mark reads");
    assert_eq!(marked, Generation(5));
    rt.block_on(admin::claim_roll(
        &*store,
        &instance,
        P2,
        revision,
        Generation(6),
    ))
    .expect("a claim at the current read");
}

/// A reopen cut short after partition 2 committed its next generation: the later partition
/// rolls forward on the next open, unless its own mark says it may have admitted writes (A-R184
/// per partition). Cuts: the later partition's guard reads no mark; `link_roll` links partition 1.
/// Slower than most (~3 s): three real opens.
#[config_log::retcd_test]
fn a_cut_short_reopen_rolls_a_later_partition_forward_unless_it_is_marked() {
    const METHOD: &str = "a_cut_short_reopen_rolls_a_later_partition_forward_unless_it_is_marked";
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    db.put_in(G2, b"k", b"v", None).expect("partition 2's put");
    db.shutdown();
    drop(db);
    let formed = generation_of(&rt, &store, P2);

    // Only node 2's commit is cut short: node 2 owns partition 2, and partition 1 recovers.
    let owner = dir.path().join("nodes").join("2");
    crate::host::set_after_commit(&owner, Some(crate::host::AfterCommit::Fault));
    let mut db = open(&rt, dir.path(), &store, None).expect("the reopen cut short");
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let deadline = std::time::Instant::now() + patience;
    while generation_of(&rt, &store, P2) <= formed {
        assert!(
            std::time::Instant::now() < deadline,
            "partition 2 never committed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    db.shutdown();
    drop(db);
    crate::host::set_after_commit(&owner, None);

    let committed = generation_of(&rt, &store, P2);
    let instance = instance_of(&rt, &store);
    let (marked, _) = rt
        .block_on(admin::read_activated(&*store, &instance, P2))
        .expect("the mark reads");
    assert!(marked < committed, "the fixture was never activated");

    rt.block_on(admin::activate_in(&*store, &instance, P2, committed))
        .expect("mark it activated");
    match open(&rt, dir.path(), &store, None).expect_err("it may have admitted writes") {
        OpenError::Stale { detail, .. } => {
            assert!(detail.contains("partitions/2"), "{detail}");
            assert!(detail.contains("activated"), "{detail}");
        }
        other => panic!("{other:?}"),
    }
    assert!(
        !rolled(METHOD).contains(&Some(2)),
        "a refused open rolls nothing"
    );

    rt.block_on(admin::claim_roll(
        &*store,
        &instance,
        P2,
        rt.block_on(admin::read_activated(&*store, &instance, P2))
            .expect("the mark reads")
            .1,
        marked,
    ))
    .expect("the mark goes back");

    let mut db = ready(&rt, dir.path(), &store, None);
    assert!(rolled(METHOD).contains(&Some(2)), "partition 2 rolled");
    let found = db.get_in(G2, b"k").expect("a read").value;
    assert_eq!(found.map(|(_, bytes)| bytes.to_vec()), Some(b"v".to_vec()));
    db.shutdown();
}

/// A formed `Some(2)` database's store with its `partitions/1` put back to the state a formation
/// leaves before partition 1's first commit: a generation-0 record, written once, no root.
fn formed_with_first_partition_unrooted(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
) -> [u8; INSTANCE_LEN] {
    let scratch = config_testkit::fs::temp_dir();
    let mut db = ready(rt, scratch.path(), store, Some(2));
    db.shutdown();
    drop(db);
    let instance = instance_of(rt, store);
    delete_key(rt, store, ControlKey::Partition(PARTITION).encode());
    untouched_record(rt, store, PARTITION, NodeId(1));
    instance
}

/// Create `partition`'s generation-0 record as a formation writes it, and return its bytes.
fn untouched_record(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    partition: PartitionId,
    owner: NodeId,
) -> Bytes {
    let value = PartitionRecord {
        partition,
        owner,
        generation: Generation(0),
        owner_epoch: OwnerEpoch(0),
        config_version: admin::CONFIG_VERSION,
        lifecycle: rdb_core::authority::partition::PartitionLifecycle::Serving,
    }
    .encode();
    rt.block_on(store.put(config_core::PutRequest {
        key: Bytes::from(ControlKey::Partition(partition).encode()),
        value: value.clone(),
        expected_mod_revision: Some(0),
        dedup: None,
    }))
    .expect("the record is written");
    value
}

/// Copies under `dir` that hold partition 2 at `generation` and nothing of partition 1.
fn copies_holding_only_partition_two(
    dir: &Path,
    instance: [u8; INSTANCE_LEN],
    generation: Generation,
) {
    for node in ALL_NODES {
        let path = dir.join("nodes").join(node.0.to_string());
        std::fs::create_dir_all(&path).expect("the copy's folder");
        let mut engine = RocksEngine::open_with(&path, crate::StorageOptions::default())
            .expect("the copy opens");
        engine.bind_instance(instance).expect("the copy binds");
        engine
            .inherit(P2, Generation(0), generation, Seq::ZERO)
            .expect("the copy links partition 2");
    }
}

/// `Db::check` judged partition 1 by whether a copy held any partition, so a formation where
/// partition 2 landed first read as another database's store (issue #79, design 7.2 item 1).
/// Slower than most (~5 s): a real form and an open that waits out its grants.
#[test]
fn a_later_partition_landing_first_is_no_mismatch_on_the_first() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let instance = formed_with_first_partition_unrooted(&rt, &store);
    copies_holding_only_partition_two(dir.path(), instance, generation_of(&rt, &store, P2));

    let mut db = ready(&rt, dir.path(), &store, None);
    assert!(generation_of(&rt, &store, PARTITION) > Generation(0));
    db.put_in(G1, b"a", b"1", None).expect("partition 1's put");
    db.put_in(G2, b"b", b"2", None).expect("partition 2's put");
    db.shutdown();
}

/// With partition 1 still to form, the later partitions were not looked at (design 7.2 item 2):
/// a store whose partition 2 belongs to another instance must be refused for that.
#[test]
fn a_first_partition_still_to_form_does_not_skip_the_others() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    formed_with_first_partition_unrooted(&rt, &store);
    match open(&rt, dir.path(), &store, None).expect_err("another instance's partition 2") {
        OpenError::Mismatch(text) => assert!(text.contains("partitions/2"), "{text}"),
        other => panic!("{other:?}"),
    }
}

/// A formation cut short right after `node`'s first commit: the record of `partition`, which
/// `node` owns, has a root and no copy linked it (design 7.2 row 4). Node `node` alone is cut,
/// so the other copies keep answering.
fn formation_cut_after_a_commit(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    partitions: Option<u32>,
    node: u32,
    partition: PartitionId,
) {
    let owner = dir.join("nodes").join(node.to_string());
    crate::host::set_after_commit(&owner, Some(crate::host::AfterCommit::Fault));
    let mut db = open(rt, dir, store, partitions).expect("the formation cut short");
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let deadline = std::time::Instant::now() + patience;
    while generation_of(rt, store, partition) == Generation(0) {
        assert!(std::time::Instant::now() < deadline, "it never committed");
        std::thread::sleep(Duration::from_millis(20));
    }
    db.shutdown();
    drop(db);
    crate::host::set_after_commit(&owner, None);
}

/// Design 7.2 row 4: the partition's root is committed, no copy linked it and no host activated
/// it, so the open links it onto the empty base, the step the crash skipped. Cut: the empty base
/// condition in `holds_base`. Slower than most (~3 s): two real opens.
#[config_log::retcd_test]
fn a_formation_cut_after_a_commit_rolls_forward_from_the_empty_base() {
    const METHOD: &str = "a_formation_cut_after_a_commit_rolls_forward_from_the_empty_base";
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    formation_cut_after_a_commit(&rt, dir.path(), &store, Some(2), 2, P2);
    let mut db = ready(&rt, dir.path(), &store, None);
    assert!(rolled(METHOD).contains(&Some(2)), "partition 2 rolled");
    db.put_in(G1, b"a", b"1", None).expect("partition 1's put");
    db.put_in(G2, b"b", b"2", None).expect("partition 2's put");
    db.shutdown();
}

/// Row 4 on partition 1, where the open counter is the mark, at N=2 (partition 2 may be anywhere
/// short of ready: this open forms or rolls it) and at N=1 (no partition 2 at all). Slower than
/// most (~6 s): four real opens.
#[config_log::retcd_test]
fn a_formation_cut_after_the_first_partitions_commit_finishes_on_the_next_open() {
    const METHOD: &str =
        "a_formation_cut_after_the_first_partitions_commit_finishes_on_the_next_open";
    let rt = runtime();
    for partitions in [Some(2), None] {
        let dir = config_testkit::fs::temp_dir();
        let store = store();
        formation_cut_after_a_commit(&rt, dir.path(), &store, partitions, 1, PARTITION);
        let mut db = ready(&rt, dir.path(), &store, partitions);
        db.put_in(G1, b"a", b"1", None).expect("partition 1's put");
        db.shutdown();
    }
    assert_eq!(
        rolled(METHOD).iter().filter(|p| p.is_none()).count(),
        2,
        "partition 1 rolled at both counts"
    );
}

/// Row 4's guard stays (ruling A-R184): an activation mark means a host may have admitted
/// writes, so an empty base is refused and nothing is linked. Slower than most (~2 s).
#[config_log::retcd_test]
fn an_empty_base_with_an_activation_mark_stays_stale() {
    const METHOD: &str = "an_empty_base_with_an_activation_mark_stays_stale";
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    formation_cut_after_a_commit(&rt, dir.path(), &store, Some(2), 2, P2);
    // Partition 2's root: partition 1 may not have landed when node 2 stopped.
    let instance = PartitionRoot::decode(&partition_value(&rt, &store, P2).value)
        .expect("a root")
        .instance;
    let generation = generation_of(&rt, &store, P2);
    rt.block_on(admin::activate_in(&*store, &instance, P2, generation))
        .expect("mark it activated");
    match open(&rt, dir.path(), &store, None).expect_err("it may have admitted writes") {
        OpenError::Stale { detail, .. } => {
            assert!(detail.contains("activated"), "{detail}");
        }
        other => panic!("{other:?}"),
    }
    assert!(rolled(METHOD).is_empty(), "nothing is linked");
}

/// Only the empty base may be missing: a root over a later predecessor, or over generation 0
/// past seq 0, still answers `predecessor_absent` and `Stale`. Cuts: either half of the empty
/// base condition. Slower than most (~2 s).
#[config_log::retcd_test]
fn a_missing_predecessor_that_is_not_the_empty_base_stays_stale() {
    const METHOD: &str = "a_missing_predecessor_that_is_not_the_empty_base_stays_stale";
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    formation_cut_after_a_commit(&rt, dir.path(), &store, Some(2), 2, P2);
    let base = PartitionRoot::decode(&partition_value(&rt, &store, P2).value).expect("a root");
    let later = tamper(&rt, &store, P2, |record, root| {
        record.generation = Generation(9);
        root.predecessor_generation = Generation(8);
    });
    let stale = |what| match open(&rt, dir.path(), &store, None).expect_err(what) {
        OpenError::Stale { detail, .. } => assert!(detail.contains("no copy holds"), "{detail}"),
        other => panic!("{what}: {other:?}"),
    };
    stale("a later predecessor");
    put_partition(&rt, &store, P2, later);
    assert_eq!(
        PartitionRoot::decode(&partition_value(&rt, &store, P2).value).expect("a root"),
        base,
        "the fixture is back"
    );
    tamper(&rt, &store, P2, |_, root| {
        root.predecessor_cutoff = Seq(5);
        root.base_seq = Seq(5);
    });
    stale("generation 0 past seq 0");
    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let verdicts = lines
        .iter()
        .filter(|line| line["@m"] == "db_open_roll_forward_copy")
        .filter(|line| line["verdict"] == "predecessor_absent")
        .count();
    assert_eq!(verdicts, 6, "three copies, two opens");
}

/// A node process never forms and never rolls from empty (design 7.2): it finds row 1, 2 or 4
/// as owner, or partition 1 still to form, and is told to run the formation again. Slower than
/// most (~2 s).
#[test]
fn a_node_process_finding_an_unfinished_formation_names_the_re_run() {
    let rt = runtime();
    let told = |error: OpenError| match error {
        OpenError::Config(text) => assert!(text.contains("--partitions"), "{text}"),
        other => panic!("{other:?}"),
    };

    // Row 4 on partition 1, as its owner: not "promote another node first".
    let dir = config_testkit::fs::temp_dir();
    let store = self::store();
    formation_cut_after_a_commit(&rt, dir.path(), &store, Some(2), 1, PARTITION);
    match open_as(&rt, dir.path(), &store, vec![NodeId(1)]).expect_err("row 4") {
        OpenError::Config(text) => {
            assert!(text.contains("partition 1 has not finished"), "{text}");
            assert!(text.contains("--partitions"), "{text}");
        }
        other => panic!("{other:?}"),
    }

    // Row 4 on partition 2, as its owner. Partition 1 may not have landed when node 2 stopped,
    // and then the refusal comes from partition 1; it names the same step.
    let dir = config_testkit::fs::temp_dir();
    let store = self::store();
    formation_cut_after_a_commit(&rt, dir.path(), &store, Some(2), 2, P2);
    told(open_as(&rt, dir.path(), &store, vec![NodeId(2)]).expect_err("row 4"));

    // Partition 1 still to form, partition 2 landed on this copy.
    let dir = config_testkit::fs::temp_dir();
    let store = self::store();
    let instance = formed_with_first_partition_unrooted(&rt, &store);
    copies_holding_only_partition_two(dir.path(), instance, generation_of(&rt, &store, P2));
    told(open_as(&rt, dir.path(), &store, vec![NodeId(1)]).expect_err("partition 1 forming"));

    // Rows 1 and 2 on partition 2, on a copy of a formed database.
    let dir = config_testkit::fs::temp_dir();
    let store = self::store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    db.shutdown();
    drop(db);
    delete_key(&rt, &store, ControlKey::Partition(P2).encode());
    told(open_as(&rt, dir.path(), &store, vec![NodeId(1)]).expect_err("row 1"));
    untouched_record(&rt, &store, P2, NodeId(2));
    told(open_as(&rt, dir.path(), &store, vec![NodeId(1)]).expect_err("row 2"));
}

/// Rows 3 and 6, and a record gone from under a copy that holds the partition: another
/// database's history, refused as partition 1 refuses it. No open reaches a heal, so ~1 s.
#[test]
fn a_later_partition_with_no_root_is_refused_unless_this_open_can_form_it() {
    let rt = runtime();
    let dir = config_testkit::fs::temp_dir();
    let store = self::store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    db.shutdown();
    drop(db);
    let mismatch = |what| match open(&rt, dir.path(), &store, None).expect_err(what) {
        OpenError::Mismatch(text) => assert!(text.contains("partition 2"), "{what}: {text}"),
        other => panic!("{what}: {other:?}"),
    };
    let key = ControlKey::Partition(P2).encode();
    delete_key(&rt, &store, key.clone());
    mismatch("no record, the copies hold it");
    let written = untouched_record(&rt, &store, P2, NodeId(2));
    mismatch("row 3");
    put_partition(&rt, &store, P2, written);
    mismatch("row 6");
}

/// The `partition` of every roll-forward logged by the current test.
fn rolled(method: &str) -> Vec<Option<u64>> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == "db_open_rolled_forward")
        .map(|line| line["partition"].as_u64())
        .collect()
}

/// A node holds one grant however many partitions it owns, so a reopen heals each owner once,
/// over all its partitions. Node 1 owns partitions 1 and 4 here. Why a heal per partition fails
/// is `a_second_heal_for_one_owner_meets_the_grant_the_first_one_led_to` in `admin`; two heals
/// run back to back can pass by luck, so this row reads the plan, not the outcome. Cut: one heal
/// per partition. Slower than most (~3 s): a real form and reopen over four partitions.
#[config_log::retcd_test]
fn a_reopen_heals_an_owner_of_two_partitions_once() {
    const METHOD: &str = "a_reopen_heals_an_owner_of_two_partitions_once";
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let (first, fourth) = (AffinityId(2), AffinityId(7));
    let mut db = ready(&rt, dir.path(), &store, Some(4));
    db.put_in(first, b"a", b"1", None).expect("partition 1");
    db.put_in(fourth, b"b", b"2", None).expect("partition 4");
    db.shutdown();
    drop(db);

    let mut db = ready(&rt, dir.path(), &store, None);
    let value = |group, object: &[u8]| {
        let found = db.get_in(group, object).expect("a read").value;
        found.map(|(_, bytes)| bytes.to_vec())
    };
    assert_eq!(value(first, b"a"), Some(b"1".to_vec()));
    assert_eq!(value(fourth, b"b"), Some(b"2".to_vec()));
    db.put_in(fourth, b"c", b"3", None)
        .expect("partition 4 again");
    db.shutdown();
    let of_node_1: Vec<String> =
        config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "db_heal_owner" && line["owner"] == 1)
            .map(|line| line["partitions"].as_str().expect("a list").to_owned())
            .collect();
    assert_eq!(of_node_1, ["[1, 4]"], "node 1's heals on the reopen");
}

fn open_as(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    local: Vec<NodeId>,
) -> Result<Db, OpenError> {
    rt.block_on(Db::open_with(
        DbConfig {
            local,
            links: Some(crate::transport::Links::new()),
            ..config(dir, None)
        },
        Arc::clone(store),
        rt.handle().clone(),
        crate::host::test_budgets(),
    ))
}

/// A node process waits only for the partitions it leads, and an empty copy of a partition's
/// owner is refused by that partition's number. Cut: `wait_ready` waits for every partition.
#[test]
fn a_node_process_waits_only_for_the_partitions_it_leads() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    db.shutdown();
    drop(db);

    let elsewhere = config_testkit::fs::temp_dir();
    let mut node = open_as(&rt, elsewhere.path(), &store, vec![NodeId(3)]).expect("node 3 opens");
    assert_eq!(node.partition_count(), 2);
    node.wait_ready(Duration::from_millis(200))
        .expect("node 3 leads no partition here");
    node.shutdown();
    drop(node);

    let empty = config_testkit::fs::temp_dir();
    match open_as(&rt, empty.path(), &store, vec![NodeId(2)]).expect_err("refused") {
        OpenError::Config(text) => {
            assert!(text.contains("partition 2 names it owner"), "{text}");
            assert!(text.contains("--partition 2"), "{text}");
        }
        other => panic!("{other:?}"),
    }
}

/// A node process whose copy is behind on partition 2 joins it, even though the same copy holds
/// partition 1 at that generation or later: each partition's lineages are judged alone. Cut:
/// `decide_partition`'s `behind` counts every partition's lineages, and this open is `Stale`.
#[test]
fn a_node_behind_on_one_partition_joins_it_though_another_is_ahead() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    db.shutdown();
    drop(db);

    let held = generation_of(&rt, &store, PARTITION);
    let next = Generation(held.0 + 1);
    let mut engine = RocksEngine::open(dir.path().join("nodes").join("3")).expect("the copy");
    let base = Seq(engine.buffered_applied(PARTITION, held).0);
    engine
        .inherit(PARTITION, held, next, base)
        .expect("partition 1 alone moves on");
    drop(engine);
    for partition in [PARTITION, P2] {
        tamper(&rt, &store, partition, |record, _| record.generation = next);
    }

    let mut node = open_as(&rt, dir.path(), &store, vec![NodeId(3)]).expect("node 3 joins");
    node.shutdown();
}

/// An app-only open is sized by the root and takes each partition's owner from its record. Cut:
/// `open_app` fills owners from `initial_owner`, as a fresh open does.
#[test]
fn an_app_open_takes_each_owner_from_its_record() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(2));
    db.shutdown();
    drop(db);
    tamper(&rt, &store, P2, |record, _| record.owner = NodeId(3));

    let elsewhere = config_testkit::fs::temp_dir();
    let mut app = open_as(&rt, elsewhere.path(), &store, Vec::new()).expect("the app opens");
    assert_eq!(app.partition_count(), 2);
    assert_eq!(app.owner_of(PARTITION), Some(NodeId(1)));
    assert_eq!(app.owner_of(P2), Some(NodeId(3)));
    app.shutdown();
}

// ---- S3: one partition's promote, readiness and the route ----

/// A group that maps to `partition` of `db`.
fn group_in(db: &Db, partition: PartitionId) -> AffinityId {
    (1..10_000)
        .map(AffinityId)
        .find(|group| db.partition_for(*group) == partition)
        .expect("some group maps to every partition")
}

fn stop_node(db: &Db, node: u32) {
    lock(&db.nodes)
        .iter_mut()
        .find(|handle| handle.node == NodeId(node))
        .expect("the node runs here")
        .stop();
}

fn partition_record(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    partition: PartitionId,
) -> PartitionRecord {
    rt.block_on(admin::read_partition(&**store, partition))
        .expect("the record reads")
        .0
}

fn grant_frozen(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>, node: u32) -> bool {
    let read = rt
        .block_on(store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Grant(NodeId(node)).encode()),
        }))
        .expect("the grant reads");
    read.record
        .and_then(|found| GrantRecord::decode(&found.value))
        .is_some_and(|grant| grant.frozen)
}

fn operations(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>) -> usize {
    rt.block_on(store.list(config_core::ListRequest {
        prefix: Bytes::from_static(b"operations/"),
        max_items: 0,
        max_bytes: 0,
    }))
    .expect("operations/ lists")
    .records
    .len()
}

/// What `db.readiness()` says of `partition`.
fn readiness_of(db: &Db, partition: PartitionId) -> PartitionReady {
    db.readiness()
        .into_iter()
        .find(|ready| ready.partition == partition)
        .expect("every partition has an entry")
}

/// Nothing a refused promote may leave behind: the record serving at its owner, the owner's grant
/// not frozen, and no plan stored.
#[track_caller]
fn nothing_written(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    partition: PartitionId,
    owner: u32,
) {
    let record = partition_record(rt, store, partition);
    assert_eq!(record.owner, NodeId(owner), "{record:?}");
    assert_eq!(record.lifecycle, PartitionLifecycle::Serving, "{record:?}");
    assert!(!grant_frozen(rt, store, owner), "grants/{owner} stays open");
    assert_eq!(operations(rt, store), 0, "no plan is stored");
}

/// Wait until `partition` is ready: a promote answers at the commit and the new owner admits a
/// little later, once its hold ends; a partition that lost a copy comes back by itself.
#[track_caller]
fn until_ready(db: &Db, partition: PartitionId) -> PartitionReady {
    let until = Instant::now() + crate::host::test_patience(Duration::from_secs(15));
    loop {
        let now = readiness_of(db, partition);
        if now.ready {
            return now;
        }
        assert!(Instant::now() < until, "{now:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

const PART2: PartitionId = PartitionId(2);
const PART3: PartitionId = PartitionId(3);
const PART5: PartitionId = PartitionId(5);
const PART8: PartitionId = PartitionId(8);

/// S3's first scenario, in one process: node 2 dies, its partitions read `NoLink` at once and the
/// rest ready; one moves to node 3, which already leads others (Y8); it is ready and takes writes,
/// node 3's own partition never stopped. A repeat to the same node is `AlreadyOwner`; a second
/// promote to a different node must not freeze the live new owner (Y2).
/// Integration (~13 s): a promote waits out node 2's grant; node 3's own partition then waits for the dead copy's drop.
#[config_log::retcd_test]
fn s3_a_dead_owners_partition_moves_onto_a_survivor_and_its_others_stay_no_link() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(8));
    let (moved, kept) = (group_in(&db, PART2), group_in(&db, PART3));
    db.put_in(moved, b"before", b"1", None).expect("before");
    db.put_in(kept, b"before", b"1", None).expect("before");

    let held_before = db
        .look_in(PART3, NodeId(3))
        .expect("node 3 answers")
        .authority;
    stop_node(&db, 2);
    let asked = Instant::now();
    let seen = db.readiness();
    assert!(
        asked.elapsed() < db.timeouts.read,
        "no wait on a node with no link: {:?}",
        asked.elapsed()
    );
    for ready in &seen {
        let dead = ready.partition.0 % 3 == 2;
        assert_eq!(ready.owner == NodeId(2), dead, "{ready:?}");
        assert_eq!(ready.ready, !dead, "{ready:?}");
        assert_eq!(ready.reason.is_some(), dead, "{ready:?}");
        if dead {
            assert_eq!(ready.reason, Some(NotReady::NoLink), "{ready:?}");
        }
    }

    let promoted = db
        .promote_partition(PART2, NodeId(3), None)
        .unwrap_or_else(|error| panic!("node 2 is dead: {error}"));
    assert_eq!(
        (promoted.partition, promoted.owner, promoted.prior_owner),
        (PART2, NodeId(3), NodeId(2)),
        "{promoted:?}"
    );
    assert_eq!(db.owner_of(PART2), Some(NodeId(3)));
    assert_eq!(until_ready(&db, PART2).owner, NodeId(3));
    for still in [PART5, PART8] {
        assert_eq!(readiness_of(&db, still).reason, Some(NotReady::NoLink));
    }
    // Y8: taking a partition does not disturb the grant node 3 already holds. Its own partition
    // paused only for node 2's loss as a copy, and comes back by itself.
    let held = db
        .look_in(PART3, NodeId(3))
        .expect("node 3 answers")
        .authority;
    assert_eq!(held, held_before, "node 3's grant was not re-acquired");
    until_ready(&db, PART3);
    db.put_in(moved, b"after", b"2", None)
        .expect("the moved partition writes");
    db.put_in(kept, b"after", b"2", None)
        .expect("node 3's own writes");
    let found = db.get_in(moved, b"before").expect("a read").value;
    assert_eq!(found.map(|(_, bytes)| bytes.to_vec()), Some(b"1".to_vec()));

    let again = db
        .promote_partition(PART2, NodeId(3), None)
        .map_err(ApiError::from)
        .expect_err("already the owner");
    assert!(
        again.detail.contains("already owns partition 2"),
        "{again:?}"
    );
    let other = db
        .promote_partition(PART2, NodeId(1), None)
        .map_err(ApiError::from)
        .expect_err("node 3 is alive");
    assert!(other.detail.contains("node 3 is alive"), "{other:?}");
    assert!(other.no_mutation, "{other:?}");
    assert!(!grant_frozen(&rt, &store, 3), "the new owner is not frozen");
    assert_eq!(
        partition_record(&rt, &store, PART2).lifecycle,
        PartitionLifecycle::Serving
    );

    let plain = db.promote(NodeId(3), None).expect_err("names no partition");
    refused(&plain, "promote", "promote_partition");
    let none = db
        .promote_partition(PartitionId(99), NodeId(3), None)
        .map_err(ApiError::from)
        .expect_err("no such partition");
    assert_eq!(none.kind, ErrorKind::InvalidArgument, "{none:?}");
    db.shutdown();
}

/// A live owner above one partition is refused by name with nothing written: no `Fencing`, no
/// `operations/` key, no frozen grant (lead ruling on 3.3a). Integration (~1 s): a fresh open.
#[test]
fn s3_a_live_owner_is_refused_by_name_and_nothing_is_written() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(8));

    let refused = db
        .promote_partition(PART2, NodeId(3), None)
        .map_err(ApiError::from)
        .expect_err("node 2 is alive");
    assert_eq!(
        (refused.kind, refused.retry, refused.no_mutation),
        (ErrorKind::Unavailable, RetryRule::NotWired, true),
        "{refused:?}"
    );
    assert!(
        refused
            .detail
            .contains("node 2 is alive and owns partition 2"),
        "{refused:?}"
    );
    nothing_written(&rt, &store, PART2, 2);
    assert!(readiness_of(&db, PART2).ready, "it never stopped");
    db.put_in(group_in(&db, PART2), b"k", b"v", None)
        .expect("it still writes");
    db.shutdown();
}

/// A paused owner (SIGSTOP, a frozen VM) answers nothing and its expiry stands still, so the
/// promote proceeds. When it resumes after the commit, calls queued at it during the pause are
/// refused, it ends fenced, and its other partitions, whose owner now answers, can still be moved:
/// the grant is frozen already, so there is nothing left to protect.
/// Integration (~3 s): a promote waits out node 2's grant.
#[test]
fn s3_a_paused_owner_resuming_after_the_commit_acks_nothing_and_ends_fenced() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(8));
    db.timeouts.read = Duration::from_millis(300);
    let (moved, stayed) = (group_in(&db, PART2), group_in(&db, PART5));

    let compiled = |request, group, partition| {
        let answer = db
            .send_call(NodeId(2), partition, compile(request, group))
            .expect("node 2 runs")
            .recv_timeout(Duration::from_secs(5))
            .expect("it compiles");
        let Answer::Compiled(request) = answer else {
            panic!("{answer:?}");
        };
        request
    };
    let compiled_moved = compiled(900, moved, PART2);
    let compiled_stayed = compiled(902, stayed, PART5);

    let (release, stall) = mpsc::channel();
    db.send_to(NodeId(2), Msg::Stall(stall)).expect("a stall");
    let promoted = db
        .promote_partition(PART2, NodeId(3), None)
        .unwrap_or_else(|error| panic!("a paused owner looks dead: {error}"));
    assert_eq!(promoted.owner, NodeId(3), "{promoted:?}");

    // A write compiled before the pause and a read of each partition, queued at the paused node
    // after the commit.
    let read = |request, group| ClientCall::Get {
        identity: RequestIdentity {
            tenant: TENANT,
            client: DEFAULT_CLIENT,
            request: RequestId(request),
        },
        group,
        object: Bytes::from_static(b"stale"),
        look: Look::Doc,
    };
    let queued: Vec<_> = [
        (
            PART2,
            ClientCall::Resend {
                request: compiled_moved,
            },
        ),
        (PART2, read(901, moved)),
        (
            PART5,
            ClientCall::Resend {
                request: compiled_stayed,
            },
        ),
        (PART5, read(903, stayed)),
    ]
    .into_iter()
    .map(|(partition, call)| {
        db.send_call(NodeId(2), partition, call)
            .expect("queued at the paused node")
    })
    .collect();
    release.send(()).expect("the node is stalled");

    let patience = crate::host::test_patience(Duration::from_secs(10));
    for answer in queued {
        let answer = answer.recv_timeout(patience).expect("it answers");
        assert!(
            refusal_kind(&answer).is_some(),
            "the old owner refuses: {answer:?}"
        );
    }
    db.timeouts.read = Duration::from_secs(1);
    let deadline = Instant::now() + patience;
    let fenced = loop {
        let seen = readiness_of(&db, PART5);
        if matches!(seen.reason, Some(NotReady::Fenced(_))) {
            break seen;
        }
        assert!(Instant::now() < deadline, "node 2 never fenced: {seen:?}");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(!fenced.ready, "{fenced:?}");
    assert!(grant_frozen(&rt, &store, 2), "and stays frozen");

    let next = db
        .promote_partition(PART5, NodeId(3), None)
        .unwrap_or_else(|error| panic!("its grant is frozen, so it can move: {error}"));
    assert_eq!(next.owner, NodeId(3), "{next:?}");
    until_ready(&db, PART5);
    db.put_in(stayed, b"after", b"v", None)
        .expect("partition 5 writes");
    db.shutdown();
}

/// An owner that resumes during the promote's wait ends one of two clean ways, never partly
/// done: refused with nothing written, or a whole promote. Unit-sized (~1 s): the owner resumes
/// inside the look, or inside the grant wait.
#[test]
fn s3_a_paused_owner_resuming_during_the_wait_is_refused_cleanly() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(8));
    for (read, after, partition) in [
        (Duration::from_secs(5), Duration::from_millis(100), PART2),
        (
            Duration::from_millis(100),
            Duration::from_millis(400),
            PART5,
        ),
    ] {
        db.timeouts.read = read;
        let (release, stall) = mpsc::channel();
        db.send_to(NodeId(2), Msg::Stall(stall)).expect("a stall");
        let resume = std::thread::spawn(move || {
            std::thread::sleep(after);
            release.send(()).expect("the node is stalled");
        });
        match db.promote_partition(partition, NodeId(3), None) {
            Err(error) => {
                assert!(error.to_string().contains("node 2 is alive"), "{error:?}");
                nothing_written(&rt, &store, partition, 2);
            }
            Ok(promoted) => assert_eq!(promoted.owner, NodeId(3), "{promoted:?}"),
        }
        resume.join().expect("the resume thread");
    }
    db.shutdown();
}

/// Each partition's fault names its own cause, and the order is the operator's: a fault, then a
/// fence, then an open not finished, then a pause. Cut: test `admits` first and a fenced copy
/// reads `Paused`. Integration (~0.2 s): a real status to vary.
#[test]
fn s3_not_ready_names_the_first_true_cause() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(4));
    let serving = db
        .look_in(PartitionId(1), NodeId(1))
        .expect("node 1 answers");
    assert_eq!(not_ready(&serving), None, "an admitting partition is ready");
    let paused = NodeStatus {
        admits: Some(false),
        protection: Some("paused".to_owned()),
        ..serving.clone()
    };
    assert_eq!(
        not_ready(&paused),
        Some(NotReady::Paused("paused".to_owned()))
    );
    let recovering = NodeStatus {
        admits: None,
        recovery: Some("waiting".to_owned()),
        ..paused.clone()
    };
    assert_eq!(
        not_ready(&recovering),
        Some(NotReady::Recovering("waiting".to_owned()))
    );
    let fenced = NodeStatus {
        fence: Some(DenyReason::Expired),
        ..recovering.clone()
    };
    assert_eq!(
        not_ready(&fenced),
        Some(NotReady::Fenced("Expired".to_owned()))
    );
    let faulted = NodeStatus {
        fault: Some("disk".to_owned()),
        ..fenced
    };
    assert_eq!(
        not_ready(&faulted),
        Some(NotReady::Faulted("disk".to_owned()))
    );
    db.shutdown();
}

/// The re-read gap is each partition's own: a read of one does not spend another's. Cut: one
/// shared instant and partition 5 never gets its read. Integration (~0.2 s): a real route.
#[test]
fn s3_a_route_refresh_is_kept_per_partition() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(8));
    let read = |partition: u32| {
        db.route
            .last_read
            .lock()
            .expect("lock")
            .contains_key(&partition)
    };
    // Only a `Db` that runs some nodes elsewhere re-reads; give this one the store it would use.
    db.route.refresh = Some((Arc::clone(&store), rt.handle().clone()));
    db.route.last_read.lock().expect("lock").clear();
    assert!(!read(2) && !read(5), "nothing has been re-read yet");
    db.refresh_owner(PART2);
    assert!(read(2) && !read(5), "only partition 2 was read");
    db.refresh_owner(PART5);
    assert!(read(2) && read(5), "partition 5 has a gap of its own");
    db.shutdown();
}

/// Promotes of different partitions run at once and share nothing but the grant's freeze. Each
/// partition has a writer; the row logs and prints its write gap, last write before the owner
/// died to first write after the move, the number #61's pause estimate rests on.
/// Integration (~3 s): one wait for node 2's grant, shared.
#[config_log::retcd_test]
fn s3_promotes_of_different_partitions_run_at_once_and_each_write_gap_is_recorded() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let db = ready(&rt, dir.path(), &store, Some(16));
    let moving: Vec<PartitionId> = (1..=16)
        .filter(|partition| partition % 3 == 2)
        .map(PartitionId)
        .collect();
    let killed = std::sync::OnceLock::<Instant>::new();
    let barrier = std::sync::Barrier::new(moving.len());
    let spans: Vec<(PartitionId, Instant, Instant, Duration)> = std::thread::scope(|scope| {
        let writers: Vec<_> = moving
            .iter()
            .map(|partition| {
                let (db, killed) = (&db, &killed);
                let group = group_in(db, *partition);
                scope.spawn(move || {
                    let mut last = Instant::now();
                    loop {
                        if db.put_in(group, b"w", b"v", None).is_err() {
                            std::thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        let now = Instant::now();
                        if killed.get().is_some_and(|at| now > *at) {
                            return now - last;
                        }
                        last = now;
                    }
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(100));
        stop_node(&db, 2);
        killed.set(Instant::now()).expect("once");
        let promotes: Vec<_> = moving
            .iter()
            .map(|partition| {
                let (db, barrier) = (&db, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    let started = Instant::now();
                    let promoted = db
                        .promote_partition(*partition, NodeId(3), None)
                        .unwrap_or_else(|error| panic!("{partition:?}: {error}"));
                    assert_eq!(promoted.partition, *partition);
                    (started, Instant::now())
                })
            })
            .collect();
        moving
            .iter()
            .zip(promotes)
            .zip(writers)
            .map(|((partition, promote), writer)| {
                let (started, ended) = promote.join().expect("a promote");
                (*partition, started, ended, writer.join().expect("a writer"))
            })
            .collect()
    });
    let last_start = spans.iter().map(|span| span.1).max().expect("some");
    let first_end = spans.iter().map(|span| span.2).min().expect("some");
    assert!(last_start < first_end, "the promotes overlapped");
    for (partition, started, ended, gap) in &spans {
        let promote_ms = (*ended - *started).as_millis();
        let gap_ms = gap.as_millis();
        tracing::info!(partition = partition.0, promote_ms, gap_ms, "s3_write_gap");
        println!(
            "s3_write_gap partition={} promote_ms={promote_ms} gap_ms={gap_ms}",
            partition.0
        );
        assert!(
            *gap < crate::host::test_patience(Duration::from_secs(30)),
            "{partition:?} {gap:?}"
        );
    }
    let mut db = db;
    db.shutdown();
}

fn open_linked(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    node: u32,
    links: &Arc<crate::transport::Links>,
) -> Result<Db, OpenError> {
    rt.block_on(Db::open_with(
        DbConfig {
            local: vec![NodeId(node)],
            links: Some(Arc::clone(links)),
            ..config(dir, None)
        },
        Arc::clone(store),
        rt.handle().clone(),
        crate::host::test_budgets(),
    ))
}

/// Three node processes on one set of links, each ready, and `apps` app handles over them, each
/// having seen every owner. The nodes live under `root/db`, the apps under `root/app-N`.
fn linked_apps(
    rt: &tokio::runtime::Runtime,
    root: &Path,
    store: &Arc<dyn ConfigStore>,
    partitions: u32,
    apps: u32,
    patience: Duration,
) -> (Vec<Db>, Vec<Db>) {
    let dir = root.join("db");
    std::fs::create_dir_all(&dir).expect("the database folder");
    let mut formed = ready(rt, &dir, store, Some(partitions));
    formed.shutdown();
    drop(formed);
    let links = crate::transport::Links::new();
    let nodes: Vec<Db> = [1, 2, 3]
        .map(|node| {
            open_linked(rt, &dir, store, node, &links)
                .unwrap_or_else(|error| panic!("node {node} opens: {error}"))
        })
        .into();
    for db in &nodes {
        db.wait_ready(patience).expect("each leads its own");
    }
    let apps: Vec<Db> = (0..apps)
        .map(|app| {
            let app_dir = root.join(format!("app-{app}"));
            std::fs::create_dir_all(&app_dir).expect("the app's folder");
            let db = rt
                .block_on(Db::open_with(
                    DbConfig {
                        local: Vec::new(),
                        links: Some(Arc::clone(&links)),
                        client: ClientId(2 + app),
                        ..config(&app_dir, None)
                    },
                    Arc::clone(store),
                    rt.handle().clone(),
                    crate::host::test_budgets(),
                ))
                .expect("the app opens");
            db.wait_ready(patience).expect("the app sees every owner");
            db
        })
        .collect();
    (nodes, apps)
}

/// Node 2 stalls, partition 2 moves to node 3 by another app's promote, and node 2 resumes: the
/// other apps hear nothing, and node 2 answers without admitting, so their routes name it until
/// they read the record again. Both `readiness` and `wait_ready` must move to node 3, with no
/// dead link to prompt them, and a `wait_ready` whose re-read finds node 3 after its `within`
/// is still a timeout. Cut: drop the re-read for an answer that does not admit, in either
/// place, and that handle waits at node 2 until its patience runs out. Integration (~4 s): a
/// promote waits out node 2's grant.
#[test]
fn s3_a_resumed_old_owner_does_not_hold_readiness_after_a_promote_from_another_app() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let patience = crate::host::test_patience(Duration::from_secs(20));
    let (mut nodes, mut apps) = linked_apps(&rt, dir.path(), &store, 2, 4, patience);

    let (release, stall) = mpsc::channel();
    nodes[1]
        .send_to(NodeId(2), Msg::Stall(stall))
        .expect("a stall");
    let promoted = apps[0]
        .promote_partition(P2, NodeId(3), None)
        .unwrap_or_else(|error| panic!("a stalled owner looks dead: {error}"));
    assert_eq!(promoted.owner, NodeId(3), "{promoted:?}");
    release.send(()).expect("the node is stalled");

    // Node 2 is back and fenced: it answers, and does not admit.
    let until = Instant::now() + patience;
    loop {
        match apps[1].look_in(P2, NodeId(2)) {
            Ok(status) if status.admits != Some(true) => break,
            _ => assert!(
                Instant::now() < until,
                "node 2 never answered without admitting"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(apps[1].owner_of(P2), Some(NodeId(2)), "it has not heard");
    assert_eq!(apps[2].owner_of(P2), Some(NodeId(2)), "it has not heard");
    assert_eq!(apps[3].owner_of(P2), Some(NodeId(2)), "it has not heard");

    let seen = loop {
        let seen = readiness_of(&apps[1], P2);
        if seen.owner == NodeId(3) && seen.ready {
            break seen;
        }
        assert!(Instant::now() < until, "{seen:?}");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(seen.owner, NodeId(3), "{seen:?}");
    // PR #102 review, round 2: a re-read that finds the new owner after `within` has passed is
    // still a timeout. Node 3 admits by now (`readiness` above saw it). `within` is zero, so the
    // deadline has passed before the first look; the owner it names is node 3, so the re-read
    // did happen. Waited past `OWNER_REREAD_GAP` first, so the re-read is not skipped. Cut: no
    // deadline check before the `continue`, and this call looks again at node 3, which admits,
    // and answers `Ok`.
    std::thread::sleep(OWNER_REREAD_GAP + Duration::from_millis(50));
    let late = apps[3].wait_ready(Duration::ZERO);
    assert!(
        matches!(
            late,
            Err(ReadyError::TimedOut {
                owner: NodeId(3),
                ..
            })
        ),
        "a new owner found after the deadline is a timeout: {late:?}"
    );
    apps[2]
        .wait_ready(patience)
        .expect("wait_ready moves to the new owner");
    assert_eq!(apps[2].owner_of(P2), Some(NodeId(3)), "its route moved");

    for db in nodes.iter_mut().chain(apps.iter_mut()) {
        db.shutdown();
    }
}

/// The control for the row above: an owner the record still names, fenced, is not read as moved.
/// Node 2 still owns partition 5 after partition 2 moved off it; a re-read names node 2 again, so
/// the partition stays not ready. Cut: `not_ready` answers `None` for a fenced status. Integration
/// (~4 s): a promote waits out node 2's grant.
#[test]
fn s3_a_current_fenced_owner_stays_not_ready() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let patience = crate::host::test_patience(Duration::from_secs(20));
    let (mut nodes, mut apps) = linked_apps(&rt, dir.path(), &store, 8, 2, patience);

    let (release, stall) = mpsc::channel();
    nodes[1]
        .send_to(NodeId(2), Msg::Stall(stall))
        .expect("a stall");
    apps[0]
        .promote_partition(PART2, NodeId(3), None)
        .unwrap_or_else(|error| panic!("a stalled owner looks dead: {error}"));
    release.send(()).expect("the node is stalled");

    let until = Instant::now() + patience;
    let fenced = loop {
        let seen = readiness_of(&apps[1], PART5);
        if matches!(seen.reason, Some(NotReady::Fenced(_))) {
            break seen;
        }
        assert!(Instant::now() < until, "node 2 never fenced: {seen:?}");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(!fenced.ready, "{fenced:?}");
    assert_eq!(fenced.owner, NodeId(2), "{fenced:?}");
    assert_eq!(
        apps[1].owner_of(PART5),
        Some(NodeId(2)),
        "it stays the owner"
    );
    // Partition 2 and every other partition ready first, so the wait below is about partition 5.
    until_ready(&apps[1], PART2);
    let refused = apps[1]
        .wait_ready(Duration::from_secs(2))
        .expect_err("a fenced owner does not become ready");
    assert!(
        matches!(
            refused,
            ReadyError::TimedOut {
                owner: NodeId(2),
                ..
            }
        ),
        "{refused:?}"
    );

    for db in nodes.iter_mut().chain(apps.iter_mut()) {
        db.shutdown();
    }
}

/// S3 acceptance finding 3: an app-only `Db` asking to move a partition off a live owner gets a
/// refusal the caller can match on, `PromoteError::OwnerAlive`, not only its text. Integration
/// (~2 s): node processes and an app over one set of links.
#[test]
fn s3_f3_a_live_owner_refusal_reaches_an_app_as_owner_alive() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let patience = crate::host::test_patience(Duration::from_secs(20));
    let mut formed = ready(&rt, dir.path(), &store, Some(8));
    formed.shutdown();
    drop(formed);

    let links = crate::transport::Links::new();
    let nodes: Vec<Db> = [1, 2, 3]
        .map(|node| {
            open_linked(&rt, dir.path(), &store, node, &links)
                .unwrap_or_else(|error| panic!("node {node} opens: {error}"))
        })
        .into();
    for db in &nodes {
        db.wait_ready(patience).expect("each leads its own");
    }
    let app_dir = config_testkit::fs::temp_dir();
    let app = rt
        .block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::clone(&links)),
                client: ClientId(2),
                ..config(app_dir.path(), None)
            },
            Arc::clone(&store),
            rt.handle().clone(),
            crate::host::test_budgets(),
        ))
        .expect("the app opens");
    app.wait_ready(patience).expect("the app sees every owner");

    let refused = app
        .promote_partition(PART2, NodeId(3), None)
        .expect_err("node 2 is alive");
    assert!(
        matches!(
            refused,
            admin::PromoteError::OwnerAlive {
                node: 2,
                partition: 2,
                ..
            }
        ),
        "{refused:?}"
    );
    nothing_written(&rt, &store, PART2, 2);
}

/// Node processes, each with its own `Db`: a peer's running `Db` promotes with the owner dead;
/// an app that opened before the move writes through it. Node 2 restarted before its other
/// partitions move fails its heal naming them; they still move, though it now answers, and once
/// it owns nothing its open needs no heal. Integration (~15 s): grant waits, real reopens.
#[test]
fn s3_a_peer_promotes_a_client_writes_across_it_and_the_old_owner_returns() {
    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let patience = crate::host::test_patience(Duration::from_secs(20));
    let mut formed = ready(&rt, dir.path(), &store, Some(8));
    formed.shutdown();
    drop(formed);

    let links = crate::transport::Links::new();
    let open = |node| {
        open_linked(&rt, dir.path(), &store, node, &links)
            .unwrap_or_else(|error| panic!("node {node} opens: {error}"))
    };
    let one = open(1);
    let mut two = open(2);
    let three = open(3);
    for db in [&one, &two, &three] {
        db.wait_ready(patience).expect("each leads its own");
    }
    let app_dir = config_testkit::fs::temp_dir();
    let app = rt
        .block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::clone(&links)),
                client: ClientId(2),
                ..config(app_dir.path(), None)
            },
            Arc::clone(&store),
            rt.handle().clone(),
            crate::host::test_budgets(),
        ))
        .expect("the app opens");
    app.wait_ready(patience).expect("the app sees every owner");
    let group = group_in(&app, PART2);
    app.put_in(group, b"before", b"1", None).expect("before");

    two.shutdown();
    drop(two);
    let promoted = one
        .promote_partition(PART2, NodeId(3), None)
        .unwrap_or_else(|error| panic!("a peer's promote: {error}"));
    assert_eq!(promoted.owner, NodeId(3), "{promoted:?}");

    assert_eq!(
        app.owner_of(PART2),
        Some(NodeId(2)),
        "the app has not heard"
    );
    let deadline = Instant::now() + patience;
    let written = loop {
        match app.put_in(group, b"after", b"2", None) {
            Ok(written) => break written,
            Err(error) => {
                assert!(Instant::now() < deadline, "{error:?}");
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    };
    assert_eq!(written.partition, PART2, "{written:?}");
    assert_eq!(app.owner_of(PART2), Some(NodeId(3)), "its route moved");
    let found = app.get_in(group, b"before").expect("a read").value;
    assert_eq!(found.map(|(_, bytes)| bytes.to_vec()), Some(b"1".to_vec()));
    assert_eq!(readiness_of(&app, PART5).reason, Some(NotReady::NoLink));

    let mut back = open(2);
    let healed = back
        .wait_ready(patience)
        .expect_err("it still owns 5 and 8");
    let text = healed.to_string();
    assert!(
        text.contains("is frozen") && text.contains("5 and 8"),
        "{text}"
    );
    for partition in [PART5, PART8] {
        one.promote_partition(partition, NodeId(3), None)
            .unwrap_or_else(|error| panic!("{partition:?}: {error}"));
    }
    back.shutdown();
    drop(back);
    let empty = open(2);
    empty
        .wait_ready(patience)
        .expect("it leads nothing now, so no heal");

    for mut db in [one, three, empty, app] {
        db.shutdown();
    }
}

/// What a copy durably holds, for the row's own node.
fn holding_of(db: &Db) -> crate::host::Holding {
    db.node_status()[0].holds.expect("a status holds a copy")
}

/// Node 3 of three node processes durably reaches `through`, and its record there is node 1's.
fn level_with_owner(three: &Db, through: (Generation, u64), patience: Duration) {
    let deadline = Instant::now() + patience;
    loop {
        let held = holding_of(three);
        if held.durable >= through.1 {
            return;
        }
        assert!(Instant::now() < deadline, "node 3 stayed short: {held:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Node 1's and node 3's records at `at`, read from their folders once both have shut down.
fn assert_digests_match(dir: &Path, at: (Generation, u64)) {
    let digest = |node: &str| {
        let engine = RocksEngine::open_existing(dir.join("nodes").join(node)).expect("a copy");
        super::tests::digest_at(&engine, at.0, Seq(at.1))
    };
    assert_eq!(digest("1"), digest("3"), "node 3 holds what node 1 holds");
}

/// Three node processes over `links`, where node 1 stopped holding a last record that each node
/// in `behind` missed. Returns nodes 2 and 3 and the record's `(generation, seq)`.
fn stopped_with_short_copies(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    links: &Arc<crate::transport::Links>,
    behind: &[u32],
) -> (Db, Db, (Generation, u64)) {
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let mut formed = ready(rt, dir, store, None);
    formed.shutdown();
    drop(formed);
    let open = |node| {
        open_linked(rt, dir, store, node, links)
            .unwrap_or_else(|error| panic!("node {node} opens: {error}"))
    };
    let mut one = open(1);
    let two = open(2);
    let three = open(3);
    for db in [&one, &two, &three] {
        db.wait_ready(patience).expect("the open heals");
    }
    one.put(b"a", b"1", None).expect("all three hold it");
    let before = holding_of(&one).applied;
    for node in behind {
        links.drop_direction(NodeId(1), NodeId(*node));
    }
    let answer = one.put(b"b", b"2", None);
    if behind.len() == 1 {
        answer.expect("nodes 1 and 2 hold it");
    }
    let held = holding_of(&one);
    assert!(held.applied > before, "node 1 did not apply: {held:?}");
    let at = (held.generation, held.applied);
    one.shutdown();
    drop(one);
    assert!(
        holding_of(&three).durable < at.1,
        "node 3 missed the record"
    );
    links.heal_all();
    (two, three, at)
}

/// D2 (S3 manual test): node 1 stops with a live copy one record short, and its open must bring
/// that copy level. The copy still pins node 1's old boot; F1's plan names the new one. One
/// partition, so it is not about partition count. Asserts the copy, not the route. Cut:
/// `sender_admitted` always reads the pin, red. Integration (~7 s): a real reopen.
#[test]
fn d2_a_restarted_owner_brings_a_live_copy_level_with_its_last_record() {
    let dir = config_testkit::fs::temp_dir();
    let (rt, store) = (runtime(), store());
    let links = crate::transport::Links::new();
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let (mut two, mut three, at) = stopped_with_short_copies(&rt, dir.path(), &store, &links, &[3]);
    let mut back = open_linked(&rt, dir.path(), &store, 1, &links).expect("node 1 opens");
    back.wait_ready(patience)
        .unwrap_or_else(|error| panic!("the restart is refused: {error}"));
    level_with_owner(&three, at, patience);
    back.put(b"c", b"3", None).expect("writes go on");
    for db in [&mut back, &mut two, &mut three] {
        db.shutdown();
    }
    drop((back, two, three));
    assert_digests_match(dir.path(), at);
}

/// S3b row 11: both copies missed the last record, so the restarted owner is the only source.
/// Node 2 stays up: with it dead this open would wait for it (D1, S4). Cut: `sender_admitted`
/// always reads the pin, red. Integration (~15 s): a put that gets no quorum, then a reopen.
#[test]
fn d2_c_the_restarted_owner_is_the_only_source_for_a_live_copy() {
    let dir = config_testkit::fs::temp_dir();
    let (rt, store) = (runtime(), store());
    let links = crate::transport::Links::new();
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let (mut two, mut three, at) =
        stopped_with_short_copies(&rt, dir.path(), &store, &links, &[2, 3]);
    assert!(holding_of(&two).durable < at.1, "node 2 missed the record");
    let mut back = open_linked(&rt, dir.path(), &store, 1, &links).expect("node 1 opens");
    back.wait_ready(patience)
        .unwrap_or_else(|error| panic!("the restart is refused: {error}"));
    level_with_owner(&three, at, patience);
    for db in [&mut back, &mut two, &mut three] {
        db.shutdown();
    }
    drop((back, two, three));
    assert_digests_match(dir.path(), at);
}

/// S3b row 13: the catch-up's frame beats its `Msg::Restore`, which is lost once. The frame is
/// refused `NotAMember`; the barrier's retry sends both again and the open completes, one
/// window late. Stands in for the design's "source on another node", which cannot be staged
/// here: a frame with no plan is refused the same way. Cut: `retry` re-sends nothing, red.
/// Integration (~10 s): a real reopen and one 2 s window.
#[config_log::retcd_test]
fn d2_d_a_frame_that_beats_its_plan_is_put_right_by_the_barrier_retry() {
    const METHOD: &str = "d2_d_a_frame_that_beats_its_plan_is_put_right_by_the_barrier_retry";
    let dir = config_testkit::fs::temp_dir();
    let (rt, store) = (runtime(), store());
    let links = crate::transport::Links::new();
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let (mut two, mut three, at) = stopped_with_short_copies(&rt, dir.path(), &store, &links, &[3]);
    links.drop_next_restore(NodeId(3));
    let mut back = open_linked(&rt, dir.path(), &store, 1, &links).expect("node 1 opens");
    back.wait_ready(patience)
        .unwrap_or_else(|error| panic!("the restart is refused: {error}"));
    level_with_owner(&three, at, patience);
    for db in [&mut back, &mut two, &mut three] {
        db.shutdown();
    }
    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let of = |name: &str| lines.iter().filter(|line| line["@m"] == name).count();
    assert_eq!(of("link_restore_dropped"), 1, "the plan was lost");
    assert!(of("catch_up_refused") >= 1, "the frame was refused first");
    assert!(
        lines
            .iter()
            .any(|line| line["@m"] == "barrier_retry" && line["attempt"] == 2),
        "the retry put it right"
    );
    drop((back, two, three));
    assert_digests_match(dir.path(), at);
}

/// #80, for the gateway: a note a recovery wrote on a partition other than 1 reads through
/// `node_status_in` on that partition and on no other, and a partition this `Db` does not have is
/// `INVALID_ARGUMENT`. Integration (~13 s): the promote waits out the dead owner's grant.
#[config_log::retcd_test]
fn node_status_in_shows_a_partitions_own_note_and_refuses_an_unknown_partition() {
    use rdb_core::contracts::recovery::NoteState;

    let dir = config_testkit::fs::temp_dir();
    let rt = runtime();
    let store = store();
    let mut db = ready(&rt, dir.path(), &store, Some(8));
    let moved = group_in(&db, PART2);
    db.put_in(moved, b"before", b"1", None).expect("before");
    stop_node(&db, 2);
    stop_node(&db, 1);
    db.promote_partition(PART2, NodeId(3), None)
        .unwrap_or_else(|error| panic!("nodes 1 and 2 are dead: {error}"));

    let entries = |partition: PartitionId| {
        let statuses = db.node_status_in(partition).expect("a partition it has");
        let status = statuses
            .iter()
            .find(|status| status.node == NodeId(3))
            .unwrap_or_else(|| panic!("node 3 answers: {statuses:?}"))
            .clone();
        match status.committed_without {
            NoteState::Known { note, .. } => note.entries,
            NoteState::Unknown => Vec::new(),
        }
    };
    let until = Instant::now() + crate::host::test_patience(Duration::from_secs(15));
    let note = loop {
        let note = entries(PART2);
        if !note.is_empty() {
            break note;
        }
        assert!(Instant::now() < until, "no note on partition 2");
        std::thread::sleep(Duration::from_millis(20));
    };
    let missing: Vec<NodeId> = note[0].copies.iter().map(|(_, node)| *node).collect();
    assert_eq!(missing, vec![NodeId(1), NodeId(2)], "{note:?}");
    assert_eq!(entries(PART3), Vec::new(), "partition 3 left nobody out");

    let unknown = db
        .node_status_in(PartitionId(99))
        .expect_err("no such partition");
    assert_eq!(unknown.kind, ErrorKind::InvalidArgument, "{unknown:?}");
    db.shutdown();
}
