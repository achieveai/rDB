//! Issue #147: tenant scopes. Four scopes whose names run into each other, A = `a`, B = `ab`,
//! C = `a` + sub `b` and D = `a` + sub `c`, try every path from one into another: ids, CAS,
//! transactions, lists past a range's end, deletes of every kind, chunks, request ids, resends
//! and status. Rows e–i, k, l and m build every handle on one affinity group
//! (`ScopedDb::__with_affinity`), so all four share one partition and one `(tenant, affinity)`
//! and the id prefix is all that tells them apart. Row j finds two real workspaces on one
//! partition. Every row is integration: one database open, about 1–5 s.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_api::{
    affinity_hash, partition_of, AffinityId, AsyncDb, Change, Db, DbConfig, ErrorKind, Limit,
    ListEdit, Look, ObjectKind, Scope, ScopedDb, ScopedWrite, Seen, Timeouts, TooLarge, TxnStatus,
    MAX_PAGE, MAX_SCOPED_ID, TENANT,
};
use rdb_core::contracts::txn::scoped_key;
use rdb_core::{Generation, Namespace, PartitionId, SnapshotHandle, SnapshotRead as _};
use rdb_value::collection::ElemOp;
use rdb_value::value::Value;
use rdb_value::Expected;
use sha2::{Digest as _, Sha256};

const PARTITIONS: u32 = 4;
/// The one affinity group rows e–i, k, l and m place every scope on.
const PIN: AffinityId = AffinityId(0x5150);
const DEADLINE: Duration = Duration::from_secs(5);

struct World {
    db: Arc<Db>,
    async_db: AsyncDb,
    rt: tokio::runtime::Runtime,
    path: std::path::PathBuf,
    _dir: tempfile_dir::Kept,
}

mod tempfile_dir {
    /// The data folder, kept alive with the database.
    pub struct Kept(#[allow(dead_code)] pub Box<dyn std::any::Any>);
}

fn world(ack_floor: bool) -> World {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let config = DbConfig {
        failover: None,
        partitions: Some(PARTITIONS),
        ack_floor,
        dir: dir.path().to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: rdb_api::db::ALL_NODES.to_vec(),
        links: None,
        client: rdb_api::db::DEFAULT_CLIENT,
        boot: None,
        storage: rdb_api::StorageOptions::default(),
        addresses: None,
    };
    let db = rt
        .block_on(Db::open(config, store, rt.handle().clone()))
        .expect("open");
    db.wait_ready(Duration::from_secs(30)).expect("ready");
    let db = Arc::new(db);
    World {
        async_db: AsyncDb::new(Arc::clone(&db)),
        db,
        rt,
        path: dir.path().to_path_buf(),
        _dir: tempfile_dir::Kept(Box::new(dir)),
    }
}

fn scopes() -> [Scope; 4] {
    [
        Scope::new(b"a", None).unwrap(),
        Scope::new(b"ab", None).unwrap(),
        Scope::new(b"a", Some(b"b")).unwrap(),
        Scope::new(b"a", Some(b"c")).unwrap(),
    ]
}

impl World {
    fn on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    /// A, B, C and D, every one placed on [`PIN`].
    fn pinned(&self) -> [ScopedDb; 4] {
        scopes().map(|scope| ScopedDb::__with_affinity(Arc::clone(&self.db), scope, PIN))
    }
}

fn b(bytes: &[u8]) -> Bytes {
    Bytes::copy_from_slice(bytes)
}

async fn value(scope: &ScopedDb, group: u64, id: &[u8]) -> Option<Bytes> {
    let got = scope.get(group, b(id)).await.expect("get");
    got.value.map(|(_, bytes)| bytes)
}

async fn version(scope: &ScopedDb, group: u64, id: &[u8]) -> u64 {
    let got = scope.get(group, b(id)).await.expect("get");
    got.value.expect("stored").0
}

async fn ids(scope: &ScopedDb, group: u64, prefix: &[u8], after: Option<&[u8]>) -> Vec<Bytes> {
    let page = scope
        .list(group, b(prefix), after.map(b), MAX_PAGE)
        .await
        .expect("list");
    page.entries.into_iter().map(|entry| entry.id).collect()
}

/// Every user record, of any kind (root, element, item, block or chunk), stored under raw key
/// prefix `prefix` in lineage `(partition, generation)`, read with the engine's own range read
/// (`SnapshotRead::scan`, as `rdb_value::repair` reads) from every copy that holds the lineage.
/// The `Db` must be shut down. Panics when no copy holds it, so an empty answer is never vacuous.
fn stored_under(
    dir: &std::path::Path,
    partition: PartitionId,
    generation: Generation,
    prefix: &[u8],
) -> Vec<Bytes> {
    let mut copies = 0;
    let mut keys = Vec::new();
    for entry in std::fs::read_dir(dir.join("nodes")).expect("the nodes folder") {
        let engine = rdb_storage::RocksEngine::open_read_only(entry.expect("an entry").path())
            .expect("read-only open");
        if !engine.lineages().contains(&(partition, generation)) {
            continue;
        }
        copies += 1;
        let view = engine
            .snapshot(partition, generation, SnapshotHandle(1))
            .expect("a view");
        keys.extend(
            view.scan(Namespace::User, prefix, 1_000_000)
                .into_iter()
                .map(|(key, _)| key)
                .take_while(|key| key.starts_with(prefix)),
        );
    }
    assert!(copies > 0, "no copy holds {partition:?} {generation:?}");
    keys
}

/// `scoped_key(TENANT, PIN, esc_body(S || group))`: the first bytes of every record of every
/// object of workspace `group` in `scope`, on [`PIN`].
fn raw_prefix(scope: &Scope, group: u64) -> Vec<u8> {
    let mut escaped = Vec::new();
    rdb_value::keys::esc(&scope.prefix(group), &mut escaped);
    escaped.truncate(escaped.len() - 2);
    scoped_key(TENANT, PIN, &escaped).to_vec()
}

/// #147 row e. Cut: build the stored id without S, and every get answers the last writer.
#[config_log::retcd_test]
fn sbx_147_e_four_scopes_one_affinity_one_group_one_id_four_values() {
    let w = world(true);
    let [a, b_, c, d] = w.pinned();
    w.on(async {
        for (scope, v) in [(&a, "A"), (&b_, "B"), (&c, "C"), (&d, "D")] {
            scope
                .put(0, b(b"doc"), b(v.as_bytes()), None)
                .await
                .expect("put");
        }
        let first = version(&a, 0, b"doc").await;
        a.put(0, b(b"doc"), b(b"A2"), Some(first))
            .await
            .expect("A's CAS at its own version");
        let a_now = version(&a, 0, b"doc").await;
        assert_ne!(a_now, version(&b_, 0, b"doc").await);
        // B's CAS at A's version is checked against B's own record only.
        let refused = b_
            .put(0, b(b"doc"), b(b"B2"), Some(a_now))
            .await
            .expect_err("B is not at A's version");
        assert_eq!(refused.error.kind, ErrorKind::ConditionFailed, "{refused}");
        for (scope, want) in [(&a, "A2"), (&b_, "B"), (&c, "C"), (&d, "D")] {
            assert_eq!(value(scope, 0, b"doc").await, Some(b(want.as_bytes())));
        }
    });
}

/// #147 row f. Cut: take an id that already starts with a scope header as that scope's id,
/// and A's write lands in C.
#[config_log::retcd_test]
fn sbx_147_f_an_id_spelling_another_scope_stays_inside_its_own() {
    let w = world(true);
    let [a, _, c, _] = w.pinned();
    let [_, _, scope_c, _] = scopes();
    w.on(async {
        c.put(0, b(b"doc"), b(b"C"), None).await.expect("put");
        let spelled = [scope_c.prefix(0), b"doc".to_vec()].concat();
        // Ids built of the escape bytes too: a zero, the escape terminator, an escaped zero.
        let odd: [&[u8]; 4] = [b"\x00", b"\x00\x01", b"\x00\xff", b"doc\x00\x01\x00"];
        a.put(0, b(&spelled), b(b"intruder"), None)
            .await
            .expect("put");
        for id in odd {
            a.put(0, b(id), b(b"odd"), None).await.expect("put");
        }
        assert_eq!(value(&c, 0, b"doc").await, Some(b(b"C")));
        assert_eq!(value(&a, 0, &spelled).await, Some(b(b"intruder")));
        assert_eq!(ids(&c, 0, b"", None).await, [b(b"doc")]);
        let mut want: Vec<_> = odd.iter().map(|id| b(id)).collect();
        want.push(b(&spelled));
        want.sort();
        assert_eq!(ids(&a, 0, b"", None).await, want);
    });
}

/// #147 row g. Group 0 and an empty prefix, the widest call a scope can make. After the delete
/// the `Db` is shut down and the store itself is read under C's raw key prefix: no root, element,
/// item or chunk of C survives, while A's record does. Cuts: delete documents only; skip objects
/// with no root; a walk that runs past its prefix; `delete_object` skips chunks (only the raw
/// scan sees that one, so it runs before the count).
#[config_log::retcd_test]
fn sbx_147_g_delete_prefix_with_an_empty_prefix_empties_one_scope_of_every_kind() {
    let w = world(true);
    let [a, b_, c, d] = w.pinned();
    let [_, _, scope_c, _] = scopes();
    let blob = b"0123456789";
    let sha: [u8; 32] = Sha256::digest(blob).into();
    let chunk = |upload, index: u32| Change::Chunk {
        upload,
        index,
        bytes: b(&blob[index as usize * 4..(index as usize * 4 + 4).min(blob.len())]),
    };
    let (partition, generation, deleted) = w.on(async {
        for at in (0..300).step_by(100) {
            let puts = (at..at + 100)
                .map(|i| ScopedWrite {
                    id: Bytes::from(format!("doc{i:03}")),
                    value: b(b"v"),
                    if_version: None,
                })
                .collect();
            c.txn(0, puts, DEADLINE).await.expect("100 docs");
        }
        for at in (0..1_200).step_by(200) {
            let ops = (at..at + 200)
                .map(|i| ElemOp::Put(Value::Text(format!("k{i}")), Value::Text("v".into())))
                .collect();
            c.write(0, b(b"map"), Change::Map(ops), None)
                .await
                .expect("map");
        }
        let push = ["x", "y", "z"].map(|v| ListEdit::Push(Value::Text(v.into())));
        c.write(0, b(b"list"), Change::List(push.to_vec()), None)
            .await
            .expect("list");
        for index in 0..3 {
            c.write(0, b(b"blob"), chunk([1; 16], index), None)
                .await
                .expect("chunk");
        }
        let publish = Change::Publish {
            upload: [1; 16],
            size: blob.len() as u64,
            chunk_size: 4,
            sha256: sha,
        };
        c.write(0, b(b"blob"), publish, None)
            .await
            .expect("publish");
        c.write(0, b(b"orphan"), chunk([2; 16], 0), None)
            .await
            .expect("an upload never published");
        let again = c.write(0, b(b"orphan"), chunk([2; 16], 0), None).await;
        assert!(
            matches!(again, Ok(None)),
            "control: the chunk is stored: {again:?}"
        );
        for scope in [&a, &b_, &d] {
            scope
                .put(0, b(b"doc000"), b(b"keep"), None)
                .await
                .expect("put");
        }

        let deleted = c
            .delete_prefix(0, Bytes::new(), DEADLINE)
            .await
            .unwrap_or_else(|e| panic!("delete_prefix: {} after {}", e.error, e.deleted));

        assert!(ids(&c, 0, b"", None).await.is_empty());
        let root =
            w.db.list_in(PIN, &scope_c.prefix(0), None, MAX_PAGE)
                .expect("list");
        assert!(root.entries.is_empty(), "{root:?}");
        let map = c
            .read(
                0,
                b(b"map"),
                Look::Members {
                    after: None,
                    limit: 10,
                },
            )
            .await
            .expect("read");
        assert_eq!(map.seen, Seen::Absent);
        for scope in [&a, &b_, &d] {
            assert_eq!(value(scope, 0, b"doc000").await, Some(b(b"keep")));
        }
        let route = w.async_db.route(PIN).await.expect("route");
        (route.partition, root.generation, deleted)
    });

    // No record of C survives, of any kind: read the store itself, not through a list.
    let World {
        db,
        async_db,
        rt,
        path,
        _dir,
        ..
    } = w;
    drop((a, b_, c, d, async_db));
    let db = Arc::try_unwrap(db).unwrap_or_else(|_| panic!("a handle is still alive"));
    drop(db);
    let [scope_a, _, scope_c, _] = scopes();
    let left = stored_under(&path, partition, generation, &raw_prefix(&scope_c, 0));
    assert!(
        left.is_empty(),
        "{} records of C survive: {left:02x?}",
        left.len()
    );
    let control = stored_under(&path, partition, generation, &raw_prefix(&scope_a, 0));
    assert!(!control.is_empty(), "control: the scan sees A's record");
    assert_eq!(
        deleted,
        300 + 4,
        "300 docs, a map, a list, a blob and an orphan upload"
    );
    drop(rt);
}

/// #147 row h. Cut: a walk that does not stop at its prefix. D, next after C in byte order, and
/// C's workspace 1, next after its workspace 0, share the affinity group.
#[config_log::retcd_test]
fn sbx_147_h_list_ranges_end_at_a_scopes_last_key() {
    let w = world(true);
    let [a, _, c, d] = w.pinned();
    w.on(async {
        for id in [&b"x1"[..], b"x2"] {
            c.put(0, b(id), b(b"C"), None).await.expect("put");
        }
        c.put(1, b(b"x0"), b(b"C1"), None).await.expect("put");
        d.put(0, b(b"x1"), b(b"D"), None).await.expect("put");
        d.put(0, b(b"zz"), b(b"D"), None).await.expect("put");
        a.put(0, b(b"x9"), b(b"A"), None).await.expect("put");

        assert_eq!(ids(&c, 0, b"", None).await, [b(b"x1"), b(b"x2")]);
        assert!(ids(&c, 0, b"", Some(b"x2")).await.is_empty());
        assert!(ids(&c, 0, b"", Some(b"\xff\xff")).await.is_empty());
        assert!(ids(&c, 0, b"x2\xff", None).await.is_empty());
        assert_eq!(ids(&c, 1, b"", None).await, [b(b"x0")]);
        assert_eq!(ids(&a, 0, b"", None).await, [b(b"x9")]);
        // Paging hands back ids inside the scope.
        let first = c.list(0, Bytes::new(), None, 1).await.expect("list");
        assert_eq!(first.next, Some(b(b"x1")));
        let second = c.list(0, Bytes::new(), first.next, 1).await.expect("list");
        assert_eq!(second.entries[0].id, b(b"x2"));
        assert_eq!(second.entries[0].kind, ObjectKind::Document);
    });
}

/// #147 row i. Cut: a chunk key without S, and C's chunk is found already stored, and D's
/// publish finds A's chunks.
#[config_log::retcd_test]
fn sbx_147_i_chunks_never_dedup_across_scopes() {
    let w = world(true);
    let [a, _, c, d] = w.pinned();
    let bytes = b(b"abcd");
    let chunk = || Change::Chunk {
        upload: [5; 16],
        index: 0,
        bytes: bytes.clone(),
    };
    w.on(async {
        let in_a = a
            .write(0, b(b"blob"), chunk(), None)
            .await
            .expect("A's chunk");
        let in_c = c
            .write(0, b(b"blob"), chunk(), None)
            .await
            .expect("C's chunk");
        assert!(
            in_a.is_some() && in_c.is_some(),
            "each scope stores its own"
        );
        let sha: [u8; 32] = Sha256::digest(&bytes).into();
        let publish = Change::Publish {
            upload: [5; 16],
            size: 4,
            chunk_size: 4,
            sha256: sha,
        };
        let refused = d
            .write(0, b(b"blob"), publish, None)
            .await
            .expect_err("D has no chunks of its own");
        assert_eq!(refused.error.kind, ErrorKind::InvalidArgument, "{refused}");
        assert!(refused.error.detail.contains("chunk"), "{refused}");
    });
}

/// #147 row j. Cut: send the app's number without the scope's hash, and the two scopes' `n = 1`
/// share one request id: C's status answers A's write.
#[config_log::retcd_test]
fn sbx_147_j_reused_app_numbers_in_two_scopes_on_one_partition() {
    let w = world(false);
    let [scope_a, _, scope_c, _] = scopes();
    let (a, c) = (w.async_db.scoped(scope_a), w.async_db.scoped(scope_c));
    let partition = |scope: &ScopedDb, group| {
        partition_of(affinity_hash(TENANT, scope.affinity(group)), PARTITIONS)
    };
    let (g1, g2) = (0..64)
        .flat_map(|g1| (0..64).map(move |g2| (g1, g2)))
        .find(|&(g1, g2)| g1 != g2 && partition(&a, g1) == partition(&c, g2))
        .expect("two workspaces on one partition");
    assert_eq!(partition(&a, g1), partition(&c, g2));
    let write = |v: &str| {
        vec![ScopedWrite {
            id: b(b"doc"),
            value: Bytes::from(v.to_owned()),
            if_version: None,
        }]
    };
    w.on(async {
        let in_a = a.txn_as(g1, 1, write("A"), DEADLINE).await.expect("A n=1");
        let in_c = c.txn_as(g2, 1, write("C"), DEADLINE).await.expect("C n=1");
        assert_ne!(in_a.ok.seq, in_c.ok.seq);
        for (scope, group, ok) in [(&a, g1, &in_a), (&c, g2, &in_c)] {
            match scope
                .status(group, 1, Some(ok.ok.generation))
                .await
                .expect("status")
            {
                TxnStatus::Resolved(result) => assert_eq!(result.seq, ok.ok.seq),
                other => panic!("its own write is resolved: {other:?}"),
            }
        }
        let started = Instant::now();
        let (one, two) = tokio::join!(
            a.txn_as(g1, 7, write("A7"), DEADLINE),
            c.txn_as(g2, 7, write("C7"), DEADLINE)
        );
        let (one, two) = (one.expect("A n=7"), two.expect("C n=7"));
        assert_ne!(one.ok.seq, two.ok.seq);
        assert!(started.elapsed() < DEADLINE, "{:?}", started.elapsed());
        assert_eq!(value(&a, g1, b"doc").await, Some(b(b"A7")));
        assert_eq!(value(&c, g2, b"doc").await, Some(b(b"C7")));
    });
}

/// #147 row k. Cut: compare the group, not the scope, and C resends A's request.
#[config_log::retcd_test]
fn sbx_147_k_resend_accepts_only_this_scopes_token() {
    let w = world(true);
    let [a, _, c, _] = w.pinned();
    w.on(async {
        let sent = a.put(0, b(b"doc"), b(b"A"), None).await.expect("put").sent;
        let refused = c
            .resend_within(sent.clone(), DEADLINE)
            .await
            .expect_err("another scope's token");
        assert_eq!(refused.error.kind, ErrorKind::InvalidArgument, "{refused}");
        assert!(refused.sent.is_none());
        let asked = c.status_of(&sent).await.expect_err("another scope's token");
        assert_eq!(asked.kind, ErrorKind::InvalidArgument, "{asked}");
        assert_eq!(value(&c, 0, b"doc").await, None);
        let again = a
            .resend_within(sent.clone(), DEADLINE)
            .await
            .expect("A's own");
        assert!(
            again.ok.sent.mutations.is_empty(),
            "no keys leave the handle"
        );
        assert!(matches!(
            a.status_of(&sent).await.expect("status"),
            TxnStatus::Resolved(_) | TxnStatus::Expired
        ));
    });
}

/// #147 row l. Cut: give each scope `1024 - |S|`, and the long scope's maximum differs.
#[config_log::retcd_test]
fn sbx_147_l_the_scoped_id_budget_is_one_constant() {
    let w = world(true);
    let longest = Scope::new(&[b'x'; 64], Some(&[b'y'; 64])).unwrap();
    let short = Scope::new(b"a", None).unwrap();
    let blob = b"abcd";
    let sha: [u8; 32] = Sha256::digest(blob).into();
    w.on(async {
        for scope in [short, longest] {
            let s = ScopedDb::__with_affinity(Arc::clone(&w.db), scope, PIN);
            let id = |tail: u8, len| {
                let mut id = vec![tail; len];
                id[0] = tail;
                Bytes::from(id)
            };
            s.put(0, id(b'd', MAX_SCOPED_ID), b(b"v"), None)
                .await
                .expect("a document at the longest id");
            let put = ElemOp::Put(Value::Text("k".into()), Value::Text("v".into()));
            s.write(0, id(b'm', MAX_SCOPED_ID), Change::Map(vec![put]), None)
                .await
                .expect("a map at the longest id");
            let blob_id = id(b'b', MAX_SCOPED_ID);
            let chunk = Change::Chunk {
                upload: [9; 16],
                index: 0,
                bytes: b(blob),
            };
            s.write(0, blob_id.clone(), chunk, None)
                .await
                .expect("chunk");
            let publish = Change::Publish {
                upload: [9; 16],
                size: 4,
                chunk_size: 4,
                sha256: sha,
            };
            s.write(0, blob_id, publish, None)
                .await
                .expect("a blob at the longest id");
            let over = TooLarge {
                limit: Limit::ObjectId,
                size: MAX_SCOPED_ID + 1,
                max: MAX_SCOPED_ID,
            };
            let refused = s
                .put(0, id(b'd', MAX_SCOPED_ID + 1), b(b"v"), None)
                .await
                .expect_err("one byte over");
            assert_eq!(refused.error.too_large(), Some(over), "{refused}");
            let read = s
                .get(0, id(b'd', MAX_SCOPED_ID + 1))
                .await
                .expect_err("one byte over");
            assert_eq!(read.too_large(), Some(over), "{read}");
        }
    });
}

/// #147 row m. Cut: compile a delete without the root's version, and the stale delete lands.
#[config_log::retcd_test]
fn sbx_147_m_txn_cas_and_delete_cas_on_one_group() {
    let w = world(true);
    let [a, ..] = w.pinned();
    let put = |id: &[u8], if_version| ScopedWrite {
        id: b(id),
        value: b(b"new"),
        if_version,
    };
    w.on(async {
        a.txn(
            0,
            vec![put(b"x", None), put(b"y", None), put(b"z", None)],
            DEADLINE,
        )
        .await
        .expect("create");
        let (x, y, z) = (
            version(&a, 0, b"x").await,
            version(&a, 0, b"y").await,
            version(&a, 0, b"z").await,
        );
        a.put(0, b(b"y"), b(b"moved"), Some(y))
            .await
            .expect("y moves on");
        let refused = a
            .txn(
                0,
                vec![put(b"x", Some(x)), put(b"y", Some(y)), put(b"z", Some(z))],
                DEADLINE,
            )
            .await
            .expect_err("y is stale");
        assert_eq!(refused.error.kind, ErrorKind::ConditionFailed, "{refused}");
        assert_eq!(value(&a, 0, b"x").await, Some(b(b"new")));
        assert_eq!(version(&a, 0, b"x").await, x, "none of the three landed");
        assert_eq!(value(&a, 0, b"y").await, Some(b(b"moved")));

        let stale = a
            .write(0, b(b"y"), Change::Delete, Some(Expected::Version(y)))
            .await
            .expect_err("a delete at y's old version");
        assert_eq!(stale.error.kind, ErrorKind::ConditionFailed, "{stale}");
        assert_eq!(value(&a, 0, b"y").await, Some(b(b"moved")));
        let now = version(&a, 0, b"y").await;
        let done = a
            .write(0, b(b"y"), Change::Delete, Some(Expected::Version(now)))
            .await
            .expect("a delete at y's version");
        assert!(done.is_some());
        assert_eq!(value(&a, 0, b"y").await, None);
        let nothing = a.write(0, b(b"y"), Change::Delete, None).await;
        assert!(matches!(nothing, Ok(None)), "{nothing:?}");
    });
}

/// #147 row n. Cut: place by S alone, and one scope's workspaces all land on one partition.
#[config_log::retcd_test]
fn sbx_147_n_real_hash_spreads_a_scopes_groups() {
    let w = world(true);
    let a = w
        .async_db
        .scoped(Scope::new(b"acme", Some(b"tenant-1")).unwrap());
    let partitions: std::collections::BTreeSet<_> = (0..64)
        .map(|g| partition_of(affinity_hash(TENANT, a.affinity(g)), PARTITIONS))
        .collect();
    assert_eq!(partitions.len(), PARTITIONS as usize, "{partitions:?}");
    w.on(async {
        for g in 0..4 {
            let route = w.async_db.route(a.affinity(g)).await.expect("route");
            assert_eq!(
                route.partition,
                partition_of(affinity_hash(TENANT, a.affinity(g)), PARTITIONS)
            );
        }
    });
}

/// Shut `w` down and read every record of scope `scope`'s workspace `group` left in the store,
/// as row g does: the engine's own range read, not a list.
fn left_after(w: World, scope: &Scope, group: u64) -> Vec<Bytes> {
    let partition = w.on(async { w.async_db.route(PIN).await.expect("route").partition });
    let generation = w.db.list_in(PIN, &scope.prefix(group), None, 1).expect("list").generation;
    let World {
        db,
        async_db,
        rt,
        path,
        _dir,
    } = w;
    drop(async_db);
    let db = Arc::try_unwrap(db).unwrap_or_else(|_| panic!("a handle is still alive"));
    drop(db);
    let left = stored_under(&path, partition, generation, &raw_prefix(scope, group));
    drop(rt);
    left
}

/// Write `chunks` one-byte upload chunks under document `id` of scope `c`, workspace 0.
async fn chunked_doc(c: &ScopedDb, id: &[u8], chunks: u32) {
    c.put(0, b(id), b(b"doc"), None).await.expect("the document");
    for index in 0..chunks {
        let chunk = Change::Chunk {
            upload: [7; 16],
            index,
            bytes: b(b"x"),
        };
        c.write(0, b(id), chunk, None).await.expect("a chunk");
    }
}

/// PR #178 review r1 T1: a document with 255 upload chunks is one more record than a request
/// deletes, so its first batch keeps the root and a chunk. Delete prefix drains it, counts it
/// once, and leaves nothing. Cut: count a document deleted after one batch (send each pack as
/// compiled, without the root check): red, the root and a chunk survive.
#[config_log::retcd_test]
fn sbx_147_o_delete_prefix_drains_a_document_with_255_chunks() {
    let w = world(true);
    let [_, _, c, _] = w.pinned();
    let deleted = w.on(async {
        chunked_doc(&c, b"big", 255).await;
        c.delete_prefix(0, Bytes::new(), DEADLINE)
            .await
            .unwrap_or_else(|e| panic!("delete_prefix: {} after {}", e.error, e.deleted))
    });
    drop(c);
    let [_, _, scope_c, _] = scopes();
    let left = left_after(w, &scope_c, 0);
    assert!(left.is_empty(), "{} records survive: {left:02x?}", left.len());
    assert_eq!(deleted, 1, "one document");
}

/// PR #178 review r1 T2: two documents with 128 chunks each are 258 deletes, over the 255 a
/// request carries, so they cannot go in one transaction. Delete prefix splits them by cost and
/// counts both. Cut: send each pack of documents as compiled: red, admission refuses it.
#[config_log::retcd_test]
fn sbx_147_p_delete_prefix_splits_documents_by_their_cost() {
    let w = world(true);
    let [_, _, c, _] = w.pinned();
    let deleted = w.on(async {
        chunked_doc(&c, b"one", 128).await;
        chunked_doc(&c, b"two", 128).await;
        c.delete_prefix(0, Bytes::new(), DEADLINE)
            .await
            .unwrap_or_else(|e| panic!("delete_prefix: {} after {}", e.error, e.deleted))
    });
    drop(c);
    let [_, _, scope_c, _] = scopes();
    let left = left_after(w, &scope_c, 0);
    assert!(left.is_empty(), "{} records survive: {left:02x?}", left.len());
    assert_eq!(deleted, 2, "two documents");
}
