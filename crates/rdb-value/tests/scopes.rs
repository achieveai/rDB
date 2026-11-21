//! Tenant scopes, the object list and the ordinary delete at the library API (#147).
//!
//! Scenario: a gateway stores each (app, sub-tenant) scope's objects under a header no other
//! scope's header can start or end inside, lists one workspace's objects by id, and deletes
//! any object of any kind, each batch leaving a valid object or nothing a read refuses.

mod common;

use bytes::Bytes;
use common::{int, text, Kernel};
use rdb_core::{AffinityId, Condition, Generation, Namespace, SnapshotRead, TenantId};
use rdb_value::blob::{publish, put_chunk, Upload};
use rdb_value::collection::{compile_collection, CollectionKind, ElemOp};
use rdb_value::delta::{ApplyError, Delta, Op};
use rdb_value::envelope::Kind;
use rdb_value::keys::{
    list_objects, root_key, scope_affinity, scoped_request, Listed, RootKey, Scope, ScopeError,
    MAX_SCOPE_PART,
};
use rdb_value::list::{create_list, ListOp, DEFAULT_BLOCK_MAX};
use rdb_value::repair::delete_object;
use rdb_value::testing::CountingSnapshot;
use rdb_value::{compile, read, Expected, ValueError};
use sha2::{Digest as _, Sha256};

const T: TenantId = TenantId(1);
const A: AffinityId = AffinityId(9);

fn scope(app: &[u8], sub: Option<&[u8]>) -> Scope {
    Scope::new(app, sub).expect("a valid scope")
}

fn id(prefix: &[u8], tail: &[u8]) -> Vec<u8> {
    [prefix, tail].concat()
}

/// Every byte string over `alphabet` of length 1 to `max`.
fn strings(alphabet: &[u8], max: usize) -> Vec<Vec<u8>> {
    let mut all: Vec<Vec<u8>> = alphabet.iter().map(|b| vec![*b]).collect();
    let mut last = all.clone();
    for _ in 1..max {
        last = last
            .iter()
            .flat_map(|s| alphabet.iter().map(move |b| [s.as_slice(), &[*b]].concat()))
            .collect();
        all.extend(last.iter().cloned());
    }
    all
}

/// In a sorted list, a string that is a prefix of a later one is a prefix of the very next one,
/// since everything between them starts with it too.
fn assert_prefix_free(mut headers: Vec<Vec<u8>>, what: &str) {
    let n = headers.len();
    headers.sort();
    headers.dedup();
    assert_eq!(headers.len(), n, "{what}: two scopes share a header");
    for pair in headers.windows(2) {
        assert!(
            !pair[1].starts_with(&pair[0]),
            "{what}: {:02x?} is a prefix of {:02x?}",
            pair[0],
            pair[1]
        );
    }
}

/// Lemma 1. Cut: drop the `0x00` that ends a header with no sub-tenant, and `a` is a prefix of
/// `a`+`b`.
#[test]
fn scope_headers_are_prefix_free() {
    let parts = strings(&[0x00, 0x01, 0x61, 0x62, 0xFF], 3);
    let mut scopes = Vec::new();
    for app in &parts {
        scopes.push(scope(app, None));
        for sub in &parts {
            scopes.push(scope(app, Some(sub)));
        }
    }
    assert_eq!(scopes.len(), 155 * 156);
    assert_prefix_free(scopes.iter().map(|s| s.as_bytes().to_vec()).collect(), "S");
    for group in [0, 1, u64::MAX] {
        assert_prefix_free(
            scopes.iter().map(|s| s.prefix(group)).collect(),
            "S || group",
        );
    }
    // Two groups of one scope too: the group is a fixed 8 bytes.
    let one = scope(b"a", None);
    assert_prefix_free(
        (0..64).map(|g| one.prefix(g)).collect(),
        "one scope's groups",
    );
    // The named boundary pairs.
    for (left, right) in [
        (scope(b"a", None), scope(b"ab", None)),
        (scope(b"a", Some(b"b")), scope(b"ab", None)),
        (scope(b"a", None), scope(b"a", Some(b"b"))),
        (scope(b"a\x00", None), scope(b"a", Some(b"\x00"))),
    ] {
        assert!(
            !left.as_bytes().starts_with(right.as_bytes())
                && !right.as_bytes().starts_with(left.as_bytes()),
            "{left:?} vs {right:?}"
        );
    }
}

/// Cut: accept `Some("")` as a sub-tenant.
#[test]
fn scope_refuses_an_empty_app_an_empty_sub_and_65_bytes() {
    let longest = [b'x'; MAX_SCOPE_PART];
    let over = [b'x'; MAX_SCOPE_PART + 1];
    assert_eq!(Scope::new(b"", None), Err(ScopeError::EmptyApp));
    assert_eq!(Scope::new(b"", Some(b"t")), Err(ScopeError::EmptyApp));
    assert_eq!(Scope::new(b"a", Some(b"")), Err(ScopeError::EmptySub));
    assert_eq!(
        Scope::new(&over, None),
        Err(ScopeError::AppTooLong { len: 65 })
    );
    assert_eq!(
        Scope::new(b"a", Some(&over)),
        Err(ScopeError::SubTooLong { len: 65 })
    );
    let both = scope(&longest, Some(&longest));
    assert_eq!(both.as_bytes().len(), 131);
    assert_eq!(scope(b"a", None).as_bytes(), b"\x01a\x00");
    assert_eq!(scope(b"a", Some(b"b")).as_bytes(), b"\x01a\x01\x01b");
}

/// Frozen: the scope bytes and both hashes are stored or sent, so they never change. Computed
/// apart from this code, with Python's `hashlib`. Cut: change a domain string.
#[test]
fn scope_affinity_and_request_id_known_answers() {
    let cases: [(Scope, u64, u64, u64, u64); 6] = [
        (
            scope(b"a", None),
            0,
            0x408a_8b49_a622_63d4,
            0x6570_2cdb_ed73_176c,
            0x7146_6794_ecd6_e8b1,
        ),
        (
            scope(b"a", None),
            u64::MAX,
            0x9657_202b_d5b3_a9f5,
            0x2a11_549e_e7a7_ea72,
            0x480a_cef3_e5db_b3cb,
        ),
        (
            scope(b"ab", None),
            1,
            0xc87e_1cd7_62ab_7f15,
            0x49ba_79f8_04b9_9a12,
            0x790a_1c8a_c030_8c66,
        ),
        (
            scope(b"a", Some(b"b")),
            1,
            0x384c_65d3_d61d_6f09,
            0x3cd4_aa89_fbe5_5b05,
            0x009c_c29e_f4d4_e97c,
        ),
        (
            scope(b"acme", Some(b"9f1c")),
            0,
            0x7cb5_97d2_ef16_a9d4,
            0x2846_6bc3_1af3_8548,
            0x604d_6286_ee9e_d906,
        ),
        (
            scope(b"acme", Some(b"9f1c")),
            u64::MAX,
            0xa916_9298_ac36_937d,
            0x590b_9c73_61bb_b454,
            0x039f_8e3f_3c2d_6754,
        ),
    ];
    for (s, group, affinity, n0, n7) in cases {
        assert_eq!(
            scope_affinity(&s, group),
            AffinityId(affinity),
            "{s:?} {group}"
        );
        assert_eq!(scoped_request(&s, group, 0), n0, "{s:?} {group} n=0");
        assert_eq!(scoped_request(&s, group, 7), n7, "{s:?} {group} n=7");
    }
    assert_eq!(
        scope(b"acme", Some(b"9f1c")).as_bytes(),
        b"\x04acme\x01\x049f1c"
    );
}

fn doc(k: &mut Kernel, root: &RootKey, v: &str) {
    let s = k.snapshot();
    let expected = s
        .version(Namespace::User, root.as_bytes())
        .map_or(Expected::Absent, Expected::Version);
    let made = compile(&s, root, expected, &Delta(vec![Op::Replace(text(v))])).unwrap();
    k.commit(&made);
}

fn map(k: &mut Kernel, root: &RootKey, entries: i128) {
    let mut done = 0;
    while done < entries {
        let s = k.snapshot();
        let expected = s
            .version(Namespace::User, root.as_bytes())
            .map_or(Expected::Absent, Expected::Version);
        let ops: Vec<_> = (done..entries.min(done + 200))
            .map(|i| ElemOp::Put(int(i), int(i)))
            .collect();
        done += ops.len() as i128;
        let made = compile_collection(&s, root, CollectionKind::Map, expected, &ops).unwrap();
        k.commit(&made);
    }
}

fn list(k: &mut Kernel, root: &RootKey) {
    let push: Vec<_> = ["x", "y", "z"]
        .iter()
        .map(|v| ListOp::Push(text(v)))
        .collect();
    let made = create_list(&k.snapshot(), root, false, DEFAULT_BLOCK_MAX, &push).unwrap();
    k.commit(made.compiled());
}

const BLOB: &[u8] = b"0123456789";

fn chunks(k: &mut Kernel, root: &RootKey, upload: &Upload) {
    for (index, piece) in (0..).zip(BLOB.chunks(4)) {
        k.commit(&put_chunk(&k.snapshot(), root, upload, index, piece).unwrap());
    }
}

fn blob(k: &mut Kernel, root: &RootKey) {
    chunks(k, root, &[7; 16]);
    let sha: [u8; 32] = Sha256::digest(BLOB).into();
    let made = publish(
        &k.snapshot(),
        root,
        Expected::Absent,
        &[7; 16],
        BLOB.len() as u64,
        4,
        &sha,
        Generation(k.generation),
    )
    .unwrap();
    k.commit(&made);
}

fn ids(listed: &[Listed]) -> Vec<Vec<u8>> {
    listed.iter().map(|l| l.id.clone()).collect()
}

/// Cut: step to the next record instead of the next object, and the map's 1,500 entries cost
/// 1,500 seeks.
#[test]
fn list_roots_walks_roots_only_and_stops_at_the_range_end() {
    let mut k = Kernel::new();
    let s1 = scope(b"a", None).prefix(5);
    // The next header in byte order after `01 61 00`, under the same (T, A).
    let s2 = scope(b"a", Some(b"b")).prefix(5);
    let at = |prefix: &[u8], tail: &[u8]| root_key(T, A, &id(prefix, tail));
    doc(&mut k, &at(&s1, b"doc"), "d");
    map(&mut k, &at(&s1, b"map"), 1_500);
    list(&mut k, &at(&s1, b"list"));
    blob(&mut k, &at(&s1, b"blob"));
    chunks(&mut k, &at(&s1, b"orphan"), &[3; 16]);
    doc(&mut k, &at(&s2, b"doc"), "other");
    let snap = k.snapshot();

    let counted = CountingSnapshot::new(&snap);
    let (page, full) = list_objects(&counted, T, A, &s1, None, 1_000, false).unwrap();
    let want: Vec<_> = [b"blob" as &[u8], b"doc", b"list", b"map"]
        .iter()
        .map(|t| id(&s1, t))
        .collect();
    assert_eq!(ids(&page), want);
    assert!(!full);
    let kinds: Vec<_> = page.iter().map(|l| l.root.map(|(_, kind)| kind)).collect();
    assert_eq!(
        kinds,
        [Kind::Blob, Kind::Document, Kind::List, Kind::Map].map(Some)
    );
    // One seek per object, the 1,500 map entries and the orphan's chunks included, plus the
    // seek that finds the range's end and one version read per root.
    assert!(counted.calls() <= 5 + 1 + 4, "{} calls", counted.calls());

    // The orphan's chunks are an object with no root: listed only when asked for.
    let (all, _) = list_objects(&snap, T, A, &s1, None, 1_000, true).unwrap();
    let orphan = all.iter().find(|l| l.id == id(&s1, b"orphan")).unwrap();
    assert_eq!(orphan.root, None);
    assert_eq!(all.len(), 5);

    // `after` each returned id resumes right past it.
    for (i, after) in want.iter().enumerate() {
        let (rest, _) = list_objects(&snap, T, A, &s1, Some(after), 1_000, false).unwrap();
        assert_eq!(ids(&rest), want[i + 1..], "after {after:02x?}");
    }
    // `after` below the prefix starts at the prefix.
    let (from_start, _) = list_objects(&snap, T, A, &s1, Some(b"\x00"), 1_000, false).unwrap();
    assert_eq!(ids(&from_start), want);
    // A full page says so.
    let (two, full) = list_objects(&snap, T, A, &s1, None, 2, false).unwrap();
    assert_eq!((ids(&two), full), (want[..2].to_vec(), true));
    // Past the last id, and past the range: nothing of S2.
    let last = want.last().unwrap();
    let (none, _) = list_objects(&snap, T, A, &s1, Some(last), 1_000, false).unwrap();
    assert!(none.is_empty(), "{none:?}");
    let beyond = id(last, b"\xff\xff");
    let (none, _) = list_objects(&snap, T, A, &beyond, None, 1_000, false).unwrap();
    assert!(none.is_empty(), "{none:?}");
    let (empty_prefix, _) = list_objects(&snap, T, A, &s1, None, 1_000, true).unwrap();
    assert!(empty_prefix.iter().all(|l| l.id.starts_with(&s1)));
}

/// Cut: drop the group from `S || group`, and the two workspaces share one key.
#[test]
fn two_groups_of_one_scope_under_one_affinity_hold_two_values() {
    let mut k = Kernel::new();
    let s = scope(b"a", None);
    let one = root_key(T, A, &id(&s.prefix(1), b"x"));
    let two = root_key(T, A, &id(&s.prefix(2), b"x"));
    doc(&mut k, &one, "one");
    doc(&mut k, &two, "two");
    let snap = k.snapshot();
    assert_eq!(read(&snap, &one).unwrap().unwrap().value, text("one"));
    assert_eq!(read(&snap, &two).unwrap().unwrap().value, text("two"));
}

/// Cuts: delete the root with the first batch; skip the root's version check.
#[test]
fn delete_empties_any_kind_root_last_and_checks_its_version() {
    let mut k = Kernel::new();
    let big = root_key(T, A, b"big");
    map(&mut k, &big, 600);
    doc(&mut k, &root_key(T, A, b"bigger"), "neighbour");
    let stale = k
        .snapshot()
        .version(Namespace::User, big.as_bytes())
        .unwrap()
        - 1;
    assert!(matches!(
        delete_object(&k.snapshot(), &big, Some(stale)),
        Err(ValueError::Apply(ApplyError::VersionConflict { .. }))
    ));
    let mut batches = 0;
    while let Some(batch) = delete_object(&k.snapshot(), &big, None).unwrap() {
        batches += 1;
        k.commit_any_order(&batch);
        let root_left = k.records.contains_key(big.as_bytes());
        let left = k
            .records
            .keys()
            .filter(|key| key.starts_with(big.object_prefix()))
            .count();
        // The root goes in the batch that empties the rest, never before.
        assert_eq!(root_left, left > 0, "batch {batches}: {left} records left");
    }
    assert_eq!(
        batches, 3,
        "600 entries at 254 a batch, the root in the last"
    );
    assert!(read(&k.snapshot(), &root_key(T, A, b"bigger"))
        .unwrap()
        .is_some());

    // A document is one batch; chunks with no root go under the root's absence.
    let one = root_key(T, A, b"one");
    doc(&mut k, &one, "v");
    let batch = delete_object(&k.snapshot(), &one, None).unwrap().unwrap();
    assert_eq!(batch.mutations.len(), 1);
    k.commit_any_order(&batch);
    let orphan = root_key(T, A, b"orphan");
    chunks(&mut k, &orphan, &[3; 16]);
    let batch = delete_object(&k.snapshot(), &orphan, None)
        .unwrap()
        .unwrap();
    assert_eq!(
        batch.conditions,
        [Condition::Absent {
            key: Bytes::copy_from_slice(orphan.as_bytes())
        }]
    );
    k.commit_any_order(&batch);
    assert_eq!(delete_object(&k.snapshot(), &orphan, None).unwrap(), None);
}

/// Delete `root` to the end, batch by batch, and check that nothing is stored under it.
fn finish_delete(k: &mut Kernel, root: &RootKey) {
    let mut batches = 0;
    while let Some(batch) = delete_object(&k.snapshot(), root, None).unwrap() {
        batches += 1;
        assert!(batches < 20, "the delete ends");
        k.commit_any_order(&batch);
    }
    let left = k
        .records
        .keys()
        .filter(|key| key.starts_with(root.object_prefix()))
        .count();
    assert_eq!(left, 0, "{left} records left after {batches} more batches");
}

/// PR #178 review r1 T4: a map of 255 entries is one record more than a request deletes. After
/// one batch it reads as a smaller map, never `CountMismatch`; then the delete finishes. Cut:
/// `partial_delete` answers `None` for a map (the raw batch, root kept at its count): red.
#[test]
fn a_map_part_deleted_reads_as_a_smaller_map() {
    use rdb_value::collection::members;
    let mut k = Kernel::new();
    let root = root_key(T, A, b"map");
    map(&mut k, &root, 255);
    let batch = delete_object(&k.snapshot(), &root, None).unwrap().unwrap();
    k.commit_any_order(&batch);
    let read = members(&k.snapshot(), &root, None, 1_000);
    let page = read.unwrap_or_else(|e| panic!("after one batch the map reads back: {e:?}"));
    assert!(page.members.len() < 255, "the batch removed members");
    assert_eq!(page.collection.count, page.members.len() as u64);
    finish_delete(&mut k, &root);
}

/// PR #178 review r1 T5: a list of 255 items with their own records. After one batch it reads
/// as a shorter list, never `ItemMissing`; then the delete finishes. Cut: `partial_delete`
/// answers `None` for a list: red.
#[test]
fn a_list_part_deleted_reads_as_a_shorter_list() {
    use rdb_value::list::{compile_list, items, Start};
    let mut k = Kernel::new();
    let root = root_key(T, A, b"list");
    let push = |from: i128, to: i128| (from..to).map(|i| ListOp::Push(int(i))).collect::<Vec<_>>();
    let made = create_list(&k.snapshot(), &root, true, DEFAULT_BLOCK_MAX, &push(0, 50)).unwrap();
    k.commit(made.compiled());
    for at in (50..255).step_by(50) {
        let s = k.snapshot();
        let version = s.version(Namespace::User, root.as_bytes()).unwrap();
        let ops = push(at, (at + 50).min(255));
        let made = compile_list(
            &s,
            &root,
            Expected::Version(version),
            DEFAULT_BLOCK_MAX,
            &ops,
        )
        .unwrap();
        k.commit(made.compiled());
    }
    let full = items(&k.snapshot(), &root, Start::Position(0), 1_000).unwrap();
    assert_eq!(full.items.len(), 255, "control: 255 items");
    let batch = delete_object(&k.snapshot(), &root, None).unwrap().unwrap();
    k.commit_any_order(&batch);
    let read = items(&k.snapshot(), &root, Start::Position(0), 1_000);
    let page = read.unwrap_or_else(|e| panic!("after one batch the list reads back: {e:?}"));
    assert!(page.items.len() < 255, "the batch removed items");
    assert_eq!(page.list.count, page.items.len() as u64);
    finish_delete(&mut k, &root);
}

/// PR #178 review r1 T6: a blob published from 255 one-byte chunks. After one batch it reads as
/// absent, never `ChunkMissing`; then the delete finishes. Cut: `partial_delete` answers `None`
/// for a blob (chunks first, root last): red.
#[test]
fn a_blob_part_deleted_reads_as_absent() {
    use rdb_value::blob::{read_blob, read_range};
    let mut k = Kernel::new();
    let root = root_key(T, A, b"blob");
    let bytes: Vec<u8> = (0..255_u8).collect();
    for (index, piece) in (0..).zip(bytes.chunks(1)) {
        k.commit(&put_chunk(&k.snapshot(), &root, &[9; 16], index, piece).unwrap());
    }
    let sha: [u8; 32] = Sha256::digest(&bytes).into();
    let made = publish(
        &k.snapshot(),
        &root,
        Expected::Absent,
        &[9; 16],
        255,
        1,
        &sha,
        Generation(k.generation),
    )
    .unwrap();
    k.commit(&made);
    assert_eq!(
        read_range(&k.snapshot(), &root, 0, 255).unwrap(),
        bytes,
        "control"
    );
    let version = k
        .snapshot()
        .version(Namespace::User, root.as_bytes())
        .unwrap();
    let batch = delete_object(&k.snapshot(), &root, Some(version))
        .unwrap()
        .unwrap();
    k.commit_any_order(&batch);
    let read = read_blob(&k.snapshot(), &root).and_then(|found| {
        found
            .map(|blob| read_range(&k.snapshot(), &root, 0, blob.manifest.size))
            .transpose()
    });
    let read = read.unwrap_or_else(|e| panic!("after one batch the blob reads back: {e:?}"));
    assert_eq!(read, None, "absent after its root went first");
    // The chunks left go on under `expect`, since the blob it named is deleted.
    let rest = delete_object(&k.snapshot(), &root, Some(version))
        .unwrap()
        .unwrap();
    assert!(!rest.mutations.is_empty());
    finish_delete(&mut k, &root);
}
