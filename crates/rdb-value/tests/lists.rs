//! Ordered lists (ADR-rdb-0016), through the public API and the shared kernel stand-in.
//!
//! These assert behaviour and error variants only: no digest, no record count and no item or
//! leaf byte layout, which ruling L-R186cn may change (lead ruling L-R186cq).

mod common;

use bytes::Bytes;
use common::{text, Kernel, Refused};
use rdb_core::{AffinityId, Generation, Mutation, TenantId};
use rdb_value::cbor::encode;
use rdb_value::collection::{compile_collection, CollectionKind, ElemOp};
use rdb_value::delta::{ApplyError, Delta, Op};
use rdb_value::envelope::{open, seal, EnvelopeError, Kind};
use rdb_value::keys::{parse, root_key, RootKey, Sub};
use rdb_value::list::{
    compile_list, drop_list, items, list, ListOp, Start, Token, DEFAULT_NODE_MAX, MIN_NODE_MAX,
};
use rdb_value::value::{Map, Value};
use rdb_value::{compile, read, Compiled, Corrupt, Expected, PageFault, ValueError};

fn todo() -> RootKey {
    root_key(TenantId(1), AffinityId(1), b"todo")
}

/// A list `todo` holding `n` items, committed one compile per push.
fn list_of(k: &mut Kernel, n: usize) {
    let root = todo();
    let made = compile_list(
        &k.snapshot(),
        &root,
        Expected::Absent,
        DEFAULT_NODE_MAX,
        &[],
    )
    .expect("create");
    k.commit(made.compiled());
    for i in 0..n {
        let version = k.seq;
        let push = [ListOp::Push(text(&format!("item {i}")))];
        let compiled = compile_list(
            &k.snapshot(),
            &root,
            Expected::Version(version),
            DEFAULT_NODE_MAX,
            &push,
        )
        .expect("push");
        k.commit(compiled.compiled());
    }
}

/// Tester W1 wording ask (ruling L-R186cm): a list's refusals named the map and set words.
/// `drop` said "the collection still has 1 elements" and a root fault read "collection root: …".
/// Each now names the list and its items; the map and set text is unchanged. (The third ask, an
/// item of the wrong kind, is checked by hand: a test of it would pin the item record format,
/// which ruling L-R186cn may change.)
#[test]
fn l_r186cm_list_refusals_name_the_list_and_its_items() {
    let root = todo();
    for (n, want) in [
        (1, "the list still has 1 item"),
        (2, "the list still has 2 items"),
    ] {
        let mut k = Kernel::new();
        list_of(&mut k, n);
        let err = drop_list(&k.snapshot(), &root, k.seq).expect_err("not empty");
        assert_eq!(err.to_string(), want);
    }
    assert_eq!(
        ApplyError::NotEmpty { count: 1 }.to_string(),
        "the collection still has 1 elements",
        "the map and set text is unchanged"
    );

    let mut k = Kernel::new();
    list_of(&mut k, 0);
    let version = k.records[&root.to_bytes()].0;
    let damaged = seal(Kind::List, &encode(&Value::Map(Map::new())).unwrap()).unwrap();
    k.records.insert(root.to_bytes(), (version, damaged));
    for err in [
        items(&k.snapshot(), &root, Start::Position(0), 10).expect_err("read"),
        drop_list(&k.snapshot(), &root, version).expect_err("drop"),
    ] {
        let said = err.to_string();
        assert!(
            said.contains("the list root") && !said.contains("collection"),
            "{said}"
        );
    }
}

/// Tester W1 U1 (ruling L-R186cm): the example routes a non-list root to the collection drop, so
/// `drop_list`'s kind check was never reached by hand. A map, empty or not, is refused as
/// `KindMismatch`, never read as a damaged list root and never dropped.
#[test]
fn u1_drop_list_refuses_a_map_root_by_kind() {
    let root = todo();
    for ops in [vec![], vec![ElemOp::Put(text("k"), text("v"))]] {
        let mut k = Kernel::new();
        let made = compile_collection(
            &k.snapshot(),
            &root,
            CollectionKind::Map,
            Expected::Absent,
            &ops,
        )
        .expect("create the map");
        let version = k.commit(&made);
        let err = drop_list(&k.snapshot(), &root, version).expect_err("a map");
        assert_eq!(
            err,
            ValueError::Apply(ApplyError::KindMismatch { found: Kind::Map }),
            "{} entries",
            ops.len()
        );
    }
}

/// The version of `root`'s record in `k`.
fn version_of(k: &Kernel, root: &RootKey) -> u64 {
    k.records[&root.to_bytes()].0
}

/// Create `root` and push `values` in one compile at `node_max`.
fn made(k: &mut Kernel, root: &RootKey, node_max: usize, values: &[&str]) {
    let ops: Vec<ListOp> = values.iter().map(|v| ListOp::Push(text(v))).collect();
    let compiled = compile_list(&k.snapshot(), root, Expected::Absent, node_max, &ops)
        .expect("create and push");
    k.commit(compiled.compiled());
}

/// Every value of `root`, in order.
fn values(k: &Kernel, root: &RootKey) -> Vec<Value> {
    items(&k.snapshot(), root, Start::Position(0), usize::MAX)
        .expect("read")
        .items
        .into_iter()
        .map(|i| i.value)
        .collect()
}

fn texts(values: &[&str]) -> Vec<Value> {
    values.iter().map(|v| text(v)).collect()
}

/// Compile `ops` against `root` at its current version and commit them.
fn write(k: &mut Kernel, root: &RootKey, node_max: usize, ops: &[ListOp]) {
    let version = version_of(k, root);
    let compiled = compile_list(
        &k.snapshot(),
        root,
        Expected::Version(version),
        node_max,
        ops,
    )
    .expect("write");
    k.commit(compiled.compiled());
}

/// A write names the version it read. A stale one is `VersionConflict`, a missing list is
/// `ObjectAbsent`, and a create over a live list compiles but the kernel refuses its
/// `Absent` condition, so nothing changes (design S15).
#[test]
fn w1_stale_expect_absent_list_and_create_twice_are_refused() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, DEFAULT_NODE_MAX, &["a"]);
    write(&mut k, &root, DEFAULT_NODE_MAX, &[ListOp::Push(text("b"))]);
    let now = version_of(&k, &root);
    let push = [ListOp::Push(text("c"))];

    let stale = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(now - 1),
        DEFAULT_NODE_MAX,
        &push,
    );
    assert_eq!(
        stale.err(),
        Some(ValueError::Apply(ApplyError::VersionConflict {
            expected: now - 1,
            found: now
        }))
    );
    let other = root_key(TenantId(1), AffinityId(1), b"nope");
    let absent = compile_list(
        &k.snapshot(),
        &other,
        Expected::Version(1),
        DEFAULT_NODE_MAX,
        &push,
    );
    assert_eq!(
        absent.err(),
        Some(ValueError::Apply(ApplyError::ObjectAbsent))
    );

    let before = k.clone();
    let again = compile_list(
        &k.snapshot(),
        &root,
        Expected::Absent,
        DEFAULT_NODE_MAX,
        &push,
    )
    .expect("a create over a live list compiles");
    assert_eq!(
        k.apply(again.compiled(), Some(again.generation().0)),
        Err(Refused::Condition(0))
    );
    assert_eq!(k, before, "nothing written");
    assert_eq!(values(&k, &root), texts(&["a", "b"]));
}

/// `Insert{at}` takes `at` up to `count`: at `count` it appends, one past is `PositionInvalid`.
/// A later op in one compile sees the list the earlier ops left.
#[test]
fn w1_insert_at_count_appends_and_one_past_is_refused() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, DEFAULT_NODE_MAX, &["a", "b"]);
    write(
        &mut k,
        &root,
        DEFAULT_NODE_MAX,
        &[
            ListOp::Insert {
                at: 2,
                value: text("c"),
            },
            ListOp::Insert {
                at: 0,
                value: text("z"),
            },
        ],
    );
    assert_eq!(values(&k, &root), texts(&["z", "a", "b", "c"]));

    let version = version_of(&k, &root);
    let refused = |ops: &[ListOp]| {
        compile_list(
            &k.snapshot(),
            &root,
            Expected::Version(version),
            DEFAULT_NODE_MAX,
            ops,
        )
        .err()
    };
    assert_eq!(
        refused(&[ListOp::Insert {
            at: 5,
            value: text("x")
        }]),
        Some(ValueError::Apply(ApplyError::PositionInvalid {
            position: 5,
            len: 4
        }))
    );
    assert_eq!(
        refused(&[
            ListOp::Insert {
                at: 0,
                value: text("x")
            },
            ListOp::Insert {
                at: 6,
                value: text("y")
            },
        ]),
        Some(ValueError::Apply(ApplyError::PositionInvalid {
            position: 6,
            len: 5
        })),
        "the second op sees the first one's item"
    );
}

/// `items` pages by position. A page that stops short names a token for the rest; the token
/// is good only at the generation and version it was read at.
#[test]
fn w1_paging_tokens_resume_and_are_refused_when_stale() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, DEFAULT_NODE_MAX, &["a", "b", "c", "d", "e"]);
    let version = version_of(&k, &root);
    let snap = k.snapshot();

    let mut got = Vec::new();
    let mut start = Start::Position(0);
    let mut tokens = Vec::new();
    loop {
        let page = items(&snap, &root, start, 2).expect("page");
        assert!(page.items.len() <= 2);
        assert_eq!((page.list.version, page.list.count), (version, 5));
        got.extend(page.items.into_iter().map(|i| (i.position, i.value)));
        match page.next {
            Some(token) => {
                tokens.push(token);
                start = Start::Token(token);
            }
            None => break,
        }
    }
    let want: Vec<(u64, Value)> = (0..).zip(texts(&["a", "b", "c", "d", "e"])).collect();
    assert_eq!(got, want);
    assert_eq!(
        tokens,
        [2, 4].map(|position| Token {
            generation: Generation(1),
            version,
            position
        })
    );

    let at = |start| items(&snap, &root, start, 10);
    let end = at(Start::Position(5)).expect("position = count");
    assert!(end.items.is_empty() && end.next.is_none());
    assert_eq!(
        at(Start::Position(6)).err(),
        Some(ValueError::Apply(ApplyError::PositionInvalid {
            position: 6,
            len: 5
        }))
    );
    let token = tokens[0];
    for generation in [0, 2] {
        let other = Token {
            generation: Generation(generation),
            ..token
        };
        assert_eq!(
            at(Start::Token(other)).err(),
            Some(ValueError::Apply(ApplyError::GenerationChanged {
                expected: generation,
                found: 1
            }))
        );
    }
    let past = Token {
        position: 6,
        ..token
    };
    assert_eq!(
        at(Start::Token(past)).err(),
        Some(ValueError::Apply(ApplyError::PositionInvalid {
            position: 6,
            len: 5
        }))
    );

    write(&mut k, &root, DEFAULT_NODE_MAX, &[ListOp::Push(text("f"))]);
    assert_eq!(
        items(&k.snapshot(), &root, Start::Token(token), 10).err(),
        Some(ValueError::Apply(ApplyError::VersionConflict {
            expected: version,
            found: version + 1
        }))
    );
}

/// A list compile carries the generation it read at, and the kernel refuses it at any other:
/// a list's pages and ids are guarded only by the root's version, which a failover can reuse
/// (ADR-rdb-0016 §8). The drop is fenced the same way.
#[test]
fn w1_a_list_write_is_fenced_by_its_generation() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, DEFAULT_NODE_MAX, &["a"]);
    let version = version_of(&k, &root);
    let push = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version),
        DEFAULT_NODE_MAX,
        &[ListOp::Push(text("b"))],
    )
    .expect("push");
    assert_eq!(push.generation(), Generation(1));

    let mut moved = k.clone();
    moved.generation = 2;
    let before = moved.clone();
    assert_eq!(
        moved.apply(push.compiled(), Some(push.generation().0)),
        Err(Refused::GenerationChanged)
    );
    assert_eq!(moved, before, "nothing written");

    write(&mut k, &root, DEFAULT_NODE_MAX, &[ListOp::Remove { at: 0 }]);
    let drop = drop_list(&k.snapshot(), &root, version_of(&k, &root)).expect("empty drop");
    assert_eq!(drop.generation(), Generation(1));
    let mut moved = k.clone();
    moved.generation = 2;
    assert_eq!(
        moved.apply(drop.compiled(), Some(drop.generation().0)),
        Err(Refused::GenerationChanged)
    );
    k.commit(drop.compiled());
    assert!(!k.records.contains_key(&root.to_bytes()), "dropped");
}

fn key_of(mutation: &Mutation) -> &Bytes {
    let (Mutation::Put { key, .. } | Mutation::Delete { key, .. }) = mutation;
    key
}

/// C17: every list write set holds one write per key in strictly ascending key order; the
/// kernel refuses anything else (check 10). Checked across a create, appends, inserts at the
/// front, splits at the smallest node size, and a drop.
#[test]
fn c17_list_write_sets_strictly_ascend() {
    let root = todo();
    let mut k = Kernel::new();
    let ascends = |compiled: &Compiled| {
        let keys: Vec<&Bytes> = compiled.mutations.iter().map(key_of).collect();
        assert!(!keys.is_empty());
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "{keys:?}");
    };
    let create = compile_list(
        &k.snapshot(),
        &root,
        Expected::Absent,
        MIN_NODE_MAX,
        &[ListOp::Push(text("first"))],
    )
    .expect("create");
    ascends(create.compiled());
    k.commit(create.compiled());
    for round in 0..12 {
        let ops: Vec<ListOp> = (0..5)
            .map(|i| match i % 2 {
                0 => ListOp::Push(text(&format!("p{round}.{i}"))),
                _ => ListOp::Insert {
                    at: 0,
                    value: text(&format!("i{round}.{i}")),
                },
            })
            .collect();
        let version = version_of(&k, &root);
        let compiled = compile_list(
            &k.snapshot(),
            &root,
            Expected::Version(version),
            MIN_NODE_MAX,
            &ops,
        )
        .expect("write");
        ascends(compiled.compiled());
        k.commit(compiled.compiled());
    }
    assert!(
        list(&k.snapshot(), &root).unwrap().unwrap().height > 1,
        "pages were made"
    );
    assert_eq!(values(&k, &root).len(), 61);
    let removes: Vec<ListOp> = (0..61).map(|_| ListOp::Remove { at: 0 }).collect();
    let version = version_of(&k, &root);
    let emptied = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version),
        MIN_NODE_MAX,
        &removes,
    )
    .expect("remove all");
    ascends(emptied.compiled());
    k.commit(emptied.compiled());
    let drop = drop_list(&k.snapshot(), &root, version_of(&k, &root)).expect("drop");
    ascends(drop.compiled());
}

/// A list root can be dropped only empty. Once dropped it is absent; a stale drop is a
/// `VersionConflict`.
#[test]
fn w1_drop_needs_an_empty_list_at_the_version_read() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, DEFAULT_NODE_MAX, &["a"]);
    let version = version_of(&k, &root);
    assert_eq!(
        drop_list(&k.snapshot(), &root, version).err(),
        Some(ValueError::Apply(ApplyError::ListNotEmpty { count: 1 }))
    );
    write(&mut k, &root, DEFAULT_NODE_MAX, &[ListOp::Remove { at: 0 }]);
    let now = version_of(&k, &root);
    assert_eq!(
        drop_list(&k.snapshot(), &root, version).err(),
        Some(ValueError::Apply(ApplyError::VersionConflict {
            expected: version,
            found: now
        }))
    );
    let drop = drop_list(&k.snapshot(), &root, now).expect("empty drop");
    k.commit(drop.compiled());
    assert_eq!(list(&k.snapshot(), &root), Ok(None));
    assert_eq!(
        drop_list(&k.snapshot(), &root, now).err(),
        Some(ValueError::Apply(ApplyError::ObjectAbsent))
    );
}

/// A list op on another kind, and another kind's op on a list, is `KindMismatch` naming the
/// kind found.
#[test]
fn w1_kind_mismatch_both_ways() {
    let mut k = Kernel::new();
    let cart = root_key(TenantId(1), AffinityId(1), b"cart");
    let doc = root_key(TenantId(1), AffinityId(1), b"doc");
    let root = todo();
    let map = compile_collection(
        &k.snapshot(),
        &cart,
        CollectionKind::Map,
        Expected::Absent,
        &[ElemOp::Put(text("k"), text("v"))],
    )
    .expect("map");
    k.commit(&map);
    let document = compile(
        &k.snapshot(),
        &doc,
        Expected::Absent,
        &Delta(vec![Op::Replace(text("d"))]),
    )
    .expect("document");
    k.commit(&document);
    made(&mut k, &root, DEFAULT_NODE_MAX, &["a"]);
    let snap = k.snapshot();
    let mismatch = |found| Some(ValueError::Apply(ApplyError::KindMismatch { found }));

    for (key, found) in [(&cart, Kind::Map), (&doc, Kind::Document)] {
        let version = version_of(&k, key);
        let push = [ListOp::Push(text("x"))];
        let op = compile_list(
            &snap,
            key,
            Expected::Version(version),
            DEFAULT_NODE_MAX,
            &push,
        );
        assert_eq!(op.err(), mismatch(found));
        assert_eq!(
            items(&snap, key, Start::Position(0), 10).err(),
            mismatch(found)
        );
        assert_eq!(list(&snap, key).err(), mismatch(found));
    }
    let version = version_of(&k, &root);
    for kind in [CollectionKind::Map, CollectionKind::Set] {
        let op = compile_collection(&snap, &root, kind, Expected::Version(version), &[]);
        assert_eq!(op.err(), mismatch(Kind::List));
    }
    assert_eq!(read(&snap, &root).err(), mismatch(Kind::List));
    let replace = Delta(vec![Op::Replace(text("d"))]);
    assert_eq!(
        compile(&snap, &root, Expected::Version(version), &replace).err(),
        mismatch(Kind::List)
    );
}

/// Damage to a list root is refused by name on a read, a write and a drop, never served.
#[test]
fn w1_a_damaged_list_root_is_refused_on_every_path() {
    let root = todo();
    let mut fresh = Kernel::new();
    made(&mut fresh, &root, DEFAULT_NODE_MAX, &[]);
    let good = fresh.records[&root.to_bytes()].1.clone();
    let mut flipped = good.to_vec();
    flipped[8] ^= 1;
    let mut truncated = good.to_vec();
    truncated.pop();
    let resealed = |payload: &[u8]| seal(Kind::List, payload).unwrap();
    let not_canonical = {
        let opened = open(&good).unwrap();
        let mut payload = opened.payload.to_vec();
        payload.push(0x00);
        resealed(&payload)
    };
    let cases: [(&str, Bytes, Named<ValueError>); 5] = [
        ("digest flipped", Bytes::from(flipped), |e| {
            matches!(
                e,
                ValueError::Corrupt(Corrupt::Envelope(EnvelopeError::DigestMismatch))
            )
        }),
        ("envelope truncated", Bytes::from(truncated), |e| {
            matches!(e, ValueError::Corrupt(Corrupt::Envelope(_)))
        }),
        ("trailing payload byte", not_canonical, |e| {
            matches!(e, ValueError::Corrupt(Corrupt::Codec(_)))
        }),
        (
            "payload not a map",
            resealed(&encode(&text("x")).unwrap()),
            |e| matches!(e, ValueError::Corrupt(Corrupt::ListRoot(_))),
        ),
        (
            "payload an empty map",
            resealed(&encode(&Value::Map(Map::new())).unwrap()),
            |e| matches!(e, ValueError::Corrupt(Corrupt::ListRoot(_))),
        ),
    ];
    for (name, damaged, named) in cases {
        let mut k = fresh.clone();
        let version = version_of(&k, &root);
        k.records.insert(root.to_bytes(), (version, damaged));
        let snap = k.snapshot();
        let push = [ListOp::Push(text("x"))];
        for err in [
            items(&snap, &root, Start::Position(0), 10).expect_err("read"),
            list(&snap, &root).expect_err("summary"),
            compile_list(
                &snap,
                &root,
                Expected::Version(version),
                DEFAULT_NODE_MAX,
                &push,
            )
            .expect_err("write"),
            drop_list(&snap, &root, version).expect_err("drop"),
        ] {
            assert!(named(&err), "{name}: {err:?}");
        }
    }
}

type Damage = fn(&mut Kernel, &Bytes, &Bytes, u64);

/// Whether a refusal is the one a damage case expects.
type Named<E> = fn(&E) -> bool;

/// Damage to every page under a list root is refused as `Corrupt::Page` on a full read and on
/// a write at either end, never served. The pages are found by key, not by their layout.
#[test]
fn w1_a_damaged_page_is_refused_on_read_and_write() {
    let root = todo();
    let mut fresh = Kernel::new();
    let names: Vec<String> = (0..40).map(|i| format!("item {i}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut fresh, &root, MIN_NODE_MAX, &names);
    let pages: Vec<Bytes> = fresh
        .records
        .keys()
        .filter(|key| parse(key).is_ok_and(|p| p.root() == root && p.sub == Sub::Page))
        .cloned()
        .collect();
    assert!(
        pages.len() > 1,
        "40 items at the smallest node size make pages"
    );
    let root_version = version_of(&fresh, &root);
    let document = seal(Kind::Document, &encode(&text("x")).unwrap()).unwrap();

    let cases: [(&str, Damage, Named<PageFault>); 4] = [
        (
            "missing",
            |k, key, _, _| {
                k.records.remove(key);
            },
            |f| matches!(f, PageFault::Missing),
        ),
        (
            "digest flipped",
            |k, key, _, _| {
                let (v, bytes) = k.records[key].clone();
                let mut bytes = bytes.to_vec();
                bytes[8] ^= 1;
                k.records.insert(key.clone(), (v, Bytes::from(bytes)));
            },
            |f| matches!(f, PageFault::Envelope(EnvelopeError::DigestMismatch)),
        ),
        (
            "a document",
            |k, key, document, _| {
                let v = k.records[key].0;
                k.records.insert(key.clone(), (v, document.clone()));
            },
            |f| {
                matches!(
                    f,
                    PageFault::NotAPage {
                        found: Kind::Document
                    }
                )
            },
        ),
        (
            "newer than the root",
            |k, key, _, root_version| {
                let bytes = k.records[key].1.clone();
                k.records.insert(key.clone(), (root_version + 1, bytes));
                k.seq = k.seq.max(root_version + 1);
            },
            |f| matches!(f, PageFault::NewerThanRoot { .. }),
        ),
    ];
    for (name, damage, named) in cases {
        let mut k = fresh.clone();
        for key in &pages {
            damage(&mut k, key, &document, root_version);
        }
        let snap = k.snapshot();
        for op in [
            ListOp::Push(text("x")),
            ListOp::Insert {
                at: 0,
                value: text("x"),
            },
        ] {
            let write = compile_list(
                &snap,
                &root,
                Expected::Version(root_version),
                MIN_NODE_MAX,
                &[op],
            );
            for err in [
                items(&snap, &root, Start::Position(0), usize::MAX).expect_err("read"),
                write.expect_err("write"),
            ] {
                assert!(
                    matches!(&err, ValueError::Corrupt(Corrupt::Page { fault, .. }) if named(fault)),
                    "{name}: {err:?}"
                );
            }
        }
    }
}
