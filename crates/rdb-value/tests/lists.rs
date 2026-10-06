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
use rdb_value::keys::{parse, root_key, KeyError, RootKey, Sub};
use rdb_value::list::{
    compile_list, drop_list, items, list, ListOp, Start, Token, DEFAULT_BLOCK_MAX, MIN_BLOCK_MAX,
};
use rdb_value::testing::CountingSnapshot;
use rdb_value::value::{Map, Value};
use rdb_value::{compile, read, BlockFault, Compiled, Corrupt, Expected, ValueError};

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
        DEFAULT_BLOCK_MAX,
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
            DEFAULT_BLOCK_MAX,
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

/// Create `root` and push `values` in one compile at `block_max`.
fn made(k: &mut Kernel, root: &RootKey, block_max: usize, values: &[&str]) {
    let ops: Vec<ListOp> = values.iter().map(|v| ListOp::Push(text(v))).collect();
    let compiled = compile_list(&k.snapshot(), root, Expected::Absent, block_max, &ops)
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
fn write(k: &mut Kernel, root: &RootKey, block_max: usize, ops: &[ListOp]) {
    let version = version_of(k, root);
    let compiled = compile_list(
        &k.snapshot(),
        root,
        Expected::Version(version),
        block_max,
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
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &["a"]);
    write(&mut k, &root, DEFAULT_BLOCK_MAX, &[ListOp::Push(text("b"))]);
    let now = version_of(&k, &root);
    let push = [ListOp::Push(text("c"))];

    let stale = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(now - 1),
        DEFAULT_BLOCK_MAX,
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
        DEFAULT_BLOCK_MAX,
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
        DEFAULT_BLOCK_MAX,
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
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &["a", "b"]);
    write(
        &mut k,
        &root,
        DEFAULT_BLOCK_MAX,
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
            DEFAULT_BLOCK_MAX,
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
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &["a", "b", "c", "d", "e"]);
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

    write(&mut k, &root, DEFAULT_BLOCK_MAX, &[ListOp::Push(text("f"))]);
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
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &["a"]);
    let version = version_of(&k, &root);
    let push = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version),
        DEFAULT_BLOCK_MAX,
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

    write(
        &mut k,
        &root,
        DEFAULT_BLOCK_MAX,
        &[ListOp::Remove { at: 0 }],
    );
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
/// front, slots and folds at the smallest block size, and a drop.
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
        MIN_BLOCK_MAX,
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
            MIN_BLOCK_MAX,
            &ops,
        )
        .expect("write");
        ascends(compiled.compiled());
        k.commit(compiled.compiled());
    }
    assert_eq!(values(&k, &root).len(), 61);
    let removes: Vec<ListOp> = (0..61).map(|_| ListOp::Remove { at: 0 }).collect();
    let version = version_of(&k, &root);
    let emptied = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version),
        MIN_BLOCK_MAX,
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
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &["a"]);
    let version = version_of(&k, &root);
    assert_eq!(
        drop_list(&k.snapshot(), &root, version).err(),
        Some(ValueError::Apply(ApplyError::ListNotEmpty { count: 1 }))
    );
    write(
        &mut k,
        &root,
        DEFAULT_BLOCK_MAX,
        &[ListOp::Remove { at: 0 }],
    );
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
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &["a"]);
    let snap = k.snapshot();
    let mismatch = |found| Some(ValueError::Apply(ApplyError::KindMismatch { found }));

    for (key, found) in [(&cart, Kind::Map), (&doc, Kind::Document)] {
        let version = version_of(&k, key);
        let push = [ListOp::Push(text("x"))];
        let op = compile_list(
            &snap,
            key,
            Expected::Version(version),
            DEFAULT_BLOCK_MAX,
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
    made(&mut fresh, &root, DEFAULT_BLOCK_MAX, &[]);
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
                DEFAULT_BLOCK_MAX,
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

/// Damage to every block base under a list root is refused as `Corrupt::Block` on a full read
/// and on a write at either end, never served. The bases are found by key, not by their layout.
#[test]
fn w1_a_damaged_block_is_refused_on_read_and_write() {
    let root = todo();
    let mut fresh = Kernel::new();
    let names: Vec<String> = (0..40).map(|i| format!("item {i}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut fresh, &root, MIN_BLOCK_MAX, &names);
    let bases: Vec<Bytes> = fresh
        .records
        .keys()
        .filter(|key| {
            parse(key).is_ok_and(|p| p.root() == root && p.sub == Sub::Block && p.slot.is_none())
        })
        .cloned()
        .collect();
    assert!(!bases.is_empty(), "a list has a block");
    let root_version = version_of(&fresh, &root);
    let document = seal(Kind::Document, &encode(&text("x")).unwrap()).unwrap();

    let cases: [(&str, Damage, Named<BlockFault>); 4] = [
        (
            "missing",
            |k, key, _, _| {
                k.records.remove(key);
            },
            |f| matches!(f, BlockFault::Missing),
        ),
        (
            "digest flipped",
            |k, key, _, _| {
                let (v, bytes) = k.records[key].clone();
                let mut bytes = bytes.to_vec();
                bytes[8] ^= 1;
                k.records.insert(key.clone(), (v, Bytes::from(bytes)));
            },
            |f| matches!(f, BlockFault::Envelope(EnvelopeError::DigestMismatch)),
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
                    BlockFault::NotABlock {
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
            |f| matches!(f, BlockFault::NewerThanRoot { .. }),
        ),
    ];
    for (name, damage, named) in cases {
        let mut k = fresh.clone();
        for key in &bases {
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
                MIN_BLOCK_MAX,
                &[op],
            );
            for err in [
                items(&snap, &root, Start::Position(0), usize::MAX).expect_err("read"),
                write.expect_err("write"),
            ] {
                assert!(
                    matches!(&err, ValueError::Corrupt(Corrupt::Block { fault, .. }) if named(fault)),
                    "{name}: {err:?}"
                );
            }
        }
    }
}

/// The bytes `items`, a push and a drop of the list at `root` read from `k`. The drop is measured
/// after removing every item.
fn opened_bytes(k: &Kernel, root: &RootKey) -> [u64; 3] {
    let snap = k.snapshot();
    let read = CountingSnapshot::new(&snap);
    items(&read, root, Start::Position(0), usize::MAX).expect("read");
    let push = CountingSnapshot::new(&snap);
    let version = version_of(k, root);
    let ops = [ListOp::Push(text("x"))];
    compile_list(&push, root, Expected::Version(version), MIN_BLOCK_MAX, &ops).expect("push");
    let mut k = k.clone();
    let all = items(&k.snapshot(), root, Start::Position(0), usize::MAX).expect("read");
    let removes: Vec<ListOp> = all.items.iter().map(|_| ListOp::Remove { at: 0 }).collect();
    write(&mut k, root, MIN_BLOCK_MAX, &removes);
    let snap = k.snapshot();
    let dropped = CountingSnapshot::new(&snap);
    drop_list(&dropped, root, version_of(&k, root)).expect("drop");
    [read.bytes(), push.bytes(), dropped.bytes()]
}

/// Tester W1 D1 (design S24, G67): a block read stays inside the block's pending slots. A tiny
/// list's read and write open the same bytes whether or not a list whose items are big records
/// sorts right after it; they used to scan on into that neighbour's records. A drop's second
/// orphan check is a limit-1 scan past the block (ADR-rdb-0016 §5), so it opens exactly one
/// foreign record, the next key: here the neighbour's root, never its items.
#[test]
fn d1_a_tiny_list_opens_no_bytes_of_a_big_neighbour() {
    let tiny = root_key(TenantId(1), AffinityId(1), b"a");
    let big = root_key(TenantId(1), AffinityId(1), b"b");
    let mut k = Kernel::new();
    made(
        &mut k,
        &tiny,
        MIN_BLOCK_MAX,
        &["a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7"],
    );
    // Below a quarter of the base, so it stays pending: the read scans its slot.
    write(&mut k, &tiny, MIN_BLOCK_MAX, &[ListOp::Push(text("tiny"))]);
    assert!(
        k.records
            .keys()
            .any(|key| parse(key).is_ok_and(|p| p.root() == tiny && p.slot.is_some())),
        "the tiny list has a pending slot"
    );
    let alone = opened_bytes(&k, &tiny);

    made(&mut k, &big, DEFAULT_BLOCK_MAX, &[]);
    let record = "w".repeat(400_000);
    for _ in 0..3 {
        write(
            &mut k,
            &big,
            DEFAULT_BLOCK_MAX,
            &[ListOp::Push(text(&record))],
        );
    }
    let neighbour: usize = k
        .records
        .iter()
        .filter(|(key, _)| parse(key).is_ok_and(|p| p.root() == big && p.sub == Sub::Item))
        .map(|(_, (_, value))| value.len())
        .sum();
    assert!(neighbour > 1_200_000, "the neighbour's items are records");
    let next = k.records[&big.to_bytes()].1.len();
    let [read, push, dropped] = opened_bytes(&k, &tiny);
    assert_eq!([read, push], [alone[0], alone[1]], "read, push");
    assert_eq!(dropped, alone[2] + u64::try_from(next).unwrap(), "drop");
}

/// Tester W1 D2: a write that sizes an item record checks it as a read does, and refuses a
/// damaged one with the same `Corrupt(..)` the read names; it used to take the record's length
/// unopened and write over it.
#[test]
fn d2_a_write_refuses_a_damaged_item_record_as_the_read_does() {
    let root = todo();
    let mut fresh = Kernel::new();
    let long = "r".repeat(300);
    made(&mut fresh, &root, MIN_BLOCK_MAX, &["a", &long]);
    let item = fresh
        .records
        .keys()
        .find(|key| parse(key).is_ok_and(|p| p.root() == root && p.sub == Sub::Item))
        .expect("the long item is a record")
        .clone();
    let (item_version, sealed) = fresh.records[&item].clone();
    let mut flipped = sealed.to_vec();
    flipped[8] ^= 1;
    let block = seal(Kind::ListBlock, &encode(&text("x")).unwrap()).unwrap();
    let not_cbor = seal(Kind::Document, &[0xff]).unwrap();
    let cases: [(&str, Bytes, Named<Corrupt>); 3] = [
        ("digest flipped", Bytes::from(flipped), |c| {
            matches!(c, Corrupt::Envelope(EnvelopeError::DigestMismatch))
        }),
        ("a block's kind", block, |c| {
            matches!(
                c,
                Corrupt::ItemNotDocument {
                    found: Kind::ListBlock
                }
            )
        }),
        ("not CBOR", not_cbor, |c| matches!(c, Corrupt::Codec(_))),
    ];
    for (name, damaged, named) in cases {
        let mut k = fresh.clone();
        k.records.insert(item.clone(), (item_version, damaged));
        let snap = k.snapshot();
        let read = items(&snap, &root, Start::Position(0), usize::MAX).expect_err("read");
        assert!(
            matches!(&read, ValueError::Corrupt(c) if named(c)),
            "{name}: {read:?}"
        );
        for op in [
            ListOp::Remove { at: 1 },
            ListOp::Replace {
                at: 1,
                value: text("short"),
            },
        ] {
            let write = compile_list(
                &snap,
                &root,
                Expected::Version(version_of(&k, &root)),
                MIN_BLOCK_MAX,
                &[op],
            )
            .expect_err("write");
            assert_eq!(write, read, "{name}");
        }
    }
}

/// Tester W1 re-walk N3: a stray key between two pending slots was reported as the next slot
/// missing (`OpMissing`), though that slot is there. The stray is the fault, named as P8 names
/// one: an 18-byte tail is `Key(ListIdTail{18})`. A read and a write name it alike.
#[test]
fn n3_a_stray_between_two_pending_slots_is_named_not_the_next_slot() {
    let root = todo();
    let mut k = Kernel::new();
    let names: Vec<String> = (0..400).map(|i| format!("item {i}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &names);
    for i in 0..30 {
        write(
            &mut k,
            &root,
            DEFAULT_BLOCK_MAX,
            &[ListOp::Push(text(&format!("p{i}")))],
        );
    }
    let slots: Vec<(Bytes, u64)> = k
        .records
        .iter()
        .filter(|(key, _)| parse(key).is_ok_and(|p| p.root() == root && p.slot.is_some()))
        .map(|(key, (version, _))| (key.clone(), *version))
        .collect();
    assert!(slots.len() >= 2, "the pushes stay pending in slots");
    let (first, version) = slots[0].clone();
    let mut stray = first.to_vec();
    stray.push(0);
    assert!(
        stray.as_slice() < &slots[1].0[..],
        "the stray sorts between two slots"
    );
    k.records
        .insert(Bytes::from(stray), (version, Bytes::from_static(b"x")));
    let named = ValueError::Corrupt(Corrupt::Key(KeyError::ListIdTail { len: 18 }));
    let snap = k.snapshot();
    let read = items(&snap, &root, Start::Position(0), usize::MAX).expect_err("read");
    assert_eq!(read, named, "read");
    let push = [ListOp::Push(text("y"))];
    let write = compile_list(
        &snap,
        &root,
        Expected::Version(version_of(&k, &root)),
        DEFAULT_BLOCK_MAX,
        &push,
    )
    .expect_err("write");
    assert_eq!(write, named, "write");
}

/// Tester W1 re-walk N4: `limit` 0 reads the root only, from any position, as the doc says.
/// From position 2 it read block 0 as well.
#[test]
fn n4_limit_0_reads_the_root_only_from_any_position() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, MIN_BLOCK_MAX, &["a", "b", "c", "d"]);
    let snap = k.snapshot();
    let opened = |position| {
        let counting = CountingSnapshot::new(&snap);
        let page = items(&counting, &root, Start::Position(position), 0).expect("read");
        assert!(page.items.is_empty() && page.next.is_none(), "{position}");
        (counting.calls(), counting.bytes())
    };
    assert_eq!(opened(2), opened(0), "from 2 and from 0");
}

fn len_u64(n: usize) -> u64 {
    u64::try_from(n).expect("fits u64")
}

fn index(at: u64) -> usize {
    usize::try_from(at).expect("fits usize")
}

/// A tiny deterministic generator (xorshift64*), so a failing seed reruns exactly.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform in `0..n`, `n > 0`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The root's block index: `(n, count)` per block, in block order.
fn block_index(k: &Kernel, root: &RootKey) -> Vec<(u64, u64)> {
    let (_, raw) = &k.records[&root.to_bytes()];
    let payload = rdb_value::cbor::decode(open(raw).expect("root envelope").payload).expect("cbor");
    let Value::Map(fields) = payload else {
        panic!("a list root is a map")
    };
    let Some(Value::Array(blocks)) = fields.get(&rdb_value::value::MapKey::new("blocks")) else {
        panic!("a list root has blocks")
    };
    let uint = |v: &Value| match v {
        Value::Integer(i) => u64::try_from(i.get()).expect("unsigned"),
        other => panic!("not an integer: {other:?}"),
    };
    blocks
        .iter()
        .map(|block| match block {
            Value::Array(parts) => (uint(&parts[0]), uint(&parts[1])),
            other => panic!("a block entry is an array: {other:?}"),
        })
        .collect()
}

/// Where a position lands, as the compile places it: the block holding `pos`, or with `end` the
/// earlier block at a boundary (ADR-rdb-0016 §4's tie).
fn locate(counts: &[u64], pos: u64, end: bool) -> usize {
    let mut first = 0;
    for (i, count) in counts.iter().enumerate() {
        let after = first + count;
        if pos < after || (end && pos == after) {
            return i;
        }
        first = after;
    }
    unreachable!("position {pos} checked against the count")
}

/// W2 model check (coordinator, after W10–W12 survived): random deltas of pushes, inserts,
/// removes, moves and replaces at `block_max` 1,024, a growing phase then a shrinking one, so
/// blocks fill, split, empty, retire and merge. Every committed step must match a plain `Vec`
/// (same values, same order, same count), keep every block base at or under B, and change the
/// block index only as §4 allows: the blocks the ops emptied retire (all but the first when the
/// list empties), at most one more block goes (one merge-back), none beside a retire, and a
/// merged-into block's base is at most ¾ · B. A refused delta (`TooLarge`, `TooManyWrites`)
/// leaves the model as it was. 20 seeds here (about 1 s in a debug build); 300 in the ignored
/// `w2_model_300_seeds`, run by hand: `cargo test -p rdb-value --test lists -- --ignored`.
#[test]
fn w2_model_random_deltas_match_a_vec_and_keep_the_block_rules() {
    model_run(1..=20);
}

/// [`w2_model_random_deltas_match_a_vec_and_keep_the_block_rules`] over 300 seeds (about 17 s).
#[test]
#[ignore = "about 17 s in a debug build; run by hand"]
fn w2_model_300_seeds() {
    model_run(1..=300);
}

fn model_run(seeds: std::ops::RangeInclusive<u64>) {
    const B: usize = MIN_BLOCK_MAX;
    let root = todo();
    let (mut merges, mut retires, mut splits, mut refused, mut steps) = (0, 0, 0, 0, 0);
    for seed in seeds {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut k = Kernel::new();
        made(&mut k, &root, B, &[]);
        let mut model: Vec<Value> = Vec::new();
        let mut minted = 0_u64;
        for step in 0..60_u64 {
            let growing = step < 30;
            let before = block_index(&k, &root);
            // Each block's post-op count, placed as the compile places ops.
            let mut counts: Vec<u64> = before.iter().map(|(_, count)| *count).collect();
            let mut next = model.clone();
            let mut ops = Vec::new();
            for _ in 0..=rng.below(6) {
                let len = len_u64(next.len());
                let roll = rng.below(100);
                let add = if growing { roll < 60 } else { roll < 25 };
                if add || len == 0 {
                    minted += 1;
                    let value = text(&format!(
                        "{seed}.{minted}{}",
                        "x".repeat(usize::try_from(rng.below(90)).expect("small"))
                    ));
                    if rng.below(2) == 0 {
                        *counts.last_mut().expect("a list has a block") += 1;
                        next.push(value.clone());
                        ops.push(ListOp::Push(value));
                    } else {
                        let at = rng.below(len + 1);
                        let i = locate(&counts, at, true);
                        counts[i] += 1;
                        next.insert(index(at), value.clone());
                        ops.push(ListOp::Insert { at, value });
                    }
                } else if roll < 80 {
                    let at = rng.below(len);
                    let i = locate(&counts, at, false);
                    counts[i] -= 1;
                    next.remove(index(at));
                    ops.push(ListOp::Remove { at });
                } else if roll < 92 {
                    let (from, to) = (rng.below(len), rng.below(len));
                    let i = locate(&counts, from, false);
                    counts[i] -= 1;
                    let i = locate(&counts, to, true);
                    counts[i] += 1;
                    let value = next.remove(index(from));
                    next.insert(index(to), value);
                    ops.push(ListOp::Move { from, to });
                } else {
                    let at = rng.below(len);
                    minted += 1;
                    let value = text(&format!("{seed}.{minted}r"));
                    next[index(at)] = value.clone();
                    ops.push(ListOp::Replace { at, value });
                }
            }
            let at = format!("seed {seed} step {step}");
            let version = version_of(&k, &root);
            let compiled =
                match compile_list(&k.snapshot(), &root, Expected::Version(version), B, &ops) {
                    Ok(compiled) => compiled,
                    Err(ValueError::Apply(
                        ApplyError::TooLarge { .. } | ApplyError::TooManyWrites { .. },
                    )) => {
                        refused += 1;
                        continue;
                    }
                    Err(e) => panic!("{at}: {e:?} for {ops:?}"),
                };
            k.commit(compiled.compiled());
            steps += 1;
            model = next;
            let page = items(&k.snapshot(), &root, Start::Position(0), usize::MAX).expect("read");
            let read: Vec<Value> = page.items.into_iter().map(|item| item.value).collect();
            assert_eq!(read, model, "{at}: items");
            assert_eq!(page.list.count, len_u64(model.len()), "{at}: count");

            let after = block_index(&k, &root);
            let live: Vec<u64> = after.iter().map(|(n, _)| *n).collect();
            let all_empty = model.is_empty();
            let emptied: Vec<u64> = before
                .iter()
                .zip(&counts)
                .enumerate()
                .filter(|(i, (_, count))| **count == 0 && !(all_empty && *i == 0))
                .map(|(_, ((n, _), _))| *n)
                .collect();
            let gone: Vec<u64> = before
                .iter()
                .map(|(n, _)| *n)
                .filter(|n| !live.contains(n))
                .collect();
            assert!(
                emptied.iter().all(|n| gone.contains(n)),
                "{at}: an emptied block stayed"
            );
            let merged: Vec<u64> = gone
                .iter()
                .copied()
                .filter(|n| !emptied.contains(n))
                .collect();
            assert!(merged.len() <= 1, "{at}: {} merge-backs", merged.len());
            assert!(
                merged.is_empty() || emptied.is_empty(),
                "{at}: a merge-back beside a retire"
            );
            retires += emptied.len();
            merges += merged.len();
            splits += live
                .iter()
                .filter(|n| !before.iter().any(|(b, _)| b == *n))
                .count();

            // Every base, by its block n (an id's low 64 bits).
            let mut bases = Vec::new();
            for (key, (_, raw)) in &k.records {
                let parsed = parse(key).expect("a list key");
                if let (Sub::Block, None, Some(id)) = (parsed.sub, parsed.slot, parsed.list_id) {
                    let len = open(raw).expect("base envelope").payload.len();
                    assert!(len <= B, "{at}: a base of {len} bytes");
                    bases.push((u64::try_from(id & u128::from(u64::MAX)).expect("low"), len));
                }
            }
            assert_eq!(bases.len(), live.len(), "{at}: one base per live block");
            if let [n] = merged[..] {
                // The pair is written to the block left of the one that went: the block before
                // its old successor now, or the last block when it had none.
                let i = before
                    .iter()
                    .position(|(b, _)| *b == n)
                    .expect("it was there");
                let j = before.get(i + 1).map_or(live.len(), |(s, _)| {
                    live.iter().position(|l| l == s).expect("live")
                });
                let left = live[j - 1];
                let len = bases.iter().find(|(m, _)| *m == left).expect("a base").1;
                assert!(4 * len <= 3 * B, "{at}: a merged base of {len} bytes");
            }
        }
    }
    eprintln!("w2 model: {steps} steps, {refused} refused, {splits} splits, {retires} retires, {merges} merges");
    assert!(
        splits > 0 && retires > 0 && merges > 0 && steps > 0,
        "every rule was exercised"
    );
}

/// One line of pushes for an end-split check: random lengths summing to a bit over B.
fn split_line(seed: u64) -> Vec<ListOp> {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut total = 0;
    let mut ops = Vec::new();
    let target = MIN_BLOCK_MAX + 100 + usize::try_from(rng.below(700)).expect("small");
    while total < target {
        let len = 1 + usize::try_from(rng.below(120)).expect("small");
        total += len + 4;
        let value = format!("{seed}.{}{}", ops.len(), "x".repeat(len));
        ops.push(ListOp::Push(text(&value)));
    }
    ops
}

/// W2 (W17 survived the walks): an end split keeps the longest prefix whose base fits B. Over
/// 300 random lines of pushes into a fresh list at 1,024, each long enough to split once, the
/// first block's base fits and would not fit with the next item's entry added.
#[test]
fn w2_an_end_split_keeps_the_longest_prefix_that_fits() {
    const B: usize = MIN_BLOCK_MAX;
    let root = todo();
    let mut checked = 0;
    for seed in 1..=300_u64 {
        let ops = split_line(seed);
        let mut k = Kernel::new();
        let Ok(compiled) = compile_list(&k.snapshot(), &root, Expected::Absent, B, &ops) else {
            continue; // a piece over B: refused, by design (G61)
        };
        k.commit(compiled.compiled());
        let index = block_index(&k, &root);
        if index.len() != 2 {
            continue;
        }
        let kept = index[0].1;
        let page = items(&k.snapshot(), &root, Start::Position(kept), 1).expect("read");
        let next = &page.items[0];
        let n = u64::try_from(next.id & u128::from(u64::MAX)).expect("low");
        let entry = Value::Array(vec![
            Value::Integer(rdb_value::value::Int::new(i128::from(n)).expect("int")),
            next.value.clone(),
        ]);
        let entry_len = encode(&entry).expect("encode").len();
        let base = k
            .records
            .iter()
            .find(|(key, _)| {
                let p = parse(key).expect("key");
                p.sub == Sub::Block
                    && p.slot.is_none()
                    && p.list_id.map(|id| id & u128::from(u64::MAX)) == Some(u128::from(index[0].0))
            })
            .map(|(_, (_, raw))| open(raw).expect("base").payload.len())
            .expect("block 0's base");
        assert!(base <= B, "seed {seed}: kept base {base}");
        // One more entry adds its bytes, and a byte of array head at 24 items (CBOR).
        let grow = usize::from(kept + 1 == 24);
        assert!(
            base + entry_len + grow > B,
            "seed {seed}: {kept} kept, base {base}, next entry {entry_len}"
        );
        checked += 1;
    }
    assert!(checked > 100, "only {checked} lines split once");
}

/// W17 pinned: seed 3's line, where the end cut sits on the binary search's last step (a search
/// that stops one step early keeps 12). The exact shape, count and bytes per block, from the root.
#[test]
fn w2_an_end_split_on_the_search_bound_has_this_shape() {
    let root = todo();
    let mut k = Kernel::new();
    let compiled = compile_list(
        &k.snapshot(),
        &root,
        Expected::Absent,
        MIN_BLOCK_MAX,
        &split_line(3),
    )
    .expect("one end split");
    k.commit(compiled.compiled());
    assert_eq!(block_shape(&k, &root), [(13, 950), (4, 356)]);
}

/// W2 (W18 survived the walks): halves takes the first of two equally good cuts (G36). A
/// non-last block that folds over B with an odd number of equal-size entries has two cuts with
/// the same gap; the left half keeps the smaller one.
#[test]
fn w2_halves_takes_the_first_of_two_equal_cuts() {
    const B: usize = MIN_BLOCK_MAX;
    let root = todo();
    let mut k = Kernel::new();
    // 23 throwaway items, then none: every later item n is 24 or more, so 40-char values all
    // encode to entries of one size.
    let zs: Vec<String> = (0..23).map(|i| format!("z{i}")).collect();
    let zs: Vec<&str> = zs.iter().map(String::as_str).collect();
    made(&mut k, &root, B, &zs);
    write(&mut k, &root, B, &vec![ListOp::Remove { at: 0 }; 23]);
    let value = |i: usize| text(&format!("{i:0>40}"));
    let pushes: Vec<ListOp> = (0..30).map(|i| ListOp::Push(value(i))).collect();
    write(&mut k, &root, B, &pushes);
    let index = block_index(&k, &root);
    assert_eq!(index.len(), 2, "an end split");
    let first = index[0].1;
    // Enough inserts into the first block to fold it, leaving an odd count over B.
    let inserts = if (first + 7) % 2 == 1 { 7 } else { 8 };
    let ops: Vec<ListOp> = (0..inserts)
        .map(|i| ListOp::Insert {
            at: 0,
            value: value(100 + i),
        })
        .collect();
    write(&mut k, &root, B, &ops);
    let index = block_index(&k, &root);
    let total = first + u64::try_from(inserts).expect("small");
    assert_eq!(index.len(), 3, "the first block split in halves: {index:?}");
    assert_eq!(index[0].1 + index[1].1, total, "{index:?}");
    assert_eq!(
        index[0].1,
        (total - 1) / 2,
        "the first of the two equal cuts: {index:?}"
    );
    // Every value is 40 chars, 42 bytes encoded: the two halves are exactly this.
    let left = (total - 1) / 2;
    assert_eq!(
        block_shape(&k, &root)[..2],
        [(left, 42 * left), (left + 1, 42 * (left + 1))]
    );
}

/// The root's block index as `(count, bytes)` per block, in block order.
fn block_shape(k: &Kernel, root: &RootKey) -> Vec<(u64, u64)> {
    let (_, raw) = &k.records[&root.to_bytes()];
    let payload = rdb_value::cbor::decode(open(raw).expect("root envelope").payload).expect("cbor");
    let Value::Map(fields) = payload else {
        panic!("a list root is a map")
    };
    let Some(Value::Array(blocks)) = fields.get(&rdb_value::value::MapKey::new("blocks")) else {
        panic!("a list root has blocks")
    };
    let uint = |v: &Value| match v {
        Value::Integer(i) => u64::try_from(i.get()).expect("unsigned"),
        other => panic!("not an integer: {other:?}"),
    };
    blocks
        .iter()
        .map(|block| match block {
            Value::Array(parts) => (uint(&parts[1]), uint(&parts[2])),
            other => panic!("a block entry is an array: {other:?}"),
        })
        .collect()
}
