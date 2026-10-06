//! Ordered lists (ADR-rdb-0016), through the public API and the shared kernel stand-in.
//!
//! One row, `w3_vectors_are_written_and_read_byte_for_byte`, pins the record bytes and digests
//! of ADR-rdb-0016 rev 6.3. The others assert behaviour, error variants, and the write and call
//! counts the ADR states.

mod common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use common::{text, Kernel, Refused};
use rdb_core::contracts::trace::Version;
use rdb_core::{
    AffinityId, Generation, Mutation, Namespace, Seq, SnapshotHandle, SnapshotRead, TenantId,
};
use rdb_value::cbor::{decode, encode};
use rdb_value::collection::{compile_collection, CollectionKind, ElemOp};
use rdb_value::delta::{ApplyError, Delta, Op, SizeLimit};
use rdb_value::envelope::{open, seal, EnvelopeError, Kind};
use rdb_value::keys::{
    block_key, item_key, parse, root_key, slot_key, KeyError, RootKey, Sub, SUB_BLOCK, SUB_ITEM,
};
use rdb_value::list::{
    compile_list, create_list, drop_list, items, list, slot_op_no, ListOp, Start, Token,
    DEFAULT_BLOCK_MAX, MIN_BLOCK_MAX,
};
use rdb_value::testing::CountingSnapshot;
use rdb_value::value::{Int, Map, MapKey, Value};
use rdb_value::{compile, read, BlockFault, Compiled, Corrupt, Expected, SlotFault, ValueError};

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
/// a list's blocks, change slots and ids are guarded only by the root's version, which a
/// failover can reuse (ADR-rdb-0016 §8). The drop is fenced the same way.
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

/// Tester W1 D1 (ADR-rdb-0016 §3): a block read stays inside the block's pending slots. A tiny
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

/// The root's `bytes` against the items, measured with the codec outside the compile: the sum of
/// each item's encoded value, which is its inline encoding or its record's payload alike
/// (ADR-rdb-0016 §1). No read checks it, so a write that mis-sizes an item passes every read and
/// shows up here only. `values`: the list's values in order, as read.
fn assert_list_bytes(k: &Kernel, root: &RootKey, values: &[Value], at: &str) {
    let found = list(&k.snapshot(), root).expect("read").expect("a list");
    let measured: usize = values
        .iter()
        .map(|v| encode(v).expect("encodes").len())
        .sum();
    assert_eq!(found.bytes, len_u64(measured), "{at}: the list's bytes");
    let counts: u64 = block_shape(k, root).iter().sum();
    assert_eq!(
        counts,
        len_u64(values.len()),
        "{at}: the blocks hold every item"
    );
}

/// D3 (tester W2 BLOCKER, basis 96a42b2), repro 1, at B = 1,024 (inline limit 240): four
/// 230-char pushes, then one delta `push <230> push <300>`. The fold is over B and splits at
/// its end, moving the new 300-char item, whose record exists only in this compile's overlay.
/// It was refused `ItemMissing { id: 6 }`, and every later push that split the same way.
#[test]
fn d3_an_end_split_that_moves_a_new_out_of_line_item_compiles() {
    let root = todo();
    let (a, l) = ("a".repeat(230), "L".repeat(300));
    let mut k = Kernel::new();
    made(&mut k, &root, MIN_BLOCK_MAX, &[]);
    write(
        &mut k,
        &root,
        MIN_BLOCK_MAX,
        &vec![ListOp::Push(text(&a)); 4],
    );
    write(
        &mut k,
        &root,
        MIN_BLOCK_MAX,
        &[ListOp::Push(text(&a)), ListOp::Push(text(&l))],
    );
    let mut want = vec![text(&a); 5];
    want.push(text(&l));
    assert_eq!(values(&k, &root), want);
    assert_eq!(block_shape(&k, &root).len(), 2, "the push split the block");
    assert_list_bytes(&k, &root, &want, "after the split");
}

/// D3 repro 2: a records list at B = 1,024, one push per compile. Push 429 is the first whose
/// fold is over B; the end split moves the item it pushes, which has no stored record yet. It
/// was refused `ItemMissing { id: 429 }`, and the list stayed one block of 428 items for good.
#[test]
fn d3_a_records_list_splits_on_a_single_push() {
    let root = todo();
    let mut k = Kernel::new();
    let made = create_list(&k.snapshot(), &root, true, MIN_BLOCK_MAX, &[]).expect("create");
    k.commit(made.compiled());
    let mut want = Vec::new();
    for i in 1..=430 {
        let value = text(&format!("x{i}"));
        write(&mut k, &root, MIN_BLOCK_MAX, &[ListOp::Push(value.clone())]);
        want.push(value);
    }
    assert_eq!(values(&k, &root), want);
    assert_eq!(block_shape(&k, &root).len(), 2, "push 429 split the block");
    assert_list_bytes(&k, &root, &want, "after the split");
}

/// The same gap, silent: a stored out-of-line item replaced in the delta that then moves it.
/// `[a, a, a, a, L]` (L 300 chars, out of line) takes `insert 0 a, replace 5 M` (M 600 chars).
/// The fold is over B and the end split moves `[a, M]`. Sized from the snapshot, M counted 302
/// bytes, not 602: the new block's `bytes` was 300 short and the kept one 300 over, and since
/// they still summed to the root's, every read passed. Blocks hold no bytes since D4; the list's
/// total still moves by M's new length, as replaced.
#[test]
fn d3_a_split_sizes_an_item_replaced_in_the_same_delta_as_replaced() {
    let root = todo();
    let (a, l, m) = ("a".repeat(230), "L".repeat(300), "M".repeat(600));
    let mut k = Kernel::new();
    made(&mut k, &root, MIN_BLOCK_MAX, &[&a, &a, &a, &a, &l]);
    assert_eq!(block_shape(&k, &root).len(), 1, "one block before");
    write(
        &mut k,
        &root,
        MIN_BLOCK_MAX,
        &[
            ListOp::Insert {
                at: 0,
                value: text(&a),
            },
            ListOp::Replace {
                at: 5,
                value: text(&m),
            },
        ],
    );
    let mut want = vec![text(&a); 5];
    want.push(text(&m));
    assert_eq!(values(&k, &root), want);
    assert_eq!(block_shape(&k, &root).len(), 2, "the delta split the block");
    assert_list_bytes(&k, &root, &want, "after the split");
}

/// A records list at `block_max` grown by lines of `per_line` pushes of `width`-char values
/// until it has two blocks, as the tester's D4 amp-driver grows it. Returns the next value number.
fn two_block_records_list(
    k: &mut Kernel,
    root: &RootKey,
    block_max: usize,
    width: usize,
    per_line: usize,
) -> u64 {
    let made = create_list(&k.snapshot(), root, true, block_max, &[]).expect("create");
    k.commit(made.compiled());
    let mut next = 0_u64;
    while list(&k.snapshot(), root)
        .expect("read")
        .expect("a list")
        .blocks
        < 2
    {
        let ops: Vec<ListOp> = (0..per_line)
            .map(|_| {
                next += 1;
                ListOp::Push(text(&format!("{next:0>width$}")))
            })
            .collect();
        write(k, root, block_max, &ops);
    }
    next
}

/// Flip the last byte of the record at `key`, keeping its version: its digest no longer holds.
fn damage(k: &mut Kernel, key: &Bytes) {
    let (version, raw) = k.records[key].clone();
    let mut bytes = raw.to_vec();
    *bytes.last_mut().expect("a record has bytes") ^= 0xff;
    k.records.insert(key.clone(), (version, Bytes::from(bytes)));
}

/// A [`SnapshotRead`] over `inner` that tallies each call by what it reads: `"get root"`,
/// `"version base"`, `"scan slot"` and so on, from the key it names (a scan by its start key).
struct Recorded<'a> {
    inner: &'a dyn SnapshotRead,
    tally: RefCell<BTreeMap<String, u64>>,
}

impl<'a> Recorded<'a> {
    fn new(inner: &'a dyn SnapshotRead) -> Self {
        Self {
            inner,
            tally: RefCell::new(BTreeMap::new()),
        }
    }

    fn note(&self, call: &str, key: &[u8]) {
        let role = match parse(key) {
            Ok(p) if p.sub == Sub::Root => "root",
            Ok(p) if p.sub == Sub::Item => "item",
            Ok(p) if p.sub == Sub::Block && p.slot.is_none() => "base",
            Ok(p) if p.sub == Sub::Block => "slot",
            _ => "other",
        };
        *self
            .tally
            .borrow_mut()
            .entry(format!("{call} {role}"))
            .or_default() += 1;
    }

    fn tally(&self) -> BTreeMap<String, u64> {
        self.tally.borrow().clone()
    }
}

impl SnapshotRead for Recorded<'_> {
    fn handle(&self) -> SnapshotHandle {
        self.inner.handle()
    }

    fn at(&self) -> Seq {
        self.inner.at()
    }

    fn generation(&self) -> Generation {
        self.inner.generation()
    }

    fn get(&self, ns: Namespace, key: &[u8]) -> Option<Bytes> {
        self.note("get", key);
        self.inner.get(ns, key)
    }

    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        self.note("version", key);
        self.inner.version(ns, key)
    }

    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        self.note("scan", from);
        self.inner.scan(ns, from, limit)
    }
}

/// What a split line reads: its calls, its bytes, and the calls by role ([`Recorded`]).
type SplitReads = (u64, u64, BTreeMap<String, u64>);

/// The calls and the bytes read by the compile of the first line of `per_line` inserts at 0 that
/// splits the first block (not the last) in halves.
fn split_line_reads(block_max: usize, width: usize, per_line: usize) -> SplitReads {
    let root = todo();
    let mut k = Kernel::new();
    two_block_records_list(&mut k, &root, block_max, width, per_line);
    for line in 0..1_000_u64 {
        let blocks = list(&k.snapshot(), &root)
            .expect("read")
            .expect("a list")
            .blocks;
        let ops: Vec<ListOp> = (0..per_line)
            .map(|_| ListOp::Insert {
                at: 0,
                value: text(&format!("{line:0>width$}")),
            })
            .collect();
        let snap = k.snapshot();
        let counting = CountingSnapshot::new(&snap);
        let recorded = Recorded::new(&counting);
        let version = version_of(&k, &root);
        let compiled = compile_list(
            &recorded,
            &root,
            Expected::Version(version),
            block_max,
            &ops,
        )
        .expect("insert line");
        let reads = (counting.calls(), counting.bytes(), recorded.tally());
        k.commit(compiled.compiled());
        if list(&k.snapshot(), &root)
            .expect("read")
            .expect("a list")
            .blocks
            > blocks
        {
            return reads;
        }
    }
    panic!("no line split the first block");
}

/// D4 (tester W2, basis 38b3586; lead ruling L-R186ds): a halves split sized the entries it moves
/// from their item records, so one insert line that split a records list at B = 128 KiB read
/// 44,021 records and 181 MB with 8 KiB items. A split reads no item record now: a bound set by B
/// and the line alone, whatever the item size. At B = 1,024, with 100 inserts a line; at 38b3586
/// the 16-char line read 685 calls and 14,854 bytes.
///
/// The line reads exactly 2 · 100 + 5 calls (W3 ruling: 205 measured against 202 claimed). The 5:
/// the root's `version` (the version check), the root's `get` and `version` (the read), and the
/// base's `get` and `version`. Two `version` calls per insert: its new id's item key, checked
/// for an orphan, then touched. No pending-slot scan, since every line before it folded.
#[test]
fn d4_a_split_reads_within_a_bound_set_by_b_whatever_the_item_size() {
    const B: usize = MIN_BLOCK_MAX;
    for width in [16, 8_192] {
        let (calls, bytes, tally) = split_line_reads(B, width, 100);
        let at = format!("{width}-char items: the split line read {calls} calls, {bytes} bytes");
        let want: BTreeMap<String, u64> = [
            ("version root", 2),
            ("get root", 1),
            ("get base", 1),
            ("version base", 1),
            ("version item", 2 * 100),
        ]
        .into_iter()
        .map(|(call, n)| (call.to_owned(), n))
        .collect();
        assert_eq!(tally, want, "{at}");
        assert_eq!(calls, 2 * 100 + 5, "{at}");
        assert!(bytes <= 2 * len_u64(B), "{at}: bytes");
    }
}

/// The calls a split line may make: those of [`d4_a_split_reads_within_a_bound_set_by_b_whatever_the_item_size`],
/// plus up to two scans of the block's pending slots (two when they wrap past slot 239), and
/// nothing else. Checked by role, so a call of another kind fails even within the count.
fn assert_split_line_bound(reads: &SplitReads, per_line: usize, at: &str) {
    let (calls, _, tally) = reads;
    let scans = tally.get("scan slot").copied().unwrap_or(0);
    assert!(scans <= 2, "{at}: {scans} pending scans");
    for (call, want) in [
        ("version root", 2),
        ("get root", 1),
        ("get base", 1),
        ("version base", 1),
        ("version item", 2 * len_u64(per_line)),
    ] {
        assert_eq!(tally.get(call).copied(), Some(want), "{at}: {call}");
    }
    assert_eq!(tally.len(), 5 + usize::from(scans > 0), "{at}: {tally:?}");
    assert!(*calls <= 2 * len_u64(per_line) + 7, "{at}: calls");
}

/// [`d4_a_split_reads_within_a_bound_set_by_b_whatever_the_item_size`] at the default B, the
/// tester's own case, within [`assert_split_line_bound`] (the tester measured 2 · per line + 7): lines of 120 inserts of 16 chars, and of 64 of 8 KiB, which keeps a line
/// within the write cap. Growing two blocks of 8 KiB items writes several hundred MB of item
/// records, so it runs by hand: `cargo test -p rdb-value --test lists -- --ignored d4_`.
#[test]
#[ignore = "writes several hundred MB of item records; run by hand"]
fn d4_a_split_at_the_default_b_reads_within_a_bound_set_by_b() {
    const B: usize = DEFAULT_BLOCK_MAX;
    for (width, per_line) in [(16, 120), (8_192, 64)] {
        let split = split_line_reads(B, width, per_line);
        let at = format!("{width}-char items: the split line read {split:?}");
        eprintln!("{at}");
        assert_split_line_bound(&split, per_line, &at);
        assert!(split.1 <= 2 * len_u64(B), "{at}: bytes");
    }
}

/// D4, the damage half: a split read every moved item, so one damaged record among them refused
/// an insert that never names it. The last item of the first block moves in any halves split of
/// that block; its record is damaged, and every insert line up to and including the split
/// compiles. The damage stays: reading the item is still refused.
#[test]
fn d4_a_damaged_item_among_the_moved_does_not_refuse_an_insert_that_splits() {
    const B: usize = MIN_BLOCK_MAX;
    let root = todo();
    let mut k = Kernel::new();
    two_block_records_list(&mut k, &root, B, 16, 100);
    let first = block_index(&k, &root)[0].1;
    let page = items(&k.snapshot(), &root, Start::Position(first - 1), 1).expect("read");
    let victim = page.items[0].id;
    damage(&mut k, &item_key(&root, victim));
    let mut split = false;
    for line in 0..100_u64 {
        let blocks = block_index(&k, &root).len();
        let ops: Vec<ListOp> = (0..100)
            .map(|_| ListOp::Insert {
                at: 0,
                value: text(&format!("{line:0>16}")),
            })
            .collect();
        write(&mut k, &root, B, &ops);
        if block_index(&k, &root).len() > blocks {
            split = true;
            break;
        }
    }
    assert!(split, "a line split the first block");
    let position = list(&k.snapshot(), &root)
        .expect("read")
        .expect("a list")
        .count;
    let read = items(
        &k.snapshot(),
        &root,
        Start::Position(0),
        usize::try_from(position).expect("fits"),
    );
    assert!(matches!(read, Err(ValueError::Corrupt(_))), "{read:?}");
}

/// D4, critic F2: a Move sized the item it moves, reading its record although a Move changes no
/// byte total. Unbounded op counts made that D4 again by another op. A Move now reads no item
/// record: moving an item whose record is damaged succeeds, within its block and across two.
#[test]
fn d4_moving_an_item_whose_record_is_damaged_succeeds() {
    const B: usize = MIN_BLOCK_MAX;
    let root = todo();
    let mut k = Kernel::new();
    two_block_records_list(&mut k, &root, B, 16, 100);
    let count = list(&k.snapshot(), &root)
        .expect("read")
        .expect("a list")
        .count;
    let page = items(&k.snapshot(), &root, Start::Position(0), 1).expect("read");
    damage(&mut k, &item_key(&root, page.items[0].id));
    write(&mut k, &root, B, &[ListOp::Move { from: 0, to: 1 }]);
    write(
        &mut k,
        &root,
        B,
        &[ListOp::Move {
            from: 1,
            to: count - 1,
        }],
    );
    let after = list(&k.snapshot(), &root).expect("read").expect("a list");
    assert_eq!(after.count, count);
    let read = items(&k.snapshot(), &root, Start::Position(count - 1), 1);
    assert!(
        matches!(read, Err(ValueError::Corrupt(_))),
        "the damage stays: {read:?}"
    );
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

/// The root's block index: `[n, count, head]` per block, in block order. Each entry has
/// exactly those 3 fields (tester W3 ask: the model checks the index's shape too).
fn block_refs(k: &Kernel, root: &RootKey) -> Vec<[u64; 3]> {
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
            Value::Array(parts) if parts.len() == 3 => {
                [uint(&parts[0]), uint(&parts[1]), uint(&parts[2])]
            }
            other => panic!("a block entry is [n, count, head]: {other:?}"),
        })
        .collect()
}

/// The root's block index: `(n, count)` per block, in block order.
fn block_index(k: &Kernel, root: &RootKey) -> Vec<(u64, u64)> {
    block_refs(k, root)
        .into_iter()
        .map(|[n, count, _]| (n, count))
        .collect()
}

/// Every record under the list's block range belongs to a block the root names, and every slot
/// holds an op no in `1 ..= head` of its block, in slot `op mod 240` (tester W3 ask: no slot key
/// outside op nos 1 … head).
fn assert_block_records(k: &Kernel, root: &RootKey, at: &str) {
    let heads: BTreeMap<u64, u64> = block_refs(k, root)
        .into_iter()
        .map(|[n, _, head]| (n, head))
        .collect();
    let prefix = root.sub_prefix(SUB_BLOCK);
    let under = k
        .records
        .range(Bytes::from(prefix.clone())..)
        .take_while(|(key, _)| key.starts_with(&prefix));
    // This runs after every step of every model run, so it reads the key's tail and the payload
    // past the header directly: `parse` and `open`'s digest doubled the random model's time.
    for (key, (_, raw)) in under {
        let (id, slot) = match &key[prefix.len()..] {
            [id @ .., slot] if id.len() == 16 => (id, Some(*slot)),
            id if id.len() == 16 => (id, None),
            tail => panic!("{at}: a block-range key with a {}-byte tail", tail.len()),
        };
        let n = u64::from_be_bytes(id[8..].try_into().expect("8 bytes"));
        let Some(&head) = heads.get(&n) else {
            panic!("{at}: a record of block {n}, which the root does not name")
        };
        if let Some(slot) = slot {
            let payload = &raw[rdb_value::envelope::HEADER_LEN..];
            let op = slot_op_no(&rdb_value::cbor::decode(payload).expect("cbor")).expect("a slot");
            assert!(
                (1..=head).contains(&op) && op % 240 == u64::from(slot),
                "{at}: slot {slot} of block {n} holds op {op}; its head is {head}"
            );
        }
    }
}

/// The id of every record under the list's item range (tester W3 G2: the model checks that these
/// are exactly the out-of-line entries, so a Remove or Replace that leaves a record behind fails
/// on the record, not on a counter).
fn item_records(k: &Kernel, root: &RootKey) -> BTreeSet<u128> {
    let prefix = root.sub_prefix(SUB_ITEM);
    k.records
        .range(Bytes::from(prefix.clone())..)
        .take_while(|(key, _)| key.starts_with(&prefix))
        .map(|(key, _)| {
            let id: [u8; 16] = key[prefix.len()..].try_into().expect("a 16-byte item id");
            u128::from_be_bytes(id)
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

/// W2 model check (coordinator, after W10–W12 survived): random deltas of pushes, inserts, removes,
/// moves and replaces at `block_max` 1,024, a growing phase then a shrinking one, so blocks fill,
/// split, empty, retire and merge. Every committed step must match a plain `Vec` (same values, same
/// order, same count, and the same ids: Move and Replace keep an item's id, a new item's id is
/// above every id read before), keep every block base at or under B, and change the block index
/// only as §4 allows: the blocks the ops emptied retire (all but the first when the list empties),
/// at most one more block goes (one merge-back), none beside a retire, and a merged-into block's
/// base is at most ¾ · B. A refused delta (`TooLarge`, `TooManyWrites`) leaves the model as it was.
/// 8 seeds here, 0.63–0.72 s in a debug build (measured 2026-10-06, 0.71–0.72 s with the
/// item-record check, 0.65 s alone with the id check; cut from 20 to stay under 1 s, ruling
/// L-R186dw); 300 in the ignored `w2_model_300_seeds`, run by hand: `cargo test -p rdb-value --test
/// lists -- --ignored`.
#[test]
fn w2_model_random_deltas_match_a_vec_and_keep_the_block_rules() {
    model_run(1..=8, SMALL);
}

/// [`w2_model_random_deltas_match_a_vec_and_keep_the_block_rules`] over 300 seeds (about 17 s).
#[test]
#[ignore = "about 17 s in a debug build; run by hand"]
fn w2_model_300_seeds() {
    model_run(1..=300, SMALL);
}

/// D3 (tester W2 BLOCKER): the model above never made an out-of-line item, so it could not see
/// a split that sizes one from the snapshot. This run draws values small, around the inline
/// limit (240 at B = 1,024) and well over it, and aims half its moves and replaces at items
/// added earlier in the same delta, whose records exist only in the compile's overlay. 6 seeds
/// here, 0.52–0.68 s in a debug build (measured 2026-10-06; cut from 12 to stay under 1 s,
/// ruling L-R186dw); 300 in the ignored `d3_model_300_seeds`.
#[test]
fn d3_model_mixed_sizes_and_same_delta_targets() {
    model_run(1..=6, MIXED);
}

/// D3 repro 2's shape in the model: a records list, so every item is out of line, with deltas
/// long enough to fill, split, empty and merge blocks of bare entries. Seed 3 alone, the one of
/// seeds 1–6 that splits, retires and merges by itself: 0.21–0.26 s in a debug build (measured
/// 2026-10-06; cut from seeds 1–4 to stay under 1 s, ruling L-R186dw); 300 in the ignored
/// `d3_model_300_seeds`.
#[test]
fn d3_model_records_list() {
    model_run(3..=3, RECORDS);
}

/// Both D3 runs over 300 seeds, by hand: `cargo test -p rdb-value --test lists -- --ignored`.
#[test]
#[ignore = "slow in a debug build; run by hand"]
fn d3_model_300_seeds() {
    model_run(1..=300, MIXED);
    model_run(1..=300, RECORDS);
}

/// What a model run draws. [`SMALL`] is the W2 run unchanged, draw for draw: inline values only.
#[derive(Clone, Copy)]
struct Mode {
    /// A records list: every item out of line.
    records: bool,
    /// Values of mixed sizes, and moves and replaces aimed at items added in the same delta.
    mixed: bool,
    steps: u64,
    /// The first `growing` steps add more than they remove.
    growing: u64,
    /// Ops per delta: `1 ..= max_ops`.
    max_ops: u64,
    /// Percent of ops that add while growing, and after.
    grow_add: u64,
    shrink_add: u64,
}

const SMALL: Mode = Mode {
    records: false,
    mixed: false,
    steps: 60,
    growing: 30,
    max_ops: 6,
    grow_add: 60,
    shrink_add: 25,
};

const MIXED: Mode = Mode {
    mixed: true,
    ..SMALL
};

const RECORDS: Mode = Mode {
    records: true,
    mixed: true,
    steps: 30,
    growing: 12,
    max_ops: 120,
    grow_add: 85,
    shrink_add: 10,
};

/// A fresh value for the model: in [`SMALL`] as the W2 run drew it; mixed, one of a short
/// value, one within 16 bytes of the inline limit either side, or one of 300 to 700 bytes.
fn model_value(rng: &mut Rng, mode: Mode, name: &str) -> Value {
    let pad = if mode.mixed {
        match rng.below(3) {
            0 => rng.below(90),
            1 => (222 + rng.below(32)).saturating_sub(len_u64(name.len())),
            _ => 300 + rng.below(400),
        }
    } else {
        rng.below(90)
    };
    text(&format!(
        "{name}{}",
        "x".repeat(usize::try_from(pad).expect("small"))
    ))
}

/// With `mode.mixed`, half the time a position holding an item added in this delta.
fn model_target(rng: &mut Rng, mode: Mode, fresh: &[bool], hits: &mut usize) -> Option<u64> {
    if !mode.mixed || rng.below(2) == 0 {
        return None;
    }
    let added: Vec<usize> = (0..fresh.len()).filter(|&i| fresh[i]).collect();
    if added.is_empty() {
        return None;
    }
    *hits += 1;
    Some(len_u64(added[index(rng.below(len_u64(added.len())))]))
}

fn model_run(seeds: std::ops::RangeInclusive<u64>, mode: Mode) {
    const B: usize = MIN_BLOCK_MAX;
    let root = todo();
    let (mut merges, mut retires, mut splits, mut refused, mut steps) = (0, 0, 0, 0, 0);
    let (mut fresh_hits, mut out_of_line) = (0, 0);
    for seed in seeds {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut k = Kernel::new();
        let made = create_list(&k.snapshot(), &root, mode.records, B, &[]).expect("create");
        k.commit(made.compiled());
        let mut model: Vec<Value> = Vec::new();
        // Each item's id, `None` until a read shows the id this delta minted for it; and the
        // highest id read so far, which every newly minted id must pass (ids are never reused).
        let mut ids: Vec<Option<u128>> = Vec::new();
        let mut high = 0_u128;
        let mut minted = 0_u64;
        for step in 0..mode.steps {
            let growing = step < mode.growing;
            let before = block_index(&k, &root);
            // Each block's post-op count, placed as the compile places ops.
            let mut counts: Vec<u64> = before.iter().map(|(_, count)| *count).collect();
            let mut next = model.clone();
            let mut next_ids = ids.clone();
            // Whether each item of `next` was added by this delta.
            let mut fresh = vec![false; next.len()];
            let mut ops = Vec::new();
            for _ in 0..=rng.below(mode.max_ops) {
                let len = len_u64(next.len());
                let roll = rng.below(100);
                let add = roll
                    < if growing {
                        mode.grow_add
                    } else {
                        mode.shrink_add
                    };
                if add || len == 0 {
                    minted += 1;
                    let value = model_value(&mut rng, mode, &format!("{seed}.{minted}"));
                    if rng.below(2) == 0 {
                        *counts.last_mut().expect("a list has a block") += 1;
                        next.push(value.clone());
                        next_ids.push(None);
                        fresh.push(true);
                        ops.push(ListOp::Push(value));
                    } else {
                        let at = rng.below(len + 1);
                        let i = locate(&counts, at, true);
                        counts[i] += 1;
                        next.insert(index(at), value.clone());
                        next_ids.insert(index(at), None);
                        fresh.insert(index(at), true);
                        ops.push(ListOp::Insert { at, value });
                    }
                } else if roll < 80 {
                    let at = rng.below(len);
                    let i = locate(&counts, at, false);
                    counts[i] -= 1;
                    next.remove(index(at));
                    next_ids.remove(index(at));
                    fresh.remove(index(at));
                    ops.push(ListOp::Remove { at });
                } else if roll < 92 {
                    let (from, to) = (rng.below(len), rng.below(len));
                    let from =
                        model_target(&mut rng, mode, &fresh, &mut fresh_hits).unwrap_or(from);
                    let i = locate(&counts, from, false);
                    counts[i] -= 1;
                    let i = locate(&counts, to, true);
                    counts[i] += 1;
                    let value = next.remove(index(from));
                    next.insert(index(to), value);
                    let id = next_ids.remove(index(from));
                    next_ids.insert(index(to), id);
                    let added = fresh.remove(index(from));
                    fresh.insert(index(to), added);
                    ops.push(ListOp::Move { from, to });
                } else {
                    let at = rng.below(len);
                    let at = model_target(&mut rng, mode, &fresh, &mut fresh_hits).unwrap_or(at);
                    minted += 1;
                    let value = if mode.mixed {
                        model_value(&mut rng, mode, &format!("{seed}.{minted}r"))
                    } else {
                        text(&format!("{seed}.{minted}r"))
                    };
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
            ids = next_ids;
            let page = items(&k.snapshot(), &root, Start::Position(0), usize::MAX).expect("read");
            let bare: BTreeSet<u128> = page
                .items
                .iter()
                .filter(|item| !item.inline)
                .map(|item| item.id)
                .collect();
            assert_eq!(item_records(&k, &root), bare, "{at}: item records");
            out_of_line += bare.len();
            assert!(
                !mode.records || page.items.iter().all(|item| !item.inline),
                "{at}: an inline entry in a records list"
            );
            let read_ids: Vec<u128> = page.items.iter().map(|item| item.id).collect();
            let read: Vec<Value> = page.items.into_iter().map(|item| item.value).collect();
            assert_eq!(read, model, "{at}: items");
            assert_eq!(page.list.count, len_u64(model.len()), "{at}: count");
            // A Move or Replace keeps its item's id; a new item gets an id above every one read.
            for (pos, (want, got)) in ids.iter().zip(&read_ids).enumerate() {
                match want {
                    Some(want) => assert_eq!(got, want, "{at}: the id at {pos}"),
                    None => assert!(
                        *got > high,
                        "{at}: new id {got} at {pos} is not above {high}"
                    ),
                }
            }
            let distinct: BTreeSet<u128> = read_ids.iter().copied().collect();
            assert_eq!(distinct.len(), read_ids.len(), "{at}: an id twice");
            high = read_ids.iter().copied().fold(high, u128::max);
            ids = read_ids.into_iter().map(Some).collect();
            assert_list_bytes(&k, &root, &model, &at);
            assert_block_records(&k, &root, &at);

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
    eprintln!(
        "model (records {}, mixed {}): {steps} steps, {refused} refused, {splits} splits, \
         {retires} retires, {merges} merges, {fresh_hits} same-delta targets, {out_of_line} \
         out-of-line items read",
        mode.records, mode.mixed
    );
    assert!(
        splits > 0 && retires > 0 && merges > 0 && steps > 0,
        "every rule was exercised"
    );
    if mode.mixed {
        assert!(
            fresh_hits > 0 && out_of_line > 0,
            "same-delta targets and out-of-line items were exercised"
        );
    }
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

/// Tester W2: a `TooLarge{List}` refusal tells the caller what to do. A line of pushes that
/// overfills one block region at B = 1,024 is refused, store unchanged, and the message says
/// the delta is too large for one transaction and should be split.
#[test]
fn w2_a_block_over_b_refusal_says_to_split_the_delta() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, MIN_BLOCK_MAX, &["seed"]);
    let before = k.records.clone();
    let ops: Vec<ListOp> = (0..60)
        .map(|i| ListOp::Push(text(&format!("{i:0>40}"))))
        .collect();
    let refused = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version_of(&k, &root)),
        MIN_BLOCK_MAX,
        &ops,
    )
    .expect_err("a line that overfills one block region is refused");
    assert!(
        matches!(
            refused,
            ValueError::Apply(ApplyError::TooLarge {
                limit: SizeLimit::List { .. }
            })
        ),
        "{refused:?}"
    );
    let message = refused.to_string();
    assert!(
        message.contains("too large for one transaction") && message.contains("split"),
        "{message}"
    );
    assert_eq!(k.records, before, "a refused compile writes nothing");
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
            continue; // a piece over B: refused, by design (ADR-rdb-0016 §4)
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
/// that stops one step early keeps 12). The exact shape, the count per block, from the root.
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
    assert_eq!(block_shape(&k, &root), [13, 4]);
}

/// W2 (W18 survived the walks): halves takes the first of two equally good cuts (§4). A
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
    // The two halves are exactly this.
    let left = (total - 1) / 2;
    assert_eq!(block_shape(&k, &root)[..2], [left, left + 1]);
}

/// The root's block index as the count per block, in block order.
fn block_shape(k: &Kernel, root: &RootKey) -> Vec<u64> {
    block_index(k, root)
        .into_iter()
        .map(|(_, count)| count)
        .collect()
}

// ---- W3 -----------------------------------------------------------------------------------------

/// Lead ruling (W3 naming): a list block or slot record at a list root key is damage, named
/// `ListRecordAtRoot` as read, compile, blob, GC and dump name it. Items, the summary, a write and
/// a drop each named it `ListRoot(..)`. One row per command.
#[test]
fn w3_a_block_or_slot_record_at_a_list_root_is_list_record_at_root() {
    let root = todo();
    for kind in [Kind::ListBlock, Kind::ListSlot] {
        let mut k = Kernel::new();
        made(&mut k, &root, MIN_BLOCK_MAX, &["a"]);
        let version = version_of(&k, &root);
        let planted = seal(kind, &encode(&text("x")).unwrap()).unwrap();
        k.records.insert(root.to_bytes(), (version, planted));
        let snap = k.snapshot();
        let named = ValueError::Corrupt(Corrupt::ListRecordAtRoot { found: kind });
        let push = [ListOp::Push(text("x"))];
        let rows: [(&str, Result<(), ValueError>); 4] = [
            (
                "items",
                items(&snap, &root, Start::Position(0), 10).map(|_| ()),
            ),
            ("list", list(&snap, &root).map(|_| ())),
            (
                "write",
                compile_list(
                    &snap,
                    &root,
                    Expected::Version(version),
                    MIN_BLOCK_MAX,
                    &push,
                )
                .map(|_| ()),
            ),
            ("drop", drop_list(&snap, &root, version).map(|_| ())),
        ];
        let wrong: Vec<String> = rows
            .into_iter()
            .filter(|(_, got)| *got != Err(named.clone()))
            .map(|(command, got)| format!("{command}: {got:?}"))
            .collect();
        assert!(wrong.is_empty(), "{kind:?}: {wrong:#?}");
    }
}

// ---- W3: vectors -------------------------------------------------------------------------------

/// The records the vectors pin: the list `todo` at the default B, created empty, then `push milk`,
/// then `push eggs, insert 0 bread`, then `push` a 300-char value (out of line). Key, then value.
const VECTOR_STEPS: [&[(&str, &str)]; 4] = [
    &[
        (
            "000000010000000000000001746f646f000100",
            "01060101000000301d861766b7f611e972cb3c5a3ae02ffd6400bb3685697a184a4aec491debbd37a6646e657874016473656564006562797465730065636f756e740066626c6f636b738183000000677265636f726473f4",
        ),
        (
            "000000010000000000000001746f646f00010300000000000000000000000000000000",
            "0107010100000010c905d12bc7dc3ca16fc36c95c609b23eac40c4e741c078037ee73e7649be6beca2656974656d738066666f6c64656400",
        ),
    ],
    &[
        (
            "000000010000000000000001746f646f000100",
            "01060101000000307c9b258966890d35c95bb61d2aa334ef39dfc272743eb4bbf35b6574493c0efda6646e657874026473656564006562797465730565636f756e740166626c6f636b738183000101677265636f726473f4",
        ),
        (
            "000000010000000000000001746f646f00010300000000000000000000000000000000",
            "0107010100000017120ddc0df7288f0edd5ea1ca9f96a51be78339ead31c79ed624d4942d5c0bf20a2656974656d73818201646d696c6b66666f6c64656401",
        ),
    ],
    &[
        (
            "000000010000000000000001746f646f000100",
            "0106010100000030be6b47987ccbb163c9ad445c2d5a477beb292bc33233a13c5d5013f2384ae790a6646e657874046473656564006562797465731065636f756e740366626c6f636b738183000303677265636f726473f4",
        ),
        (
            "000000010000000000000001746f646f00010300000000000000000000000000000000",
            "01070101000000262694db072a53dabc57dfcbc0c46dba06b887c1d384d5600227db85062bf2c7efa2656974656d738382036562726561648201646d696c6b8202646567677366666f6c64656403",
        ),
    ],
    &[
        (
            "000000010000000000000001746f646f000100",
            "01060101000000328cea08afdbd63a2eb69afec80576674ba701a4e2a16d578f449b4a519d36ace5a6646e6578740564736565640065627974657319013f65636f756e740466626c6f636b738183000404677265636f726473f4",
        ),
        (
            "000000010000000000000001746f646f00010200000000000000000000000000000004",
            // The 300 `o`s follow, added in `vector_records`.
            "010101010000012fba493bb045ff78fae58555902376181ee354a5fa1523fee92aca07fe1e07d0a779012c",
        ),
        (
            "000000010000000000000001746f646f0001030000000000000000000000000000000004",
            "010801010000000610637fe3eaa76507b317a7ae1bb817a697adfb720309ca578db522c8e6e9cf39820483000304",
        ),
    ],
];

/// One step's pinned records as bytes.
fn vector_records(step: usize) -> Vec<(Bytes, Bytes)> {
    VECTOR_STEPS[step]
        .iter()
        .map(|(key, value)| {
            let mut value = common::h(value);
            if step == 3 && value[1] == Kind::Document.byte() {
                value.extend("o".repeat(300).bytes());
            }
            (Bytes::from(common::h(key)), Bytes::from(value))
        })
        .collect()
}

/// W3 (tester ask 3; the format changed in W2): the ADR's example list, both ways. The write side:
/// each of four compiles puts exactly the pinned records, byte for byte. Steps 1 and 2's root
/// digests are the ADR's. The read side: a store holding only the pinned records reads back the
/// four items, in order, with their ids, versions and placement, and the root's summary.
#[test]
fn w3_vectors_are_written_and_read_byte_for_byte() {
    let root = todo();
    let mut k = Kernel::new();
    let steps: [Vec<ListOp>; 4] = [
        vec![],
        vec![ListOp::Push(text("milk"))],
        vec![
            ListOp::Push(text("eggs")),
            ListOp::Insert {
                at: 0,
                value: text("bread"),
            },
        ],
        vec![ListOp::Push(text(&"o".repeat(300)))],
    ];
    for (step, ops) in steps.iter().enumerate() {
        let expected = if step == 0 {
            Expected::Absent
        } else {
            Expected::Version(version_of(&k, &root))
        };
        let compiled =
            compile_list(&k.snapshot(), &root, expected, DEFAULT_BLOCK_MAX, ops).expect("compiles");
        let written: Vec<(Bytes, Bytes)> = compiled
            .compiled()
            .mutations
            .iter()
            .map(|m| match m {
                Mutation::Put { key, value, .. } => (key.clone(), value.clone()),
                Mutation::Delete { key, .. } => panic!("step {}: a delete of {key:?}", step + 1),
            })
            .collect();
        assert_eq!(written, vector_records(step), "step {}", step + 1);
        k.commit(compiled.compiled());
    }

    let mut pinned = Kernel::new();
    for (step, records) in (1_u64..).zip((0..4).map(vector_records)) {
        for (key, value) in records {
            pinned.records.insert(key, (step, value));
        }
    }
    pinned.seq = 4;
    assert_eq!(pinned.records, k.records, "the store the steps left");
    let page = items(&pinned.snapshot(), &root, Start::Position(0), usize::MAX).expect("read");
    let read: Vec<(u64, u128, Value, bool, u64)> = page
        .items
        .into_iter()
        .map(|i| (i.position, i.id, i.value, i.inline, i.version))
        .collect();
    assert_eq!(
        read,
        [
            (0, 3, text("bread"), true, 4),
            (1, 1, text("milk"), true, 4),
            (2, 2, text("eggs"), true, 4),
            (3, 4, text(&"o".repeat(300)), false, 4),
        ]
    );
    assert_eq!(
        page.list,
        rdb_value::list::List {
            version: 4,
            count: 4,
            bytes: 319,
            blocks: 1,
            records: false,
        }
    );
    assert!(page.next.is_none());
}

// ---- W3: hand damage ---------------------------------------------------------------------------

fn uint(v: u64) -> Value {
    Value::Integer(Int::from(v))
}

/// A root's block index from `[n, count, head]` triples.
fn refs(blocks: &[[u64; 3]]) -> Value {
    Value::Array(
        blocks
            .iter()
            .map(|b| Value::Array(b.iter().copied().map(uint).collect()))
            .collect(),
    )
}

/// The decoded payload of the record at `key`.
fn payload_of(k: &Kernel, key: &Bytes) -> Value {
    decode(open(&k.records[key].1).expect("an envelope").payload).expect("CBOR")
}

/// Seal `payload` as a `kind` record at `key`, written at `version`.
fn plant(k: &mut Kernel, key: &[u8], version: u64, kind: Kind, payload: &[u8]) {
    let sealed = seal(kind, payload).expect("seal");
    k.records
        .insert(Bytes::copy_from_slice(key), (version, sealed));
    k.seq = k.seq.max(version);
}

/// Reseal the record at `key` as `kind` holding `value`, keeping its version.
fn reseal(k: &mut Kernel, key: &Bytes, kind: Kind, value: &Value) {
    let version = k.records[key].0;
    plant(k, key, version, kind, &encode(value).expect("encode"));
}

/// Edit the map payload of the record at `key`, and reseal it as `kind`.
fn edit_map(k: &mut Kernel, key: &Bytes, kind: Kind, edit: impl FnOnce(&mut Map)) {
    let Value::Map(mut fields) = payload_of(k, key) else {
        panic!("a map payload")
    };
    edit(&mut fields);
    reseal(k, key, kind, &Value::Map(fields));
}

/// Edit q's root map.
fn edit_q_root(k: &mut Kernel, edit: impl FnOnce(&mut Map)) {
    edit_map(k, &q().to_bytes(), Kind::List, edit);
}

/// Edit q's base map.
fn edit_q_base(k: &mut Kernel, edit: impl FnOnce(&mut Map)) {
    edit_map(k, &q_base(), Kind::ListBlock, edit);
}

/// Reseal q's slot 22, the pending op 22 `[22, [0, 21, [22, "b22"]]]`, holding `value`.
fn q_slot_22(k: &mut Kernel, value: &Value) {
    reseal(k, &q_slot(22), Kind::ListSlot, value);
}

/// `[op no, [code, at, entry]]`, the entry `[n, value]`.
fn slot_op(op_no: Value, code: u64, at: u64, entry: Option<(u64, &str)>) -> Value {
    let mut op = vec![uint(code), uint(at)];
    if let Some((n, value)) = entry {
        op.push(Value::Array(vec![uint(n), text(value)]));
    }
    Value::Array(vec![op_no, Value::Array(op)])
}

/// Move the record at `key` to `version`, and the kernel's seq up to it.
fn set_version(k: &mut Kernel, key: &Bytes, version: u64) {
    let raw = k.records[key].1.clone();
    k.records.insert(key.clone(), (version, raw));
    k.seq = k.seq.max(version);
}

fn field(name: &str) -> MapKey {
    MapKey::new(name)
}

fn q() -> RootKey {
    root_key(TenantId(1), AffinityId(1), b"q")
}

fn e() -> RootKey {
    root_key(TenantId(1), AffinityId(1), b"e")
}

/// q's ids are seeded 0: it is made first.
fn q_base() -> Bytes {
    block_key(&q(), 0)
}

fn q_slot(op: u8) -> Bytes {
    slot_key(&q(), 0, op)
}

/// Item 23, the 300-char value at position 22.
fn q_item() -> Bytes {
    item_key(&q(), 23)
}

/// e's ids are seeded 4: the kernel's seq when it is made.
fn e_id(n: u64) -> u128 {
    (4 << 64) | u128::from(n)
}

fn e_base() -> Bytes {
    block_key(&e(), e_id(0))
}

/// `key` with `tail` appended.
fn with_tail(key: &[u8], tail: &[u8]) -> Vec<u8> {
    [key, tail].concat()
}

/// The tester's damage store (W1 `dbase.store`, `dmg.sh`): list `q` at B = 1,024, items 1–20
/// folded in its one base and ops 21–23 pending in slots 21–23, item 23 a 300-char value out of
/// line; and beside it list `e`, made with one item and emptied, both ops folded, version 6.
fn damage_store() -> Kernel {
    let mut k = Kernel::new();
    let names: Vec<String> = (1..=20).map(|i| format!("a{i}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut k, &q(), MIN_BLOCK_MAX, &names);
    for value in ["b21".to_owned(), "b22".to_owned(), "x".repeat(300)] {
        write(&mut k, &q(), MIN_BLOCK_MAX, &[ListOp::Push(text(&value))]);
    }
    made(&mut k, &e(), MIN_BLOCK_MAX, &["a"]);
    write(&mut k, &e(), MIN_BLOCK_MAX, &[ListOp::Remove { at: 0 }]);
    for op in 21..=23 {
        assert!(k.records.contains_key(&q_slot(op)), "op {op} is pending");
    }
    assert!(k.records.contains_key(&q_item()), "item 23 is a record");
    assert!(k.records.contains_key(&e_base()), "e's base");
    assert_eq!(
        (version_of(&k, &q()), version_of(&k, &e())),
        (4, 6),
        "the versions the rows name"
    );
    assert_eq!(
        payload_of(&k, &q_slot(22)),
        slot_op(uint(22), 0, 21, Some((22, "b22"))),
        "slot 22"
    );
    k
}

/// What a damage row expects.
enum Want {
    /// q's `items`, `list`, a push and a drop each refuse with this.
    QRoot(ValueError),
    /// q's `items` and a push each refuse with this.
    Q(ValueError),
    /// q's `items`, a remove of position 22 and a replace of it each refuse with this, and a
    /// move of it to 0 compiles (D2; since D4 a move reads no item record).
    Item(ValueError),
    /// e's drop refuses with this, and q reads.
    DropE(ValueError),
    /// q reads and takes a push, and e reads and drops.
    Served,
}

type DamageRow = (&'static str, fn(&mut Kernel), Want);

const fn row(name: &'static str, harm: fn(&mut Kernel), want: Want) -> DamageRow {
    (name, harm, want)
}

/// A command, what it gave, and what it should give.
type Outcome = (&'static str, Result<(), ValueError>, Result<(), ValueError>);

/// The 43 rows of the tester's `dmg.sh` at the current format, with D2's write rows and the
/// root, block and slot shapes it did not reach. Root faults are `ListRoot(..)` by text.
#[allow(clippy::too_many_lines)]
fn damage_rows() -> Vec<DamageRow> {
    use Want::{DropE, Item, QRoot, Served, Q};
    let corrupt = ValueError::Corrupt;
    let root = |what| corrupt(Corrupt::ListRoot(what));
    let block = |fault| {
        corrupt(Corrupt::Block {
            id: 0,
            fault: BlockFault::Shape(fault),
        })
    };
    let q_fault = |fault| corrupt(Corrupt::Block { id: 0, fault });
    let slot = |fault| q_fault(BlockFault::OpBad { op: 22, fault });
    let codec = |bytes: &[u8]| decode(bytes).expect_err("not CBOR");
    let digest = EnvelopeError::DigestMismatch;
    let not_uint = |name| match name {
        "next" => "the list root's next is not an unsigned integer",
        "seed" => "the list root's seed is not an unsigned integer",
        "bytes" => "the list root's bytes is not an unsigned integer",
        _ => "the list root's count is not an unsigned integer",
    };
    const EXACTLY: &str =
        "the list root is not exactly next, seed, bytes, count, blocks and records";
    const REF_SHAPE: &str = "a list root's block entry is not [n, count, head]";
    const ENTRY: &str = "a list block entry is not n or [n, value]";
    const OP: &str =
        "a list change slot is not [op no, [0, at, entry] or [1, at] or [2, at, entry]]";
    vec![
        // The root.
        row(
            "R01 root digest flipped",
            |k| damage(k, &q().to_bytes()),
            QRoot(corrupt(Corrupt::Envelope(digest.clone()))),
        ),
        row(
            "R02 root not a map",
            |k| {
                reseal(
                    k,
                    &q().to_bytes(),
                    Kind::List,
                    &Value::Array(vec![uint(1), uint(2)]),
                )
            },
            QRoot(root("the list root's payload is not a map")),
        ),
        row(
            "R03 records not a bool",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("records"), uint(0));
                })
            },
            QRoot(root("the list root's records is not a bool")),
        ),
        row(
            "R04 five fields",
            |k| {
                edit_q_root(k, |m| {
                    m.remove(&field("records"));
                })
            },
            QRoot(root(EXACTLY)),
        ),
        row(
            "R04b seven fields",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("x"), uint(1));
                })
            },
            QRoot(root(EXACTLY)),
        ),
        row(
            "R05 no blocks",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("blocks"), refs(&[]));
                })
            },
            QRoot(root("the list root has no blocks, or more than 512")),
        ),
        row(
            "R05b blocks not an array",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("blocks"), uint(1));
                })
            },
            QRoot(root("the list root's blocks is not an array")),
        ),
        row(
            "R07 a two-field block entry",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(
                        field("blocks"),
                        Value::Array(vec![Value::Array(vec![uint(0), uint(23)])]),
                    );
                })
            },
            QRoot(root(REF_SHAPE)),
        ),
        row(
            "R07b a four-field block entry",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(
                        field("blocks"),
                        Value::Array(vec![Value::Array(vec![
                            uint(0),
                            uint(23),
                            uint(23),
                            uint(0),
                        ])]),
                    );
                })
            },
            QRoot(root(REF_SHAPE)),
        ),
        row(
            "R07c a block entry's head a string",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(
                        field("blocks"),
                        Value::Array(vec![Value::Array(vec![uint(0), uint(23), text("23")])]),
                    );
                })
            },
            QRoot(root(REF_SHAPE)),
        ),
        row(
            "R07d a block entry not an array",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("blocks"), Value::Array(vec![uint(0)]));
                })
            },
            QRoot(root(REF_SHAPE)),
        ),
        row(
            "R08 count off by one",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("count"), uint(24));
                })
            },
            QRoot(root("the list root's count is not the sum of its blocks'")),
        ),
        row(
            "R09 block n not below next",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("blocks"), refs(&[[24, 23, 23]]));
                })
            },
            QRoot(root("a list root's block n is not below next")),
        ),
        row(
            "R10 next a string",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("next"), text("x"));
                })
            },
            QRoot(root(not_uint("next"))),
        ),
        row(
            "R10b seed a string",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("seed"), text("x"));
                })
            },
            QRoot(root(not_uint("seed"))),
        ),
        row(
            "R10c bytes a string",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("bytes"), text("x"));
                })
            },
            QRoot(root(not_uint("bytes"))),
        ),
        row(
            "R10d count negative",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("count"), Value::Integer(Int::from(-1_i64)));
                })
            },
            QRoot(root(not_uint("count"))),
        ),
        row(
            "R11 root of kind block",
            |k| {
                let p = payload_of(k, &q().to_bytes());
                reseal(k, &q().to_bytes(), Kind::ListBlock, &p);
            },
            QRoot(corrupt(Corrupt::ListRecordAtRoot {
                found: Kind::ListBlock,
            })),
        ),
        row(
            "R12 root payload not CBOR",
            |k| plant(k, &q().to_bytes(), 4, Kind::List, &[0xa6, 0x64, 0x6e, 0x65]),
            QRoot(corrupt(Corrupt::Codec(codec(&[0xa6, 0x64, 0x6e, 0x65])))),
        ),
        row(
            "R13 block counts leave u64",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("blocks"), refs(&[[0, u64::MAX, 23], [1, 1, 0]]));
                })
            },
            QRoot(root("the list root's block counts leave u64")),
        ),
        row(
            "R14 root of kind document",
            |k| {
                let p = payload_of(k, &q().to_bytes());
                reseal(k, &q().to_bytes(), Kind::Document, &p);
            },
            QRoot(ValueError::Apply(ApplyError::KindMismatch {
                found: Kind::Document,
            })),
        ),
        row(
            "R15 one block named twice",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("blocks"), refs(&[[0, 23, 23], [0, 23, 23]]));
                    m.insert(field("count"), uint(46));
                })
            },
            QRoot(root("a list root names one block twice")),
        ),
        row(
            "R16 a block head of u64::MAX",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("blocks"), refs(&[[0, 23, u64::MAX]]));
                });
                edit_q_base(k, |m| {
                    m.insert(field("folded"), uint(u64::MAX - 1));
                });
            },
            QRoot(root("a list root's block head is the largest there is")),
        ),
        row(
            "D11 an empty block beside others",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("next"), uint(25));
                    m.insert(field("blocks"), refs(&[[0, 23, 23], [24, 0, 0]]));
                })
            },
            QRoot(root(
                "the list root names an empty block beside others; only an only block is empty",
            )),
        ),
        // The base.
        row(
            "B01 base digest flipped",
            |k| damage(k, &q_base()),
            Q(q_fault(BlockFault::Envelope(digest.clone()))),
        ),
        row(
            "B02 base of kind slot",
            |k| {
                let p = payload_of(k, &q_base());
                reseal(k, &q_base(), Kind::ListSlot, &p);
            },
            Q(q_fault(BlockFault::NotABlock {
                found: Kind::ListSlot,
            })),
        ),
        row(
            "B03 base missing",
            |k| {
                k.records.remove(&q_base());
            },
            Q(q_fault(BlockFault::Missing)),
        ),
        row(
            "B04 base payload not CBOR",
            |k| plant(k, &q_base(), 1, Kind::ListBlock, &[0xa2, 0xff]),
            Q(q_fault(BlockFault::Codec(codec(&[0xa2, 0xff])))),
        ),
        row(
            "B05 base of three fields",
            |k| {
                edit_q_base(k, |m| {
                    m.insert(field("x"), uint(1));
                })
            },
            Q(block("a list block is not exactly items and folded")),
        ),
        row(
            "B05b base not a map",
            |k| reseal(k, &q_base(), Kind::ListBlock, &text("x")),
            Q(block("a list block is not a map")),
        ),
        row(
            "B05c base items not an array",
            |k| {
                edit_q_base(k, |m| {
                    m.insert(field("items"), uint(1));
                })
            },
            Q(block("a list block is not exactly items and folded")),
        ),
        row(
            "B06 folded a string",
            |k| {
                edit_q_base(k, |m| {
                    m.insert(field("folded"), text("20"));
                })
            },
            Q(block("a list block's folded is not an unsigned integer")),
        ),
        row(
            "B07 a three-field entry",
            |k| {
                edit_q_base(k, |m| {
                    let Some(Value::Array(items)) = m.get_mut(&field("items")) else {
                        panic!("items")
                    };
                    items[0] = Value::Array(vec![uint(1), text("a1"), uint(9)]);
                })
            },
            Q(block(ENTRY)),
        ),
        row(
            "B08 an entry's n a string",
            |k| {
                edit_q_base(k, |m| {
                    let Some(Value::Array(items)) = m.get_mut(&field("items")) else {
                        panic!("items")
                    };
                    items[0] = Value::Array(vec![text("x"), text("a1")]);
                })
            },
            Q(block(ENTRY)),
        ),
        row(
            "B09 folded past head",
            |k| {
                edit_q_base(k, |m| {
                    m.insert(field("folded"), uint(30));
                })
            },
            Q(block("a list block's folded is past its root entry's head")),
        ),
        row(
            "B10 base over 262,144 bytes",
            |k| plant(k, &q_base(), 1, Kind::ListBlock, &vec![0; 262_145]),
            Q(q_fault(BlockFault::TooLarge { len: 262_145 })),
        ),
        row(
            "B11 base newer than the root",
            |k| set_version(k, &q_base(), 99),
            Q(q_fault(BlockFault::NewerThanRoot { block: 99, root: 4 })),
        ),
        row(
            "B12 more than 240 pending",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("blocks"), refs(&[[0, 23, 300]]));
                })
            },
            Q(block("a list block has more than 240 pending ops")),
        ),
        row(
            "B13 replayed count not the root's",
            |k| {
                edit_q_root(k, |m| {
                    m.insert(field("count"), uint(22));
                    m.insert(field("blocks"), refs(&[[0, 22, 23]]));
                })
            },
            Q(block(
                "a list block's replayed count is not its root entry's",
            )),
        ),
        // Pending op 22's slot.
        row(
            "S01 pending slot missing",
            |k| {
                k.records.remove(&q_slot(22));
            },
            Q(q_fault(BlockFault::OpMissing { op: 22 })),
        ),
        row(
            "S02 slot holds op 99",
            |k| q_slot_22(k, &slot_op(uint(99), 0, 21, Some((22, "b22")))),
            Q(q_fault(BlockFault::OpMissing { op: 22 })),
        ),
        row(
            "S03 slot of kind block",
            |k| {
                let p = payload_of(k, &q_slot(22));
                reseal(k, &q_slot(22), Kind::ListBlock, &p);
            },
            Q(slot(SlotFault::NotASlot {
                found: Kind::ListBlock,
            })),
        ),
        row(
            "S04 slot digest flipped",
            |k| damage(k, &q_slot(22)),
            Q(slot(SlotFault::Envelope(digest.clone()))),
        ),
        row(
            "S05 slot payload not CBOR",
            |k| plant(k, &q_slot(22), 3, Kind::ListSlot, &[0x82, 0xff]),
            Q(slot(SlotFault::Codec(codec(&[0x82, 0xff])))),
        ),
        row(
            "S06 op code 3",
            |k| q_slot_22(k, &slot_op(uint(22), 3, 21, Some((22, "b22")))),
            Q(slot(SlotFault::Shape(OP))),
        ),
        row(
            "S07 op at out of range",
            |k| q_slot_22(k, &slot_op(uint(22), 0, 50, Some((22, "b22")))),
            Q(q_fault(BlockFault::OpOutOfRange { op: 22 })),
        ),
        row(
            "S08 a remove with an entry",
            |k| q_slot_22(k, &slot_op(uint(22), 1, 0, Some((5, "x")))),
            Q(slot(SlotFault::Shape(OP))),
        ),
        row(
            "S09 a slot key 240",
            |k| {
                plant(
                    k,
                    &with_tail(&q_base(), &[240]),
                    4,
                    Kind::ListSlot,
                    &encode(&slot_op(uint(240), 1, 0, None)).unwrap(),
                )
            },
            Served,
        ),
        row(
            "S10 slot newer than the root",
            |k| set_version(k, &q_slot(22), 99),
            Served,
        ),
        row(
            "S11 op no negative",
            |k| {
                q_slot_22(
                    k,
                    &slot_op(Value::Integer(Int::from(-1_i64)), 0, 21, Some((22, "b22"))),
                )
            },
            Q(slot(SlotFault::Shape(OP))),
        ),
        row(
            "S12 slot not [op no, op]",
            |k| q_slot_22(k, &uint(22)),
            Q(slot(SlotFault::Shape(OP))),
        ),
        // Item 23, out of line at position 22 (D2: a write that sizes it checks it as a read does).
        row(
            "I01 item missing",
            |k| {
                k.records.remove(&q_item());
            },
            Item(corrupt(Corrupt::ItemMissing { id: 23 })),
        ),
        row(
            "I02 item of kind block",
            |k| {
                let p = payload_of(k, &q_item());
                reseal(k, &q_item(), Kind::ListBlock, &p);
            },
            Item(corrupt(Corrupt::ItemNotDocument {
                found: Kind::ListBlock,
            })),
        ),
        row(
            "I03 item digest flipped",
            |k| damage(k, &q_item()),
            Item(corrupt(Corrupt::Envelope(digest.clone()))),
        ),
        row(
            "I04 item newer than the root",
            |k| set_version(k, &q_item(), 99),
            Item(corrupt(Corrupt::ElementNewerThanRoot {
                element: 99,
                root: 4,
            })),
        ),
        row(
            "I05 item payload not CBOR",
            |k| plant(k, &q_item(), 4, Kind::Document, &[0xff]),
            Item(corrupt(Corrupt::Codec(codec(&[0xff])))),
        ),
        row(
            "I06 an item key with a 17-byte tail",
            |k| plant(k, &with_tail(&q_item(), &[0]), 4, Kind::Document, &[0x01]),
            Served,
        ),
        // Beside the empty list e (drop's orphan checks).
        row(
            "K01 an item beside an empty list",
            |k| plant(k, &item_key(&e(), e_id(1)), 6, Kind::Document, &[0x01]),
            DropE(corrupt(Corrupt::OrphanElement)),
        ),
        row(
            "K02 a block key with an 18-byte tail",
            |k| {
                plant(
                    k,
                    &with_tail(&e_base(), &[0, 0]),
                    6,
                    Kind::ListSlot,
                    &[0x01],
                )
            },
            Served,
        ),
        row(
            "K03 another block beside an empty list",
            |k| {
                plant(
                    k,
                    &block_key(&e(), e_id(9)),
                    6,
                    Kind::ListBlock,
                    &encode(&block_payload(&[], 0)).unwrap(),
                )
            },
            DropE(corrupt(Corrupt::OrphanElement)),
        ),
        row(
            "K04 an item key with a 17-byte tail",
            |k| {
                plant(
                    k,
                    &with_tail(&item_key(&e(), e_id(1)), &[0]),
                    6,
                    Kind::Document,
                    &[0x01],
                )
            },
            DropE(corrupt(Corrupt::Key(KeyError::ListIdTail { len: 17 }))),
        ),
        row(
            "K05 a slot key 240",
            |k| plant(k, &with_tail(&e_base(), &[240]), 6, Kind::ListSlot, &[0x01]),
            DropE(corrupt(Corrupt::Key(KeyError::SlotOutOfRange {
                slot: 240,
            }))),
        ),
        row(
            "K06 a block key with an 18-byte tail past the slots",
            |k| {
                plant(
                    k,
                    &with_tail(&e_base(), &[241, 0]),
                    6,
                    Kind::ListSlot,
                    &[0x01],
                )
            },
            DropE(corrupt(Corrupt::Key(KeyError::ListIdTail { len: 18 }))),
        ),
        row(
            "K07 a block key with a 1-byte tail",
            |k| {
                plant(
                    k,
                    &with_tail(&e().sub_prefix(SUB_BLOCK), &[0]),
                    6,
                    Kind::ListBlock,
                    &[0x01],
                )
            },
            DropE(corrupt(Corrupt::Key(KeyError::ListIdTail { len: 1 }))),
        ),
        row(
            "K08 e's base digest flipped",
            |k| damage(k, &e_base()),
            DropE(corrupt(Corrupt::Block {
                id: e_id(0),
                fault: BlockFault::Envelope(digest.clone()),
            })),
        ),
    ]
}

/// A block payload `{items, folded}` from `[n, value]` entries.
fn block_payload(entries: &[(u64, Value)], folded: u64) -> Value {
    let mut map = Map::new();
    map.insert(
        field("items"),
        Value::Array(
            entries
                .iter()
                .map(|(n, v)| Value::Array(vec![uint(*n), v.clone()]))
                .collect(),
        ),
    );
    map.insert(field("folded"), uint(folded));
    Value::Map(map)
}

fn unit<T>(result: Result<T, ValueError>) -> Result<(), ValueError> {
    result.map(|_| ())
}

/// Every command a row names, against `k`, and what each should give.
fn damage_outcomes(k: &Kernel, want: &Want) -> Vec<Outcome> {
    let snap = k.snapshot();
    let (qr, er) = (q(), e());
    let all = |root: &RootKey| unit(items(&snap, root, Start::Position(0), usize::MAX));
    let on_q = |op: ListOp| {
        let version = version_of(k, &qr);
        unit(compile_list(
            &snap,
            &qr,
            Expected::Version(version),
            MIN_BLOCK_MAX,
            &[op],
        ))
    };
    let push = || on_q(ListOp::Push(text("w")));
    let drop_e = || unit(drop_list(&snap, &er, version_of(k, &er)));
    let mut rows = Vec::new();
    match want {
        Want::QRoot(err) => {
            let refused = Err(err.clone());
            rows.push(("items q", all(&qr), refused.clone()));
            rows.push(("list q", unit(list(&snap, &qr)), refused.clone()));
            rows.push(("push q", push(), refused.clone()));
            rows.push((
                "drop q",
                unit(drop_list(&snap, &qr, version_of(k, &qr))),
                refused,
            ));
        }
        Want::Q(err) => {
            rows.push(("items q", all(&qr), Err(err.clone())));
            rows.push(("push q", push(), Err(err.clone())));
        }
        Want::Item(err) => {
            let at = Q_ITEM_AT;
            rows.push(("items q", all(&qr), Err(err.clone())));
            rows.push(("remove 22", on_q(ListOp::Remove { at }), Err(err.clone())));
            let replace = ListOp::Replace {
                at,
                value: text("short"),
            };
            rows.push(("replace 22", on_q(replace), Err(err.clone())));
            rows.push(("move 22 0", on_q(ListOp::Move { from: at, to: 0 }), Ok(())));
        }
        Want::DropE(err) => {
            rows.push(("drop e", drop_e(), Err(err.clone())));
            rows.push(("items q", all(&qr), Ok(())));
        }
        Want::Served => {
            rows.push(("items q", all(&qr), Ok(())));
            rows.push(("push q", push(), Ok(())));
            rows.push(("drop e", drop_e(), Ok(())));
        }
    }
    if !matches!(want, Want::DropE(_)) {
        rows.push(("items e", all(&er), Ok(())));
        rows.push(("drop e", drop_e(), Ok(())));
    }
    rows
}

/// Item 23's position in q.
const Q_ITEM_AT: u64 = 22;

/// W3 (tester asks 2 and D2): one table over the tester's damage rows. Each row damages a copy
/// of the damage store by hand, then runs every command the row names: a fault is refused by
/// its exact name on every path that reads it and served by none, and damage to q never stops e.
/// Every wrong command of every row is reported at once.
#[test]
fn w3_every_damage_row_is_refused_by_name_on_every_path() {
    let store = damage_store();
    let rows = damage_rows();
    assert_eq!(rows.len(), 65, "every damage row, counted");
    let mut wrong = Vec::new();
    for (name, harm, want) in &rows {
        let mut k = store.clone();
        harm(&mut k);
        for (command, got, expected) in damage_outcomes(&k, want) {
            if got != expected {
                wrong.push(format!("{name}: {command}: got {got:?}, want {expected:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// W3 (N02, N03 survived the walks): a root holds at most 512 blocks. 512 otherwise valid entries
/// read past the root, to the first block missing; 513 are refused at the root, before any.
#[test]
fn w3_a_root_names_at_most_512_blocks() {
    for (blocks, want) in [
        (
            512,
            Err(ValueError::Corrupt(Corrupt::Block {
                id: 100,
                fault: BlockFault::Missing,
            })),
        ),
        (
            513,
            Err(ValueError::Corrupt(Corrupt::ListRoot(
                "the list root has no blocks, or more than 512",
            ))),
        ),
    ] {
        let mut k = damage_store();
        edit_q_root(&mut k, |m| {
            let mut index = vec![[0, 23, 23]];
            index.extend((100..).take(blocks - 1).map(|n| [n, 1, 0]));
            m.insert(field("next"), uint(1_000));
            m.insert(field("count"), uint(23 + len_u64(blocks) - 1));
            m.insert(field("blocks"), refs(&index));
        });
        let summary = list(&k.snapshot(), &q()).map(|l| l.map(|l| l.blocks));
        let read = unit(items(&k.snapshot(), &q(), Start::Position(0), usize::MAX));
        assert_eq!(read, want, "{blocks} blocks");
        if blocks == 512 {
            assert_eq!(summary, Ok(Some(512)));
        } else {
            assert_eq!(summary, want.map(|()| None));
        }
    }
}

// ---- W3: limits, cost and drop -----------------------------------------------------------------

/// A create of `root` at `block_max` with `ops`, on an empty store.
fn create_alone(root: &RootKey, block_max: usize, ops: &[ListOp]) -> Result<(), ValueError> {
    let k = Kernel::new();
    unit(compile_list(
        &k.snapshot(),
        root,
        Expected::Absent,
        block_max,
        ops,
    ))
}

/// The base `{items: [[1, <len chars>]], folded: 1}` whose payload is exactly `target` bytes.
fn base_of(target: usize) -> Vec<u8> {
    let payload =
        |len: usize| encode(&block_payload(&[(1, text(&"p".repeat(len)))], 1)).expect("encode");
    // Past 65,535 chars a text's head is 5 bytes, so the payload grows one byte a char.
    let overhead = payload(100_000).len() - 100_000;
    let bytes = payload(target - overhead);
    assert_eq!(bytes.len(), target);
    bytes
}

/// W3 (tester ask 3, limits; N12, N25–N27 survived or were walk-only): each limit at its edge,
/// both sides, through the library.
#[test]
fn w3_each_limit_holds_at_its_edge() {
    let root = todo();
    let too_large = |limit| Err(ValueError::Apply(ApplyError::TooLarge { limit }));
    // An item record is at most 524,288 bytes. A text of n ≥ 65,536 chars seals to 45 + n.
    let item = |n: usize| ListOp::Push(text(&"i".repeat(n)));
    assert_eq!(
        create_alone(&root, DEFAULT_BLOCK_MAX, &[item(524_243)]),
        Ok(())
    );
    assert_eq!(
        create_alone(&root, DEFAULT_BLOCK_MAX, &[item(524_244)]),
        too_large(SizeLimit::Item)
    );
    assert_eq!(
        create_alone(&root, DEFAULT_BLOCK_MAX, &[item(524_243), item(524_243)]),
        too_large(SizeLimit::Write),
        "two of the largest items pass the 1 MiB write"
    );
    // An object id is at most 3,072 bytes escaped, its 2-byte terminator included; a zero byte
    // escapes to two.
    for (id, want) in [
        (vec![b'a'; 3_070], Ok(())),
        (vec![b'a'; 3_071], too_large(SizeLimit::ObjectId)),
        (vec![0; 1_535], Ok(())),
        (vec![0; 1_536], too_large(SizeLimit::ObjectId)),
    ] {
        let at = format!("{} bytes of {:#04x}", id.len(), id[0]);
        let long = root_key(TenantId(1), AffinityId(1), &id);
        assert_eq!(create_alone(&long, DEFAULT_BLOCK_MAX, &[]), want, "{at}");
    }
    // B is 1,024 to 196,608.
    for (block_max, want) in [
        (
            1_023,
            Err(ValueError::Apply(ApplyError::InvalidBlockSize {
                found: 1_023,
            })),
        ),
        (MIN_BLOCK_MAX, Ok(())),
        (196_608, Ok(())),
        (
            196_609,
            Err(ValueError::Apply(ApplyError::InvalidBlockSize {
                found: 196_609,
            })),
        ),
    ] {
        assert_eq!(create_alone(&root, block_max, &[]), want, "B {block_max}");
    }
    // A records list's create writes the root, the base and a record a push: 255 at most.
    for (pushes, want) in [
        (253, Ok(())),
        (
            254,
            Err(ValueError::Apply(ApplyError::TooManyWrites { writes: 256 })),
        ),
    ] {
        let ops: Vec<ListOp> = (0..pushes)
            .map(|i| ListOp::Push(text(&format!("r{i}"))))
            .collect();
        let k = Kernel::new();
        let made = create_list(&k.snapshot(), &root, true, MIN_BLOCK_MAX, &ops);
        assert_eq!(unit(made), want, "{pushes} pushes");
    }
    // A base of 262,144 payload bytes is read; one more byte is refused before it is decoded.
    for (len, refused) in [(262_144, false), (262_145, true)] {
        let mut k = Kernel::new();
        made(&mut k, &root, MIN_BLOCK_MAX, &["a"]);
        plant(
            &mut k,
            &block_key(&root, 0),
            1,
            Kind::ListBlock,
            &base_of(len),
        );
        let read = items(&k.snapshot(), &root, Start::Position(0), usize::MAX);
        if refused {
            assert_eq!(
                unit(read),
                Err(ValueError::Corrupt(Corrupt::Block {
                    id: 0,
                    fault: BlockFault::TooLarge { len },
                }))
            );
        } else {
            let value = &read.expect("a 262,144-byte base reads").items[0].value;
            assert_eq!(
                encode(value).expect("encode").len(),
                len - 18,
                "its one value"
            );
        }
    }
}

/// The calls one `items` read of `limit` items from `position` made, by role ([`Recorded`]).
fn read_tally(k: &Kernel, root: &RootKey, position: u64, limit: usize) -> BTreeMap<String, u64> {
    let snap = k.snapshot();
    let recorded = Recorded::new(&snap);
    items(&recorded, root, Start::Position(position), limit).expect("read");
    recorded.tally()
}

fn tally_of(calls: &[(&str, u64)]) -> BTreeMap<String, u64> {
    calls
        .iter()
        .map(|(call, n)| ((*call).to_owned(), *n))
        .collect()
}

/// The value bytes of every record of the list at `root`.
fn own_bytes(k: &Kernel, root: &RootKey) -> u64 {
    k.records
        .iter()
        .filter(|(key, _)| parse(key).is_ok_and(|p| p.root() == *root))
        .map(|(_, (_, raw))| len_u64(raw.len()))
        .sum()
}

/// W3 (tester ask 3, cost; ADR-rdb-0016 §6): a point read with no pending op is 4 calls: the
/// root's `get` and `version`, then the base's. An out-of-line item adds its record's 2.
#[test]
fn w3_a_point_read_is_four_calls_and_two_more_out_of_line() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &["a", &"o".repeat(300)]);
    let inline = [
        ("get root", 1),
        ("version root", 1),
        ("get base", 1),
        ("version base", 1),
    ];
    assert_eq!(read_tally(&k, &root, 0, 1), tally_of(&inline), "inline");
    let mut out_of_line = inline.to_vec();
    out_of_line.extend([("get item", 1), ("version item", 1)]);
    assert_eq!(
        read_tally(&k, &root, 1, 1),
        tally_of(&out_of_line),
        "out of line"
    );
}

/// W3 (tester ask 3, cost; D1 extended): beside a 600 KB document, the first key past it, a tiny
/// list opens its own records and at most one foreign one. A create opens exactly the document:
/// its orphan check is one limit-1 scan. A read opens exactly the root, the base and the pending
/// slot. A drop opens the root, the base twice (the block read, then the first orphan check) and
/// the document (the second check), and no stale slot.
#[test]
fn w3_a_tiny_list_opens_its_own_records_and_one_foreign_one_at_most() {
    let tiny = root_key(TenantId(1), AffinityId(1), b"a");
    let doc = root_key(TenantId(1), AffinityId(1), b"b");
    let mut k = Kernel::new();
    let big = encode(&text(&"d".repeat(600_000))).expect("encode");
    plant(&mut k, &doc.to_bytes(), 1, Kind::Document, &big);
    let doc_len = len_u64(k.records[&doc.to_bytes()].1.len());

    let snap = k.snapshot();
    let counting = CountingSnapshot::new(&snap);
    let pushes: Vec<ListOp> = (0..8)
        .map(|i| ListOp::Push(text(&format!("a{i}"))))
        .collect();
    let made =
        compile_list(&counting, &tiny, Expected::Absent, MIN_BLOCK_MAX, &pushes).expect("create");
    assert_eq!(counting.bytes(), doc_len, "create");
    k.commit(made.compiled());
    write(&mut k, &tiny, MIN_BLOCK_MAX, &[ListOp::Push(text("tiny"))]);
    assert_eq!(slots_of(&k, &tiny, 0), 1, "a pending slot");
    let own = own_bytes(&k, &tiny);

    let snap = k.snapshot();
    let counting = CountingSnapshot::new(&snap);
    items(&counting, &tiny, Start::Position(0), usize::MAX).expect("read");
    assert_eq!(counting.bytes(), own, "read");

    write(
        &mut k,
        &tiny,
        MIN_BLOCK_MAX,
        &vec![ListOp::Remove { at: 0 }; 9],
    );
    let stale = k
        .records
        .keys()
        .filter(|key| parse(key).is_ok_and(|p| p.root() == tiny && p.slot.is_some()))
        .count();
    assert_eq!(stale, 1, "the pending slot stays, stale");
    // Seeded 1: the document is at seq 1.
    let base = len_u64(k.records[&block_key(&tiny, 1 << 64)].1.len());
    let root_len = len_u64(k.records[&tiny.to_bytes()].1.len());
    let snap = k.snapshot();
    let counting = CountingSnapshot::new(&snap);
    drop_list(&counting, &tiny, version_of(&k, &tiny)).expect("drop");
    assert_eq!(counting.bytes(), root_len + 2 * base + doc_len, "drop");
}

/// Pushes of `p<i>` for `i` in `from..from + n`.
fn pushes(from: usize, n: usize) -> Vec<ListOp> {
    (from..from + n)
        .map(|i| ListOp::Push(text(&format!("p{i}"))))
        .collect()
}

/// Each slot record of `root`'s block `n`.
fn slots_of(k: &Kernel, root: &RootKey, n: u64) -> usize {
    k.records
        .keys()
        .filter(|key| {
            parse(key).is_ok_and(|p| {
                p.root() == *root
                    && p.slot.is_some()
                    && p.list_id.map(|id| id & u128::from(u64::MAX)) == Some(u128::from(n))
            })
        })
        .count()
}

/// Whether any record of `root`'s block `n` remains: its base or a slot.
fn block_remains(k: &Kernel, root: &RootKey, n: u64) -> bool {
    k.records.keys().any(|key| {
        parse(key).is_ok_and(|p| {
            p.root() == *root
                && p.sub == Sub::Block
                && p.list_id.map(|id| id & u128::from(u64::MAX)) == Some(u128::from(n))
        })
    })
}

/// W3 (tester asks 3 and S12; N20 and N33 were walk-only): 240 pushes in one write are each a
/// slot; the 241st pending op folds the block on the count rule and leaves the 240 slots, stale.
/// After the list is emptied, a drop deletes the root, the base and every slot: 242 deletes,
/// and nothing under the list remains. A recreate seeds its ids from the drop's seq.
#[test]
fn w3_a_drop_deletes_every_slot_and_a_recreate_seeds_from_its_seq() {
    let root = todo();
    let mut k = Kernel::new();
    let names: Vec<String> = (0..1_500).map(|i| format!("{i:0>16}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &names);
    let slotted = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version_of(&k, &root)),
        DEFAULT_BLOCK_MAX,
        &pushes(0, 240),
    )
    .expect("240 pushes");
    assert_eq!(
        slotted.compiled().mutations.len(),
        241,
        "the root and 240 slots"
    );
    k.commit(slotted.compiled());
    let folded = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version_of(&k, &root)),
        DEFAULT_BLOCK_MAX,
        &pushes(240, 1),
    )
    .expect("the 241st");
    let keys: Vec<&Bytes> = folded.compiled().mutations.iter().map(key_of).collect();
    assert_eq!(keys, [&root.to_bytes(), &block_key(&root, 0)], "it folds");
    k.commit(folded.compiled());
    assert_eq!(slots_of(&k, &root, 0), 240, "stale slots");

    write(
        &mut k,
        &root,
        DEFAULT_BLOCK_MAX,
        &vec![ListOp::Remove { at: 0 }; 1_741],
    );
    assert_eq!(slots_of(&k, &root, 0), 240, "an emptying fold leaves them");
    let dropped = drop_list(&k.snapshot(), &root, version_of(&k, &root)).expect("drop");
    let mutations = &dropped.compiled().mutations;
    assert_eq!(mutations.len(), 242);
    assert!(
        mutations
            .iter()
            .all(|m| matches!(m, Mutation::Delete { .. })),
        "every write is a delete"
    );
    let seq = k.commit(dropped.compiled());
    assert!(
        !k.records
            .keys()
            .any(|key| parse(key).is_ok_and(|p| p.root() == root)),
        "nothing under the list remains"
    );
    let remade = compile_list(
        &k.snapshot(),
        &root,
        Expected::Absent,
        DEFAULT_BLOCK_MAX,
        &[ListOp::Push(text("x"))],
    )
    .expect("recreate");
    assert_eq!(remade.ids()[0] >> 64, u128::from(seq), "the seed");
}

/// W3 (tester obs 6; N28, N29 were walk-only): a drop's orphan checks do not see a block key
/// with an 18-byte tail between the base and slot 240 (by design), so the drop leaves it, and
/// the create after it refuses it. Under an absent root, an item, block or slot record refuses
/// a create; an element or chunk record does not.
#[test]
fn w3_a_create_refuses_any_record_under_the_list_ranges() {
    let mut k = damage_store();
    let stray = with_tail(&e_base(), &[0, 0]);
    plant(&mut k, &stray, 6, Kind::ListSlot, &[0x01]);
    let dropped = drop_list(&k.snapshot(), &e(), 6).expect("the drop does not see it");
    k.commit(dropped.compiled());
    assert!(k.records.contains_key(stray.as_slice()), "the stray stays");
    let orphan = Err(ValueError::Corrupt(Corrupt::OrphanElement));
    assert_eq!(create_in(&k, &e()), orphan, "create after the drop");

    let root = root_key(TenantId(1), AffinityId(1), b"o");
    for (what, key, want) in [
        ("item", item_key(&root, 7), orphan.clone()),
        ("block", block_key(&root, 7), orphan.clone()),
        ("slot", slot_key(&root, 7, 3), orphan.clone()),
        (
            "element",
            rdb_value::keys::element_key(&root, &text("k")).expect("a key"),
            Ok(()),
        ),
        (
            "chunk",
            rdb_value::keys::chunk_key(&root, &[0; 16], 0),
            Ok(()),
        ),
    ] {
        let mut k = Kernel::new();
        plant(&mut k, &key, 1, Kind::Document, &[0x01]);
        assert_eq!(create_in(&k, &root), want, "{what}");
    }
}

/// A create of an empty list at `root` against `k`.
fn create_in(k: &Kernel, root: &RootKey) -> Result<(), ValueError> {
    unit(compile_list(
        &k.snapshot(),
        root,
        Expected::Absent,
        MIN_BLOCK_MAX,
        &[],
    ))
}

// ---- W3: the pending run, retire and merge-back ------------------------------------------------

/// W3 (F02 was walk-only; tester asks to extend d1): ops 231–245 pending in slots 231–239, then
/// 0–5. A read takes two scans, one each side of slot 239, and reads every value in order; a
/// write replays the same run.
#[test]
fn w3_pending_ops_that_wrap_past_slot_239_read_in_two_scans() {
    let root = todo();
    let mut k = Kernel::new();
    let names: Vec<String> = (0..230).map(|i| format!("{i:0>16}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &names);
    write(&mut k, &root, DEFAULT_BLOCK_MAX, &pushes(0, 15));
    for op in 231..=245_u64 {
        let slot = u8::try_from(op % 240).expect("a slot");
        assert!(
            k.records.contains_key(&slot_key(&root, 0, slot)),
            "op {op} pending in slot {slot}"
        );
    }
    let mut want: Vec<Value> = names.iter().map(|v| text(v)).collect();
    want.extend((0..15).map(|i| text(&format!("p{i}"))));
    let tally = read_tally(&k, &root, 0, usize::MAX);
    assert_eq!(tally.get("scan slot"), Some(&2), "{tally:?}");
    assert_eq!(values(&k, &root), want);
    write(&mut k, &root, DEFAULT_BLOCK_MAX, &pushes(15, 1));
    want.push(text("p15"));
    assert_eq!(values(&k, &root), want);
}

/// A list `root` at the default B in two blocks: 590 values of 250 chars (inline entries of 256
/// bytes), split at the end; then 240 pushes, each a slot of the second block, and one more,
/// which folds it and leaves 240 stale slots. Returns the values and the second block's `n`.
fn two_blocks_with_stale_slots(k: &mut Kernel, root: &RootKey) -> (Vec<Value>, u64) {
    let names: Vec<String> = (0..590).map(|i| format!("{i:0>250}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(k, root, DEFAULT_BLOCK_MAX, &names);
    write(k, root, DEFAULT_BLOCK_MAX, &pushes(0, 240));
    write(k, root, DEFAULT_BLOCK_MAX, &pushes(240, 1));
    let index = block_index(k, root);
    assert_eq!(index.len(), 2, "{index:?}");
    let second = index[1].0;
    assert_eq!(slots_of(k, root, second), 240, "stale slots");
    let mut values: Vec<Value> = names.iter().map(|v| text(v)).collect();
    values.extend((0..241).map(|i| text(&format!("p{i}"))));
    (values, second)
}

/// W3 (W14 was walk-only): a retire deletes every key its block can hold, the slot of each op
/// no `1 … head` included. Emptying the second block retires it: its base and 240 stale slots go,
/// and no record of it remains.
#[test]
fn w3_a_retire_deletes_its_stale_slots() {
    let root = todo();
    let mut k = Kernel::new();
    let (mut model, second) = two_blocks_with_stale_slots(&mut k, &root);
    let first = block_index(&k, &root)[0].1;
    let removes = len_u64(model.len()) - first;
    let ops = vec![ListOp::Remove { at: first }; index(removes)];
    let compiled = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version_of(&k, &root)),
        DEFAULT_BLOCK_MAX,
        &ops,
    )
    .expect("retire");
    assert_eq!(
        compiled.compiled().mutations.len(),
        242,
        "the root, the base and 240 slots"
    );
    k.commit(compiled.compiled());
    model.truncate(index(first));
    assert_eq!(block_index(&k, &root).len(), 1);
    assert!(
        !block_remains(&k, &root, second),
        "a record of the retired block remains"
    );
    assert_eq!(values(&k, &root), model);
}

/// W3 (W13, W15 were walk-only): a merge-back deletes every key the right block can hold. The
/// first block, emptied to 10 items, merges with the second; the second's base and 240 stale
/// slots go. With 12 new item records in the same write the merge is 255 writes and is taken; with
/// 13 it would be 256, so it is not, and the write compiles unmerged.
#[test]
fn w3_a_merge_back_deletes_the_right_blocks_stale_slots_when_it_fits() {
    let root = todo();
    for (records, merged) in [(0_u64, true), (12, true), (13, false)] {
        let at = format!("{records} new records");
        let mut k = Kernel::new();
        let (mut model, second) = two_blocks_with_stale_slots(&mut k, &root);
        let first = block_index(&k, &root)[0].1;
        let mut ops = vec![ListOp::Remove { at: 0 }; index(first - 10)];
        model.drain(..index(first - 10));
        for i in 0..records {
            let value = text(&format!("{i:0>300}"));
            ops.push(ListOp::Insert {
                at: 0,
                value: value.clone(),
            });
            model.insert(0, value);
        }
        let compiled = compile_list(
            &k.snapshot(),
            &root,
            Expected::Version(version_of(&k, &root)),
            DEFAULT_BLOCK_MAX,
            &ops,
        )
        .expect(&at);
        let writes = compiled.compiled().mutations.len();
        k.commit(compiled.compiled());
        assert_eq!(values(&k, &root), model, "{at}");
        if merged {
            assert_eq!(writes, 243 + index(records), "{at}");
            assert_eq!(block_index(&k, &root).len(), 1, "{at}");
            assert!(
                !block_remains(&k, &root, second),
                "{at}: a record of the right block remains"
            );
        } else {
            assert_eq!(writes, 2 + index(records), "{at}");
            assert_eq!(block_index(&k, &root).len(), 2, "{at}");
            assert_eq!(slots_of(&k, &root, second), 240, "{at}");
        }
    }
}

// ---- W3: fold triggers, the inline limit, keys and the block cap -------------------------------

/// The root's `next`, `count` and its block 0's `head`, from its payload.
fn root_counters(k: &Kernel, root: &RootKey) -> (u64, u64, u64) {
    let Value::Map(fields) = payload_of(k, &root.to_bytes()) else {
        panic!("a map")
    };
    let get = |name| match fields.get(&field(name)) {
        Some(Value::Integer(i)) => u64::try_from(i.get()).expect("unsigned"),
        other => panic!("{name}: {other:?}"),
    };
    (get("next"), get("count"), block_refs(k, root)[0][2])
}

/// W3 (N21 was walk-only; tester S2): a block takes a write's ops as slots until the pending
/// slots' bytes and the new ones pass a quarter of its base's; then it folds. Each push's outcome
/// is predicted from the measured base and slot lengths, and both outcomes are seen.
#[test]
fn w3_a_block_folds_when_its_pending_bytes_pass_a_quarter_of_its_base() {
    let root = todo();
    let mut k = Kernel::new();
    let names: Vec<String> = (1..=20).map(|i| format!("a{i}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut k, &root, MIN_BLOCK_MAX, &names);
    let (mut pending, mut seen) = (0, [0; 2]);
    for i in 21..=40 {
        let value = format!("b{i}");
        let (next, count, head) = root_counters(&k, &root);
        let entry = Value::Array(vec![uint(next), text(&value)]);
        let op = Value::Array(vec![uint(0), uint(count), entry]);
        let new = encode(&Value::Array(vec![uint(head + 1), op]))
            .expect("encode")
            .len();
        let base = open(&k.records[&block_key(&root, 0)].1)
            .expect("base")
            .payload
            .len();
        let fold = 4 * (pending + new) > base;
        let compiled = compile_list(
            &k.snapshot(),
            &root,
            Expected::Version(version_of(&k, &root)),
            MIN_BLOCK_MAX,
            &[ListOp::Push(text(&value))],
        )
        .expect("push");
        let keys: Vec<&Bytes> = compiled.compiled().mutations.iter().map(key_of).collect();
        let want = if fold {
            block_key(&root, 0)
        } else {
            slot_key(&root, 0, u8::try_from((head + 1) % 240).expect("a slot"))
        };
        assert_eq!(
            keys,
            [&root.to_bytes(), &want],
            "push {i}: pending {pending}, new {new}, base {base}"
        );
        k.commit(compiled.compiled());
        pending = if fold { 0 } else { pending + new };
        seen[usize::from(fold)] += 1;
    }
    assert!(seen[0] > 0 && seen[1] > 0, "{seen:?}");
}

/// W3 (N22 was walk-only; ADR-rdb-0016 §3, §4): a base over B always folds, so a write at a
/// smaller B splits it, though its op alone would take a slot. Written at the default B, 30
/// items of 40 chars make a base over 1,024; one push at 1,024 splits it, and no base is over
/// 1,024 after.
#[test]
fn w3_a_base_over_b_folds_and_splits_on_any_write() {
    let root = todo();
    let mut k = Kernel::new();
    let names: Vec<String> = (0..30).map(|i| format!("{i:0>40}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &names);
    let base = |k: &Kernel| {
        open(&k.records[&block_key(&root, 0)].1)
            .expect("base")
            .payload
            .len()
    };
    assert!(base(&k) > MIN_BLOCK_MAX, "{}", base(&k));
    write(&mut k, &root, MIN_BLOCK_MAX, &[ListOp::Push(text("x"))]);
    assert_eq!(block_index(&k, &root).len(), 2);
    for (key, (_, raw)) in &k.records {
        let p = parse(key).expect("a key");
        if p.sub == Sub::Block && p.slot.is_none() {
            let len = open(raw).expect("base").payload.len();
            assert!(len <= MIN_BLOCK_MAX, "a base of {len} bytes");
        }
    }
    let mut want: Vec<Value> = names.iter().map(|v| text(v)).collect();
    want.push(text("x"));
    assert_eq!(values(&k, &root), want);
}

/// W3 (N35–N37 were walk-only; tester S21): a value goes in its block entry up to
/// `min(256, B / 4 − 16)` encoded bytes, and in its own record past it; a records list puts every
/// value in a record. A text of 24–255 chars encodes to 2 + its length.
#[test]
fn w3_an_item_is_inline_up_to_the_limit_b_sets() {
    let root = todo();
    for (block_max, limit) in [
        (MIN_BLOCK_MAX, 240),
        (1_084, 255),
        (1_088, 256),
        (DEFAULT_BLOCK_MAX, 256),
    ] {
        let mut k = Kernel::new();
        let at = "x".repeat(limit - 2);
        let over = "x".repeat(limit - 1);
        made(&mut k, &root, block_max, &[at.as_str(), over.as_str()]);
        let page = items(&k.snapshot(), &root, Start::Position(0), usize::MAX).expect("read");
        let inline: Vec<bool> = page.items.iter().map(|i| i.inline).collect();
        assert_eq!(inline, [true, false], "B {block_max}: limit {limit}");
    }
    let mut k = Kernel::new();
    let made = create_list(
        &k.snapshot(),
        &root,
        true,
        MIN_BLOCK_MAX,
        &[ListOp::Push(text("a"))],
    )
    .expect("create");
    k.commit(made.compiled());
    let page = items(&k.snapshot(), &root, Start::Position(0), usize::MAX).expect("read");
    assert!(!page.items[0].inline, "a records list's one-byte value");
}

/// W3 (F18, F19 were walk-only): a block key takes a 16-byte id, and a slot byte below 240 after
/// it; an item key takes the id only.
#[test]
fn w3_a_block_key_parses_a_slot_below_240_only() {
    let root = todo();
    let base = block_key(&root, 5);
    let slot = |key: &[u8]| parse(key).map(|p| (p.list_id, p.slot));
    assert_eq!(slot(&base), Ok((Some(5), None)));
    assert_eq!(slot(&slot_key(&root, 5, 239)), Ok((Some(5), Some(239))));
    assert_eq!(
        slot(&with_tail(&base, &[240])),
        Err(KeyError::SlotOutOfRange { slot: 240 })
    );
    assert_eq!(
        slot(&with_tail(&base, &[0, 0])),
        Err(KeyError::ListIdTail { len: 18 })
    );
    assert_eq!(
        slot(&with_tail(&item_key(&root, 5), &[0])),
        Err(KeyError::ListIdTail { len: 17 })
    );
}

/// A list `todo` at B = 1,024 planted by hand with 512 blocks, the cap: blocks 0–510 hold one
/// item each, and block 511 holds 30 items of 40 chars, a base over B. Item `n`s start at 512.
fn at_the_block_cap() -> Kernel {
    let root = todo();
    let mut k = Kernel::new();
    let (mut index, mut next, mut bytes) = (Vec::new(), 512_u64, 0);
    for b in 0..512_u64 {
        let count = if b == 511 { 30 } else { 1 };
        let entries: Vec<(u64, Value)> = (0..count)
            .map(|_| {
                next += 1;
                let value = text(&format!("{next:0>40}"));
                bytes += len_u64(encode(&value).expect("encode").len());
                (next, value)
            })
            .collect();
        let payload = encode(&block_payload(&entries, 0)).expect("encode");
        plant(
            &mut k,
            &block_key(&root, u128::from(b)),
            1,
            Kind::ListBlock,
            &payload,
        );
        index.push([b, count, 0]);
    }
    let mut fields = Map::new();
    fields.insert(field("next"), uint(next + 1));
    fields.insert(field("seed"), uint(0));
    fields.insert(field("bytes"), uint(bytes));
    fields.insert(field("count"), uint(511 + 30));
    fields.insert(field("blocks"), refs(&index));
    fields.insert(field("records"), Value::Bool(false));
    let payload = encode(&Value::Map(fields)).expect("encode");
    plant(&mut k, &root.to_bytes(), 1, Kind::List, &payload);
    k
}

/// W3 (W07–W09 and W22 were walk-only; ADR-rdb-0016 §4): at 512 blocks a fold over B is
/// written unsplit when the ops did not grow the block, and refused when they did. A same-size
/// replace in the last block folds it (its base is over B) and writes it whole: 512 blocks,
/// every item read. A push into it is refused `TooLarge{List}`, even though it alone would take
/// a slot.
#[test]
fn w3_at_the_block_cap_a_fold_is_written_unsplit_unless_the_ops_grew_it() {
    let root = todo();
    let mut k = at_the_block_cap();
    assert_eq!(values(&k, &root).len(), 541, "the planted list reads");
    let last = 540;
    let replace = ListOp::Replace {
        at: last,
        value: text(&"z".repeat(40)),
    };
    let compiled = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(1),
        MIN_BLOCK_MAX,
        &[replace],
    )
    .expect("a same-size replace at the cap");
    let keys: Vec<&Bytes> = compiled.compiled().mutations.iter().map(key_of).collect();
    assert_eq!(
        keys,
        [&root.to_bytes(), &block_key(&root, 511)],
        "written whole"
    );
    k.commit(compiled.compiled());
    assert_eq!(block_index(&k, &root).len(), 512);
    let read = values(&k, &root);
    assert_eq!(read[index(last)], text(&"z".repeat(40)));
    let push = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version_of(&k, &root)),
        MIN_BLOCK_MAX,
        &[ListOp::Push(text("x"))],
    );
    assert!(
        matches!(
            push,
            Err(ValueError::Apply(ApplyError::TooLarge {
                limit: SizeLimit::List {
                    block_max: MIN_BLOCK_MAX,
                    ..
                }
            }))
        ),
        "{push:?}"
    );
    // Review B3: an op that grows an under-B block at the cap still compiles; the cap refuses
    // only a fold over B that would need a 513th block.
    write(
        &mut k,
        &root,
        MIN_BLOCK_MAX,
        &[ListOp::Insert {
            at: 0,
            value: text("y"),
        }],
    );
    assert_eq!(block_index(&k, &root).len(), 512, "the insert into block 0");
    assert_eq!(values(&k, &root)[0], text("y"));
}

/// The payload length of block `n`'s base, and its `folded`.
fn base_of_block(k: &Kernel, root: &RootKey, n: u64) -> (usize, u64) {
    let (key, (_, raw)) = k
        .records
        .iter()
        .find(|(key, _)| {
            parse(key).is_ok_and(|p| {
                p.root() == *root
                    && p.sub == Sub::Block
                    && p.slot.is_none()
                    && p.list_id.map(|id| id & u128::from(u64::MAX)) == Some(u128::from(n))
            })
        })
        .expect("the block's base");
    let Value::Map(fields) = payload_of(k, key) else {
        panic!("a base is a map")
    };
    let Some(Value::Integer(folded)) = fields.get(&field("folded")) else {
        panic!("a base has folded")
    };
    let folded = u64::try_from(folded.get()).expect("unsigned");
    (open(raw).expect("envelope").payload.len(), folded)
}

/// W3 (W06 survived the sweep): a delta that leaves the list empty folds its only block. One
/// remove's op is far under a quarter of a 200-char base, so no other rule folds it; still the
/// write is the root and the base alone, the base is empty and `folded = head`, and no slot is
/// written (ADR-rdb-0016 §4).
#[test]
fn w3_a_delta_that_empties_the_list_folds_its_block_empty() {
    let root = todo();
    let mut k = Kernel::new();
    let big = "b".repeat(200);
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &[big.as_str()]);
    let compiled = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version_of(&k, &root)),
        DEFAULT_BLOCK_MAX,
        &[ListOp::Remove { at: 0 }],
    )
    .expect("remove");
    let subs: Vec<Option<u8>> = compiled
        .compiled()
        .mutations
        .iter()
        .map(|m| parse(key_of(m)).expect("key").slot)
        .collect();
    assert_eq!(subs, [None, None], "the root and the base, no slot");
    k.commit(compiled.compiled());
    let [[n, count, head]] = block_refs(&k, &root)[..] else {
        panic!("one block")
    };
    assert_eq!((count, head), (0, 2));
    let (len, folded) = base_of_block(&k, &root, n);
    assert_eq!(folded, head, "folded = head");
    assert_eq!(
        len,
        encode(&block_payload(&[], head)).expect("encode").len(),
        "an empty base"
    );
    assert_eq!(slots_of(&k, &root, n), 0);
    assert!(values(&k, &root).is_empty());
}

/// W3 (W10 survived the sweep): only a block folded under B/4 merges back. At B = 1024, 30
/// items of 40 chars split at create; one delta then trims the first block to 7 items and adds
/// pairs of insert-and-remove until 242 ops pass the slots, so it folds. Its base lands in
/// B/4 … B/2 and the pair would fit ¾ · B, so a looser bound would merge; the blocks stay two.
#[test]
fn w3_a_block_folded_at_or_over_a_quarter_of_b_does_not_merge_back() {
    let root = todo();
    let mut k = Kernel::new();
    let names: Vec<String> = (0..30).map(|i| format!("{i:0>40}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut k, &root, MIN_BLOCK_MAX, &names);
    let blocks = block_index(&k, &root);
    assert_eq!(blocks.len(), 2, "{blocks:?}");
    let ((left, left_count), (right, right_count)) = (blocks[0], blocks[1]);
    let mut ops = vec![ListOp::Remove { at: 0 }; index(left_count - 7)];
    while ops.len() < 242 {
        ops.push(ListOp::Insert {
            at: 0,
            value: text("z"),
        });
        ops.push(ListOp::Remove { at: 0 });
    }
    write(&mut k, &root, MIN_BLOCK_MAX, &ops);
    let after = block_index(&k, &root);
    assert_eq!(after, [(left, 7), (right, right_count)], "no merge-back");
    let (left_len, folded) = base_of_block(&k, &root, left);
    let (right_len, _) = base_of_block(&k, &root, right);
    assert_eq!(
        folded,
        block_refs(&k, &root)[0][2],
        "the first block folded"
    );
    assert!(
        (MIN_BLOCK_MAX / 4..MIN_BLOCK_MAX / 2).contains(&left_len),
        "the fold is in B/4 … B/2: {left_len}"
    );
    assert!(
        4 * (left_len + right_len) <= 3 * MIN_BLOCK_MAX,
        "the pair fits ¾ · B: {left_len} + {right_len}"
    );
    let want = texts(&names[index(left_count - 7)..]);
    assert_eq!(values(&k, &root), want);
}

/// W3 (W12 survived once the model seeds were cut, ruling L-R186dw): no merge-back beside a
/// retire. Three blocks at B = 1024: the first trimmed to 7 items and folded at or over B/4 (no
/// merge), the last split off by 20 pushes. One delta then empties the last block, which
/// retires, and trims the middle one to 3 items with enough insert-and-remove pairs to fold it.
/// The middle base is under B/4 and the pair with the first fits ¾ · B, so without the retire it
/// would merge; beside the retire the two blocks stay.
#[test]
fn w3_no_merge_back_beside_a_retire() {
    let root = todo();
    let mut k = Kernel::new();
    let names: Vec<String> = (0..30).map(|i| format!("{i:0>40}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    made(&mut k, &root, MIN_BLOCK_MAX, &names);
    let first_count = block_index(&k, &root)[0].1;
    let mut ops = vec![ListOp::Remove { at: 0 }; index(first_count - 7)];
    while ops.len() < 242 {
        ops.push(ListOp::Insert {
            at: 0,
            value: text("z"),
        });
        ops.push(ListOp::Remove { at: 0 });
    }
    write(&mut k, &root, MIN_BLOCK_MAX, &ops);
    let more: Vec<String> = (30..50).map(|i| format!("{i:0>40}")).collect();
    let pushes: Vec<ListOp> = more.iter().map(|v| ListOp::Push(text(v))).collect();
    write(&mut k, &root, MIN_BLOCK_MAX, &pushes);
    let blocks = block_index(&k, &root);
    let [(a, 7), (b, b_count), (_, c_count)] = blocks[..] else {
        panic!("three blocks, the first of 7: {blocks:?}")
    };
    // Empty the last block, then trim the middle one to 3 and fold it.
    let c_at = 7 + b_count;
    let mut ops = vec![ListOp::Remove { at: c_at }; index(c_count)];
    ops.extend(vec![ListOp::Remove { at: 7 }; index(b_count - 3)]);
    while ops.len() < 241 {
        ops.push(ListOp::Insert {
            at: 8,
            value: text("z"),
        });
        ops.push(ListOp::Remove { at: 8 });
    }
    let mut want = values(&k, &root);
    want.truncate(index(c_at));
    want.drain(7..index(7 + b_count - 3));
    write(&mut k, &root, MIN_BLOCK_MAX, &ops);
    assert_eq!(
        block_index(&k, &root),
        [(a, 7), (b, 3)],
        "a retire, no merge"
    );
    let (a_len, _) = base_of_block(&k, &root, a);
    let (b_len, b_folded) = base_of_block(&k, &root, b);
    assert_eq!(
        b_folded,
        block_refs(&k, &root)[1][2],
        "the middle block folded"
    );
    assert!(
        4 * b_len < MIN_BLOCK_MAX,
        "the middle base is under B/4: {b_len}"
    );
    assert!(
        4 * (a_len + b_len) <= 3 * MIN_BLOCK_MAX,
        "the pair fits ¾ · B: {a_len} + {b_len}"
    );
    assert_eq!(values(&k, &root), want);
}

/// Tester W3 G1 (mutant T3 survived): a Replace that moves an inline item out of its block
/// writes the item's record, so a record already at that id is damage. The compile is refused
/// `OrphanElement` and writes nothing; the store is as it was.
#[test]
fn w3_a_replace_out_of_the_block_over_a_stray_item_record_is_orphan_element() {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, DEFAULT_BLOCK_MAX, &["a"]);
    let item = items(&k.snapshot(), &root, Start::Position(0), 1)
        .expect("read")
        .items
        .remove(0);
    assert!(item.inline, "the item starts in its block");
    let version = version_of(&k, &root);
    let stray = encode(&text("stray")).expect("encode");
    plant(
        &mut k,
        &item_key(&root, item.id),
        version,
        Kind::Document,
        &stray,
    );
    let before = k.records.clone();
    let replace = [ListOp::Replace {
        at: 0,
        value: text(&"o".repeat(300)),
    }];
    let got = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version),
        DEFAULT_BLOCK_MAX,
        &replace,
    )
    .map(|_| ());
    assert_eq!(got, Err(ValueError::Corrupt(Corrupt::OrphanElement)));
    assert_eq!(k.records, before, "the store is unchanged");
}

/// Tester W3 advisory: a retire and a merge-back of a block whose slots are still pending. Five
/// more pushes leave the second block of [`two_blocks_with_stale_slots`] 5 pending ops. Emptying
/// it retires it; shrinking the first block to 10 items merges the second into it, its pending
/// ops replayed. Either way no record of the second block remains, and the values read back.
#[test]
fn w3_a_retire_or_merge_back_of_a_block_with_pending_slots_leaves_none_of_it() {
    let root = todo();
    for merge in [false, true] {
        let mut k = Kernel::new();
        let (mut model, second) = two_blocks_with_stale_slots(&mut k, &root);
        write(&mut k, &root, DEFAULT_BLOCK_MAX, &pushes(241, 5));
        model.extend(texts(&["p241", "p242", "p243", "p244", "p245"]));
        let head = block_refs(&k, &root)[1][2];
        assert_eq!(head - base_of_block(&k, &root, second).1, 5, "pending ops");
        let first = block_index(&k, &root)[0].1;
        let ops = if merge {
            model.drain(..index(first - 10));
            vec![ListOp::Remove { at: 0 }; index(first - 10)]
        } else {
            let removes = model.len() - index(first);
            model.truncate(index(first));
            vec![ListOp::Remove { at: first }; removes]
        };
        write(&mut k, &root, DEFAULT_BLOCK_MAX, &ops);
        assert_eq!(block_index(&k, &root).len(), 1, "merge {merge}");
        assert!(
            !block_remains(&k, &root, second),
            "merge {merge}: a record of the second block remains"
        );
        assert_eq!(values(&k, &root), model, "merge {merge}");
    }
}

// ---- Review L-R186ee: a root naming one block twice, a head at u64::MAX ---------------------

/// A one-item list `todo` whose root is edited by `edit`.
fn one_item_with_root(edit: impl FnOnce(&mut Map, [u64; 3])) -> Kernel {
    let root = todo();
    let mut k = Kernel::new();
    made(&mut k, &root, MIN_BLOCK_MAX, &["a"]);
    let [block] = block_refs(&k, &root)[..] else {
        panic!("one block")
    };
    edit_map(&mut k, &root.to_bytes(), Kind::List, |m| edit(m, block));
    k
}

/// Review A1: a root naming one block `n` twice made `compile_list` panic. The compile keyed its
/// work by `n` but walked blocks by position, so the second Remove indexed past the entries. It
/// is root damage now: the compile and the read both refuse it by name.
#[test]
fn l_r186ee_a_root_naming_one_block_twice_is_refused_not_a_panic() {
    let root = todo();
    let k = one_item_with_root(|m, block| {
        m.insert(field("blocks"), refs(&[block, block]));
        m.insert(field("count"), uint(2));
    });
    let want = Err(ValueError::Corrupt(Corrupt::ListRoot(
        "a list root names one block twice",
    )));
    let ops = [ListOp::Remove { at: 1 }, ListOp::Remove { at: 0 }];
    let compiled = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version_of(&k, &root)),
        MIN_BLOCK_MAX,
        &ops,
    );
    assert_eq!(unit(compiled), want, "compile");
    let read = items(&k.snapshot(), &root, Start::Position(0), usize::MAX);
    assert_eq!(unit(read), want, "items");
}

/// Review A2: a read refuses a block head of `u64::MAX` (damage row R16), so a write must never
/// make one. A push onto a block at head `u64::MAX − 1` is refused as the other root counters
/// are when they would leave their range, and writes nothing.
#[test]
fn l_r186ee_a_write_never_makes_a_block_head_of_u64_max() {
    let root = todo();
    let mut k = one_item_with_root(|m, [n, count, _]| {
        m.insert(field("blocks"), refs(&[[n, count, u64::MAX - 1]]));
    });
    let base = block_key(&root, u128::from(block_refs(&k, &root)[0][0]));
    edit_map(&mut k, &base, Kind::ListBlock, |m| {
        m.insert(field("folded"), uint(u64::MAX - 1));
    });
    assert_eq!(values(&k, &root), texts(&["a"]), "the edited list reads");
    let pushed = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version_of(&k, &root)),
        MIN_BLOCK_MAX,
        &[ListOp::Push(text("b"))],
    );
    assert_eq!(
        unit(pushed),
        Err(ValueError::Corrupt(Corrupt::ListRoot(
            "a list's count or bytes, or a block's count or head, leaves u64"
        )))
    );
}

// ---- Review L-R186ee: halves by bytes, and a merge-back reads no item record ----------------

/// Review B1: a fold over B in a block that is not the last splits where the two halves' bytes
/// are closest, not at half the count. 23 throwaway items keep every later n two bytes wide, as
/// in [`w2_halves_takes_the_first_of_two_equal_cuts`]. Three 230-char values (inline: the limit
/// is 240 at B = 1,024) inserted at the front of the first of two blocks of 40-char values fold
/// it over B. The cut is checked against the cut computed here from each entry's encoded length.
#[test]
fn l_r186ee_halves_cuts_a_block_by_bytes_not_by_count() {
    const B: usize = MIN_BLOCK_MAX;
    let root = todo();
    let mut k = Kernel::new();
    let zs: Vec<String> = (0..23).map(|i| format!("z{i}")).collect();
    let zs: Vec<&str> = zs.iter().map(String::as_str).collect();
    made(&mut k, &root, B, &zs);
    write(&mut k, &root, B, &vec![ListOp::Remove { at: 0 }; 23]);
    let pushes: Vec<ListOp> = (0..30)
        .map(|i| ListOp::Push(text(&format!("{i:0>40}"))))
        .collect();
    write(&mut k, &root, B, &pushes);
    assert_eq!(block_index(&k, &root).len(), 2, "an end split");
    let big: Vec<ListOp> = (0..3)
        .map(|i| ListOp::Insert {
            at: 0,
            value: text(&format!("{i}{}", "b".repeat(229))),
        })
        .collect();
    write(&mut k, &root, B, &big);
    let blocks = block_index(&k, &root);
    assert_eq!(
        blocks.len(),
        3,
        "the first block split in halves: {blocks:?}"
    );
    let total = blocks[0].1 + blocks[1].1;
    // The split block's entries, in order: the first two blocks now. Each is `[n, value]`.
    let page = items(&k.snapshot(), &root, Start::Position(0), index(total)).expect("read");
    let sizes: Vec<usize> = page
        .items
        .iter()
        .map(|item| {
            assert!(item.inline, "every item here is inline");
            let n = u64::try_from(item.id & u128::from(u64::MAX)).expect("low 64 bits");
            let entry = Value::Array(vec![uint(n), item.value.clone()]);
            encode(&entry).expect("encode").len()
        })
        .collect();
    let all: usize = sizes.iter().sum();
    let (mut best, mut cut, mut left) = (usize::MAX, 0, 0);
    for (k, size) in sizes.iter().enumerate().take(sizes.len() - 1) {
        left += size;
        let gap = (2 * left).abs_diff(all);
        if gap < best {
            (best, cut) = (gap, k + 1);
        }
    }
    assert_eq!(
        blocks[0].1,
        len_u64(cut),
        "the byte-balanced cut: {sizes:?}"
    );
    assert_ne!(blocks[0].1, total / 2, "not half the count");
}

/// Review B2: a merge-back moves the right block's entries as they stand and reads no item
/// record. In a records list of two blocks, the item record of an entry in the right block is
/// damaged and no op names it. Removes at the front, at most 100 a delta (each deletes its item
/// record, and a request holds at most 255 writes), shrink the left block until a fold under B/4
/// merges it back with its right neighbour: every compile succeeds, one block is left, and the
/// damaged item still reads as damage.
#[test]
fn l_r186ee_a_merge_back_reads_no_item_record() {
    const B: usize = MIN_BLOCK_MAX;
    let root = todo();
    let mut k = Kernel::new();
    two_block_records_list(&mut k, &root, B, 16, 100);
    let first = block_index(&k, &root)[0].1;
    let page = items(&k.snapshot(), &root, Start::Position(first), 1).expect("read");
    damage(&mut k, &item_key(&root, page.items[0].id));
    let mut removed = 0;
    while block_index(&k, &root).len() == 2 && first - removed > 5 {
        let batch = (first - removed - 5).min(100);
        write(
            &mut k,
            &root,
            B,
            &vec![ListOp::Remove { at: 0 }; index(batch)],
        );
        removed += batch;
    }
    assert_eq!(
        block_index(&k, &root).len(),
        1,
        "merged back after {removed} removes"
    );
    let read = items(&k.snapshot(), &root, Start::Position(first - removed), 1);
    assert!(
        matches!(read, Err(ValueError::Corrupt(_))),
        "the damage stays: {read:?}"
    );
}

// ---- timing probes (L-R186ee D1, D2): ignored, run by hand ---------------------------------
//
// `cargo test -p rdb-value --test lists --release -- --ignored --nocapture --test-threads=1
// l_r186ee_probe` prints the numbers; one thread, so the two do not share the host's cores.
// Drop `--release` for a debug build. They assert only what makes the
// numbers mean what they say: the block shape before timing, and the outcome of each compile.

/// The median of `runs` timings of `f`, in milliseconds.
fn median_ms<T>(runs: usize, mut f: impl FnMut() -> T) -> (f64, T) {
    let mut times = Vec::with_capacity(runs);
    let mut last = None;
    for _ in 0..runs {
        let started = std::time::Instant::now();
        let out = f();
        times.push(started.elapsed().as_secs_f64() * 1e3);
        last = Some(out);
    }
    times.sort_by(f64::total_cmp);
    (times[runs / 2], last.expect("runs > 0"))
}

/// D1: at the default B, one block of 10,000 small entries with 240 pending `Insert{at: 0}`.
/// Times one whole read, one point read, and one compile that opens the block (one more
/// `Insert{at: 0}`, which folds it), each the median of 5.
#[test]
#[ignore = "a timing probe; run by hand"]
fn l_r186ee_probe_d1_a_full_block_with_240_pending_front_inserts() {
    const ENTRIES: usize = 10_000;
    let root = todo();
    let mut k = Kernel::new();
    let pushes: Vec<ListOp> = (0..ENTRIES)
        .map(|i| ListOp::Push(text(&format!("{i:04}"))))
        .collect();
    let made = compile_list(
        &k.snapshot(),
        &root,
        Expected::Absent,
        DEFAULT_BLOCK_MAX,
        &pushes,
    )
    .expect("create");
    k.commit(made.compiled());
    for _ in 0..240 {
        write(
            &mut k,
            &root,
            DEFAULT_BLOCK_MAX,
            &[ListOp::Insert {
                at: 0,
                value: text("x"),
            }],
        );
    }
    let refs = block_refs(&k, &root);
    assert_eq!(refs.len(), 1, "one block");
    let prefix = root.sub_prefix(SUB_BLOCK);
    let slots = k
        .records
        .range(Bytes::from(prefix.clone())..)
        .take_while(|(key, _)| key.starts_with(&prefix))
        .filter(|(key, _)| key.len() == prefix.len() + 17)
        .count();
    assert_eq!(slots, 240, "240 pending slots");
    let base = k
        .records
        .range(Bytes::from(prefix.clone())..)
        .next()
        .map(|(_, (_, raw))| open(raw).expect("base").payload.len())
        .expect("a base");
    let snap = k.snapshot();
    let (full, page) = median_ms(5, || {
        items(&snap, &root, Start::Position(0), usize::MAX).expect("read")
    });
    assert_eq!(page.items.len(), ENTRIES + 240);
    let (point, page) = median_ms(5, || {
        items(&snap, &root, Start::Position(0), 1).expect("read")
    });
    assert_eq!(page.items.len(), 1);
    let version = version_of(&k, &root);
    let insert = [ListOp::Insert {
        at: 0,
        value: text("x"),
    }];
    let (compile, _) = median_ms(5, || {
        compile_list(
            &snap,
            &root,
            Expected::Version(version),
            DEFAULT_BLOCK_MAX,
            &insert,
        )
        .expect("compile")
    });
    eprintln!(
        "D1: {ENTRIES} entries + 240 pending, base {base} B, B {DEFAULT_BLOCK_MAX}: whole read \
         {full:.2} ms, point read {point:.2} ms, compile of one insert {compile:.2} ms (median \
         of 5)"
    );
}

/// D2: one delta of k `Insert{at: 0}` into an empty list at the default B, for k = 1,000 to
/// past the refusal point. Times the compile alone, the median of 3, and prints its outcome.
#[test]
#[ignore = "a timing probe; run by hand"]
fn l_r186ee_probe_d2_one_delta_of_k_front_inserts() {
    let root = todo();
    let mut k = Kernel::new();
    let made = compile_list(
        &k.snapshot(),
        &root,
        Expected::Absent,
        DEFAULT_BLOCK_MAX,
        &[],
    )
    .expect("create");
    k.commit(made.compiled());
    let snap = k.snapshot();
    let version = version_of(&k, &root);
    for count in [
        1_000, 5_000, 10_000, 20_000, 43_000, 44_000, 50_000, 100_000,
    ] {
        let ops = vec![
            ListOp::Insert {
                at: 0,
                value: text("x"),
            };
            count
        ];
        let (ms, out) = median_ms(3, || {
            compile_list(
                &snap,
                &root,
                Expected::Version(version),
                DEFAULT_BLOCK_MAX,
                &ops,
            )
        });
        let outcome = match out {
            Ok(compiled) => format!("ok, {} writes", compiled.compiled().mutations.len()),
            Err(e) => format!("refused {e:?}"),
        };
        eprintln!("D2: k {count}: {ms:.1} ms (median of 3), {outcome}");
    }
}
