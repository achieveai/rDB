//! Clearing a damaged object at the library API (M9 S6; ADR-rdb-0013 decision 11).
//!
//! Scenario: an admin clears an object the read refuses, and the clear never deletes a record a
//! newer build wrote, whatever else is wrong with the object.

mod common;

use bytes::Bytes;
use common::{float, int, text, Kernel, Refused};
use rdb_core::{AffinityId, Condition, Generation, Mutation, TenantId};
use rdb_value::blob::{publish, put_chunk, read_blob, read_range, Upload};
use rdb_value::cbor::{decode, encode};
use rdb_value::collection::{collection, compile_collection, members, CollectionKind, ElemOp};
use rdb_value::delta::{ApplyError, Delta, Op};
use rdb_value::envelope::{digest, open, seal, EnvelopeError, Kind};
use rdb_value::keys::{chunk_key, element_key, item_key, parse, root_key, KeyError, RootKey, Sub};
use rdb_value::list::{compile_list, create_list, items, list, ListOp, Start, DEFAULT_BLOCK_MAX};
use rdb_value::repair::clear_object;
use rdb_value::value::{Int, MapKey, Value};
use rdb_value::{compile, read, BlockFault, Corrupt, Expected, SlotFault, ValueError};
use sha2::{Digest as _, Sha256};

fn object(id: &[u8]) -> RootKey {
    root_key(TenantId(1), AffinityId(1), id)
}

/// A sound document envelope with `codec_version` `0x02` and its digest recomputed: a record a
/// newer build could have written, as `rdb_dev admin damage --as newer` writes it.
fn newer_build_record() -> Bytes {
    let mut bytes = seal(Kind::Document, &[0xf6]).unwrap().to_vec();
    bytes[2] = 0x02;
    let head: [u8; 8] = bytes[..8].try_into().unwrap();
    let sum = digest(&head, &bytes[40..]);
    bytes[8..40].copy_from_slice(&sum);
    Bytes::from(bytes)
}

/// Store `bytes` at `key` in a commit of its own, so its version is newer than the root's.
fn overwrite(k: &mut Kernel, key: Bytes, bytes: Bytes) {
    k.seq += 1;
    k.records.insert(key, (k.seq, bytes));
}

/// A map `m` holding `a` and `b`, created in one commit.
fn map_of_two(k: &mut Kernel, root: &RootKey) {
    let ops = [
        ElemOp::Put(text("a"), text("1")),
        ElemOp::Put(text("b"), text("2")),
    ];
    let made = compile_collection(
        &k.snapshot(),
        root,
        CollectionKind::Map,
        Expected::Absent,
        &ops,
    )
    .unwrap();
    k.commit(&made);
}

fn assert_newer_build_refused(k: &Kernel, root: &RootKey, at: &str) {
    let got = clear_object(&k.snapshot(), root);
    assert!(
        matches!(
            &got,
            Err(ValueError::Corrupt(Corrupt::Envelope(
                EnvelopeError::UnknownCodec(0x02)
            )))
        ),
        "{at}: {got:?}"
    );
}

/// Guard 4 (s6-dev smoke, basis 949cc8c): a newer build's map entry written after the root was
/// read as `ElementNewerThanRoot`, which is damage, so the clear proved the map damaged and
/// would have deleted the entry. Every record a read opens is now checked for a newer build's
/// envelope before any other check (A-R139 D1), so the whole clear is refused.
///
#[test]
fn g4_a_newer_build_map_entry_refuses_the_clear_of_a_sound_map() {
    let mut k = Kernel::new();
    let root = object(b"m");
    map_of_two(&mut k, &root);
    overwrite(
        &mut k,
        element_key(&root, &text("a")).unwrap(),
        newer_build_record(),
    );
    // The proof reads the members, so the refusal names the entry's key (#59, A-R230).
    let got = clear_object(&k.snapshot(), &root);
    assert!(
        matches!(
            &got,
            Err(ValueError::Corrupt(Corrupt::Element { key, error }))
                if *key == text("a")
                    && **error == Corrupt::Envelope(EnvelopeError::UnknownCodec(0x02))
        ),
        "sound map root: {got:?}"
    );
}

/// D9 (A-R141): a root whose envelope opens but whose payload does not decode is checked by
/// open only, and a newer build's entry beside it still refuses the clear.
#[test]
fn g4_a_newer_build_map_entry_refuses_the_clear_beside_a_bad_payload_root() {
    let mut k = Kernel::new();
    let root = object(b"m");
    map_of_two(&mut k, &root);
    overwrite(
        &mut k,
        element_key(&root, &text("a")).unwrap(),
        newer_build_record(),
    );
    let bad_root = seal(Kind::Map, &[0xf6]).unwrap();
    overwrite(&mut k, root.to_bytes(), bad_root);
    assert_newer_build_refused(&k, &root, "bad-payload map root");
}

/// Guard 4 in a list: a newer build's block base written after the root, where `NewerThanRoot`
/// was checked first. Since M9 S7 the proof's own read names the newer base before its version
/// (A-R144), so the refusal names the block; before, the scan named the bare envelope.
#[test]
fn g4_a_newer_build_list_block_refuses_the_clear_of_a_sound_list() {
    let mut k = Kernel::new();
    let list = object(b"l");
    let push: Vec<_> = ["x", "y", "z"]
        .iter()
        .map(|v| ListOp::Push(text(v)))
        .collect();
    let made = create_list(&k.snapshot(), &list, false, DEFAULT_BLOCK_MAX, &push).unwrap();
    k.commit(made.compiled());
    let block = k
        .records
        .keys()
        .find(|key| parse(key).is_ok_and(|p| p.sub == Sub::Block && p.slot.is_none()))
        .expect("the list has a block base")
        .clone();
    overwrite(&mut k, block, newer_build_record());
    let got = clear_object(&k.snapshot(), &list);
    assert!(
        matches!(
            &got,
            Err(ValueError::Corrupt(Corrupt::Block {
                fault: BlockFault::Envelope(EnvelopeError::UnknownCodec(0x02)),
                ..
            }))
        ),
        "sound list root: {got:?}"
    );
}

/// A map of two at `id`, whose records sort right after every record of `of` (tester-s6 M1):
/// a page that reaches the last element of `of` then meets this object's first record.
fn neighbour(k: &mut Kernel, of: &RootKey, id: &[u8]) {
    let next = object(id);
    assert!(next.object_prefix() > of.object_prefix(), "sorts after");
    map_of_two(k, &next);
    let (last, _) = k.records.iter().next_back().unwrap();
    assert!(last.starts_with(next.object_prefix()), "the last object");
}

/// The root version of the map or set at `root`.
fn root_version(k: &Kernel, root: &RootKey) -> u64 {
    collection(&k.snapshot(), root).unwrap().unwrap().version
}

/// A map (`k0000` …) or set (0 …) at `root` of `n` elements, written 200 to a commit.
fn filled(k: &mut Kernel, root: &RootKey, kind: CollectionKind, n: usize) -> Vec<Value> {
    let keys: Vec<Value> = (0..n)
        .map(|i| match kind {
            CollectionKind::Map => text(&format!("k{i:04}")),
            CollectionKind::Set => int(i128::try_from(i).unwrap()),
        })
        .collect();
    for (i, chunk) in keys.chunks(200).enumerate() {
        let ops: Vec<_> = chunk
            .iter()
            .map(|key| match kind {
                CollectionKind::Map => ElemOp::Put(key.clone(), text("1")),
                CollectionKind::Set => ElemOp::Add(key.clone()),
            })
            .collect();
        let expected = if i == 0 {
            Expected::Absent
        } else {
            Expected::Version(root_version(k, root))
        };
        let made = compile_collection(&k.snapshot(), root, kind, expected, &ops).unwrap();
        k.commit(&made);
    }
    keys
}

/// The first batch of a clear of a map or set of `n` elements with one damaged, then the app's
/// own fix as api-s6 tells it: a put or add of a new element, then a del of the damaged one.
/// Returns how many elements are stored afterwards.
fn half_cleared_then_fixed(k: &mut Kernel, root: &RootKey, kind: CollectionKind, n: usize) -> u64 {
    let keys = filled(k, root, kind, n);
    let damaged = keys[n / 2].clone();
    overwrite(
        k,
        element_key(root, &damaged).unwrap(),
        Bytes::from_static(b"damaged"),
    );
    let first = clear_object(&k.snapshot(), root).unwrap().expect("damaged");
    assert_eq!(
        first.compiled.mutations.len(),
        254,
        "the first batch is full"
    );
    k.commit(&first.compiled);
    let fresh = match kind {
        CollectionKind::Map => ElemOp::Put(text("z"), text("1")),
        CollectionKind::Set => ElemOp::Add(int(-1)),
    };
    for op in [fresh, ElemOp::Remove(damaged)] {
        let version = Expected::Version(root_version(k, root));
        let made = compile_collection(&k.snapshot(), root, kind, version, &[op]).unwrap();
        k.commit(&made);
    }
    u64::try_from(n - 254).unwrap()
}

/// W2-A (tester-s6 Walk 2, basis 949cc8c; ruling A-R142): after the first batch of a clear and
/// the app's own fix, a full `members` answered ok with `count=300 listed=46`, while the next
/// clear proved the same state damaged. A read that starts at the first element and reaches the
/// last, judged by looking one element past the page, now checks the count as the clear does.
/// Another object follows, so the record past the page is the neighbour's, not none (M1).
#[test]
fn w2a_a_half_cleared_map_or_set_never_reads_as_healthy() {
    for kind in [CollectionKind::Map, CollectionKind::Set] {
        let mut k = Kernel::new();
        let root = object(b"big");
        let stored = half_cleared_then_fixed(&mut k, &root, kind, 300);
        neighbour(&mut k, &root, b"bih");
        let s = k.snapshot();
        let want = Corrupt::CountMismatch { count: 300, stored };
        // A page of exactly the elements left reaches the end too.
        for limit in [1000, 46, usize::MAX] {
            let got = members(&s, &root, None, limit);
            assert!(
                matches!(&got, Err(ValueError::Corrupt(c)) if *c == want),
                "{kind:?} limit {limit}: {got:?}"
            );
        }
        // A page that stops short, or starts after an element, cannot see the whole count.
        let short = members(&s, &root, None, 45).unwrap();
        assert_eq!(short.members.len(), 45, "{kind:?}");
        let first = short.members[0].key.clone();
        assert!(members(&s, &root, Some(&first), 1000).is_ok(), "{kind:?}");
        // And the clear proves the same state damaged, by the same rule.
        let next = clear_object(&s, &root).unwrap().expect("still damaged");
        assert_eq!(next.damage, want, "{kind:?}");
    }
}

/// A-R142 (lead): a map over 1000 elements, more than any one API page, half cleared and fixed
/// by the app, is still proved damaged by the next clear: its proof reads every element.
#[test]
fn w2a_a_half_cleared_map_over_1000_elements_is_still_proved_damaged() {
    let mut k = Kernel::new();
    let root = object(b"huge");
    let stored = half_cleared_then_fixed(&mut k, &root, CollectionKind::Map, 1100);
    let s = k.snapshot();
    let next = clear_object(&s, &root).unwrap().expect("still damaged");
    assert_eq!(
        next.damage,
        Corrupt::CountMismatch {
            count: 1100,
            stored
        }
    );
    // No API page sees it whole: 1000 of the 846 left fit, so this page does reach the end.
    let page = members(&s, &root, None, 1000);
    assert!(matches!(&page, Err(ValueError::Corrupt(_))), "{page:?}");
}

/// A-R142 (3): a list's count is checked by its structure. A full read loads every block the
/// root names, and a block whose replayed entries are not the root's count for it is refused.
/// Here the root claims one item more in its only block, and in its total, so it still opens.
#[test]
fn a_r142_a_list_block_holding_fewer_items_than_its_root_counts_is_refused() {
    let mut k = Kernel::new();
    let root = object(b"l");
    let push: Vec<_> = ["x", "y", "z"]
        .iter()
        .map(|v| ListOp::Push(text(v)))
        .collect();
    let made = create_list(&k.snapshot(), &root, false, DEFAULT_BLOCK_MAX, &push).unwrap();
    k.commit(made.compiled());
    let (version, bytes) = k.records[&root.to_bytes()].clone();
    let Value::Map(mut fields) = decode(open(&bytes).unwrap().payload).unwrap() else {
        panic!("a list root is a map");
    };
    let plus_one = |v: &mut Value| {
        let Value::Integer(n) = v else {
            panic!("{v:?}")
        };
        *n = Int::new(n.get() + 1).unwrap();
    };
    plus_one(fields.get_mut(&MapKey::new("count")).unwrap());
    let Some(Value::Array(blocks)) = fields.get_mut(&MapKey::new("blocks")) else {
        panic!("blocks");
    };
    let Value::Array(block) = &mut blocks[0] else {
        panic!("a block ref")
    };
    plus_one(&mut block[1]);
    let sealed = seal(Kind::List, &encode(&Value::Map(fields)).unwrap()).unwrap();
    k.records.insert(root.to_bytes(), (version, sealed));

    let s = k.snapshot();
    assert_eq!(list(&s, &root).unwrap().unwrap().count, 4, "the root opens");
    let got = items(&s, &root, Start::Position(0), 4);
    assert!(
        matches!(
            &got,
            Err(ValueError::Corrupt(Corrupt::Block {
                fault: BlockFault::Shape(_),
                ..
            }))
        ),
        "{got:?}"
    );
    let next = clear_object(&s, &root).unwrap().expect("damaged");
    assert!(matches!(next.damage, Corrupt::Block { .. }), "{next:?}");
}

/// A-R142 (3): a records list of over 254 records, after the first batch of its clear, reads as
/// damaged: its root still names records the batch deleted. Not reachable by hand (tester-s6).
#[test]
fn a_r142_a_half_cleared_list_never_reads_as_healthy() {
    let mut k = Kernel::new();
    let root = object(b"l");
    let made = create_list(&k.snapshot(), &root, true, DEFAULT_BLOCK_MAX, &[]).unwrap();
    k.commit(made.compiled());
    for round in 0..3 {
        let push: Vec<_> = (0..100)
            .map(|i| ListOp::Push(text(&format!("item {round}-{i}"))))
            .collect();
        let version = list(&k.snapshot(), &root).unwrap().unwrap().version;
        let made = compile_list(
            &k.snapshot(),
            &root,
            Expected::Version(version),
            DEFAULT_BLOCK_MAX,
            &push,
        )
        .unwrap();
        k.commit(made.compiled());
    }
    // One item damaged, so the clear has something to prove.
    let item = k
        .records
        .keys()
        .find(|key| parse(key).is_ok_and(|p| p.sub == Sub::Item))
        .unwrap()
        .clone();
    let at = k.records[&item].0;
    k.records.insert(item, (at, Bytes::from_static(b"damaged")));
    let first = clear_object(&k.snapshot(), &root)
        .unwrap()
        .expect("damaged");
    assert_eq!(
        first.compiled.mutations.len(),
        254,
        "the first batch is full"
    );
    k.commit(&first.compiled);

    let s = k.snapshot();
    let got = items(&s, &root, Start::Position(0), 300);
    assert!(matches!(&got, Err(ValueError::Corrupt(_))), "{got:?}");
    assert!(clear_object(&s, &root).unwrap().is_some(), "still damaged");
}

/// P-5 (tester-s6 Walks 1 and 2): upload chunks alone, with no root, answered "the object reads
/// back cleanly", which reads oddly for an object that does not exist. They are still not
/// damage (ADR-rdb-0014 §9), and the refusal now says what they are and what removes them.
#[test]
fn p5_upload_chunks_alone_are_named_and_not_cleared() {
    let mut k = Kernel::new();
    let root = object(b"z");
    overwrite(
        &mut k,
        rdb_value::keys::chunk_key(&root, &[1; 16], 0),
        Bytes::from_static(b"chunk"),
    );
    let got = clear_object(&k.snapshot(), &root);
    assert_eq!(got, Err(ValueError::Apply(ApplyError::OnlyChunks)));
    let text = got.unwrap_err().to_string();
    assert!(text.contains("blob gc"), "{text}");
}

// ---- material contracts (after tester-s6's thumbs-up on c3fd2b3) ----------------------------

/// The kinds a clear proves damaged, each by its own read.
#[derive(Debug, Clone, Copy)]
enum Shape {
    Doc,
    Map,
    Set,
    List,
    Blob,
}

const SHAPES: [Shape; 5] = [Shape::Doc, Shape::Map, Shape::Set, Shape::List, Shape::Blob];

/// The upload every blob here is written under.
const UP: Upload = [7; 16];

/// A blob's bytes: three chunks of 4.
const BLOB: &[u8] = b"0123456789";

/// Create a healthy `shape` at `root` as the app writes it, through the kernel's checks.
fn create(k: &mut Kernel, root: &RootKey, shape: Shape) {
    let s = k.snapshot();
    match shape {
        Shape::Doc => {
            let delta = Delta(vec![Op::Replace(text("v"))]);
            k.commit(&compile(&s, root, Expected::Absent, &delta).unwrap());
        }
        Shape::Map => {
            let ops = [
                ElemOp::Put(text("a"), text("1")),
                ElemOp::Put(text("b"), text("2")),
            ];
            let made =
                compile_collection(&s, root, CollectionKind::Map, Expected::Absent, &ops).unwrap();
            k.commit(&made);
        }
        Shape::Set => {
            let ops = [ElemOp::Add(int(1)), ElemOp::Add(int(2))];
            let made =
                compile_collection(&s, root, CollectionKind::Set, Expected::Absent, &ops).unwrap();
            k.commit(&made);
        }
        Shape::List => {
            let push: Vec<_> = ["x", "y", "z"]
                .iter()
                .map(|v| ListOp::Push(text(v)))
                .collect();
            let made = create_list(&s, root, false, DEFAULT_BLOCK_MAX, &push).unwrap();
            k.commit(made.compiled());
        }
        Shape::Blob => {
            for (index, piece) in (0..).zip(BLOB.chunks(4)) {
                k.commit(&put_chunk(&k.snapshot(), root, &UP, index, piece).unwrap());
            }
            let sha: [u8; 32] = Sha256::digest(BLOB).into();
            let made = publish(
                &k.snapshot(),
                root,
                Expected::Absent,
                &UP,
                BLOB.len() as u64,
                4,
                &sha,
                Generation(k.generation),
            )
            .unwrap();
            k.commit(&made);
        }
    }
}

/// The record of `shape` that [`damage`] breaks: the root of a document, an element of a map
/// or set, a list's block, a blob's middle chunk.
fn damaged_key(k: &Kernel, root: &RootKey, shape: Shape) -> Bytes {
    match shape {
        Shape::Doc => root.to_bytes(),
        Shape::Map => element_key(root, &text("a")).unwrap(),
        Shape::Set => element_key(root, &int(1)).unwrap(),
        Shape::List => k
            .records
            .keys()
            .find(|key| parse(key).is_ok_and(|p| p.sub == Sub::Block && p.slot.is_none()))
            .expect("the list has a block base")
            .clone(),
        Shape::Blob => chunk_key(root, &UP, 1),
    }
}

/// Break one record of `shape` in place, at the version it had: what `admin damage` does.
fn damage(k: &mut Kernel, root: &RootKey, shape: Shape) {
    let key = damaged_key(k, root, shape);
    let at = k.records[&key].0;
    k.records.insert(key, (at, Bytes::from_static(b"damaged")));
}

/// The whole object as the app reads it: `true` when it reads back, `false` when it is absent.
fn whole(k: &Kernel, root: &RootKey, shape: Shape) -> Result<bool, ValueError> {
    let s = k.snapshot();
    match shape {
        Shape::Doc => read(&s, root).map(|found| found.is_some()),
        Shape::Map | Shape::Set => match collection(&s, root)? {
            None => Ok(false),
            Some(_) => members(&s, root, None, usize::MAX).map(|_| true),
        },
        Shape::List => match list(&s, root)? {
            None => Ok(false),
            Some(_) => items(&s, root, Start::Position(0), usize::MAX).map(|_| true),
        },
        Shape::Blob => match read_blob(&s, root)? {
            None => Ok(false),
            Some(blob) => read_range(&s, root, 0, blob.manifest.size).map(|_| true),
        },
    }
}

/// Clear `root` batch by batch until nothing is stored; the number of batches.
fn clear_to_the_end(k: &mut Kernel, root: &RootKey) -> usize {
    let mut batches = 0;
    while let Some(cleared) = clear_object(&k.snapshot(), root).unwrap() {
        k.commit_any_order(&cleared.compiled);
        batches += 1;
        assert!(batches < 10, "the clear ends");
    }
    batches
}

/// Contract 1: an admin clears a damaged object of every kind, the object then reads as absent
/// with nothing left under it, and the app creates it again as new.
#[test]
fn contract_1_every_kind_damaged_is_cleared_to_absent_and_created_again() {
    for shape in SHAPES {
        let mut k = Kernel::new();
        let root = object(b"o");
        create(&mut k, &root, shape);
        assert_eq!(whole(&k, &root, shape), Ok(true), "{shape:?} healthy");
        damage(&mut k, &root, shape);
        let read = whole(&k, &root, shape);
        assert!(
            matches!(read, Err(ValueError::Corrupt(_))),
            "{shape:?} damaged: {read:?}"
        );
        assert_eq!(clear_to_the_end(&mut k, &root), 1, "{shape:?}");
        assert!(k.records.is_empty(), "{shape:?}: {:?}", k.records.keys());
        assert_eq!(whole(&k, &root, shape), Ok(false), "{shape:?} absent");
        create(&mut k, &root, shape);
        assert_eq!(whole(&k, &root, shape), Ok(true), "{shape:?} created again");
    }
}

/// Contract 2: a healthy object of every kind, an empty one, a missing one and a newer build's
/// one compile no clear at all. The host's answers to these, and that they send nothing, are
/// its own rows.
#[test]
fn contract_2_healthy_empty_missing_and_newer_objects_are_never_cleared() {
    let not_damaged = Err(ValueError::Apply(ApplyError::NotDamaged));
    for shape in SHAPES {
        let mut k = Kernel::new();
        let root = object(b"o");
        create(&mut k, &root, shape);
        assert_eq!(clear_object(&k.snapshot(), &root), not_damaged, "{shape:?}");
    }

    // Empty: a map and a set whose one element was removed, a list of no items, a blob of no
    // bytes.
    let root = object(b"e");
    for (kind, element) in [
        (CollectionKind::Map, ElemOp::Put(text("a"), text("1"))),
        (CollectionKind::Set, ElemOp::Add(int(1))),
    ] {
        let mut k = Kernel::new();
        let made = compile_collection(&k.snapshot(), &root, kind, Expected::Absent, &[element]);
        k.commit(&made.unwrap());
        let key = match kind {
            CollectionKind::Map => text("a"),
            CollectionKind::Set => int(1),
        };
        let version = Expected::Version(root_version(&k, &root));
        let removed =
            compile_collection(&k.snapshot(), &root, kind, version, &[ElemOp::Remove(key)]);
        k.commit(&removed.unwrap());
        assert_eq!(collection(&k.snapshot(), &root).unwrap().unwrap().count, 0);
        assert_eq!(clear_object(&k.snapshot(), &root), not_damaged, "{kind:?}");
    }
    let mut k = Kernel::new();
    let made = create_list(&k.snapshot(), &root, false, DEFAULT_BLOCK_MAX, &[]).unwrap();
    k.commit(made.compiled());
    assert_eq!(
        clear_object(&k.snapshot(), &root),
        not_damaged,
        "empty list"
    );
    let mut k = Kernel::new();
    let sha: [u8; 32] = Sha256::digest(b"").into();
    let made = publish(
        &k.snapshot(),
        &root,
        Expected::Absent,
        &UP,
        0,
        4,
        &sha,
        Generation(k.generation),
    );
    k.commit(&made.unwrap());
    assert_eq!(
        clear_object(&k.snapshot(), &root),
        not_damaged,
        "empty blob"
    );

    // Missing: nothing is stored, so there is nothing to clear.
    assert_eq!(clear_object(&Kernel::new().snapshot(), &root), Ok(None));

    // A newer build's root: its error, which the host answers INCOMPATIBLE_VERSION.
    let mut k = Kernel::new();
    overwrite(&mut k, root.to_bytes(), newer_build_record());
    let got = clear_object(&k.snapshot(), &root);
    assert!(
        matches!(&got, Err(ValueError::Corrupt(c)) if c.is_newer_build()),
        "{got:?}"
    );
}

/// Contract 3: a clear's request sent again after the object was cleared and created again is
/// refused whole, so a late retry never deletes the new object.
#[test]
fn contract_3_a_late_retry_of_a_clear_never_deletes_the_object_created_since() {
    for shape in SHAPES {
        let mut k = Kernel::new();
        let root = object(b"o");
        create(&mut k, &root, shape);
        damage(&mut k, &root, shape);
        let cleared = clear_object(&k.snapshot(), &root)
            .unwrap()
            .expect("damaged");
        k.commit_any_order(&cleared.compiled);
        create(&mut k, &root, shape);
        let before = k.clone();
        let late = k.apply_any_order(&cleared.compiled, None);
        assert!(
            matches!(late, Err(Refused::ExpectedVersion(_))),
            "{shape:?}: {late:?}"
        );
        assert_eq!(k, before, "{shape:?}");
        assert_eq!(whole(&k, &root, shape), Ok(true), "{shape:?}");
    }
}

/// Guard 5 (arch §8): each delete names the version the clear read. A clean record written
/// again between the compile and the apply, the root untouched, refuses the whole batch.
#[test]
fn guard_5_a_record_written_again_before_apply_refuses_the_whole_clear() {
    let mut k = Kernel::new();
    let root = object(b"m");
    create(&mut k, &root, Shape::Map);
    damage(&mut k, &root, Shape::Map);
    let cleared = clear_object(&k.snapshot(), &root)
        .unwrap()
        .expect("damaged");
    // Clean records go first: `b` is the first delete.
    let b = element_key(&root, &text("b")).unwrap();
    assert!(
        matches!(&cleared.compiled.mutations[0], Mutation::Delete { key, .. } if *key == b),
        "{:?}",
        cleared.compiled.mutations
    );
    let bytes = k.records[&b].1.clone();
    overwrite(&mut k, b, bytes);
    let before = k.clone();
    assert_eq!(
        k.apply_any_order(&cleared.compiled, None),
        Err(Refused::ExpectedVersion(0))
    );
    assert_eq!(k, before);
}

/// Contract 4 and guard 6: a batch that does not empty the object keeps the root and is
/// guarded by it. The app writing the object between the compile and the apply refuses it.
#[test]
fn guard_6_a_batch_that_keeps_the_root_is_refused_once_the_root_is_written() {
    let mut k = Kernel::new();
    let root = object(b"big");
    let keys = filled(&mut k, &root, CollectionKind::Map, 300);
    let damaged = element_key(&root, &keys[150]).unwrap();
    let at = k.records[&damaged].0;
    k.records
        .insert(damaged, (at, Bytes::from_static(b"damaged")));
    let first = clear_object(&k.snapshot(), &root)
        .unwrap()
        .expect("damaged");
    let root_bytes = root.to_bytes();
    assert!(
        !first
            .compiled
            .mutations
            .iter()
            .any(|m| matches!(m, Mutation::Delete { key, .. } if *key == root_bytes)),
        "the first batch keeps the root"
    );
    let version = Expected::Version(root_version(&k, &root));
    let put = [ElemOp::Put(text("z"), text("1"))];
    let made = compile_collection(&k.snapshot(), &root, CollectionKind::Map, version, &put);
    k.commit(&made.unwrap());
    let before = k.clone();
    assert_eq!(
        k.apply_any_order(&first.compiled, None),
        Err(Refused::Condition(0))
    );
    assert_eq!(k, before);
}

/// Guard 16: a chunk the blob names and the proof found missing gets an `Absent` condition. A
/// chunk stored again before the apply refuses the clear, so it is never left behind unproved.
#[test]
fn guard_16_a_chunk_found_missing_and_stored_again_before_apply_refuses_the_clear() {
    let mut k = Kernel::new();
    let root = object(b"b");
    create(&mut k, &root, Shape::Blob);
    let gone = chunk_key(&root, &UP, 1);
    let (_, bytes) = k.records.remove(&gone).unwrap();
    let cleared = clear_object(&k.snapshot(), &root)
        .unwrap()
        .expect("damaged");
    let absent = Condition::Absent { key: gone.clone() };
    let at = cleared
        .compiled
        .conditions
        .iter()
        .position(|c| *c == absent)
        .expect("an Absent for the missing chunk");
    overwrite(&mut k, gone, bytes);
    let before = k.clone();
    assert_eq!(
        k.apply_any_order(&cleared.compiled, None),
        Err(Refused::Condition(at))
    );
    assert_eq!(k, before);
}

/// Guard 17: the proof also compiles the drop, which finds damage the read does not look for.
/// An item record beside an empty list reads cleanly, and is still proved damaged and cleared.
#[test]
fn guard_17_an_orphan_item_beside_an_empty_list_is_damage_the_drop_finds() {
    let mut k = Kernel::new();
    let root = object(b"l");
    let made = create_list(&k.snapshot(), &root, false, DEFAULT_BLOCK_MAX, &[]).unwrap();
    k.commit(made.compiled());
    let at = k.records[&root.to_bytes()].0;
    let orphan = seal(Kind::Document, &[0xf6]).unwrap();
    k.records.insert(item_key(&root, 7), (at, orphan));
    let s = k.snapshot();
    assert_eq!(
        whole(&k, &root, Shape::List),
        Ok(true),
        "the read does not look"
    );
    let cleared = clear_object(&s, &root).unwrap().expect("damaged");
    assert_eq!(cleared.damage, Corrupt::OrphanElement);
    k.commit_any_order(&cleared.compiled);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// Contract 6 (tester-s6): a healthy map and set of mixed keys, after an overwrite of one and a
/// del of a key not stored, read whole at every limit that reaches the end, and are not damaged.
/// Another object follows, and its records are never read as elements (M1).
#[test]
fn contract_6_a_healthy_map_and_set_of_mixed_keys_read_whole_and_are_not_damaged() {
    let keys = [
        text("a"),
        int(-5),
        int(7),
        float(1.5),
        float(-0.0),
        Value::Bytes(vec![0, 1]),
    ];
    for kind in [CollectionKind::Map, CollectionKind::Set] {
        let mut k = Kernel::new();
        let root = object(b"mixed");
        let element = |key: &Value| match kind {
            CollectionKind::Map => ElemOp::Put(key.clone(), text("1")),
            CollectionKind::Set => ElemOp::Add(key.clone()),
        };
        let ops: Vec<_> = keys.iter().map(element).collect();
        let made = compile_collection(&k.snapshot(), &root, kind, Expected::Absent, &ops);
        k.commit(&made.unwrap());
        let again = match kind {
            CollectionKind::Map => ElemOp::Put(float(-0.0), text("2")),
            CollectionKind::Set => ElemOp::Add(float(-0.0)),
        };
        for op in [again, ElemOp::Remove(text("missing"))] {
            let version = Expected::Version(root_version(&k, &root));
            let made = compile_collection(&k.snapshot(), &root, kind, version, &[op]).unwrap();
            if !made.mutations.is_empty() {
                k.commit(&made);
            }
        }
        neighbour(&mut k, &root, b"mixee");
        let s = k.snapshot();
        let n = keys.len();
        for limit in [n, n + 1, usize::MAX] {
            let got = members(&s, &root, None, limit).unwrap();
            assert_eq!(got.members.len(), n, "{kind:?} limit {limit}");
        }
        assert_eq!(
            clear_object(&s, &root),
            Err(ValueError::Apply(ApplyError::NotDamaged)),
            "{kind:?}"
        );
    }
}

/// Guard 3 (A-R139): envelope kind 0 is no kind any build writes, so a root of kind 0 is damage
/// and the clear deletes it. Only a kind above the known ones is a newer build's.
#[test]
fn guard_3_a_root_of_kind_0_is_damage_and_is_cleared() {
    let mut k = Kernel::new();
    let root = object(b"d");
    let mut bytes = seal(Kind::Document, &[0xf6]).unwrap().to_vec();
    bytes[1] = 0x00;
    let head: [u8; 8] = bytes[..8].try_into().unwrap();
    let sum = digest(&head, &bytes[40..]);
    bytes[8..40].copy_from_slice(&sum);
    overwrite(&mut k, root.to_bytes(), Bytes::from(bytes));
    let cleared = clear_object(&k.snapshot(), &root)
        .unwrap()
        .expect("damaged");
    assert_eq!(
        cleared.damage,
        Corrupt::Envelope(EnvelopeError::UnknownKind(0))
    );
    k.commit_any_order(&cleared.compiled);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

// ---- coverage gaps (tester-s6's review of ef7653c) -------------------------------------------

/// Clear what is stored under `root` while it has no root: one batch that deletes every record,
/// guarded by the root's absence. A root written before the apply refuses it whole.
fn assert_rootless_clear(mut k: Kernel, root: &RootKey, want: impl Fn(&Corrupt) -> bool) {
    let stored = k.records.len();
    let cleared = clear_object(&k.snapshot(), root).unwrap().expect("damaged");
    assert!(want(&cleared.damage), "{:?}", cleared.damage);
    let absent = Condition::Absent {
        key: root.to_bytes(),
    };
    assert_eq!(cleared.compiled.conditions, vec![absent]);
    assert_eq!(
        cleared.compiled.mutations.len(),
        stored,
        "every record, no root"
    );

    let mut raced = k.clone();
    overwrite(
        &mut raced,
        root.to_bytes(),
        seal(Kind::Document, &[0xf6]).unwrap(),
    );
    let before = raced.clone();
    assert_eq!(
        raced.apply_any_order(&cleared.compiled, None),
        Err(Refused::Condition(0))
    );
    assert_eq!(raced, before);

    k.commit_any_order(&cleared.compiled);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
    assert_eq!(clear_object(&k.snapshot(), root), Ok(None));
}

/// Gap 2: records with no root are an orphan (an element, an item, a block) or damage (a key
/// that does not parse). The clear deletes them, guarded by the root being absent.
#[test]
fn gap_2_records_with_no_root_are_cleared_while_the_root_stays_absent() {
    let root = object(b"o");
    let orphan = |c: &Corrupt| *c == Corrupt::OrphanElement;

    // Map elements.
    let mut k = Kernel::new();
    map_of_two(&mut k, &root);
    k.records.remove(&root.to_bytes()).unwrap();
    assert_rootless_clear(k, &root, orphan);

    // List items first, then its block; and a list's block alone.
    for records in [true, false] {
        let mut k = Kernel::new();
        let push: Vec<_> = ["x", "y"].iter().map(|v| ListOp::Push(text(v))).collect();
        let made = create_list(&k.snapshot(), &root, records, DEFAULT_BLOCK_MAX, &push).unwrap();
        k.commit(made.compiled());
        k.records.remove(&root.to_bytes()).unwrap();
        let first = parse(k.records.keys().next().unwrap()).unwrap().sub;
        assert_eq!(first, if records { Sub::Item } else { Sub::Block });
        assert_rootless_clear(k, &root, orphan);
    }

    // An item key whose id is 3 bytes, not 16.
    let mut k = Kernel::new();
    let mut bad = root.sub_prefix(2);
    bad.extend_from_slice(&[1, 2, 3]);
    assert!(parse(&bad).is_err(), "the key does not parse");
    overwrite(
        &mut k,
        Bytes::from(bad),
        seal(Kind::Document, &[0xf6]).unwrap(),
    );
    assert_rootless_clear(k, &root, |c| matches!(c, Corrupt::Key(_)));
}

/// A list of 40 items at `root` with one push pending in a change slot: a push far below a
/// quarter of the block's base is not folded into it. The slot's key.
fn list_with_a_pending_slot(k: &mut Kernel, root: &RootKey) -> Bytes {
    let push: Vec<_> = (0..40)
        .map(|i| ListOp::Push(text(&format!("item {i}"))))
        .collect();
    let made = create_list(&k.snapshot(), root, false, DEFAULT_BLOCK_MAX, &push).unwrap();
    k.commit(made.compiled());
    let version = list(&k.snapshot(), root).unwrap().unwrap().version;
    let push = [ListOp::Push(text("w"))];
    let made = compile_list(
        &k.snapshot(),
        root,
        Expected::Version(version),
        DEFAULT_BLOCK_MAX,
        &push,
    )
    .unwrap();
    k.commit(made.compiled());
    k.records
        .keys()
        .find(|key| parse(key).is_ok_and(|p| p.slot.is_some()))
        .expect("the push is pending in a slot")
        .clone()
}

/// Gap 3: never delete what this build cannot read. Each record below is a newer build's, beside
/// or inside an object that is otherwise damaged or sound, and refuses the whole clear.
#[test]
fn gap_3_records_only_a_newer_build_writes_refuse_the_clear() {
    let root = object(b"o");

    // A record under a `sub` no build this one knows writes, beside a damaged map.
    let mut k = Kernel::new();
    create(&mut k, &root, Shape::Map);
    damage(&mut k, &root, Shape::Map);
    overwrite(
        &mut k,
        Bytes::from(root.sub_prefix(0x09)),
        seal(Kind::Document, &[0xf6]).unwrap(),
    );
    assert_eq!(
        clear_object(&k.snapshot(), &root),
        Err(ValueError::Corrupt(Corrupt::UnknownSub(0x09)))
    );

    // A map root of an element key profile this build does not know.
    let mut k = Kernel::new();
    let mut fields = rdb_value::value::Map::new();
    fields.insert(MapKey::new("keys"), int(2));
    fields.insert(MapKey::new("count"), int(0));
    let payload = encode(&Value::Map(fields)).unwrap();
    overwrite(&mut k, root.to_bytes(), seal(Kind::Map, &payload).unwrap());
    assert_eq!(
        clear_object(&k.snapshot(), &root),
        Err(ValueError::Corrupt(Corrupt::UnknownKeyProfile(2)))
    );

    // A pending change slot a newer build wrote, inside a sound list.
    let mut k = Kernel::new();
    let slot = list_with_a_pending_slot(&mut k, &root);
    let at = k.records[&slot].0;
    k.records.insert(slot, (at, newer_build_record()));
    let got = clear_object(&k.snapshot(), &root);
    assert!(
        matches!(
            &got,
            Err(ValueError::Corrupt(Corrupt::Block {
                fault: BlockFault::OpBad {
                    fault: SlotFault::Envelope(EnvelopeError::UnknownCodec(0x02)),
                    ..
                },
                ..
            }))
        ),
        "{got:?}"
    );
}

/// Gap 3, the other side: a pending slot that is damaged, not a newer build's, and a block base
/// stored again after the root, are damage. Each is faulting, and the clear deletes it.
#[test]
fn gap_3_a_damaged_slot_and_a_block_newer_than_its_root_are_cleared() {
    let root = object(b"o");

    let mut k = Kernel::new();
    let slot = list_with_a_pending_slot(&mut k, &root);
    let at = k.records[&slot].0;
    k.records.insert(slot, (at, Bytes::from_static(b"damaged")));
    let cleared = clear_object(&k.snapshot(), &root)
        .unwrap()
        .expect("damaged");
    assert!(
        matches!(
            cleared.damage,
            Corrupt::Block {
                fault: BlockFault::OpBad { .. },
                ..
            }
        ),
        "{:?}",
        cleared.damage
    );
    assert_eq!(cleared.faulting, 1, "the slot");
    k.commit_any_order(&cleared.compiled);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());

    let mut k = Kernel::new();
    create(&mut k, &root, Shape::List);
    let block = damaged_key(&k, &root, Shape::List);
    let bytes = k.records[&block].1.clone();
    overwrite(&mut k, block, bytes);
    let cleared = clear_object(&k.snapshot(), &root)
        .unwrap()
        .expect("damaged");
    assert!(
        matches!(
            cleared.damage,
            Corrupt::Block {
                fault: BlockFault::NewerThanRoot { .. },
                ..
            }
        ),
        "{:?}",
        cleared.damage
    );
    assert_eq!(cleared.faulting, 1, "the block");
    k.commit_any_order(&cleared.compiled);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// Gap 4: a batch is cut by bytes before it is cut by count. A map of 300 elements with ~5 KiB
/// keys does not fit one 1 MiB record, so its first batch holds fewer than 254 deletes, keeps
/// the root and is guarded by it, and the root goes only in the batch that empties the rest.
/// Not reachable through the API, whose keys are at most 1 KiB (tester-s6).
#[test]
fn gap_4_a_clear_of_wide_keys_is_cut_by_bytes_and_keeps_the_root_until_the_end() {
    use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
    use rdb_core::transaction::record_len;

    let mut k = Kernel::new();
    let root = object(b"wide");
    let pad = "x".repeat(5000);
    let keys: Vec<Value> = (0..300).map(|i| text(&format!("{i:04}{pad}"))).collect();
    for (i, chunk) in keys.chunks(50).enumerate() {
        let ops: Vec<_> = chunk
            .iter()
            .map(|key| ElemOp::Put(key.clone(), text("1")))
            .collect();
        let expected = if i == 0 {
            Expected::Absent
        } else {
            Expected::Version(root_version(&k, &root))
        };
        let made =
            compile_collection(&k.snapshot(), &root, CollectionKind::Map, expected, &ops).unwrap();
        k.commit(&made);
    }
    let damaged = element_key(&root, &keys[150]).unwrap();
    let at = k.records[&damaged].0;
    k.records
        .insert(damaged, (at, Bytes::from_static(b"damaged")));

    let root_bytes = root.to_bytes();
    let mut sizes = Vec::new();
    while let Some(cleared) = clear_object(&k.snapshot(), &root).unwrap() {
        let c = &cleared.compiled;
        assert!(
            record_len(c.conditions.len(), &c.mutations) <= MAX_ENVELOPE_BYTES,
            "batch {} fits one record",
            sizes.len()
        );
        let deletes_root = c
            .mutations
            .iter()
            .any(|m| matches!(m, Mutation::Delete { key, .. } if *key == root_bytes));
        if !deletes_root {
            let guard = Condition::VersionEquals {
                key: root_bytes.clone(),
                version: root_version(&k, &root),
            };
            assert_eq!(c.conditions, vec![guard], "batch {}", sizes.len());
        }
        sizes.push((c.mutations.len(), deletes_root));
        k.commit_any_order(c);
        assert!(sizes.len() < 10, "the clear ends");
    }
    assert!(sizes.len() >= 2, "{sizes:?}");
    assert!(sizes[0].0 < 254, "cut by bytes, not count: {sizes:?}");
    let (last, earlier) = sizes.split_last().unwrap();
    assert!(last.1, "the last batch deletes the root: {sizes:?}");
    assert!(earlier.iter().all(|b| !b.1), "{sizes:?}");
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// Whether `cleared` deletes the root of `root`: the batch that empties the object.
fn deletes_root(cleared: &rdb_value::repair::Cleared, root: &RootKey) -> bool {
    let root = root.to_bytes();
    cleared
        .compiled
        .mutations
        .iter()
        .any(|m| matches!(m, Mutation::Delete { key, .. } if *key == root))
}

/// Review F-003 (C-1): a map root that counts 46 elements over 300 stored, every element clean.
/// A first batch of 254 would leave exactly the 46 the root counts, so the map would read back
/// as healthy while half cleared, and the next clear would answer "not damaged" over 46
/// leftovers. A batch that keeps the root never leaves exactly `count` elements, so after every
/// call but the last the map still reads as damaged. Not reachable through any M9 write.
#[test]
fn f003_a_batch_that_keeps_the_root_never_leaves_the_count_it_names() {
    let mut k = Kernel::new();
    let root = object(b"big");
    filled(&mut k, &root, CollectionKind::Map, 300);
    let (version, bytes) = k.records[&root.to_bytes()].clone();
    let Value::Map(mut fields) = decode(open(&bytes).unwrap().payload).unwrap() else {
        panic!("a map root is a map");
    };
    *fields.get_mut(&MapKey::new("count")).unwrap() = int(46);
    let sealed = seal(Kind::Map, &encode(&Value::Map(fields)).unwrap()).unwrap();
    k.records.insert(root.to_bytes(), (version, sealed));

    let mut calls = 0;
    while let Some(cleared) = clear_object(&k.snapshot(), &root).unwrap() {
        calls += 1;
        assert!(calls < 5, "the clear ends");
        let last = deletes_root(&cleared, &root);
        k.commit_any_order(&cleared.compiled);
        if !last {
            let read = members(&k.snapshot(), &root, None, usize::MAX);
            assert!(
                matches!(
                    &read,
                    Err(ValueError::Corrupt(Corrupt::CountMismatch {
                        count: 46,
                        ..
                    }))
                ),
                "after call {calls}, the root kept: {read:?}"
            );
        }
    }
    assert_eq!(calls, 2);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// Review F-006: `Cleared.faulting` counts the faulting records a batch deletes, on a batch that
/// keeps the root too. A map of 300 with 290 damaged: the first batch deletes the 10 clean
/// elements, then 244 faulting ones, and keeps the root; the second deletes the last 46 and the
/// root.
#[test]
fn f006_faulting_counts_the_faulting_deletes_of_every_batch() {
    let mut k = Kernel::new();
    let root = object(b"big");
    let keys = filled(&mut k, &root, CollectionKind::Map, 300);
    for key in &keys[10..] {
        let key = element_key(&root, key).unwrap();
        let at = k.records[&key].0;
        k.records.insert(key, (at, Bytes::from_static(b"damaged")));
    }
    let mut batches = Vec::new();
    while let Some(cleared) = clear_object(&k.snapshot(), &root).unwrap() {
        batches.push((
            cleared.compiled.mutations.len(),
            cleared.faulting,
            deletes_root(&cleared, &root),
        ));
        k.commit_any_order(&cleared.compiled);
        assert!(batches.len() < 5, "the clear ends");
    }
    assert_eq!(batches, vec![(254, 244, false), (47, 46, true)]);
}

/// `shape` created, then the record [`damage`] would break replaced by a newer build's, at the
/// version it had, so the read opens it rather than stopping at a version check. The whole read.
fn whole_with_a_newer_build_record(shape: Shape) -> Result<bool, ValueError> {
    let mut k = Kernel::new();
    let root = object(b"o");
    create(&mut k, &root, shape);
    let key = damaged_key(&k, &root, shape);
    let at = k.records[&key].0;
    k.records.insert(key, (at, newer_build_record()));
    whole(&k, &root, shape)
}

/// Review F-005 (ADR-rdb-0012 §12): a range read that meets a newer build's chunk names a newer
/// build's record, which the host answers `INCOMPATIBLE_VERSION`, not `CORRUPT_BLOB`, and logs
/// no `stored_damage` for.
#[test]
fn f005_a_read_of_a_newer_builds_chunk_names_a_newer_build() {
    let read = whole_with_a_newer_build_record(Shape::Blob);
    assert!(
        matches!(&read, Err(ValueError::Corrupt(c @ Corrupt::Chunk { .. })) if c.is_newer_build()),
        "{read:?}"
    );
}

/// Review F-005: an items read that meets a newer build's list block base names a newer build's
/// record, which the host answers `INCOMPATIBLE_VERSION`, not `CORRUPT_HISTORY`.
#[test]
fn f005_a_read_of_a_newer_builds_list_block_names_a_newer_build() {
    let read = whole_with_a_newer_build_record(Shape::List);
    assert!(
        matches!(
            &read,
            Err(ValueError::Corrupt(
                c @ Corrupt::Block {
                    fault: BlockFault::Envelope(_),
                    ..
                }
            )) if c.is_newer_build()
        ),
        "{read:?}"
    );
}

/// Review R-1 (PR #53): an empty list with 254 sound orphan items beside its block base. Item
/// keys sort before block keys, so a first batch of 254 deleted every orphan and kept the base
/// and the root: a healthy empty list, and the next clear answered "not damaged" with the object
/// still there. A batch that keeps any record leaves the object still proved damaged, so
/// the first batch takes back one orphan delete (253), and the second takes the last orphan,
/// the base and the root (3).
#[test]
fn r1_an_empty_list_with_254_orphan_items_is_cleared_to_the_end() {
    let mut k = Kernel::new();
    let root = object(b"l");
    let made = create_list(&k.snapshot(), &root, false, DEFAULT_BLOCK_MAX, &[]).unwrap();
    k.commit(made.compiled());
    let at = k.records[&root.to_bytes()].0;
    for id in 1..=254 {
        let orphan = seal(Kind::Document, &[0xf6]).unwrap();
        k.records.insert(item_key(&root, id), (at, orphan));
    }
    assert_eq!(calls_to_clear(&mut k, &root), vec![253, 3]);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// Clears the object until `Ok(None)`, and returns how many deletes each batch sent. Any error
/// fails the test, with the call it came on.
fn calls_to_clear(k: &mut Kernel, root: &RootKey) -> Vec<usize> {
    let mut sent = Vec::new();
    loop {
        let cleared = match clear_object(&k.snapshot(), root) {
            Ok(Some(cleared)) => cleared,
            Ok(None) => return sent,
            Err(error) => panic!(
                "call {} with {} records left: {error:?}",
                sent.len() + 1,
                k.records.len()
            ),
        };
        sent.push(cleared.compiled.mutations.len());
        assert!(sent.len() < 5, "the clear ends: {sent:?}");
        k.commit_any_order(&cleared.compiled);
    }
}

/// `n` upload chunks stored under the object, whatever its root's kind: `put_chunk` does not
/// check it.
fn stray_chunks(k: &mut Kernel, root: &RootKey, n: u32) {
    for i in 0..n {
        let upload = [u8::try_from(i / 255).unwrap() + 1; 16];
        k.seq += 1;
        let chunk = seal(Kind::Chunk, b"x").unwrap();
        k.records
            .insert(chunk_key(root, &upload, i % 255), (k.seq, chunk));
    }
}

/// No root, one orphan element, and `chunks` upload chunks: they sort after the orphan.
fn orphan_beside(chunks: u32) -> (Kernel, RootKey) {
    let mut k = Kernel::new();
    let root = object(b"o");
    map_of_two(&mut k, &root);
    k.records.remove(&root.to_bytes()).unwrap();
    k.records
        .remove(&element_key(&root, &text("b")).unwrap())
        .unwrap();
    stray_chunks(&mut k, &root, chunks);
    (k, root)
}

/// Review R2-01 (PR #53), case A: with no root, the chunks go in the first batch and the orphan
/// stays, so the object still proves damaged; the next batch takes the orphan. Before the fix
/// the chunks sorted after the orphan, every delete was taken back, and each call answered
/// `TooLarge`.
#[test]
fn r2_01_a_rootless_orphan_beside_254_chunks_is_cleared_to_the_end() {
    let (mut k, root) = orphan_beside(254);
    assert_eq!(calls_to_clear(&mut k, &root), vec![254, 1]);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// Review R2-01, control C: 1 orphan and 253 chunks fit one batch, which is final.
#[test]
fn r2_01_c_a_rootless_orphan_beside_253_chunks_is_cleared_in_one_batch() {
    let (mut k, root) = orphan_beside(253);
    assert_eq!(calls_to_clear(&mut k, &root), vec![254]);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// Review R2-01, case B: a map whose root says 0 elements, with 2 and 300 chunks. The first
/// batch is 254 chunks and is sent whole, so it costs one proof and not one per delete taken
/// back; the second takes the rest and the root.
#[test]
fn r2_01_b_a_map_counting_0_beside_300_chunks_is_cleared_to_the_end() {
    let mut k = Kernel::new();
    let root = object(b"m");
    map_of_two(&mut k, &root);
    let (at, bytes) = k.records[&root.to_bytes()].clone();
    let Value::Map(mut fields) = decode(open(&bytes).unwrap().payload).unwrap() else {
        panic!("a map root");
    };
    *fields.get_mut(&MapKey::new("count")).unwrap() = int(0);
    let sealed = seal(Kind::Map, &encode(&Value::Map(fields)).unwrap()).unwrap();
    k.records.insert(root.to_bytes(), (at, sealed));
    stray_chunks(&mut k, &root, 300);
    assert_eq!(calls_to_clear(&mut k, &root), vec![254, 49]);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// Review R-4 and Q-1 (PR #53): the clear of a list compiled while `gone` is missing carries
/// an `Absent` for it. Left missing, the batch applies. Stored again alone, with the root not
/// rewritten, the whole batch is refused and nothing changes: the proof is stale, since the
/// list may now read back cleanly.
fn assert_missing_record_guarded(mut k: Kernel, root: &RootKey, gone: &Bytes) {
    let (_, bytes) = k.records.remove(gone).expect("the record to remove");
    let cleared = clear_object(&k.snapshot(), root).unwrap().expect("damaged");
    let absent = Condition::Absent { key: gone.clone() };
    let at = cleared
        .compiled
        .conditions
        .iter()
        .position(|c| *c == absent)
        .expect("an Absent for the missing record");
    let mut control = k.clone();
    assert!(
        control.apply_any_order(&cleared.compiled, None).is_ok(),
        "the batch applies while the record is still missing"
    );
    overwrite(&mut k, gone.clone(), bytes);
    let before = k.clone();
    assert_eq!(
        k.apply_any_order(&cleared.compiled, None),
        Err(Refused::Condition(at))
    );
    assert_eq!(k, before);
}

/// Review R-4: a block base the list names, found missing.
#[test]
fn r4_a_list_block_found_missing_and_stored_again_before_apply_refuses_the_clear() {
    let mut k = Kernel::new();
    let root = object(b"l");
    create(&mut k, &root, Shape::List);
    let base = damaged_key(&k, &root, Shape::List);
    assert_missing_record_guarded(k, &root, &base);
}

/// Review R-4: an item record the list names, found missing.
#[test]
fn r4_a_list_item_found_missing_and_stored_again_before_apply_refuses_the_clear() {
    let mut k = Kernel::new();
    let root = object(b"l");
    let push: Vec<_> = ["x", "y", "z"]
        .iter()
        .map(|v| ListOp::Push(text(v)))
        .collect();
    let made = create_list(&k.snapshot(), &root, true, DEFAULT_BLOCK_MAX, &push).unwrap();
    k.commit(made.compiled());
    let item = k
        .records
        .keys()
        .find(|key| parse(key).is_ok_and(|p| p.sub == Sub::Item))
        .expect("the list stores its items as records")
        .clone();
    assert_missing_record_guarded(k, &root, &item);
}

/// Review Q-1: a pending change slot the root names, found missing (`BlockFault::OpMissing`).
#[test]
fn q1_a_pending_slot_found_missing_and_stored_again_before_apply_refuses_the_clear() {
    let mut k = Kernel::new();
    let root = object(b"l");
    let slot = list_with_a_pending_slot(&mut k, &root);
    assert_missing_record_guarded(k, &root, &slot);
}

/// Review Q-1, the other side: a pending slot that is stored but holds another op is also
/// `OpMissing`. It is deleted at its version, never guarded `Absent`, or no batch could apply.
#[test]
fn q1_a_slot_holding_another_op_is_deleted_not_guarded_absent() {
    let mut k = Kernel::new();
    let root = object(b"l");
    let first = list_with_a_pending_slot(&mut k, &root);
    let version = list(&k.snapshot(), &root).unwrap().unwrap().version;
    let push = [ListOp::Push(text("v"))];
    let made = compile_list(
        &k.snapshot(),
        &root,
        Expected::Version(version),
        DEFAULT_BLOCK_MAX,
        &push,
    )
    .unwrap();
    k.commit(made.compiled());
    let second = k
        .records
        .keys()
        .find(|key| **key != first && parse(key).is_ok_and(|p| p.slot.is_some()))
        .expect("the second push is pending in a slot")
        .clone();
    let at = k.records[&first].0;
    let other = k.records[&second].1.clone();
    k.records.insert(first.clone(), (at, other));
    let cleared = clear_object(&k.snapshot(), &root)
        .unwrap()
        .expect("damaged");
    assert!(
        matches!(
            cleared.damage,
            Corrupt::Block {
                fault: BlockFault::OpMissing { .. },
                ..
            }
        ),
        "{:?}",
        cleared.damage
    );
    assert!(
        !cleared
            .compiled
            .conditions
            .iter()
            .any(|c| matches!(c, Condition::Absent { key } if *key == first)),
        "{:?}",
        cleared.compiled.conditions
    );
    assert_eq!(clear_to_the_end(&mut k, &root), 1);
    assert!(k.records.is_empty(), "{:?}", k.records.keys());
}

/// A map element key under tag `0x11`, which profile 1 does not define: a key type a newer
/// build may add (ADR-rdb-0013 decision 4).
fn unknown_tag_key(root: &RootKey) -> Bytes {
    let mut key = root.element_prefix();
    key.extend_from_slice(&[0x11, 0x01, 0x02]);
    Bytes::from(key)
}

/// Review R-3 (PR #53, A-R145): an entry under an element key tag this build does not know may
/// be a newer build's valid entry, so it refuses the whole clear as an unknown sub does, and
/// nothing is deleted. In a sound map the read finds it first; beside a root that does not open,
/// only the scan does. A read of the map names it a newer build's too.
#[test]
fn r3_a_map_entry_under_an_unknown_key_tag_refuses_the_clear() {
    let root = object(b"m");
    let refused = Err(ValueError::Corrupt(Corrupt::Key(KeyError::UnknownTag(
        0x11,
    ))));

    let mut k = Kernel::new();
    map_of_two(&mut k, &root);
    let (at, bytes) = k.records[&root.to_bytes()].clone();
    let Value::Map(mut fields) = decode(open(&bytes).unwrap().payload).unwrap() else {
        panic!("a map root is a map");
    };
    *fields.get_mut(&MapKey::new("count")).unwrap() = int(3);
    let sealed = seal(Kind::Map, &encode(&Value::Map(fields)).unwrap()).unwrap();
    k.records.insert(root.to_bytes(), (at, sealed));
    let entry = k.records[&element_key(&root, &text("a")).unwrap()]
        .1
        .clone();
    k.records
        .insert(unknown_tag_key(&root), (at, entry.clone()));
    let read = members(&k.snapshot(), &root, None, usize::MAX);
    assert!(
        matches!(&read, Err(ValueError::Corrupt(c)) if c.is_newer_build()),
        "{read:?}"
    );
    assert_eq!(clear_object(&k.snapshot(), &root), refused, "a sound map");

    let mut k = Kernel::new();
    map_of_two(&mut k, &root);
    let at = k.records[&root.to_bytes()].0;
    k.records
        .insert(root.to_bytes(), (at, Bytes::from_static(b"damaged")));
    k.records.insert(unknown_tag_key(&root), (at, entry));
    assert_eq!(
        clear_object(&k.snapshot(), &root),
        refused,
        "beside a root that does not open"
    );
}
