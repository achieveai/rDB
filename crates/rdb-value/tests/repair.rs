//! Clearing a damaged object at the library API (M9 S6; ADR-rdb-0013 decision 11).
//!
//! Scenario: an admin clears an object the read refuses, and the clear never deletes a record a
//! newer build wrote, whatever else is wrong with the object.

mod common;

use bytes::Bytes;
use common::{int, text, Kernel};
use rdb_core::{AffinityId, TenantId};
use rdb_value::cbor::{decode, encode};
use rdb_value::collection::{collection, compile_collection, members, CollectionKind, ElemOp};
use rdb_value::envelope::{digest, open, seal, EnvelopeError, Kind};
use rdb_value::keys::{element_key, parse, root_key, RootKey, Sub};
use rdb_value::list::{compile_list, create_list, items, list, ListOp, Start, DEFAULT_BLOCK_MAX};
use rdb_value::repair::clear_object;
use rdb_value::value::{Int, MapKey, Value};
use rdb_value::{BlockFault, Corrupt, Expected, ValueError};

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
    assert_newer_build_refused(&k, &root, "sound map root");
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
/// is checked first.
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
    assert_newer_build_refused(&k, &list, "sound list root");
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
#[test]
fn w2a_a_half_cleared_map_or_set_never_reads_as_healthy() {
    for kind in [CollectionKind::Map, CollectionKind::Set] {
        let mut k = Kernel::new();
        let root = object(b"big");
        let stored = half_cleared_then_fixed(&mut k, &root, kind, 300);
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
    assert_eq!(
        got,
        Err(ValueError::Apply(rdb_value::delta::ApplyError::OnlyChunks))
    );
    let text = got.unwrap_err().to_string();
    assert!(text.contains("blob gc"), "{text}");
}
