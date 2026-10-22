//! A newer build's record meets this build (M9 S7; ADR-rdb-0012 §12, ruling A-R144).
//!
//! Scenario: a rollback leaves records a newer build wrote. Within one record, a newer build's
//! envelope is named before anything else about it, by every read and by a map write, and a map
//! write never overwrites, deletes or vouches for one. Damage is still damage.

mod common;

use bytes::Bytes;
use common::{int, text, Kernel};
use rdb_core::{AffinityId, TenantId};
use rdb_value::collection::{compile_collection, member, members, CollectionKind, ElemOp};
use rdb_value::delta::ApplyError;
use rdb_value::envelope::{
    digest, newer_build, open, seal, EnvelopeError, Kind, HEADER_LEN, MAX_ENVELOPE, MAX_PAYLOAD,
};
use rdb_value::keys::{element_key, parse, root_key, RootKey, Sub};
use rdb_value::list::{compile_list, create_list, items, ListOp, Start, MIN_BLOCK_MAX};
use rdb_value::repair::clear_object;
use rdb_value::{BlockFault, Corrupt, Expected, ValueError};

fn object(id: &[u8]) -> RootKey {
    root_key(TenantId(1), AffinityId(1), id)
}

/// `bytes` with its digest recomputed over header bytes 0..8 and the payload.
fn rehash(mut bytes: Vec<u8>) -> Vec<u8> {
    let head: [u8; 8] = bytes[..8].try_into().unwrap();
    let sum = digest(&head, &bytes[HEADER_LEN..]);
    bytes[8..HEADER_LEN].copy_from_slice(&sum);
    bytes
}

/// A sound document envelope with `codec_version` `0x02`, digest correct: what
/// `rdb_dev admin damage --as newer` writes.
fn newer_record() -> Bytes {
    let mut bytes = seal(Kind::Document, &[0xf6]).unwrap().to_vec();
    bytes[2] = 0x02;
    Bytes::from(rehash(bytes))
}

/// A document envelope whose payload byte is flipped: damage, not a newer build's.
fn damaged_record() -> Bytes {
    let mut bytes = seal(Kind::Document, &[0xf6]).unwrap().to_vec();
    bytes[HEADER_LEN] ^= 0xff;
    Bytes::from(bytes)
}

fn is_newer(got: &ValueError) -> bool {
    matches!(got, ValueError::Corrupt(c) if c.is_newer_build())
}

const NEWER: ValueError = ValueError::Corrupt(Corrupt::Envelope(EnvelopeError::UnknownCodec(0x02)));

/// A-R144 / A-1, guard 2: the fast path is exact. For every value of every header byte 0–3,
/// with a correct digest and with a flipped payload byte, and at the size edges, `newer_build`
/// answers exactly what `open` would: its error when that is a newer build's, else `None`.
#[test]
fn s7_newer_build_answers_as_open_for_every_header_byte_value() {
    let same = |bytes: &[u8], at: &str| {
        let want = open(bytes).err().filter(EnvelopeError::is_newer_build);
        assert_eq!(newer_build(bytes), want, "{at}");
    };
    let sound = seal(Kind::Document, &[0xa1, 0x61, 0x6e, 0x01])
        .unwrap()
        .to_vec();
    for at in 0..4 {
        for value in 0..=255_u8 {
            let mut changed = sound.clone();
            changed[at] = value;
            let hashed = rehash(changed.clone());
            same(
                &hashed,
                &format!("byte {at} = {value:#04x}, digest correct"),
            );
            let mut flipped = hashed;
            flipped[HEADER_LEN] ^= 0x01;
            same(
                &flipped,
                &format!("byte {at} = {value:#04x}, payload flipped"),
            );
            same(&changed, &format!("byte {at} = {value:#04x}, digest stale"));
        }
    }
    let biggest = seal(Kind::Document, &vec![0; MAX_PAYLOAD])
        .unwrap()
        .to_vec();
    let mut over = biggest.clone();
    over.push(0);
    let mut newer_biggest = biggest.clone();
    newer_biggest[2] = 0x02;
    let newer_biggest = rehash(newer_biggest);
    let empty = seal(Kind::Document, &[]).unwrap().to_vec();
    let mut newer_empty = empty.clone();
    newer_empty[2] = 0x02;
    let newer_empty = rehash(newer_empty);
    for (bytes, at) in [
        (&sound[..HEADER_LEN - 1], "39 bytes"),
        (&empty[..], "40 bytes"),
        (&newer_empty[..], "40 bytes, codec 0x02"),
        (&biggest[..], "MAX_ENVELOPE"),
        (&newer_biggest[..], "MAX_ENVELOPE, codec 0x02"),
        (&over[..], "MAX_ENVELOPE + 1"),
    ] {
        same(bytes, at);
    }
    assert_eq!(biggest.len(), MAX_ENVELOPE);
    // Controls: the rule's two answers each occur.
    assert_eq!(
        newer_build(&newer_empty),
        Some(EnvelopeError::UnknownCodec(0x02))
    );
    assert_eq!(newer_build(&damaged_record()), None);
}

/// A map `m` holding `a` and `b`, at one version.
fn map_ab(k: &mut Kernel, root: &RootKey) {
    let ops = [
        ElemOp::Put(text("a"), int(1)),
        ElemOp::Put(text("b"), int(2)),
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

/// `bytes` at `key`, either in a commit of its own (above its root, as `admin damage` leaves it)
/// or at the version it had (at its root, as a real newer build leaves it).
fn store(k: &mut Kernel, key: Bytes, bytes: Bytes, above_root: bool) {
    let at = if above_root {
        k.seq += 1;
        k.seq
    } else {
        k.records[&key].0
    };
    k.records.insert(key, (at, bytes));
}

fn root_version(k: &Kernel, root: &RootKey) -> u64 {
    k.records[&root.to_bytes()].0
}

/// S7-1 steps 3 and 7, guard 3: a newer build's map entry reads as a newer build's, above or at
/// its root, by `members` and by `member`. A page that stops before it reads as usual.
#[test]
fn s7_a_newer_map_entry_reads_as_a_newer_build_above_or_at_its_root() {
    for above_root in [true, false] {
        let mut k = Kernel::new();
        let root = object(b"m");
        map_ab(&mut k, &root);
        let b = element_key(&root, &text("b")).unwrap();
        store(&mut k, b, newer_record(), above_root);
        let s = k.snapshot();
        let at = format!("above_root={above_root}");
        assert_eq!(
            members(&s, &root, None, usize::MAX).unwrap_err(),
            NEWER,
            "{at}"
        );
        assert_eq!(member(&s, &root, &text("b")).unwrap_err(), NEWER, "{at}");
        let page = members(&s, &root, None, 1).expect("a page before b");
        assert_eq!(page.members.len(), 1, "{at}");
        assert!(members(&s, &root, Some(&text("b")), 10).is_ok(), "{at}");
    }
}

/// Guard 4: `members` names a newer build's entry before it decodes the entry's key, so an
/// element key that does not decode, holding a newer build's envelope, reads as a newer build's.
#[test]
fn s7_members_names_a_newer_entry_before_its_key() {
    let mut k = Kernel::new();
    let root = object(b"m");
    map_ab(&mut k, &root);
    let mut key = root.element_prefix();
    key.extend_from_slice(&[0x00, 0xff]);
    let key = Bytes::from(key);
    let at = root_version(&k, &root);
    k.records.insert(key.clone(), (at, damaged_record()));
    let damaged = members(&k.snapshot(), &root, None, usize::MAX).unwrap_err();
    assert!(
        matches!(damaged, ValueError::Corrupt(Corrupt::Key(_))),
        "control: the key is named for damage: {damaged:?}"
    );
    k.records.insert(key, (at, newer_record()));
    assert_eq!(
        members(&k.snapshot(), &root, None, usize::MAX).unwrap_err(),
        NEWER
    );
}

/// S7-1 steps 4, 8, 9 and row 1.10, guards 5 and 6 (A-R144 Q2, A-R146): a map write that names a
/// newer build's entry — `put`, `del`, `need`, as the first op or a later one — is refused as a
/// newer build's, above or at its root, before `ElementNewerThanRoot`. One naming other keys
/// compiles and writes nothing for the entry.
#[test]
fn s7_a_map_write_naming_a_newer_entry_is_refused() {
    for above_root in [true, false] {
        let mut k = Kernel::new();
        let root = object(b"m");
        map_ab(&mut k, &root);
        let b = element_key(&root, &text("b")).unwrap();
        store(&mut k, b.clone(), newer_record(), above_root);
        let s = k.snapshot();
        let version = Expected::Version(root_version(&k, &root));
        for ops in [
            vec![ElemOp::Put(text("b"), int(5))],
            vec![ElemOp::Remove(text("b"))],
            vec![ElemOp::NeedPresent(text("b"))],
            vec![ElemOp::NeedAbsent(text("b"))],
            vec![
                ElemOp::Put(text("d"), int(4)),
                ElemOp::Put(text("b"), int(9)),
            ],
            vec![ElemOp::Remove(text("b")), ElemOp::Put(text("z"), int(1))],
        ] {
            let got = compile_collection(&s, &root, CollectionKind::Map, version, &ops);
            assert_eq!(got.unwrap_err(), NEWER, "above_root={above_root} {ops:?}");
        }
        let others = [
            ElemOp::Put(text("a"), int(7)),
            ElemOp::Put(text("c"), int(3)),
        ];
        let ok = compile_collection(&s, &root, CollectionKind::Map, version, &others)
            .expect("a write naming other keys compiles");
        assert!(
            ok.mutations.iter().all(|m| !matches!(m,
                rdb_core::Mutation::Put { key, .. } | rdb_core::Mutation::Delete { key, .. }
                    if *key == b)),
            "above_root={above_root}: b is not written"
        );
    }
}

/// ADR-rdb-0013 §10 precedence (A-R144): a write naming a newer build's entry answers that
/// before `TooManyWrites`, whether the entry is named first or last.
#[test]
fn s7_a_newer_entry_is_named_before_too_many_writes() {
    let mut k = Kernel::new();
    let root = object(b"m");
    map_ab(&mut k, &root);
    let b = element_key(&root, &text("b")).unwrap();
    store(&mut k, b, newer_record(), false);
    let s = k.snapshot();
    let version = Expected::Version(root_version(&k, &root));
    let many: Vec<_> = (0..300).map(|i| ElemOp::Put(int(i), int(i))).collect();
    let too_many = compile_collection(&s, &root, CollectionKind::Map, version, &many).unwrap_err();
    assert!(
        matches!(
            too_many,
            ValueError::Apply(ApplyError::TooManyWrites { .. })
        ),
        "control: {too_many:?}"
    );
    for first in [true, false] {
        let mut ops = many.clone();
        let named = ElemOp::Put(text("b"), int(5));
        if first {
            ops.insert(0, named);
        } else {
            ops.push(named);
        }
        let got = compile_collection(&s, &root, CollectionKind::Map, version, &ops);
        assert_eq!(got.unwrap_err(), NEWER, "first={first}");
    }
}

/// S6's remedy still holds (ADR-rdb-0013 §10): a `put` or `del` over a damaged map entry, at its
/// root, compiles and replaces or removes it. Damage is not a newer build's record.
#[test]
fn s7_a_put_or_del_over_a_damaged_entry_still_compiles() {
    let mut k = Kernel::new();
    let root = object(b"m");
    map_ab(&mut k, &root);
    let b = element_key(&root, &text("b")).unwrap();
    store(&mut k, b, damaged_record(), false);
    let s = k.snapshot();
    let version = Expected::Version(root_version(&k, &root));
    assert!(!is_newer(
        &members(&s, &root, None, usize::MAX).unwrap_err()
    ));
    for ops in [
        vec![ElemOp::Put(text("b"), int(5))],
        vec![ElemOp::Remove(text("b"))],
    ] {
        assert!(
            compile_collection(&s, &root, CollectionKind::Map, version, &ops).is_ok(),
            "{ops:?}"
        );
    }
}

/// S7-1 row 1.6, guard 7: a set member has no envelope, so one holding a newer build's envelope
/// is damage on read, a set write does not open it, and the clear removes it.
#[test]
fn s7_a_set_member_is_never_a_newer_builds() {
    let mut k = Kernel::new();
    let root = object(b"s");
    let ops = [ElemOp::Add(int(1)), ElemOp::Add(int(2))];
    let made = compile_collection(
        &k.snapshot(),
        &root,
        CollectionKind::Set,
        Expected::Absent,
        &ops,
    )
    .unwrap();
    k.commit(&made);
    let one = element_key(&root, &int(1)).unwrap();
    store(&mut k, one, newer_record(), false);
    let s = k.snapshot();
    let read = members(&s, &root, None, usize::MAX).unwrap_err();
    assert!(
        matches!(read, ValueError::Corrupt(Corrupt::SetMemberHasValue { .. })),
        "{read:?}"
    );
    let version = Expected::Version(root_version(&k, &root));
    for ops in [vec![ElemOp::Add(int(1))], vec![ElemOp::Remove(int(1))]] {
        assert!(
            compile_collection(&s, &root, CollectionKind::Set, version, &ops).is_ok(),
            "{ops:?}"
        );
    }
    assert!(clear_object(&s, &root).expect("cleared").is_some());
}

/// A list `l` of a 300-byte item (its own record) and a short one, in a block of `MIN_BLOCK_MAX`.
fn list_with_record(k: &mut Kernel, root: &RootKey) {
    let push = [ListOp::Push(text(&"x".repeat(300))), ListOp::Push(int(1))];
    let made = create_list(&k.snapshot(), root, false, MIN_BLOCK_MAX, &push).unwrap();
    k.commit(made.compiled());
}

fn record_of(k: &Kernel, sub: Sub) -> Bytes {
    k.records
        .keys()
        .find(|key| parse(key).is_ok_and(|p| p.sub == sub && p.slot.is_none()))
        .expect("the list has that record")
        .clone()
}

/// S7-2 steps 3–5, guard 8: a newer build's item record reads as a newer build's, above or at
/// its root, and a `remove` or `replace` of it is refused the same way. A page that stops before
/// it reads as usual.
#[test]
fn s7_a_newer_list_item_reads_and_writes_as_a_newer_build() {
    for above_root in [true, false] {
        let mut k = Kernel::new();
        let root = object(b"l");
        list_with_record(&mut k, &root);
        let item = record_of(&k, Sub::Item);
        store(&mut k, item, newer_record(), above_root);
        let s = k.snapshot();
        let at = format!("above_root={above_root}");
        let read = items(&s, &root, Start::Position(0), usize::MAX).unwrap_err();
        assert_eq!(read, NEWER, "{at}");
        assert!(items(&s, &root, Start::Position(1), 10).is_ok(), "{at}");
        let version = Expected::Version(root_version(&k, &root));
        for op in [
            ListOp::Remove { at: 0 },
            ListOp::Replace {
                at: 0,
                value: int(3),
            },
        ] {
            let got = compile_list(&s, &root, version, MIN_BLOCK_MAX, std::slice::from_ref(&op));
            assert_eq!(got.map(drop).unwrap_err(), NEWER, "{at} {op:?}");
        }
    }
}

/// Guard 9: a newer build's block base above its root reads as a newer build's block, not
/// `NewerThanRoot`.
#[test]
fn s7_a_newer_list_block_above_its_root_reads_as_a_newer_build() {
    let mut k = Kernel::new();
    let root = object(b"l");
    list_with_record(&mut k, &root);
    let block = record_of(&k, Sub::Block);
    store(&mut k, block, newer_record(), true);
    let read = items(&k.snapshot(), &root, Start::Position(0), usize::MAX).unwrap_err();
    assert!(
        matches!(
            read,
            ValueError::Corrupt(Corrupt::Block {
                fault: BlockFault::Envelope(EnvelopeError::UnknownCodec(0x02)),
                ..
            })
        ),
        "{read:?}"
    );
}

/// S7-1.9, the Q1 invariant (A-R144, critic C-2): a map holding a damaged entry `a` and a newer
/// build's entry `b`. The read names the first record it cannot serve (damage); the clear stays
/// at least as strict and refuses on `b`; the app's S6 repair of `a` works; then the read and a
/// `del` of `b` name the newer build, and `b` keeps its bytes throughout.
#[test]
fn s7_two_faults_in_one_map_never_lose_the_newer_entry() {
    let mut k = Kernel::new();
    let root = object(b"q");
    map_ab(&mut k, &root);
    let a = element_key(&root, &text("a")).unwrap();
    let b = element_key(&root, &text("b")).unwrap();
    store(&mut k, a, damaged_record(), true);
    store(&mut k, b.clone(), newer_record(), true);
    let read = members(&k.snapshot(), &root, None, usize::MAX).unwrap_err();
    assert!(
        matches!(read, ValueError::Corrupt(ref c) if !c.is_newer_build()),
        "the read names the damage first: {read:?}"
    );
    assert_eq!(clear_object(&k.snapshot(), &root).unwrap_err(), NEWER);
    // The app's S6 remedy: write another key so the root moves above both, then delete `a`.
    for ops in [
        vec![ElemOp::Put(text("c"), int(3))],
        vec![ElemOp::Remove(text("a"))],
    ] {
        let version = Expected::Version(root_version(&k, &root));
        let made = compile_collection(&k.snapshot(), &root, CollectionKind::Map, version, &ops)
            .unwrap_or_else(|e| panic!("{ops:?}: {e:?}"));
        k.commit(&made);
    }
    let s = k.snapshot();
    assert_eq!(members(&s, &root, None, usize::MAX).unwrap_err(), NEWER);
    let version = Expected::Version(root_version(&k, &root));
    let del_b = [ElemOp::Remove(text("b"))];
    assert_eq!(
        compile_collection(&s, &root, CollectionKind::Map, version, &del_b).unwrap_err(),
        NEWER
    );
    assert_eq!(clear_object(&s, &root).unwrap_err(), NEWER);
    assert_eq!(
        k.records[&b].1,
        newer_record(),
        "b keeps the newer build's bytes"
    );
}
