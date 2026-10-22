//! Clearing a damaged object at the library API (M9 S6; ADR-rdb-0013 decision 11).
//!
//! Scenario: an admin clears an object the read refuses, and the clear never deletes a record a
//! newer build wrote, whatever else is wrong with the object.

mod common;

use bytes::Bytes;
use common::{text, Kernel};
use rdb_core::{AffinityId, TenantId};
use rdb_value::collection::{compile_collection, CollectionKind, ElemOp};
use rdb_value::envelope::{digest, seal, EnvelopeError, Kind};
use rdb_value::keys::{element_key, parse, root_key, RootKey, Sub};
use rdb_value::list::{create_list, ListOp, DEFAULT_BLOCK_MAX};
use rdb_value::repair::clear_object;
use rdb_value::{Corrupt, Expected, ValueError};

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
