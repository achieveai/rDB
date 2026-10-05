//! Ordered lists (ADR-rdb-0016), through the public API and the shared kernel stand-in.

mod common;

use common::{text, Kernel};
use rdb_core::{AffinityId, TenantId};
use rdb_value::cbor::encode;
use rdb_value::collection::{compile_collection, CollectionKind, ElemOp};
use rdb_value::delta::ApplyError;
use rdb_value::envelope::{seal, Kind};
use rdb_value::keys::{root_key, RootKey};
use rdb_value::list::{compile_list, drop_list, items, ListOp, Start, DEFAULT_NODE_MAX};
use rdb_value::value::{Map, Value};
use rdb_value::{Expected, ValueError};

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
