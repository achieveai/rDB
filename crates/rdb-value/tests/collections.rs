//! Maps and sets at the library API (ADR-rdb-0013 §7–§13; s3-design §5).
//!
//! Scenario: the primary's transaction step compiles map and set ops against a snapshot, and
//! a damaged store is refused by name, never by a panic.

use bytes::Bytes;
use rdb_core::{AffinityId, Generation, Mutation, TenantId};
use rdb_value::cbor::encode;
use rdb_value::collection::{collection, compile_collection, CollectionKind, ElemOp};
use rdb_value::envelope::{seal, Kind};
use rdb_value::keys::{element_key, root_key, RootKey};
use rdb_value::testing::MapSnapshot;
use rdb_value::value::{Int, Map, MapKey, Value};
use rdb_value::{Corrupt, Expected, ValueError};

fn cart() -> RootKey {
    root_key(TenantId(1), AffinityId(1), b"cart")
}

fn text(t: &str) -> Value {
    Value::Text(t.to_owned())
}

fn int(i: u64) -> Value {
    Value::Integer(Int::from(i))
}

/// A map root whose payload claims `count`, sealed soundly, so it opens and decodes.
fn map_root(count: u64) -> Bytes {
    let mut fields = Map::new();
    fields.insert(MapKey::new("keys"), int(1));
    fields.insert(MapKey::new("count"), int(count));
    seal(Kind::Map, &encode(&Value::Map(fields)).unwrap()).unwrap()
}

/// D1 (tester-m8-s3, basis a26d9d9): a stored root whose count is `u64::MAX` panicked on
/// `put` of a new key ("attempt to add with overflow"), and a release build would wrap the
/// count to 0 and write it. It is refused as `Corrupt(Root(..))` instead, and a `put` that
/// leaves the count unchanged still compiles.
#[test]
fn d1_a_root_count_at_u64_max_is_refused_by_name_never_wrapped() {
    let root = cart();
    let mut s = MapSnapshot::new(Generation(1));
    s.insert(root.to_bytes(), 4, map_root(u64::MAX));
    let banana = element_key(&root, &text("banana")).unwrap();
    s.insert(
        banana,
        4,
        seal(Kind::Document, &encode(&int(5)).unwrap()).unwrap(),
    );
    assert_eq!(collection(&s, &root).unwrap().unwrap().count, u64::MAX);

    let grow = [ElemOp::Put(text("z"), int(1))];
    let got = compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &grow);
    assert!(
        matches!(got, Err(ValueError::Corrupt(Corrupt::Root(_)))),
        "{got:?}"
    );

    // Same count afterwards: one in, one out, or a replace.
    for ops in [
        vec![ElemOp::Put(text("banana"), int(6))],
        vec![
            ElemOp::Put(text("z"), int(1)),
            ElemOp::Remove(text("banana")),
        ],
    ] {
        let compiled =
            compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &ops)
                .unwrap_or_else(|e| panic!("{ops:?}: {e:?}"));
        let Mutation::Put { value, .. } = &compiled.mutations[0] else {
            panic!("the root Put comes first");
        };
        assert_eq!(value, &map_root(u64::MAX), "{ops:?}");
    }
}
