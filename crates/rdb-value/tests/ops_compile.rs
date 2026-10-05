//! Path ops, `compile` and `read`, at the library API (ADR-rdb-0012 decisions 10–11; ADR-rdb-0011
//! laws L1, L3–L6; tester W1 contracts 1, 5–8, W2 contracts 6–10).
//!
//! Scenario: the primary's transaction step turns a caller's ops into one whole-document `Put`
//! with its precondition, and the document reads back as written.

mod common;

use bytes::Bytes;
use common::{arb_value, int, map, nested, text};
use proptest::prelude::*;
use rdb_core::{AffinityId, Condition, Generation, Mutation, TenantId};
use rdb_value::cbor::encode;
use rdb_value::delta::{materialize, ApplyError, Delta, Location, Op, SizeLimit};
use rdb_value::envelope::{open, seal, Kind, MAX_PAYLOAD};
use rdb_value::keys::{root_key, RootKey};
use rdb_value::path::Path;
use rdb_value::testing::MapSnapshot;
use rdb_value::value::{Int, Value, INT_MAX, INT_MIN};
use rdb_value::{compile, read, Compiled, Corrupt, Expected, ValueError};

/// The document `user:1`'s root key (ADR-rdb-0013 §1).
fn key() -> RootKey {
    root_key(TenantId(1), AffinityId(1), b"user:1")
}

fn p(text: &str) -> Path {
    Path::parse(text).expect("test path")
}

fn n(v: i128) -> Int {
    Int::new(v).expect("in range")
}

fn delta(ops: Vec<Op>) -> Delta {
    Delta(ops)
}

/// What the kernel does at apply, for one `Put`: check the condition and `expected_version`,
/// then store the after-image at `version`. Returns `false` when a check fails.
fn apply(snapshot: &mut MapSnapshot, compiled: &Compiled, version: u64) -> bool {
    use rdb_core::{Namespace, SnapshotRead};
    let Mutation::Put {
        key,
        value,
        expected_version,
    } = &compiled.mutations[0]
    else {
        panic!("compile returns a Put");
    };
    let held = compiled.conditions.iter().all(|condition| match condition {
        Condition::Absent { key } => snapshot.version(Namespace::User, key).is_none(),
        other => panic!("compile attaches only Absent, got {other:?}"),
    }) && expected_version
        .is_none_or(|want| snapshot.version(Namespace::User, key) == Some(want));
    if held {
        snapshot.insert(key.clone(), version, value.clone());
    }
    held
}

fn payload_of(compiled: &Compiled) -> Vec<u8> {
    let Mutation::Put { value, .. } = &compiled.mutations[0] else {
        panic!("a Put");
    };
    open(value)
        .expect("compile seals a sound envelope")
        .payload
        .to_vec()
}

fn snapshot_with(doc: &Value) -> MapSnapshot {
    let mut s = MapSnapshot::new(Generation(1));
    let c = compile(
        &s,
        &key(),
        Expected::Absent,
        &delta(vec![Op::Replace(doc.clone())]),
    )
    .expect("create");
    assert!(apply(&mut s, &c, 1));
    s
}

/// Scenario §2 steps 1, 2 and 5, byte for byte: create, increment, set a new key. The digests
/// are SHA-256 over header bytes 0..8 then the payload (ruling L-R186s), computed with Node
/// `crypto` from the pinned header and payload (tester W1 contract 1).
#[test]
fn the_counter_scenario_produces_the_designed_bytes() {
    let mut s = MapSnapshot::new(Generation(1));
    let doc = map(&[("name", text("ada")), ("visits", int(0))]);
    let step1 = compile(&s, &key(), Expected::Absent, &delta(vec![Op::Replace(doc)])).unwrap();
    let Mutation::Put {
        value,
        expected_version,
        ..
    } = &step1.mutations[0]
    else {
        panic!()
    };
    assert_eq!(value.len(), 58);
    assert_eq!(hex::encode(&value[..8]), "0101010100000012");
    assert_eq!(
        hex::encode(&value[8..40]),
        "01844f7eef38c8601d6153621026f35437ebf1765e8790c875e676c26f7bbb76"
    );
    assert_eq!(
        hex::encode(payload_of(&step1)),
        "a2646e616d65636164616676697369747300"
    );
    assert_eq!(*expected_version, None);
    assert_eq!(
        step1.conditions,
        vec![Condition::Absent {
            key: key().to_bytes()
        }]
    );
    assert!(apply(&mut s, &step1, 1));

    let step2 = compile(
        &s,
        &key(),
        Expected::Version(1),
        &delta(vec![Op::Increment(p("/visits"), n(1))]),
    )
    .unwrap();
    let Mutation::Put {
        value,
        expected_version,
        ..
    } = &step2.mutations[0]
    else {
        panic!()
    };
    assert_eq!(*expected_version, Some(1));
    assert!(step2.conditions.is_empty());
    assert_eq!(
        hex::encode(&value[8..40]),
        "8f318d2a0176969d2ea626a083338a6288f90c514cb8d3ea9c651edb22668426"
    );
    assert!(apply(&mut s, &step2, 2));

    // Step 3: the same op at the old version is refused before any work.
    assert_eq!(
        compile(
            &s,
            &key(),
            Expected::Version(1),
            &delta(vec![Op::Increment(p("/visits"), n(1))])
        ),
        Err(ValueError::Apply(ApplyError::VersionConflict {
            expected: 1,
            found: 2
        }))
    );

    // Step 5: a new key lands between `name` and `visits` (length first).
    let step5 = compile(
        &s,
        &key(),
        Expected::Version(2),
        &delta(vec![Op::Set(p("/email"), text("ada@x.io"))]),
    )
    .unwrap();
    assert_eq!(
        hex::encode(payload_of(&step5)),
        "a3646e616d656361646165656d61696c6861646140782e696f6676697369747301"
    );
    let Mutation::Put { value, .. } = &step5.mutations[0] else {
        panic!()
    };
    assert_eq!(
        hex::encode(&value[8..40]),
        "e3581d1437f73bda739264803f3a81bf905b919193a3f3025908d716c9a4021e"
    );
}

/// Scenario §2 step 4: two callers create the same key from one "absent" snapshot. Both
/// compile; the condition travels with each, so the second fails at apply and the first is kept.
#[test]
fn racing_creates_keep_their_absent_condition_and_the_second_fails() {
    let mut s = MapSnapshot::new(Generation(1));
    let a = compile(
        &s,
        &key(),
        Expected::Absent,
        &delta(vec![Op::Replace(int(1))]),
    )
    .unwrap();
    let b = compile(
        &s,
        &key(),
        Expected::Absent,
        &delta(vec![Op::Replace(int(2))]),
    )
    .unwrap();
    assert!(apply(&mut s, &a, 1));
    assert!(
        !apply(&mut s, &b, 2),
        "second create must fail its condition"
    );
    assert_eq!(
        read(&s, &key()).unwrap().map(|d| (d.version, d.value)),
        Some((1, int(1)))
    );
    // A stale `expected_version` is refused at apply too.
    let first = compile(
        &s,
        &key(),
        Expected::Version(1),
        &delta(vec![Op::Replace(int(3))]),
    )
    .unwrap();
    let stale = compile(
        &s,
        &key(),
        Expected::Version(1),
        &delta(vec![Op::Replace(int(4))]),
    )
    .unwrap();
    assert!(apply(&mut s, &first, 5));
    assert!(
        !apply(&mut s, &stale, 6),
        "expected_version 1 no longer holds"
    );
    assert_eq!(
        read(&s, &key()).unwrap().map(|d| (d.version, d.value)),
        Some((5, int(3)))
    );
}

/// Scenario §2 step 7: one stored byte is flipped. `read` and `compile` both report `Corrupt`,
/// and nothing is repaired (decision 12). A record that does not even open is `Corrupt` too.
#[test]
fn a_damaged_record_is_corrupt_on_read_and_on_compile() {
    let mut s = snapshot_with(&map(&[("visits", int(1))]));
    let (stored, version, value) = s
        .records()
        .next()
        .map(|(k, v, b)| (k.clone(), v, b.clone()))
        .unwrap();
    let mut flipped = value.to_vec();
    *flipped.last_mut().unwrap() ^= 0x01;
    s.insert(stored, version, Bytes::from(flipped));
    let corrupt = ValueError::Corrupt(Corrupt::Envelope(
        rdb_value::envelope::EnvelopeError::DigestMismatch,
    ));
    assert_eq!(read(&s, &key()), Err(corrupt.clone()));
    let op = delta(vec![Op::Increment(p("/visits"), n(1))]);
    assert_eq!(
        compile(&s, &key(), Expected::Version(version), &op),
        Err(corrupt.clone())
    );
    // Even a whole replace at that version: the base must read back first (A2, by design).
    assert_eq!(
        compile(
            &s,
            &key(),
            Expected::Version(version),
            &delta(vec![Op::Replace(int(0))])
        ),
        Err(corrupt)
    );
    // A sound envelope around non-canonical bytes is `Corrupt(Codec)`, not a client error.
    let mut s = MapSnapshot::new(Generation(1));
    let sealed =
        rdb_value::envelope::seal(rdb_value::envelope::Kind::Document, &[0x18, 0x01]).unwrap();
    s.insert(key().to_bytes(), 1, sealed);
    assert!(matches!(
        read(&s, &key()),
        Err(ValueError::Corrupt(Corrupt::Codec(
            rdb_value::cbor::CodecError::NonCanonical { .. }
        )))
    ));
    // A header with an empty payload opens, and the empty payload does not decode.
    let mut s = MapSnapshot::new(Generation(1));
    let empty = rdb_value::envelope::seal(rdb_value::envelope::Kind::Document, &[]).unwrap();
    s.insert(key().to_bytes(), 1, empty);
    assert!(matches!(
        read(&s, &key()),
        Err(ValueError::Corrupt(Corrupt::Codec(_)))
    ));
}

/// Scenario: a record written by a newer build, standing in as codec `0x02` with a sound digest.
/// Today it comes back exactly like damage: `Corrupt(Envelope(UnknownCodec(0x02)))`, reason
/// "unknown codec version 0x02", and nothing is changed. The newer-build vs damage split is
/// **docs only** (ADR-rdb-0012 §7 and §12) until the M9 predicate lands; this pins what a caller
/// can see before then, so the predicate's arrival shows up as a change here.
#[test]
fn a_newer_build_record_reads_as_corrupt_until_the_m9_predicate() {
    use rdb_value::envelope::{digest, seal, EnvelopeError, Kind, HEADER_LEN};
    let payload = encode(&map(&[("visits", int(1))])).unwrap();
    let mut header_and_payload = seal(Kind::Document, &payload).unwrap().to_vec();
    header_and_payload[2] = 0x02;
    // Re-sealed, so the digest is sound: since ruling L-R186s it covers the codec byte.
    let head: [u8; 8] = header_and_payload[..8].try_into().unwrap();
    header_and_payload[8..HEADER_LEN].copy_from_slice(&digest(&head, &payload));
    let mut s = MapSnapshot::new(Generation(1));
    s.insert(key().to_bytes(), 1, Bytes::from(header_and_payload));
    let before = s.clone();

    let newer = ValueError::Corrupt(Corrupt::Envelope(EnvelopeError::UnknownCodec(0x02)));
    assert_eq!(read(&s, &key()), Err(newer.clone()));
    assert_eq!(
        newer.to_string(),
        "corrupt record: unknown codec version 0x02"
    );
    let update = delta(vec![Op::Increment(p("/visits"), n(1))]);
    assert_eq!(
        compile(&s, &key(), Expected::Version(1), &update),
        Err(newer.clone())
    );
    assert_eq!(
        compile(
            &s,
            &key(),
            Expected::Version(1),
            &delta(vec![Op::Replace(int(0))])
        ),
        Err(newer)
    );
    // A create does not read the record; its `Absent` condition fails at apply instead.
    let create = compile(
        &s,
        &key(),
        Expected::Absent,
        &delta(vec![Op::Replace(int(0))]),
    )
    .expect("a create compiles without reading");
    assert!(!apply(&mut s, &create, 2));
    assert_eq!(s, before, "nothing is changed");
}

/// Scenario: a caller edits a document with each op, on maps and arrays (tester W2 contract 6).
#[test]
fn ops_edit_maps_and_arrays() {
    let doc = map(&[
        ("t", Value::Array(vec![text("a"), text("b"), text("c")])),
        ("m", map(&[("k", Value::Array(vec![int(1)]))])),
    ]);
    let run = |ops: Vec<Op>| materialize(Some(doc.clone()), &delta(ops));
    let arr = |items: &[&str]| Value::Array(items.iter().map(|s| text(s)).collect());
    let with_t = |t: Value| {
        let mut d = doc.clone();
        if let Value::Map(m) = &mut d {
            m.insert(rdb_value::value::MapKey::new("t"), t);
        }
        d
    };
    // set first and last; remove first and last; past the end is refused.
    assert_eq!(
        run(vec![Op::Set(p("/t/0"), text("A"))]),
        Ok(with_t(arr(&["A", "b", "c"])))
    );
    assert_eq!(
        run(vec![Op::Set(p("/t/2"), text("C"))]),
        Ok(with_t(arr(&["a", "b", "C"])))
    );
    assert_eq!(
        run(vec![Op::Remove(p("/t/0"))]),
        Ok(with_t(arr(&["b", "c"])))
    );
    assert_eq!(
        run(vec![Op::Remove(p("/t/2"))]),
        Ok(with_t(arr(&["a", "b"])))
    );
    for op in [Op::Set(p("/t/3"), int(0)), Op::Remove(p("/t/3"))] {
        assert_eq!(
            run(vec![op]),
            Err(ApplyError::IndexInvalid {
                segment: "3".to_owned(),
                len: 3
            })
        );
    }
    // Two removes of the last index over-run on the second; the whole delta is refused.
    assert_eq!(
        run(vec![Op::Remove(p("/t/2")), Op::Remove(p("/t/2"))]),
        Err(ApplyError::IndexInvalid {
            segment: "2".to_owned(),
            len: 2
        })
    );
    // Increment through an array; maps insert and remove keys.
    let incremented = run(vec![Op::Increment(p("/m/k/0"), n(4))]).unwrap();
    assert_eq!(
        rdb_value::delta::resolve(&incremented, &p("/m/k/0")),
        Ok(&int(5))
    );
    let added = run(vec![Op::Set(p("/m/new"), int(1)), Op::Remove(p("/m/k"))]).unwrap();
    assert_eq!(
        rdb_value::delta::resolve(&added, &p("/m")),
        Ok(&map(&[("new", int(1))]))
    );
}

/// Scenario: a caller's op does not fit the document. Each refusal is named, and a delta with
/// one bad op changes nothing (tester W1 contract 5, W2 contract 6).
#[test]
fn op_refusals_are_named_and_all_or_nothing() {
    let doc = map(&[
        ("name", text("ada")),
        ("n", map(&[("k", int(1))])),
        ("f", common::float(1.5)),
        ("b", Value::Bool(true)),
        ("max", int(INT_MAX)),
        ("min", int(INT_MIN)),
    ]);
    let run = |ops: Vec<Op>| materialize(Some(doc.clone()), &delta(ops));
    assert_eq!(
        run(vec![Op::Remove(p("/nope"))]),
        Err(ApplyError::PathNotFound {
            segment: "nope".to_owned()
        })
    );
    assert_eq!(
        run(vec![Op::Set(p("/nope/x"), int(1))]),
        Err(ApplyError::PathNotFound {
            segment: "nope".to_owned()
        })
    );
    assert_eq!(
        run(vec![Op::Set(p("/name/x"), int(1))]),
        Err(ApplyError::NotAContainer {
            at: Location::Segment("name".to_owned())
        })
    );
    assert_eq!(
        run(vec![Op::Remove(p("/name/x"))]),
        Err(ApplyError::NotAContainer {
            at: Location::Segment("name".to_owned())
        })
    );
    for target in ["/name", "/n", "/f", "/b"] {
        assert_eq!(
            run(vec![Op::Increment(p(target), n(1))]),
            Err(ApplyError::TypeMismatch),
            "{target}"
        );
    }
    assert_eq!(
        run(vec![Op::Increment(p("/max"), n(1))]),
        Err(ApplyError::Overflow)
    );
    assert_eq!(
        run(vec![Op::Increment(p("/min"), n(-1))]),
        Err(ApplyError::Overflow)
    );
    assert_eq!(
        run(vec![
            Op::Increment(p("/max"), n(-1)),
            Op::Increment(p("/max"), n(1))
        ]),
        Ok(doc.clone())
    );
    // A root that is not a container is named as the root, not as the empty key.
    assert_eq!(
        materialize(Some(int(5)), &delta(vec![Op::Set(p("/a"), int(1))])),
        Err(ApplyError::NotAContainer { at: Location::Root })
    );
    // Ops on an absent document, and an empty delta on one.
    assert_eq!(
        materialize(None, &delta(vec![Op::Set(p("/a"), int(1))])),
        Err(ApplyError::ObjectAbsent)
    );
    assert_eq!(
        materialize(None, &delta(vec![])),
        Err(ApplyError::ObjectAbsent)
    );
    // All or nothing through compile: the first op is not written when the second fails.
    let s = snapshot_with(&doc);
    let mixed = delta(vec![
        Op::Set(p("/m"), int(1)),
        Op::Increment(p("/name"), n(1)),
    ]);
    assert_eq!(
        compile(&s, &key(), Expected::Version(1), &mixed),
        Err(ValueError::Apply(ApplyError::TypeMismatch))
    );
    assert_eq!(
        compile(
            &MapSnapshot::new(Generation(1)),
            &key(),
            Expected::Version(1),
            &mixed
        ),
        Err(ValueError::Apply(ApplyError::ObjectAbsent))
    );
}

/// Scenario: a caller grows a document past the depth or size limit with an op. `compile`
/// refuses it, so nothing is written that could not be read back (decision 11; tester W1
/// contract 6, W2 contract 7). A result of exactly 64 levels is accepted.
#[test]
fn compile_refuses_results_that_are_too_deep_or_too_large() {
    let s = snapshot_with(&nested(60, Value::Null));
    let path = |depth: usize| p(&"/0".repeat(depth));
    // Leaf at depth 60 replaced by 5 more arrays: 65 levels.
    let too_deep = delta(vec![Op::Set(path(60), nested(5, Value::Null))]);
    assert_eq!(
        compile(&s, &key(), Expected::Version(1), &too_deep),
        Err(ValueError::Apply(ApplyError::TooDeep))
    );
    let deepest = delta(vec![Op::Set(path(60), nested(4, Value::Null))]);
    let ok = compile(&s, &key(), Expected::Version(1), &deepest).expect("64 levels");
    assert_eq!(
        rdb_value::cbor::decode(&payload_of(&ok)).map(|_| ()),
        Ok(())
    );

    // A byte string at exactly the payload limit, then one more key.
    let big = Value::Bytes(vec![7; MAX_PAYLOAD - 5]);
    // L-R186v: its create is refused, because the record the kernel would ship (key and
    // framing added) is over the kernel's cap. A record stored at the limit still reads back,
    // so the store is seeded with it directly.
    assert_eq!(
        compile(
            &MapSnapshot::new(Generation(1)),
            &key(),
            Expected::Absent,
            &delta(vec![Op::Replace(big.clone())])
        ),
        Err(ValueError::Apply(ApplyError::TooLarge {
            limit: SizeLimit::Write
        }))
    );
    let mut s = MapSnapshot::new(Generation(1));
    let sealed = seal(Kind::Document, &encode(&big).unwrap()).unwrap();
    s.insert(key().to_bytes(), 1, sealed);
    assert_eq!(read(&s, &key()).unwrap().unwrap().value, big);
    let grow = delta(vec![Op::Replace(map(&[("b", big.clone())]))]);
    assert_eq!(
        compile(&s, &key(), Expected::Version(1), &grow),
        Err(ValueError::Apply(ApplyError::TooLarge {
            limit: SizeLimit::Value
        }))
    );
}

/// ADR-rdb-0011 L5 by example: at `MAX−1`, `+2` then `−2` overflows on the first step, whether
/// the two ops are applied one delta at a time or as one concatenated delta. L4 by example:
/// set-then-remove and remove-then-set give different documents.
#[test]
fn strict_folding_and_order_matter() {
    let doc = map(&[("c", int(INT_MAX - 1))]);
    let plus = delta(vec![Op::Increment(p("/c"), n(2))]);
    let minus = delta(vec![Op::Increment(p("/c"), n(-2))]);
    let both = delta([plus.0.clone(), minus.0.clone()].concat());
    assert_eq!(
        materialize(Some(doc.clone()), &plus),
        Err(ApplyError::Overflow)
    );
    assert_eq!(
        materialize(Some(doc.clone()), &both),
        Err(ApplyError::Overflow)
    );
    let set = delta(vec![Op::Set(p("/x"), int(1))]);
    let remove = delta(vec![Op::Remove(p("/x"))]);
    let seq = |a: &Delta, b: &Delta| {
        materialize(
            Some(doc.clone()),
            &delta([a.0.clone(), b.0.clone()].concat()),
        )
    };
    assert_eq!(seq(&set, &remove), Ok(doc.clone()));
    assert_ne!(seq(&remove, &set), seq(&set, &remove));
}

/// Random ops over a small document: sets of random values, removes and increments on a few
/// paths, so that some ops apply and some fail.
fn arb_op() -> impl Strategy<Value = Op> {
    let paths = prop::sample::select(vec!["/a", "/b", "/a/0", "/a/1", "/m", "/m/k", "/c"]);
    prop_oneof![
        arb_value().prop_map(Op::Replace),
        (paths.clone(), arb_value()).prop_map(|(t, v)| Op::Set(p(t), v)),
        paths.clone().prop_map(|t| Op::Remove(p(t))),
        (paths, -3_i128..3).prop_map(|(t, by)| Op::Increment(p(t), n(by))),
    ]
}

fn arb_base() -> impl Strategy<Value = Option<Value>> {
    prop_oneof![
        Just(None),
        arb_value().prop_map(Some),
        Just(Some(map(&[
            ("a", Value::Array(vec![int(1), int(2)])),
            ("c", int(0)),
            ("m", map(&[("k", int(INT_MAX))])),
        ]))),
    ]
}

proptest! {
    /// ADR-rdb-0011 L1 (with ADR-rdb-0012's `partial_merge` = concatenation): applying `a` then
    /// `b` equals applying `a ++ b`, errors included, absent base included. L3: a replace
    /// discards everything before it.
    #[test]
    fn fold_law_and_replace_law(
        base in arb_base(),
        a in proptest::collection::vec(arb_op(), 0..4),
        b in proptest::collection::vec(arb_op(), 0..4),
        w in arb_value(),
    ) {
        let (a, b) = (delta(a), delta(b));
        let stepwise = materialize(base.clone(), &a).and_then(|v| materialize(Some(v), &b));
        let together = materialize(base.clone(), &delta([a.0.clone(), b.0.clone()].concat()));
        // `materialize(None, [])` is `ObjectAbsent`, but stepping through an empty `a` on an
        // absent base must carry the absence to `b`, not stop: compare only where `a` is
        // non-empty or the base exists.
        if base.is_some() || !a.0.is_empty() {
            prop_assert_eq!(stepwise, together);
        }
        let replaced = delta([a.0, vec![Op::Replace(w.clone())]].concat());
        if let Ok(v) = materialize(base, &replaced) {
            prop_assert_eq!(v, w);
        }
    }

    /// Scenario: every accepted write reads back. A random document stored at version 1, then
    /// 1-3 random ops at version 1, on 7 fixed paths (`arb_op`): when `compile` accepts, the
    /// stored payload is `encode` of the value `read` returns, and that value is what
    /// `materialize` computed (tester W1 contract 7, W2 9). Only version 1 and these paths are
    /// drawn; other versions are covered by the stale-version and S1 differential tests.
    #[test]
    fn every_accepted_write_reads_back(doc in arb_value(), ops in proptest::collection::vec(arb_op(), 1..4)) {
        let s = snapshot_with(&doc);
        let read_doc = read(&s, &key()).unwrap().unwrap();
        prop_assert_eq!(read_doc.version, 1);
        prop_assert_eq!(&read_doc.value, &doc);
        let ops = delta(ops);
        if let Ok(c) = compile(&s, &key(), Expected::Version(1), &ops) {
            let mut s = s.clone();
            prop_assert!(apply(&mut s, &c, 2));
            let after = read(&s, &key()).unwrap().unwrap();
            prop_assert_eq!(Ok(after.value.clone()), materialize(Some(doc), &ops));
            prop_assert_eq!(payload_of(&c), encode(&after.value).unwrap());
        }
    }

    /// Scenario §2 step 6: the same op on two snapshot instances holding the same document
    /// compiles to the same `Compiled`, byte for byte (tester W1 contract 8, W2 contract 10).
    /// Both are `MapSnapshot`s built the same way, so this checks that `compile` is
    /// deterministic, not that two kinds of store agree. That is the S1 value differential in
    /// `rdb-storage` (RocksDB against the oracle).
    #[test]
    fn the_same_op_on_two_snapshots_compiles_identically(doc in arb_value(), op in arb_op()) {
        let one = snapshot_with(&doc);
        let two = snapshot_with(&doc);
        let ops = delta(vec![op]);
        prop_assert_eq!(
            compile(&one, &key(), Expected::Version(1), &ops),
            compile(&two, &key(), Expected::Version(1), &ops)
        );
    }
}
