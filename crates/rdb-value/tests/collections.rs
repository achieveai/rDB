//! Maps and sets at the library API (ADR-rdb-0013 §7–§13, Verification).
//!
//! Scenario: the primary's transaction step compiles map and set ops against a snapshot, and
//! a damaged store is refused by name, never by a panic.

mod common;

use std::cmp::Ordering;
use std::collections::BTreeMap;

use bytes::Bytes;
use common as c;
use proptest::prelude::*;
use rdb_core::contracts::envelope::{EnvelopeHeader, ReplicationEnvelope};
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::dedup::{dedup_key, dedup_value};
use rdb_core::{
    AffinityId, ClientId, Condition, ConditionOutcome, ConfigVersion, Digest, Generation, LeaseId,
    Mutation, Namespace, Outcome, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Seq,
    SnapshotRead, TenantId, Write,
};
use rdb_value::cbor::{decode, encode};
use rdb_value::collection::{
    collection, compile_collection, drop_collection, member, members, Collection, CollectionKind,
    ElemOp, Member,
};
use rdb_value::envelope::{seal, EnvelopeError, Kind};
use rdb_value::keys::{
    chunk_key, decode_element, decode_element_key, element_key, encode_element, esc, parse,
    root_key, KeyError, Parsed, RootKey, Sub,
};
use rdb_value::testing::MapSnapshot;
use rdb_value::value::{Decimal, Int, Map, MapKey, Value};

use rdb_value::delta::{ApplyError, Delta, Op, SizeLimit};
use rdb_value::{compile, read, Compiled, Corrupt, Expected, ValueError};

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
    root_record(Kind::Map, count)
}

/// A root of `kind` whose payload claims `count`, sealed soundly.
fn root_record(kind: Kind, count: u64) -> Bytes {
    let mut fields = Map::new();
    fields.insert(MapKey::new("keys"), int(1));
    fields.insert(MapKey::new("count"), int(count));
    seal(kind, &encode(&Value::Map(fields)).unwrap()).unwrap()
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

/// The record the kernel ships for these writes, built as `TxnKernel::envelope` builds it (the
/// writes, then step 13's one `Dedup` write) and measured by the contract's own `encode`. Every
/// field outside the request is fixed-width, so the ids chosen here do not move the length.
fn kernel_record_len(conditions: usize, mutations: &[Mutation]) -> usize {
    let identity = RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(1),
    };
    let mut writes: Vec<Write> = mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, value, .. } => Write {
                ns: Namespace::User,
                key: key.clone(),
                value: Some(value.clone()),
            },
            Mutation::Delete { key, .. } => Write {
                ns: Namespace::User,
                key: key.clone(),
                value: None,
            },
        })
        .collect();
    writes.push(Write {
        ns: Namespace::Dedup,
        key: dedup_key(Generation(1), AffinityId(1), identity),
        value: Some(dedup_value(Digest::ROOT, Seq(1), OwnerEpoch(1))),
    });
    ReplicationEnvelope {
        header: EnvelopeHeader {
            protocol_version: ENVELOPE_VERSION,
            partition: PartitionId(1),
            generation: Generation(1),
            config_version: ConfigVersion(1),
            owner_epoch: OwnerEpoch(1),
            seq: Seq(1),
            body_len: 0,
        },
        lease_id: LeaseId(1),
        prev_digest: Digest::ROOT,
        request_identity: identity,
        request_digest: Digest::ROOT,
        conditions_result: vec![ConditionOutcome::Met; conditions],
        mutations: writes,
        result: Outcome::Published,
        record_digest: Digest::ROOT,
    }
    .encode()
    .expect("fits the wire's u32 lengths")
    .len()
}

/// L-R186v (architect-m8-s5): compile measured `TooLarge` as key plus value bytes, while the
/// kernel's admission check 10 measures the whole record against `MAX_ENVELOPE_BYTES`. A map
/// create of one large entry is walked across the kernel's cap: every compile that succeeds
/// must fit the record, and one byte past the cap must be `TooLarge`.
#[test]
fn l_r186v_a_compile_that_succeeds_fits_the_kernels_record_cap() {
    let root = cart();
    let s = MapSnapshot::new(Generation(1));
    let create = |n: usize| {
        let ops = [ElemOp::Put(text("x"), Value::Bytes(vec![0xAB; n]))];
        compile_collection(&s, &root, CollectionKind::Map, Expected::Absent, &ops)
    };
    let record = |n: usize| {
        let compiled = create(n).unwrap_or_else(|e| panic!("{n}: {e:?}"));
        kernel_record_len(compiled.conditions.len(), &compiled.mutations)
    };
    // One more value byte is one more record byte here (CBOR's 4-byte length covers both).
    let probe = 100_000;
    assert_eq!(record(probe + 1), record(probe) + 1);
    let at_cap = probe + (MAX_ENVELOPE_BYTES - record(probe));

    assert_eq!(
        record(at_cap),
        MAX_ENVELOPE_BYTES,
        "the largest create the kernel admits"
    );
    let over = create(at_cap + 1);
    assert!(
        matches!(
            over,
            Err(ValueError::Apply(ApplyError::TooLarge {
                limit: SizeLimit::Write
            }))
        ),
        "one byte past the kernel's cap: {:?}",
        over.as_ref()
            .map(|c| kernel_record_len(c.conditions.len(), &c.mutations))
    );
}

/// L-R186v, the document half. It sits here beside [`kernel_record_len`] rather than in
/// `ops_compile.rs` so the oracle is written once. A document whose own envelope is within
/// 1 MiB can still make a record over the kernel's cap, once the key and the record's framing
/// are added.
#[test]
fn l_r186v_a_document_compile_that_succeeds_fits_the_kernels_record_cap() {
    let root = cart();
    let s = MapSnapshot::new(Generation(1));
    let create = |n: usize| {
        let delta = Delta(vec![Op::Replace(Value::Bytes(vec![0xAB; n]))]);
        compile(&s, &root, Expected::Absent, &delta)
    };
    let record = |n: usize| {
        let compiled = create(n).unwrap_or_else(|e| panic!("{n}: {e:?}"));
        kernel_record_len(compiled.conditions.len(), &compiled.mutations)
    };
    let probe = 100_000;
    assert_eq!(record(probe + 1), record(probe) + 1);
    let at_cap = probe + (MAX_ENVELOPE_BYTES - record(probe));

    assert_eq!(
        record(at_cap),
        MAX_ENVELOPE_BYTES,
        "the largest create the kernel admits"
    );
    let over = create(at_cap + 1);
    assert!(
        matches!(
            over,
            Err(ValueError::Apply(ApplyError::TooLarge {
                limit: SizeLimit::Write
            }))
        ),
        "one byte past the kernel's cap: {:?}",
        over.as_ref()
            .map(|c| kernel_record_len(c.conditions.len(), &c.mutations))
    );
}

/// Ruling L-R186s, the tester's W1 A3 case (`p5-rootdoc.store`): a map root whose `kind` byte
/// is flipped to `0x01` read as the document `{"keys":1,"count":1}`, and one flipped to `0x03`
/// read as a set. The digest now covers the header, so every read path names the damage.
#[test]
fn l_r186s_a_flipped_root_kind_is_damage_on_every_read_path() {
    let root = cart();
    let damage = Err(ValueError::Corrupt(Corrupt::Envelope(
        EnvelopeError::DigestMismatch,
    )));
    for to in [0x01, 0x03] {
        let mut flipped = map_root(1).to_vec();
        flipped[1] = to;
        let mut s = MapSnapshot::new(Generation(1));
        s.insert(root.to_bytes(), 4, Bytes::from(flipped));
        assert_eq!(read(&s, &root).map(|_| ()), damage, "read, {to:#04x}");
        assert_eq!(
            collection(&s, &root).map(|_| ()),
            damage,
            "collection, {to:#04x}"
        );
        assert_eq!(
            member(&s, &root, &text("banana")).map(|_| ()),
            damage,
            "member, {to:#04x}"
        );
        assert_eq!(
            members(&s, &root, None, 10).map(|_| ()),
            damage,
            "members, {to:#04x}"
        );
        assert_eq!(
            drop_collection(&s, &root, 4).map(|_| ()),
            damage,
            "drop, {to:#04x}"
        );
        let put = [ElemOp::Put(text("banana"), int(1))];
        assert_eq!(
            compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &put)
                .map(|_| ()),
            damage,
            "compile, {to:#04x}"
        );
    }
}

/// Lead ruling on tester W1 A2: `ElementNewerThanRoot` was erased by the next write. Step 10(a)
/// leaves banana at version 6 under a root at 4. Every op that touches banana (put, remove,
/// need) is refused by name; compile already reads the element's version, so this costs no
/// read. A write that touches only other elements still compiles (ADR-0013 §10's masking).
#[test]
fn a2_a_write_that_touches_an_element_newer_than_its_root_is_refused() {
    let root = cart();
    let mut s = MapSnapshot::new(Generation(1));
    s.insert(root.to_bytes(), 4, map_root(1));
    let banana = element_key(&root, &text("banana")).unwrap();
    s.insert(
        banana,
        6,
        seal(Kind::Document, &encode(&int(5)).unwrap()).unwrap(),
    );
    let newer = Err(ValueError::Corrupt(Corrupt::ElementNewerThanRoot {
        element: 6,
        root: 4,
    }));
    for ops in [
        vec![ElemOp::Put(text("banana"), int(1))],
        vec![ElemOp::Remove(text("banana"))],
        vec![ElemOp::NeedPresent(text("banana"))],
        vec![
            ElemOp::Put(text("kiwi"), int(1)),
            ElemOp::Remove(text("banana")),
        ],
    ] {
        let got = compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &ops);
        assert_eq!(got.map(|_| ()), newer, "{ops:?}");
    }
    let kiwi = [ElemOp::Put(text("kiwi"), int(1))];
    assert!(
        compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &kiwi).is_ok(),
        "an untouched element is not read"
    );
}

/// Tester W2 PC4: `set snew --absent put 1 1` said "the object is a Set" for an object that
/// does not exist; it is the command that is a set. `KindMismatch` keeps its name and field
/// (design C13, C24) and says what is true either way.
#[test]
fn pc4_kind_mismatch_on_an_absent_object_does_not_claim_it_exists() {
    let s = MapSnapshot::new(Generation(1));
    let root = root_key(TenantId(1), AffinityId(1), b"snew");
    let put = [ElemOp::Put(int(1), int(1))];
    let err = compile_collection(&s, &root, CollectionKind::Set, Expected::Absent, &put)
        .expect_err("a set takes no put");
    assert_eq!(
        err,
        ValueError::Apply(ApplyError::KindMismatch { found: Kind::Set })
    );
    assert_eq!(err.to_string(), "a Set does not take this op");
}

/// Tester W2 PC5: a collection write over the record cap said "over the 1 MiB envelope
/// (1,048,536-byte payload) limit", but no envelope was over it: the whole write was. Since
/// L-R186z there are two caps, and the detail names the one that was hit.
#[test]
fn pc5_too_large_names_the_cap_it_hit() {
    let s = MapSnapshot::new(Generation(1));
    let root = cart();
    let half = || Value::Bytes(vec![0; 600 * 1024]);
    let write = [
        ElemOp::Put(text("k1"), half()),
        ElemOp::Put(text("k2"), half()),
    ];
    let err = compile_collection(&s, &root, CollectionKind::Map, Expected::Absent, &write)
        .expect_err("over the write cap");
    let detail = err.to_string();
    assert!(detail.contains("whole write"), "{detail}");
    assert!(!detail.contains("envelope"), "{detail}");

    let one = [ElemOp::Put(text("k"), Value::Bytes(vec![0; 1 << 20]))];
    let err = compile_collection(&s, &root, CollectionKind::Map, Expected::Absent, &one)
        .expect_err("over the value cap");
    assert!(err.to_string().contains("one value"), "{err}");
}

// ================================================================================================
// Rows R1–R13 at the library API (ADR-rdb-0013 Verification). Each test names the walked
// scenario it protects; the tester's mapping is in working notes, not in the repository.
// ================================================================================================

/// Bytes from hex written with spaces, as ADR-rdb-0013's tables write them.
fn spaced(hex: &str) -> Vec<u8> {
    c::h(&hex.replace(' ', ""))
}

/// The tenant 1, affinity 1 scope every key here starts with.
const SCOPE: &str = "00000001 0000000000000001";

/// A `Corrupt` refusal, for any result type.
fn corrupt<T>(cause: Corrupt) -> Result<T, ValueError> {
    Err(ValueError::Corrupt(cause))
}

// ---- R1–R5: the element key profile (ADR-rdb-0013 decisions 4 and 5) --------------------------

/// Type rank, in ADR-rdb-0013 decision 4's table order.
fn rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Integer(_) => 2,
        Value::Float(_) => 3,
        Value::Decimal(_) => 4,
        Value::Text(_) => 5,
        Value::Bytes(_) => 6,
        Value::Timestamp(_) => 7,
        Value::Array(_) | Value::Map(_) => unreachable!("not a key: {v:?}"),
    }
}

/// `m · 10^e` against `n · 10^f`, both magnitudes non-zero: cross-multiplied in `u128` when the
/// exponents differ by 19 or less; at 20 or more the larger exponent wins (ADR-rdb-0013
/// decision 5, "The Rust tests").
fn cmp_magnitude(m: u128, e: i64, n: u128, f: i64) -> Ordering {
    let d = i128::from(e) - i128::from(f);
    let scale = |by: i128| 10_u128.pow(u32::try_from(by).expect("0..=19"));
    match d {
        0 => m.cmp(&n),
        1..=19 => (m * scale(d)).cmp(&n),
        -19..=-1 => m.cmp(&(n * scale(-d))),
        20.. => Ordering::Greater,
        _ => Ordering::Less,
    }
}

/// The order oracle, written from ADR-rdb-0013 decision 4's table. It never calls the encoder.
fn oracle(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Integer(x), Value::Integer(y)) => x.get().cmp(&y.get()),
        (Value::Float(x), Value::Float(y)) => x.get().total_cmp(&y.get()),
        (Value::Decimal(x), Value::Decimal(y)) => {
            let (m, n) = (x.mantissa().get(), y.mantissa().get());
            match m.signum().cmp(&n.signum()) {
                Ordering::Equal if m == 0 => Ordering::Equal,
                Ordering::Equal => {
                    let by = cmp_magnitude(
                        m.unsigned_abs(),
                        x.exponent(),
                        n.unsigned_abs(),
                        y.exponent(),
                    );
                    if m < 0 {
                        by.reverse()
                    } else {
                        by
                    }
                }
                sign => sign,
            }
        }
        (Value::Text(x), Value::Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::Bytes(x), Value::Bytes(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => {
            (x.secs(), x.nanos()).cmp(&(y.secs(), y.nanos()))
        }
        _ => rank(a).cmp(&rank(b)),
    }
}

/// Keys packed close together, so a random pair often shares a type, an adjusted exponent or a
/// prefix: the cases `arb_leaf`'s wide draws rarely reach.
fn near_key() -> impl Strategy<Value = Value> {
    let floats = vec![
        -1.5,
        -1.0,
        -f64::MIN_POSITIVE,
        -5e-324,
        -0.0,
        0.0,
        5e-324,
        f64::MIN_POSITIVE,
        1.0,
        1.5,
        f64::MAX,
        f64::MIN,
    ];
    let chars = vec!['a', 'b', '\0', '\u{ffff}', '\u{10000}'];
    prop_oneof![
        (-3_i128..=3).prop_map(c::int),
        proptest::sample::select(floats).prop_map(c::float),
        (-3_i64..=3, -1200_i128..=1200).prop_filter_map("normalised", |(e, m)| {
            Decimal::new(e, Int::new(m)?).map(Value::Decimal)
        }),
        proptest::collection::vec(proptest::sample::select(chars), 0..4)
            .prop_map(|cs| Value::Text(cs.into_iter().collect())),
        proptest::collection::vec(proptest::sample::select(vec![0_u8, 1, 2, 0xFE, 0xFF]), 0..4)
            .prop_map(Value::Bytes),
        (
            -2_i64..=2,
            proptest::sample::select(vec![0_u32, 1, 999_999_999])
        )
            .prop_map(|(s, n)| c::timestamp(s, n)),
    ]
}

/// Any key: the shared wide scalars, or keys packed close together.
fn arb_key() -> impl Strategy<Value = Value> {
    prop_oneof![c::arb_leaf(), near_key()]
}

/// Two decimals whose exponents differ by -25..=25 around any exponent, the ends included, so
/// the comparator's 19/20 boundary is crossed both ways.
fn decimal_pair() -> impl Strategy<Value = (Value, Value)> {
    use rdb_value::value::{INT_MAX, INT_MIN};
    let mantissa = || {
        prop_oneof![
            INT_MIN..=INT_MAX,
            -1000_i128..=1000,
            proptest::sample::select(vec![1, -1, 9, 11, 10_i128.pow(19) + 1, INT_MAX, INT_MIN]),
        ]
    };
    let exponent = prop_oneof![
        any::<i64>(),
        proptest::sample::select(vec![i64::MIN, i64::MIN + 5, 0, i64::MAX - 5, i64::MAX]),
    ];
    (exponent, -25_i64..=25, mantissa(), mantissa()).prop_filter_map(
        "normalised",
        |(e, d, m, n)| {
            let x = Decimal::new(e, Int::new(m)?)?;
            let y = Decimal::new(e.checked_add(d)?, Int::new(n)?)?;
            Some((Value::Decimal(x), Value::Decimal(y)))
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    /// R1 (protects walk step 7, C3, C4, C7): element key order is the oracle's order on random
    /// pairs. And P4: two keys are equal exactly when the values are (floats by bits, so -0.0
    /// and 0.0 are two keys).
    #[test]
    fn r1_element_keys_sort_as_the_oracle_on_random_pairs(a in arb_key(), b in arb_key()) {
        let (ka, kb) = (encode_element(&a).unwrap(), encode_element(&b).unwrap());
        prop_assert_eq!(ka.cmp(&kb), oracle(&a, &b), "{:?} vs {:?}", a, b);
        prop_assert_eq!(ka == kb, a == b, "{:?} vs {:?}", a, b);
    }

    /// R1, the decimal half (protects C4): the full-range comparator, over every exponent.
    #[test]
    fn r1_decimal_keys_sort_as_the_oracle_across_the_exponent_range((a, b) in decimal_pair()) {
        let (ka, kb) = (encode_element(&a).unwrap(), encode_element(&b).unwrap());
        prop_assert_eq!(ka.cmp(&kb), oracle(&a, &b), "{:?} vs {:?}", a, b);
    }
}

/// R1's oracle at the gap ADR-rdb-0013 decision 5 corrects: at an exponent difference of 19 the
/// larger exponent does not always win. The encoder agrees.
#[test]
fn r1_the_oracle_and_the_encoder_agree_at_an_exponent_gap_of_19() {
    let small = c::decimal(19, 1);
    let large = c::decimal(0, rdb_value::value::INT_MAX);
    assert_eq!(oracle(&small, &large), Ordering::Less);
    assert!(encode_element(&small).unwrap() < encode_element(&large).unwrap());
}

/// ADR-rdb-0013 decision 4's byte examples, in the table's order, which is ascending.
fn adr_examples() -> Vec<(Value, &'static str)> {
    use rdb_value::value::{INT_MAX, INT_MIN};
    vec![
        (Value::Null, "10"),
        (Value::Bool(false), "20"),
        (Value::Bool(true), "21"),
        (c::int(INT_MIN), "30 0000000000000000"),
        (c::int(-1), "30 ffffffffffffffff"),
        (c::int(0), "31 0000000000000000"),
        (c::int(10), "31 000000000000000a"),
        (c::int(INT_MAX), "31 ffffffffffffffff"),
        (c::float(-1.5), "40 4007ffffffffffff"),
        (c::float(-5e-324), "40 7ffffffffffffffe"),
        (c::float(-0.0), "40 7fffffffffffffff"),
        (c::float(0.0), "40 8000000000000000"),
        (c::float(5e-324), "40 8000000000000001"),
        (c::float(1.5), "40 bff8000000000000"),
        (c::decimal(i64::MAX, -1), "50 ff0000000000000000 fd ff"),
        (c::decimal(0, -15), "50 ff7ffffffffffffffe fdf9 ff"),
        (c::decimal(-1, -15), "50 ff7fffffffffffffff fdf9 ff"),
        (c::decimal(i64::MIN, -1), "50 ffffffffffffffffff fd ff"),
        (c::decimal(0, 0), "51"),
        (c::decimal(i64::MIN, 1), "52 000000000000000000 02 00"),
        (c::decimal(-2, 123), "52 008000000000000000 020304 00"),
        (c::decimal(-1, 15), "52 008000000000000000 0206 00"),
        (c::decimal(0, 2), "52 008000000000000000 03 00"),
        (
            c::decimal(i64::MAX, INT_MAX),
            "52 010000000000000012 020905050708050501080408010a06060207020600",
        ),
        (c::text(""), "60 0001"),
        (c::text("a"), "60 61 0001"),
        (c::text("a\0"), "60 61 00ff 0001"),
        (c::text("b"), "60 62 0001"),
        (c::text("\u{ffff}"), "60 efbfbf 0001"),
        (c::text("\u{10000}"), "60 f0908080 0001"),
        (Value::Bytes(vec![]), "70 0001"),
        (Value::Bytes(vec![0]), "70 00ff 0001"),
        (c::timestamp(i64::MIN, 0), "80 0000000000000000 00000000"),
        (
            c::timestamp(-1, 999_999_999),
            "80 7fffffffffffffff 3b9ac9ff",
        ),
        (c::timestamp(0, 0), "80 8000000000000000 00000000"),
        (
            c::timestamp(1_700_000_000, 5),
            "80 800000006553f100 00000005",
        ),
    ]
}

/// R2 (protects walk steps 1–2, C3–C6): ADR-rdb-0013 decision 4's examples, encoded and decoded
/// byte for byte, and ascending by both the bytes and the oracle.
#[test]
fn r2_adr_examples_encode_and_decode_byte_for_byte() {
    let examples = adr_examples();
    for (value, hex) in &examples {
        let bytes = spaced(hex);
        assert_eq!(encode_element(value).unwrap(), bytes, "encode {value:?}");
        assert_eq!(decode_element(&bytes).as_ref(), Ok(value), "decode {hex}");
    }
    for pair in examples.windows(2) {
        let [(a, ha), (b, hb)] = pair else {
            unreachable!()
        };
        assert!(spaced(ha) < spaced(hb), "{ha} then {hb}");
        assert_eq!(oracle(a, b), Ordering::Less, "{a:?} then {b:?}");
    }
}

/// R2 (protects walk step 10(c)): ADR-rdb-0013 decision 4's refusal vectors, each refused by its
/// named check. A negative decimal's checks run on the inverted bytes, and a magnitude of 2^64
/// is the largest negative and one past the largest positive.
#[test]
fn r2_adr_refusal_vectors_are_refused_by_name() {
    let twenty_one = format!("52 008000000000000014 {} 00", "02".repeat(21));
    let cases = [
        (
            "52 008000000000000000 0201 00",
            KeyError::DecimalTrailingZero,
        ),
        (
            "52 008000000000000000 0b 00",
            KeyError::DecimalDigitByte(0x0B),
        ),
        ("52 008000000000000000 00", KeyError::DecimalNoDigits),
        (
            "52 008000000000000000 0102 00",
            KeyError::DecimalLeadingZero,
        ),
        (twenty_one.as_str(), KeyError::DecimalMantissaRange),
        (
            "52 008000000000000013 020905050708050501080408010a06060207020700",
            KeyError::DecimalMantissaRange,
        ),
        (
            "52 000000000000000000 0302 00",
            KeyError::DecimalExponentRange,
        ),
        (
            "52 010000000000000013 02 00",
            KeyError::DecimalExponentRange,
        ),
    ];
    for (hex, fault) in cases {
        assert_eq!(decode_element(&spaced(hex)), Err(fault), "{hex}");
    }

    let negative = |positive_body: &str| {
        let mut bytes = vec![0x50];
        bytes.extend(spaced(positive_body).iter().map(|b| !b));
        bytes
    };
    assert_eq!(
        decode_element(&negative("008000000000000000 0201 00")),
        Err(KeyError::DecimalTrailingZero)
    );
    let two_to_64 = "008000000000000013 020905050708050501080408010a06060207020700";
    assert_eq!(
        decode_element(&negative(two_to_64)),
        Ok(c::decimal(0, rdb_value::value::INT_MIN))
    );
    let one_more = "008000000000000013 020905050708050501080408010a06060207020800";
    assert_eq!(
        decode_element(&negative(one_more)),
        Err(KeyError::DecimalMantissaRange)
    );
}

/// R2 (protects walk steps 1–4, whose hex the walk prints): ADR-rdb-0013 decision 6's keys and
/// decision 7's root envelopes. Decision 7 gives digests by their ends, so the ends are checked.
#[test]
fn r2_adr_object_keys_and_root_envelopes_byte_for_byte() {
    let root = cart();
    assert_eq!(
        hex::encode(root.as_bytes()),
        hex::encode(spaced(&format!("{SCOPE} 63617274 0001 00")))
    );
    for (name, hex) in [("apple", "6170706c65"), ("banana", "62616e616e61")] {
        assert_eq!(
            hex::encode(element_key(&root, &text(name)).unwrap()),
            hex::encode(spaced(&format!("{SCOPE} 63617274 0001 01 60 {hex} 0001"))),
            "{name}"
        );
    }
    assert_eq!(
        hex::encode(root_key(TenantId(1), AffinityId(1), b"user:1").as_bytes()),
        hex::encode(spaced(&format!("{SCOPE} 757365723a31 0001 00")))
    );

    let s = MapSnapshot::new(Generation(1));
    let root_value = |ops: &[ElemOp]| {
        let compiled =
            compile_collection(&s, &root, CollectionKind::Map, Expected::Absent, ops).unwrap();
        let Mutation::Put { value, .. } = &compiled.mutations[0] else {
            panic!("the root Put comes first");
        };
        value.clone()
    };
    let empty = hex::encode(root_value(&[]));
    assert_eq!(empty.len(), 2 * 54, "{empty}");
    assert!(empty.starts_with("010201010000000e1cbfe09b"), "{empty}");
    assert!(
        empty.ends_with("96fa3962a2646b6579730165636f756e7400"),
        "{empty}"
    );
    let digest = |value: &Bytes| hex::encode(&value[8..40]);
    let one = digest(&root_value(&[ElemOp::Put(text("a"), int(1))]));
    assert!(
        one.starts_with("7c1e7ad7") && one.ends_with("7b223737"),
        "{one}"
    );
    let two = digest(&root_value(&[
        ElemOp::Put(text("a"), int(1)),
        ElemOp::Put(text("b"), int(1)),
    ]));
    assert!(
        two.starts_with("7f9d1928") && two.ends_with("1b8d5e24"),
        "{two}"
    );
    let document = compile(
        &s,
        &root,
        Expected::Absent,
        &Delta(vec![Op::Replace(int(3))]),
    )
    .unwrap();
    let Mutation::Put { value, .. } = &document.mutations[0] else {
        panic!("a document compiles to one Put");
    };
    let three = digest(value);
    assert!(
        three.starts_with("eb74d6f1") && three.ends_with("e820b491"),
        "{three}"
    );
}

/// The 781 byte strings of length 0–4 over {00, 01, 02, FE, FF} (ADR-rdb-0013 decision 5).
fn ids_781() -> Vec<Vec<u8>> {
    let alphabet = [0x00, 0x01, 0x02, 0xFE, 0xFF];
    let mut layer = vec![Vec::new()];
    let mut out = layer.clone();
    for _ in 0..4 {
        layer = layer
            .iter()
            .flat_map(|p: &Vec<u8>| {
                alphabet.iter().map(move |&b| {
                    let mut x = p.clone();
                    x.push(b);
                    x
                })
            })
            .collect();
        out.extend(layer.iter().cloned());
    }
    assert_eq!(out.len(), 781);
    out
}

/// R3 (protects C1, C23, walk step 9): over the 781-string set, `esc` keeps byte order and is
/// prefix-free, and every record of one object sorts together with its root first (P1, P2);
/// each root key parses back to its id. `""`, `"a"` and `"a\0"` are three ordered keys.
#[test]
fn r3_escaped_ids_are_ordered_prefix_free_and_contiguous_over_781_strings() {
    let mut ids = ids_781();
    ids.sort();
    let escaped: Vec<Vec<u8>> = ids
        .iter()
        .map(|id| {
            let mut out = Vec::new();
            esc(id, &mut out);
            out
        })
        .collect();
    // Sorted ids give strictly ascending escapes, so checking neighbours finds any prefix.
    for (i, pair) in escaped.windows(2).enumerate() {
        let (a, b) = (&ids[i], &ids[i + 1]);
        assert!(pair[0] < pair[1], "order: {a:02x?} then {b:02x?}");
        assert!(
            !pair[1].starts_with(&pair[0]),
            "prefix: {a:02x?} of {b:02x?}"
        );
    }

    let subs = [0x00_u8, 0x01, 0x04, 0xFF];
    let tails: [&[u8]; 4] = [&[], &[0x00], &[0x10], &[0xFF, 0xFF]];
    let mut keys = Vec::new();
    for (i, id_escaped) in escaped.iter().enumerate() {
        for sub in subs {
            for (t, tail) in tails.iter().enumerate() {
                let mut key = id_escaped.clone();
                key.push(sub);
                key.extend_from_slice(tail);
                keys.push((key, (i, sub, t)));
            }
        }
    }
    keys.sort();
    for pair in keys.windows(2) {
        assert!(
            pair[0].1 < pair[1].1,
            "{:?} then {:?}",
            pair[0].1,
            pair[1].1
        );
    }

    for id in &ids {
        let root = root_key(TenantId(1), AffinityId(1), id);
        let parsed = parse(root.as_bytes()).unwrap();
        assert_eq!(
            (&parsed.object_id, parsed.sub, &parsed.element),
            (id, Sub::Root, &None)
        );
        assert_eq!(parsed.root(), root);
    }

    let c1: Vec<Vec<u8>> = ["", "a", "a\0"]
        .iter()
        .map(|t| encode_element(&text(t)).unwrap())
        .collect();
    assert!(c1[0] < c1[1] && c1[1] < c1[2], "{c1:02x?}");
}

/// R4 (protects C2): text keys sort by UTF-8 bytes, so U+FFFF comes before U+10000. UTF-16 order
/// is the other way round, which is the case this row exists for.
#[test]
fn r4_text_keys_sort_by_utf8_not_utf16() {
    let key = |t: &str| encode_element(&text(t)).unwrap();
    assert!(key("\u{ffff}") < key("\u{10000}"));
    let utf16 = |t: &str| t.encode_utf16().collect::<Vec<u16>>();
    assert!(utf16("\u{ffff}") > utf16("\u{10000}"));
}

/// A key's bytes with up to two bytes overwritten, up to two cut from the end, and up to two
/// added.
fn arb_mangled() -> impl Strategy<Value = Vec<u8>> {
    (
        arb_key(),
        proptest::collection::vec((any::<proptest::sample::Index>(), any::<u8>()), 0..3),
        0..3_usize,
        proptest::collection::vec(any::<u8>(), 0..3),
    )
        .prop_map(|(key, edits, cut, extra)| {
            let mut bytes = encode_element(&key).expect("a scalar");
            for (at, byte) in edits {
                let i = at.index(bytes.len());
                bytes[i] = byte;
            }
            bytes.truncate(bytes.len().saturating_sub(cut));
            bytes.extend(extra);
            bytes
        })
}

/// Whether `parsed` names exactly `key`, rebuilt through the encoder. A reserved `sub`'s tail is
/// not decoded, so only the part up to the `sub` is compared.
fn names_exactly(key: &[u8], parsed: &Parsed) -> bool {
    match parsed.sub {
        Sub::Root => parsed.root().as_bytes() == key,
        Sub::Element => {
            let element = parsed.element.as_ref().expect("an element is decoded");
            element_key(&parsed.root(), element).unwrap()[..] == *key
        }
        Sub::Chunk => {
            let (upload, index) = parsed.chunk.expect("a chunk tail is decoded");
            chunk_key(&parsed.root(), &upload, index)[..] == *key
        }
        Sub::Reserved(sub) => {
            let mut head = parsed.root().object_prefix().to_vec();
            head.push(sub);
            key.starts_with(&head)
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5_000))]

    /// R5 (protects walk step 10(c)): `decode(key(x)) = x`; mangled and random bytes never
    /// panic, and whatever decodes, as an element key or as a whole key, re-encodes to the same
    /// bytes (P5).
    #[test]
    fn r5_keys_round_trip_and_damaged_bytes_never_decode_loosely(
        x in arb_key(),
        mangled in arb_mangled(),
        raw in proptest::collection::vec(any::<u8>(), 0..20),
    ) {
        prop_assert_eq!(decode_element(&encode_element(&x).unwrap()), Ok(x.clone()));
        for tail in [&mangled, &raw] {
            if let Ok(v) = decode_element(tail) {
                prop_assert_eq!(&encode_element(&v).unwrap(), tail);
            }
            let mut key = root_key(TenantId(1), AffinityId(1), b"c\0t").element_prefix();
            key.extend_from_slice(tail);
            if let Ok(parsed) = parse(&key) {
                prop_assert!(names_exactly(&key, &parsed), "{:02x?}", key);
            }
        }
        let mut key = spaced(SCOPE);
        key.extend_from_slice(&raw);
        if let Ok(parsed) = parse(&key) {
            prop_assert!(names_exactly(&key, &parsed), "{:02x?}", key);
        }
    }
}

/// R5, by name (protects walk step 10(c)): each way a stored key can be wrong has its own
/// `KeyError`, from the element decoder and from `parse`. `61 00 02` is the escape a loose reader
/// would take for a terminator.
#[test]
fn r5_each_damaged_key_is_refused_by_name() {
    let element = [
        ("", KeyError::Truncated { tag: 0 }),
        ("11", KeyError::UnknownTag(0x11)),
        ("31 00", KeyError::Truncated { tag: 0x31 }),
        ("30 ffff", KeyError::Truncated { tag: 0x30 }),
        ("40 00", KeyError::Truncated { tag: 0x40 }),
        ("80 8000000000000000", KeyError::Truncated { tag: 0x80 }),
        (
            "52 008000000000000000 02",
            KeyError::Truncated { tag: 0x52 },
        ),
        ("50 ff7fffffffffffffff", KeyError::Truncated { tag: 0x50 }),
        ("60 ff 0001", KeyError::NotUtf8),
        ("40 fff0000000000000", KeyError::NonFiniteFloat),
        ("40 000fffffffffffff", KeyError::NonFiniteFloat),
        ("40 fff8000000000000", KeyError::NonFiniteFloat),
        (
            "80 8000000000000000 3b9aca00",
            KeyError::NanosOutOfRange(1_000_000_000),
        ),
        ("10 00", KeyError::TrailingBytes { len: 1 }),
        ("60 61 0002", KeyError::BadEscape { at: 2 }),
        ("70 00", KeyError::Unterminated),
        ("60 61", KeyError::Unterminated),
    ];
    for (hex, fault) in element {
        assert_eq!(decode_element(&spaced(hex)), Err(fault), "{hex:?}");
    }

    let whole = [
        (
            "00000001 00000000000000".to_owned(),
            KeyError::ShortScope { len: 11 },
        ),
        (
            format!("{SCOPE} 61 0002 00"),
            KeyError::BadEscape { at: 13 },
        ),
        (format!("{SCOPE} 61"), KeyError::Unterminated),
        (format!("{SCOPE} 61 0001"), KeyError::MissingSub),
        (
            format!("{SCOPE} 61 0001 00 10"),
            KeyError::RootHasTail { len: 1 },
        ),
        (format!("{SCOPE} 61 0001 01 11"), KeyError::UnknownTag(0x11)),
    ];
    for (hex, fault) in whole {
        assert_eq!(parse(&spaced(&hex)), Err(fault), "{hex}");
    }
    let reserved = parse(&spaced(&format!("{SCOPE} 61 0001 02 ffff"))).unwrap();
    assert_eq!(
        (
            reserved.object_id.as_slice(),
            reserved.sub,
            reserved.element
        ),
        (&b"a"[..], Sub::Reserved(0x02), None)
    );

    let root = cart();
    let elsewhere = root_key(TenantId(1), AffinityId(1), b"cars").element_prefix();
    assert_eq!(
        decode_element_key(&root, &[elsewhere, vec![0x10]].concat()),
        Err(KeyError::OutsideObject)
    );
    let apple = element_key(&root, &text("apple")).unwrap();
    assert_eq!(decode_element_key(&root, &apple), Ok(text("apple")));
}

// ---- R6–R13: compile, against a model and by row (ADR-rdb-0013 decisions 9–11) ---------------

/// Apply `compiled` at `seq` as the kernel would: every condition and `expected_version` is
/// checked first, then every write lands. `Err` names the check that failed.
fn apply(s: &MapSnapshot, compiled: &Compiled, seq: u64) -> Result<MapSnapshot, String> {
    for condition in &compiled.conditions {
        match condition {
            Condition::Absent { key } if s.version(Namespace::User, key).is_some() => {
                return Err(format!("Absent failed on {}", hex::encode(key)));
            }
            Condition::Absent { .. } => {}
            other => panic!("compile emits only Absent, got {other:?}"),
        }
    }
    let mut records: BTreeMap<Bytes, (u64, Bytes)> = s
        .records()
        .map(|(k, v, b)| (k.clone(), (v, b.clone())))
        .collect();
    for mutation in &compiled.mutations {
        let (Mutation::Put {
            key,
            expected_version,
            ..
        }
        | Mutation::Delete {
            key,
            expected_version,
        }) = mutation;
        if let Some(want) = expected_version {
            let found = s.version(Namespace::User, key);
            if found != Some(*want) {
                return Err(format!("expected_version {want}, found {found:?}"));
            }
        }
        match mutation {
            Mutation::Put { key, value, .. } => {
                records.insert(key.clone(), (seq, value.clone()));
            }
            Mutation::Delete { key, .. } => {
                records.remove(key);
            }
        }
    }
    let mut out = MapSnapshot::new(Generation(1));
    for (key, (version, value)) in records {
        out.insert(key, version, value);
    }
    Ok(out)
}

/// A store with the collection at `cart()` created by `ops` at version 1.
fn created(kind: CollectionKind, ops: &[ElemOp]) -> MapSnapshot {
    let empty = MapSnapshot::new(Generation(1));
    let compiled = compile_collection(&empty, &cart(), kind, Expected::Absent, ops).unwrap();
    apply(&empty, &compiled, 1).unwrap()
}

/// Every element, in order, and the root's count.
fn listed(s: &MapSnapshot, root: &RootKey) -> (u64, Vec<(Value, Option<Value>)>) {
    let page = members(s, root, None, usize::MAX).expect("a sound store lists");
    let list = page.members.into_iter().map(|m| (m.key, m.value)).collect();
    (page.collection.count, list)
}

/// A model collection: element key bytes → (key, value). The bytes only order it.
type Model = BTreeMap<Vec<u8>, (Value, Option<Value>)>;

/// The ops on the model, one at a time; a `need` that fails refuses the lot.
fn model_apply(before: &Model, ops: &[ElemOp]) -> Result<Model, ApplyError> {
    let mut m = before.clone();
    let at = |k: &Value| encode_element(k).unwrap();
    for op in ops {
        match op {
            ElemOp::Put(k, v) => {
                m.insert(at(k), (k.clone(), Some(v.clone())));
            }
            ElemOp::Add(k) => {
                m.insert(at(k), (k.clone(), None));
            }
            ElemOp::Remove(k) => {
                m.remove(&at(k));
            }
            ElemOp::NeedPresent(k) if !m.contains_key(&at(k)) => {
                return Err(ApplyError::ElementAbsent)
            }
            ElemOp::NeedAbsent(k) if m.contains_key(&at(k)) => {
                return Err(ApplyError::ElementExists)
            }
            ElemOp::NeedPresent(_) | ElemOp::NeedAbsent(_) => {}
        }
    }
    Ok(m)
}

/// A few keys of several types, so random op lists hit the same key often.
fn universe() -> Vec<Value> {
    vec![
        Value::Null,
        int(0),
        int(7),
        text(""),
        text("a"),
        text("a\0"),
        Value::Bytes(vec![0]),
        c::float(-0.0),
        c::float(0.0),
        c::decimal(-1, 15),
    ]
}

/// Up to 7 ops of `kind` over [`universe`].
fn arb_ops(kind: CollectionKind) -> BoxedStrategy<Vec<ElemOp>> {
    let key = || proptest::sample::select(universe());
    let write = match kind {
        CollectionKind::Map => (key(), 0_u64..3)
            .prop_map(|(k, v)| ElemOp::Put(k, int(v)))
            .boxed(),
        CollectionKind::Set => key().prop_map(ElemOp::Add).boxed(),
    };
    let op = prop_oneof![
        4 => write,
        3 => key().prop_map(ElemOp::Remove),
        1 => key().prop_map(ElemOp::NeedPresent),
        1 => key().prop_map(ElemOp::NeedAbsent),
    ];
    proptest::collection::vec(op, 0..8).boxed()
}

fn arb_kind() -> impl Strategy<Value = CollectionKind> {
    prop_oneof![Just(CollectionKind::Map), Just(CollectionKind::Set)]
}

/// R6 over one compiled batch: the root `Put` first, then one write per element key in key
/// order, none with its own `expected_version`; a `Delete` only for a key present before and
/// gone after; and every key whose presence or value changed is written.
fn check_writes(
    root: &RootKey,
    before: &Model,
    after: &Model,
    compiled: &Compiled,
) -> Result<(), TestCaseError> {
    let (head, elements) = compiled.mutations.split_first().expect("the root Put");
    prop_assert!(
        matches!(head, Mutation::Put { key, .. } if key[..] == *root.as_bytes()),
        "{:?}",
        head
    );
    let mut tails = Vec::new();
    for mutation in elements {
        let (Mutation::Put {
            key,
            expected_version,
            ..
        }
        | Mutation::Delete {
            key,
            expected_version,
        }) = mutation;
        prop_assert_eq!(*expected_version, None);
        let element = decode_element_key(root, key).expect("an element key");
        let tail = encode_element(&element).unwrap();
        match mutation {
            Mutation::Delete { .. } => prop_assert!(
                before.contains_key(&tail) && !after.contains_key(&tail),
                "a Delete for {:?}",
                element
            ),
            Mutation::Put { .. } => prop_assert!(after.contains_key(&tail)),
        }
        tails.push(tail);
    }
    prop_assert!(
        tails.windows(2).all(|w| w[0] < w[1]),
        "one write per key, in key order: {:?}",
        compiled.mutations
    );
    for (tail, now) in after {
        if before.get(tail) != Some(now) {
            prop_assert!(tails.contains(tail), "{:?} changed but is not written", now);
        }
    }
    for tail in before.keys() {
        if !after.contains_key(tail) {
            prop_assert!(tails.contains(tail), "a removed key is not deleted");
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1_000))]

    /// R7 and R6 (protect walk steps 2, 3 and 6, C9, C10): random batches of ops, each one
    /// transaction from an empty store, match a `BTreeMap` model in members, values and
    /// `count`, with one write per changed key. A batch whose `need` fails is refused with the
    /// model's error and changes nothing.
    #[test]
    fn r7_count_members_and_writes_match_a_model_over_random_batches(
        (kind, batches) in arb_kind()
            .prop_flat_map(|kind| (Just(kind), proptest::collection::vec(arb_ops(kind), 1..6)))
    ) {
        let root = cart();
        let mut s = MapSnapshot::new(Generation(1));
        let mut model: Option<Model> = None;
        for (seq, ops) in (1_u64..).zip(&batches) {
            let expected = match model {
                None => Expected::Absent,
                Some(_) => Expected::Version(collection(&s, &root).unwrap().unwrap().version),
            };
            let before = model.clone().unwrap_or_default();
            match (model_apply(&before, ops), compile_collection(&s, &root, kind, expected, ops)) {
                (Err(want), got) => prop_assert_eq!(got, Err(ValueError::Apply(want))),
                (Ok(after), Ok(compiled)) => {
                    check_writes(&root, &before, &after, &compiled)?;
                    s = apply(&s, &compiled, seq).expect("its own conditions hold");
                    let (count, list) = listed(&s, &root);
                    prop_assert_eq!(count, u64::try_from(after.len()).unwrap());
                    prop_assert_eq!(list, after.values().cloned().collect::<Vec<_>>());
                    model = Some(after);
                }
                (Ok(after), Err(got)) => {
                    return Err(TestCaseError::fail(format!("model {after:?}, compile {got:?}")));
                }
            }
        }
    }

    /// R13 L1 (protects walk step 3 against steps 2+3): two deltas in two transactions equal
    /// their concatenation in one, by members and `count`, or both are refused alike.
    #[test]
    fn r13_l1_two_transactions_equal_their_concatenation(
        (kind, start, a, b) in arb_kind()
            .prop_flat_map(|kind| (Just(kind), arb_ops(kind), arb_ops(kind), arb_ops(kind)))
    ) {
        let root = cart();
        let empty = MapSnapshot::new(Generation(1));
        let create = compile_collection(&empty, &root, kind, Expected::Absent, &start)
            .or_else(|_| compile_collection(&empty, &root, kind, Expected::Absent, &[]))
            .unwrap();
        let s = apply(&empty, &create, 1).unwrap();
        let two = compile_collection(&s, &root, kind, Expected::Version(1), &a).and_then(|ca| {
            let s2 = apply(&s, &ca, 2).unwrap();
            compile_collection(&s2, &root, kind, Expected::Version(2), &b)
                .map(|cb| apply(&s2, &cb, 3).unwrap())
        });
        let ab: Vec<ElemOp> = a.iter().chain(&b).cloned().collect();
        let one = compile_collection(&s, &root, kind, Expected::Version(1), &ab)
            .map(|compiled| apply(&s, &compiled, 2).unwrap());
        match (two, one) {
            (Ok(x), Ok(y)) => prop_assert_eq!(listed(&x, &root), listed(&y, &root)),
            (Err(x), Err(y)) => prop_assert_eq!(x, y),
            (x, y) => {
                return Err(TestCaseError::fail(format!(
                    "two {:?}, one {:?}",
                    x.map(|_| ()),
                    y.map(|_| ())
                )));
            }
        }
    }

    /// R13 L6 (protects C20): the same ops on the same records give the same bytes, from the
    /// same snapshot twice and from a copy of it.
    #[test]
    fn r13_l6_the_same_ops_give_the_same_bytes(
        (kind, start, ops) in arb_kind()
            .prop_flat_map(|kind| (Just(kind), arb_ops(kind), arb_ops(kind)))
    ) {
        let root = cart();
        let empty = MapSnapshot::new(Generation(1));
        let s = compile_collection(&empty, &root, kind, Expected::Absent, &start)
            .map_or(empty.clone(), |compiled| apply(&empty, &compiled, 1).unwrap());
        let expected = if s.records().next().is_some() {
            Expected::Version(1)
        } else {
            Expected::Absent
        };
        let mut copy = MapSnapshot::new(Generation(1));
        for (k, v, b) in s.records() {
            copy.insert(k.clone(), v, b.clone());
        }
        let once = compile_collection(&s, &root, kind, expected, &ops);
        prop_assert_eq!(&once, &compile_collection(&s, &root, kind, expected, &ops));
        prop_assert_eq!(&once, &compile_collection(&copy, &root, kind, expected, &ops));
    }
}

/// R6 (protects walk steps 3 and 6, C9, C10), by example: one mutation per touched key, root
/// first, in key order; none for a key absent before and after, and none for a key that only a
/// `need` touched.
#[test]
fn r6_one_mutation_per_touched_key_and_none_for_absent_before_and_after() {
    let root = cart();
    let s = created(
        CollectionKind::Map,
        &[
            ElemOp::Put(text("apple"), int(3)),
            ElemOp::Put(text("kiwi"), int(1)),
        ],
    );
    let ops = [
        ElemOp::Put(text("banana"), int(5)),
        ElemOp::Put(text("banana"), int(4)),
        ElemOp::Put(text("zzz"), int(1)),
        ElemOp::Remove(text("zzz")),
        ElemOp::Remove(text("nope")),
        ElemOp::NeedPresent(text("kiwi")),
        ElemOp::Remove(text("apple")),
    ];
    let compiled =
        compile_collection(&s, &root, CollectionKind::Map, Expected::Version(1), &ops).unwrap();
    let key = |t: &str| element_key(&root, &text(t)).unwrap();
    let entry = |v: u64| seal(Kind::Document, &encode(&int(v)).unwrap()).unwrap();
    assert_eq!(
        compiled.mutations,
        vec![
            Mutation::Put {
                key: root.to_bytes(),
                value: map_root(2),
                expected_version: Some(1),
            },
            Mutation::Delete {
                key: key("apple"),
                expected_version: None,
            },
            Mutation::Put {
                key: key("banana"),
                value: entry(4),
                expected_version: None,
            },
        ]
    );
}

/// Review Q-1: ADR-rdb-0013 decision 9's table makes `Add` of a present member a no-op. So it
/// writes nothing for that member: the member keeps its version, takes no mutation slot, and a
/// member holding bytes is not rewritten behind the reader's back (decision 11). The root is
/// still written, as for every compiled delta (decisions 8 and 9).
#[test]
fn q1_add_of_a_present_member_writes_nothing_for_it() {
    let root = cart();
    let mut s = created(
        CollectionKind::Set,
        &[ElemOp::Add(text("a")), ElemOp::Add(text("b"))],
    );
    let (a, b) = (
        element_key(&root, &text("a")).unwrap(),
        element_key(&root, &text("b")).unwrap(),
    );
    s.insert(b, 1, Bytes::from_static(b"x"));
    let ops = [ElemOp::Add(text("a")), ElemOp::Add(text("b"))];
    let compiled =
        compile_collection(&s, &root, CollectionKind::Set, Expected::Version(1), &ops).unwrap();
    assert_eq!(
        compiled.mutations,
        vec![Mutation::Put {
            key: root.to_bytes(),
            value: root_record(Kind::Set, 2),
            expected_version: Some(1),
        }]
    );
    let after = apply(&s, &compiled, 2).unwrap();
    assert_eq!(after.version(Namespace::User, &a), Some(1));
    assert_eq!(
        members(&after, &root, None, 10).map(|_| ()),
        corrupt(Corrupt::SetMemberHasValue { len: 1 })
    );
}

/// R8 (protects walk step 5, C20): an update carries the root's version and is refused at a
/// stale one, here and by the kernel; two racing creates compile to the same bytes, and the
/// second fails its `Absent` condition.
#[test]
fn r8_a_stale_version_is_refused_and_the_second_racing_create_fails() {
    let root = cart();
    let empty = MapSnapshot::new(Generation(1));
    let create = [ElemOp::Put(text("apple"), int(3))];
    let a = compile_collection(
        &empty,
        &root,
        CollectionKind::Map,
        Expected::Absent,
        &create,
    )
    .unwrap();
    let b = compile_collection(
        &empty,
        &root,
        CollectionKind::Map,
        Expected::Absent,
        &create,
    )
    .unwrap();
    assert_eq!(a, b, "C20: the same bytes");
    assert_eq!(
        a.conditions,
        vec![Condition::Absent {
            key: root.to_bytes()
        }]
    );
    assert!(matches!(
        &a.mutations[0],
        Mutation::Put {
            expected_version: None,
            ..
        }
    ));
    let s = apply(&empty, &a, 1).unwrap();
    assert!(apply(&s, &b, 2).is_err(), "the second create fails");

    let put = [ElemOp::Put(text("banana"), int(5))];
    let update =
        compile_collection(&s, &root, CollectionKind::Map, Expected::Version(1), &put).unwrap();
    assert!(update.conditions.is_empty());
    assert!(
        matches!(
            &update.mutations[0],
            Mutation::Put { key, expected_version: Some(1), .. } if key == &root.to_bytes()
        ),
        "{:?}",
        update.mutations[0]
    );
    let s2 = apply(&s, &update, 2).unwrap();
    assert!(
        apply(&s2, &update, 3).is_err(),
        "the kernel refuses a raced update"
    );
    assert_eq!(
        compile_collection(&s2, &root, CollectionKind::Map, Expected::Version(1), &put),
        Err(ValueError::Apply(ApplyError::VersionConflict {
            expected: 1,
            found: 2
        }))
    );
    assert_eq!(
        compile_collection(
            &empty,
            &root,
            CollectionKind::Map,
            Expected::Version(1),
            &put
        ),
        Err(ValueError::Apply(ApplyError::ObjectAbsent))
    );
}

/// R9 (protects walk step 8, C13, C24): `KindMismatch` in both directions, on every entry
/// point. The `RootKey` `compile_fail` doctest is the other half: a document op cannot be aimed
/// at an element key.
#[test]
fn r9_kind_mismatch_in_both_directions() {
    let root = cart();
    let empty = MapSnapshot::new(Generation(1));
    let map = created(CollectionKind::Map, &[ElemOp::Put(text("a"), int(1))]);
    let set = created(CollectionKind::Set, &[ElemOp::Add(text("a"))]);
    let document = compile(
        &empty,
        &root,
        Expected::Absent,
        &Delta(vec![Op::Replace(int(1))]),
    )
    .unwrap();
    let document = apply(&empty, &document, 1).unwrap();
    let mismatch = |found| Err(ValueError::Apply(ApplyError::KindMismatch { found }));
    let replace = Delta(vec![Op::Replace(int(2))]);
    for (s, found) in [(&map, Kind::Map), (&set, Kind::Set)] {
        assert_eq!(read(s, &root).map(|_| ()), mismatch(found));
        assert_eq!(
            compile(s, &root, Expected::Version(1), &replace).map(|_| ()),
            mismatch(found)
        );
    }

    let d = &document;
    let found = Kind::Document;
    assert_eq!(collection(d, &root).map(|_| ()), mismatch(found));
    let ops: &[ElemOp] = &[];
    assert_eq!(
        compile_collection(d, &root, CollectionKind::Map, Expected::Version(1), ops).map(|_| ()),
        mismatch(found)
    );
    assert_eq!(member(d, &root, &text("a")).map(|_| ()), mismatch(found));
    assert_eq!(members(d, &root, None, 10).map(|_| ()), mismatch(found));
    assert_eq!(drop_collection(d, &root, 1).map(|_| ()), mismatch(found));

    let at = |s: &MapSnapshot, kind, ops: &[ElemOp]| {
        compile_collection(s, &root, kind, Expected::Version(1), ops).map(|_| ())
    };
    assert_eq!(at(&set, CollectionKind::Map, &[]), mismatch(Kind::Set));
    assert_eq!(at(&map, CollectionKind::Set, &[]), mismatch(Kind::Map));
    assert_eq!(
        at(&map, CollectionKind::Map, &[ElemOp::Add(text("b"))]),
        mismatch(Kind::Map)
    );
    assert_eq!(
        at(&set, CollectionKind::Set, &[ElemOp::Put(text("b"), int(1))]),
        mismatch(Kind::Set)
    );
    assert_eq!(
        at(
            &map,
            CollectionKind::Map,
            &[ElemOp::Put(Value::Array(vec![]), int(1))]
        ),
        Err(ValueError::Apply(ApplyError::UnsupportedKeyType))
    );
}

/// R10 (protects walk step 10, W1 A4): each damaged root, element key or element value is
/// refused by name on the read paths, never read as data.
#[test]
fn r10_each_corrupt_cause_is_named_on_read() {
    let root = cart();
    let apple = element_key(&root, &text("apple")).unwrap();
    let entry = || seal(Kind::Document, &encode(&int(3)).unwrap()).unwrap();
    let store = |root_value: Bytes, element: Bytes, element_version: u64| {
        let mut s = MapSnapshot::new(Generation(1));
        s.insert(root.to_bytes(), 4, root_value);
        s.insert(apple.clone(), element_version, element);
        s
    };
    let reads = |s: &MapSnapshot, want: &Result<(), ValueError>, why: &str| {
        let point = member(s, &root, &text("apple")).map(|_| ());
        assert_eq!(&point, want, "member: {why}");
        let listed = members(s, &root, None, 10).map(|_| ());
        assert_eq!(&listed, want, "members: {why}");
    };

    reads(&store(map_root(1), entry(), 4), &Ok(()), "sound map");
    reads(
        &store(root_record(Kind::Set, 1), Bytes::new(), 4),
        &Ok(()),
        "sound set",
    );
    reads(
        &store(map_root(1), entry(), 6),
        &corrupt(Corrupt::ElementNewerThanRoot {
            element: 6,
            root: 4,
        }),
        "newer than its root",
    );
    let mut flipped = entry().to_vec();
    *flipped.last_mut().unwrap() ^= 1;
    reads(
        &store(map_root(1), Bytes::from(flipped), 4),
        &corrupt(Corrupt::Envelope(EnvelopeError::DigestMismatch)),
        "entry digest",
    );
    reads(
        &store(map_root(1), map_root(0), 4),
        &corrupt(Corrupt::EntryNotDocument { found: Kind::Map }),
        "entry sealed as a map",
    );
    reads(
        &store(map_root(1), seal(Kind::Document, &[0xFF]).unwrap(), 4),
        &corrupt(Corrupt::Codec(decode(&[0xFF]).unwrap_err())),
        "entry not CBOR",
    );
    reads(
        &store(root_record(Kind::Set, 1), Bytes::from_static(b"x"), 4),
        &corrupt(Corrupt::SetMemberHasValue { len: 1 }),
        "set member with bytes",
    );

    // ADR-rdb-0013 decision 4: an element key that does not decode is `Corrupt(Key(..))`.
    let mut s = MapSnapshot::new(Generation(1));
    s.insert(root.to_bytes(), 4, map_root(1));
    let bad = [
        root.element_prefix(),
        spaced("52 008000000000000000 0201 00"),
    ]
    .concat();
    s.insert(Bytes::from(bad), 4, entry());
    assert_eq!(
        members(&s, &root, None, 10).map(|_| ()),
        corrupt(Corrupt::Key(KeyError::DecimalTrailingZero))
    );

    let root_with =
        |fields: &[(&str, Value)]| seal(Kind::Map, &encode(&c::map(fields)).unwrap()).unwrap();
    let mut damaged_digest = map_root(1).to_vec();
    *damaged_digest.last_mut().unwrap() ^= 1;
    let roots = [
        (
            seal(Kind::Map, &encode(&int(1)).unwrap()).unwrap(),
            Corrupt::Root("the payload is not a map"),
        ),
        (
            root_with(&[("keys", c::int(1)), ("count", text("x"))]),
            Corrupt::Root("a field is not an integer"),
        ),
        (
            root_with(&[("keys", c::int(1))]),
            Corrupt::Root("a field is missing"),
        ),
        (
            root_with(&[("count", c::int(0))]),
            Corrupt::Root("a field is missing"),
        ),
        (
            root_with(&[("keys", c::int(1)), ("count", c::int(-1))]),
            Corrupt::Root("count is negative"),
        ),
        (
            root_with(&[("keys", c::int(1)), ("count", c::int(0)), ("x", c::int(0))]),
            Corrupt::Root("fields other than keys and count"),
        ),
        (
            root_with(&[("keys", c::int(2)), ("count", c::int(0))]),
            Corrupt::UnknownKeyProfile(2),
        ),
        // Review F-005: a newer profile is read as that before anything else in the root is
        // judged, so a root this build would call damaged still names the newer profile.
        (
            root_with(&[("keys", c::int(2)), ("count", c::int(0)), ("x", c::int(0))]),
            Corrupt::UnknownKeyProfile(2),
        ),
        (
            root_with(&[("keys", c::int(2))]),
            Corrupt::UnknownKeyProfile(2),
        ),
        (
            root_with(&[("keys", c::int(2)), ("count", c::int(-1))]),
            Corrupt::UnknownKeyProfile(2),
        ),
        (
            seal(Kind::Map, &[0xFF]).unwrap(),
            Corrupt::Codec(decode(&[0xFF]).unwrap_err()),
        ),
        (
            Bytes::from(damaged_digest),
            Corrupt::Envelope(EnvelopeError::DigestMismatch),
        ),
    ];
    for (value, cause) in roots {
        let mut s = MapSnapshot::new(Generation(1));
        s.insert(root.to_bytes(), 4, value);
        assert_eq!(
            collection(&s, &root).map(|_| ()),
            corrupt(cause.clone()),
            "{cause:?}"
        );
    }
}

/// R11 (protects C21, C22): a create over leftover elements is `OrphanElement`, and another
/// object's elements are not leftovers; `drop` deletes only an empty root, says `NotEmpty`
/// while elements exist, and names a drifted count either way.
#[test]
fn r11_orphans_on_create_and_drop_and_count_drift_on_drop() {
    let root = cart();
    let apple = element_key(&root, &text("apple")).unwrap();
    let entry = seal(Kind::Document, &encode(&int(3)).unwrap()).unwrap();
    let with = |count: Option<u64>, element: bool| {
        let mut s = MapSnapshot::new(Generation(1));
        if let Some(count) = count {
            s.insert(root.to_bytes(), 4, map_root(count));
        }
        if element {
            s.insert(apple.clone(), 4, entry.clone());
        }
        s
    };
    let create = |s: &MapSnapshot| {
        compile_collection(s, &root, CollectionKind::Map, Expected::Absent, &[]).map(|_| ())
    };
    assert_eq!(
        create(&with(None, true)),
        corrupt(Corrupt::OrphanElement),
        "C22"
    );
    assert_eq!(create(&with(None, false)), Ok(()));
    let mut neighbour = MapSnapshot::new(Generation(1));
    let next_door = root_key(TenantId(1), AffinityId(1), b"cart\0");
    neighbour.insert(
        element_key(&next_door, &text("apple")).unwrap(),
        4,
        entry.clone(),
    );
    assert_eq!(
        create(&neighbour),
        Ok(()),
        "an element of cart\\0 is not cart's"
    );

    let dropping = |s: &MapSnapshot, version| drop_collection(s, &root, version);
    assert_eq!(
        dropping(&with(Some(0), false), 4),
        Ok(Compiled {
            mutations: vec![Mutation::Delete {
                key: root.to_bytes(),
                expected_version: Some(4),
            }],
            conditions: Vec::new(),
        })
    );
    assert_eq!(
        dropping(&with(Some(1), true), 4),
        Err(ValueError::Apply(ApplyError::NotEmpty { count: 1 }))
    );
    assert_eq!(
        dropping(&with(Some(1), false), 4),
        corrupt(Corrupt::CountMismatch { count: 1 }),
        "C21"
    );
    assert_eq!(
        dropping(&with(Some(0), true), 4),
        corrupt(Corrupt::OrphanElement),
        "C22"
    );
    assert_eq!(
        dropping(&with(None, false), 4),
        Err(ValueError::Apply(ApplyError::ObjectAbsent))
    );
    assert_eq!(
        dropping(&with(Some(0), false), 3),
        Err(ValueError::Apply(ApplyError::VersionConflict {
            expected: 3,
            found: 4
        }))
    );

    let remove = [ElemOp::Remove(text("apple"))];
    assert_eq!(
        compile_collection(
            &with(Some(0), true),
            &root,
            CollectionKind::Map,
            Expected::Version(4),
            &remove
        )
        .map(|_| ()),
        corrupt(Corrupt::OrphanElement),
        "more removed than the root counts"
    );
}

/// R12 (protects C14, C15): 254 elements and the root are 255 writes, the kernel's limit, and
/// compile; one more is refused with the number it would have written. The byte cap's exact
/// boundary is the `l_r186v_*` rows above.
#[test]
fn r12_too_many_writes_at_256_and_accepted_at_255() {
    assert_eq!(MAX_REQUEST_MUTATIONS, 255);
    let root = cart();
    let s = MapSnapshot::new(Generation(1));
    let adds = |n: u64| (0..n).map(|i| ElemOp::Add(int(i))).collect::<Vec<_>>();
    let at = |n| compile_collection(&s, &root, CollectionKind::Set, Expected::Absent, &adds(n));
    assert_eq!(at(254).unwrap().mutations.len(), 255);
    assert_eq!(
        at(255),
        Err(ValueError::Apply(ApplyError::TooManyWrites { writes: 256 }))
    );
}

/// R13 L4 and L5 (protect C10, C9): on a present key, `Put` then `Remove` deletes it and
/// `Remove` then `Put` keeps it; nothing is folded, so a `need` sees each earlier op.
#[test]
fn r13_l4_l5_op_order_matters_and_nothing_is_folded() {
    let root = cart();
    let s = created(CollectionKind::Map, &[ElemOp::Put(text("apple"), int(3))]);
    let apple = element_key(&root, &text("apple")).unwrap();
    let run = |ops: &[ElemOp]| {
        compile_collection(&s, &root, CollectionKind::Map, Expected::Version(1), ops)
    };
    let put_remove = run(&[
        ElemOp::Put(text("apple"), int(9)),
        ElemOp::Remove(text("apple")),
    ])
    .unwrap();
    assert_eq!(
        put_remove.mutations[1..],
        [Mutation::Delete {
            key: apple.clone(),
            expected_version: None
        }]
    );
    let remove_put = run(&[
        ElemOp::Remove(text("apple")),
        ElemOp::Put(text("apple"), int(9)),
    ])
    .unwrap();
    assert_eq!(
        remove_put.mutations[1..],
        [Mutation::Put {
            key: apple,
            value: seal(Kind::Document, &encode(&int(9)).unwrap()).unwrap(),
            expected_version: None
        }]
    );

    assert_eq!(
        run(&[
            ElemOp::Remove(text("apple")),
            ElemOp::NeedPresent(text("apple"))
        ]),
        Err(ValueError::Apply(ApplyError::ElementAbsent))
    );
    assert_eq!(
        run(&[
            ElemOp::Put(text("kiwi"), int(1)),
            ElemOp::NeedAbsent(text("kiwi"))
        ]),
        Err(ValueError::Apply(ApplyError::ElementExists))
    );
    let gone = run(&[
        ElemOp::Put(text("kiwi"), int(1)),
        ElemOp::Remove(text("kiwi")),
        ElemOp::NeedAbsent(text("kiwi")),
    ])
    .unwrap();
    assert_eq!(gone.mutations.len(), 1, "kiwi was absent before and after");
}

/// Paging (protects C17, C18 and the tester's extras): `after` need not be a member, may be of
/// another type and may be past the end; `limit` 0 lists nothing; a neighbouring object's
/// records are never listed.
#[test]
fn members_pages_from_any_after_and_stops_at_the_object() {
    let root = cart();
    let mut s = created(
        CollectionKind::Set,
        &[
            ElemOp::Add(text("a")),
            ElemOp::Add(int(3)),
            ElemOp::Add(int(1)),
            ElemOp::Add(int(2)),
        ],
    );
    let next_door = root_key(TenantId(1), AffinityId(1), b"cart\0");
    s.insert(element_key(&next_door, &int(0)).unwrap(), 1, Bytes::new());
    let page = |after: Option<Value>, limit| {
        let listed = members(&s, &root, after.as_ref(), limit).unwrap();
        listed
            .members
            .into_iter()
            .map(|m| m.key)
            .collect::<Vec<_>>()
    };
    let all = vec![int(1), int(2), int(3), text("a")];
    assert_eq!(page(None, 10), all);
    assert_eq!(page(None, 2), all[..2]);
    assert_eq!(page(Some(int(2)), 10), all[2..], "after a member");
    assert_eq!(page(Some(c::int(-5)), 10), all, "after a non-member");
    assert_eq!(page(Some(text("")), 10), all[3..], "after another type");
    assert!(
        page(Some(Value::Bytes(vec![])), 10).is_empty(),
        "past the end"
    );
    assert!(page(None, 0).is_empty(), "limit 0");
    assert_eq!(
        members(&s, &root, Some(&Value::Array(vec![])), 10).map(|_| ()),
        Err(ValueError::Apply(ApplyError::UnsupportedKeyType))
    );

    let listed = members(&s, &root, None, 1).unwrap();
    assert_eq!(
        listed.collection,
        Collection {
            kind: CollectionKind::Set,
            version: 1,
            count: 4
        }
    );
    assert_eq!(
        listed.members,
        [Member {
            key: int(1),
            value: None,
            version: 1
        }]
    );
    assert_eq!(
        members(&MapSnapshot::new(Generation(1)), &root, None, 10).map(|_| ()),
        Err(ValueError::Apply(ApplyError::ObjectAbsent))
    );
}

/// `member` (protects walk step 4's reads and step 10's point read): one exact key, with its
/// own version, which is not the root's.
#[test]
fn member_reads_one_exact_key() {
    let root = cart();
    let s = created(
        CollectionKind::Map,
        &[
            ElemOp::Put(text("apple"), int(3)),
            ElemOp::Put(text("banana"), int(5)),
        ],
    );
    let update = [ElemOp::Put(text("banana"), int(6))];
    let compiled = compile_collection(
        &s,
        &root,
        CollectionKind::Map,
        Expected::Version(1),
        &update,
    )
    .unwrap();
    let s = apply(&s, &compiled, 2).unwrap();
    let get = |k: Value| member(&s, &root, &k);
    assert_eq!(
        get(text("banana")),
        Ok(Some(Member {
            key: text("banana"),
            value: Some(int(6)),
            version: 2
        }))
    );
    assert_eq!(
        get(text("apple")),
        Ok(Some(Member {
            key: text("apple"),
            value: Some(int(3)),
            version: 1
        }))
    );
    assert_eq!(get(text("kiwi")), Ok(None));
    assert_eq!(
        get(Value::Map(Map::new())),
        Err(ValueError::Apply(ApplyError::UnsupportedKeyType))
    );
    assert_eq!(
        member(&MapSnapshot::new(Generation(1)), &root, &text("apple")),
        Err(ValueError::Apply(ApplyError::ObjectAbsent))
    );
    let set = created(CollectionKind::Set, &[ElemOp::Add(int(1))]);
    assert_eq!(
        member(&set, &root, &int(1)),
        Ok(Some(Member {
            key: int(1),
            value: None,
            version: 1
        }))
    );
}
