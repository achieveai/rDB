//! The deterministic CBOR profile, at the library API (ADR-rdb-0012 decisions 2–5).
//!
//! Scenario: a client sends document bytes, and the primary keeps exactly the bytes our encoder
//! would write, refusing anything else (the S2 design's corner table and the tester's contracts,
//! in working notes not in the repository).

mod common;

use common::{
    arb_value, ciborium_reads, decimal, float, h, int, map, nested, text, timestamp, walk,
};
use proptest::prelude::*;
use rdb_value::cbor::{decode, encode, CodecError, EncodeError, MAX_DEPTH, MAX_PAYLOAD};
use rdb_value::value::{Map, MapKey, Value, INT_MAX, INT_MIN};

/// Both ways, byte-exact: `decode(hex) == value` and `encode(value) == hex`. Then the second
/// reader and the byte walk agree on the same bytes.
fn vector(hex: &str, value: &Value) {
    let bytes = h(hex);
    assert_eq!(decode(&bytes).as_ref(), Ok(value), "decode {hex}");
    assert_eq!(
        hex::encode(encode(value).expect("encodes")),
        hex,
        "encode {value:?}"
    );
    assert_eq!(&ciborium_reads(&bytes), value, "second reader on {hex}");
    walk(&bytes).unwrap_or_else(|e| panic!("byte walk on {hex}: {e}"));
}

/// Scenario: a caller stores each profile type and reads back the same bytes. One row per
/// profile rule (ADR Verification "Vectors"), including every head-width boundary.
///
/// These are the frozen v1 bytes (ADR-rdb-0012 §7). The v1 decoder accepts only what the v1
/// encoder writes, so a change to any row would make stored records unreadable: it needs a new
/// `codec_version`, never an edit here.
#[test]
fn canonical_vectors_round_trip_byte_exact_in_both_directions() {
    vector("f6", &Value::Null);
    vector("f4", &Value::Bool(false));
    vector("f5", &Value::Bool(true));
    for (hex, v) in [
        ("00", 0),
        ("17", 23),
        ("1818", 24),
        ("18ff", 255),
        ("190100", 256),
        ("19ffff", 65_535),
        ("1a00010000", 65_536),
        ("1affffffff", (1 << 32) - 1),
        ("1b0000000100000000", 1 << 32),
        ("1bffffffffffffffff", INT_MAX),
        ("20", -1),
        ("37", -24),
        ("3818", -25),
        ("3bffffffffffffffff", INT_MIN),
    ] {
        vector(hex, &int(v));
    }
    // Floats: always `fb`; -0.0 kept; smallest subnormal; largest; 0.1.
    vector("fb3ff0000000000000", &float(1.0));
    vector("fb8000000000000000", &float(-0.0));
    vector("fb0000000000000001", &float(f64::from_bits(1)));
    vector("fb7fefffffffffffff", &float(f64::MAX));
    vector("fb3fb999999999999a", &float(0.1));
    // Text and bytes at the 23 / 24 length boundary.
    vector(&format!("77{}", "61".repeat(23)), &text(&"a".repeat(23)));
    vector(&format!("7818{}", "61".repeat(24)), &text(&"a".repeat(24)));
    vector("40", &Value::Bytes(vec![]));
    vector(
        &format!("5818{}", "00".repeat(24)),
        &Value::Bytes(vec![0; 24]),
    );
    vector("80", &Value::Array(vec![]));
    vector("a0", &map(&[]));
    vector(
        "82a080",
        &Value::Array(vec![map(&[]), Value::Array(vec![])]),
    );
    // Decimal and timestamp; nanos 0 is omitted, key 1 sorts before -9.
    vector("c482211904d2", &decimal(-2, 1234));
    vector("c4820000", &decimal(0, 0));
    vector("d903e9a1011a5f000000", &timestamp(0x5f00_0000, 0));
    vector("d903e9a2012028187b", &timestamp(-1, 123));
}

/// Scenario: two clients write the same keys in different orders and get the same bytes.
/// Order is (byte length, bytes), not alphabetical: `z`, `aa`, `é` (tester W1 contract 1).
#[test]
fn map_keys_are_ordered_by_length_then_bytes() {
    let doc = map(&[("é", int(1)), ("z", int(2)), ("aa", int(3))]);
    vector("a3617a026261610362c3a901", &doc);
    // `{"b":2,"aa":1}` is the canonical order; `{"aa":1,"b":2}` is refused at its first key.
    vector("a261620262616101", &map(&[("aa", int(1)), ("b", int(2))]));
    assert_eq!(
        decode(&h("a262616101616202")),
        Err(CodecError::NonCanonical { offset: 1 })
    );
}

/// Scenario: a client sends something the profile names a refusal for. Each row asserts the
/// class the ADR names (decision 4 step 4, decision 12), so the diagnostic is pinned too.
#[test]
fn named_refusals_carry_their_class() {
    use CodecError::*;
    let rows: &[(&str, CodecError)] = &[
        // The four that re-encode byte-identically and need a named check.
        ("a2616101616102", DuplicateKey),
        ("00ff", TrailingBytes { offset: 1 }),
        ("fb7ff8000000000001", NonFiniteFloat),
        ("fb7ff8000000000000", NonFiniteFloat),
        ("fb7ff0000000000000", NonFiniteFloat),
        ("fbfff0000000000000", NonFiniteFloat),
        ("fa7fc00000", NonFiniteFloat),
        ("a10000", NonTextKey),
        // Tags other than 4 and 1001.
        ("c000", UnsupportedTag(0)),
        ("c100", UnsupportedTag(1)),
        ("c240", UnsupportedTag(2)),
        ("c340", UnsupportedTag(3)),
        ("c500", UnsupportedTag(5)),
        ("d81800", UnsupportedTag(24)),
        ("d82000", UnsupportedTag(32)),
        ("d9d9f700", UnsupportedTag(55_799)),
        // Tag 4 shapes: not normalised, wrong shape, exponent past i64, bignum mantissa.
        ("c482000a", InvalidDecimal),
        ("c48201190258", InvalidDecimal),
        ("c4820100", InvalidDecimal),
        ("c48200f6", InvalidDecimal),
        ("c483000101", InvalidDecimal),
        ("c400", InvalidDecimal),
        ("c4821b800000000000000001", InvalidDecimal),
        ("c48200c249010000000000000000", InvalidDecimal),
        // Tag 1001 shapes: nanos 0 present, nanos 1e9, negative nanos, no secs, another key,
        // a repeated key, secs past i64, a text key, a float secs, not a map.
        ("d903e9a201002800", InvalidTimestamp),
        ("d903e9a2011a5f5e1000281a3b9aca00", InvalidTimestamp),
        ("d903e9a201002820", InvalidTimestamp),
        ("d903e9a128187b", InvalidTimestamp),
        ("d903e9a201000200", InvalidTimestamp),
        ("d903e9a201000101", InvalidTimestamp),
        ("d903e9a1011b8000000000000000", InvalidTimestamp),
        ("d903e9a161610a", InvalidTimestamp),
        ("d903e9a101fb3ff0000000000000", InvalidTimestamp),
        ("d903e9a0", InvalidTimestamp),
        ("d903e900", InvalidTimestamp),
        // Caught by the whole-input compare: long heads (int, text, bytes, array, map, tag),
        // f32, `undefined`, a timestamp's keys out of order.
        ("1805", NonCanonical { offset: 0 }),
        ("1817", NonCanonical { offset: 0 }),
        ("1900ff", NonCanonical { offset: 0 }),
        ("1b0000000000000001", NonCanonical { offset: 0 }),
        ("3800", NonCanonical { offset: 0 }),
        ("3817", NonCanonical { offset: 0 }),
        ("780161", NonCanonical { offset: 0 }),
        ("5800", NonCanonical { offset: 0 }),
        ("980100", NonCanonical { offset: 0 }),
        ("b801616101", NonCanonical { offset: 0 }),
        ("b90001616101", NonCanonical { offset: 0 }),
        ("d804820000", NonCanonical { offset: 0 }),
        ("da000003e9a10100", NonCanonical { offset: 0 }),
        ("fa3f800000", NonCanonical { offset: 0 }),
        ("f7", NonCanonical { offset: 0 }),
        ("d903e9a228187b0100", NonCanonical { offset: 4 }),
    ];
    for (hex, want) in rows {
        assert_eq!(decode(&h(hex)).as_ref(), Err(want), "{hex}");
    }
    // Not one well-formed item; hostile length heads fail fast, without allocating. The offset
    // is where the reader stopped, a diagnostic, so only the class is pinned.
    for hex in [
        "",
        "1a00",
        "62c328",
        "ff",
        "1c",
        "7bffffffffffffffff",
        "5bffffffffffffffff",
        "9bffffffffffffffff",
        "bbffffffffffffffff",
        "7a7fffffff61",
    ] {
        assert!(matches!(decode(&h(hex)), Err(Malformed { .. })), "{hex}");
    }
    // `long 7817` text: 23 bytes behind a 1-byte length head.
    assert_eq!(
        decode(&h(&format!("7817{}", "61".repeat(23)))),
        Err(NonCanonical { offset: 0 })
    );
}

/// Scenario: a client sends something outside the profile whose class the ADR does not name
/// (f16 floats, other simple values) or whose class varies with the library (every
/// indefinite-length shape, A1). The contract is "refused"; the class is not asserted.
#[test]
fn indefinite_lengths_and_other_simple_values_are_refused() {
    let rows = [
        // f16 (the pinned library cannot read it) and simple values other than f4/f5/f6.
        "f93c00",
        "f97e00",
        "f90000",
        "e0",
        "f0",
        "f818",
        "f820",
        "f8ff",
        "fc",
        "fd",
        "fe",
        // Indefinite text and bytes, top level and after the break.
        "5f4101ff",
        "5fff",
        "7f6161ff",
        "7fff",
        "7f61616162ff",
        "7f6161ff00",
        "7f6161ff6162",
        "7f6161ff7f6162ff",
        // As a map value, inside an indefinite map, as a key, nested.
        "a161617f6162ff",
        "a161615f4162ff",
        "a161617fff",
        "bf616101ff",
        "bfff",
        "a16161bf616201ff",
        "a16161bfff",
        "bf61617f6162ff",
        "bf61617f6162ffff",
        "bf61617f6162ff616301ff",
        "a261617f6162ff616301",
        "a17f6162ff01",
        "a27f6161ff01616202",
        "7f7f6161ffff",
        // Indefinite arrays (re-probed in W2).
        "9f00ff",
        "9fff",
        "a161619f00ff",
        "9f7f6161ffff",
        "9f7f6161ff",
        "9f0102ff",
        "82019f02ff",
        // Inside the two allowed tags.
        "c49f0001ff",
        "d903e9bf0100ff",
    ];
    for hex in rows {
        assert!(decode(&h(hex)).is_err(), "{hex} was accepted");
    }
    // Deep in a document: 63 maps, then an indefinite text.
    let deep = format!("{}7f6161ff", "a16161".repeat(63));
    assert!(decode(&h(&deep)).is_err());
}

/// Scenario: a client nests containers. 64 is allowed, 65 and past are `TooDeep`, from our
/// check (65–127) and from the library's guard (≥128) alike; `encode` refuses the same depth,
/// so nothing accepted can fail to read back (decision 5; tester W1 contract 2).
#[test]
fn depth_64_is_accepted_and_65_and_past_are_too_deep() {
    let deep = |n: usize| h(&format!("{}80", "81".repeat(n - 1)));
    assert_eq!(MAX_DEPTH, 64);
    let ok = decode(&deep(64)).expect("64 arrays");
    walk(&deep(64)).expect("walk at 64");
    assert_eq!(encode(&ok).expect("encodes"), deep(64));
    for n in [65, 127, 128, 200] {
        assert_eq!(decode(&deep(n)), Err(CodecError::TooDeep), "{n} arrays");
    }
    // Maps and arrays mixed count alike.
    let mixed64 = h(&format!("{}a1616180", "a1616181".repeat(31)));
    assert!(decode(&mixed64).is_ok(), "64 mixed levels");
    let mixed65 = h(&format!("{}80", "a1616181".repeat(32)));
    assert_eq!(decode(&mixed65), Err(CodecError::TooDeep));
    // The writer refuses what the reader refuses.
    assert_eq!(encode(&nested(65, Value::Null)), Err(EncodeError::TooDeep));
    assert_eq!(encode(&nested(64, Value::Null)).map(|b| b.len()), Ok(65));
}

/// Scenario: a decimal or timestamp sits at the deepest allowed level. They are scalars of the
/// data model, so their inner array or map does not count toward depth (tester W2 contract 3).
#[test]
fn decimal_and_timestamp_count_as_scalars_for_depth() {
    for scalar in [decimal(-2, 1234), timestamp(-1, 123)] {
        let doc = nested(64, scalar);
        let bytes = encode(&doc).expect("64 containers and a scalar");
        assert_eq!(decode(&bytes), Ok(doc.clone()));
        walk(&bytes).expect("walk");
        assert_eq!(ciborium_reads(&bytes), doc);
    }
}

/// Scenario: a client sends a document of exactly the limit, and one byte over (V13
/// "oversized"). Over the limit is refused before anything is decoded.
#[test]
fn the_size_limit_is_exact() {
    assert_eq!(MAX_PAYLOAD, 1_048_536);
    // A byte string: `5a` + 4-byte length + data.
    let at_limit = |n: usize| {
        let mut b = vec![0x5a];
        b.extend_from_slice(&u32::try_from(n).unwrap().to_be_bytes());
        b.resize(5 + n, 0x07);
        b
    };
    let max = at_limit(MAX_PAYLOAD - 5);
    assert_eq!(max.len(), MAX_PAYLOAD);
    let value = decode(&max).expect("exactly the limit");
    assert_eq!(encode(&value).map(|b| b.len()), Ok(MAX_PAYLOAD));
    let over = at_limit(MAX_PAYLOAD - 4);
    assert_eq!(decode(&over), Err(CodecError::TooLarge));
    // The writer refuses one byte over too, so it never writes what the reader refuses.
    assert_eq!(
        encode(&Value::Bytes(vec![0; MAX_PAYLOAD - 4])),
        Err(EncodeError::TooLarge)
    );
}

/// Scenario: a hostile client nests 1,000 and 500,000 tags. Refused, and the process survives
/// (tester W2 contract 2).
#[test]
fn deeply_nested_tags_are_refused_without_a_crash() {
    for n in [1_000, 500_000] {
        let mut bytes = vec![0xc1; n];
        bytes.push(0x00);
        assert!(decode(&bytes).is_err(), "{n} nested tags");
    }
}

/// Scenario: M9 decodes on its own runtime threads, and a Windows main thread has a 1 MiB stack,
/// half of libtest's 2 MiB (security lane, L-R185t). The deepest recursion the decoder allows
/// runs on a spawned 1 MiB thread: 64 levels accepted, 127 (the library builds the whole tree, then
/// our check refuses it), 128 and 200 (the library's step budget), 127 mixed map levels, 500,000
/// nested tags, and a 200-deep `Value` refused by `encode` and then dropped. A stack overflow
/// aborts the test binary, so this cannot pass by accident.
#[test]
fn worst_case_nesting_decodes_on_a_1_mib_stack() {
    let worker = std::thread::Builder::new()
        .name("decode-1mib".into())
        .stack_size(1 << 20)
        .spawn(|| {
            let deep = |n: usize| h(&format!("{}80", "81".repeat(n - 1)));
            assert!(decode(&deep(64)).is_ok(), "64 arrays");
            for n in [65, 127, 128, 200] {
                assert_eq!(decode(&deep(n)), Err(CodecError::TooDeep), "{n} arrays");
            }
            let mixed127 = h(&format!("{}80", "a1616181".repeat(63)));
            assert_eq!(decode(&mixed127), Err(CodecError::TooDeep), "127 mixed");
            let mut tags = vec![0xc1; 500_000];
            tags.push(0x00);
            assert!(decode(&tags).is_err(), "500,000 nested tags");
            let too_deep = nested(200, Value::Null);
            assert_eq!(encode(&too_deep), Err(EncodeError::TooDeep));
            drop(too_deep);
        })
        .expect("spawn a 1 MiB thread");
    worker.join().expect("decoding fits in a 1 MiB stack");
}

proptest! {
    /// Scenario: every document the encoder writes is canonical by a check written apart from
    /// it, is read back as itself by our decoder, and is read as the same value by a second,
    /// independent reader (ADR Verification: byte walk, second decoder, read-back). The
    /// generator nests at most 4 levels (`arb_value`); the depth limit itself is covered by
    /// `depth_64_is_accepted_and_65_and_past_are_too_deep`.
    #[test]
    fn every_encoded_document_walks_clean_and_both_readers_agree(doc in arb_value()) {
        let bytes = encode(&doc).expect("small documents encode");
        prop_assert_eq!(walk(&bytes), Ok(()));
        prop_assert_eq!(decode(&bytes), Ok(doc.clone()));
        prop_assert_eq!(ciborium_reads(&bytes), doc);
    }

    /// Scenario: two clients insert the same entries in opposite orders; the bytes are equal (L6).
    /// `Map` sorts its keys into encoded order on every insert, so insertion order is gone before
    /// `encode` runs; this checks the type keeps that promise and `encode` adds no order of its
    /// own. It cannot see an order the type has already erased; key order in bytes is pinned by
    /// `map_keys_are_ordered_by_length_then_bytes`.
    #[test]
    fn map_bytes_do_not_depend_on_insertion_order(
        entries in proptest::collection::vec(("\\PC{0,6}", arb_value()), 0..8)
    ) {
        let build = |items: &mut dyn Iterator<Item = &(String, Value)>| {
            let mut m = Map::new();
            for (k, v) in items {
                m.insert(MapKey::new(k.clone()), v.clone());
            }
            m
        };
        // Last writer wins per key in both directions; keep one entry per key first.
        let mut unique: Vec<(String, Value)> = Vec::new();
        for (k, v) in entries {
            if !unique.iter().any(|(seen, _)| *seen == k) {
                unique.push((k, v));
            }
        }
        let forward = build(&mut unique.iter());
        let backward = build(&mut unique.iter().rev());
        prop_assert_eq!(encode(&Value::Map(forward)), encode(&Value::Map(backward)));
    }

    /// Scenario: a client sends random bytes. The decoder never panics, and whatever it accepts
    /// is canonical by the independent walk and is exactly what the encoder writes.
    #[test]
    fn random_bytes_never_panic_and_acceptance_means_canonical(
        bytes in proptest::collection::vec(any::<u8>(), 0..48)
    ) {
        if let Ok(value) = decode(&bytes) {
            prop_assert_eq!(walk(&bytes), Ok(()));
            prop_assert_eq!(encode(&value), Ok(bytes));
        }
    }

    /// Scenario: a valid document is damaged in transit (one byte changed, inserted or
    /// removed). Never a panic; if the damage still decodes, it is canonical.
    #[test]
    fn mutated_documents_never_panic_and_acceptance_means_canonical(
        doc in arb_value(),
        at in any::<prop::sample::Index>(),
        byte in any::<u8>(),
        how in 0_u8..3,
    ) {
        let mut bytes = encode(&doc).expect("encodes");
        let i = at.index(bytes.len() + 1);
        match how {
            0 if i < bytes.len() => bytes[i] = byte,
            1 => bytes.insert(i, byte),
            _ if i < bytes.len() => { bytes.remove(i); }
            _ => bytes.push(byte),
        }
        if let Ok(value) = decode(&bytes) {
            prop_assert_eq!(walk(&bytes), Ok(()));
            prop_assert_eq!(encode(&value), Ok(bytes));
        }
    }
}
