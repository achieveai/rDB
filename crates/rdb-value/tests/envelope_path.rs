//! The object envelope and JSON Pointer paths, at the library API (ADR-rdb-0012 decisions 7,
//! 10; tester W1 contracts 3–4, W2 contracts 4–5).

mod common;

use common::{h, int, map, text};
use rdb_value::delta::{resolve, ApplyError};
use rdb_value::envelope::{
    open, seal, EnvelopeError, Kind, OversizedPayload, HEADER_LEN, MAX_ENVELOPE, MAX_PAYLOAD,
};
use rdb_value::path::{Path, PathError, MAX_SEGMENTS};
use rdb_value::value::Value;

/// Scenario: the primary seals a payload and later opens it. The header is the documented 40
/// bytes, and the payload comes back unchanged with its digest checked.
#[test]
fn seal_writes_the_documented_header_and_open_reads_it_back() {
    let payload = h("a2646e616d65636164616676697369747300");
    let sealed = seal(Kind::Document, &payload).expect("seals");
    assert_eq!(sealed.len(), HEADER_LEN + payload.len());
    assert_eq!(hex::encode(&sealed[..8]), "0101010100000012");
    assert_eq!(
        hex::encode(&sealed[8..HEADER_LEN]),
        "83b192c67d90cd32fe30cd7bf6bab13a7d7a49fee08d07cbc17b4a9a4fe96b26"
    );
    let opened = open(&sealed).expect("opens");
    assert_eq!(opened.kind, Kind::Document);
    assert_eq!(opened.payload, payload.as_slice());
    assert_eq!(opened.digest.as_slice(), &sealed[8..HEADER_LEN]);
}

/// Scenario: a stored record is damaged or written by a newer build. Every unknown header
/// byte, a length mismatch either way and a digest mismatch is named; nothing is guessed
/// (decision 7, fail closed; V12).
#[test]
fn open_fails_closed_on_every_damaged_header_field() {
    use EnvelopeError::*;
    let good = seal(Kind::Document, &h("a0")).expect("seals").to_vec();
    let with = |at: usize, byte: u8| {
        let mut b = good.clone();
        b[at] = byte;
        b
    };
    assert_eq!(open(&[]), Err(Truncated { len: 0 }));
    assert_eq!(open(&good[..39]), Err(Truncated { len: 39 }));
    for byte in [0x00, 0x02, 0xff] {
        assert_eq!(open(&with(0, byte)), Err(UnknownFormat(byte)));
        assert_eq!(open(&with(1, byte)), Err(UnknownKind(byte)));
        assert_eq!(open(&with(2, byte)), Err(UnknownCodec(byte)));
        assert_eq!(open(&with(3, byte)), Err(UnknownDigest(byte)));
    }
    // payload_len says 0 and 2 against 1 byte; then one extra byte after the payload.
    assert_eq!(
        open(&with(7, 0)),
        Err(LengthMismatch {
            declared: 0,
            actual: 1
        })
    );
    assert_eq!(
        open(&with(7, 2)),
        Err(LengthMismatch {
            declared: 2,
            actual: 1
        })
    );
    let mut longer = good.clone();
    longer.push(0x00);
    assert_eq!(
        open(&longer),
        Err(LengthMismatch {
            declared: 1,
            actual: 2
        })
    );
    // A flipped payload byte, and an all-zero digest.
    assert_eq!(open(&with(HEADER_LEN, 0xa1)), Err(DigestMismatch));
    let mut zero = good.clone();
    zero[8..HEADER_LEN].fill(0);
    assert_eq!(open(&zero), Err(DigestMismatch));
    // Over the limit is refused before the header is read.
    assert_eq!(
        open(&vec![0; MAX_ENVELOPE + 1]),
        Err(TooLarge {
            len: MAX_ENVELOPE + 1
        })
    );
}

/// Scenario: the largest payload seals to exactly 1 MiB; one byte more is refused.
#[test]
fn seal_accepts_exactly_the_limit() {
    let sealed = seal(Kind::Document, &vec![0; MAX_PAYLOAD]).expect("the limit");
    assert_eq!(sealed.len(), MAX_ENVELOPE);
    assert!(open(&sealed).is_ok());
    assert_eq!(
        seal(Kind::Document, &vec![0; MAX_PAYLOAD + 1]),
        Err(OversizedPayload {
            len: MAX_PAYLOAD + 1
        })
    );
}

/// Scenario: a caller names a place in a document with RFC 6901. Escapes decode, `/` is the
/// empty key, `""` is refused (ops take a path below the root), and 64 segments is the limit.
#[test]
fn json_pointer_parses_per_rfc_6901_with_the_profile_limits() {
    let segments = |text: &str| Path::parse(text).map(|p| p.segments().to_vec());
    assert_eq!(segments("/a/b"), Ok(vec!["a".to_owned(), "b".to_owned()]));
    assert_eq!(segments("/a~1b"), Ok(vec!["a/b".to_owned()]));
    assert_eq!(segments("/a~0b"), Ok(vec!["a~b".to_owned()]));
    assert_eq!(segments("/~01"), Ok(vec!["~1".to_owned()]));
    assert_eq!(segments("/"), Ok(vec![String::new()]));
    assert_eq!(segments("//"), Ok(vec![String::new(), String::new()]));
    assert_eq!(segments(""), Err(PathError::RootNotAllowed));
    assert_eq!(segments("nolead"), Err(PathError::PathSyntax { pos: 0 }));
    assert_eq!(segments("/a~2"), Err(PathError::PathSyntax { pos: 2 }));
    assert_eq!(segments("/a~"), Err(PathError::PathSyntax { pos: 2 }));
    assert_eq!(MAX_SEGMENTS, 64);
    assert_eq!(segments(&"/a".repeat(64)).map(|s| s.len()), Ok(64));
    assert_eq!(segments(&"/a".repeat(65)), Err(PathError::PathTooLong));
    // Display escapes back.
    let path = Path::parse("/a~1b/~0").expect("parses");
    assert_eq!(path.to_string(), "/a~1b/~0");
}

/// Scenario: a caller indexes an array. Only `0` or `[1-9][0-9]*` below the length is an
/// index; the error names the length (tester W2 contract 5, L-R185o 3a).
#[test]
fn array_index_rules() {
    let doc = map(&[("t", Value::Array(vec![text("a"), text("b"), text("c")]))]);
    let get = |p: &str| resolve(&doc, &Path::parse(p).expect("parses")).cloned();
    assert_eq!(get("/t/0"), Ok(text("a")));
    assert_eq!(get("/t/2"), Ok(text("c")));
    for bad in [
        "3",
        "-",
        "01",
        "00",
        "-1",
        "+1",
        "1e0",
        "",
        "99999999999999999999999",
        "18446744073709551615",
    ] {
        assert_eq!(
            get(&format!("/t/{bad}")),
            Err(ApplyError::IndexInvalid {
                segment: bad.to_owned(),
                len: 3
            }),
            "{bad}"
        );
    }
    // A map key that looks like an index is still a key.
    let keyed = map(&[("0", int(7))]);
    assert_eq!(resolve(&keyed, &Path::parse("/0").unwrap()), Ok(&int(7)));
}
