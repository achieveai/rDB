//! M0-40..M0-51 — the canonical command envelope (ADR-0007).
//!
//! The golden-byte rows are the contract: a future refactor that changes the layout has to
//! change a hex literal here, which is exactly the kind of change a reviewer notices. The
//! malformed-input rows exist because a decoder that trusts a length field is a remote
//! allocation primitive, not a parser.

mod common;

use common::b;
use config_core::{Command, DecodeError, COMMAND_ENVELOPE_VERSION, COMMAND_MAGIC};
use proptest::prelude::*;

fn hex_of(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// `Put { key: b"a", value: b"b", expected: None, dedup: None }` — 83 bytes: 26 as in
/// envelope v1 plus the 57-byte dedup group M5 appended (ADR-0025).
const GOLDEN_PUT_NO_EXPECTED: &str =
    "52 43 4d 44 02 00 01 01 00 00 00 61 01 00 00 00 62 00 00 00 00 00 00 00 00 00 00";

/// Same command with `has_expected = 1` and `expected = 7`.
const GOLDEN_PUT_EXPECTED_7: &str =
    "52 43 4d 44 02 00 01 01 00 00 00 61 01 00 00 00 62 01 07 00 00 00 00 00 00 00 00";

/// `Delete { key: b"a", expected: Some(7) }` — 22 bytes, per the OQ-1 decision recorded in
/// ADR-0007 Clarifications.
const GOLDEN_DELETE_EXPECTED_7: &str =
    "52 43 4d 44 02 00 02 01 00 00 00 61 01 07 00 00 00 00 00 00 00 00";

#[config_log::retcd_test]
fn m0_40_encode_golden_put_no_expected() {
    let cmd = Command::Put {
        key: b(b"a"),
        value: b(b"b"),
        expected_mod_revision: None,
        dedup: None,
    };

    let bytes = cmd.encode();

    assert_eq!(hex_of(&bytes), GOLDEN_PUT_NO_EXPECTED);
    assert_eq!(bytes.len(), 27);
    assert_eq!(bytes.len(), cmd.encoded_len());
    assert_eq!(&bytes[0..4], &COMMAND_MAGIC);
    assert_eq!(Command::decode(&bytes).unwrap(), cmd);
}

#[config_log::retcd_test]
fn m0_41_encode_golden_put_expected_7() {
    let cmd = Command::Put {
        key: b(b"a"),
        value: b(b"b"),
        expected_mod_revision: Some(7),
        dedup: None,
    };

    let bytes = cmd.encode();

    assert_eq!(hex_of(&bytes), GOLDEN_PUT_EXPECTED_7);
    assert_eq!(
        bytes.len(),
        27,
        "the expected revision field is present either way"
    );
    assert_eq!(Command::decode(&bytes).unwrap(), cmd);
}

/// OQ-1 decided that a Delete omits the value length field *entirely* rather than writing a
/// zero. The rejected alternative is asserted alongside so the decision is visible.
#[config_log::retcd_test]
fn m0_42_encode_golden_delete() {
    let cmd = Command::Delete {
        key: b(b"a"),
        expected_mod_revision: Some(7),
        dedup: None,
    };

    let bytes = cmd.encode();

    assert_eq!(hex_of(&bytes), GOLDEN_DELETE_EXPECTED_7);
    assert_eq!(bytes.len(), 22);
    assert_eq!(Command::decode(&bytes).unwrap(), cmd);

    // The rejected candidate: a `00 00 00 00` value length between the key and has_expected.
    let rejected = "52 43 4d 44 02 00 02 01 00 00 00 61 00 00 00 00 01 07 00 00 00 00 00 00 00 00";
    assert_ne!(
        hex_of(&bytes),
        rejected,
        "the zero-length-field encoding was rejected"
    );
    let rejected_bytes: Vec<u8> = rejected
        .split(' ')
        .map(|h| u8::from_str_radix(h, 16).unwrap())
        .collect();
    assert!(
        Command::decode(&rejected_bytes).is_err(),
        "the rejected encoding must not also decode, or the format would not be canonical"
    );
}

#[config_log::retcd_test]
fn m0_43_decode_rejects_bad_magic() {
    let mut bytes = Command::Put {
        key: b(b"a"),
        value: b(b"b"),
        expected_mod_revision: None,
        dedup: None,
    }
    .encode();
    bytes[0] = b'X';

    let err = Command::decode(&bytes).expect_err("foreign bytes must not decode");

    assert_eq!(err, DecodeError::BadMagic { got: *b"XCMD" });
}

#[config_log::retcd_test]
fn m0_44_decode_rejects_unknown_version() {
    let mut bytes = Command::Delete {
        key: b(b"a"),
        expected_mod_revision: None,
        dedup: None,
    }
    .encode();
    bytes[4..6].copy_from_slice(&3u16.to_le_bytes());

    let err = Command::decode(&bytes).expect_err("a future version must not be guessed at");

    assert_eq!(err, DecodeError::UnsupportedVersion(3));
    assert_eq!(COMMAND_ENVELOPE_VERSION, 2);
}

#[config_log::retcd_test]
fn m0_45_decode_rejects_unknown_op() {
    let mut bytes = Command::Delete {
        key: b(b"a"),
        expected_mod_revision: None,
        dedup: None,
    }
    .encode();
    bytes[6] = 0x05;

    let err = Command::decode(&bytes).expect_err("an unknown op must not decode");

    assert_eq!(err, DecodeError::UnknownOp(5));
}

/// A canonical encoding admits no slack in either direction: every short prefix is an error,
/// and so is a single trailing byte.
#[config_log::retcd_test]
fn m0_46_decode_rejects_truncated_and_overlong() {
    let cmd = Command::Put {
        key: b(b"abc"),
        value: b(b"defg"),
        expected_mod_revision: Some(11),
        dedup: None,
    };
    let bytes = cmd.encode();

    for len in 0..bytes.len() {
        assert!(
            Command::decode(&bytes[..len]).is_err(),
            "prefix of length {len} must not decode"
        );
    }
    assert_eq!(Command::decode(&bytes).unwrap(), cmd);

    let mut overlong = bytes.clone();
    overlong.push(0x00);
    assert_eq!(
        Command::decode(&overlong),
        Err(DecodeError::TrailingBytes { extra: 1 })
    );
}

/// A length field is untrusted input. `u32::MAX` must be checked against what remains before
/// anything is allocated.
#[config_log::retcd_test]
fn m0_47_decode_rejects_length_overflow() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&COMMAND_MAGIC);
    bytes.extend_from_slice(&COMMAND_ENVELOPE_VERSION.to_le_bytes());
    bytes.push(1); // Put
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 9]);
    assert_eq!(bytes.len(), 20);

    let err = Command::decode(&bytes).expect_err("a 4 GiB key length in a 20-byte buffer");

    assert!(
        matches!(err, DecodeError::Truncated { needed, .. } if needed == u32::MAX as usize),
        "the length is rejected as unsatisfiable, not allocated: {err:?}"
    );
}

/// Same logical command, two constructions: the bytes must be identical, so "same command
/// sequence" and "same bytes" mean the same thing on every voter.
#[config_log::retcd_test]
fn m0_50_encode_is_canonical_and_stable() {
    let owned = Command::Put {
        key: bytes::Bytes::from(b"key".to_vec()),
        value: bytes::Bytes::from(String::from("value").into_bytes()),
        expected_mod_revision: Some(42),
        dedup: None,
    };
    let sliced = Command::Put {
        key: bytes::Bytes::from_static(b"__key__").slice(2..5),
        value: bytes::Bytes::copy_from_slice(b"value"),
        expected_mod_revision: Some(42),
        dedup: None,
    };

    assert_eq!(owned, sliced);
    assert_eq!(owned.encode(), sliced.encode());
    // Stability across runs is frozen by the golden constants above rather than by spawning a
    // second process: the encoding reads nothing outside the command, so a differing run
    // would have to differ from the literal.
    assert_eq!(
        hex_of(
            &Command::Delete {
                key: b(b"a"),
                expected_mod_revision: Some(7),
                dedup: None,
            }
            .encode()
        ),
        GOLDEN_DELETE_EXPECTED_7
    );
}

/// The serde derive exists only because OpenRaft requires it on `D`/`R`. It is *not* the
/// canonical form, and this test fails if someone ever swaps one for the other.
#[config_log::retcd_test]
fn m0_51_serde_is_not_the_canonical_form() {
    let cmd = Command::Delete {
        key: b(b"a"),
        expected_mod_revision: Some(7),
        dedup: None,
    };

    let canonical = cmd.encode();
    let json = serde_json::to_vec(&cmd).expect("serde derive is present for OpenRaft");

    assert_ne!(canonical, json);
    assert_eq!(hex_of(&canonical), GOLDEN_DELETE_EXPECTED_7);
    assert!(
        Command::decode(&json).is_err(),
        "the serde form must not be accepted as a log entry"
    );
    assert_eq!(
        serde_json::from_slice::<Command>(&json).unwrap(),
        cmd,
        "serde still round-trips structurally, it is simply not the wire contract"
    );
}

fn command_strategy() -> impl Strategy<Value = Command> {
    let expected = prop_oneof![
        Just(None),
        (0u64..u64::MAX).prop_map(Some),
        Just(Some(u64::MAX)),
    ];
    prop_oneof![
        (
            prop::collection::vec(any::<u8>(), 0..1024),
            prop::collection::vec(any::<u8>(), 0..4096),
            expected.clone(),
        )
            .prop_map(|(key, value, expected_mod_revision)| Command::Put {
                key: bytes::Bytes::from(key),
                value: bytes::Bytes::from(value),
                expected_mod_revision,
                dedup: None,
            }),
        (prop::collection::vec(any::<u8>(), 0..1024), expected).prop_map(
            |(key, expected_mod_revision)| Command::Delete {
                key: bytes::Bytes::from(key),
                expected_mod_revision,
                dedup: None,
            }
        ),
    ]
}

#[config_log::retcd_test]
fn m0_48_encode_decode_roundtrip_proptest() {
    proptest!(|(cmd in command_strategy())| {
        let bytes = cmd.encode();
        prop_assert_eq!(bytes.len(), cmd.encoded_len());
        prop_assert_eq!(Command::decode(&bytes).unwrap(), cmd);
    });
}

/// Decoding must be total on arbitrary input, and on mutations of valid input — the shapes a
/// real attacker sends. It returns `Ok` or a typed error, and never panics.
#[config_log::retcd_test]
fn m0_49_decode_fuzz_never_panics() {
    proptest!(|(raw in prop::collection::vec(any::<u8>(), 0..4096))| {
        let _ = Command::decode(&raw);
    });

    proptest!(|(cmd in command_strategy(), index in 0usize..64, mask in 1u8..=255)| {
        let mut bytes = cmd.encode();

        let flip = index % bytes.len();
        bytes[flip] ^= mask;
        let _ = Command::decode(&bytes);

        let cut = index % bytes.len();
        let _ = Command::decode(&bytes[..cut]);

        // Tamper with the key length field directly; the decoder must not trust it.
        let mut tampered = cmd.encode();
        tampered[7..11].copy_from_slice(&u32::MAX.to_le_bytes());
        let _ = Command::decode(&tampered);
    });
}

/// `has_expected = 0` with a non-zero revision would give one logical command two encodings.
/// Rejecting it is what makes byte equality a sound determinism oracle.
#[config_log::retcd_test]
fn non_canonical_expected_is_rejected() {
    let mut bytes = Command::Delete {
        key: b(b"a"),
        expected_mod_revision: None,
        dedup: None,
    }
    .encode();
    // The expected revision sits ahead of the absent dedup group's single flag byte.
    let tail = bytes.len() - 8 - 1;
    bytes[tail..tail + 8].copy_from_slice(&9u64.to_le_bytes());

    assert_eq!(
        Command::decode(&bytes),
        Err(DecodeError::NonCanonicalExpected(9))
    );

    let mut bad_flag = Command::Delete {
        key: b(b"a"),
        expected_mod_revision: None,
        dedup: None,
    }
    .encode();
    bad_flag[12] = 2;
    assert_eq!(
        Command::decode(&bad_flag),
        Err(DecodeError::InvalidHasExpected(2))
    );
}
