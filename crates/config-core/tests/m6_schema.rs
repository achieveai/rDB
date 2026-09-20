//! M6-99, M6-104 — the schema gate's two pure rows (ADR-0030, spec §17).
//!
//! Both run without a node, a store or a network, because both are statements about *bytes
//! and source* rather than about a running cluster. M6-99 is the reason the whole gate exists:
//! if a schema-1 build could be made to decode a schema-2 envelope into a plausible value, the
//! propose-time gate would be an optimisation rather than a safety property.

mod common;

use config_core::{
    Command, DecodeError, DedupKey, NodeId, SchemaError, SchemaTriple, COMMAND_SCHEMA_V1,
    COMMAND_SCHEMA_V2, COMPAT_SCHEMA_1, CURRENT_SCHEMA, FEATURE_COMPACT, FEATURE_DEDUP,
    FEATURE_RETIRE_NODE,
};

use common::b;

/// The exact OpenRaft requirement this workspace is allowed to carry (A8, M5-48).
const OPENRAFT_PIN: &str = "openraft = { version = \"=0.9.25\"";

/// Every schema-2-only command shape, with its golden bytes and the feature it is gated by.
///
/// Hand-written from the layout in `config_core::command`'s module docs, exactly as M0-40 and
/// M5-74 are: a refactor that changes the encoding has to fail here rather than re-record
/// itself from the encoder it is supposed to be pinning.
fn v2_only_shapes() -> Vec<(&'static str, Command, String, &'static str)> {
    let stamp = DedupKey::new([0x22; 16], 7).stamp([0x11; 32]);
    // `has_expected = 0` still carries its eight zero revision bytes: the envelope is
    // fixed-shape there, and the canonical encoding admits no slack (M0-40).
    let no_guard = ["00"; 9].join(" ");
    let dedup_tail = format!(
        "{no_guard} 01 {} {} 07 00 00 00 00 00 00 00",
        vec!["11"; 32].join(" "),
        vec!["22"; 16].join(" "),
    );
    vec![
        (
            "compact with a trim watermark",
            Command::Compact {
                up_to_revision: 5,
                dedup_trim_below: Some(3),
            },
            "52 43 4d 44 02 00 03 05 00 00 00 00 00 00 00 01 03 00 00 00 00 00 00 00".to_string(),
            FEATURE_COMPACT,
        ),
        (
            "compact without a trim watermark",
            Command::Compact {
                up_to_revision: 5,
                dedup_trim_below: None,
            },
            "52 43 4d 44 02 00 03 05 00 00 00 00 00 00 00 00".to_string(),
            FEATURE_COMPACT,
        ),
        (
            "retire node",
            Command::RetireNode { node_id: NodeId(9) },
            "52 43 4d 44 02 00 04 09 00 00 00 00 00 00 00".to_string(),
            FEATURE_RETIRE_NODE,
        ),
        (
            "put carrying a dedup stamp",
            Command::Put {
                key: b(b"a"),
                value: b(b"b"),
                expected_mod_revision: None,
                dedup: Some(stamp),
            },
            format!("52 43 4d 44 02 00 01 01 00 00 00 61 01 00 00 00 62 {dedup_tail}"),
            FEATURE_DEDUP,
        ),
        (
            "delete carrying a dedup stamp",
            Command::Delete {
                key: b(b"a"),
                expected_mod_revision: None,
                dedup: Some(stamp),
            },
            format!("52 43 4d 44 02 00 02 01 00 00 00 61 {dedup_tail}"),
            FEATURE_DEDUP,
        ),
    ]
}

fn hex_of(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// M6-99 `v1_decoder_rejects_a_v2_envelope_on_golden_bytes`.
///
/// The "never a plausible-but-wrong v1 value" half is asserted directly: the schema-1 decode
/// must be an `Err`, and the *typed* one that names the missing feature — not a `Truncated`,
/// not an `UnknownOp`, and above all not an `Ok` carrying a command whose dedup group or trim
/// watermark silently vanished.
#[config_log::retcd_test]
fn m6_99_v1_decoder_rejects_a_v2_envelope_on_golden_bytes() {
    for (what, cmd, golden, feature) in v2_only_shapes() {
        let bytes = cmd.encode();
        assert_eq!(hex_of(&bytes), golden, "golden bytes drifted for {what}");

        let refusal = match COMPAT_SCHEMA_1.decode_command(&bytes) {
            Err(refusal) => refusal,
            Ok(decoded) => panic!("a schema-1 build must refuse {what}, it decoded {decoded:?}"),
        };
        assert_eq!(
            refusal,
            SchemaError::CommandTooNew {
                feature,
                required: COMMAND_SCHEMA_V2,
                supported: COMMAND_SCHEMA_V1,
            },
            "the refusal for {what} must name the feature and both levels"
        );

        // The bytes are not malformed — they are exactly what the build that wrote them meant.
        // Asserting the round trip here is what makes the refusal above a *policy* rather than
        // a parse failure that would have happened anyway.
        assert_eq!(
            CURRENT_SCHEMA.decode_command(&bytes).expect("own bytes"),
            cmd,
            "{what} must round-trip under the schema that produced it"
        );
    }
}

/// M6-99, companion: the shapes a schema-1 build *must* keep accepting.
///
/// Without this the row would pass for a decoder that refuses everything, which would stop a
/// rolling upgrade writing anything at all (the failure mode M6-95 exists to catch).
#[config_log::retcd_test]
fn m6_99b_v1_decoder_still_accepts_every_schema_1_shape() {
    let shapes = [
        Command::Put {
            key: b(b"a"),
            value: b(b"b"),
            expected_mod_revision: None,
            dedup: None,
        },
        Command::Put {
            key: b(b"a"),
            value: b(b"b"),
            expected_mod_revision: Some(7),
            dedup: None,
        },
        Command::Delete {
            key: b(b"a"),
            expected_mod_revision: Some(7),
            dedup: None,
        },
    ];
    for cmd in shapes {
        assert_eq!(
            COMPAT_SCHEMA_1
                .decode_command(&cmd.encode())
                .expect("a schema-1 shape"),
            cmd
        );
    }
}

/// M6-99, companion: a corrupt envelope is still a *decode* error, not a gate refusal.
///
/// The two failures are reported differently on purpose — one is an operator's upgrade
/// problem, the other is a corruption problem — so a change that collapsed them into one
/// error would hide the difference exactly when it matters.
#[config_log::retcd_test]
fn m6_99c_a_malformed_envelope_is_not_reported_as_a_version_problem() {
    let err = COMPAT_SCHEMA_1
        .decode_command(b"not an envelope at all")
        .expect_err("garbage");
    assert!(
        matches!(err, SchemaError::Decode(DecodeError::BadMagic { .. })),
        "expected a decode error, got {err:?}"
    );
}

/// M6-99, companion: an unknown *higher* schema unlocks nothing (the M6-111 half that is
/// provable in the core).
///
/// A peer advertising a future schema must be treated as "not compatible with me", never as
/// permission. The assertion is that a future triple does not change what *this* build admits.
#[config_log::retcd_test]
fn m6_99d_an_unknown_future_schema_is_not_taken_as_permission() {
    let future = SchemaTriple {
        format_version: 99,
        command_schema: 99,
        proto_rev: 99,
    };
    assert!(future > CURRENT_SCHEMA);
    // The minimum over {this build, a future peer} is still this build: a newer peer can never
    // raise the level the gate runs at.
    assert_eq!(CURRENT_SCHEMA.min(future), CURRENT_SCHEMA);
    assert_eq!(COMPAT_SCHEMA_1.min(future), COMPAT_SCHEMA_1);
}

/// M6-104 `openraft_wire_floor_is_pinned_and_documented`.
///
/// Re-asserts M5-48's pin at the M6 gate and adds the part that makes it a *policy*: §17's
/// "upgrade OpenRaft only after staging tests cover mixed versions" is discharged by declaring
/// one supported version, and a discharge that is not written down is an assumption.
#[config_log::retcd_test]
fn m6_104_openraft_wire_floor_is_pinned_and_documented() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/config-core sits two levels below the workspace root")
        .to_path_buf();

    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).expect("workspace manifest");
    assert!(
        manifest.contains(OPENRAFT_PIN),
        "the workspace manifest must pin openraft exactly: {OPENRAFT_PIN}"
    );

    // `=0.9.25` in the manifest only *requests* one version; the lock file is what proves the
    // build resolved to it, and that exactly one openraft is in the graph.
    let lock = std::fs::read_to_string(root.join("Cargo.lock")).expect("workspace lock");
    let resolved: Vec<&str> = lock
        .split("[[package]]")
        .filter(|block| block.contains("name = \"openraft\""))
        .collect();
    assert_eq!(
        resolved.len(),
        1,
        "exactly one openraft must be in the dependency graph; rEtcd runs no mixed-openraft \
         configuration"
    );
    assert!(
        resolved[0].contains("version = \"0.9.25\""),
        "the lock file must resolve openraft to 0.9.25, found: {}",
        resolved[0].trim()
    );

    // The written-down half. ADR-0030 is the home of the statement rather than the README:
    // the README is not a dev-compat artifact (see the row's annotation in the M6 test plan).
    let adr = std::fs::read_to_string(root.join("docs/ADRs/0030-mixed-version-gating.md"))
        .expect("ADR-0030");
    assert!(
        adr.contains("=0.9.25"),
        "ADR-0030 must name the exact pinned version"
    );
    assert!(
        adr.contains("no claim"),
        "ADR-0030 must state that rEtcd makes no claim about any older 0.9.x"
    );
}
