//! M4 core rows: the v2 command envelope, `Compact` apply semantics, the compaction-aware
//! error set, and the watch types (test plan §3.1, §3.3, §3.10; ADR-0007 note, ADR-0019).
//!
//! These are the `config-core` half of M4. Everything here is synchronous and store-free: the
//! rows that need a real journal live in `config-storage`'s `m4_store*.rs`, and the ones that
//! need a cluster live in the workspace test crate. Splitting them that way is what lets a
//! failure here mean "the state machine is wrong" and never "the harness is flaky".

mod common;

use common::b;
use config_core::{
    Command, CommandResponse, ConfigError, DecodeError, KvState, Limits, MutationOutcome,
    StatusClass, WatchLimits, WatchRetention, COMMAND_ENVELOPE_VERSION, COMMAND_MAGIC, OP_COMPACT,
};

fn hex_of(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn put(state: &mut KvState, key: &[u8], value: &[u8]) -> u64 {
    let response = state.apply(&Command::Put {
        key: b(key),
        value: b(value),
        expected_mod_revision: None,
        dedup: None,
    });
    response.mutation().expect("a put applies").revision
}

/// `Compact { up_to_revision: 7 }` in full. 15 bytes: magic, version `02 00`, `op = 3`, and
/// the watermark as `u64` LE. No key length, no value, no `has_expected`.
const GOLDEN_COMPACT_7: &str = "52 43 4d 44 02 00 03 07 00 00 00 00 00 00 00 00";

/// M4-01 (core half) `compact_envelope_is_canonical`: the golden bytes of the new op.
///
/// A hex literal, like M0-40..M0-42, so a layout change has to edit a constant a reviewer can
/// see rather than quietly re-encoding every replicated maintenance command.
#[config_log::retcd_test]
fn m4_01_compact_envelope_golden_bytes() {
    let cmd = Command::Compact {
        up_to_revision: 7,
        dedup_trim_below: None,
    };

    let bytes = cmd.encode();

    assert_eq!(hex_of(&bytes), GOLDEN_COMPACT_7);
    // 16 from M5: `Compact` gained `dedup_trim_below`, encoded as the same one-byte presence
    // flag `has_expected` uses, and absent here (ADR-0025, lead ruling M5-R17).
    assert_eq!(bytes.len(), 16);
    assert_eq!(bytes.len(), cmd.encoded_len());
    assert_eq!(&bytes[0..4], &COMMAND_MAGIC);
    assert_eq!(bytes[4..6], COMMAND_ENVELOPE_VERSION.to_le_bytes());
    assert_eq!(bytes[6], OP_COMPACT);
    assert_eq!(Command::decode(&bytes).unwrap(), cmd);
}

/// The envelope stays canonical with the new op: trailing slack is an error, and a `Compact`
/// whose watermark field is short is truncated rather than defaulted to zero. A `Compact{0}`
/// silently invented out of a short buffer would be a no-op on one voter and a decode error on
/// another built with a stricter parser.
#[config_log::retcd_test]
fn m4_02_compact_envelope_admits_no_slack() {
    let mut bytes = Command::Compact {
        up_to_revision: 7,
        dedup_trim_below: None,
    }
    .encode();
    bytes.push(0);
    assert_eq!(
        Command::decode(&bytes),
        Err(DecodeError::TrailingBytes { extra: 1 })
    );

    let short = &Command::Compact {
        up_to_revision: 7,
        dedup_trim_below: None,
    }
    .encode()[..11];
    assert!(
        matches!(
            Command::decode(short),
            Err(DecodeError::Truncated {
                field: "up_to_revision",
                ..
            })
        ),
        "a short watermark is truncation, never an implied zero: {:?}",
        Command::decode(short)
    );
}

/// A version-1 build refuses a version-2 envelope.
///
/// The check is reproduced here rather than asserted through `Command::decode`, because
/// `Command::decode` *is* the v2 decoder — asking it about v2 bytes proves nothing about the
/// build being rolled over. `v1_decode` is the v1 decoder's first two steps verbatim (magic,
/// then `version == 1`), which is exactly where a v1 build parts company with these bytes.
/// Spec §17: upgrade every voter before emitting a v2 envelope.
#[config_log::retcd_test]
fn m4_03_v1_decoder_rejects_a_v2_envelope() {
    /// The v1 decoder's prologue: magic, then a hard equality on `version == 1`.
    fn v1_decode(buf: &[u8]) -> Result<(), String> {
        if buf.len() < 6 {
            return Err("truncated".to_string());
        }
        if buf[0..4] != COMMAND_MAGIC {
            return Err("bad magic".to_string());
        }
        let version = u16::from_le_bytes([buf[4], buf[5]]);
        if version != 1 {
            return Err(format!("unsupported command envelope version {version}"));
        }
        Ok(())
    }

    for cmd in [
        Command::Put {
            key: b(b"a"),
            value: b(b"b"),
            expected_mod_revision: None,
            dedup: None,
        },
        Command::Delete {
            key: b(b"a"),
            expected_mod_revision: None,
            dedup: None,
        },
        Command::Compact {
            up_to_revision: 7,
            dedup_trim_below: None,
        },
    ] {
        let bytes = cmd.encode();
        let err = v1_decode(&bytes)
            .expect_err("a v1 build must refuse every v2 envelope, including the unchanged ops");
        assert_eq!(err, "unsupported command envelope version 2");
    }

    // And the v2 decoder is equally strict the other way: no silent acceptance of v1 bytes,
    // whose op set is a *different* set.
    let mut v1_bytes = Command::Compact {
        up_to_revision: 7,
        dedup_trim_below: None,
    }
    .encode();
    v1_bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
    assert_eq!(
        Command::decode(&v1_bytes),
        Err(DecodeError::UnsupportedVersion(1))
    );
}

/// The v2 op set is closed. `5` is the next free op byte and must not decode into anything.
///
/// It was `4` through M4; M5 allocated 4 to `RetireNode` (ADR-0023, reserved by ADR-0019's
/// note), so the probe moved up rather than the rule changing.
#[config_log::retcd_test]
fn m4_04_v2_rejects_an_unknown_op() {
    let mut bytes = Command::Compact {
        up_to_revision: 7,
        dedup_trim_below: None,
    }
    .encode();
    bytes[6] = 5;

    assert_eq!(Command::decode(&bytes), Err(DecodeError::UnknownOp(5)));

    // Byte 0 too: an "absent op" is not a default op.
    bytes[6] = 0;
    assert_eq!(Command::decode(&bytes), Err(DecodeError::UnknownOp(0)));
}

/// M4-21/M4-22 (core half) `compact_advances_the_watermark_and_allocates_no_revision`.
///
/// The state-machine half of "the `Compact` entry is a log entry, not a public revision"
/// (spec §19.3). The journal deletion itself is storage's half of the same batch.
#[config_log::retcd_test]
fn m4_05_compact_allocates_no_revision_and_changes_no_record() {
    let mut state = KvState::new();
    for i in 0..5u8 {
        put(&mut state, &[b'k', b'0' + i], b"v");
    }
    assert_eq!(state.cluster_revision(), 5);
    assert_eq!(state.compact_revision(), 0, "nothing compacted yet");
    let hash_before = state.state_hash();

    let response = state.apply(&Command::Compact {
        up_to_revision: 3,
        dedup_trim_below: None,
    });

    assert_eq!(
        response,
        CommandResponse::Compacted {
            compact_revision: 3
        }
    );
    assert_eq!(state.compact_revision(), 3);
    assert_eq!(
        state.cluster_revision(),
        5,
        "compaction allocates no public revision"
    );
    assert_eq!(state.len(), 5, "compaction sheds history, never records");
    assert_eq!(
        state.state_hash(),
        hash_before,
        "the journal and the watermark are outside the divergence oracle (ruling R1)"
    );
    assert!(
        response.mutation().is_none() && response.event().is_none(),
        "a Compact is not a mutation and produces no event"
    );
    assert!(!response.is_applied());
}

/// M4-25 `compact_is_monotonic`: a watermark at or below the current one is a no-op that
/// still answers, with the *unchanged* watermark.
///
/// Answering rather than erroring is the load-bearing part. A `Compact` that errored on some
/// voters and no-opped on others is divergence, and a re-proposal after a failover is exactly
/// the situation that produces one.
#[config_log::retcd_test]
fn m4_06_compact_is_monotonic() {
    let mut state = KvState::new();
    for i in 0..5u8 {
        put(&mut state, &[b'k', b'0' + i], b"v");
    }
    state.apply(&Command::Compact {
        up_to_revision: 4,
        dedup_trim_below: None,
    });
    assert_eq!(state.compact_revision(), 4);

    for below in [0u64, 1, 3, 4] {
        let response = state.apply(&Command::Compact {
            up_to_revision: below,
            dedup_trim_below: None,
        });
        assert_eq!(
            response,
            CommandResponse::Compacted {
                compact_revision: 4
            },
            "Compact({below}) below the watermark must be an answering no-op"
        );
        assert_eq!(state.compact_revision(), 4);
    }

    assert_eq!(
        state.apply(&Command::Compact {
            up_to_revision: 5,
            dedup_trim_below: None,
        }),
        CommandResponse::Compacted {
            compact_revision: 5
        },
        "and it still advances when the watermark is genuinely higher"
    );
}

/// M4-26 `compact_above_applied_revision_clamped`: a watermark above applied state is clamped
/// to `cluster_revision`, not accepted and not rejected.
///
/// Accepting it would report a revision that has not happened yet as already compacted, so a
/// watch that arrived a moment later would be refused for history that still exists. Rejecting
/// it would fail an honest leader's proposal whenever a follower's applied revision lagged at
/// proposal time — and a command that fails on some voters is not deterministic.
#[config_log::retcd_test]
fn m4_07_compact_above_applied_revision_is_clamped() {
    let mut state = KvState::new();
    for i in 0..3u8 {
        put(&mut state, &[b'k', b'0' + i], b"v");
    }

    let response = state.apply(&Command::Compact {
        up_to_revision: 500,
        dedup_trim_below: None,
    });

    assert_eq!(
        response,
        CommandResponse::Compacted {
            compact_revision: 3
        },
        "clamped to cluster_revision"
    );
    assert_eq!(state.compact_revision(), 3);
    assert_eq!(state.cluster_revision(), 3);

    // The clamp is the same function of applied state on every voter, so a voter that later
    // catches up does not retroactively widen the watermark.
    put(&mut state, b"k9", b"v");
    assert_eq!(
        state.compact_revision(),
        3,
        "a later mutation does not re-apply the old proposal"
    );
}

/// An empty machine can be compacted. `Compact` on `cluster_revision == 0` clamps to 0, which
/// is the "nothing compacted" value — so it is a no-op, and a cursor at revision 1 stays
/// valid. OQ-27 says the check is `compact_revision > 0 && R <= compact_revision`; this is the
/// row that keeps `0` from ever meaning "everything is gone".
#[config_log::retcd_test]
fn m4_08_compact_on_an_empty_machine_is_a_noop() {
    let mut state = KvState::new();

    assert_eq!(
        state.apply(&Command::Compact {
            up_to_revision: 9,
            dedup_trim_below: None,
        }),
        CommandResponse::Compacted {
            compact_revision: 0
        }
    );
    assert_eq!(state.compact_revision(), 0);
    assert_eq!(state.cluster_revision(), 0);
}

/// The restore seam storage uses on open, and the seam the v1 -> v2 migration stamps through
/// (ADR-0021). Monotonic for the same reason apply is: a stale metadata read must not
/// resurrect history the store no longer holds.
#[config_log::retcd_test]
fn m4_09_restore_compact_revision_is_monotonic() {
    let mut state = KvState::new();
    state.restore_compact_revision(137);
    assert_eq!(state.compact_revision(), 137);

    state.restore_compact_revision(40);
    assert_eq!(
        state.compact_revision(),
        137,
        "a lower restored value is ignored, never applied"
    );

    state.restore_compact_revision(200);
    assert_eq!(state.compact_revision(), 200);
}

/// M4-27 (core half): `Compact` does not make apply consult anything outside its inputs.
///
/// The machine-checked source scan (M0-58) already forbids a clock in this crate. This row
/// asserts the behavioural consequence for the new command: the same sequence, applied to two
/// independent machines, produces the same watermark and the same responses.
#[config_log::retcd_test]
fn m4_10_compact_apply_is_deterministic() {
    let commands = [
        Command::Put {
            key: b(b"k0"),
            value: b(b"a"),
            expected_mod_revision: None,
            dedup: None,
        },
        Command::Compact {
            up_to_revision: 1,
            dedup_trim_below: None,
        },
        Command::Put {
            key: b(b"k1"),
            value: b(b"b"),
            expected_mod_revision: None,
            dedup: None,
        },
        Command::Compact {
            up_to_revision: 900,
            dedup_trim_below: None,
        },
        Command::Compact {
            up_to_revision: 1,
            dedup_trim_below: None,
        },
        Command::Delete {
            key: b(b"k0"),
            expected_mod_revision: None,
            dedup: None,
        },
    ];

    let mut a = KvState::new();
    let mut c = KvState::new();
    let responses_a: Vec<_> = commands.iter().map(|cmd| a.apply(cmd)).collect();
    let responses_c: Vec<_> = commands
        .iter()
        .map(|cmd| Command::decode(&cmd.encode()).expect("round trip"))
        .map(|cmd| c.apply(&cmd))
        .collect();

    assert_eq!(responses_a, responses_c);
    assert_eq!(a.state_hash(), c.state_hash());
    assert_eq!(a.compact_revision(), c.compact_revision());
    assert_eq!(
        a.compact_revision(),
        2,
        "clamped to cluster_revision at the time"
    );
}

/// The `Compact` variant does not disturb the accessors every other crate calls on a
/// `Command`. `key()` is infallible by design (it is only ever hexed into a log field), so the
/// honest answer for a keyless command is an empty key — never a panic and never a stand-in
/// key that would appear in `key_hex`.
#[config_log::retcd_test]
fn m4_11_compact_command_accessors() {
    let cmd = Command::Compact {
        up_to_revision: 7,
        dedup_trim_below: None,
    };

    assert!(cmd.key().is_empty());
    assert_eq!(cmd.expected_mod_revision(), None);
    assert_eq!(cmd.op_name(), "compact");
    assert_eq!(cmd.encoded_len(), 16);
}

/// A `Compact` passes apply-time validation regardless of the configured caps: it carries no
/// key and no value, and its 15 encoded bytes cannot exceed any sane request budget. A cap so
/// small that it rejected `Compact` would make a cluster unable to shed history.
#[config_log::retcd_test]
fn m4_12_compact_is_not_rejected_by_key_or_value_caps() {
    let tiny = Limits {
        max_key_bytes: 1,
        max_value_bytes: 1,
        max_request_bytes: 64,
        ..Limits::DEFAULT
    };
    let mut state = KvState::with_limits(tiny);
    state.apply(&Command::Put {
        key: b(b"k"),
        value: b(b"v"),
        expected_mod_revision: None,
        dedup: None,
    });

    assert_eq!(
        state.apply(&Command::Compact {
            up_to_revision: 1,
            dedup_trim_below: None,
        }),
        CommandResponse::Compacted {
            compact_revision: 1
        }
    );
}

/// M4-17 / spec §16: the compacted-cursor error names the *next usable* revision, not the
/// watermark, so a client can act on it without re-deriving the `+ 1`.
#[config_log::retcd_test]
fn m4_13_revision_compacted_reports_the_next_usable_revision() {
    let err = ConfigError::revision_compacted(137);

    assert_eq!(
        err,
        ConfigError::RevisionCompacted {
            minimum_available_revision: 138
        }
    );
    assert_eq!(err.kind(), StatusClass::OutOfRange);
    assert!(
        !err.is_safe_to_resubmit(),
        "the history is gone; resubmitting the same cursor fails identically forever"
    );
    assert!(
        err.to_string().contains("138"),
        "the operator-facing message must carry the actionable number: {err}"
    );

    // Saturating, because `compact_revision` is a u64 and an overflow here would silently
    // report `0` — "everything is available" — for the one case where nothing is.
    assert_eq!(
        ConfigError::revision_compacted(u64::MAX),
        ConfigError::RevisionCompacted {
            minimum_available_revision: u64::MAX
        }
    );
}

/// `ResourceExhausted` now distinguishes "shrink your request" from "reconnect at your
/// cursor" (ruling R9). Both are `RESOURCE_EXHAUSTED` on the wire; the difference rides in a
/// trailer, so it has to be a field rather than a second error variant.
#[config_log::retcd_test]
fn m4_14_resource_exhausted_carries_resumability() {
    let budget = ConfigError::resource_exhausted("value is 2 MiB, limit is 1 MiB");
    let overload = ConfigError::resource_exhausted_resumable("watch queue: 1024 events");

    assert_eq!(
        budget,
        ConfigError::ResourceExhausted {
            detail: "value is 2 MiB, limit is 1 MiB".to_string(),
            resumable: false,
        }
    );
    assert_eq!(
        overload,
        ConfigError::ResourceExhausted {
            detail: "watch queue: 1024 events".to_string(),
            resumable: true,
        }
    );
    assert_eq!(budget.kind(), StatusClass::ResourceExhausted);
    assert_eq!(overload.kind(), StatusClass::ResourceExhausted);

    // `resumable` is about a *stream*, `is_safe_to_resubmit` is about a *mutation*. Neither
    // implies the other, and conflating them would tell a client to replay a write.
    assert!(!budget.is_safe_to_resubmit());
    assert!(!overload.is_safe_to_resubmit());
    assert!(overload.to_string().contains("resumable"));
}

/// Spec §11.5 / §11.4 starting caps, spelled out in full so a typo in a `DEFAULT` cannot ship
/// green — the same contract M0-71 holds over `Limits`.
#[config_log::retcd_test]
fn m4_15_watch_limits_and_retention_defaults_match_spec() {
    assert_eq!(
        WatchLimits::DEFAULT,
        WatchLimits {
            max_streams_per_node: 1000,
            max_streams_per_principal: 100,
            queue_events: 1024,
            queue_bytes: 16 * 1024 * 1024,
            live_buffer_batches: 256,
        }
    );
    assert_eq!(Limits::DEFAULT.watch, WatchLimits::DEFAULT);

    assert_eq!(
        WatchRetention::DEFAULT,
        WatchRetention {
            max_age: std::time::Duration::from_secs(86_400),
            max_revisions: 10_000_000,
            max_bytes: 2 * 1024 * 1024 * 1024,
            check_interval: std::time::Duration::from_secs(60),
        }
    );
    assert_eq!(WatchRetention::default(), WatchRetention::DEFAULT);
}

/// A `Compact` interleaved with mutations neither skips nor re-uses a revision. This is the
/// state-machine precondition for "retained revisions are contiguous from `compact_revision+1`
/// to `cluster_revision`", which §3.8's `assert_journal_invariants` checks over a real store.
#[config_log::retcd_test]
fn m4_16_revisions_stay_contiguous_across_compaction() {
    let mut state = KvState::new();
    let mut revisions = Vec::new();

    for i in 0..4u8 {
        revisions.push(put(&mut state, &[b'k', b'0' + i], b"v"));
    }
    state.apply(&Command::Compact {
        up_to_revision: 2,
        dedup_trim_below: None,
    });
    for i in 4..8u8 {
        revisions.push(put(&mut state, &[b'k', b'0' + i], b"v"));
    }

    assert_eq!(revisions, (1..=8).collect::<Vec<u64>>());
    assert_eq!(state.compact_revision(), 2);
    assert_eq!(state.cluster_revision(), 8);

    // A conflicting mutation after a compaction still allocates nothing, so the retained range
    // stays exactly `3..=8`.
    let conflict = state.apply(&Command::Put {
        key: b(b"k0"),
        value: b(b"x"),
        expected_mod_revision: Some(99),
        dedup: None,
    });
    assert_eq!(
        conflict
            .mutation()
            .expect("a conflict is an outcome")
            .outcome,
        MutationOutcome::Conflict
    );
    assert_eq!(state.cluster_revision(), 8);
}
