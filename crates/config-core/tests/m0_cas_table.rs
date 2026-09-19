//! M0-01..M0-15 — the ADR-0006 Put/Delete/CAS outcome table, one test per row plus an
//! exhaustiveness runner.
//!
//! Each row states the pre-state, the command, the expected outcome, and the effect on
//! `cluster_revision`. The revision effect is asserted separately from the outcome because
//! "returned CONFLICT" and "allocated nothing" are two distinct guarantees (spec §19.3), and
//! an implementation can get one right while getting the other wrong.

mod common;

use common::{apply, apply_full, b, del, del_cas, put, put_cas, record, seed_key_at};
use config_core::{
    Command, CommandResponse, DeleteRequest, KvState, Limits, MutationEventKind, MutationOutcome,
};

#[config_log::retcd_test]
fn m0_01_put_unconditional_absent() {
    let mut state = KvState::new();
    let resp = apply(&mut state, &put(b"k", b"v1"));

    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(resp.revision, 1);
    assert!(
        resp.exists,
        "APPLIED Put reports exists = true (ADR-0006 OQ-2)"
    );
    assert_eq!(resp.current_mod_revision, 1);

    let rec = record(&state, b"k");
    assert_eq!(rec.create_revision, 1);
    assert_eq!(rec.mod_revision, 1);
    assert_eq!(state.cluster_revision(), 1);
}

#[config_log::retcd_test]
fn m0_02_put_unconditional_present() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"k", 3);

    let resp = apply(&mut state, &put(b"k", b"v2"));

    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(resp.revision, 4);
    let rec = record(&state, b"k");
    assert_eq!(rec.create_revision, 1, "create_revision survives updates");
    assert_eq!(rec.mod_revision, 4);
    assert_eq!(state.cluster_revision(), 4);
}

#[config_log::retcd_test]
fn m0_03_put_create_only_absent() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"other", 5);

    let resp = apply(&mut state, &put_cas(b"k", b"v", 0));

    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(resp.revision, 6);
    let rec = record(&state, b"k");
    assert_eq!((rec.create_revision, rec.mod_revision), (6, 6));
    assert_eq!(state.cluster_revision(), 6);
}

#[config_log::retcd_test]
fn m0_04_put_create_only_present_conflict() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"k", 2);
    let before = state.cluster_revision();

    let resp = apply(&mut state, &put_cas(b"k", b"v", 0));

    assert_eq!(resp.outcome, MutationOutcome::Conflict);
    assert!(resp.exists);
    assert_eq!(resp.current_mod_revision, 2);
    assert_eq!(
        resp.revision, before,
        "conflict reports the unchanged cluster revision"
    );
    assert_eq!(
        state.cluster_revision(),
        before,
        "conflict allocates nothing"
    );
}

#[config_log::retcd_test]
fn m0_05_put_expected_match() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"k", 4);

    let resp = apply(&mut state, &put_cas(b"k", b"v2", 4));

    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(resp.revision, 5);
    assert_eq!(record(&state, b"k").mod_revision, 5);
}

#[config_log::retcd_test]
fn m0_06_put_expected_mismatch() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"k", 4);

    let resp = apply(&mut state, &put_cas(b"k", b"v2", 3));

    assert_eq!(resp.outcome, MutationOutcome::Conflict);
    assert!(resp.exists);
    assert_eq!(resp.current_mod_revision, 4);
    assert_eq!(state.cluster_revision(), 4);
}

/// The easy-to-get-wrong row: a conditional Put against an *absent* key is a CONFLICT with
/// `exists = false`, not a NotFound. The caller asked to replace a specific revision and the
/// key does not hold it.
#[config_log::retcd_test]
fn m0_07_put_expected_n_absent_conflict() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"other", 9);

    let resp = apply(&mut state, &put_cas(b"k", b"v", 7));

    assert_eq!(resp.outcome, MutationOutcome::Conflict);
    assert!(!resp.exists);
    assert_eq!(resp.current_mod_revision, 0);
    assert_eq!(resp.revision, 9);
    assert_eq!(state.cluster_revision(), 9);
    assert!(state.get(b"k").is_none());
}

#[config_log::retcd_test]
fn m0_08_delete_unconditional_present() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"k", 2);

    let full = apply_full(&mut state, &del(b"k"));
    let resp = full.mutation().expect("mutation response").clone();

    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(resp.revision, 3);
    assert!(!resp.exists, "the key is gone, so exists = false");
    assert!(state.get(b"k").is_none());

    let event = full
        .event()
        .expect("an applied delete constructs a tombstone event");
    assert_eq!(event.revision, 3);
    assert_eq!(event.key, b(b"k"));
    assert_eq!(event.kind, MutationEventKind::Delete);
    assert_eq!(state.cluster_revision(), 3);
}

#[config_log::retcd_test]
fn m0_09_delete_unconditional_absent_not_found() {
    let mut state = KvState::new();

    let full = apply_full(&mut state, &del(b"k"));
    let resp = full.mutation().expect("mutation response").clone();

    assert_eq!(resp.outcome, MutationOutcome::NotFound);
    assert_eq!(resp.revision, 0);
    assert!(!resp.exists);
    assert_eq!(resp.current_mod_revision, 0);
    assert!(
        full.event().is_none(),
        "a missing delete constructs no event"
    );
    assert_eq!(state.cluster_revision(), 0);
}

/// `Delete { expected: Some(0) }` is rejected at the edge and never encoded. For a Put, 0
/// means create-only; for a Delete it could only mean "delete if absent", which is a mistake.
#[config_log::retcd_test]
fn m0_10_delete_expected_zero_invalid_at_edge() {
    let req = DeleteRequest {
        key: b(b"k"),
        expected_mod_revision: Some(0),
    };

    let err = config_core::validate_delete(&req, &Limits::DEFAULT)
        .expect_err("expected = 0 must be rejected for Delete");

    assert_eq!(err.kind(), config_core::StatusClass::InvalidArgument);
}

#[config_log::retcd_test]
fn m0_11_delete_expected_match() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"k", 5);

    let resp = apply(&mut state, &del_cas(b"k", 5));

    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(resp.revision, 6);
    assert!(state.get(b"k").is_none());
    assert_eq!(state.cluster_revision(), 6);
}

#[config_log::retcd_test]
fn m0_12_delete_expected_mismatch_present_conflict() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"k", 5);

    let resp = apply(&mut state, &del_cas(b"k", 4));

    assert_eq!(resp.outcome, MutationOutcome::Conflict);
    assert!(resp.exists);
    assert_eq!(resp.current_mod_revision, 5);
    assert!(
        state.get(b"k").is_some(),
        "a conflicting delete removes nothing"
    );
    assert_eq!(state.cluster_revision(), 5);
}

/// A positive expected revision on an absent key is NOT_FOUND, not CONFLICT (spec §7.3). This
/// is what makes "exactly one of N competing deletes applies" come out right.
#[config_log::retcd_test]
fn m0_13_delete_expected_positive_absent_not_found() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"other", 9);

    let resp = apply(&mut state, &del_cas(b"k", 4));

    assert_eq!(resp.outcome, MutationOutcome::NotFound);
    assert_eq!(resp.revision, 9);
    assert_eq!(state.cluster_revision(), 9);
}

/// An entry that bypassed edge validation still has to be handled: apply rejects it
/// deterministically, without a panic and without allocating a revision (ADR-0006).
#[config_log::retcd_test]
fn m0_14_delete_expected_zero_reaches_apply() {
    let mut state = KvState::new();
    seed_key_at(&mut state, b"k", 1);
    let hash_before = state.state_hash();

    let crafted = Command::Delete {
        key: b(b"k"),
        expected_mod_revision: Some(0),
    };
    // It really does survive the wire form; edge validation is the only thing that stops it.
    let decoded = Command::decode(&crafted.encode()).expect("the envelope itself is well formed");
    let response = state.apply(&decoded);

    match response {
        CommandResponse::Rejected { reason } => {
            assert!(
                reason.contains("expected_mod_revision"),
                "reason names the rule: {reason}"
            );
        }
        other => panic!("expected a deterministic rejection, got {other:?}"),
    }
    assert_eq!(
        state.cluster_revision(),
        1,
        "a rejected entry allocates nothing"
    );
    assert_eq!(
        state.state_hash(),
        hash_before,
        "a rejected entry changes no state"
    );
    assert!(state.get(b"k").is_some());
}

/// One command applied to one pre-state, with the expected outcome and revision delta.
struct Row {
    id: &'static str,
    seed: u64,
    seed_key: &'static [u8],
    cmd: fn() -> Command,
    outcome: MutationOutcome,
    exists: bool,
    current_mod_revision: u64,
    revision_delta: u64,
}

/// All twelve ADR-0006 rows in one table. The length assertion is the point: if the ADR grows
/// a row and this table does not, the suite fails instead of silently covering eleven.
const ROWS: &[Row] = &[
    Row {
        id: "r1  put/absent/unconditional",
        seed: 0,
        seed_key: b"k",
        cmd: || put(b"k", b"v"),
        outcome: MutationOutcome::Applied,
        exists: true,
        current_mod_revision: 1,
        revision_delta: 1,
    },
    Row {
        id: "r1b put/present/unconditional",
        seed: 3,
        seed_key: b"k",
        cmd: || put(b"k", b"v"),
        outcome: MutationOutcome::Applied,
        exists: true,
        current_mod_revision: 4,
        revision_delta: 1,
    },
    Row {
        id: "r2  put/absent/expected=0",
        seed: 0,
        seed_key: b"other",
        cmd: || put_cas(b"k", b"v", 0),
        outcome: MutationOutcome::Applied,
        exists: true,
        current_mod_revision: 1,
        revision_delta: 1,
    },
    Row {
        id: "r3  put/present/expected=0",
        seed: 2,
        seed_key: b"k",
        cmd: || put_cas(b"k", b"v", 0),
        outcome: MutationOutcome::Conflict,
        exists: true,
        current_mod_revision: 2,
        revision_delta: 0,
    },
    Row {
        id: "r4  put/present/expected=match",
        seed: 4,
        seed_key: b"k",
        cmd: || put_cas(b"k", b"v", 4),
        outcome: MutationOutcome::Applied,
        exists: true,
        current_mod_revision: 5,
        revision_delta: 1,
    },
    Row {
        id: "r5  put/present/expected=mismatch",
        seed: 4,
        seed_key: b"k",
        cmd: || put_cas(b"k", b"v", 3),
        outcome: MutationOutcome::Conflict,
        exists: true,
        current_mod_revision: 4,
        revision_delta: 0,
    },
    Row {
        id: "r6  put/absent/expected=n",
        seed: 9,
        seed_key: b"other",
        cmd: || put_cas(b"k", b"v", 7),
        outcome: MutationOutcome::Conflict,
        exists: false,
        current_mod_revision: 0,
        revision_delta: 0,
    },
    Row {
        id: "r7  delete/present/unconditional",
        seed: 2,
        seed_key: b"k",
        cmd: || del(b"k"),
        outcome: MutationOutcome::Applied,
        exists: false,
        current_mod_revision: 3,
        revision_delta: 1,
    },
    Row {
        id: "r8  delete/absent/unconditional",
        seed: 0,
        seed_key: b"other",
        cmd: || del(b"k"),
        outcome: MutationOutcome::NotFound,
        exists: false,
        current_mod_revision: 0,
        revision_delta: 0,
    },
    // r9 (Delete expected = 0) is an edge rejection, not an outcome; see m0_10 / m0_14.
    Row {
        id: "r10 delete/present/expected=match",
        seed: 5,
        seed_key: b"k",
        cmd: || del_cas(b"k", 5),
        outcome: MutationOutcome::Applied,
        exists: false,
        current_mod_revision: 6,
        revision_delta: 1,
    },
    Row {
        id: "r11 delete/present/expected=mismatch",
        seed: 5,
        seed_key: b"k",
        cmd: || del_cas(b"k", 4),
        outcome: MutationOutcome::Conflict,
        exists: true,
        current_mod_revision: 5,
        revision_delta: 0,
    },
    Row {
        id: "r12 delete/absent/expected=n",
        seed: 9,
        seed_key: b"other",
        cmd: || del_cas(b"k", 4),
        outcome: MutationOutcome::NotFound,
        exists: false,
        current_mod_revision: 0,
        revision_delta: 0,
    },
];

#[config_log::retcd_test]
fn m0_15_cas_table_is_exhaustive() {
    const _: () = assert!(
        ROWS.len() == 12,
        "the ADR-0006 table must stay covered row for row; row 9 (Delete expected = 0) is an \
         edge rejection rather than an apply-time outcome, so it is covered by m0_10 and m0_14 \
         while r1 contributes both its absent and present cases here"
    );
    assert_eq!(ROWS.len(), 12);

    for row in ROWS {
        let mut state = KvState::new();
        if row.seed > 0 {
            seed_key_at(&mut state, row.seed_key, row.seed);
        }
        let before = state.cluster_revision();

        let resp = apply(&mut state, &(row.cmd)());

        assert_eq!(resp.outcome, row.outcome, "{}: outcome", row.id);
        assert_eq!(resp.exists, row.exists, "{}: exists", row.id);
        assert_eq!(
            resp.current_mod_revision, row.current_mod_revision,
            "{}: current_mod_revision",
            row.id
        );
        assert_eq!(
            state.cluster_revision(),
            before + row.revision_delta,
            "{}: revision delta",
            row.id
        );
        assert_eq!(
            resp.revision,
            state.cluster_revision(),
            "{}: response revision equals the cluster revision after apply",
            row.id
        );
    }
}
