//! M0-16..M0-22 — public revision allocation (ADR-0005, spec §19.3).
//!
//! The recurring question behind every row: did this command allocate a revision, and does
//! the response say so honestly? A conflict that quietly consumed a revision would break the
//! "+1 per state-changing mutation" contract that clients use to reason about progress.

mod common;

use common::{apply, b, del, del_cas, put, put_cas};
use config_core::{KvState, ListRequest, MutationOutcome};

#[config_log::retcd_test]
fn m0_16_revision_starts_at_zero() {
    let state = KvState::new();

    assert_eq!(state.cluster_revision(), 0);

    let get = state.get_response(b"missing");
    assert_eq!(get.record, None);
    assert_eq!(get.read_revision, 0, "an empty store reads at revision 0");
}

#[config_log::retcd_test]
fn m0_17_revision_increments_by_exactly_one() {
    let mut state = KvState::new();
    let keys: [&[u8]; 5] = [b"a", b"b", b"c", b"d", b"e"];

    let revisions: Vec<u64> = keys
        .iter()
        .map(|k| apply(&mut state, &put(k, b"v")).revision)
        .collect();

    assert_eq!(revisions, vec![1, 2, 3, 4, 5]);
    assert_eq!(state.cluster_revision(), 5);
}

/// A same-value Put is a state-changing mutation: it allocates a revision and bumps
/// `mod_revision` (spec §7.3). Treating it as a no-op would make a client's CAS loop wrong.
#[config_log::retcd_test]
fn m0_18_same_value_put_bumps_revision() {
    let mut state = KvState::new();
    apply(&mut state, &put(b"k", b"v"));

    let resp = apply(&mut state, &put(b"k", b"v"));

    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(resp.revision, 2);
    let rec = state.get(b"k").expect("present").clone();
    assert_eq!(rec.mod_revision, 2);
    assert_eq!(rec.create_revision, 1);
}

#[config_log::retcd_test]
fn m0_19_rejections_allocate_nothing() {
    let mut state = KvState::new();

    let ok1 = apply(&mut state, &put(b"k", b"v1"));
    let conflict = apply(&mut state, &put_cas(b"k", b"v2", 99));
    let not_found = apply(&mut state, &del(b"missing"));
    let del_conflict = apply(&mut state, &del_cas(b"k", 99));
    let ok2 = apply(&mut state, &put(b"k", b"v3"));

    assert_eq!(ok1.revision, 1);
    assert_eq!(
        ok2.revision, 2,
        "only the two applied puts consumed revisions"
    );
    assert_eq!(state.cluster_revision(), 2);

    for rejected in [&conflict, &not_found, &del_conflict] {
        assert_ne!(rejected.outcome, MutationOutcome::Applied);
        assert_eq!(
            rejected.revision, 1,
            "a rejected mutation reports the cluster revision current at that moment"
        );
    }
}

/// `create_revision` names the revision at which the key came into existence *this* time, so
/// a delete-then-recreate resets it (ADR-0005).
#[config_log::retcd_test]
fn m0_20_create_revision_resets_after_delete() {
    let mut state = KvState::new();
    apply(&mut state, &put(b"k", b"v1"));
    apply(&mut state, &del(b"k"));
    apply(&mut state, &put(b"k", b"v2"));

    let rec = state.get(b"k").expect("present").clone();
    assert_eq!((rec.create_revision, rec.mod_revision), (3, 3));
}

#[config_log::retcd_test]
fn m0_21_create_revision_stable_across_updates() {
    let mut state = KvState::new();
    apply(&mut state, &put(b"k", b"v1"));
    apply(&mut state, &put(b"k", b"v2"));
    apply(&mut state, &put(b"k", b"v3"));

    let rec = state.get(b"k").expect("present").clone();
    assert_eq!((rec.create_revision, rec.mod_revision), (1, 3));
}

#[config_log::retcd_test]
fn m0_22_read_revision_on_get_and_list() {
    let mut state = KvState::new();
    apply(&mut state, &put(b"a", b"1"));
    apply(&mut state, &put(b"b", b"2"));
    apply(&mut state, &put(b"c", b"3"));

    let present = state.get_response(b"a");
    let absent = state.get_response(b"zzz");
    let listed = state.list(&ListRequest {
        prefix: b(b""),
        max_items: 0,
        max_bytes: 0,
    });

    assert_eq!(present.read_revision, 3);
    assert!(present.record.is_some());
    assert_eq!(
        absent.read_revision, 3,
        "a missing key still reports the read revision"
    );
    assert_eq!(absent.record, None);
    assert_eq!(listed.read_revision, 3);
}
