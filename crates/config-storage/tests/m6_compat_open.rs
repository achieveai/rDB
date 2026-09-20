//! M6-98 — a build pinned below the directory's format refuses to open it, with **one**
//! message (test plan `docs/testing/test-plan-m6.md` §6.3; ADR-0030 OQ-65, ADR-0021).
//!
//! The row exists because of an ordering bug, not a missing check. M4 §11 item 5 recorded that
//! `RocksStore::open` ran `verify_column_families` *before* `check_format_version`, so an
//! operator pointing an older build at a newer directory could be told a column family was
//! unexpected — true, but the wrong problem and the wrong fix. "Some error" is not an operator
//! procedure, so this file asserts the exact one and asserts that the other is not possible.
//!
//! It also carries the two rows for ruling **M6-R20** (2026-09-19), which amends M5-R19's
//! drain precondition — see the block comment above `m6_r20_a_…` below.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use config_core::{
    ClusterId, ClusterIdentity, Command, Limits, NodeId, RecoveryEpoch, COMMAND_SCHEMA_V1,
    COMMAND_SCHEMA_V2, FEATURE_COMPACT,
};
use config_log::retcd_test;
use config_storage::{
    NoFaults, NoopSink, RaftNodeId, RocksOptions, RocksStore, StorageOpenError, TypeConfig,
    FORMAT_VERSION,
};
use openraft::storage::{RaftLogStorageExt, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use tracing::Span;

fn identity() -> ClusterIdentity {
    ClusterIdentity {
        cluster_id: ClusterId::from_bytes([7u8; 16]),
        recovery_epoch: RecoveryEpoch(0),
        node_id: NodeId(1),
    }
}

/// An unconditional `Put` at term 1, index `index` — the shape a leader appends.
fn put(index: u64, key: &str, value: &str) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::<RaftNodeId>::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(Command::Put {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
            expected_mod_revision: None,
            dedup: None,
        }),
    }
}

/// Open `dir` as a build whose newest readable format is `max_format_version`.
fn open_with_ceiling(dir: &Path, max_format_version: u32) -> Result<RocksStore, StorageOpenError> {
    RocksStore::open_with(
        dir,
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
        RocksOptions {
            max_format_version,
            ..RocksOptions::DEFAULT
        },
        Arc::new(NoopSink),
    )
}

/// M6-98 `downgrading_a_v2_store_is_refused_with_one_specific_message`.
///
/// The `UnsupportedFormat`/`MissingColumnFamily` pair is the whole point: a current directory
/// has *more* column families than an older build expects, so both refusals are physically
/// available and only the check order decides which one the operator reads.
#[retcd_test]
fn m6_98_downgrading_a_newer_store_is_refused_with_one_specific_message() {
    let dir = tempfile::tempdir().expect("temp dir");

    // A directory written by this build, i.e. the newest format there is.
    drop(open_with_ceiling(dir.path(), FORMAT_VERSION).expect("a current build opens it"));

    let refusal = open_with_ceiling(dir.path(), 1).expect_err("a schema-1 build must refuse it");
    match &refusal {
        StorageOpenError::UnsupportedFormat {
            found, supported, ..
        } => {
            assert_eq!(*found, FORMAT_VERSION, "the marker actually on disk");
            assert_eq!(*supported, 1, "the ceiling this open was given");
        }
        other => panic!("expected a version refusal, got {other:?}"),
    }
    // The other message must not be reachable for this directory: an operator who sees
    // "unexpected column family" is sent to look for corruption instead of for the build they
    // meant to run.
    assert!(
        !matches!(refusal, StorageOpenError::MissingColumnFamily { .. }),
        "the column-family check must not run before the version check"
    );
    let rendered = refusal.to_string();
    assert!(
        rendered.contains(&FORMAT_VERSION.to_string()) && rendered.contains('1'),
        "the message must name both the found and the supported version: {rendered}"
    );
}

/// M6-98, companion: the refusal is read-only.
///
/// A refused downgrade that had already rewritten the marker would have destroyed the very
/// directory the operator now has to go back and run the newer build against.
#[retcd_test]
fn m6_98b_a_refused_downgrade_leaves_the_directory_openable_by_the_build_that_wrote_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    drop(open_with_ceiling(dir.path(), FORMAT_VERSION).expect("a current build opens it"));

    open_with_ceiling(dir.path(), 1).expect_err("refused");

    drop(open_with_ceiling(dir.path(), FORMAT_VERSION).expect("still openable, byte for byte"));
}

/// M6-98, companion: a pinned build's *own* directory reopens.
///
/// Without this the compat mode would be unusable for M6-95's rolling upgrade: a node that
/// stamped the current marker on a fresh directory would refuse its own data on its very next
/// start, and the refusal above would look correct while making the feature useless.
#[retcd_test]
fn m6_98c_a_pinned_build_can_reopen_the_directory_it_created() {
    let dir = tempfile::tempdir().expect("temp dir");

    drop(open_with_ceiling(dir.path(), 1).expect("a pinned build creates its directory"));
    drop(open_with_ceiling(dir.path(), 1).expect("and opens it again"));

    // And the newer build reads it, because forward migration is the supported direction
    // (ADR-0030: v2 is defined to be able to read v1).
    drop(open_with_ceiling(dir.path(), FORMAT_VERSION).expect("a current build migrates it"));
    // Which is a one-way door: after the migration the pinned build is out.
    open_with_ceiling(dir.path(), 1).expect_err("migrated past the ceiling");
}

// -------------------------------------------------------------------------------------------
// M6-R20 — what "drained" means for an in-place format migration
// -------------------------------------------------------------------------------------------
//
// Ruling M5-R19 refused an in-place format migration unless `raft_log` held **exactly zero**
// entries, using the `format_version` marker as a proxy for "an older build wrote this log".
// ADR-0030 broke that proxy: a current binary started with `--compat-schema 1` lowers its
// ceiling to 1, stamps marker 1 — and writes **current-grammar** log entries under it, because
// there is no schema-1 command encoder in this build. And the zero-entry half is unreachable
// besides: openraft's purge leaves a residual tail (measured at 2 entries across 6 runs under
// every `[snapshot]` tuning, tester-m6c), so no sequence of operator actions satisfies it.
//
// M6-R20 replaces the proxy with the two facts that actually matter, tested directly:
// every retained entry must (a) decode as `Entry<TypeConfig>` under this build and (b) sit at
// or below `last_applied`. The rows below are the two halves.

/// M6-R20 half (a)+(b) pass: a directory a pinned build wrote, still holding the applied
/// residual openraft's purge always leaves, migrates forward instead of stranding the operator.
///
/// This is the row E2E-42's rolling upgrade stands on. Before M6-R20 this open was refused
/// with `UpgradeRequiresDrainedLog`, on a log the very same binary had just written.
#[retcd_test]
async fn m6_r20_a_pinned_directory_with_an_applied_log_residual_migrates() {
    let dir = tempfile::tempdir().expect("temp dir");
    let entries: Vec<Entry<TypeConfig>> = (1..=3)
        .map(|i| put(i, &format!("/m6/r20/k{i}"), "v"))
        .collect();

    {
        let pinned = open_with_ceiling(dir.path(), 1).expect("a pinned build creates its own dir");
        pinned
            .log_store()
            .blocking_append(entries.clone())
            .await
            .expect("append");
        pinned
            .state_machine()
            .apply(entries.clone())
            .await
            .expect("apply");
        assert_eq!(pinned.raft_log_len(), 3, "the residual is left in the log");
    }

    let upgraded = open_with_ceiling(dir.path(), FORMAT_VERSION)
        .expect("this build must be able to carry a log it wrote itself across the upgrade");
    assert_eq!(
        upgraded.raft_log_len(),
        3,
        "migration upgrades state, never history: the log is carried, not rewritten"
    );
    assert_eq!(
        upgraded.reader().cluster_revision(),
        3,
        "and the applied state came back intact"
    );
    assert_eq!(
        upgraded.reader().compact_revision().expect("compact_revision readable"),
        0,
        "the v1 clause is for the v1 layout, not the v1 marker: a journal-carrying directory keeps its watermark (M6-R22)"
    );
    drop(upgraded);

    // One-way door, unchanged from M6-98c: the pinned build is out once the marker moved.
    open_with_ceiling(dir.path(), 1).expect_err("migrated past the ceiling");
}

/// M6-R20 half (b) fails: one entry above `last_applied` is still refused, by name.
///
/// This is the clause a bare decodability check would lose. An entry this build is going to
/// **execute** is the unrecoverable case: a postcard decode that happens to succeed is not
/// proof that the bytes mean the same command, and by apply time there is no way back.
#[retcd_test]
async fn m6_r20_b_an_unapplied_entry_still_refuses_the_migration() {
    let dir = tempfile::tempdir().expect("temp dir");
    let entries: Vec<Entry<TypeConfig>> = (1..=3)
        .map(|i| put(i, &format!("/m6/r20b/k{i}"), "v"))
        .collect();

    {
        let pinned = open_with_ceiling(dir.path(), 1).expect("a pinned build creates its own dir");
        pinned
            .log_store()
            .blocking_append(entries.clone())
            .await
            .expect("append");
        // Entry 3 is appended but never applied — the node was stopped mid-replay.
        pinned
            .state_machine()
            .apply(entries[..2].to_vec())
            .await
            .expect("apply");
    }

    let refused = open_with_ceiling(dir.path(), FORMAT_VERSION)
        .expect_err("an unapplied entry must still block the in-place upgrade");
    match &refused {
        StorageOpenError::UpgradeRequiresDrainedLog {
            format,
            log_entries,
            path,
        } => {
            assert_eq!(*format, 1, "the refusal names the version it found");
            assert_eq!(
                *log_entries, 1,
                "and counts only the entries actually in the way, not the applied residual"
            );
            assert_eq!(path.as_path(), dir.path());
        }
        other => panic!("expected a typed drained-log refusal, got {other:?}"),
    }
    // The operator has to be able to act on this without reading the source — and finding
    // F-016 is that "act on it" was the part missing. This directory is stamped format 1, and
    // the message's old single remedy named a snapshot trigger and a log purge, neither of
    // which exists on the only released build that writes a format-1 directory (M0-M3,
    // `main` 7d524ac: no snapshot engine, no admin plane). The format-1 arm must therefore
    // lead with the path that works everywhere — rejoin as a fresh learner — while still
    // naming the drain for the other producer of a marker-1 directory, this build pinned.
    let message = refused.to_string();
    for needle in [
        "learner",
        "surviving",
        "--compat-schema 1",
        "snapshot",
        "purge",
    ] {
        assert!(
            message.contains(needle),
            "the refusal must name a performable fix; {needle:?} missing from {message:?}"
        );
    }

    // And the refusal is read-only, so the pinned build can still open its directory, finish
    // applying, and be retried.
    drop(open_with_ceiling(dir.path(), 1).expect("still openable by the build that wrote it"));
}

// ---------------------------------------------------------------------------------------
// Finding F-015 — the decode fence ADR-0030's `--compat-schema` contract always promised.
//
// `SchemaTriple::decode_command` and `SchemaTriple::admits` shipped at M6 with no caller
// outside tests, so the promise was documentation only: the apply path decoded every
// committed entry with the full grammar, applied it, and raised the node's durable
// `max_applied_command_schema` to 2 while the node went on advertising schema 1. The
// advertisement a cluster gates on became a lie told by the node itself.
//
// The fence is now `RocksOptions::command_schema`, checked in the apply path and again on
// snapshot install. These rows drive a real store: the refusal is the product of an actual
// apply, not of a helper asked about a shape.
// ---------------------------------------------------------------------------------------

/// A `Compact` entry at term 1, index `index` — a command only schema 2 can carry.
fn compact(index: u64, up_to_revision: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::<RaftNodeId>::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(Command::Compact {
            up_to_revision,
            dedup_trim_below: None,
        }),
    }
}

/// Open `dir` pinned on both of ADR-0030's axes, the way `--compat-schema 1` starts a daemon.
fn open_pinned(dir: &Path) -> Result<RocksStore, StorageOpenError> {
    RocksStore::open_with(
        dir,
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
        RocksOptions {
            max_format_version: 1,
            command_schema: COMMAND_SCHEMA_V1,
            ..RocksOptions::DEFAULT
        },
        Arc::new(NoopSink),
    )
}

/// This store's durable activation watermark (ruling M6-R15).
fn watermark(store: &RocksStore) -> u16 {
    let mut out = 0u16;
    store
        .reader()
        .with_state(&mut |s| out = s.max_applied_command_schema());
    out
}

/// F-015 `a_pinned_store_refuses_to_apply_a_committed_schema_2_command`.
///
/// The row that makes `--compat-schema 1`'s central claim true. The entry is *committed* —
/// that is the whole scenario, and it is why the refusal has to be an error rather than a
/// skip: postcard is not self-describing, so a voter that cannot carry a committed entry has
/// no way past it (ADR-0030 A7). Stopping the node is the correct outcome and the only honest
/// one; the alternative this replaces was to apply it and then misreport what had been applied.
#[retcd_test]
async fn f015_a_pinned_store_refuses_to_apply_a_committed_schema_2_command() {
    let dir = tempfile::tempdir().expect("temp dir");
    let pinned = open_pinned(dir.path()).expect("a pinned build creates its own dir");

    // An ordinary write first: the fence is about the *dedup/maintenance* grammar, not about
    // writing. A rolling upgrade that stopped ordinary traffic would be its own outage.
    pinned
        .state_machine()
        .apply(vec![put(1, "/f015/plain", "v")])
        .await
        .expect("a schema-1 command must still apply on a pinned node");
    assert_eq!(watermark(&pinned), COMMAND_SCHEMA_V1);

    let refused = pinned
        .state_machine()
        .apply(vec![compact(2, 1)])
        .await
        .expect_err("a pinned node must refuse a committed schema-2 command");

    // Named, not merely typed: the operator has to learn which feature and which generation.
    let message = refused.to_string();
    for needle in [FEATURE_COMPACT, "command_schema"] {
        assert!(
            message.contains(needle),
            "the refusal must name the gate; {needle:?} missing from {message:?}"
        );
    }

    // And the lie the fence exists to prevent was never told: the watermark did not move, so
    // this node still advertises exactly what it can actually decode.
    assert_eq!(
        watermark(&pinned),
        COMMAND_SCHEMA_V1,
        "a refused command must not raise the durable activation watermark"
    );
    drop(pinned);

    // The same directory, unpinned, carries the same entry without complaint — proving the
    // refusal is the pin talking and not damage to the log.
    let full = open_with_ceiling(dir.path(), FORMAT_VERSION).expect("unpinned open");
    full.state_machine()
        .apply(vec![compact(2, 1)])
        .await
        .expect("this build can carry its own generation");
    assert_eq!(watermark(&full), COMMAND_SCHEMA_V2);
}
