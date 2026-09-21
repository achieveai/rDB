//! Rows M7F-06, M7F-07, M7F-08 and M7F-18: package M1, the memory engine and what it loses.
//!
//! | Row | Claim |
//! |---|---|
//! | M7F-06 | one pre-crash image: `ProcessCrash` reopens with `applied` intact, `HostCrash` reopens with `applied == durable`; the two reopened engines differ (K-F-03) |
//! | M7F-07 | `FalseDurable` advances no durable watermark |
//! | M7F-08 | `SnapshotRead::version` on a populated snapshot answers the writing transaction's sequence; `EmptySnapshot` answers `None` (K-F-04) |
//! | M7F-18 | `ShortFlush { through }` makes the flush's `durable` land at `through`, shorter than the capture; the engine's watermark is `through` (K-F-25) |
//!
//! Log fields are sequences and counts; never a key or value byte.

mod support;

use config_log::retcd_test;
use rdb_core::contracts::ids::{
    AppliedSeq, DurableSeq, Generation, NodeId, PartitionId, SnapshotHandle,
};
use rdb_core::contracts::storage::{CapturedPrefix, Namespace, SnapshotRead, StorageFault};
use rdb_sim::storage::crash_image::{CrashImage, SurvivingPrefix};
use rdb_sim::storage::memory::MemoryEngine;
use rdb_sim::storage::snapshot::EmptySnapshot;
use rdb_sim::storage::StorageOp;

const NODE: NodeId = NodeId(1);
const PARTITION: PartitionId = PartitionId(1);
const GENERATION: Generation = Generation(1);

/// Three batches applied, the first two synced.
fn engine_with_two_durable_of_three() -> MemoryEngine {
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=3 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("commit");
    }
    let durable = engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: PARTITION,
            generation: GENERATION,
            through: AppliedSeq(2),
        }])
        .expect("flush");
    assert_eq!(durable[0].through, DurableSeq(2));
    engine
}

#[retcd_test]
fn m7f_06_process_crash_keeps_applied_and_host_crash_truncates_to_durable() {
    support::preamble();
    let engine = engine_with_two_durable_of_three();
    assert_eq!(
        engine.buffered_applied(PARTITION, GENERATION),
        AppliedSeq(3)
    );
    assert_eq!(engine.durable(PARTITION, GENERATION), DurableSeq(2));

    let process = CrashImage::of(&engine, StorageFault::ProcessCrash).expect("process image");
    let host = CrashImage::of(&engine, StorageFault::HostCrash).expect("host image");

    assert_eq!(
        process.surviving,
        vec![SurvivingPrefix {
            partition: PARTITION,
            generation: GENERATION,
            durable: DurableSeq(2),
            applied: AppliedSeq(3),
        }],
        "a process crash keeps the buffered prefix"
    );
    assert_eq!(
        host.surviving,
        vec![SurvivingPrefix {
            partition: PARTITION,
            generation: GENERATION,
            durable: DurableSeq(2),
            applied: AppliedSeq(2),
        }],
        "a host crash keeps only what a real sync covered"
    );

    let after_process = process.reopen(NODE);
    let after_host = host.reopen(NODE);
    tracing::info!(
        process_applied = after_process.buffered_applied(PARTITION, GENERATION).0,
        host_applied = after_host.buffered_applied(PARTITION, GENERATION).0,
        "m7f_06 reopened"
    );

    assert_eq!(
        after_process.buffered_applied(PARTITION, GENERATION),
        AppliedSeq(3)
    );
    assert_eq!(after_process.durable(PARTITION, GENERATION), DurableSeq(2));
    assert_eq!(
        after_host.buffered_applied(PARTITION, GENERATION),
        AppliedSeq(2)
    );
    assert_eq!(after_host.durable(PARTITION, GENERATION), DurableSeq(2));
    assert_ne!(after_process, after_host, "the two reopened engines differ");
    // The lost batch's write is not visible after a host crash: seq 3 wrote version 3.
    assert_eq!(
        after_host.version_of(PARTITION, Namespace::User, b"k"),
        Some(2)
    );
    assert_eq!(
        after_process.version_of(PARTITION, Namespace::User, b"k"),
        Some(3)
    );
}

/// A crash image of a non-crash is refused, not invented.
#[retcd_test]
fn m7f_06_a_crash_image_needs_a_crash() {
    support::preamble();
    let engine = engine_with_two_durable_of_three();

    let error = CrashImage::of(&engine, StorageFault::WriteFailed).expect_err("not a crash");

    assert_eq!(
        error,
        rdb_sim::SimError::Config { field: "fault" },
        "the refusal names the field"
    );
}

#[retcd_test]
fn m7f_07_false_durable_advances_no_durable_watermark() {
    support::preamble();
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=2 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("commit");
    }
    engine
        .inject(StorageOp::FalseDurable {
            node: NODE,
            through: AppliedSeq(2),
        })
        .expect("inject");

    let durable = engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: PARTITION,
            generation: GENERATION,
            through: AppliedSeq(2),
        }])
        .expect("the false flush looks like a success");

    assert!(
        durable.is_empty(),
        "no DurableSeq comes back from a flush that synced nothing"
    );
    assert_eq!(engine.durable(PARTITION, GENERATION), DurableSeq(0));
    assert_eq!(engine.false_claims(), &[AppliedSeq(2)]);

    // And a host crash now loses everything the false flush claimed.
    let host = CrashImage::of(&engine, StorageFault::HostCrash).expect("host image");
    assert_eq!(host.surviving[0].applied, AppliedSeq(0));
}

#[retcd_test]
fn m7f_08_snapshot_version_answers_the_writing_sequence() {
    support::preamble();
    let mut engine = MemoryEngine::new(NODE);
    engine
        .commit(support::batch(1, 1, b"a", b"1"))
        .expect("commit 1");
    engine
        .commit(support::batch(1, 2, b"b", b"2"))
        .expect("commit 2");
    engine
        .commit(support::batch(1, 3, b"a", b"3"))
        .expect("commit 3");

    let snapshot = engine.snapshot(PARTITION, GENERATION, SnapshotHandle(7));
    // A commit after the snapshot does not reach into it.
    engine
        .commit(support::batch(1, 4, b"b", b"4"))
        .expect("commit 4");

    tracing::info!(
        records = snapshot.len(),
        at = snapshot.at().0,
        "m7f_08 snapshot"
    );
    assert_eq!(snapshot.handle(), SnapshotHandle(7));
    assert_eq!(snapshot.at().0, 3);
    assert_eq!(snapshot.generation(), GENERATION);
    assert_eq!(snapshot.version(Namespace::User, b"a"), Some(3));
    assert_eq!(snapshot.version(Namespace::User, b"b"), Some(2));
    assert_eq!(snapshot.version(Namespace::User, b"c"), None);
    assert_eq!(snapshot.version(Namespace::History, b"a"), None);
    assert_eq!(
        snapshot.scan(Namespace::User, b"", 10).len(),
        2,
        "scan is ordered and namespace-bounded"
    );

    let empty = EmptySnapshot::new();
    assert_eq!(empty.version(Namespace::User, b"a"), None);
    assert!(empty.get(Namespace::User, b"a").is_none());
}

#[retcd_test]
fn m7f_18_short_flush_reports_and_keeps_the_shorter_prefix() {
    support::preamble();
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=5 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("commit");
    }
    engine
        .inject(StorageOp::ShortFlush {
            node: NODE,
            through: AppliedSeq(3),
        })
        .expect("inject");

    let durable = engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: PARTITION,
            generation: GENERATION,
            through: AppliedSeq(5),
        }])
        .expect("a short flush is a real success");

    tracing::info!(
        captured = 5,
        durable = durable[0].through.0,
        "m7f_18 short flush"
    );
    assert_eq!(durable.len(), 1);
    assert_eq!(
        durable[0].through,
        DurableSeq(3),
        "the answer is the engine's, not an echo of the capture"
    );
    assert_eq!(engine.durable(PARTITION, GENERATION), DurableSeq(3));

    // The next flush, unplanned, syncs the rest.
    let rest = engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: PARTITION,
            generation: GENERATION,
            through: AppliedSeq(5),
        }])
        .expect("flush");
    assert_eq!(rest[0].through, DurableSeq(5));
}

/// A fault aimed at another engine, or of the wrong kind, is refused at injection.
#[retcd_test]
fn m7f_18_misdirected_faults_are_refused_at_injection() {
    support::preamble();
    let mut engine = MemoryEngine::new(NODE);

    assert_eq!(
        engine.inject(StorageOp::ShortFlush {
            node: NodeId(2),
            through: AppliedSeq(1),
        }),
        Err(rdb_sim::SimError::Config { field: "node" })
    );
    assert_eq!(
        engine.inject(StorageOp::Fail {
            node: NODE,
            fault: StorageFault::HostCrash,
        }),
        Err(rdb_sim::SimError::Config { field: "fault" })
    );
    assert_eq!(
        engine.inject(StorageOp::Crash {
            node: NODE,
            fault: StorageFault::FlushFailed,
        }),
        Err(rdb_sim::SimError::Config { field: "fault" })
    );
}
