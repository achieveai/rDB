//! Rows M7F-06, M7F-07, M7F-08 and M7F-18: package M1, the memory engine and what it loses.
//!
//! | Row | Claim |
//! |---|---|
//! | M7F-06 | one pre-crash image: `ProcessCrash` reopens with `applied` intact, `HostCrash` reopens with `applied == durable`; the two reopened engines differ (K-F-03) |
//! | M7F-07 | `FalseDurable` advances no durable watermark |
//! | M7F-08 | `SnapshotRead::version` on a populated snapshot answers the writing transaction's sequence; `EmptySnapshot` answers `None` (K-F-04) |
//! | M7F-18 | `ShortFlush { through }` makes the flush's `durable` land at `through`, shorter than the capture; the engine's watermark is `through` (K-F-25) |
//! | M7F-44 | a snapshot taken before a multi-write batch never sees any of it, and one taken after sees all of it, across two namespaces and a delete |
//! | M7F-45 | every crash boundary lands on a batch boundary: both crash kinds, every key of a surviving batch readable and every key of a lost one gone |
//! | M7F-46 | `sync_wal_through` answers `min(captured, applied, short_flush)` — a commit that arrived after the capture is never covered by it |
//!
//! Log fields are sequences and counts; never a key or value byte.

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::contracts::ids::FlushTicket;
use rdb_core::contracts::ids::{
    AppliedSeq, BatchId, DurableSeq, Generation, NodeId, PartitionId, Seq, SnapshotHandle,
};
use rdb_core::contracts::storage::{
    Batch, CapturedPrefix, DurablePrefix, Namespace, SnapshotRead, StorageEvent, StorageFault,
    StoreEffect, Write,
};
use rdb_core::contracts::trace::Version;
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

/// M7F-06, second row — a failed commit keeps **none** of a multi-write batch.
///
/// Manual-tester finding F4, 2026-09-21, and the finding is about the fixture, not the row.
/// `MemoryEngine::commit`'s doc states "whole batch or none: a planned failure is decided before
/// the first write". Every existing row that commits builds its batch with `support::batch`,
/// which constructs **exactly one** write. A one-write batch cannot tell "whole batch or none"
/// from "first write or none", so moving the fault check from before the loop to inside it — so
/// that write 0 lands and write 1 fails — left the whole workspace green, 149/149.
///
/// This row therefore builds its `Batch` directly instead of through the helper. Two writes are
/// the minimum that can distinguish the two readings, and asserting on **both** keys is the
/// point: checking only the second would pass against the very mutation that leaks the first.
#[retcd_test]
fn m7f_06_a_failed_commit_keeps_none_of_a_multi_write_batch() {
    support::preamble();
    let mut engine = MemoryEngine::new(NODE);

    engine
        .inject(StorageOp::Fail {
            node: NODE,
            fault: StorageFault::WriteFailed,
        })
        .expect("a write fault is plannable on this engine");

    let batch = Batch {
        id: BatchId(1),
        partition: PARTITION,
        generation: GENERATION,
        seq: Seq(1),
        writes: vec![
            Write {
                ns: Namespace::User,
                key: Bytes::from_static(b"first"),
                value: Some(Bytes::from_static(b"1")),
            },
            Write {
                ns: Namespace::User,
                key: Bytes::from_static(b"second"),
                value: Some(Bytes::from_static(b"2")),
            },
        ],
    };

    assert_eq!(
        engine.commit(batch),
        Err(StorageFault::WriteFailed),
        "the planned fault is the commit's answer"
    );

    for key in [b"first".as_slice(), b"second".as_slice()] {
        assert_eq!(
            engine.version_of(PARTITION, Namespace::User, key),
            None,
            "a failed commit touches nothing, so {} must not exist — a fault decided inside \
             the write loop instead of before it leaks every write ahead of the failing one",
            String::from_utf8_lossy(key)
        );
    }
    assert_eq!(
        engine.buffered_applied(PARTITION, GENERATION),
        AppliedSeq(0),
        "and the applied watermark never moved"
    );
}

// ---------------------------------------------------------------------------------------------
// M7F-44, M7F-45, M7F-46 — charter M1 acceptance
// ---------------------------------------------------------------------------------------------

/// One record a snapshot must answer for: where it lives, and what the answer is.
///
/// Named because clippy's `type_complexity` refuses the tuple inline under `-D warnings`, and
/// the row wants the expectation written as a table rather than as prose.
type Expected<'a> = (Namespace, &'a [u8], Option<&'a [u8]>, Option<Version>);

/// Where one key of a batch lives, for the crash rows' present/absent lists.
type Located<'a> = (Namespace, &'a [u8]);

/// What one crash kind leaves behind: the two watermarks, the keys still readable, the keys gone.
type Survives<'a> = (AppliedSeq, DurableSeq, Vec<Located<'a>>, &'a [Located<'a>]);

/// One batch of four writes that spans two namespaces, overwrites, creates and deletes.
///
/// `support::batch` builds exactly one write, and a one-write batch cannot tell "whole batch"
/// from "first write" — the reading that left the workspace green against a fault moved inside
/// the write loop (see `m7f_06_a_failed_commit_keeps_none_of_a_multi_write_batch`). Every row
/// below that says "the whole batch" therefore builds its own.
fn spanning_batch(seq: u64, value: &'static [u8], deletes: &'static [u8]) -> Batch {
    Batch {
        id: BatchId(seq),
        partition: PARTITION,
        generation: GENERATION,
        seq: Seq(seq),
        writes: vec![
            Write {
                ns: Namespace::User,
                key: Bytes::from_static(b"a"),
                value: Some(Bytes::from_static(value)),
            },
            Write {
                ns: Namespace::User,
                key: Bytes::from_static(b"b"),
                value: Some(Bytes::from_static(value)),
            },
            Write {
                ns: Namespace::Meta,
                key: Bytes::from_static(b"m"),
                value: Some(Bytes::from_static(value)),
            },
            Write {
                ns: Namespace::User,
                key: Bytes::from_static(deletes),
                value: None,
            },
        ],
    }
}

/// M7F-44: a snapshot never sees a partial batch (charter M1 acceptance; design §4.3; spec §5.2
/// step 3).
///
/// **What turns this red:** narrowing `MemoryEngine::snapshot`'s range from "every namespace of
/// this partition" to `Namespace::User` — the batch's `Meta` write then never reaches the
/// post-batch view, so the batch is half visible. Or binding `MemorySnapshot::at` to anything
/// but the applied watermark at the moment of the snapshot, which is what makes `at` a committed
/// sequence rather than a guess. Or having a snapshot share the engine's record map instead of
/// owning a copy of it, which lets the held pre-batch view see a commit that happened after it.
/// Or dropping the `None` arm of `commit`'s write loop, so a delete inside a batch is skipped and
/// the post-batch view still answers the old value.
///
/// `M7F-08` already asserts that `version` answers the writing sequence and that one later commit
/// does not reach into a snapshot. What it cannot say is *all or nothing*: it commits one write
/// per batch, so "the whole batch is invisible" and "the first write is invisible" are the same
/// sentence. Here the batch is four writes over two namespaces including a delete, and every key
/// is checked on both sides.
#[retcd_test]
fn m7f_44_a_snapshot_never_sees_a_partial_batch() {
    support::preamble();
    let mut engine = MemoryEngine::new(NODE);

    // Seq 1 gives `a`, `b` and `m` an old version, seq 2 gives `d` one, so the pre-batch view
    // has something to still say about every key the multi-write batch touches.
    engine
        .commit(spanning_batch(1, b"v1", b"unused"))
        .expect("commit 1");
    engine
        .commit(support::batch(1, 2, b"d", b"v1"))
        .expect("commit 2 seeds the key seq 3 deletes");

    let before = engine.snapshot(PARTITION, GENERATION, SnapshotHandle(1));
    assert_eq!(before.at(), Seq(2), "bound to a committed sequence");

    // The multi-write batch: overwrite `a` and `b`, write `m` in another namespace, delete `d`.
    engine
        .commit(spanning_batch(3, b"v3", b"d"))
        .expect("commit 3");

    let after = engine.snapshot(PARTITION, GENERATION, SnapshotHandle(2));
    assert_eq!(after.at(), Seq(3), "bound to a committed sequence");

    // Not one key of the batch reached the view that was open before it.
    let untouched: [Expected<'_>; 4] = [
        (Namespace::User, b"a", Some(b"v1"), Some(1)),
        (Namespace::User, b"b", Some(b"v1"), Some(1)),
        (Namespace::Meta, b"m", Some(b"v1"), Some(1)),
        (Namespace::User, b"d", Some(b"v1"), Some(2)),
    ];
    for (ns, key, value, version) in untouched {
        assert_eq!(
            before.get(ns, key).as_deref(),
            value,
            "the pre-batch view still answers the old value for every key of the batch, and {} \
             is one of them",
            String::from_utf8_lossy(key)
        );
        assert_eq!(
            before.version(ns, key),
            version,
            "and the old version with it, for {}",
            String::from_utf8_lossy(key)
        );
    }

    // And every key of the batch reached the view taken after it — including the delete.
    let applied: [Expected<'_>; 4] = [
        (Namespace::User, b"a", Some(b"v3"), Some(3)),
        (Namespace::User, b"b", Some(b"v3"), Some(3)),
        (Namespace::Meta, b"m", Some(b"v3"), Some(3)),
        (Namespace::User, b"d", None, None),
    ];
    for (ns, key, value, version) in applied {
        assert_eq!(
            after.get(ns, key).as_deref(),
            value,
            "the post-batch view answers the new value for every key of the batch, and {} is \
             one of them",
            String::from_utf8_lossy(key)
        );
        assert_eq!(
            after.version(ns, key),
            version,
            "and the new version with it, for {}",
            String::from_utf8_lossy(key)
        );
    }

    tracing::info!(
        before_at = before.at().0,
        after_at = after.at().0,
        keys = untouched.len(),
        "m7f_44 snapshot atomicity"
    );
}

/// M7F-45: every crash boundary yields a whole batch or none (charter M1 acceptance; design §5
/// `CrashImage`; gate V1).
///
/// **What turns this red:** `CrashImage::of`'s `HostCrash` arm returning `lineage.applied`
/// instead of `AppliedSeq(lineage.durable.0)` — the silent promotion that makes an unsynced batch
/// survive a host crash, which is the exact mutation spike §7 names. Or `CrashImage::reopen`
/// restoring `durable` from `prefix.applied`, which lifts a watermark above its own value
/// (FA-3). Or widening the surviving-batch filter past `batch.seq.0 <= applied.0`, which makes
/// a lost batch's keys readable after the reopen while the watermarks still look right.
///
/// The `match` on `StorageFault` has no `_` arm. A third crash kind cannot be added without
/// being given a surviving prefix here, and a sixth fault of any kind stops this row compiling.
///
/// `M7F-06` asserts the two watermarks after each crash. What it does not assert is that the
/// watermark lands on a *batch* boundary: its batches are one write each, so a watermark in the
/// middle of a batch is not expressible. Both batches here are multi-write, and every key of
/// each is checked for presence or absence rather than sampled.
#[retcd_test]
fn m7f_45_every_crash_boundary_yields_a_whole_batch_or_none() {
    support::preamble();

    // (namespace, key) of each batch's puts, so "no key of a lost batch" is a list and not a
    // sample. `a` is left out of both because batch 2 deletes it: it has its own assertion
    // below, and it is the sharpest one here.
    let batch_one: [Located<'_>; 2] = [(Namespace::User, b"b"), (Namespace::Meta, b"m")];
    let batch_two: [Located<'_>; 1] = [(Namespace::User, b"late")];

    for fault in [
        StorageFault::ProcessCrash,
        StorageFault::HostCrash,
        StorageFault::WriteFailed,
        StorageFault::FlushFailed,
        StorageFault::Corrupt,
    ] {
        let mut engine = MemoryEngine::new(NODE);
        // Batch 1: four writes over two namespaces, synced. Batch 2: two writes, one of them a
        // delete of a key batch 1 wrote, applied and never synced.
        engine
            .commit(support::batch(1, 1, b"gone", b"v0"))
            .expect("commit 0 seeds the key batch 1 deletes");
        engine
            .commit(spanning_batch(2, b"v1", b"gone"))
            .expect("commit 1");
        let durable = engine
            .sync_wal_through(vec![CapturedPrefix {
                partition: PARTITION,
                generation: GENERATION,
                through: AppliedSeq(2),
            }])
            .expect("flush");
        assert_eq!(durable[0].through, DurableSeq(2));
        engine
            .commit(Batch {
                id: BatchId(3),
                partition: PARTITION,
                generation: GENERATION,
                seq: Seq(3),
                writes: vec![
                    Write {
                        ns: Namespace::User,
                        key: Bytes::from_static(b"late"),
                        value: Some(Bytes::from_static(b"v3")),
                    },
                    Write {
                        ns: Namespace::User,
                        key: Bytes::from_static(b"a"),
                        value: None,
                    },
                ],
            })
            .expect("commit 2");
        assert_eq!(
            engine.buffered_applied(PARTITION, GENERATION),
            AppliedSeq(3)
        );
        assert_eq!(engine.durable(PARTITION, GENERATION), DurableSeq(2));

        // Literal expectations per crash kind, not a second implementation of the rule.
        let (expected_applied, expected_durable, kept, lost): Survives<'_> = match fault {
            // The page cache outlives the process, so the unsynced batch is still there.
            StorageFault::ProcessCrash => (
                AppliedSeq(3),
                DurableSeq(2),
                [batch_one.as_slice(), batch_two.as_slice()].concat(),
                &[],
            ),
            // The host took the unsynced suffix with it: the last batch is gone, whole.
            StorageFault::HostCrash => {
                (AppliedSeq(2), DurableSeq(2), batch_one.to_vec(), &batch_two)
            }
            StorageFault::WriteFailed | StorageFault::FlushFailed | StorageFault::Corrupt => {
                assert!(
                    CrashImage::of(&engine, fault).is_err(),
                    "{fault:?} is not a crash, and what it keeps is not a question"
                );
                continue;
            }
        };

        let image = CrashImage::of(&engine, fault).expect("a crash image");
        assert_eq!(image.surviving.len(), 1, "one lineage in this fixture");
        for prefix in &image.surviving {
            let SurvivingPrefix {
                partition,
                generation,
                durable,
                applied,
            } = *prefix;
            assert_eq!((partition, generation), (PARTITION, GENERATION));
            // FA-3: no conversion exists, so the comparison is spelled out on the inner values.
            assert!(
                applied.0 >= durable.0,
                "{fault:?}: applied {applied:?} fell below durable {durable:?}"
            );
            assert_eq!(applied, expected_applied, "{fault:?}: surviving applied");
            assert_eq!(durable, expected_durable, "{fault:?}: surviving durable");
        }

        let reopened = image.reopen(NODE);
        assert_eq!(
            reopened.buffered_applied(PARTITION, GENERATION),
            expected_applied,
            "{fault:?}: reopen restores applied to its own value and never above it"
        );
        assert_eq!(
            reopened.durable(PARTITION, GENERATION),
            expected_durable,
            "{fault:?}: reopen restores durable to its own value and never above it"
        );

        for (ns, key) in &kept {
            assert!(
                reopened.version_of(PARTITION, *ns, key).is_some(),
                "{fault:?}: {} belongs to a batch at or below the watermark and must be readable",
                String::from_utf8_lossy(key)
            );
        }
        for (ns, key) in lost {
            assert_eq!(
                reopened.version_of(PARTITION, *ns, key),
                None,
                "{fault:?}: {} belongs to a batch beyond the watermark; not one of its keys may \
                 be readable",
                String::from_utf8_lossy(key)
            );
        }
        // The unsynced batch also deletes `a`, which batch 1 put. A batch boundary means a
        // batch's deletes are undone with its puts: `a` comes back exactly when that batch is
        // lost, and a crash image that replayed the puts of a lost batch but not its deletes —
        // or the reverse — fails right here and nowhere else in this file.
        assert_eq!(
            reopened
                .version_of(PARTITION, Namespace::User, b"a")
                .is_some(),
            fault == StorageFault::HostCrash,
            "{fault:?}: the last batch's delete of `a` survives exactly when that batch does"
        );
        // Batch 1's own delete, of a key seq 1 put. It is at or below the watermark under both
        // crash kinds, so it stays deleted under both.
        assert_eq!(
            reopened.version_of(PARTITION, Namespace::User, b"gone"),
            None,
            "{fault:?}: batch 1 deleted `gone` and batch 1 survived"
        );

        tracing::info!(
            fault = ?fault,
            applied = expected_applied.0,
            durable = expected_durable.0,
            lost = lost.len(),
            "m7f_45 crash boundary"
        );
    }
}

/// M7F-46: `sync_wal_through` answers under the write order (charter M1 acceptance; design §4.3,
/// §5; spike §6).
///
/// **What turns this red:** dropping the capture from `MemoryEngine::sync_wal_through`'s minimum
/// — syncing `lineage.applied` instead of `min(capture.through, lineage.applied)` — which lets a
/// commit that arrived *after* the capture be covered by that capture. That is the write-order
/// mutex the charter asks to be modelled, and block A below is the only place it fails. Dropping
/// the `ShortFlush` term fails block B; dropping the applied clamp fails block C.
///
/// Three blocks, one per term of the minimum, each written so that term alone decides the answer.
/// `M7F-18(a)` covers the short-flush term with the capture and the applied watermark both above
/// it, so it passes against a flush that ignores the capture entirely; that is the hole this row
/// closes.
#[retcd_test]
fn m7f_46_sync_wal_through_answers_under_the_write_order() {
    support::preamble();

    // The two types the seam is built out of, read by field. `Flush.captured` is an AppliedSeq
    // and `Flushed.durable` is a DurableSeq: you may ask to sync what is applied, and only what
    // comes back may advance a watermark (FA-3, B-R13). Swapping them stops this compiling.
    let capture = CapturedPrefix {
        partition: PARTITION,
        generation: GENERATION,
        through: AppliedSeq(3),
    };
    let StoreEffect::Flush { captured, .. } = (StoreEffect::Flush {
        ticket: FlushTicket(1),
        captured: vec![capture],
    }) else {
        panic!("a Flush is a Flush");
    };
    let asked: AppliedSeq = captured[0].through;
    assert_eq!(asked, AppliedSeq(3));

    // Block A — the capture decides. Captured at 3; seq 4 commits before the flush runs.
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=3 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("commit");
    }
    // The interleave: the capture is already taken, and this write is not in it.
    engine
        .commit(support::batch(1, 4, b"k", b"v"))
        .expect("commit 4 arrives after the capture");
    assert_eq!(
        engine.buffered_applied(PARTITION, GENERATION),
        AppliedSeq(4)
    );

    let flushed = engine
        .sync_wal_through(captured)
        .expect("a flush over the captured prefix");
    let StorageEvent::Flushed { durable, .. } = (StorageEvent::Flushed {
        ticket: FlushTicket(1),
        durable: flushed,
    }) else {
        panic!("a Flushed is a Flushed");
    };
    let answered: DurablePrefix = durable[0];
    let confirmed: DurableSeq = answered.through;
    assert_eq!(
        confirmed,
        DurableSeq(3),
        "a write that arrived after the capture cannot be covered by that capture"
    );
    assert_eq!(engine.durable(PARTITION, GENERATION), DurableSeq(3));

    // Block B — the short flush decides. Captured at 3, applied 4, the engine syncs only 2.
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=4 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("commit");
    }
    engine
        .inject(StorageOp::ShortFlush {
            node: NODE,
            through: AppliedSeq(2),
        })
        .expect("inject");
    let durable = engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: PARTITION,
            generation: GENERATION,
            through: AppliedSeq(3),
        }])
        .expect("a short flush is a real success");
    assert_eq!(
        durable[0].through,
        DurableSeq(2),
        "the least of captured 3, applied 4 and a flush that reached 2"
    );

    // Block C — the applied watermark decides. Captured at 5, only 4 is applied.
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=4 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("commit");
    }
    let durable = engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: PARTITION,
            generation: GENERATION,
            through: AppliedSeq(5),
        }])
        .expect("flush");
    assert_eq!(
        durable[0].through,
        DurableSeq(4),
        "asking to sync what is not applied cannot make it durable"
    );

    tracing::info!(
        captured = asked.0,
        confirmed = confirmed.0,
        "m7f_46 write order"
    );
}
