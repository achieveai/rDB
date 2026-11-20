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
//! | t61 A9a | the snapshot install twins (issue #61 S2): `install_begin` wipes every level in one step; `install_finish` reads as a lineage trimmed at its barrier, moves no durable mark, and survives a host crash; an installing lineage refuses commit and inherit and is cleared at open; a host crash before a sync covers the begin brings the old copy back |
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
use rdb_sim::storage::memory::{InstallEntry, InstallError, MemoryEngine};
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

// ---- issue #61 A9a: the snapshot install twins ----

const INSTALL_AT: u64 = 4;

/// Generation 1 committed through 3, and generation 2 inherited from it at 2 and committed at 3.
fn engine_with_two_levels() -> MemoryEngine {
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=3 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("commit");
    }
    engine
        .inherit(PARTITION, Generation(1), Generation(2), Seq(2))
        .expect("inherit");
    engine
        .commit(support::batch(2, 3, b"j", b"w"))
        .expect("commit");
    engine
}

/// The record and progress value T1 commits at `seq` of generation 3: a barrier the source
/// would send.
fn barrier(seq: u64) -> (Bytes, Bytes) {
    let history = rdb_sim::storage::history::canonical_history(
        rdb_core::contracts::authority::Lineage {
            partition: PARTITION,
            generation: Generation(3),
            owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
        },
        rdb_core::contracts::ids::ConfigVersion(1),
        seq,
    )
    .expect("a canonical history");
    let (record, progress) =
        rdb_sim::storage::history::history_writes(history.batch(seq), Seq(seq))
            .expect("the record");
    (record, progress.expect("the progress value"))
}

fn chunk_entry(key: &'static [u8], value: &'static [u8], version: Version) -> InstallEntry {
    (
        Namespace::User,
        Bytes::from_static(key),
        Bytes::from_static(value),
        version,
    )
}

#[retcd_test]
fn t61_sim_install_begin_wipes_every_level() {
    support::preamble();
    let mut engine = engine_with_two_levels();
    // Control: both levels are there to be wiped.
    assert_eq!(
        engine.buffered_applied(PARTITION, Generation(2)),
        AppliedSeq(3)
    );
    assert_eq!(engine.version_of(PARTITION, Namespace::User, b"k"), Some(3));

    // A begin for an older generation than the copy holds is refused and writes nothing.
    assert_eq!(
        engine.install_begin(PARTITION, Generation(1), Seq(INSTALL_AT)),
        Err(InstallError::NewerGeneration {
            held: Generation(2)
        })
    );
    assert_eq!(
        engine.buffered_applied(PARTITION, Generation(1)),
        AppliedSeq(3)
    );

    engine
        .install_begin(PARTITION, Generation(3), Seq(INSTALL_AT))
        .expect("begin");
    for level in [Generation(1), Generation(2)] {
        assert_eq!(engine.buffered_applied(PARTITION, level), AppliedSeq(0));
        assert_eq!(engine.parent(PARTITION, level), None);
    }
    assert_eq!(engine.version_of(PARTITION, Namespace::User, b"k"), None);
    assert_eq!(engine.version_of(PARTITION, Namespace::User, b"j"), None);
    assert_eq!(
        engine.installing(PARTITION, Generation(3)),
        Some((Generation(3), Seq(INSTALL_AT)))
    );
    let view = engine.snapshot(PARTITION, Generation(3), SnapshotHandle(1));
    assert_eq!(view.scan(Namespace::User, b"", 10), Vec::new());
}

#[retcd_test]
fn t61_sim_install_finish_reads_trimmed_moves_no_durable_and_survives_a_host_crash() {
    support::preamble();
    let mut engine = engine_with_two_levels();
    let (g, at) = (Generation(3), Seq(INSTALL_AT));
    engine.install_begin(PARTITION, g, at).expect("begin");

    // Entries the export never sends are refused, and nothing of the chunk lands.
    let history = (
        Namespace::History,
        Bytes::copy_from_slice(&1_u64.to_be_bytes()),
        Bytes::from_static(b"r"),
        1,
    );
    let head = (
        Namespace::Progress,
        Bytes::from_static(rdb_core::replication::append::PROGRESS_KEY),
        Bytes::from_static(b"p"),
        1,
    );
    for foreign in [history, head] {
        let ns = foreign.0;
        assert_eq!(
            engine.install_chunk(PARTITION, g, at, &[chunk_entry(b"k", b"s", 3), foreign]),
            Err(InstallError::ForeignEntry { ns })
        );
    }
    assert_eq!(engine.version_of(PARTITION, Namespace::User, b"k"), None);
    assert_eq!(
        engine.install_chunk(PARTITION, g, at, &[chunk_entry(b"k", b"s", 3)]),
        Ok(1)
    );

    // A barrier at another seq is refused, and the lineage stays installing.
    let (wrong, _) = barrier(INSTALL_AT - 1);
    let root = (Seq::ZERO, rdb_core::contracts::digest::Digest::ROOT);
    assert_eq!(
        engine.install_finish(PARTITION, g, at, &wrong, root),
        Err(InstallError::BadBarrier { at })
    );
    assert_eq!(engine.installing(PARTITION, g), Some((g, at)));

    let (record, progress) = barrier(INSTALL_AT);
    engine
        .install_finish(PARTITION, g, at, &record, root)
        .expect("finish");
    assert_eq!(engine.installing(PARTITION, g), None);
    assert_eq!(
        engine.buffered_applied(PARTITION, g),
        AppliedSeq(INSTALL_AT)
    );
    assert_eq!(
        engine.durable(PARTITION, g),
        DurableSeq(0),
        "install_finish moves no durable mark"
    );
    assert_eq!(engine.floor_seq(PARTITION, g), at);
    assert_eq!(
        engine.history_at(PARTITION, g, at),
        Some((record.clone(), Some(progress.clone()))),
        "the barrier record and the head progress T1 would commit beside it"
    );
    assert_eq!(engine.history_at(PARTITION, g, Seq(INSTALL_AT - 1)), None);
    let view = engine.snapshot(PARTITION, g, SnapshotHandle(1));
    assert_eq!(
        view.get(Namespace::User, b"k"),
        Some(Bytes::from_static(b"s"))
    );
    assert_eq!(view.version(Namespace::User, b"k"), Some(3));

    // A recovery inheriting at the barrier reads the installed state through the link.
    engine
        .inherit(PARTITION, g, Generation(4), at)
        .expect("inherit at the barrier");
    let child = engine.snapshot(PARTITION, Generation(4), SnapshotHandle(3));
    assert_eq!(
        child.get(Namespace::User, b"k"),
        Some(Bytes::from_static(b"s"))
    );

    // The lineage takes commits again; an unsynced one above the barrier is lost by a host
    // crash, the install is not.
    engine
        .commit(support::batch(3, INSTALL_AT + 1, b"k", b"t"))
        .expect("a commit after the install");
    let reopened = CrashImage::of(&engine, StorageFault::HostCrash)
        .expect("image")
        .reopen(NODE);
    assert_eq!(
        reopened.buffered_applied(PARTITION, g),
        AppliedSeq(INSTALL_AT)
    );
    assert_eq!(reopened.durable(PARTITION, g), DurableSeq(0));
    assert_eq!(reopened.floor_seq(PARTITION, g), at);
    assert_eq!(
        reopened.history_at(PARTITION, g, at),
        Some((record, Some(progress)))
    );
    let view = reopened.snapshot(PARTITION, g, SnapshotHandle(2));
    assert_eq!(
        view.get(Namespace::User, b"k"),
        Some(Bytes::from_static(b"s"))
    );
    assert_eq!(view.version(Namespace::User, b"k"), Some(3));
}

#[retcd_test]
fn t61_sim_an_installing_lineage_refuses_commit_and_inherit_and_is_cleared_at_open() {
    support::preamble();
    let mut engine = engine_with_two_levels();
    let (g, at) = (Generation(3), Seq(INSTALL_AT));
    engine.install_begin(PARTITION, g, at).expect("begin");
    engine
        .install_chunk(PARTITION, g, at, &[chunk_entry(b"k", b"s", 3)])
        .expect("chunk");

    assert_eq!(
        engine.commit(support::batch(3, 1, b"k", b"x")),
        Err(StorageFault::WriteFailed)
    );
    assert_eq!(engine.buffered_applied(PARTITION, g), AppliedSeq(0));
    assert_eq!(engine.version_of(PARTITION, Namespace::User, b"k"), Some(3));
    let refused = Err(rdb_sim::SimError::Config {
        field: "inherit_installing",
    });
    assert_eq!(
        engine.inherit(PARTITION, g, Generation(4), Seq(0)),
        refused,
        "nothing inherits from a half-written view"
    );
    assert_eq!(engine.parent(PARTITION, Generation(4)), None);
    assert_eq!(
        engine.inherit(PARTITION, Generation(1), g, Seq(0)),
        refused,
        "nothing links under an installing lineage"
    );
    assert_eq!(engine.parent(PARTITION, g), None);

    // A crash mid install: the reopened engine clears it and the copy rejoins empty.
    let reopened = CrashImage::of(&engine, StorageFault::ProcessCrash)
        .expect("image")
        .reopen(NODE);
    assert_eq!(reopened.installing(PARTITION, g), None);
    assert_eq!(reopened.version_of(PARTITION, Namespace::User, b"k"), None);
    assert_eq!(reopened.buffered_applied(PARTITION, g), AppliedSeq(0));
    let mut reopened = reopened;
    assert_eq!(
        reopened.commit(support::batch(3, 1, b"k", b"x")),
        Ok(AppliedSeq(1)),
        "the cleared lineage takes writes again"
    );
}

#[retcd_test]
fn t61_sim_a_host_crash_after_an_unsynced_begin_brings_the_old_copy_back() {
    support::preamble();
    let mut engine = engine_with_two_levels();
    let synced = engine
        .sync_wal_through(
            [Generation(1), Generation(2)]
                .into_iter()
                .map(|generation| CapturedPrefix {
                    partition: PARTITION,
                    generation,
                    through: AppliedSeq(3),
                })
                .collect(),
        )
        .expect("flush");
    assert_eq!(synced.len(), 2, "both levels are durable through 3");
    let (g, at) = (Generation(3), Seq(INSTALL_AT));
    engine.install_begin(PARTITION, g, at).expect("begin");
    engine
        .install_chunk(PARTITION, g, at, &[chunk_entry(b"k", b"s", 3)])
        .expect("chunk");

    // No sync covered the begin: the host loses it with its wipe.
    let reopened = CrashImage::of(&engine, StorageFault::HostCrash)
        .expect("image")
        .reopen(NODE);
    assert_eq!(reopened.installing(PARTITION, g), None);
    for level in [Generation(1), Generation(2)] {
        assert_eq!(reopened.buffered_applied(PARTITION, level), AppliedSeq(3));
        assert_eq!(reopened.durable(PARTITION, level), DurableSeq(3));
    }
    assert_eq!(
        reopened.parent(PARTITION, Generation(2)),
        Some(Generation(1))
    );
    assert_eq!(reopened.base(PARTITION, Generation(2)), Seq(2));
    let view = reopened.snapshot(PARTITION, Generation(2), SnapshotHandle(1));
    assert_eq!(
        view.get(Namespace::User, b"k"),
        Some(Bytes::from_static(b"v"))
    );
    assert_eq!(view.version(Namespace::User, b"k"), Some(2));
    assert_eq!(
        view.get(Namespace::User, b"j"),
        Some(Bytes::from_static(b"w"))
    );

    // Control: once a sync covers the begin, a host crash keeps the wipe, and the open clears
    // the unfinished install.
    engine.sync_wal_through(Vec::new()).expect("flush");
    let reopened = CrashImage::of(&engine, StorageFault::HostCrash)
        .expect("image")
        .reopen(NODE);
    assert_eq!(reopened.installing(PARTITION, g), None);
    for level in [Generation(1), Generation(2), g] {
        assert_eq!(reopened.buffered_applied(PARTITION, level), AppliedSeq(0));
    }
    assert_eq!(reopened.version_of(PARTITION, Namespace::User, b"k"), None);
}

// ---- issue #61 S3: the bytes the retention cap searches ----

/// `history_bytes` (the twin of the proposed `RocksEngine::history_bytes`) counts the History a
/// lineage shows from a seq through its head: it falls as the seq rises and is 0 above the head;
/// below a trim floor nothing counts, so asking from 1 and from the floor agree; a linked child
/// counts its parent's records through the link. Red if the floor is ignored (bytes from 1 stay
/// above bytes from the floor), or the link is not read (the child's bytes are 0).
#[retcd_test]
fn t61_sim_history_bytes_fall_with_the_seq_and_skip_the_trimmed() {
    use rdb_core::contracts::authority::Lineage;
    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::ids::{ConfigVersion, OwnerEpoch};
    use rdb_sim::storage::memory::TrimFloor;
    support::preamble();
    let history = rdb_sim::storage::history::canonical_history(
        Lineage {
            partition: PARTITION,
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
        },
        ConfigVersion(1),
        6,
    )
    .expect("a canonical history");
    let mut engine = MemoryEngine::new(NODE);
    for batch in &history.batches {
        engine.commit(batch.clone()).expect("commit");
    }
    let from = |engine: &MemoryEngine, generation: u64, seq: u64| {
        engine.history_bytes(PARTITION, Generation(generation), Seq(seq))
    };
    let all: Vec<u64> = (1..=7).map(|seq| from(&engine, 1, seq)).collect();
    assert!(all[0] > 0, "six records hold bytes: {all:?}");
    assert!(
        all.windows(2).all(|pair| pair[0] > pair[1]),
        "each record raises the count: {all:?}"
    );
    assert_eq!(all[6], 0, "nothing above the head");

    engine
        .trim_history(
            PARTITION,
            Generation(1),
            TrimFloor {
                seq: Seq(3),
                root_base: Seq::ZERO,
                root_digest: Digest::ROOT,
            },
        )
        .expect("a trim");
    assert_eq!(
        from(&engine, 1, 1),
        from(&engine, 1, 3),
        "nothing below the floor counts"
    );
    assert_eq!(from(&engine, 1, 3), all[2], "from the floor up, unchanged");

    engine
        .inherit(PARTITION, Generation(1), Generation(2), Seq(6))
        .expect("link generation 2 at 6");
    assert_eq!(
        from(&engine, 2, 4),
        all[3],
        "the child counts its parent's records through the link"
    );
}
