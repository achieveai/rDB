//! Every kernel module's [`TimerId`]s sit in a block no other module's ids can reach.
//!
//! Not a plan row. A timer id is a bare `u64`, and a host keys its timer table by it, so two
//! modules sharing an id would make one module's re-arm stale the other's firing. The blocks
//! used to be `0x10_0000` apart with the partition added on top, so P1's timer for partition
//! `0x20_0000` *was* F1's discovery timer.
//!
//! The claim is structural, so it holds for every partition and not only the sampled ones:
//! a module's ids all carry that module's tag in bits 48..64, no two modules share a tag, and a
//! per-partition timer carries its partition in bits 0..32.

use config_log::retcd_test;
use rdb_core::authority::AuthorityTimer;
use rdb_core::contracts::ids::{PartitionId, TimerId};
use rdb_core::protection::HEALTH_EVAL_TIMER;
use rdb_core::publication::post_apply_timer;
use rdb_core::recovery::DISCOVERY_TIMER;
use rdb_core::replication::catchup::retransmit_timer;
use rdb_core::replication::primary::keepalive_timer;

/// Partitions at the edges of the old `0x10_0000` spacing, and the ends of the range.
const PARTITIONS: [u32; 7] = [0, 1, 0xF_FFFF, 0x10_0000, 0x20_0000, 0x30_0000, u32::MAX];

/// A per-partition timer constructor.
type PerPartition = fn(PartitionId) -> TimerId;

fn tag(id: TimerId) -> u64 {
    id.0 >> 48
}

/// Each module's ids, named. A module that gains a timer adds it here.
fn modules() -> Vec<(&'static str, Vec<TimerId>)> {
    let per_partition = |f: PerPartition| {
        PARTITIONS
            .iter()
            .map(|&p| f(PartitionId(p)))
            .collect::<Vec<_>>()
    };
    vec![
        ("A1", AuthorityTimer::ALL.iter().map(|k| k.id()).collect()),
        ("L1", vec![HEALTH_EVAL_TIMER]),
        ("P1", per_partition(post_apply_timer)),
        ("F1", vec![DISCOVERY_TIMER]),
        (
            "R1",
            [
                per_partition(keepalive_timer),
                per_partition(retransmit_timer),
            ]
            .concat(),
        ),
    ]
}

#[retcd_test]
fn no_partition_turns_one_modules_timer_into_anothers() {
    assert_ne!(
        post_apply_timer(PartitionId(0x20_0000)),
        DISCOVERY_TIMER,
        "P1's timer for partition 0x20_0000 is F1's discovery timer"
    );

    let modules = modules();
    for (name, ids) in &modules {
        let first = tag(ids[0]);
        assert_ne!(first, 0, "{name}: no module tag in bits 48..64");
        for id in ids {
            assert_eq!(tag(*id), first, "{name}: {id:?} leaves its block");
        }
    }
    for (i, (a, a_ids)) in modules.iter().enumerate() {
        for (b, b_ids) in &modules[i + 1..] {
            assert_ne!(tag(a_ids[0]), tag(b_ids[0]), "{a} and {b} share a tag");
        }
    }
}

#[retcd_test]
fn a_per_partition_timer_carries_its_partition_in_the_low_32_bits() {
    let kinds: [(&str, PerPartition); 3] = [
        ("P1 post-apply", post_apply_timer),
        ("R1 keepalive", keepalive_timer),
        ("R1 retransmit", retransmit_timer),
    ];
    for (name, timer) in kinds {
        for p in PARTITIONS {
            let id = timer(PartitionId(p));
            assert_eq!(id.0 & 0xFFFF_FFFF, u64::from(p), "{name} {p:#x}");
            assert_eq!(id.0 >> 32, timer(PartitionId(0)).0 >> 32, "{name} {p:#x}");
        }
    }
    // R1's two kinds share a tag, so the kind bits alone keep them apart.
    assert_ne!(
        keepalive_timer(PartitionId(0)).0 >> 32,
        retransmit_timer(PartitionId(0)).0 >> 32
    );
}
