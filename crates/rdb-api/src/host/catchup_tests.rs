//! Host catch-up and landing rows: inventory, `land_link`, the missed-generation links and
//! the D7 catch-up wiring. Moved out of `host.rs` (M10 S2 event 0).

use super::trio::{
    current_thread, host_over, logged, write_chain, write_chain_after, write_chain_in, Trio,
};
use super::*;
// `host.rs` imports `TrimFloor` only under `debug_assertions`, but these rows also build in
// the gate's release check (`cargo check --release --tests`).
use rdb_storage::TrimFloor;

/// M9 serves only an empty copy, so a copy that holds anything must answer
/// `InventoryFailed`, never an invented empty inventory: F1 would select a prefix that
/// drops what the copy holds. Two ways to hold something: an anchor past the root, and a
/// lineage in the engine. Before bootstrap node 2 is empty and reports the root of the
/// bootstrap's generation-0 anchor. M10: a later generation is a reopen's, answered only
/// from what the copy stores, so an empty copy asked for one answers `InventoryFailed`.
#[test]
fn a_copy_that_holds_anything_never_reports_an_empty_inventory() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    let root = LineageAnchor {
        lineage: Lineage {
            partition: PartitionId(1),
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
        },
        base_seq: Seq::ZERO,
        base_digest: Digest::ROOT,
    };
    let past_root = LineageAnchor {
        base_seq: Seq(1),
        ..root
    };
    let bootstrap = LineageAnchor {
        lineage: Lineage {
            generation: Generation(0),
            owner_epoch: OwnerEpoch(0),
            ..root.lineage
        },
        ..root
    };
    let report = |trio: &mut Trio, anchor| {
        trio.nodes[1]
            .0
            .inventory(PartitionId(1), CopyId(1), anchor, None, None)
    };

    assert!(
        matches!(
            report(&mut trio, bootstrap),
            RecoveryEvent::InventoryReported(_)
        ),
        "an empty copy reports the root"
    );
    assert_eq!(
        report(
            &mut trio,
            LineageAnchor {
                base_seq: Seq(1),
                ..bootstrap
            }
        ),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) },
        "an anchor past the root"
    );
    assert_eq!(
        report(&mut trio, root),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) },
        "an empty copy asked for a reopen's generation 1"
    );
    trio.bootstrap();
    trio.ready();
    assert_eq!(
        report(&mut trio, bootstrap),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) },
        "a copy holding generation 1, asked for the bootstrap's empty anchor"
    );
    // M10 (reopen): a copy holding the anchor's generation reports from its engine, and
    // one whose stored base is not the anchor's answers nothing.
    assert!(
        matches!(report(&mut trio, root), RecoveryEvent::InventoryReported(_)),
        "a copy holding generation 1 at its anchor"
    );
    assert_eq!(
        report(&mut trio, past_root),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) },
        "a copy whose generation 1 starts elsewhere"
    );
}

/// A reopen's anchor at `generation`, from `base` with `digest` there.
fn anchor_at(generation: u64, base: u64, digest: Digest) -> LineageAnchor {
    LineageAnchor {
        lineage: Lineage {
            partition: PartitionId(1),
            generation: Generation(generation),
            owner_epoch: OwnerEpoch(generation),
        },
        base_seq: Seq(base),
        base_digest: digest,
    }
}

/// A-R177 row 2 (critic-f5 C1-C4): a copy that missed generation 2, written nowhere, holds
/// generation 1 exactly through the anchor's base with the anchor's digest there. Its
/// inventory links generation 2 onto generation 1 at the base and reports from it.
#[test]
fn a_copy_that_missed_a_writeless_generation_links_it_at_inventory() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        2,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    match host.inventory(
        PartitionId(1),
        CopyId(1),
        anchor_at(2, 2, digests[1]),
        None,
        None,
    ) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(2), digests[1]));
        }
        other => panic!("expected a report, got {other:?}"),
    }
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(2)),
        Some(rdb_storage::Link {
            parent: Generation(1),
            base: Seq(2),
            copied: false,
        })
    );
}

/// A-R374 (#59, the host row for the Gs == G-1 jump; critic-f5 row 7): a copy that missed
/// generations 2 and 3, both written nowhere, holds generation 1 exactly through the anchor's
/// base. Its inventory for generation 4 links 4 straight onto 1 at the base, skipping the two it
/// never saw, and reports from it. The simulator cannot reach this: it lands every generation
/// for a member in order (B-R56a.3).
#[test]
fn a_copy_that_missed_two_writeless_generations_links_the_newest_onto_its_own() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        2,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    match host.inventory(
        PartitionId(1),
        CopyId(1),
        anchor_at(4, 2, digests[1]),
        None,
        None,
    ) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(2), digests[1]));
        }
        other => panic!("expected a report, got {other:?}"),
    }
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(4)),
        Some(rdb_storage::Link {
            parent: Generation(1),
            base: Seq(2),
            copied: false,
        })
    );
}

/// A commit into generation 2 at `(cutoff, digest)`, as node 2 hears it: a trio's
/// bootstrap commit, moved on a generation. Its pin names node 2 a secondary.
fn landing_at(cutoff: u64, digest: Digest) -> RecoveryResult {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.until("node 1 committed", |trio| {
        trio.nodes[0].0.committed.get(&PartitionId(1)).cloned()
    });
    let mut result = (*trio.nodes[0].0.committed[&PartitionId(1)]).clone();
    result.new_generation = Generation(2);
    result.selected.root.generation = Generation(2);
    result.selected.cutoff_seq = Seq(cutoff);
    result.selected.cutoff_digest = digest;
    result.retained_status_map.predecessor_generation = Generation(1);
    result
}

/// A-R217 rows: node 1's host, the pinned primary of [`landing_at`]'s commit, over the engine
/// at `path`, with an open bound, so a landing it links can go on to `activate`.
#[cfg(debug_assertions)]
fn primary_over(path: &std::path::Path, rt: &tokio::runtime::Runtime) -> Host {
    let engine = RocksEngine::open(path).expect("open engine");
    let links = Links::new();
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let control = ControlAdapter::new(store, rt.handle().clone(), Arc::clone(&links));
    let mut host = Host::new(NodeId(1), engine, links, control, HostClock::start());
    host.instance = Some([7_u8; INSTANCE_LEN]);
    host
}

/// The newest generation `host` asked the open counter to mark activated for partition 1.
#[cfg(debug_assertions)]
fn asked(host: &Host) -> Generation {
    host.activation
        .get(&PartitionId(1))
        .map_or(Generation::default(), |activation| activation.asked)
}

/// Ruling A-R217 (review-s1 F-001), row 1: the path through `inherit`, which `recovered`
/// takes on the copy that ran F1 and `land` takes on a barrier copy. The link is synced
/// before `activate` may run: with the WAL sync failing, `recovered` faults and nothing
/// asks the open counter to mark generation 2. Without the fault the same landing reaches
/// `activate`, so the refusal is the sync's doing. Cut: `sync_link` returns before syncing;
/// the armed landing is `Ok` and asks for generation 2, red. Unit, under a second.
#[cfg(debug_assertions)]
#[test]
fn a_link_that_cannot_be_synced_never_reaches_activate_through_inherit() {
    let rt = current_thread();
    let site = Site {
        partition: PartitionId(1),
        correlation: CorrelationId(0),
    };
    for armed in [true, false] {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        let digests = write_chain(
            &mut RocksEngine::open(&path).expect("open engine"),
            Generation(1),
            2,
        );
        let result = landing_at(2, digests[1]);
        let mut host = primary_over(&path, &rt);
        if armed {
            host.engine
                .inject_fault(rdb_storage::InjectedFault::WalFlush);
            let outcome = host.recovered(&result, site);
            assert!(
                outcome
                    .as_ref()
                    .is_err_and(|fault| fault.detail.contains("sync the link")),
                "a failed link sync faults the node: {outcome:?}"
            );
            assert_eq!(asked(&host), Generation::default(), "activate never ran");
        } else {
            host.recovered(&result, site).expect("recovered");
            assert_eq!(asked(&host), Generation(2), "the landing reaches activate");
        }
    }
}

/// Ruling A-R217 (review-s1 F-001), row 2: the path through `land_link`, which `land` takes
/// on a copy outside the barrier. That link is synced before `activate` may run too. The
/// pinned primary lands here outside an empty barrier, so `schedule_acquire` would ask
/// for generation 2 next. Cut: the `sync_link` after `land_link` is dropped; the armed
/// landing is `Ok` and asks for generation 2, red. Unit, under a second.
#[cfg(debug_assertions)]
#[test]
fn a_link_that_cannot_be_synced_never_reaches_activate_through_land_link() {
    let rt = current_thread();
    let site = Site {
        partition: PartitionId(1),
        correlation: CorrelationId(0),
    };
    for armed in [true, false] {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        let digests = write_chain(
            &mut RocksEngine::open(&path).expect("open engine"),
            Generation(1),
            2,
        );
        let mut result = landing_at(2, digests[1]);
        result.barrier = rdb_core::contracts::recovery::RecoveryBarrier::try_new(
            &[],
            &BTreeSet::new(),
            Seq(2),
            digests[1],
        )
        .expect("an empty barrier");
        assert!(!in_barrier(&result, NodeId(1)), "node 1 is outside it");
        let mut host = primary_over(&path, &rt);
        if armed {
            host.engine
                .inject_fault(rdb_storage::InjectedFault::WalFlush);
            let outcome = host.land(Box::new(result), site, NodeId(3));
            assert!(
                outcome
                    .as_ref()
                    .is_err_and(|fault| fault.detail.contains("sync the link")),
                "a failed link sync faults the node: {outcome:?}"
            );
            assert_eq!(asked(&host), Generation::default(), "activate never ran");
        } else {
            host.land(Box::new(result), site, NodeId(3)).expect("land");
            assert_eq!(
                host.engine.link(PartitionId(1), Generation(2)),
                Some(rdb_storage::Link {
                    parent: Generation(1),
                    base: Seq(2),
                    copied: false,
                }),
                "land_link linked generation 2"
            );
            assert_eq!(asked(&host), Generation(2), "the landing reaches activate");
        }
    }
}

/// Ruling A-R185 (critic-d7fix (c)): a copy that lands a commit outside its barrier while it
/// holds its older generation exactly through the cutoff, with the cutoff digest there,
/// links the new generation at the cutoff, gets the receiver a barrier copy gets (A-R185a),
/// and answers the `CopyCaughtUp` that `land` sends to F1. Cut: the receiver install is
/// dropped, and R1 holds none. Unit, about 2.5 s (the trio's bootstrap window).
#[config_log::retcd_test]
fn a_copy_that_lands_outside_its_barrier_links_at_the_cutoff_and_reports_caught_up() {
    const METHOD: &str =
        "a_copy_that_lands_outside_its_barrier_links_at_the_cutoff_and_reports_caught_up";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        2,
    );
    let result = landing_at(2, digests[1]);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let copy = result
        .committed
        .pinned_config
        .members
        .iter()
        .find(|member| member.node == NodeId(2))
        .expect("node 2 is pinned")
        .copy;
    assert_eq!(
        host.land_link(PartitionId(1), &result).expect("no fault"),
        Some(RecoveryEvent::CopyCaughtUp {
            copy,
            head: Seq(2),
            digest: digests[1],
        })
    );
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(2)),
        Some(rdb_storage::Link {
            parent: Generation(1),
            base: Seq(2),
            copied: false,
        })
    );
    let receiver = host
        .replication
        .receiver(NodeId(2), PartitionId(1))
        .expect("a receiver at the cutoff");
    assert_eq!(
        (receiver.lineage().generation, receiver.applied_head().seq),
        (Generation(2), Seq(2))
    );
    assert_eq!(logged!(METHOD, "land_link_inherited").len(), 1);
    assert_eq!(logged!(METHOD, "land_receiver_installed").len(), 1);
}

/// Ruling A-R185 rows 3 and 4 (critic-d7fix C2): a copy holding past the cutoff (data an
/// ask must not truncate), or holding a different digest at it, links nothing, installs no
/// receiver, answers nothing to send, and logs `land_link_refused` with the reason; the
/// host does not fault. Unit, about 2.5 s.
#[config_log::retcd_test]
fn a_copy_that_lands_outside_its_barrier_past_the_cutoff_or_off_its_digest_links_nothing() {
    const METHOD: &str =
        "a_copy_that_lands_outside_its_barrier_past_the_cutoff_or_off_its_digest_links_nothing";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        3,
    );
    let past = landing_at(2, digests[1]);
    let mut off = past.clone();
    off.selected.cutoff_seq = Seq(3);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    for result in [&past, &off] {
        assert_eq!(host.land_link(PartitionId(1), result), Ok(None));
        assert_eq!(host.engine.lineages(), held, "nothing linked");
        assert!(host
            .replication
            .receiver(NodeId(2), PartitionId(1))
            .is_none());
        assert_eq!(host.fault, None);
    }
    let reasons: Vec<_> = logged!(METHOD, "land_link_refused")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        reasons,
        ["held_past_base", "base_digest_differs"].map(serde_json::Value::from)
    );
}

/// Arm 3 (arch-s2 `land_at`, A-R224 item 3): a copy that lands a commit outside its barrier
/// while it holds less than the cutoff links the new generation at its own head, gets a
/// receiver there, and answers nothing to send: its `Recovered` asks the primary for the
/// rest. Cut: `land_at` refuses below the base, as slice 1 did; nothing links, red. Unit,
/// about 2.5 s (the trio's bootstrap window).
#[config_log::retcd_test]
fn a_copy_that_lands_outside_its_barrier_below_the_cutoff_links_at_its_own_head() {
    const METHOD: &str =
        "a_copy_that_lands_outside_its_barrier_below_the_cutoff_links_at_its_own_head";
    let full = config_testkit::fs::temp_dir();
    let digests = write_chain(
        &mut RocksEngine::open(full.path().join("node")).expect("open engine"),
        Generation(1),
        2,
    );
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        1,
    );
    let result = landing_at(2, digests[1]);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    assert_eq!(
        host.land_link(PartitionId(1), &result).expect("no fault"),
        None,
        "not caught up"
    );
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(2)),
        Some(rdb_storage::Link {
            parent: Generation(1),
            base: Seq(1),
            copied: false,
        })
    );
    let receiver = host
        .replication
        .receiver(NodeId(2), PartitionId(1))
        .expect("a receiver at its head");
    assert_eq!(
        (
            receiver.lineage().generation,
            receiver.applied_head().seq,
            receiver.applied_head().digest
        ),
        (Generation(2), Seq(1), digests[0])
    );
    let behind: Vec<_> = logged!(METHOD, "land_link_behind")
        .into_iter()
        .map(|line| (line["head"].clone(), line["base"].clone()))
        .collect();
    assert_eq!(behind, [(1, 2)].map(|(h, b)| (h.into(), b.into())));
    assert!(logged!(METHOD, "land_link_refused").is_empty());
    assert_eq!(host.fault, None);
}

/// review-r185 ADV2: the refusals no row reached, each by name. A copy already holding the new
/// generation, or one the pin does not name, logs `land_link_refused` at warn; none links,
/// installs a receiver or faults the host. #69 changed this row: a copy holding nothing of the
/// partition used to be refused here at info (`anchor_generation_absent`, ADV3) and now lands
/// ([`m10_bb_69_a_copy_holding_nothing_lands_as_head_zero_at_the_root`]). Unit.
#[config_log::retcd_test]
fn a_landing_with_nothing_to_link_is_refused_by_name() {
    const METHOD: &str = "a_landing_with_nothing_to_link_is_refused_by_name";
    let result = landing_at(0, Digest::ROOT);
    let mut outside = result.clone();
    outside
        .committed
        .pinned_config
        .members
        .retain(|member| member.node != NodeId(2));
    let rt = current_thread();
    let empty = config_testkit::fs::temp_dir();
    let mut fresh = host_over(&empty.path().join("node"), &rt);
    let ahead = config_testkit::fs::temp_dir();
    let path = ahead.path().join("node");
    write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(2),
        1,
    );
    let mut holding = host_over(&path, &rt);
    let refuses = |host: &mut Host, result: &RecoveryResult| {
        let held = host.engine.lineages();
        assert_eq!(host.land_link(PartitionId(1), result), Ok(None));
        assert_eq!(host.engine.lineages(), held, "nothing linked");
        assert!(host
            .replication
            .receiver(NodeId(2), PartitionId(1))
            .is_none());
        assert_eq!(host.fault, None);
    };
    refuses(&mut holding, &result);
    refuses(&mut fresh, &outside);
    let refused: Vec<_> = logged!(METHOD, "land_link_refused")
        .into_iter()
        .map(|line| (line["reason"].clone(), line["@l"].clone()))
        .collect();
    assert_eq!(
        refused,
        [
            ("held_at_or_above_generation", "Warning"),
            ("not_a_member", "Warning"),
        ]
        .map(|(reason, level)| (
            serde_json::Value::from(reason),
            serde_json::Value::from(level)
        ))
    );
}

/// #69 (critic-w9 option (c)): a copy holding no lineage of the partition, as one missed at
/// formation does, lands a commit outside its barrier as head 0 at the root, from generation 0.
/// At cutoff 0 (arm 2) it links at base 0, gets a receiver at the root and answers
/// `CopyCaughtUp`. Above it (arm 3) it links behind the cutoff at head 0 and answers nothing, so
/// its `Recovered` asks R1 for the rest. A copy holding any lineage, here only a newer one, is
/// not empty: `land_at` refuses it by name and links nothing.
/// Cuts: `land_at` refuses an empty copy, as before #69, red (nothing links); the empty arm
/// taken whatever the copy holds, red (generation 2 linked under a copy holding generation 3).
/// Unit, about 2.5 s (the trio's bootstrap window).
#[config_log::retcd_test]
fn m10_bb_69_a_copy_holding_nothing_lands_as_head_zero_at_the_root() {
    const METHOD: &str = "m10_bb_69_a_copy_holding_nothing_lands_as_head_zero_at_the_root";
    let at_root = landing_at(0, Digest::ROOT);
    // Arm 3 never reads the cutoff digest (`land_at`), so the root's stands in for one.
    let mut above = at_root.clone();
    above.selected.cutoff_seq = Seq(2);
    let rt = current_thread();
    let empty = |dir: &std::path::Path| host_over(&dir.join("node"), &rt);
    let root_dir = config_testkit::fs::temp_dir();
    let above_dir = config_testkit::fs::temp_dir();
    let linked_from_root = Some(rdb_storage::Link {
        parent: Generation(0),
        base: Seq(0),
        copied: false,
    });

    let mut host = empty(root_dir.path());
    let answered = host.land_link(PartitionId(1), &at_root).expect("no fault");
    assert!(
        matches!(
            answered,
            Some(RecoveryEvent::CopyCaughtUp { head: Seq(0), digest, .. }) if digest == Digest::ROOT
        ),
        "caught up at the root: {answered:?}"
    );
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(2)),
        linked_from_root
    );
    let receiver = host
        .replication
        .receiver(NodeId(2), PartitionId(1))
        .expect("a receiver at the root");
    assert_eq!(
        (
            receiver.lineage().generation,
            receiver.applied_head().seq,
            receiver.applied_head().digest
        ),
        (Generation(2), Seq(0), Digest::ROOT)
    );
    assert_eq!(host.fault, None);

    let mut host = empty(above_dir.path());
    assert_eq!(
        host.land_link(PartitionId(1), &above).expect("no fault"),
        None,
        "behind the cutoff, not caught up"
    );
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(2)),
        linked_from_root
    );
    let behind: Vec<_> = logged!(METHOD, "land_link_behind")
        .into_iter()
        .map(|line| (line["head"].clone(), line["base"].clone()))
        .collect();
    assert_eq!(behind, [(0, 2)].map(|(h, b)| (h.into(), b.into())));
    assert_eq!(logged!(METHOD, "land_link_inherited").len(), 1);
    assert!(logged!(METHOD, "land_link_refused").is_empty());

    let newer = config_testkit::fs::temp_dir();
    let path = newer.path().join("node");
    write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(3),
        1,
    );
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    let anchor = LineageAnchor {
        lineage: at_root.selected.root,
        base_seq: Seq(0),
        base_digest: Digest::ROOT,
    };
    assert_eq!(
        host.land_at(PartitionId(1), anchor),
        Err("anchor_generation_absent")
    );
    assert_eq!(host.engine.lineages(), held, "nothing linked");

    // No lineage, but publish marks stored for the partition: not empty (critic-bb M9).
    let marked = config_testkit::fs::temp_dir();
    let path = marked.path().join("node");
    RocksEngine::open(&path)
        .expect("open engine")
        .put_publish_marks(
            PartitionId(1),
            &[rdb_storage::PublishMark {
                generation: Generation(1),
                seq: Seq(1),
                tick: Tick(1),
            }],
        )
        .expect("put marks");
    let mut host = host_over(&path, &rt);
    assert_eq!(
        host.land_at(PartitionId(1), anchor),
        Err("publish_marks_held")
    );
    assert!(host.engine.lineages().is_empty(), "nothing linked");
}

/// #59 item 5 (review-r185 ADV2), the receiver arm of a landing outside the barrier. (a) The
/// pin names this copy its primary: it links, holds no receiver, answers nothing to send and
/// logs `land_receiver_primary` at info; it does not fault (the A-R217 `land_link` row lands
/// the primary this way). (b) The pin names it a secondary but the receiver still refuses: the
/// commit's root and pin name different partitions, a broken commit, so the node faults by name
/// (`land_link_receiver_refused`), as `inherit` does.
/// Cuts: no primary arm, red (a faults); the refusal logs and answers `Ok(None)` as before, red
/// (b does not fault). Unit, about 2.5 s (the trio's bootstrap window).
#[config_log::retcd_test]
fn m10_s2_59_a_landing_whose_receiver_is_refused_faults_by_name() {
    const METHOD: &str = "m10_s2_59_a_landing_whose_receiver_is_refused_faults_by_name";
    let rt = current_thread();
    let primary_dir = config_testkit::fs::temp_dir();
    let broken_dir = config_testkit::fs::temp_dir();
    let mut hosts = [&primary_dir, &broken_dir].map(|dir| {
        let path = dir.path().join("node");
        let digests = write_chain(
            &mut RocksEngine::open(&path).expect("open engine"),
            Generation(1),
            2,
        );
        (host_over(&path, &rt), digests[1])
    });
    let mut primary = landing_at(2, hosts[0].1);
    let mut broken = primary.clone();
    for member in &mut primary.committed.pinned_config.members {
        if member.node == NodeId(2) {
            member.role = ReplicaRole::Primary;
        }
    }
    broken.committed.pinned_config.partition = PartitionId(9);
    let [(primary_host, _), (broken_host, _)] = &mut hosts;
    assert_eq!(primary_host.land_link(PartitionId(1), &primary), Ok(None));
    assert_eq!(logged!(METHOD, "land_receiver_primary").len(), 1);
    let fault = broken_host
        .land_link(PartitionId(1), &broken)
        .expect_err("a refused receiver faults");
    assert!(
        fault
            .detail
            .starts_with("land_link_receiver_refused p1 g2: "),
        "{fault:?}"
    );
    for (host, _) in &hosts {
        assert!(host
            .replication
            .receiver(NodeId(2), PartitionId(1))
            .is_none());
    }
}

/// #59 item 7 (arch-s2 arm 2, critic-d7fix follow-up A2): two commits land outside the barrier
/// in one boot, each at the cutoff. The first installs a receiver at generation 2. The second
/// links generation 3 onto 2 and keeps that live receiver (`land_receiver_kept`), which its
/// `Recovered` re-anchors from its own ladder, and still answers `CopyCaughtUp`.
/// Cut: the live receiver is replaced like a dormant one, red (generation 3 installed).
/// Unit, about 2.5 s (the trio's bootstrap window).
#[config_log::retcd_test]
fn m10_s2_59_a_second_landing_in_one_boot_keeps_the_live_receiver() {
    const METHOD: &str = "m10_s2_59_a_second_landing_in_one_boot_keeps_the_live_receiver";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        2,
    );
    let first = landing_at(2, digests[1]);
    let mut second = first.clone();
    second.new_generation = Generation(3);
    second.selected.root.generation = Generation(3);
    second.retained_status_map.predecessor_generation = Generation(2);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    for result in [&first, &second] {
        assert!(
            matches!(
                host.land_link(PartitionId(1), result),
                Ok(Some(RecoveryEvent::CopyCaughtUp { head: Seq(2), .. }))
            ),
            "generation {} caught up at the cutoff",
            result.new_generation.0
        );
    }
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(3)),
        Some(rdb_storage::Link {
            parent: Generation(2),
            base: Seq(2),
            copied: false,
        })
    );
    let receiver = host
        .replication
        .receiver(NodeId(2), PartitionId(1))
        .expect("a receiver");
    assert_eq!(receiver.lineage().generation, Generation(2), "kept");
    let kept: Vec<_> = logged!(METHOD, "land_receiver_kept")
        .into_iter()
        .map(|line| {
            (
                line["generation"].clone(),
                line["kept_generation"].clone(),
                line["quarantined"].clone(),
            )
        })
        .collect();
    assert_eq!(kept, [(3.into(), 2.into(), false.into())]);
    assert_eq!(logged!(METHOD, "land_receiver_installed").len(), 1);
}

/// #59 item 4 (A-R422 F1): F1 may sync a copy after the commit before the copy lands; the
/// sync waits in `deferred_syncs` and is answered at the landing. That sync is the one that
/// ends the landing's `CopyCaughtUp` re-sends, so the re-send is kept before it is answered:
/// with the first send lost, the landing ends with no re-send left (`caught_up_resend_done`).
/// Item 2: a re-send is kept only when F1 can use it, so with no sync waiting it is kept for
/// a regular secondary under a non-`Active` commit, and not for an `Active` commit or a
/// shadow. Cut: the re-send is kept after `answer_deferred_syncs`, red (one left).
/// Unit, about 2.5 s per case (the trio's bootstrap window).
#[config_log::retcd_test]
fn m10_s2_59_a_post_commit_sync_before_the_landing_ends_the_re_sends() {
    const METHOD: &str = "m10_s2_59_a_post_commit_sync_before_the_landing_ends_the_re_sends";
    let rt = current_thread();
    let site = Site {
        partition: PartitionId(1),
        correlation: CorrelationId(0),
    };
    // (sync waiting, mode, node 2's role in the pin) -> re-sends kept after the landing.
    let cases = [
        (
            true,
            PartitionMode::DegradedRf2,
            ReplicaRole::RegularSecondary,
            0,
        ),
        (
            false,
            PartitionMode::DegradedRf2,
            ReplicaRole::RegularSecondary,
            1,
        ),
        (
            false,
            PartitionMode::Active,
            ReplicaRole::RegularSecondary,
            0,
        ),
        (false, PartitionMode::DegradedRf2, ReplicaRole::Shadow, 0),
    ];
    for (waiting, mode, role, kept) in cases {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        let digests = write_chain(
            &mut RocksEngine::open(&path).expect("open engine"),
            Generation(1),
            2,
        );
        let mut result = landing_at(2, digests[1]);
        result.barrier = rdb_core::contracts::recovery::RecoveryBarrier::try_new(
            &[],
            &BTreeSet::new(),
            Seq(2),
            digests[1],
        )
        .expect("an empty barrier");
        result.mode = mode.clone();
        let member = result
            .committed
            .pinned_config
            .members
            .iter_mut()
            .find(|member| member.node == NodeId(2))
            .expect("node 2 is pinned");
        member.role = role;
        let copy = member.copy;
        let mut host = host_over(&path, &rt);
        if waiting {
            host.deferred_syncs.insert(
                (PartitionId(1), Generation(2)),
                (
                    NodeId(1),
                    CorrelationId(0),
                    PeerAsk::Sync {
                        copy,
                        cutoff: Seq(2),
                        generation: Generation(2),
                        post_commit: true,
                    },
                ),
            );
            crate::host::drop_first_caught_up(&path);
        }
        host.land(Box::new(result), site, NodeId(1)).expect("land");
        assert_eq!(
            host.caught_up_resends.len(),
            kept,
            "sync waiting {waiting}, {mode:?}, {role:?}"
        );
    }
    let done = logged!(METHOD, "caught_up_resend_done");
    assert_eq!(
        done.len(),
        1,
        "the waiting sync ended the re-sends: {done:?}"
    );
    assert_eq!(logged!(METHOD, "test_caught_up_dropped").len(), 1);
}

/// A-R177 rows 3-5 (critic-f5 C2): the guards refuse before anything is written, each by
/// name: the digest at the base differs, the copy holds past the base (uncertain data an
/// ask must not truncate), or below it. Issue #105 (design section 3.3): the copy holding past
/// the base no longer answers a bare `InventoryFailed`. With L unknown its tail on the newest
/// lineage below the anchor is treated as current, so it answers `TailHeld` naming the head
/// and its stamp; that is the refusal F1 caps on. The other two hold no tail and still fail.
#[test]
fn a_missed_generation_is_not_linked_unless_held_exactly_through_the_base() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        3,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    for (anchor, reason) in [
        (
            anchor_at(2, 3, digests[1]),
            "missed_generation_base_digest_differs",
        ),
        (
            anchor_at(2, 2, digests[1]),
            "missed_generation_held_past_base",
        ),
        (
            anchor_at(2, 4, digests[2]),
            "missed_generation_held_below_base",
        ),
    ] {
        assert_eq!(
            host.survivor(PartitionId(1), CopyId(1), anchor, None)
                .map(drop),
            Err(reason)
        );
        let answer = if reason == "missed_generation_held_past_base" {
            RecoveryEvent::TailHeld {
                copy: CopyId(1),
                head: Seq(3),
                stamp: Some(Generation(1)),
                reason: reason.to_owned(),
            }
        } else {
            RecoveryEvent::InventoryFailed { copy: CopyId(1) }
        };
        assert_eq!(
            host.inventory(PartitionId(1), CopyId(1), anchor, None, None),
            answer,
            "{reason}"
        );
        assert_eq!(host.engine.lineages(), held, "{reason}: nothing written");
    }
}

/// A chain of `n` records in generation 1 whose head does not chain from the record below it, so
/// adoption refuses that tail, `tail_not_contiguous` (P2, design section 5.3). Seqs 1 to `n - 1`
/// are [`write_chain`]'s. Answers every digest, `[0]` at seq 1.
fn write_unchained_head(path: &std::path::Path, n: u64) -> Vec<Digest> {
    let mut engine = RocksEngine::open(path).expect("open engine");
    let mut below = write_chain(&mut engine, Generation(1), n - 1);
    let mut wrong = below.clone();
    *wrong.last_mut().expect("a record below the head") = Digest([9; 32]);
    let head = *super::trio::write_chain_after(&mut engine, Generation(1), wrong, 1)
        .last()
        .expect("the head");
    below.push(head);
    below
}

/// Issue #105 (design section 7, "add one with L set"): with the last writable generation L
/// known, the same tail is judged by its head's stamp. Stamped L (here generation 1) it is
/// current and answers `TailHeld`. With L at generation 2 it is superseded: a writable commit
/// went on past it, so it is today's refusal, a bare `InventoryFailed`. Nothing is written
/// either way. Unit 2 (design section 5.1) adopts a current tail whose proofs hold, so this
/// tail's head is planted not chaining from the record below it: adoption refuses it, and the
/// copy's status says `tail_not_contiguous`. The adopted outcome on a sound tail is
/// `u2_a_current_tail_whose_proofs_hold_is_linked_at_its_head_and_reported`.
/// Cut: `tail_scan` keeps a head stamped below L, red (the second answer is `TailHeld`).
#[test]
fn a_tail_past_the_base_is_held_only_when_stamped_by_the_last_writable_generation() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_unchained_head(&path, 3);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    let anchor = anchor_at(2, 2, digests[1]);
    assert_eq!(
        host.inventory(PartitionId(1), CopyId(1), anchor, Some(Generation(1)), None),
        RecoveryEvent::TailHeld {
            copy: CopyId(1),
            head: Seq(3),
            stamp: Some(Generation(1)),
            reason: "missed_generation_held_past_base".to_owned(),
        },
        "stamped L: current"
    );
    assert_eq!(
        host.tail_refused
            .get(&PartitionId(1))
            .map(|refused| refused.adoption),
        Some(TailAdoption::Refused("tail_not_contiguous")),
        "the status names why it is not adopted"
    );
    assert_eq!(
        host.inventory(PartitionId(1), CopyId(1), anchor, Some(Generation(2)), None),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) },
        "stamped below L: superseded"
    );
    assert_eq!(host.engine.lineages(), held, "nothing written");
}

/// Issue #105 unit 2 (design section 5.1, the Link branch): the shape of
/// `a_tail_past_the_base_is_held_only_when_stamped_by_the_last_writable_generation` with a sound
/// tail. Stamped below L it is superseded, `InventoryFailed`, nothing written. Stamped L its
/// proofs hold, so the copy, which never held the anchor's generation, links it at the tail's
/// head and reports that head, holding L.
/// Cut: `Host::adoption` answers its refusal for every tail, red (the second answer is
/// `TailHeld`).
#[test]
fn u2_a_current_tail_whose_proofs_hold_is_linked_at_its_head_and_reported() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        3,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    let anchor = anchor_at(2, 2, digests[1]);
    assert_eq!(
        host.inventory(PartitionId(1), CopyId(1), anchor, Some(Generation(2)), None),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) },
        "stamped below L: superseded"
    );
    assert_eq!(host.engine.lineages(), held, "nothing written");
    match host.inventory(PartitionId(1), CopyId(1), anchor, Some(Generation(1)), None) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(3), digests[2]), "stamped L: adopted");
            assert!(inventory.holds_last_writable, "{inventory:?}");
        }
        other => panic!("stamped L: adopted and reported, not {other:?}"),
    }
    assert_eq!(
        host.engine
            .link(PartitionId(1), Generation(2))
            .map(|link| (link.parent, link.base)),
        Some((Generation(1), Seq(3))),
        "linked at the tail's head"
    );
}

/// Issue #105 (design section 3.3): a copy that holds the anchor's generation at the base, and
/// past it on an older lineage, holds a tail there: `TailHeld` with `tail_on_older_lineage`,
/// stamped generation 1, whether L is that generation or unknown. Stamped below L it is
/// superseded and the copy reports. The control: the anchor's lineage holds the same record at
/// that head (linked there), so there is no tail and the answer is not `TailHeld`. That is the
/// shape an adopted tail leaves (unit 2, design section 5.1 M1): the chain from the anchor's base
/// through the link verifies, so the copy reports its head. Before unit 2 it answered its own
/// refusal, `InventoryFailed`. And N3(a): a normal recovery's copy holds
/// no tail and reports, holding L intact. Cuts: `tail_scan` skips a lineage while the anchor is
/// held, red (the first answer reports); the same-digest check removed, red (the control is
/// `TailHeld`).
#[config_log::retcd_test]
fn issue105_a_tail_on_an_older_lineage_beside_the_anchor_is_held_and_a_shared_head_is_not() {
    const METHOD: &str =
        "issue105_a_tail_on_an_older_lineage_beside_the_anchor_is_held_and_a_shared_head_is_not";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 5);
        engine
            .inherit(PartitionId(1), Generation(1), Generation(2), Seq(3))
            .expect("inherit");
        digests
    };
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let anchor = anchor_at(2, 3, digests[2]);
    let held = RecoveryEvent::TailHeld {
        copy: CopyId(1),
        head: Seq(5),
        stamp: Some(Generation(1)),
        reason: "tail_on_older_lineage".to_owned(),
    };
    assert_eq!(
        host.inventory(PartitionId(1), CopyId(1), anchor, Some(Generation(1)), None),
        held,
        "stamped L"
    );
    assert_eq!(
        host.inventory(PartitionId(1), CopyId(1), anchor, None, None),
        held,
        "L unknown: the newest lineage below the anchor"
    );
    assert!(
        matches!(
            host.inventory(PartitionId(1), CopyId(1), anchor, Some(Generation(2)), None),
            RecoveryEvent::InventoryReported(_)
        ),
        "stamped below L: superseded, the copy reports"
    );
    drop(host);

    // The anchor's lineage holds the same record at the old lineage's head (linked there, past
    // the base): no tail. That is an adopted tail's shape, so the copy reports its head.
    let shared = dir.path().join("shared");
    let digests = {
        let mut engine = RocksEngine::open(&shared).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 5);
        engine
            .inherit(PartitionId(1), Generation(1), Generation(2), Seq(5))
            .expect("inherit");
        digests
    };
    let mut host = host_over(&shared, &rt);
    match host.inventory(
        PartitionId(1),
        CopyId(1),
        anchor_at(2, 3, digests[2]),
        Some(Generation(1)),
        None,
    ) {
        RecoveryEvent::InventoryReported(inventory) => assert_eq!(
            inventory.head,
            (Seq(5), digests[4]),
            "the same record at the head is no tail: the copy reports it"
        ),
        other => panic!("the same record at the head is no tail: {other:?}"),
    }
    drop(host);

    // N3(a): a normal recovery's copy, linked at the base, holds no tail and reports, holding
    // L (generation 2) intact through the link.
    let normal = dir.path().join("normal");
    let digests = {
        let mut engine = RocksEngine::open(&normal).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 3);
        engine
            .inherit(PartitionId(1), Generation(1), Generation(2), Seq(3))
            .expect("inherit");
        digests
    };
    let mut host = host_over(&normal, &rt);
    match host.inventory(
        PartitionId(1),
        CopyId(1),
        anchor_at(2, 3, digests[2]),
        Some(Generation(2)),
        None,
    ) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head.0, Seq(3));
            assert!(inventory.holds_last_writable, "{inventory:?}");
        }
        other => panic!("a normal recovery reports: {other:?}"),
    }
    let hits: Vec<_> = logged!(METHOD, "tail_scan")
        .into_iter()
        .map(|line| line["hit"].clone())
        .collect();
    assert_eq!(
        hits,
        [true, true, false, false, false].map(serde_json::Value::Bool),
        "one scan per ask"
    );
}

/// Issue #105 (design section 3.7): a copy holds L intact when it holds L's lineage with a
/// record at seq 1, read through any link. Not when L is unknown, not when the copy lacks L
/// (wiped, or never held it), and not when L's lineage is holed below its head. A lineage
/// linked onto one that holds seq 1 holds it. Cut: `holds_last_writable` skips the seq-1
/// read, red (the holed lineage counts).
#[test]
fn issue105_a_copy_holds_l_intact_only_with_l_from_seq_1() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain(&mut engine, Generation(1), 3);
        engine
            .inherit(PartitionId(1), Generation(1), Generation(3), Seq(3))
            .expect("inherit");
        // Generation 2 holds seqs 4 and 5 only: holed below its head.
        write_chain_after(&mut engine, Generation(2), vec![Digest::ROOT; 3], 2);
    }
    let rt = current_thread();
    let host = host_over(&path, &rt);
    let p = PartitionId(1);
    assert!(!host.holds_last_writable(p, None), "L unknown");
    assert!(
        host.holds_last_writable(p, Some(Generation(1))),
        "written from seq 1"
    );
    assert!(
        host.holds_last_writable(p, Some(Generation(3))),
        "linked onto seq 1"
    );
    assert!(!host.holds_last_writable(p, Some(Generation(2))), "holed");
    assert!(
        !host.holds_last_writable(p, Some(Generation(9))),
        "L not held"
    );
}

/// Issue #105 (design section 3.4, layer 2): after a commit, a copy whose own status holds a
/// tail above the cutoff withholds its durability proof, so a rebuild that needs it stalls and
/// never activates past the tail. Before the commit, or with the tail at or below the cutoff,
/// the tail does not withhold it. Cut: the `tail_held` check removed, red (the first reason is
/// `short`).
#[config_log::retcd_test]
fn issue105_a_copy_holding_a_tail_withholds_its_post_commit_proof() {
    const METHOD: &str = "issue105_a_copy_holding_a_tail_withholds_its_post_commit_proof";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        3,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let capture = CapturedPrefix {
        partition: PartitionId(1),
        generation: Generation(1),
        through: AppliedSeq(3),
    };
    let tail = |head: u64| TailRefused {
        generation: Generation(2),
        head: Seq(head),
        base: Seq(3),
        reason: "tail_on_older_lineage",
        adoption: TailAdoption::Unchecked,
    };
    // `write_chain` publishes no durability, so every answer here is withheld; the reason
    // says which check withheld it.
    host.tail_refused.insert(PartitionId(1), tail(5));
    assert_eq!(host.sync_answer(capture, CopyId(1), true, true), None);
    assert_eq!(host.sync_answer(capture, CopyId(1), false, true), None);
    host.tail_refused.insert(PartitionId(1), tail(3));
    assert_eq!(host.sync_answer(capture, CopyId(1), true, true), None);
    let reasons: Vec<_> = logged!(METHOD, "sync_withheld")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        reasons,
        ["tail_held", "short", "short"].map(|reason| serde_json::json!(reason)),
        "after the commit the tail withholds; before it, or at the cutoff, it does not"
    );
}

/// S1-8 D2: each `step_view` line names the depth of the view it built, the levels a read
/// through it walks (O(1), never a walk of the lineage). Generation 1 holds its own
/// writes, one level; generation 2 inherited onto it reads through two.
#[config_log::retcd_test]
fn the_step_view_line_names_the_view_depth() {
    const METHOD: &str = "the_step_view_line_names_the_view_depth";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let mut engine = RocksEngine::open(&path).expect("open engine");
    write_chain(&mut engine, Generation(1), 2);
    engine
        .inherit(PartitionId(1), Generation(1), Generation(2), Seq(2))
        .expect("inherit");
    drop(engine);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    for generation in [Generation(1), Generation(2)] {
        host.refresh_view(PartitionId(1), generation)
            .expect("step view");
    }
    let depths: Vec<_> = logged!(METHOD, "step_view")
        .into_iter()
        .map(|line| (line["generation"].clone(), line["depth"].clone()))
        .collect();
    assert_eq!(
        depths,
        [(1, 1), (2, 2)].map(|(g, d)| (serde_json::json!(g), serde_json::json!(d))),
        "the step_view lines"
    );
}

/// A-R177 row 6 (critic-f5 C1): a link onto a parent already at the chain ceiling would be
/// a multi-batch flatten inside the discovery window, so it is refused, nothing staged.
#[test]
fn a_missed_generation_is_not_linked_onto_a_parent_at_the_chain_ceiling() {
    let ceiling = MAX_CHAIN_DEPTH as u64;
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 2);
        for to in 2..=ceiling {
            assert_eq!(
                engine.inherit(PartitionId(1), Generation(to - 1), Generation(to), Seq(2)),
                Ok(Inherited::Linked)
            );
        }
        assert_eq!(
            engine.chain_depth(PartitionId(1), Generation(ceiling)),
            MAX_CHAIN_DEPTH
        );
        digests
    };
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    let anchor = anchor_at(ceiling + 1, 2, digests[1]);
    assert_eq!(
        host.survivor(PartitionId(1), CopyId(1), anchor, None)
            .map(drop),
        Err("missed_generation_link_depth_ceiling")
    );
    assert_eq!(host.engine.lineages(), held, "nothing staged or linked");
}

/// A-R177 row 8 (critic-f5 C3): the node stops after the link and before its answer. The
/// next open's inventory, from the same directory, reports as a copy that landed the
/// generation, and the link is unchanged.
#[test]
fn a_link_made_at_inventory_survives_a_restart_before_the_answer() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        2,
    );
    let anchor = anchor_at(2, 2, digests[1]);
    let rt = current_thread();
    {
        let mut host = host_over(&path, &rt);
        assert!(host
            .survivor(PartitionId(1), CopyId(1), anchor, None)
            .is_ok());
    }
    let mut host = host_over(&path, &rt);
    match host.inventory(PartitionId(1), CopyId(1), anchor, None, None) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(2), digests[1]));
        }
        other => panic!("expected a report after the restart, got {other:?}"),
    }
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(2)),
        Some(rdb_storage::Link {
            parent: Generation(1),
            base: Seq(2),
            copied: false,
        })
    );
}
/// D7 host wiring (B-R59, the sim's `catch_up`): F1's `CatchUp` on node 1 goes to node 2,
/// which holds the source copy; R1 there sends its stored records to node 3 as recovery
/// appends, and its `CopyCaughtUp` comes back to node 1, the node that asked. Live receivers
/// stand in for the sides a reopen does not have yet (A-R173 rules on those).
/// Integration (~3 s): bootstrap's discovery window, then one catch-up.
#[config_log::retcd_test]
fn d7_a_catch_up_goes_to_its_source_and_reports_to_the_node_that_asked() {
    use rdb_core::contracts::ids::Revision;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let receivers = |trio: &Trio| {
        [1usize, 2].map(|i| {
            let host = &trio.nodes[i].0;
            host.replication
                .receiver(host.node, PartitionId(1))
                .map(|receiver| (receiver.lineage(), receiver.applied_head().seq))
        })
    };
    let (lineage, through) = trio.until("nodes 2 and 3 hold node 1's head", |trio| {
        let head = trio.head()?;
        match receivers(trio) {
            [Some((lineage, a)), Some((_, b))] if a.0 == head && b.0 == head && head > 0 => {
                Some((lineage, a))
            }
            _ => None,
        }
    });
    let credential = FenceCredential {
        partition: PartitionId(1),
        prior_generation: lineage.generation,
        prior_owner_epoch: lineage.owner_epoch,
        control_revision: Revision(u64::MAX),
        sender: CopyId(1),
    };
    let site = Site {
        partition: PartitionId(1),
        correlation: CorrelationId(u64::MAX),
    };
    let through_digest = trio.nodes[1]
        .0
        .replication
        .receiver(NodeId(2), PartitionId(1))
        .expect("node 2's receiver")
        .applied_head()
        .digest;
    let effect = RecoveryEffect::CatchUp {
        from: CopyId(1),
        to: CopyId(2),
        through,
        through_digest,
        credential,
    };
    trio.nodes[0]
        .0
        .recovery_effect(&effect, site)
        .expect("routed");

    // Node 1's mail is read here before it is handled, to see the report arrive.
    let patience = test_patience(Trio::PATIENCE);
    let deadline = Instant::now() + patience;
    let reported = loop {
        let mut reported = None;
        for (host, _, rx) in &mut trio.nodes {
            host.run_due();
            host.drain();
            while let Ok(msg) = rx.try_recv() {
                if let Msg::Routed {
                    kind: KernelEvent::Recovery(RecoveryEvent::CopyCaughtUp { copy, head, .. }),
                    correlation,
                    ..
                } = &msg
                {
                    reported = Some((host.node, *copy, *head, *correlation));
                }
                host.handle(msg);
            }
            assert_eq!(host.fault, None, "node {} faulted", host.node.0);
        }
        if let Some(reported) = reported {
            break reported;
        }
        assert!(
            Instant::now() < deadline,
            "no CopyCaughtUp reached node 1 within {patience:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(
        reported,
        (NodeId(1), CopyId(2), through, CorrelationId(u64::MAX)),
        "node 2's report reaches node 1 under the asking correlation"
    );
    assert!(
        trio.nodes[1].0.catch_ups.is_empty(),
        "the source forgets the asker once it has reported"
    );
    let sent: Vec<_> = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "d7_a_catch_up_goes_to_its_source_and_reports_to_the_node_that_asked",
    )
    .into_iter()
    .filter(|line| line["@m"] == "recovery_envelopes_sent")
    .map(|line| (line["node"].clone(), line["to"].clone()))
    .collect();
    assert!(
        sent.contains(&(serde_json::json!(2), serde_json::json!(3))),
        "node 2 sent recovery appends to node 3: {sent:?}"
    );
}

/// H2 (A-R223, A-R254; issue #61 lead ruling 1): R1's `SnapshotCatchupRequired` on the primary
/// is served, never by faulting the node, by one of two branches. When the primary's ladder is
/// thinner than its engine (its first rung above the engine's floor), the ladder is refilled
/// from the engine and logged `snapshot_catchup_served{how = "ladder"}` with every rung, root
/// through the head. Once it is dense, a new ask opens a snapshot transfer at the head
/// (`snapshot_transfer_opened`): copy 2's second ask, at the same barrier, since a refill is
/// never deduped. Its third reaches the open transfer and is dropped
/// (`snapshot_catchup_in_flight{at = S}`, lead ruling 2), and a node that hosts no primary logs
/// `snapshot_catchup_unserved{reason = no_primary}`.
/// Cuts: the arm removed, red (a fault); the in-flight check removed, red (the third ask is
/// deduped silently, no `in_flight` line); a refill deduped, red (the second ask is dropped, no transfer); the transfer
/// branch always taken, red (the first ask opens a transfer).
/// Integration (~2 s): bootstrap's discovery window, then direct calls.
#[config_log::retcd_test]
fn h2_a_snapshot_catch_up_refills_a_thin_ladder_then_opens_a_transfer() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let head = trio.head().expect("node 1 holds a head");
    let p = PartitionId(1);
    // Thin the primary's ladder only: its engine still holds every record from the root.
    let primary = &mut trio.nodes[0].0;
    let rung = primary
        .replication
        .primary(NodeId(1), p)
        .and_then(|primary| primary.tracker().history().digest_at(Seq(head)))
        .expect("node 1's ladder holds its head");
    primary
        .replication
        .trim_history(NodeId(1), p, Seq(head), rung)
        .expect("thin the primary's ladder");
    assert_eq!(super::trio::ladder_floor(primary), Some(Seq(head)));
    let site = Site {
        partition: p,
        correlation: CorrelationId(u64::MAX),
    };
    let ask = |copy: u8, barrier: u64| KernelEffect::SnapshotCatchupRequired {
        copy: CopyId(copy),
        barrier: Seq(barrier),
    };
    for (node, copy, barrier) in [(0, 2, head), (0, 2, head), (0, 2, head), (1, 3, head)] {
        let host = &mut trio.nodes[node].0;
        assert_eq!(
            host.kernel(
                ModuleName::Replication,
                &ask(copy, barrier),
                site,
                &[],
                &StepFences::default(),
            ),
            Ok(()),
            "node {} asked for copy {copy} at {barrier}",
            host.node.0
        );
        assert_eq!(host.fault, None, "node {} faulted", host.node.0);
    }
    assert_eq!(
        super::trio::ladder_floor(&trio.nodes[0].0),
        Some(Seq::ZERO),
        "the refill made node 1's ladder dense from the root"
    );
    let lines: Vec<_> = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "h2_a_snapshot_catch_up_refills_a_thin_ladder_then_opens_a_transfer",
    )
    .into_iter()
    .filter(|line| {
        line["@m"]
            .as_str()
            .is_some_and(|m| m.starts_with("snapshot_catchup_") || m == "snapshot_transfer_opened")
    })
    .map(|line| {
        (
            line["@m"].as_str().unwrap_or_default().to_owned(),
            line["node"].as_u64(),
            line["copy"].as_u64(),
            line["rungs"].as_u64().or(line["at"].as_u64()),
            line["how"]
                .as_str()
                .or(line["reason"].as_str())
                .map(str::to_owned),
        )
    })
    .collect();
    assert_eq!(
        lines,
        [
            (
                "snapshot_catchup_served".to_owned(),
                Some(1),
                Some(2),
                Some(head + 1),
                Some("ladder".to_owned())
            ),
            (
                "snapshot_transfer_opened".to_owned(),
                Some(1),
                Some(2),
                Some(head),
                None
            ),
            (
                "snapshot_catchup_in_flight".to_owned(),
                Some(1),
                Some(2),
                Some(head),
                None
            ),
            (
                "snapshot_catchup_unserved".to_owned(),
                Some(2),
                Some(3),
                None,
                Some("no_primary".to_owned())
            ),
        ],
        "refilled, then a transfer at the head, the third ask in flight, no primary unserved"
    );
    assert_eq!(
        trio.nodes[0].0.transfers.keys().collect::<Vec<_>>(),
        [&(p, CopyId(2))],
        "node 1 holds copy 2's transfer only"
    );
}

/// H3 (I1, A-R254): R1's `CopyAheadOnControl` on the primary re-posts its committed root to
/// the copy's node through [`Links::send`], so a held link holds it: nothing reaches node 3
/// while 1-3 is held, and the heal delivers one `Landed` from node 1 at the committed
/// generation. A partition with no committed root logs `copy_ahead_unrouted`. Neither faults
/// the node; before H3 both faulted it `host_unsupported: kernel_effect CopyAheadOnControl`.
/// Cuts: the arm removed, red (a fault); `Links::post` for `send`, red (node 3 hears it
/// through the hold).
/// Integration (~2 s): bootstrap's discovery window, then direct calls.
#[config_log::retcd_test]
fn h3_a_copy_ahead_on_control_is_sent_the_root_again_through_the_hold() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let committed = trio.nodes[0].0.committed[&PartitionId(1)].new_generation;
    let copy = trio.nodes[0].0.committed[&PartitionId(1)]
        .committed
        .pinned_config
        .members
        .iter()
        .find(|member| member.node == NodeId(3))
        .expect("node 3 is a member")
        .copy;
    // Node 3's mailbox is drained now, so anything in it later is the re-post.
    while let Ok(msg) = trio.nodes[2].2.try_recv() {
        trio.nodes[2].0.handle(msg);
    }
    trio.links.hold(NodeId(1), NodeId(3));
    let ahead = KernelEffect::CopyAheadOnControl { copy };
    for partition in [PartitionId(1), PartitionId(9)] {
        let site = Site {
            partition,
            correlation: CorrelationId(u64::MAX),
        };
        let host = &mut trio.nodes[0].0;
        assert_eq!(
            host.kernel(
                ModuleName::Replication,
                &ahead,
                site,
                &[],
                &StepFences::default(),
            ),
            Ok(()),
            "partition {}",
            partition.0
        );
        assert_eq!(host.fault, None, "node 1 faulted");
    }
    let landed = |rx: &Receiver<Msg>| {
        let mut landed = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let Msg::Landed {
                result, emitter, ..
            } = msg
            {
                landed.push((emitter, result.new_generation));
            }
        }
        landed
    };
    assert_eq!(landed(&trio.nodes[2].2), [], "nothing passes the hold");
    assert_eq!(trio.links.heal(NodeId(1), NodeId(3)), 1, "one message held");
    assert_eq!(
        landed(&trio.nodes[2].2),
        [(NodeId(1), committed)],
        "the heal delivers node 1's root"
    );
    let lines: Vec<_> = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "h3_a_copy_ahead_on_control_is_sent_the_root_again_through_the_hold",
    )
    .into_iter()
    .filter(|line| {
        line["@m"]
            .as_str()
            .is_some_and(|m| m.starts_with("copy_ahead_"))
    })
    .map(|line| {
        (
            line["@m"].as_str().unwrap_or_default().to_owned(),
            line["partition"].as_u64(),
            line["member"].as_u64(),
        )
    })
    .collect();
    assert_eq!(
        lines,
        [
            ("copy_ahead_reposted".to_owned(), Some(1), Some(3)),
            ("copy_ahead_unrouted".to_owned(), Some(9), None),
        ]
    );
}

/// Shadow probe (A-R228), host side: a `Recovered` whose pin names a shadow arms R1's
/// `shadow_timer` on the primary, the host keeps it in its timer table, and fires it to R1,
/// which sends the shadow the head and arms the next probe under a newer version. The host
/// has no shadow-specific code: it arms and fires every timer by id. This row shows the
/// generic path carries this one. Here node 3's copy is made the shadow by restating node 1's
/// committed root with that role.
/// Cut: the host drops a `TimerEffect::Arm` for `shadow_timer`, red (nothing is armed).
/// Integration (~1 s): bootstrap's discovery window, then one probe period.
#[test]
fn a_shadow_named_by_the_pin_gets_its_probe_timer_armed_and_fired() {
    use rdb_core::replication::primary::{shadow_timer, SHADOW_PROBE_MS};
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let mut result = (*trio.nodes[0].0.committed[&PartitionId(1)]).clone();
    let mut shadowed = false;
    for member in &mut result.committed.pinned_config.members {
        if member.node == NodeId(3) {
            member.role = ReplicaRole::Shadow;
            shadowed = true;
        }
    }
    assert!(shadowed, "node 3 is a member of the pin");
    let id = shadow_timer(PartitionId(1));
    assert!(
        !trio.nodes[0].0.timers.contains_key(&id),
        "no shadow, no probe"
    );
    let site = Site {
        partition: PartitionId(1),
        correlation: CorrelationId::default(),
    };
    let host = &mut trio.nodes[0].0;
    host.push(
        site,
        EventKind::Kernel(KernelEvent::Recovered(Box::new(result))),
        true,
        None,
    );
    host.drain();
    assert_eq!(host.fault, None, "node 1 faulted");
    let (armed, at, _) = *host.timers.get(&id).expect("the shadow probe is armed");
    assert!(
        at.0 <= trio.clock.now().0 + SHADOW_PROBE_MS,
        "armed one probe period out"
    );
    let fired = trio.until("the probe fired and armed the next", |trio| {
        trio.nodes[0]
            .0
            .timers
            .get(&id)
            .map(|(version, _, _)| *version)
            .filter(|version| *version > armed)
    });
    assert_eq!(fired.0, armed.0 + 1, "R1 took the fire and armed the next");
}

/// D8 fix 1 (A-R337, critic-d8 finding 5): a copy that linked generation 2 below the anchor's
/// base, as `land_link_behind` does, and stored the base's record since, reports for that
/// anchor: the digest it stores at the base is the anchor's. The same copy asked with another
/// digest at the base answers nothing, and so does a copy that never stored the base's record.
/// Cut: `link.base != anchor.base_seq` in `survivor`, red (`base_differs` for the first).
#[test]
fn a_copy_linked_below_the_anchor_base_reports_only_when_it_holds_the_base() {
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let linked_at_two = |name: &str| {
        let path = dir.path().join(name);
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 2);
        engine
            .inherit(PartitionId(1), Generation(1), Generation(2), Seq(2))
            .expect("inherit");
        (path, engine, digests)
    };

    let (path, mut engine, digests) = linked_at_two("holding");
    let digests = write_chain_after(&mut engine, Generation(2), digests, 1);
    drop(engine);
    let mut holding = host_over(&path, &rt);
    match holding.survivor(PartitionId(1), CopyId(1), anchor_at(2, 3, digests[2]), None) {
        Ok(inventory) => {
            assert_eq!(inventory.head, (Seq(3), digests[2]));
            assert_eq!(inventory.ladder, vec![(Seq(3), digests[2])]);
        }
        Err(reason) => panic!("linked at 2 and holding 3: {reason}"),
    }
    assert_eq!(
        holding
            .survivor(PartitionId(1), CopyId(1), anchor_at(2, 3, digests[1]), None)
            .map(drop),
        Err("base_digest_differs"),
        "another record at the base"
    );

    let (path, engine, digests) = linked_at_two("behind");
    drop(engine);
    let mut behind = host_over(&path, &rt);
    assert_eq!(
        behind
            .survivor(PartitionId(1), CopyId(1), anchor_at(2, 3, digests[1]), None)
            .map(drop),
        Err("no_record"),
        "linked at 2 and holding only 2"
    );
}

/// A put of key `r<request>`, as the rejoin rows below make them.
fn rejoin_put(request: u64) -> ClientCall {
    use rdb_core::contracts::ids::{ClientId, TenantId};
    use rdb_value::delta::Op;
    ClientCall::Put {
        identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(request),
        },
        object: Bytes::from(format!("r{request}")),
        change: Change::Doc(Delta(vec![Op::Replace(Value::Bytes(b"v".to_vec()))])),
        expect: None,
        remaining_millis: 5_000,
    }
}

/// Node 3's process dies: nothing crosses its links until [`restart_three`]. Then puts from
/// `request` on land until L1 pauses on node 3, so node 3 misses them. Returns the next
/// request id and how many landed.
fn three_dies_and_misses_puts(trio: &mut Trio, mut request: u64) -> (u64, u64) {
    for other in [1, 2] {
        trio.links.drop_direction(NodeId(3), NodeId(other));
        trio.links.drop_direction(NodeId(other), NodeId(3));
    }
    let mut landed = 0;
    loop {
        assert!(landed < 2_000, "{landed} puts landed and L1 never paused");
        let answer = trio.ask(rejoin_put(request));
        request += 1;
        match answer {
            Answer::Txn { .. } => landed += 1,
            Answer::Error { error, .. } if error.name() == "PROTECTION_PAUSED" => {
                return (request, landed)
            }
            other => panic!("put {}: {other:?}", request - 1),
        }
    }
}

/// Node 3's process starts again, as `Db::restart_node` restarts one: a new host on its own
/// engine at the next boot, and nodes 1 and 2 told the new boot (`PeerBooted`). Its links heal.
fn restart_three(trio: &mut Trio, dir: &std::path::Path, tune: impl Fn(&mut Host)) -> BootId {
    let (old, _, _) = trio.nodes.remove(2);
    let boot = BootId(old.boot.0 + 1);
    let control = Arc::clone(&old.control);
    drop(old);
    control.forget_node(NodeId(3), BootId(boot.0 - 1));
    let engine = RocksEngine::open(dir.join("3")).expect("reopen node 3's engine");
    let (tx, rx) = mpsc::channel();
    trio.links.heal_all();
    trio.links.register(NodeId(3), tx.clone(), boot);
    let mut host = Host::new(
        NodeId(3),
        engine,
        Arc::clone(&trio.links),
        control,
        trio.clock,
    );
    host.boot = boot;
    tune(&mut host);
    trio.nodes.push((host, tx, rx));
    for node in 0..2 {
        trio.nodes[node]
            .1
            .send(Msg::PeerBooted {
                node: NodeId(3),
                boot,
            })
            .expect("node up");
    }
    boot
}

/// [`Trio::fast_pause`] on the regular rejoin path: D9 (A-R398) pins a copy restarted from an
/// `Active` commit as a shadow, and the rows below are for F1's routed catch-up, which every
/// other restart still takes.
fn regular_rejoin(host: &mut Host) {
    Trio::fast_pause(host);
    host.restart_shadow = false;
}

/// A-R278 (W6b): a secondary restarted behind a live owner catches up from the owner's own
/// copy and rejoins, twice. Node 3 dies, misses puts until L1 pauses on it, and restarts. The
/// owner re-plans (#42) and F1 routes node 3's catch-up to the owner's copy, which the owner
/// holds as a live primary. The owner restores a dormant receiver beside its primary to source
/// it (`dormant_restored` on node 1), node 3 catches up, the re-plan commits with all three
/// (`rejoin_committed`), writes land in the new generation, and node 3 holds it durably through
/// the owner's head. The dormant receiver is gone at the commit, so the second restart of the
/// same copy restores a fresh one and rejoins the same way. Nothing is refused (`NotASource`)
/// and no rejoin is abandoned.
/// Cut: `restore_dormant` returns early on a node holding a primary, red (F1 blocked:
/// `BarrierIncomplete`, after `catch_up_refused` `NotASource`).
/// Integration (~6 s): bootstrap's discovery window, then two rounds of puts until L1 pauses,
/// a restart and the re-plan's window.
#[config_log::retcd_test]
fn a_secondary_restarted_behind_a_live_owner_catches_up_from_the_owner_and_rejoins_twice() {
    const METHOD: &str =
        "a_secondary_restarted_behind_a_live_owner_catches_up_from_the_owner_and_rejoins_twice";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), regular_rejoin);
    trio.bootstrap();
    trio.admits_for(4 * Trio::PAUSE_AGE_MILLIS);
    let mut generation = trio.status(0).recovered.expect("bootstrap committed");
    let mut request = 1;
    for round in 1..=2u64 {
        let (next, landed) = three_dies_and_misses_puts(&mut trio, request);
        assert!(landed >= 1, "round {round}: node 3 missed no put");
        let boot = restart_three(&mut trio, dir.path(), regular_rejoin);
        let (rejoined, role) = trio.until("node 1 commits the rejoin with all three", |trio| {
            let committed = trio.nodes[0].0.committed.get(&PartitionId(1))?;
            let three = committed
                .committed
                .pinned_config
                .members
                .iter()
                .find(|member| member.node == NodeId(3) && member.boot == boot)?;
            (committed.new_generation > generation)
                .then_some((committed.new_generation, three.role))
        });
        // ADV-4 (A-R406): name the path when the restart was shadowed after all.
        assert_eq!(
            role,
            ReplicaRole::RegularSecondary,
            "round {round}: node 3 rejoined as {role:?}; this row is F1's regular rejoin, which \
             `regular_rejoin` keeps by turning D9's shadow off"
        );
        assert_eq!(rejoined, generation.next(), "round {round}: one re-plan");
        assert!(
            trio.nodes[0]
                .0
                .replication
                .receiver(NodeId(1), PartitionId(1))
                .is_none(),
            "round {round}: node 1's dormant receiver outlived the commit"
        );
        trio.admits_for(4 * Trio::PAUSE_AGE_MILLIS);
        match trio.ask(rejoin_put(next)) {
            Answer::Txn { result, .. } => assert_eq!(result.generation, rejoined),
            other => panic!("round {round}: a put after the rejoin: {other:?}"),
        }
        request = next + 1;
        let head = trio.head().expect("node 1 holds the partition");
        trio.until(
            "node 3 holds the new generation through node 1's head",
            |trio| {
                trio.status(2)
                    .holds
                    .filter(|held| held.generation == rejoined && held.durable >= head)
                    .map(|_| ())
            },
        );
        generation = rejoined;
    }

    let refused = logged!(METHOD, "catch_up_refused");
    assert!(refused.is_empty(), "a catch-up was refused: {refused:?}");
    assert_eq!(
        logged!(METHOD, "rejoin_abandoned").len(),
        0,
        "a rejoin was abandoned"
    );
    assert_eq!(logged!(METHOD, "rejoin_committed").len(), 2);
    let sourced: Vec<_> = logged!(METHOD, "dormant_restored")
        .into_iter()
        .filter(|line| line["node"] == 1)
        .collect();
    assert_eq!(
        sourced.len(),
        2,
        "node 1 restores one per rejoin: {sourced:?}"
    );
}

/// A-R323 A-6 (W6b): beside a serving primary the dormant receiver only sources. Node 1 owns
/// the partition after bootstrap. A target restore there, `with_history` false as
/// `route_catch_up` makes when F1 names node 1 the target, installs nothing and is logged
/// `dormant_restore_refused{serving_primary}`, so no recovery append can reach node 1's store
/// beside its primary. A source restore with the same seed, `with_history` true, still installs
/// the dormant receiver with its ladder, so the refusal is the target's. Cut: the target skip
/// removed; node 1 holds a receiver after the target restore, red.
/// Integration (~6 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn a_target_restore_beside_a_serving_primary_installs_nothing() {
    const METHOD: &str = "a_target_restore_beside_a_serving_primary_installs_nothing";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    let committed = trio.until("node 1 committed", |trio| {
        trio.nodes[0].0.committed.get(&PartitionId(1)).cloned()
    });
    let seed = DormantSeed {
        config: committed.committed.pinned_config.clone(),
        lineage: Lineage {
            partition: PartitionId(1),
            generation: committed.new_generation,
            owner_epoch: OwnerEpoch(1),
        },
        revision: Revision(1),
        cutoff: (Seq::ZERO, Digest::ROOT),
    };
    let one = &mut trio.nodes[0].0;
    let serving = one
        .replication
        .primary(NodeId(1), PartitionId(1))
        .is_some_and(|primary| !primary.tracker().retired());
    assert!(serving, "node 1 serves the partition");

    one.restore_dormant(PartitionId(1), &seed, false);
    assert!(
        one.replication
            .receiver(NodeId(1), PartitionId(1))
            .is_none(),
        "a target restore installed a receiver beside the primary"
    );
    let refused = logged!(METHOD, "dormant_restore_refused");
    assert!(
        refused
            .iter()
            .any(|line| line["node"] == 1 && line["reason"] == "serving_primary"),
        "{refused:?}"
    );

    one.restore_dormant(PartitionId(1), &seed, true);
    assert!(
        one.replication
            .receiver(NodeId(1), PartitionId(1))
            .is_some_and(AppendReceiver::is_dormant),
        "a source restore installs the dormant receiver"
    );
    assert_eq!(logged!(METHOD, "dormant_ladder_built").len(), 1);
}

/// MAT-2 (A-R406, D10's host half): an owner promoted from a secondary still holds that side's
/// receiver, which the promote's `Recovered` retired (B-R58a). A source restore beside the
/// serving primary replaces it with a fresh dormant receiver, so R1 sources the catch-up F1
/// routes there instead of refusing it `NotASource`. Node 1 serves the partition after
/// bootstrap; a receiver built for its copy and retired by the committed pin, which names that
/// copy the primary, stands in for the promoted side's.
/// Cut: `restore_dormant`'s `stale` forced false, red (the retired receiver is kept and the
/// catch-up is refused `NotASource`).
/// Integration (~3 s): bootstrap's discovery window, then direct calls.
#[config_log::retcd_test]
fn m10_s2_mat2_a_retired_receiver_beside_a_serving_primary_is_replaced_for_a_source() {
    use rdb_core::contracts::ids::Revision;
    const METHOD: &str =
        "m10_s2_mat2_a_retired_receiver_beside_a_serving_primary_is_replaced_for_a_source";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let through = Seq(trio.until("node 1 wrote past the root", |trio| {
        trio.head().filter(|head| *head > 0)
    }));
    let committed = trio.until("node 1 committed", |trio| {
        trio.nodes[0].0.committed.get(&PartitionId(1)).cloned()
    });
    let config = committed.committed.pinned_config.clone();
    let copy_of = |n: u32| {
        config
            .members
            .iter()
            .find(|member| member.node == NodeId(n))
            .map(|member| member.copy)
            .expect("a member")
    };
    let (from, to) = (copy_of(1), copy_of(3));
    let seed = DormantSeed {
        config: config.clone(),
        lineage: Lineage {
            partition: PartitionId(1),
            generation: committed.new_generation,
            owner_epoch: OwnerEpoch(1),
        },
        revision: Revision(1),
        cutoff: (Seq::ZERO, Digest::ROOT),
    };
    let one = &mut trio.nodes[0].0;
    assert!(
        one.replication
            .primary(NodeId(1), PartitionId(1))
            .is_some_and(|primary| !primary.tracker().retired()),
        "node 1 serves the partition"
    );
    let mut stale = one
        .dormant_receiver(PartitionId(1), &seed)
        .expect("a receiver for node 1's copy");
    let _ = stale.on_recovered(&committed);
    assert!(stale.retired(), "the pin names node 1's copy the primary");
    one.replication.install_receiver(stale);

    let credential = FenceCredential {
        partition: PartitionId(1),
        prior_generation: committed.new_generation,
        prior_owner_epoch: OwnerEpoch(1),
        control_revision: Revision(u64::MAX),
        sender: from,
    };
    let site = Site {
        partition: PartitionId(1),
        correlation: CorrelationId(u64::MAX),
    };
    one.source_catch_up(NodeId(2), site, (from, to, through), credential, &seed);
    one.drain();

    // Refusal first, so the cut fails on R1's answer itself.
    let refused = logged!(METHOD, "catch_up_refused");
    assert!(
        refused.is_empty(),
        "R1 refused node 1's catch-up: {refused:?}"
    );
    assert!(
        one.replication
            .source(NodeId(1), PartitionId(1), to)
            .is_some(),
        "R1 sources no catch-up from node 1"
    );
    let held = one.replication.receiver(NodeId(1), PartitionId(1));
    assert!(
        held.is_some_and(|receiver| receiver.is_dormant() && !receiver.retired()),
        "the retired receiver was kept"
    );
    assert!(
        logged!(METHOD, "dormant_restored")
            .iter()
            .any(|line| line["node"] == 1),
        "node 1 restored no dormant receiver"
    );
}

/// Critic-s1b A1: a node with no kept inventory ladder (every node after a reopen) read its
/// first one from seq 0, not from its generation's base, so a reopen's first inventory read
/// the whole history before the base and kept it in memory for good. Here node 2 holds 40
/// records of generation 1 and an inventory for generation 2 links it at base 40: the ladder
/// starts at the base and reads nothing below it.
/// Cut: a cold ladder starts at `(0, ROOT)`, red (first rung seq 0, 40 records read).
#[config_log::retcd_test]
fn m11_a1_a_a_reopened_node_reads_its_first_ladder_from_the_base() {
    const METHOD: &str = "m11_a1_a_a_reopened_node_reads_its_first_ladder_from_the_base";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        40,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    match host.inventory(
        PartitionId(1),
        CopyId(1),
        anchor_at(2, 40, digests[39]),
        None,
        None,
    ) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(40), digests[39]));
            assert_eq!(inventory.ladder, vec![(Seq(40), digests[39])]);
        }
        other => panic!("expected a report, got {other:?}"),
    }
    let rungs = &host.inventory_ladders[&PartitionId(1)].rungs;
    assert_eq!(rungs.first(), Some(&(Seq(40), digests[39])), "{rungs:?}");
    assert_eq!(rungs.len(), 1, "{rungs:?}");
    let read = logged!(METHOD, "inventory_ladder");
    assert_eq!(read.len(), 1, "{read:?}");
    assert_eq!(read[0]["read"], 0, "{read:?}");
    assert_eq!(
        host.inventory_ladder_matches_rebuild(PartitionId(1)),
        Some((true, Seq(40)))
    );
}

/// Issue #61 R26: node 2 holds generation 2 as a snapshot install leaves it: 6 records with no
/// link (no parent) and its own trim mark at 4, rooted at `(4, digest 4)` (B1). Answers the
/// host over it and the digests.
fn installed_at_four(path: &std::path::Path, rt: &tokio::runtime::Runtime) -> (Host, Vec<Digest>) {
    let digests = {
        let mut engine = RocksEngine::open(path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(2), 6);
        engine
            .stage_trim_history(
                PartitionId(1),
                Generation(2),
                TrimFloor {
                    seq: Seq(4),
                    root_base: Seq(4),
                    root_digest: digests[3],
                },
            )
            .expect("the mark is staged");
        digests
    };
    let host = host_over(path, rt);
    assert_eq!(
        host.engine.link(PartitionId(1), Generation(2)),
        None,
        "unlinked"
    );
    (host, digests)
}

/// Issue #61 R26 (i): a lineage a snapshot installed has no link, only its own trim mark at the
/// barrier. A re-plan anchored at that barrier asks it for an inventory, and it answers, where
/// it was refused `anchor_generation_unlinked`: the first re-plan after every snapshot rebuild
/// then dropped the rebuilt copy as `Stalled` and committed `ReadOnly` again (R26, manual D1).
/// Cut: the old `ok_or("anchor_generation_unlinked")` on the link, red.
/// Unit.
#[test]
fn t61_r26_an_installed_lineage_answers_an_inventory_at_its_barrier() {
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let (mut host, digests) = installed_at_four(&dir.path().join("node"), &rt);
    let answer = host.survivor(PartitionId(1), CopyId(1), anchor_at(2, 4, digests[3]), None);
    assert!(answer.is_ok(), "{:?}", answer.err());
}

/// Issue #61 R26 (ii): an unlinked lineage answers only from its own mark's root up. Anchored
/// at 2, below the mark's root at 4, it is still refused `anchor_generation_unlinked`.
/// Cut: the root-base condition skipped, red (refused `base_below_floor` instead).
/// Unit.
#[test]
fn t61_r26_an_unlinked_lineage_rooted_above_the_base_is_refused_unlinked() {
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let (mut host, digests) = installed_at_four(&dir.path().join("node"), &rt);
    let answer = host.survivor(PartitionId(1), CopyId(1), anchor_at(2, 2, digests[1]), None);
    assert_eq!(answer.err(), Some("anchor_generation_unlinked"));
}

/// Issue #61 R26 (iii): an unlinked lineage rooted at the base still proves the base by its
/// digest there. Anchored at 4 with record 3's digest, it is refused `base_digest_differs`.
/// Unit.
#[test]
fn t61_r26_an_unlinked_lineage_with_another_digest_at_the_base_is_refused() {
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let (mut host, digests) = installed_at_four(&dir.path().join("node"), &rt);
    let answer = host.survivor(PartitionId(1), CopyId(1), anchor_at(2, 4, digests[2]), None);
    assert_eq!(answer.err(), Some("base_digest_differs"));
}

/// Critic-s1b A2: `ladder_duty` warmed partitions in a fixed order, [`WARM_RUNGS`] records a
/// pass across all of them, so while partition 1 had more to read than that, partition 2 was
/// never reached. Here partition 1 holds two passes' worth and partition 2 holds 10 records:
/// the second pass starts at partition 2, which is then warmed through its head.
/// Cut: no rotation, red (partition 2 has no ladder after two passes).
#[test]
fn m11_a2_a_the_ladder_duty_starves_no_partition() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain(&mut engine, Generation(1), 2 * WARM_RUNGS + 1);
        write_chain_in(&mut engine, PartitionId(2), Generation(1), Vec::new(), 10);
    }
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    host.ladder_duty();
    assert!(
        !host.inventory_ladders.contains_key(&PartitionId(2)),
        "control: the first pass spends its budget on partition 1"
    );
    host.ladder_duty();
    assert_eq!(
        host.inventory_ladder_matches_rebuild(PartitionId(2)),
        Some((true, Seq(10))),
        "the second pass warms partition 2 through its head"
    );
}

/// #79 S3b, row 9's host half: a target restore hands the seed to a held receiver, live or
/// dormant, and the later seed wins. A source restore (`with_history`) never does: its seed is
/// not this copy's plan. Cut: `restore_dormant` keeps the plan only when `dormant` is false,
/// the dormant stanza is red; the keep skipped, every stanza is red.
/// Integration (~3 s): bootstrap's discovery window, then direct calls.
#[config_log::retcd_test]
fn s3b_9_a_target_restore_keeps_the_latest_plan_on_a_live_and_a_dormant_receiver() {
    use rdb_core::contracts::ids::Revision;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    let committed = trio.until("node 1 committed", |trio| {
        trio.nodes[0].0.committed.get(&PartitionId(1)).cloned()
    });
    let seed = |boot: u64| {
        let mut config = committed.committed.pinned_config.clone();
        config.members[0].boot = BootId(boot);
        DormantSeed {
            config,
            lineage: Lineage {
                partition: PartitionId(1),
                generation: committed.new_generation,
                owner_epoch: OwnerEpoch(1),
            },
            revision: Revision(1),
            cutoff: (Seq::ZERO, Digest::ROOT),
        }
    };
    let held_boot = |two: &Host| {
        two.replication
            .receiver(NodeId(2), PartitionId(1))
            .expect("a receiver")
            .recovery_plan()
            .map(|plan| plan.config.members[0].boot)
    };
    trio.until("node 2 holds a live receiver", |trio| {
        trio.nodes[1]
            .0
            .replication
            .receiver(NodeId(2), PartitionId(1))
            .filter(|held| !held.is_dormant())
            .map(|_| ())
    });
    let two = &mut trio.nodes[1].0;

    two.restore_dormant(PartitionId(1), &seed(30), false);
    assert_eq!(held_boot(two), Some(BootId(30)), "live, the first plan");
    two.restore_dormant(PartitionId(1), &seed(31), false);
    assert_eq!(held_boot(two), Some(BootId(31)), "live, the later plan");
    two.restore_dormant(PartitionId(1), &seed(32), true);
    assert_eq!(
        held_boot(two),
        Some(BootId(31)),
        "a source restore is no plan"
    );
    assert!(
        two.replication
            .receiver(NodeId(2), PartitionId(1))
            .is_some_and(|held| !held.is_dormant()),
        "the live receiver is kept"
    );

    let dormant = two
        .dormant_receiver(PartitionId(1), &seed(40))
        .expect("a dormant receiver");
    two.replication.install_receiver(dormant);
    two.restore_dormant(PartitionId(1), &seed(41), false);
    assert_eq!(held_boot(two), Some(BootId(41)), "dormant, the later plan");

    // S3b-9: beside a serving primary the receiver only sources, so it is given no plan.
    let one = &mut trio.nodes[0].0;
    assert!(
        one.replication
            .primary(NodeId(1), PartitionId(1))
            .is_some_and(|primary| !primary.tracker().retired()),
        "node 1 serves the partition"
    );
    one.restore_dormant(PartitionId(1), &seed(50), true);
    one.restore_dormant(PartitionId(1), &seed(51), false);
    let source = one
        .replication
        .receiver(NodeId(1), PartitionId(1))
        .expect("the source restore installed a dormant receiver");
    assert!(source.is_dormant(), "the source receiver stays dormant");
    assert!(
        source.recovery_plan().is_none(),
        "a target restore gave a plan to the receiver beside a serving primary"
    );
}

/// Issue #105 unit 2, critic-s15 B1: the receiver a target restore installs holds the plan. On
/// a node holding no receiver, `restore_dormant` installs a dormant one, and that receiver's
/// plan names the credential the seed came from (prior generation, prior epoch, control
/// revision) and carries the seed's cutoff, so section 15.3's plan rule applies to the
/// catch-up that follows. Before B1 the plan was kept before the install, on no receiver, and
/// the receiver the catch-up reached held none.
/// Cut: the plan kept before `restore_receiver`, red (no plan).
/// Integration (~3 s): bootstrap's discovery window, then direct calls.
#[config_log::retcd_test]
fn u2_b1_a_target_restore_installs_a_receiver_holding_the_plan_and_its_cutoff() {
    use rdb_core::contracts::ids::Revision;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    let committed = trio.until("node 1 committed", |trio| {
        trio.nodes[0].0.committed.get(&PartitionId(1)).cloned()
    });
    let credential = FenceCredential {
        partition: PartitionId(1),
        prior_generation: committed.new_generation,
        prior_owner_epoch: OwnerEpoch(1),
        control_revision: Revision(1),
        sender: CopyId(0),
    };
    let cutoff = (Seq(7), Digest([7; 32]));
    let seed = DormantSeed::new(
        committed.committed.pinned_config.clone(),
        &credential,
        cutoff,
    );
    // Node 2's store holds the seed's generation once its receiver is live.
    trio.until("node 2 holds a live receiver", |trio| {
        trio.nodes[1]
            .0
            .replication
            .receiver(NodeId(2), PartitionId(1))
            .filter(|held| !held.is_dormant())
            .map(|_| ())
    });
    let two = &mut trio.nodes[1].0;
    two.replication.forget_node(NodeId(2));
    assert!(
        two.replication
            .receiver(NodeId(2), PartitionId(1))
            .is_none(),
        "control: node 2 holds no receiver"
    );

    two.restore_dormant(PartitionId(1), &seed, false);
    let receiver = two
        .replication
        .receiver(NodeId(2), PartitionId(1))
        .expect("the target restore installs a dormant receiver");
    assert!(receiver.is_dormant());
    let plan = receiver
        .recovery_plan()
        .expect("the installed receiver holds the plan");
    assert_eq!(
        (
            plan.lineage.generation,
            plan.lineage.owner_epoch,
            plan.revision
        ),
        (
            credential.prior_generation,
            credential.prior_owner_epoch,
            credential.control_revision
        ),
        "the plan names the credential"
    );
    assert_eq!(plan.cutoff, cutoff, "the plan carries the cutoff");
}

/// Issue #61, PR #204 review: a held copy's offer hold (rev 5b) survives a refused combined
/// forget-tail. `--through H --no-intact-copy --give-up-live-copy`, while the intact rule is
/// not what holds the partition, is refused, and the owner's `needs_operator` still names the
/// held copy: no hold was released, since no re-plan follows a refusal.
/// Cut: the intact-rule check run after `offer_holds_released` (as at 67d028c), red
/// (`needs_operator` is `None`).
#[config_log::retcd_test]
fn t61_pr204_a_refused_combined_forget_tail_keeps_the_held_copy_warning() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let p = PartitionId(1);
    let cutoff = trio.nodes[0].0.committed[&p].selected.cutoff_seq;
    let head = Seq(cutoff.0 + 3);
    let owner = &mut trio.nodes[0].0;
    owner.offer_holds.entry(p).or_default().insert(
        CopyId(2),
        crate::host::snapshot::OfferHold {
            node: NodeId(3),
            head,
        },
    );
    let warning = owner.needs_operator(p).expect("the held copy is named");
    let error = owner
        .forget_tail_asked(
            p,
            TailRelease {
                through: Some(head),
                no_intact_copy: true,
            },
            true,
        )
        .expect_err("the intact rule does not hold the partition");
    assert!(error.no_mutation, "{error:?}");
    assert!(
        error.detail.contains("the intact rule is not what holds"),
        "{error:?}"
    );
    assert_eq!(owner.needs_operator(p), Some(warning), "the warning stands");
    assert!(!owner.rejoins.contains_key(&p), "no re-plan");
}

/// Issue #105 (design section 8.1): `admin forget-tail` on the owner. Each refusal is
/// `no_mutation`, names why, and ends "nothing was given up": no flag; a node that is not the
/// owner; `--through` at or below the cutoff; `--through` with no tail anywhere to give up; a
/// live copy's tail without `--give-up-live-copy`; `--no-intact-copy` while the intact rule is
/// not what holds the partition. With the live copy named it is accepted: the answer names what
/// is given up and the owner re-plans, committing a later generation that carries the release.
/// Cut: `forget_tail_asked` skips the live-copy check, red (the bare `--through` is accepted).
#[config_log::retcd_test]
fn issue105_forget_tail_refuses_by_name_and_re_plans_once_it_names_what_it_gives_up() {
    const METHOD: &str =
        "issue105_forget_tail_refuses_by_name_and_re_plans_once_it_names_what_it_gives_up";
    use rdb_core::contracts::errors::ErrorKind;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let p = PartitionId(1);
    let (before, cutoff) = {
        let committed = &trio.nodes[0].0.committed[&p];
        (committed.new_generation, committed.selected.cutoff_seq)
    };
    let through = |head: u64| TailRelease {
        through: Some(Seq(head)),
        no_intact_copy: false,
    };
    let refused = |answer: Result<String, ApiError>, kind: ErrorKind, says: &str| {
        let error = answer.expect_err(says);
        assert_eq!(error.kind, kind, "{error:?}");
        assert!(error.no_mutation, "{error:?}");
        assert!(error.detail.contains(says), "{says}: {error:?}");
    };
    let owner = &mut trio.nodes[0].0;
    refused(
        owner.forget_tail_asked(p, TailRelease::default(), false),
        ErrorKind::InvalidArgument,
        "--through H or --no-intact-copy",
    );
    refused(
        owner.forget_tail_asked(p, through(cutoff.0), false),
        ErrorKind::InvalidArgument,
        "at or below",
    );
    refused(
        owner.forget_tail_asked(p, through(cutoff.0 + 5), false),
        ErrorKind::InvalidArgument,
        "nothing for --through to give up",
    );
    refused(
        owner.forget_tail_asked(
            p,
            TailRelease {
                through: None,
                no_intact_copy: true,
            },
            false,
        ),
        ErrorKind::InvalidArgument,
        "the intact rule is not what holds",
    );
    owner.tail_refused.insert(
        p,
        TailRefused {
            generation: before,
            head: Seq(cutoff.0 + 3),
            base: cutoff,
            reason: "tail_on_older_lineage",
            adoption: TailAdoption::Unchecked,
        },
    );
    refused(
        owner.forget_tail_asked(p, through(cutoff.0 + 5), false),
        ErrorKind::InvalidArgument,
        "add --give-up-live-copy",
    );
    assert!(
        trio.nodes[1]
            .0
            .forget_tail_asked(p, through(cutoff.0 + 5), true)
            .is_err(),
        "node 2 is not the owner"
    );
    let owner = &mut trio.nodes[0].0;
    let line = owner
        .forget_tail_asked(p, through(cutoff.0 + 5), true)
        .expect("accepted");
    assert!(
        line.starts_with(&format!(
            "forget-tail: partition 1 re-plans from generation {}: seqs up to {} ",
            before.0,
            cutoff.0 + 5
        )),
        "{line}"
    );
    // The tail stood in for a probe's report; the re-plan's own inventory decides.
    owner.tail_refused.remove(&p);
    let after = trio.until("node 1 committed a later generation", |trio| {
        trio.nodes[0]
            .0
            .committed
            .get(&p)
            .map(|committed| committed.new_generation)
            .filter(|generation| *generation > before)
    });
    assert!(after > before);
    let plans: Vec<_> = logged!(METHOD, "rejoin_plan")
        .into_iter()
        .map(|line| line["release_through"].clone())
        .collect();
    assert!(
        plans.contains(&serde_json::json!(format!("Some({})", cutoff.0 + 5))),
        "the re-plan carried the release: {plans:?}"
    );
    assert_eq!(logged!(METHOD, "forget_tail_planned").len(), 1);
    let refusals = logged!(METHOD, "forget_tail_refused")
        .into_iter()
        .filter(|line| line["node"] == 1)
        .count();
    assert_eq!(refusals, 5, "node 1 refused five times");
}

/// Issue #61 (lead rulings R10 and R16): a tail whose adoption could not run because its history
/// is trimmed is `Trimmed`. A bare `--through` over it is refused, and the refusal says the
/// seqs cannot come back and names the whole command, `admin forget-tail --partition 1
/// --through H --give-up-live-copy`; never "not checked". With the flag it is accepted. Both
/// trimmed reasons map to `Trimmed` and need the flag. Planted on the owner's status.
/// Cut: `TailAdoption::of` maps the trimmed reasons to `Refused`, red (the bare ask is accepted).
#[config_log::retcd_test]
fn t61_forget_tail_over_a_trimmed_tail_needs_the_flag_and_says_it_cannot_come_back() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let p = PartitionId(1);
    let (before, cutoff) = {
        let committed = &trio.nodes[0].0.committed[&p];
        (committed.new_generation, committed.selected.cutoff_seq)
    };
    let head = Seq(cutoff.0 + 3);
    let through = TailRelease {
        through: Some(head),
        no_intact_copy: false,
    };
    for reason in crate::host::ADOPTION_TRIMMED {
        let adoption = TailAdoption::of::<()>(&Err(reason));
        assert_eq!(adoption, TailAdoption::Trimmed(reason), "{reason}");
        assert!(adoption.needs_give_up_flag(), "{reason}");
    }
    let owner = &mut trio.nodes[0].0;
    owner.tail_refused.insert(
        p,
        TailRefused {
            generation: before,
            head,
            base: cutoff,
            reason: "tail_on_older_lineage",
            adoption: TailAdoption::of::<()>(&Err("tail_below_floor")),
        },
    );
    let error = owner
        .forget_tail_asked(p, through, false)
        .expect_err("a bare --through over a trimmed tail");
    assert!(error.no_mutation, "{error:?}");
    assert!(
        error.detail.starts_with(&format!(
            "node 1 holds seqs {} to {} of partition 1; they cannot come back: the history \
             needed to adopt them is trimmed. To give them up anyway, for good, run admin \
             forget-tail --partition 1 --through {} --give-up-live-copy",
            cutoff.0 + 1,
            head.0,
            head.0
        )),
        "{error:?}"
    );
    assert!(!error.detail.contains("not been checked"), "{error:?}");
    let line = owner
        .forget_tail_asked(p, through, true)
        .expect("accepted with the flag");
    assert!(line.contains("given up, for good"), "{line}");
}

/// Issue #105 unit 2 (lead ruling on the forget-tail narrowing, architect section 14 item 2):
/// `--give-up-live-copy` is needed only for a live tail that could come back. Planted on the
/// owner's status, one state at a time: `Adoptable`, `NotReady` and `Unchecked` each refuse a
/// bare `--through` by name. A `Refused` own tail beside a member's `NotReady` hold is refused
/// too, naming that member. A `Refused` tail alone (a failed proof, here `base_digest_differs`)
/// is accepted with no flag, and the owner re-plans with the release.
/// Cuts: every live tail needs the flag (unit 1's rule), red (the `Refused` ask is refused);
/// `TailAdoption::needs_give_up_flag` true only for `Adoptable`, red (the `NotReady` ask is accepted).
#[config_log::retcd_test]
fn u2_forget_tail_needs_the_live_copy_flag_only_for_a_tail_that_could_return() {
    const METHOD: &str =
        "u2_forget_tail_needs_the_live_copy_flag_only_for_a_tail_that_could_return";
    use rdb_core::contracts::errors::ErrorKind;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let p = PartitionId(1);
    let (before, cutoff) = {
        let committed = &trio.nodes[0].0.committed[&p];
        (committed.new_generation, committed.selected.cutoff_seq)
    };
    let through = || TailRelease {
        through: Some(Seq(cutoff.0 + 5)),
        no_intact_copy: false,
    };
    let tail = |adoption| TailRefused {
        generation: before,
        head: Seq(cutoff.0 + 3),
        base: cutoff,
        reason: "tail_on_older_lineage",
        adoption,
    };
    // Lead ruling R8: the refusal says what is true of the tail's state. One that can come
    // back by itself says so; an unchecked one says it was not checked. Neither says it cannot.
    let needs_flag = |answer: Result<String, ApiError>, node: u64, state: &str| {
        let error = answer.expect_err(state);
        assert_eq!(error.kind, ErrorKind::InvalidArgument, "{state}: {error:?}");
        assert!(error.no_mutation, "{state}: {error:?}");
        assert!(
            error.detail.contains("add --give-up-live-copy")
                && error.detail.contains(&format!("node {node} holds seqs")),
            "{state}: {error:?}"
        );
        let says = if state.starts_with("Unchecked") {
            "not been checked"
        } else {
            "can bring them back; no operator action is needed"
        };
        assert!(error.detail.contains(says), "{state}: {error:?}");
        assert!(
            !error.detail.contains("cannot bring them back"),
            "{state}: {error:?}"
        );
    };
    let owner = &mut trio.nodes[0].0;
    for adoption in [
        TailAdoption::Adoptable,
        TailAdoption::NotReady("replay_not_ready"),
        TailAdoption::Unchecked,
    ] {
        owner.tail_refused.insert(p, tail(adoption));
        needs_flag(
            owner.forget_tail_asked(p, through(), false),
            1,
            &format!("{adoption:?}"),
        );
    }
    owner
        .tail_refused
        .insert(p, tail(TailAdoption::Refused("base_digest_differs")));
    owner.tail_holds.insert(
        p,
        vec![TailHold {
            node: NodeId(3),
            copy: CopyId(2),
            refused: tail(TailAdoption::NotReady("replay_not_ready")),
        }],
    );
    needs_flag(
        owner.forget_tail_asked(p, through(), false),
        3,
        "a member's NotReady hold",
    );
    owner.tail_holds.remove(&p);
    let line = owner
        .forget_tail_asked(p, through(), false)
        .expect("a tail whose proof failed needs no flag");
    assert!(
        line.starts_with(&format!(
            "forget-tail: partition 1 re-plans from generation {}: seqs up to {} ",
            before.0,
            cutoff.0 + 5
        )),
        "{line}"
    );
    // The tail stood in for a probe's report; the re-plan's own inventory decides.
    owner.tail_refused.remove(&p);
    trio.until("node 1 committed a later generation", |trio| {
        trio.nodes[0]
            .0
            .committed
            .get(&p)
            .map(|committed| committed.new_generation)
            .filter(|generation| *generation > before)
    });
    assert_eq!(logged!(METHOD, "forget_tail_planned").len(), 1);
}

/// PR #110 review blocker 1: a tail the committed ledger marks on a member, before any repair
/// probe has reported on it, is `Unchecked`, so a bare `--through` over it is refused naming
/// that member. A partial Replay that failed leaves exactly this: the ledger names the holder,
/// and the status cache is empty until repair's first probe. Once a probe reports the tail
/// `Refused` (a failed proof), the probe's word stands and the same bare ask is accepted.
/// Cut: `forget_tail_asked` reads only the status cache, red (the bare ask is accepted).
#[config_log::retcd_test]
fn pr110_forget_tail_counts_a_ledger_held_tail_no_probe_has_checked_as_unchecked() {
    use rdb_core::contracts::errors::ErrorKind;
    use rdb_core::contracts::recovery::HeldTail;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let p = PartitionId(1);
    let (before, cutoff) = {
        let committed = &trio.nodes[0].0.committed[&p];
        (committed.new_generation, committed.selected.cutoff_seq)
    };
    let through = || TailRelease {
        through: Some(Seq(cutoff.0 + 5)),
        no_intact_copy: false,
    };
    let owner = &mut trio.nodes[0].0;
    assert!(
        owner.tail_holds.get(&p).is_none_or(Vec::is_empty) && !owner.tail_refused.contains_key(&p),
        "control: no probe has reported a tail"
    );
    owner
        .committed
        .get_mut(&p)
        .expect("committed")
        .ledger
        .held
        .push(HeldTail {
            node: NodeId(3),
            head: Seq(cutoff.0 + 3),
        });
    let error = owner
        .forget_tail_asked(p, through(), false)
        .expect_err("a ledger-held tail no probe has checked needs the flag");
    assert_eq!(error.kind, ErrorKind::InvalidArgument, "{error:?}");
    assert!(error.no_mutation, "{error:?}");
    assert!(
        error.detail.contains(&format!(
            "node 3 holds seqs {} to {}",
            cutoff.0 + 1,
            cutoff.0 + 3
        )) && error.detail.contains("not been checked")
            && error.detail.contains("add --give-up-live-copy"),
        "{error:?}"
    );
    // A probe that found the proof failed has the last word: that tail can restore nothing.
    owner.tail_holds.insert(
        p,
        vec![TailHold {
            node: NodeId(3),
            copy: CopyId(2),
            refused: TailRefused {
                generation: before,
                head: Seq(cutoff.0 + 3),
                base: cutoff,
                reason: "tail_on_older_lineage",
                adoption: TailAdoption::Refused("base_digest_differs"),
            },
        }],
    );
    owner
        .forget_tail_asked(p, through(), false)
        .expect("a tail a probe found Refused needs no flag");
    owner.tail_holds.remove(&p);
    trio.until("node 1 committed a later generation", |trio| {
        trio.nodes[0]
            .0
            .committed
            .get(&p)
            .map(|committed| committed.new_generation)
            .filter(|generation| *generation > before)
    });
}

/// Issue #105 (design section 3.3, assumption A7): the tail scan over 512 partitions, as a
/// whole-node recovery runs it. 256 healthy copies (generation 2 linked at generation 1's
/// head) read no record; 256 with a two-record tail on generation 1 read one each and are hits.
/// Every scan logs its `read` count and micros; the totals are printed for the report, and no
/// time bound is asserted, since this host's load is not this row's subject.
#[config_log::retcd_test]
fn issue105_a_tail_scan_over_512_partitions_reads_one_record_per_tail_and_none_when_healthy() {
    const METHOD: &str =
        "issue105_a_tail_scan_over_512_partitions_reads_one_record_per_tail_and_none_when_healthy";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let mut bases = Vec::new();
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        for p in 1..=512u32 {
            let partition = PartitionId(p);
            let n = if p <= 256 { 3 } else { 5 };
            let digests = write_chain_in(&mut engine, partition, Generation(1), Vec::new(), n);
            engine
                .inherit(partition, Generation(1), Generation(2), Seq(3))
                .expect("inherit");
            bases.push((partition, digests[2]));
        }
    }
    let rt = current_thread();
    let host = host_over(&path, &rt);
    let started = std::time::Instant::now();
    let hits: Vec<bool> = bases
        .iter()
        .map(|(partition, digest)| {
            let anchor = LineageAnchor {
                lineage: Lineage {
                    partition: *partition,
                    generation: Generation(2),
                    owner_epoch: OwnerEpoch(2),
                },
                base_seq: Seq(3),
                base_digest: *digest,
            };
            host.tail_scan(*partition, anchor, Some(Generation(1)))
                .is_some()
        })
        .collect();
    let wall = started.elapsed();
    assert_eq!(hits.iter().filter(|hit| **hit).count(), 256);
    assert!(
        hits[..256].iter().all(|hit| !hit),
        "healthy copies hold no tail"
    );
    let lines = logged!(METHOD, "tail_scan");
    assert_eq!(lines.len(), 512);
    let reads = |healthy: bool| -> u64 {
        lines
            .iter()
            .filter(|line| (line["partition"].as_u64().expect("a partition") <= 256) == healthy)
            .map(|line| line["read"].as_u64().expect("a count"))
            .sum()
    };
    assert_eq!(reads(true), 0, "a healthy copy reads no record");
    assert_eq!(reads(false), 256, "one point read per tail");
    let micros: Vec<u64> = lines
        .iter()
        .map(|line| line["micros"].as_u64().expect("micros"))
        .collect();
    eprintln!(
        "tail_scan x512: wall {} us, sum {} us, max {} us",
        wall.as_micros(),
        micros.iter().sum::<u64>(),
        micros.iter().max().expect("512 scans")
    );
}

/// Issue #105 (design section 3.3, "the same scan runs at landing"): at a first landing a copy
/// that holds a tail above the commit's cutoff on an older lineage stamped L marks its own
/// status `tail_on_older_lineage`, so the owner holds its repair and a client is told which
/// copy holds the seqs, even when the landing itself linked. Stamped below the result's L, the
/// tail is superseded and nothing is marked. A refusal the landing already made at that
/// generation keeps its own reason. Unit 2: the mark also says where the tail stands against
/// adoption. This fixture's `inherit` below generation 1's head is a full copy, the shape a
/// release leaves, so the tail is `Refused("tail_set_aside")`.
/// Cut: `landing_scan` finds nothing, red (no mark).
#[test]
fn issue105_a_landing_over_a_current_tail_marks_the_copy_and_a_superseded_one_does_not() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(&dir.path().join("trio"), Trio::fast);
    trio.bootstrap();
    let template = trio.until("node 1 committed", |trio| {
        trio.nodes[0].0.committed.get(&PartitionId(1)).cloned()
    });
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 5);
        engine
            .inherit(PartitionId(1), Generation(1), Generation(2), Seq(3))
            .expect("inherit");
        digests
    };
    let result = |last_writable: u64| {
        let mut result = (*template).clone();
        result.selected.root = Lineage {
            partition: PartitionId(1),
            generation: Generation(2),
            owner_epoch: OwnerEpoch(2),
        };
        result.selected.cutoff_seq = Seq(3);
        result.selected.cutoff_digest = digests[2];
        result.ledger.last_writable = Some(Generation(last_writable));
        result
    };
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    host.landing_scan(PartitionId(1), &result(2));
    assert_eq!(
        host.tail_refused.get(&PartitionId(1)),
        None,
        "stamped below L"
    );
    host.landing_scan(PartitionId(1), &result(1));
    assert_eq!(
        host.tail_refused.get(&PartitionId(1)),
        Some(&TailRefused {
            generation: Generation(2),
            head: Seq(5),
            base: Seq(3),
            reason: "tail_on_older_lineage",
            adoption: TailAdoption::Refused("tail_set_aside"),
        }),
        "stamped L: marked"
    );
    let refused = TailRefused {
        generation: Generation(2),
        head: Seq(5),
        base: Seq(3),
        reason: "held_past_base",
        adoption: TailAdoption::Unchecked,
    };
    host.tail_refused.insert(PartitionId(1), refused);
    host.landing_scan(PartitionId(1), &result(1));
    assert_eq!(
        host.tail_refused.get(&PartitionId(1)),
        Some(&TailRefused {
            adoption: TailAdoption::Refused("tail_set_aside"),
            ..refused
        }),
        "the landing's own reason stays"
    );
}

/// Issue #105 (design section 13), walk C's shape: a copy that missed the anchor's generation
/// and holds a tail past its base on the lineage below. Without a release it answers `TailHeld`;
/// with `--through` below the tail's head it still does (a tail above `through` caps). Unit 2
/// would adopt a sound tail on those asks, so the tail's head is planted not chaining from the
/// record below it (`tail_not_contiguous`); the release half reads nothing above the base. With
/// `--through` at the head the tail is released: not reported, logged `tail_released`, and the
/// anchor's generation is inherited by copy at the base (`Copied`, the set-aside). The copy then
/// reports at the base, its status clears, and the tail stays on disk on the sealed parent. Its
/// post-commit proof is not withheld for the tail. A later ask with no release, as an
/// automatic run after a `ReadOnly` commit makes it, finds the tail current again: `TailHeld`.
/// Cut: `inventory` reports a released hit as `TailHeld`, red (the third answer).
#[config_log::retcd_test]
fn issue105_a_released_tail_on_a_missed_generation_is_copied_at_the_base_and_reported() {
    const METHOD: &str =
        "issue105_a_released_tail_on_a_missed_generation_is_copied_at_the_base_and_reported";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_unchained_head(&path, 3);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let l = Some(Generation(1));
    let anchor = anchor_at(2, 2, digests[1]);
    let held = host.engine.lineages();
    let tail_held = RecoveryEvent::TailHeld {
        copy: CopyId(1),
        head: Seq(3),
        stamp: Some(Generation(1)),
        reason: "missed_generation_held_past_base".to_owned(),
    };
    assert_eq!(
        host.inventory(p, CopyId(1), anchor, l, None),
        tail_held,
        "automatic"
    );
    assert_eq!(
        host.inventory(p, CopyId(1), anchor, l, Some(Seq(2))),
        tail_held,
        "a tail above --through still holds"
    );
    assert_eq!(host.engine.lineages(), held, "nothing written");

    match host.inventory(p, CopyId(1), anchor, l, Some(Seq(3))) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(2), digests[1]), "reported at the base");
        }
        other => panic!("a released copy reports: {other:?}"),
    }
    assert_eq!(
        host.engine
            .link(p, Generation(2))
            .map(|link| (link.parent, link.base, link.copied)),
        Some((Generation(1), Seq(2), true)),
        "copied at the base"
    );
    assert_eq!(host.tail_refused.get(&p), None, "the status clears");
    assert_eq!(
        host.engine.buffered_applied(p, Generation(1)),
        AppliedSeq(3),
        "the tail stays on its own lineage"
    );
    assert!(
        host.engine
            .history_at(p, Generation(1), Seq(3))
            .expect("read")
            .is_some(),
        "the given-up record stays on disk"
    );
    assert_eq!(host.engine.sealed_by(p, Generation(1)), Some(Generation(2)));
    let released = logged!(METHOD, "tail_released");
    assert_eq!(released.len(), 1, "{released:?}");
    assert_eq!(released[0]["head"], 3);
    assert_eq!(released[0]["through"], 3);
    assert_eq!(logged!(METHOD, "inventory_tail_set_aside").len(), 1);

    // The post-commit proof at the base is not withheld for the released tail. `write_chain`
    // publishes no durability, so the answer is withheld as `short`, never `tail_held`.
    let capture = CapturedPrefix {
        partition: p,
        generation: Generation(2),
        through: AppliedSeq(2),
    };
    assert_eq!(host.sync_answer(capture, CopyId(1), true, true), None);
    let reasons: Vec<_> = logged!(METHOD, "sync_withheld")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        reasons,
        [serde_json::json!("short")],
        "not withheld for the tail"
    );

    // The release run committed `ReadOnly` (L stays generation 1); the next, automatic run.
    assert_eq!(
        host.inventory(p, CopyId(1), anchor_at(3, 2, digests[1]), l, None),
        tail_held,
        "the tail is current again with no release"
    );
}

/// Issue #105 (design section 13, failure behaviour): a released tail this copy cannot report
/// past still holds. Here the digest at the base differs from the anchor's: the answer is
/// `TailHeld` naming that refusal, nothing is written, and the status keeps the tail.
/// Cut: a release failure answers `InventoryFailed`, red.
#[config_log::retcd_test]
fn issue105_a_released_tail_that_cannot_be_copied_still_answers_tail_held_and_writes_nothing() {
    const METHOD: &str =
        "issue105_a_released_tail_that_cannot_be_copied_still_answers_tail_held_and_writes_nothing";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        3,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let held = host.engine.lineages();
    assert_eq!(
        host.inventory(
            p,
            CopyId(1),
            anchor_at(2, 2, digests[0]),
            Some(Generation(1)),
            Some(Seq(3)),
        ),
        RecoveryEvent::TailHeld {
            copy: CopyId(1),
            head: Seq(3),
            stamp: Some(Generation(1)),
            reason: "missed_generation_base_digest_differs".to_owned(),
        }
    );
    assert_eq!(host.engine.lineages(), held, "nothing written");
    assert_eq!(
        host.tail_refused
            .get(&p)
            .map(|refused| (refused.head, refused.reason)),
        Some((Seq(3), "missed_generation_held_past_base")),
        "the status keeps the tail, with a reason the wire carries"
    );
    let refused = logged!(METHOD, "tail_release_refused");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(
        refused[0]["reason"],
        "missed_generation_base_digest_differs"
    );
}

/// Issue #105 (design section 13): a crash part way through the released copy leaves the
/// anchor's generation staged. That ask answers `TailHeld`; the next ask with the release runs
/// the same inherit again, which clears the staged keys and copies again, and the copy
/// reports. Cut: `survivor` does not re-enter a staged anchor, red (the second answer fails).
#[cfg(debug_assertions)]
#[test]
fn issue105_a_crash_during_the_released_copy_is_finished_by_the_next_ask() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        3,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let anchor = anchor_at(2, 2, digests[1]);
    let release = Some(Seq(3));
    host.engine
        .inject_fault(rdb_storage::InjectedFault::CopyBatch);
    assert!(
        matches!(
            host.inventory(p, CopyId(1), anchor, Some(Generation(1)), release),
            RecoveryEvent::TailHeld { ref reason, .. } if reason == "missed_generation_link_refused"
        ),
        "the copy failed part way"
    );
    assert_eq!(
        host.engine.staging(p, Generation(2)),
        Some((Generation(1), Seq(2))),
        "staged, not switched"
    );
    match host.inventory(p, CopyId(1), anchor, Some(Generation(1)), release) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(2), digests[1]));
        }
        other => panic!("the re-run reports: {other:?}"),
    }
    assert_eq!(host.engine.staging(p, Generation(2)), None, "switched");
    assert_eq!(
        host.engine.link(p, Generation(2)).map(|link| link.copied),
        Some(true)
    );
}

/// Issue #105 (design section 13), walk A's shape: a copy holding the anchor's generation at
/// the base and a tail past it on an older lineage. Released, the tail is not reported: the copy
/// reports the anchor's lineage as without a tail, nothing is written, and a refusal its landing
/// recorded for that tail clears. Above `through` it still answers `TailHeld`.
/// Cut: `inventory` reports a released hit as `TailHeld`, red.
#[test]
fn issue105_a_released_tail_beside_the_anchor_is_not_reported_and_nothing_is_written() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 5);
        engine
            .inherit(PartitionId(1), Generation(1), Generation(2), Seq(3))
            .expect("inherit");
        digests
    };
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let anchor = anchor_at(2, 3, digests[2]);
    let held = host.engine.lineages();
    host.tail_refused.insert(
        p,
        TailRefused {
            generation: Generation(2),
            head: Seq(5),
            base: Seq(3),
            reason: "tail_on_older_lineage",
            adoption: TailAdoption::Unchecked,
        },
    );
    assert!(
        matches!(
            host.inventory(p, CopyId(1), anchor, Some(Generation(1)), Some(Seq(4))),
            RecoveryEvent::TailHeld { head: Seq(5), .. }
        ),
        "above --through"
    );
    match host.inventory(p, CopyId(1), anchor, Some(Generation(1)), Some(Seq(5))) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(3), digests[2]));
        }
        other => panic!("a released copy reports: {other:?}"),
    }
    assert_eq!(host.engine.lineages(), held, "nothing written");
    assert_eq!(
        host.tail_refused.get(&p),
        None,
        "the landing's refusal clears"
    );
}

/// Section 15.4 R6-7 (rule 15): adoption ends the copy's tail fact, so the copy proves the
/// adopted head after the commit. A copy whose landing marked its tail `Adoptable` (the fact a
/// later inventory finds) adopts it onto the anchor's lineage; the fact is gone, and the
/// post-commit sync at the adopted head, the cutoff the plan brings, is not withheld for the
/// tail (`tail_held`). Rule 15's "above": a fact whose head equals the cutoff withholds nothing,
/// while one above it still does.
/// Cuts: `Host::adopt` keeps the fact, red (the fact is still there); `sync_answer` withholds at
/// a head equal to the cutoff (`>=`), red (the second sync is withheld `tail_held`).
#[config_log::retcd_test]
fn u2_r6_7_an_adopted_tail_clears_its_fact_and_its_post_commit_proof_is_not_withheld() {
    const METHOD: &str =
        "u2_r6_7_an_adopted_tail_clears_its_fact_and_its_post_commit_proof_is_not_withheld";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        3,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let fact = |head: u64| TailRefused {
        generation: Generation(2),
        head: Seq(head),
        base: Seq(2),
        reason: "tail_on_older_lineage",
        adoption: TailAdoption::Adoptable,
    };
    host.tail_refused.insert(PartitionId(1), fact(3));
    match host.inventory(
        PartitionId(1),
        CopyId(1),
        anchor_at(2, 2, digests[1]),
        Some(Generation(1)),
        None,
    ) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(3), digests[2]), "adopted");
        }
        other => panic!("an adoptable tail is adopted and reported, not {other:?}"),
    }
    assert_eq!(
        host.tail_refused.get(&PartitionId(1)),
        None,
        "adoption ends the fact"
    );
    let first = host.sync(PartitionId(1), CopyId(1), Seq(3), Generation(2), true);
    // A fact at the cutoff, as a landing that recorded the adopted head would leave it: not
    // above the cutoff, so it withholds nothing. One above it does (the control).
    host.tail_refused.insert(PartitionId(1), fact(3));
    let at = host.sync(PartitionId(1), CopyId(1), Seq(3), Generation(2), true);
    host.tail_refused.insert(PartitionId(1), fact(4));
    let above = host.sync(PartitionId(1), CopyId(1), Seq(3), Generation(2), true);
    let reasons: Vec<_> = logged!(METHOD, "sync_withheld")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        reasons,
        [serde_json::json!("tail_held")],
        "only the fact above the cutoff withholds: {first:?} {at:?} {above:?}"
    );
    for proof in [first, at] {
        assert!(
            matches!(proof, Some(RecoveryEvent::DurableAt(ref p)) if p.seq == DurableSeq(3)),
            "the adopted head is proven: {proof:?}"
        );
    }
    assert_eq!(above, None);
}

/// critic-s15 M2 (1), the host half: the owner's anchor lineage holds a staged record below a
/// cutoff that quarantined (rdb-core `u2_m2_1_a_divergent_record_below_the_cutoff_is_caught_at_the_cutoff`):
/// seq 3, stamped L (generation 2) but not L's record, staged onto the anchor's lineage
/// (generation 3, linked at base 2). The next run's inventory reports that head as it is, with
/// its own digest, so F1 sees the fork (rdb-core
/// `before_commit_a_foreign_digest_at_the_cutoff_is_divergence`). The scan never reads the
/// anchor's own lineage, so it is no tail: not `TailHeld`, no fact, nothing written.
/// Two guards hold it. `tail_scan` reads only lineages below the anchor's, and its same-record
/// check skips a lineage whose head the anchor's lineage holds; the anchor's lineage always
/// holds its own. Cut either alone, green (observed for `<=`). Cut both, red (the answer is
/// `TailHeld`).
#[config_log::retcd_test]
fn u2_m2_1_a_divergent_record_staged_below_the_cutoff_is_reported_as_the_head_not_held() {
    use rdb_core::contracts::digest::Domain;
    use rdb_core::contracts::envelope::EnvelopeHeader;
    use rdb_core::contracts::ids::{BatchId, ClientId, LeaseId, TenantId};
    use rdb_core::contracts::storage::{Batch, Write};
    use rdb_core::contracts::txn::Outcome;
    use rdb_core::contracts::version::ENVELOPE_VERSION;
    const METHOD: &str =
        "u2_m2_1_a_divergent_record_staged_below_the_cutoff_is_reported_as_the_head_not_held";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let (digests, forged) = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 2);
        engine
            .inherit(PartitionId(1), Generation(1), Generation(3), Seq(2))
            .expect("inherit");
        // As the receiver stages a catch-up record: L's stamp, onto the anchor's lineage.
        let mut env = ReplicationEnvelope {
            header: EnvelopeHeader {
                protocol_version: ENVELOPE_VERSION,
                partition: PartitionId(1),
                generation: Generation(2),
                config_version: ConfigVersion(1),
                owner_epoch: OwnerEpoch(2),
                seq: Seq(3),
                body_len: 0,
            },
            lease_id: LeaseId(1),
            prev_digest: digests[1],
            request_identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(3),
            },
            request_digest: Digest::of(Domain::Record, &[b"forged"]),
            conditions_result: Vec::new(),
            mutations: vec![Write {
                ns: Namespace::User,
                key: Bytes::from_static(b"k"),
                value: Some(Bytes::from_static(b"forged")),
            }],
            result: Outcome::Published,
            record_digest: Digest::ROOT,
        };
        env.record_digest = env.compute_record_digest().expect("digest");
        let mut progress = 3u64.to_le_bytes().to_vec();
        progress.extend_from_slice(&env.record_digest.0);
        let mut writes = env.mutations.clone();
        writes.push(Write {
            ns: Namespace::History,
            key: Bytes::copy_from_slice(&3u64.to_be_bytes()),
            value: Some(env.encode().expect("encode")),
        });
        writes.push(Write {
            ns: Namespace::Progress,
            key: Bytes::from_static(rdb_core::replication::append::PROGRESS_KEY),
            value: Some(Bytes::from(progress)),
        });
        engine
            .commit(Batch {
                id: BatchId(3),
                partition: PartitionId(1),
                generation: Generation(3),
                seq: Seq(3),
                writes,
            })
            .expect("commit");
        (digests, env.record_digest)
    };
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    match host.inventory(
        PartitionId(1),
        CopyId(1),
        anchor_at(3, 2, digests[1]),
        Some(Generation(2)),
        None,
    ) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(
                inventory.head,
                (Seq(3), forged),
                "the staged head, as it is"
            );
            assert!(!inventory.holds_last_writable, "{inventory:?}");
        }
        other => panic!("the owner reports its staged head, not {other:?}"),
    }
    assert_eq!(host.engine.lineages(), held, "nothing written");
    assert_eq!(host.tail_refused.get(&PartitionId(1)), None, "no tail fact");
    let hits: Vec<_> = logged!(METHOD, "tail_scan")
        .into_iter()
        .map(|line| line["hit"].clone())
        .collect();
    assert_eq!(hits, [serde_json::Value::Bool(false)], "one scan, no hit");
}

// ---------------------------------------------------------------- design section 16 (Rev 3)

/// Section 16 rows: a tail of 3 records in `generation`, written as R1 stages them, so every
/// record is stamped `generation` (the last writable generation L). Answers the digests.
fn tail_in(path: &std::path::Path, generation: u64) -> Vec<Digest> {
    write_chain(
        &mut RocksEngine::open(path).expect("open engine"),
        Generation(generation),
        3,
    )
}

/// Section 16 rows: generation 1 holds 3 records and generations 2 to [`MAX_CHAIN_DEPTH`] are
/// linked onto it at its head, so generation 32 reads through 32 levels and a link onto it would
/// pass the ceiling: `inherit` flattens it instead. Every record is stamped generation 1 (L).
fn tail_at_the_ceiling(path: &std::path::Path) -> Vec<Digest> {
    let mut engine = RocksEngine::open(path).expect("open engine");
    let digests = write_chain(&mut engine, Generation(1), 3);
    let top = u64::try_from(rdb_storage::MAX_CHAIN_DEPTH).expect("a small ceiling");
    for to in 2..=top {
        engine
            .inherit(PartitionId(1), Generation(to - 1), Generation(to), Seq(3))
            .expect("link one level deeper");
    }
    digests
}

/// The `(from, to, how)` of every `tail_adopted` line this row logged.
fn adoptions(method: &str) -> Vec<(u64, u64, String)> {
    logged!(method, "tail_adopted")
        .into_iter()
        .map(|line| {
            (
                line["from"].as_u64().expect("from"),
                line["to"].as_u64().expect("to"),
                line["how"].as_str().expect("how").to_owned(),
            )
        })
        .collect()
}

/// R7-1 (design section 16, G1): a tail of 3 on generation 2 (L). An ask at anchor 3 adopts it by
/// a link, which seals generation 2 for generation 3. An ask at anchor 4 with the same L, as a
/// backlog after a restart delivers it, finds generations 2 and 3 holding the same record at
/// head 3: the newer wins, generation 4 links onto 3, and the copy reports head 3. No hold, and
/// the status names no tail.
/// Cut c1 (the tie back to older-wins): red, the second ask is `TailHeld` with
/// `NotReady("tail_sealed_for_other")`.
#[config_log::retcd_test]
fn u2_r7_1_a_backlogged_ask_with_the_same_l_adopts_from_the_newer_lineage() {
    const METHOD: &str = "u2_r7_1_a_backlogged_ask_with_the_same_l_adopts_from_the_newer_lineage";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = tail_in(&path, 2);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let l = Some(Generation(2));
    for generation in [3, 4] {
        match host.inventory(p, CopyId(1), anchor_at(generation, 2, digests[1]), l, None) {
            RecoveryEvent::InventoryReported(inventory) => {
                assert_eq!(inventory.head, (Seq(3), digests[2]), "anchor {generation}");
            }
            other => panic!("anchor {generation}: adopted and reported, not {other:?}"),
        }
    }
    assert_eq!(
        adoptions(METHOD),
        [(2, 3, "link".to_owned()), (3, 4, "link".to_owned())]
    );
    assert!(logged!(METHOD, "inventory_tail_held").is_empty());
    assert_eq!(host.tail_refused_of(p), None);
}

/// Issue #121, check B at adoption, the at-rest case: generation 2 holds a tail at seqs 2 and 3
/// with its seq 1 gone (lost at rest, or written by a build from before checks A and B). An ask
/// at anchor 3 with L = 2 passes every adoption proof above the base, so before #121 it linked
/// generation 3 onto the holed lineage. Now it is refused `tail_has_no_floor`: `TailHeld`, the
/// tail held `Unchecked` (the reason is not in the `ADOPTION_REFUSALS` wire vocabulary), no
/// `tail_adopted`, and nothing written. The same tail whole from seq 1 adopts (R7-1), so the
/// refusal is the hole's.
/// Cut: drop check B from `Host::adoption`; the ask adopts by a link, red.
/// Unit.
#[config_log::retcd_test]
fn i121_adoption_refuses_a_tail_holed_below_its_base() {
    const METHOD: &str = "i121_adoption_refuses_a_tail_holed_below_its_base";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain_after(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(2),
        vec![Digest::ROOT],
        2,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let held = host.engine.lineages();
    let answer = host.inventory(
        p,
        CopyId(1),
        anchor_at(3, 2, digests[1]),
        Some(Generation(2)),
        None,
    );
    assert!(
        matches!(answer, RecoveryEvent::TailHeld { .. }),
        "a holed tail is held, not adopted: {answer:?}"
    );
    assert_eq!(host.engine.lineages(), held, "the refusal wrote nothing");
    assert!(adoptions(METHOD).is_empty(), "{:?}", adoptions(METHOD));
    let refused: Vec<_> = logged!(METHOD, "tail_not_adoptable")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(refused, [serde_json::Value::from("tail_has_no_floor")]);
    assert_eq!(
        host.tail_refused.get(&p).map(|refused| refused.adoption),
        Some(TailAdoption::Unchecked)
    );
}

/// R7-2 (design section 16, G1 and G3): after R7-1's first link (generation 3 onto 2), a
/// `ReadOnly` landing of generation 4 at cutoff 2 reports the tail `Adoptable`, and the next
/// inventory at generation 4 does adopt it: what the landing tells the owner's repair is what the
/// inventory then does.
/// Cut c1: red, the landing reports `NotReady("tail_sealed_for_other")`.
#[config_log::retcd_test]
fn u2_r7_2_a_landing_that_reports_adoptable_is_followed_by_an_adoption() {
    const METHOD: &str = "u2_r7_2_a_landing_that_reports_adoptable_is_followed_by_an_adoption";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = tail_in(&path, 2);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let l = Some(Generation(2));
    assert!(matches!(
        host.inventory(p, CopyId(1), anchor_at(3, 2, digests[1]), l, None),
        RecoveryEvent::InventoryReported(_)
    ));
    let mut landing = landing_at(2, digests[1]);
    landing.new_generation = Generation(4);
    landing.selected.root.generation = Generation(4);
    landing.ledger.last_writable = l;
    host.landing_scan(p, &landing);
    assert_eq!(
        host.tail_refused.get(&p).map(|refused| refused.adoption),
        Some(TailAdoption::Adoptable),
        "the landing reports the tail adoptable"
    );
    match host.inventory(p, CopyId(1), anchor_at(4, 2, digests[1]), l, None) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(3), digests[2]));
        }
        other => panic!("the inventory after the landing adopts: {other:?}"),
    }
    assert_eq!(adoptions(METHOD).len(), 2, "{:?}", adoptions(METHOD));
    assert_eq!(host.tail_refused_of(p), None);
}

/// R7-3 (design section 16, G1): R7-1 with generation 33's adoption a flatten. The tail
/// lineage reads through 32 levels, so the first link copies it (`copied`, base = head 3, depth
/// 1). The next ask, with the same L, ties the flattened child with every level below it and
/// links onto the child.
/// Cut c1: red at the first ask already, `TailHeld`: older-wins keeps generation 1, sealed by 2.
#[config_log::retcd_test]
fn u2_r7_3_a_flattened_child_ties_with_its_parent_and_is_linked_onto() {
    const METHOD: &str = "u2_r7_3_a_flattened_child_ties_with_its_parent_and_is_linked_onto";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = tail_at_the_ceiling(&path);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let l = Some(Generation(1));
    for generation in [33, 34] {
        match host.inventory(p, CopyId(1), anchor_at(generation, 2, digests[1]), l, None) {
            RecoveryEvent::InventoryReported(inventory) => {
                assert_eq!(inventory.head, (Seq(3), digests[2]), "anchor {generation}");
            }
            other => panic!("anchor {generation}: adopted and reported, not {other:?}"),
        }
    }
    assert_eq!(
        host.engine
            .link(p, Generation(33))
            .map(|link| (link.parent, link.base, link.copied)),
        Some((Generation(32), Seq(3), true)),
        "the first adoption flattened"
    );
    assert_eq!(
        adoptions(METHOD),
        [(32, 33, "flatten".to_owned()), (33, 34, "link".to_owned())],
        "the copy says flatten (lead ruling R8); the link onto it says link"
    );
}

/// R7-4 (design section 16, G1, critic F2): the Replay shape. Generation 3 holds the tail (head
/// 3, stamped 3) linked onto generation 1's first two records; the anchor's lineage, generation
/// 4, is linked at the base onto generation 2, which holds the same two records. The ask at
/// anchor 4 replays the tail onto generation 4 (`how=replay`); its answer is dropped. At the
/// next anchor, 5, generations 3 and 4 hold the same record at head 3 and the newer, the
/// anchor lineage that was replayed, is linked onto. This pins the choice only: the older
/// lineage is unsealed in this shape, so older-wins would link too.
/// Cut c1: red on the `from` assertion (3, not 4).
#[config_log::retcd_test]
fn u2_r7_4_after_a_replay_the_next_anchor_links_onto_the_replayed_lineage() {
    const METHOD: &str = "u2_r7_4_after_a_replay_the_next_anchor_links_onto_the_replayed_lineage";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let p = PartitionId(1);
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let below = write_chain(&mut engine, Generation(1), 2);
        for seq in 1..=2 {
            let (record, _) = engine
                .history_at(p, Generation(1), Seq(seq))
                .expect("read")
                .expect("a record");
            let envelope = ReplicationEnvelope::decode(&record).expect("decode");
            engine
                .commit(record_batch(
                    BatchId(seq),
                    p,
                    Generation(2),
                    &envelope,
                    record,
                ))
                .expect("the same record in generation 2");
        }
        engine
            .inherit(p, Generation(1), Generation(3), Seq(2))
            .expect("the tail's lineage");
        let digests = write_chain_after(&mut engine, Generation(3), below, 1);
        engine
            .inherit(p, Generation(2), Generation(4), Seq(2))
            .expect("the anchor's lineage");
        digests
    };
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let l = Some(Generation(3));
    for generation in [4, 5] {
        match host.inventory(p, CopyId(1), anchor_at(generation, 2, digests[1]), l, None) {
            RecoveryEvent::InventoryReported(inventory) => {
                assert_eq!(inventory.head, (Seq(3), digests[2]), "anchor {generation}");
            }
            other => panic!("anchor {generation}: adopted and reported, not {other:?}"),
        }
    }
    assert_eq!(
        adoptions(METHOD),
        [(3, 4, "replay".to_owned()), (4, 5, "link".to_owned())],
        "the next anchor links onto the replayed lineage"
    );
}

/// PR #110 review blocker 2: a Replay gap over [`ADOPT_REPLAY_MAX`] is refused before the
/// chain walk. R7-4's shape, with the tail on generation 3 running 1,026 records past the base
/// (head 1,028), and its chain broken at seq 10 (that record names a `prev_digest` no record
/// has). The walk from base to head would stop at seq 10 with `tail_not_contiguous`; refused
/// first, the answer is `tail_too_long`, so no walk ran. A hit within the limit over the same
/// break (head 20) still walks and answers `tail_not_contiguous`: Replay keeps its chain check.
/// Cut: the gap check after the walk, as before, red (`tail_not_contiguous` for head 1,028).
#[test]
fn pr110_an_oversized_replay_is_refused_before_the_chain_walk() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let p = PartitionId(1);
    let below = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let below = write_chain(&mut engine, Generation(1), 2);
        for seq in 1..=2 {
            let (record, _) = engine
                .history_at(p, Generation(1), Seq(seq))
                .expect("read")
                .expect("a record");
            let envelope = ReplicationEnvelope::decode(&record).expect("decode");
            engine
                .commit(record_batch(
                    BatchId(seq),
                    p,
                    Generation(2),
                    &envelope,
                    record,
                ))
                .expect("the same record in generation 2");
        }
        engine
            .inherit(p, Generation(1), Generation(3), Seq(2))
            .expect("the tail's lineage");
        let mut tail = write_chain_after(&mut engine, Generation(3), below.clone(), 7);
        *tail.last_mut().expect("seq 9") = Digest([9; 32]);
        let tail = write_chain_after(&mut engine, Generation(3), tail, 1);
        let tail = write_chain_after(&mut engine, Generation(3), tail, 1_018);
        assert_eq!(tail.len(), 1_028, "head 1,028");
        engine
            .inherit(p, Generation(2), Generation(4), Seq(2))
            .expect("the anchor's lineage");
        below
    };
    let rt = current_thread();
    let host = host_over(&path, &rt);
    assert_eq!(
        host.engine.buffered_applied(p, Generation(4)).0,
        2,
        "control: the anchor lineage replays from the base"
    );
    let hit = |head: u64| TailHit {
        from: Generation(3),
        head: Seq(head),
        stamp: Some(Generation(3)),
        digest: None,
        reason: "missed_generation_held_past_base",
    };
    let l = Some(Generation(3));
    assert_eq!(
        host.adoption(p, anchor_at(4, 2, below[1]), l, hit(1_028)),
        Err("tail_too_long"),
        "refused before the walk reaches the break"
    );
    assert_eq!(
        host.adoption(p, anchor_at(4, 2, below[1]), l, hit(20)),
        Err("tail_not_contiguous"),
        "a gap within the limit still walks the chain"
    );
}

/// R7-5 (design section 16, G2, critic F4): a copy holding generation 5, linked onto its tail
/// on generation 2, gets an ask at anchor 4 with L = 2. The ask is late: its CAS cannot succeed,
/// since a lineage at 5 exists only after a commit of 4. No scan and no adoption: it answers
/// `InventoryFailed`, logged `inventory_late` at info and no `inventory_failed`. Nothing is written
/// and the status names no tail.
/// Cuts: c2 (no early return), red, the answer is `TailHeld` with
/// `NotReady("tail_sealed_for_other")`; c3 (the C4 arm not widened), red, `inventory_failed` at
/// error.
#[config_log::retcd_test]
fn u2_r7_5_an_ask_behind_a_newer_lineage_is_late_and_writes_nothing() {
    const METHOD: &str = "u2_r7_5_an_ask_behind_a_newer_lineage_is_late_and_writes_nothing";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(2), 3);
        engine
            .inherit(PartitionId(1), Generation(2), Generation(5), Seq(3))
            .expect("generation 5 onto the tail");
        digests
    };
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let held = host.engine.lineages();
    assert_eq!(
        host.inventory(
            p,
            CopyId(1),
            anchor_at(4, 2, digests[1]),
            Some(Generation(2)),
            None
        ),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) }
    );
    let late = logged!(METHOD, "inventory_late");
    assert_eq!(late.len(), 1, "{late:?}");
    assert_eq!(late[0]["@l"], "Information", "{late:?}");
    assert!(logged!(METHOD, "inventory_failed").is_empty());
    assert!(logged!(METHOD, "tail_scan").is_empty(), "no scan");
    assert_eq!(host.engine.lineages(), held, "nothing written");
    assert_eq!(host.tail_refused_of(p), None);
}

/// R7-7 (design section 16, G3, critic F1): `Host::adoption` called with a hit the scan cannot
/// give since G1, the tail's lineage (2) sealed by a non-copied child (3) other than the anchor's
/// generation (4). The link would be refused, so adoption says so first, as a hold that keeps the
/// flag (`NotReady`): the child holds a pointer, not the bytes. Nothing is written.
/// Cut c4 (G3's seal check removed): red, `Ok(Link)`.
#[test]
fn u2_r7_7_adoption_names_a_tail_sealed_for_another_child() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(2), 3);
        engine
            .inherit(PartitionId(1), Generation(2), Generation(3), Seq(3))
            .expect("generation 3 onto the tail");
        digests
    };
    let rt = current_thread();
    let host = host_over(&path, &rt);
    let held = host.engine.lineages();
    let hit = TailHit {
        from: Generation(2),
        head: Seq(3),
        stamp: Some(Generation(2)),
        digest: Some(digests[2]),
        reason: "missed_generation_held_past_base",
    };
    let answer = host.adoption(
        PartitionId(1),
        anchor_at(4, 2, digests[1]),
        Some(Generation(2)),
        hit,
    );
    assert_eq!(answer, Err("tail_sealed_for_other"));
    assert_eq!(
        TailAdoption::of(&answer),
        TailAdoption::NotReady("tail_sealed_for_other")
    );
    assert_eq!(host.engine.lineages(), held, "nothing written");
}

/// R7-8 (design section 16, G3): a tail of 3 on generation 2 with generation 3 staging a
/// release set-aside from it at base 2 (a released run that crashed part way). Generation 4's
/// landing reports `NotReady("tail_parent_copying")`, and its inventory answers `TailHeld`
/// with that state: the repair holds and a bare `forget-tail` needs `--give-up-live-copy`. Once
/// the same-arguments re-run finishes the copy, the tail is set aside: `Refused("tail_set_aside")`.
/// Cut c5 (G3's staging-child check removed): red, the landing says `Adoptable`, then the
/// inventory's link is refused (`Unchecked`).
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn u2_r7_8_a_tail_whose_set_aside_is_still_copying_is_not_ready_until_it_finishes() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let p = PartitionId(1);
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(2), 3);
        engine.inject_fault(rdb_storage::InjectedFault::CopyBatch);
        assert!(engine
            .inherit(p, Generation(2), Generation(3), Seq(2))
            .is_err());
        assert_eq!(
            engine.staging(p, Generation(3)),
            Some((Generation(2), Seq(2)))
        );
        digests
    };
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let l = Some(Generation(2));
    let mut landing = landing_at(2, digests[1]);
    landing.new_generation = Generation(4);
    landing.selected.root.generation = Generation(4);
    landing.ledger.last_writable = l;
    host.landing_scan(p, &landing);
    let copying = TailAdoption::NotReady("tail_parent_copying");
    assert_eq!(
        host.tail_refused.get(&p).map(|refused| refused.adoption),
        Some(copying),
        "the landing"
    );
    assert!(
        matches!(
            host.inventory(p, CopyId(1), anchor_at(4, 2, digests[1]), l, None),
            RecoveryEvent::TailHeld { .. }
        ),
        "the inventory holds"
    );
    let adoption = host.tail_refused.get(&p).map(|refused| refused.adoption);
    assert_eq!(adoption, Some(copying), "the inventory");
    assert!(!copying.is_adoptable(), "the repair holds");
    assert!(copying.needs_give_up_flag(), "forget-tail needs the flag");

    host.engine
        .inherit(p, Generation(2), Generation(3), Seq(2))
        .expect("the same-arguments re-run finishes the copy");
    let hit = host
        .tail_scan(p, anchor_at(4, 2, digests[1]), l)
        .expect("the tail");
    assert_eq!(
        host.adoption(p, anchor_at(4, 2, digests[1]), l, hit),
        Err("tail_set_aside"),
        "set aside once the copy finished"
    );
}

/// R7-9 (design section 16, F7): a tail whose lineage reads through 32 levels is adopted, the
/// child flattened (`copied` at base = head 3), and the copy reports head 3. `tail_adopted`
/// says `how=flatten` (lead ruling R8): the link became a full copy.
/// Cut c6 (refuse at the ceiling, `NotReady`): red, `TailHeld`. Red under c1 too, as R7-3.
/// Cut: `how` says `link` for a copy, red on the `how` assertion.
#[config_log::retcd_test]
fn u2_r7_9_a_tail_at_the_depth_ceiling_is_adopted_by_a_flatten() {
    const METHOD: &str = "u2_r7_9_a_tail_at_the_depth_ceiling_is_adopted_by_a_flatten";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = tail_at_the_ceiling(&path);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    match host.inventory(
        p,
        CopyId(1),
        anchor_at(33, 2, digests[1]),
        Some(Generation(1)),
        None,
    ) {
        RecoveryEvent::InventoryReported(inventory) => {
            assert_eq!(inventory.head, (Seq(3), digests[2]));
        }
        other => panic!("adopted at the ceiling: {other:?}"),
    }
    assert_eq!(
        host.engine
            .link(p, Generation(33))
            .map(|link| (link.base, link.copied)),
        Some((Seq(3), true)),
        "flattened at the head"
    );
    assert_eq!(
        adoptions(METHOD),
        [(32, 33, "flatten".to_owned())],
        "a link that became a full copy says so"
    );
}

/// R7-10 (design section 16, G4): the flatten of R7-9 faults on its copy batch, so generation 33
/// is left staging a copy of the whole tail, and the node restarts.
/// (a) The same ask again: adoption resumes the copy into 33 (`tail_adopted from=32 to=33`,
/// `how=flatten`, lead ruling R8), and 33 is `copied` at base = head 3.
/// (b) Variant, the next ask is at anchor 34: the staged child 33 is finished, not aborted, and
/// 34 links onto it (`tail_adopted from=33 to=34`, `how=resume`); no `tail_parent_copying`.
/// Cuts: c7 (no same-anchor arm), red in (a), `NotReady("replay_not_ready")`; c8 (no
/// later-anchor arm), red in (b), `NotReady("tail_parent_copying")`. Red under c1 and c6 too: the
/// flatten is never staged.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn u2_r7_10_a_flatten_that_crashed_part_way_is_resumed_by_the_next_ask() {
    const METHOD: &str = "u2_r7_10_a_flatten_that_crashed_part_way_is_resumed_by_the_next_ask";
    let rt = current_thread();
    let p = PartitionId(1);
    let l = Some(Generation(1));
    for next in [33, 34] {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        let digests = tail_at_the_ceiling(&path);
        let mut host = host_over(&path, &rt);
        host.engine
            .inject_fault(rdb_storage::InjectedFault::CopyBatch);
        assert!(
            matches!(
                host.inventory(p, CopyId(1), anchor_at(33, 2, digests[1]), l, None),
                RecoveryEvent::TailHeld { .. }
            ),
            "the flatten faulted"
        );
        assert_eq!(
            host.engine.staging(p, Generation(33)),
            Some((Generation(32), Seq(3))),
            "staged: a copy of the whole tail"
        );
        drop(host);
        let mut host = host_over(&path, &rt);
        let before = adoptions(METHOD).len();
        match host.inventory(p, CopyId(1), anchor_at(next, 2, digests[1]), l, None) {
            RecoveryEvent::InventoryReported(inventory) => {
                assert_eq!(inventory.head, (Seq(3), digests[2]), "anchor {next}");
            }
            other => panic!("anchor {next}: resumed and reported, not {other:?}"),
        }
        assert_eq!(host.engine.staging(p, Generation(33)), None, "finished");
        assert_eq!(
            host.engine
                .link(p, Generation(33))
                .map(|link| (link.parent, link.base, link.copied)),
            Some((Generation(32), Seq(3), true)),
            "33 holds the whole tail, copied"
        );
        let expected = if next == 33 {
            (32, 33, "flatten".to_owned())
        } else {
            (33, 34, "resume".to_owned())
        };
        assert_eq!(adoptions(METHOD)[before..], [expected], "anchor {next}");
    }
    let held: Vec<_> = logged!(METHOD, "inventory_tail_held")
        .into_iter()
        .filter(|line| line["adoption"] == "NotReady(\"tail_parent_copying\")")
        .collect();
    assert!(held.is_empty(), "{held:?}");
}

// ------------------------------------------------- issue #107 slice 1: checks A and B

/// Node 2's live receiver of `result`'s pin, in lineage `generation` at `head`, as a process
/// that never restarted still holds it from an earlier generation.
fn live_receiver(result: &RecoveryResult, generation: u64, head: Head) -> AppendReceiver {
    AppendReceiver::new(receiver_init(result, generation, head)).expect("a live receiver")
}

/// Node 2's receiver arguments for [`live_receiver`] and the dormant row.
fn receiver_init(result: &RecoveryResult, generation: u64, head: Head) -> ReceiverInit {
    let config = result.committed.pinned_config.clone();
    let own = config
        .members
        .iter()
        .find(|member| member.node == NodeId(2))
        .expect("node 2 is pinned")
        .copy;
    ReceiverInit {
        config,
        own,
        lineage: Lineage {
            partition: PartitionId(1),
            generation: Generation(generation),
            owner_epoch: OwnerEpoch(generation),
        },
        head,
        durable: DurableSeq(0),
    }
}

/// Quarantine `receiver` the way R1 does: its pinned primary sends the next record with a
/// record digest that is not its own (row 7, `CorruptHistory`).
fn quarantine(receiver: &mut AppendReceiver, config: &PartitionConfig) {
    use rdb_core::contracts::envelope::EnvelopeHeader;
    use rdb_core::contracts::ids::{ClientId, LeaseId, TenantId};
    use rdb_core::contracts::storage::Write;
    use rdb_core::contracts::txn::Outcome;
    let lineage = receiver.lineage();
    let primary = config
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .expect("a pinned primary");
    let seq = receiver.applied_head().seq.next();
    let envelope = ReplicationEnvelope {
        header: EnvelopeHeader {
            protocol_version: ENVELOPE_VERSION,
            partition: lineage.partition,
            generation: lineage.generation,
            config_version: config.config_version,
            owner_epoch: lineage.owner_epoch,
            seq,
            body_len: 0,
        },
        lease_id: LeaseId(1),
        prev_digest: receiver.applied_head().digest,
        request_identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(seq.0),
        },
        request_digest: Digest::of(Domain::Record, &[&seq.0.to_le_bytes()]),
        conditions_result: Vec::new(),
        mutations: vec![Write {
            ns: Namespace::User,
            key: Bytes::from_static(b"k"),
            value: Some(Bytes::from_static(b"v")),
        }],
        result: Outcome::Published,
        record_digest: Digest::ROOT,
    };
    let from = PeerLabel {
        node: primary.node,
        boot: primary.boot,
        authenticated: true,
    };
    let frame = Frame {
        id: MessageId(1),
        protocol: ENVELOPE_VERSION,
        config: config.config_version,
        sender: lineage,
        body: envelope.encode().expect("encode"),
    };
    let effects = receiver.on_append(&from, &frame);
    assert!(
        receiver.quarantine().is_some(),
        "the receiver quarantined: {effects:?}"
    );
}

/// Issue #107 slice 1, rows 2a and 31a (#107), with row 5's refusal clause:
/// check A's refusal arm in `land_link`, on a copy that never restarted. Node 2 holds
/// generation 1 to seq 3 and a live receiver there; the commit's cutoff is 2, so `land_at`
/// refuses `held_past_base`. In a commit that takes writes (`DegradedRf2`) the live receiver is
/// removed, quarantined or not, and `land_receiver_dropped` names the reason, so the copy
/// walks up from seq 1 rather than re-anchoring over a hole. In a `ReadOnly` commit the
/// receiver stays, as the held tail does, with no `land_receiver_dropped`. With `live_readd`
/// off it stays unchanged. With it on (#107 slice 2a, row 31b) it is also marked refused at
/// this generation (`AppendReceiver::refuse_landing`, one `land_receiver_marked_refused` per
/// case), so its `Recovered` never anchors it below the cutoff: at the cutoff it holds, or at
/// the root. Nothing links in any case.
/// Cuts: keep the receiver on refusal, red (2a); remove the `ReadOnly` exception, red (31a);
/// the mark set whatever the setting, red (an OFF receiver marked).
/// Unit, under a second here (one trio bootstrap, for the commit it lands).
#[config_log::retcd_test]
fn issue107_a_refused_landing_drops_a_live_receiver_only_in_a_writable_commit() {
    const METHOD: &str =
        "issue107_a_refused_landing_drops_a_live_receiver_only_in_a_writable_commit";
    let rt = current_thread();
    let mut writable: Option<RecoveryResult> = None;
    for (mode, quarantined, live) in [
        (PartitionMode::DegradedRf2, false, false),
        (PartitionMode::DegradedRf2, true, false),
        (PartitionMode::ReadOnly, false, false),
        (PartitionMode::ReadOnly, true, false),
        (PartitionMode::ReadOnly, false, true),
        (PartitionMode::ReadOnly, true, true),
    ] {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        let digests = write_chain(
            &mut RocksEngine::open(&path).expect("open engine"),
            Generation(1),
            3,
        );
        let mut result = writable
            .get_or_insert_with(|| landing_at(2, digests[1]))
            .clone();
        result.mode = mode.clone();
        let mut receiver = live_receiver(
            &result,
            1,
            Head {
                seq: Seq(3),
                digest: digests[2],
            },
        );
        if quarantined {
            quarantine(&mut receiver, &result.committed.pinned_config);
        }
        let mut host = host_over(&path, &rt);
        host.live_readd.on = live;
        host.replication.install_receiver(receiver.clone());
        let held = host.engine.lineages();
        let case = format!("{mode:?}, quarantined {quarantined}, live_readd {live}");
        assert_eq!(host.land_link(PartitionId(1), &result), Ok(None), "{case}");
        assert_eq!(host.engine.lineages(), held, "{case}: nothing linked");
        let after = host.replication.receiver(NodeId(2), PartitionId(1));
        if mode == PartitionMode::ReadOnly {
            // #107 slice 2a (lead ruling on row 31b): kept; with the setting on, also marked so
            // it claims nothing of the lineage it refused. Otherwise unchanged.
            let mut kept = receiver.clone();
            if live {
                kept.refuse_landing(result.new_generation);
            }
            assert_eq!(after, Some(&kept), "{case}: the receiver stays");
        } else {
            assert_eq!(after, None, "{case}: the live receiver is dropped");
        }
        assert_eq!(host.fault, None, "{case}");
    }
    let dropped: Vec<_> = logged!(METHOD, "land_receiver_dropped")
        .into_iter()
        .map(|line| {
            (
                line["reason"].clone(),
                line["quarantined"].clone(),
                line["mode"].clone(),
            )
        })
        .collect();
    assert_eq!(
        dropped,
        [false, true].map(|quarantined| (
            "held_past_base".into(),
            quarantined.into(),
            "DegradedRf2".into()
        )),
        "one drop per writable case, none in ReadOnly"
    );
    let refused = logged!(METHOD, "land_link_refused");
    assert_eq!(refused.len(), 6, "every case refused: {refused:?}");
    let marked = logged!(METHOD, "land_receiver_marked_refused");
    assert_eq!(
        marked.len(),
        2,
        "the two ReadOnly cases with the setting on: {marked:?}"
    );
}

/// Issue #107 slice 1, row 5 (#107, check A): check A's success arm. Node 2
/// holds generation 1 exactly through the cutoff and lands generation 2 there, linked. A live
/// receiver it holds at another head is replaced by one at the linked head, compared whole
/// (`land_receiver_installed`): one at seq 1, below the link, and one at the linked seq with
/// another digest, which a sequence-only comparison would keep. A quarantined one is kept
/// (`land_receiver_kept`), as it must stay. A live receiver at the linked head is kept too:
/// `m10_s2_59_a_second_landing_in_one_boot_keeps_the_live_receiver`.
/// Cuts: keep the live receiver always, red (the mismatched receiver stays at seq 1); compare
/// the seq only, red (the wrong-digest receiver stays).
/// Unit, under a second here (one trio bootstrap, for the commit it lands).
#[config_log::retcd_test]
fn issue107_a_linked_landing_keeps_a_live_receiver_only_at_the_linked_head() {
    const METHOD: &str = "issue107_a_linked_landing_keeps_a_live_receiver_only_at_the_linked_head";
    let rt = current_thread();
    let mut landing: Option<RecoveryResult> = None;
    for (case, seq, wrong_digest, quarantined) in [
        ("below the link", 1, false, false),
        ("below the link, quarantined", 1, false, true),
        ("at the linked seq with another digest", 2, true, false),
    ] {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        let digests = write_chain(
            &mut RocksEngine::open(&path).expect("open engine"),
            Generation(1),
            2,
        );
        let result = landing
            .get_or_insert_with(|| landing_at(2, digests[1]))
            .clone();
        let digest = if wrong_digest {
            digests[0]
        } else {
            digests[seq - 1]
        };
        let mut receiver = live_receiver(
            &result,
            1,
            Head {
                seq: Seq(seq as u64),
                digest,
            },
        );
        if quarantined {
            quarantine(&mut receiver, &result.committed.pinned_config);
        }
        let mut host = host_over(&path, &rt);
        host.replication.install_receiver(receiver.clone());
        assert!(
            matches!(
                host.land_link(PartitionId(1), &result),
                Ok(Some(RecoveryEvent::CopyCaughtUp { head: Seq(2), .. }))
            ),
            "{case}: linked and caught up"
        );
        let after = host
            .replication
            .receiver(NodeId(2), PartitionId(1))
            .expect("a receiver");
        if quarantined {
            assert_eq!(after, &receiver, "{case}: a quarantined receiver stays");
        } else {
            assert_eq!(
                (after.lineage(), after.applied_head()),
                (
                    result.selected.root,
                    Head {
                        seq: Seq(2),
                        digest: digests[1],
                    }
                ),
                "{case}: replaced at the linked head"
            );
        }
    }
    assert_eq!(logged!(METHOD, "land_receiver_installed").len(), 2);
    let kept: Vec<_> = logged!(METHOD, "land_receiver_kept")
        .into_iter()
        .map(|line| line["quarantined"].clone())
        .collect();
    assert_eq!(kept, [serde_json::Value::Bool(true)]);
}

/// Issue #107 slice 1, row 3: check B. Generation 1 holds seqs 3 and 4 only, holed below its
/// head. Neither a landing (`land_at`, `from_has_no_floor`) nor an inventory
/// (`inherit_missed_link`, `missed_generation_from_has_no_floor`, answered `InventoryFailed`)
/// links a new generation onto it, and neither does a landing onto generation 2, linked onto
/// that holed parent: one read walks the chain. Nothing is written by any refusal. A lineage
/// flattened at the depth ceiling (a copied level) still reaches seq 1 and links.
/// Cuts: check B out of `land_at`, red (the holed landing links); out of
/// `inherit_missed_link`, red (the inventory reports the holed lineage as a survivor);
/// `has_floor` always true, red (here and `issue105_a_copy_holds_l_intact_only_with_l_from_seq_1`,
/// its other caller). Unit.
#[config_log::retcd_test]
fn issue107_no_floor_no_link() {
    const METHOD: &str = "issue107_no_floor_no_link";
    let p = PartitionId(1);
    let rt = current_thread();
    // A holed generation 1, a landing and an inventory onto it.
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain_after(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        vec![Digest::ROOT; 2],
        2,
    );
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    assert_eq!(
        host.land_at(p, anchor_at(2, 4, digests[3])),
        Err("from_has_no_floor")
    );
    assert_eq!(host.engine.lineages(), held, "the landing wrote nothing");
    assert_eq!(
        host.inventory(p, CopyId(1), anchor_at(2, 4, digests[3]), None, None),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) }
    );
    assert_eq!(host.engine.lineages(), held, "the inventory wrote nothing");
    let failed: Vec<_> = logged!(METHOD, "inventory_failed")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        failed,
        [serde_json::Value::from(
            "missed_generation_from_has_no_floor"
        )]
    );
    drop(host);

    // Generation 2 linked at seq 4 onto the holed generation 1: a landing of 3 from 2.
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain_after(&mut engine, Generation(1), vec![Digest::ROOT; 2], 2);
        engine
            .inherit(p, Generation(1), Generation(2), Seq(4))
            .expect("link onto the holed parent");
        digests
    };
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    assert_eq!(
        host.land_at(p, anchor_at(3, 4, digests[3])),
        Err("from_has_no_floor")
    );
    assert_eq!(host.engine.lineages(), held, "nothing linked onto 2");
    drop(host);

    // Generation 33 flattened at the ceiling onto 32 (copied): a landing of 34 from 33 links.
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let digests = tail_at_the_ceiling(&path);
        let mut engine = RocksEngine::open(&path).expect("open engine");
        engine
            .inherit(p, Generation(32), Generation(33), Seq(3))
            .expect("flatten at the ceiling");
        assert!(
            engine
                .link(p, Generation(33))
                .is_some_and(|link| link.copied),
            "33 is a copied level"
        );
        digests
    };
    let mut host = host_over(&path, &rt);
    assert_eq!(
        host.land_at(p, anchor_at(34, 3, digests[2])),
        Ok((Generation(33), Seq(3), digests[2]))
    );
}

/// Issue #107 slice 1, check B below the base (`land_at` arm 3). Generation 1 holds seqs 3 and
/// 4 only, holed below its head 4. A landing of 2 at base 5 sits above that head, so the base
/// digest is not compared and the head's record reads and verifies: only check B stands
/// between it and a link. It refuses `from_has_no_floor`, and nothing is written: lineages,
/// generation 2's link and the cached view are unchanged.
/// Cut: run check B only when `head == base`, red (generation 2 links at seq 4).
/// Unit.
#[config_log::retcd_test]
fn issue107_a_landing_above_a_holed_head_refuses_and_writes_nothing() {
    let p = PartitionId(1);
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain_after(&mut engine, Generation(1), vec![Digest::ROOT; 2], 2);
    }
    let mut host = host_over(&path, &rt);
    host.refresh_view(p, Generation(1)).expect("a step view");
    let before = untouched(&host);
    assert!(before.view.is_some(), "a cached view");
    assert_eq!(
        host.land_at(p, anchor_at(2, 5, Digest::ROOT)),
        Err("from_has_no_floor")
    );
    assert_eq!(untouched(&host), before, "the refusal wrote nothing");
}

/// Issue #107 slice 1, row 2a's dormant clause: check A drops only a live receiver. Node 2
/// holds generation 1 to seq 3 and a dormant receiver there (one restored to source a
/// catch-up); the commit's cutoff is 2, so `land_at` refuses `held_past_base` in a commit that
/// takes writes (`DegradedRf2`). The dormant receiver stays installed and no
/// `land_receiver_dropped` is logged. Nothing links. With `live_readd` off it stays unchanged.
/// With it on (#107 slice 2a) it is also marked refused at generation 2
/// (`AppendReceiver::refuse_landing`, one `land_receiver_marked_refused`). Generation 2's own
/// `Recovered` does not read that mark: `Replication::recovered` removes a dormant receiver
/// first and builds a fresh one from the barrier's proof or the root. The mark matters when an
/// earlier generation's `Recovered` in the same due batch rebuilds the copy first: the fresh
/// receiver takes the mark over (rdb-core
/// `issue107_r2_a_mark_on_a_dormant_receiver_outlives_an_earlier_barriers_rebuild`, and through
/// the host's due batch
/// `issue107_r3_a_dormant_receivers_mark_outlives_an_earlier_barrier_in_the_same_due_batch`).
/// Cuts: drop the `is_dormant` filter on the refusal arm, red (the dormant receiver is
/// removed); the mark set whatever the setting, red (the OFF receiver marked).
/// Unit, under a second here (one trio bootstrap, for the commit it lands).
#[config_log::retcd_test]
fn issue107_a_refused_writable_landing_keeps_a_dormant_receiver() {
    const METHOD: &str = "issue107_a_refused_writable_landing_keeps_a_dormant_receiver";
    use rdb_core::contracts::ids::Revision;
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        3,
    );
    let mut result = landing_at(2, digests[1]);
    result.mode = PartitionMode::DegradedRf2;
    let receiver = AppendReceiver::dormant(
        receiver_init(
            &result,
            1,
            Head {
                seq: Seq(3),
                digest: digests[2],
            },
        ),
        Revision(1),
    )
    .expect("a dormant receiver");
    assert!(receiver.is_dormant());
    for live in [false, true] {
        let mut host = host_over(&path, &rt);
        host.live_readd.on = live;
        host.replication.install_receiver(receiver.clone());
        let held = host.engine.lineages();
        assert_eq!(host.land_link(PartitionId(1), &result), Ok(None));
        assert_eq!(host.engine.lineages(), held, "nothing linked");
        let mut kept = receiver.clone();
        if live {
            kept.refuse_landing(Generation(2));
        }
        assert_eq!(
            host.replication.receiver(NodeId(2), PartitionId(1)),
            Some(&kept),
            "live_readd {live}: the dormant receiver stays installed"
        );
        assert_eq!(host.fault, None);
        assert_eq!(
            logged!(METHOD, "land_receiver_marked_refused").len(),
            usize::from(live),
            "live_readd {live}"
        );
    }
    assert_eq!(logged!(METHOD, "land_receiver_dropped").len(), 0);
    let refused: Vec<_> = logged!(METHOD, "land_link_refused")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        refused,
        ["held_past_base", "held_past_base"].map(serde_json::Value::from)
    );
}

/// #107 slice 2a review, round 3: the round-2 question (an earlier barrier and a later refused
/// `ReadOnly` landing in one due batch, over a dormant receiver) through the host's own wiring,
/// with no mark set by hand. Node 2 holds generation 1 at seqs 3 to 5 only, holed below 5, and
/// a dormant receiver there, with `live_readd` on. Two `Landed` messages arrive and fall due in
/// one `run_due`: generation 2, whose barrier node 2 is in, at cutoff 5 (`inherit` links it),
/// then generation 3, `ReadOnly`, not in its barrier, at cutoff 7. `land_link` refuses 3
/// `from_has_no_floor` (check B: 2 is linked onto the holed 1) and marks the dormant receiver,
/// because both `Recovered`s are still queued. The drain then hands them to R1, oldest first:
/// 2's removes the dormant receiver and builds a fresh one at the barrier's proved `(5, d5)`;
/// 3's cutoff is not one it holds (`NotRetained`), so with the mark carried over it anchors at
/// the root, generation 3, and never on rung 5, inside the prefix it refused.
/// Cuts: `Replication::recovered` carries no marks to the receiver built in the dormant one's
/// place, red (the receiver anchors at seq 5); `land_link` never marks, red (no
/// `land_receiver_marked_refused`).
/// Unit, under a second here (one trio bootstrap, for the commit it lands).
#[config_log::retcd_test]
fn issue107_r3_a_dormant_receivers_mark_outlives_an_earlier_barrier_in_the_same_due_batch() {
    const METHOD: &str =
        "issue107_r3_a_dormant_receivers_mark_outlives_an_earlier_barrier_in_the_same_due_batch";
    use rdb_core::contracts::ids::Revision;
    use rdb_core::contracts::recovery::RecoveryBarrier;
    let p = PartitionId(1);
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain_after(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(1),
        vec![Digest::ROOT; 2],
        3,
    );
    let at_five = Head {
        seq: Seq(5),
        digest: digests[4],
    };
    // Generation 2 at (5, d5), proved by node 2's copy.
    let mut barrier = landing_at(5, digests[4]);
    let own = barrier
        .committed
        .pinned_config
        .members
        .iter()
        .find(|member| member.node == NodeId(2))
        .copied()
        .expect("node 2 is pinned");
    let proof = DurableProof {
        copy: own.copy,
        partition: p,
        seq: DurableSeq(5),
        digest: digests[4],
        boot: own.boot,
    };
    barrier.barrier =
        RecoveryBarrier::try_new(&[proof], &BTreeSet::from([own.copy]), Seq(5), digests[4])
            .expect("node 2 proved the cutoff");
    // Generation 3, ReadOnly, at a cutoff above anything node 2 holds; node 2 is outside it.
    let mut refused = barrier.clone();
    refused.new_generation = Generation(3);
    refused.selected.root.generation = Generation(3);
    refused.selected.cutoff_seq = Seq(7);
    refused.selected.cutoff_digest = Digest([7; 32]);
    refused.retained_status_map.predecessor_generation = Generation(2);
    refused.mode = PartitionMode::ReadOnly;
    refused.barrier =
        RecoveryBarrier::try_new(&[], &BTreeSet::new(), Seq(7), Digest([7; 32])).expect("empty");

    let mut host = host_over(&path, &rt);
    host.live_readd.on = true;
    let dormant = AppendReceiver::dormant(receiver_init(&barrier, 1, at_five), Revision(1))
        .expect("a dormant receiver");
    host.replication.install_receiver(dormant);
    let now = host.clock.now();
    for result in [barrier, refused] {
        host.handle(Msg::Landed {
            result: Box::new(result),
            partition: p,
            correlation: CorrelationId(0),
            emitter: NodeId(1),
            not_before: now,
        });
    }
    host.run_due();
    assert_eq!(
        logged!(METHOD, "land_receiver_marked_refused").len(),
        1,
        "the refusal marked the receiver before either Recovered ran"
    );
    assert!(
        host.replication
            .receiver(NodeId(2), p)
            .is_some_and(AppendReceiver::is_dormant),
        "still dormant: both Recovered events are queued"
    );
    host.drain();
    assert_eq!(host.fault, None);
    let refusals: Vec<_> = logged!(METHOD, "land_link_refused")
        .into_iter()
        .map(|line| (line["generation"].clone(), line["reason"].clone()))
        .collect();
    assert_eq!(
        refusals,
        [(3.into(), "from_has_no_floor".into())],
        "only generation 3 is refused"
    );
    let receiver = host
        .replication
        .receiver(NodeId(2), p)
        .expect("node 2 holds a receiver");
    assert!(
        !receiver.is_dormant(),
        "rebuilt by generation 2's Recovered"
    );
    assert_eq!(
        (receiver.lineage().generation, receiver.applied_head()),
        (
            Generation(3),
            Head {
                seq: Seq::ZERO,
                digest: Digest::ROOT,
            }
        ),
        "generation 3's Recovered anchors it at the root, not at seq 5"
    );
}

/// Generation 1 of partition 1 at `path`, seqs 1 to 3, with the stored `History` value at seq 1
/// cut to 3 bytes (shorter than a frame): the head reads and verifies, and a read of seq 1
/// faults `Corrupt`. Answers the digests as written, `[0]` at seq 1.
fn floor_unreadable(path: &std::path::Path) -> Vec<Digest> {
    let digests = write_chain(
        &mut RocksEngine::open(path).expect("open engine"),
        Generation(1),
        3,
    );
    let raw = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        path,
        rdb_storage::keys::COLUMN_FAMILIES,
    )
    .expect("raw open");
    let cf = raw
        .cf_handle(rdb_storage::keys::cf_for(Namespace::History))
        .expect("history cf");
    let key = rdb_storage::keys::encode_key(
        PartitionId(1),
        Generation(1),
        Namespace::History,
        &1u64.to_be_bytes(),
    );
    assert!(
        raw.get_cf(cf, &key).expect("raw read").is_some(),
        "seq 1 is stored"
    );
    raw.put_cf(cf, &key, [0, 0, 0]).expect("raw put");
    digests
}

/// What a check B refusal must leave as it was: the lineages, generation 2's link (none), and
/// the cached step view's key.
#[derive(Debug, PartialEq)]
struct Untouched {
    lineages: Vec<(PartitionId, Generation)>,
    link: Option<rdb_storage::Link>,
    view: Option<(Generation, AppliedSeq)>,
}

fn untouched(host: &Host) -> Untouched {
    let p = PartitionId(1);
    Untouched {
        lineages: host.engine.lineages(),
        link: host.engine.link(p, Generation(2)),
        view: host.views.get(&p).map(|(key, _)| *key),
    }
}

/// Issue #107 slice 1, check B's read-error arm in `land_at`. Generation 1's head (seq 3) reads,
/// and its seq 1 does not. A landing of 2 at base 3 passes every guard before check B, and
/// check B's read faults: `land_at` refuses `history_read_failed`, logs
/// `land_floor_read_failed` with the partition, the source generation and the fault, and links
/// nothing: lineages, links and the cached view are unchanged.
/// Cut: swallow the read error (`Err(_) => {}`), red (generation 2 links).
/// Unit.
#[config_log::retcd_test]
fn issue107_a_landing_whose_floor_read_fails_refuses_and_writes_nothing() {
    const METHOD: &str = "issue107_a_landing_whose_floor_read_fails_refuses_and_writes_nothing";
    let p = PartitionId(1);
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = floor_unreadable(&path);
    let mut host = host_over(&path, &rt);
    host.refresh_view(p, Generation(1)).expect("a step view");
    let before = untouched(&host);
    assert!(before.view.is_some(), "a cached view");
    assert_eq!(
        host.land_at(p, anchor_at(2, 3, digests[2])),
        Err("history_read_failed")
    );
    assert_eq!(untouched(&host), before, "the refusal wrote nothing");
    let logged: Vec<_> = logged!(METHOD, "land_floor_read_failed")
        .into_iter()
        .map(|line| {
            (
                line["partition"].clone(),
                line["from"].clone(),
                line["fault"]
                    .as_str()
                    .is_some_and(|f| f.contains("Corrupt")),
            )
        })
        .collect();
    assert_eq!(logged, [(1.into(), 1.into(), true)]);
}

/// Issue #107 slice 1, check B's read-error arm in `inherit_missed_link`, on the inventory
/// path. The copy missed generation 2 and holds generation 1 exactly through its base (seq 3),
/// whose head reads and whose seq 1 does not. The inventory answers `InventoryFailed`, named
/// `missed_generation_history_read_failed` in `inventory_failed`, after
/// `missed_generation_floor_read_failed` logs the partition, the source generation and the
/// fault. Nothing is linked: lineages, links and the cached view are unchanged.
/// Cut: swallow the read error (`Err(_) => {}`), red (the inventory links 2 and reports).
/// Unit.
#[config_log::retcd_test]
fn issue107_an_inventory_whose_floor_read_fails_refuses_and_writes_nothing() {
    const METHOD: &str = "issue107_an_inventory_whose_floor_read_fails_refuses_and_writes_nothing";
    let p = PartitionId(1);
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = floor_unreadable(&path);
    let mut host = host_over(&path, &rt);
    host.refresh_view(p, Generation(1)).expect("a step view");
    let before = untouched(&host);
    assert!(before.view.is_some(), "a cached view");
    assert_eq!(
        host.inventory(p, CopyId(1), anchor_at(2, 3, digests[2]), None, None),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) }
    );
    assert_eq!(untouched(&host), before, "the refusal wrote nothing");
    let failed: Vec<_> = logged!(METHOD, "inventory_failed")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        failed,
        [serde_json::Value::from(
            "missed_generation_history_read_failed"
        )]
    );
    let logged: Vec<_> = logged!(METHOD, "missed_generation_floor_read_failed")
        .into_iter()
        .map(|line| {
            (
                line["partition"].clone(),
                line["from"].clone(),
                line["fault"]
                    .as_str()
                    .is_some_and(|f| f.contains("Corrupt")),
            )
        })
        .collect();
    assert_eq!(logged, [(1.into(), 1.into(), true)]);
}

// ------------------------------------------------------------------ issue #61 (A5) rows

/// Issue #61: trim `generation`'s History on `engine` to `seq`, rooted at `(0, ROOT)`, as the
/// host's trim stages it. The record at `seq` is kept.
fn trimmed_to(engine: &mut RocksEngine, generation: Generation, seq: u64) {
    trimmed_at(engine, generation, seq, (0, Digest::ROOT));
}

/// [`trimmed_to`], with the mark rooted at `(root_base, root_digest)`.
fn trimmed_at(engine: &mut RocksEngine, generation: Generation, seq: u64, root: (u64, Digest)) {
    engine
        .stage_trim_history(
            PartitionId(1),
            generation,
            TrimFloor {
                seq: Seq(seq),
                root_base: Seq(root.0),
                root_digest: root.1,
            },
        )
        .expect("the trim is staged");
}

/// Issue #61 (t4's note, design rev 5): check B after a trim. Generation 1 holds seqs 1-6,
/// trimmed to 4: seqs 4-6 are kept and 1-3 are gone. `has_floor` reads the record at the floor
/// and finds it, so the trimmed lineage passes check B: a landing of 2 at base 6 links onto it,
/// as does a missed generation 2 asked of an inventory at base 6.
/// Cut: `has_floor` reads seq 1 (not the floor), red (`Ok(false)`, the landing refused
/// `from_has_no_floor`). Unit.
#[config_log::retcd_test]
fn t61_trimmed_lineage_passes_check_b() {
    let p = PartitionId(1);
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 6);
        trimmed_to(&mut engine, Generation(1), 4);
        digests
    };
    let mut host = host_over(&path, &rt);
    assert_eq!(host.engine.floor_seq(p, Generation(1)), Seq(4));
    assert_eq!(
        host.has_floor(p, Generation(1)),
        Ok(true),
        "the record at F reads"
    );
    assert_eq!(
        host.land_at(p, anchor_at(2, 6, digests[5])),
        Ok((Generation(1), Seq(6), digests[5])),
        "a landing links onto the trimmed lineage"
    );
}

/// Issue #61 S1-R3: check B after a trim, both paths and the hole. Generation 1 trimmed to 4
/// (seqs 4-6 kept): the landing (`land_at`) and the missed link (`inherit_missed_link`, through
/// an inventory) both pass. A lineage whose floor is 4 and which holds only 5-6, the record at
/// F missing, is refused `from_has_no_floor` by the landing and
/// `missed_generation_from_has_no_floor` by the inventory, and nothing is written.
/// The design's damaged record is a written hole here: `damage_record` leaves a record that
/// still reads as a value, which is the read-error arm's case, not a hole at F.
/// Cut: `has_floor` reads seq 1, red (both passing cases refused). Unit.
#[config_log::retcd_test]
fn s1_r3_check_b_after_a_trim_links_and_a_hole_at_the_floor_refuses() {
    const METHOD: &str = "s1_r3_check_b_after_a_trim_links_and_a_hole_at_the_floor_refuses";
    let p = PartitionId(1);
    let rt = current_thread();
    // The trimmed lineage: a landing links, and so does a missed generation.
    for path_of in ["land", "inventory"] {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        let digests = {
            let mut engine = RocksEngine::open(&path).expect("open engine");
            let digests = write_chain(&mut engine, Generation(1), 6);
            trimmed_to(&mut engine, Generation(1), 4);
            digests
        };
        let mut host = host_over(&path, &rt);
        if path_of == "land" {
            assert_eq!(
                host.land_at(p, anchor_at(2, 6, digests[5])),
                Ok((Generation(1), Seq(6), digests[5]))
            );
        } else {
            let answer = host.inventory(p, CopyId(1), anchor_at(2, 6, digests[5]), None, None);
            assert!(
                matches!(&answer, RecoveryEvent::InventoryReported(inventory)
                    if inventory.head == (Seq(6), digests[5])),
                "the missed generation links onto the trimmed lineage: {answer:?}"
            );
            assert!(
                host.engine.lineages().contains(&(p, Generation(2))),
                "generation 2 linked"
            );
        }
    }

    // The record at F missing: seqs 5-6 only, floor 4.
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain_after(&mut engine, Generation(1), vec![Digest::ROOT; 4], 2);
        trimmed_to(&mut engine, Generation(1), 4);
        digests
    };
    let mut host = host_over(&path, &rt);
    let held = host.engine.lineages();
    assert_eq!(
        host.land_at(p, anchor_at(2, 6, digests[5])),
        Err("from_has_no_floor")
    );
    assert_eq!(
        host.inventory(p, CopyId(1), anchor_at(2, 6, digests[5]), None, None),
        RecoveryEvent::InventoryFailed { copy: CopyId(1) }
    );
    assert_eq!(host.engine.lineages(), held, "nothing linked");
    let failed: Vec<_> = logged!(METHOD, "inventory_failed")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        failed,
        [serde_json::Value::from(
            "missed_generation_from_has_no_floor"
        )]
    );
}

/// Issue #61 (design rev 4 m-e): `survivor` takes its root rung from a trim mark only when the
/// mark is the anchor's own generation's and its `root_base` is the anchor's base.
/// * Own mark: generation 1 holds seqs 1-3, generation 2 is linked onto it at 3 and holds 4-8,
///   trimmed to 5 at root `(3, d3)`. An anchor at generation 2, base 3, answers: its ladder is
///   the root rung `(3, d3)` and then the kept records 5-8.
/// * The same, trimmed at root `(0, ROOT)`: the mark is not rooted at the anchor's base:
///   `base_below_floor`, never `base_digest_differs`.
/// * Parent mark: generation 1 holds seqs 1-6, trimmed to 5 at root `(3, d3)`, and generation 2
///   is linked onto it at 6 (an adopted tail's link, above the anchor's base 3). The mark that
///   cuts generation 2 is its parent's: `base_below_floor`.
///
/// Cut: drop the generation check (`level != generation`), red (the parent mark answers).
/// Unit.
#[config_log::retcd_test]
fn t61_survivor_refuses_a_parent_mark() {
    let p = PartitionId(1);
    let rt = current_thread();
    for root_base in [3, 0] {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        let digests = {
            let mut engine = RocksEngine::open(&path).expect("open engine");
            let digests = write_chain(&mut engine, Generation(1), 3);
            engine
                .inherit(p, Generation(1), Generation(2), Seq(3))
                .expect("link generation 2 at 3");
            let digests = write_chain_after(&mut engine, Generation(2), digests, 5);
            let root_digest = if root_base == 3 {
                digests[2]
            } else {
                Digest::ROOT
            };
            trimmed_at(&mut engine, Generation(2), 5, (root_base, root_digest));
            digests
        };
        let mut host = host_over(&path, &rt);
        assert_eq!(
            host.engine.floor(p, Generation(2)).map(|(level, _)| level),
            Some(Generation(2))
        );
        let answer = host.survivor(p, CopyId(1), anchor_at(2, 3, digests[2]), None);
        if root_base == 3 {
            let own = answer.expect("its own mark at its base");
            assert_eq!(own.head, (Seq(8), digests[7]));
            assert_eq!(
                own.ladder,
                [
                    (Seq(3), digests[2]),
                    (Seq(5), digests[4]),
                    (Seq(6), digests[5]),
                    (Seq(7), digests[6]),
                    (Seq(8), digests[7]),
                ]
            );
        } else {
            assert_eq!(
                answer.map(|_| ()),
                Err("base_below_floor"),
                "rooted elsewhere"
            );
        }
    }

    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let digests = write_chain(&mut engine, Generation(1), 6);
        trimmed_at(&mut engine, Generation(1), 5, (3, digests[2]));
        engine
            .inherit(p, Generation(1), Generation(2), Seq(6))
            .expect("link generation 2 at 6");
        digests
    };
    let mut host = host_over(&path, &rt);
    assert_eq!(
        host.engine.floor(p, Generation(2)).map(|(level, _)| level),
        Some(Generation(1)),
        "generation 2 reads under its parent's mark"
    );
    assert_eq!(
        host.survivor(p, CopyId(1), anchor_at(2, 3, digests[2]), None)
            .map(|_| ()),
        Err("base_below_floor")
    );
}

/// Issue #61 (design rev 4 m-a, N4; rev 5e): a probe is answered in the generation of the
/// anchor it names, never in a newer one this copy landed since. Generation 1 holds seqs 1-6;
/// generation 2 takes it at seq 4 and holds its own 5 and 6, whose digests differ from
/// generation 1's, and is trimmed to 3.
/// * Generation 1 at seq 6: generation 1's digest. Generation 2 at seq 6: generation 2's.
/// * Generation 2 at seq 2 (below its floor) and generation 1 at seq 7 (above its head):
///   `ProbeUnavailable`.
/// * Generation 3, not held: no answer, `probe_unanswered{anchor_generation_not_held}`.
///
/// Cut: answer from the newest held generation, red (generation 1's seq 6 answered with 2's).
/// Unit.
#[config_log::retcd_test]
fn t61_probe_is_answered_in_the_pinned_generation() {
    const METHOD: &str = "t61_probe_is_answered_in_the_pinned_generation";
    let p = PartitionId(1);
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let (one, two) = {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        let one = write_chain(&mut engine, Generation(1), 6);
        engine
            .inherit(p, Generation(1), Generation(2), Seq(4))
            .expect("generation 2 at 4");
        let two = write_chain_after(&mut engine, Generation(2), one[..4].to_vec(), 2);
        trimmed_to(&mut engine, Generation(2), 3);
        (one, two)
    };
    assert_ne!(one[5], two[5], "the generations differ at seq 6");
    let host = host_over(&path, &rt);
    let probe = |generation: u64, seq: u64| {
        host.probe(
            p,
            CopyId(1),
            anchor_at(generation, 0, Digest::ROOT),
            Seq(seq),
        )
    };
    assert_eq!(
        probe(1, 6),
        Some(RecoveryEvent::ProbeAnswered {
            copy: CopyId(1),
            seq: Seq(6),
            digest: one[5]
        })
    );
    assert_eq!(
        probe(2, 6),
        Some(RecoveryEvent::ProbeAnswered {
            copy: CopyId(1),
            seq: Seq(6),
            digest: two[5]
        })
    );
    for (generation, seq) in [(2, 2), (1, 7)] {
        assert_eq!(
            probe(generation, seq),
            Some(RecoveryEvent::ProbeUnavailable {
                copy: CopyId(1),
                seq: Seq(seq)
            }),
            "generation {generation} holds no record at {seq}"
        );
    }
    assert_eq!(probe(3, 6), None, "generation 3 is not held");
    let unanswered: Vec<_> = logged!(METHOD, "probe_unanswered")
        .into_iter()
        .map(|line| (line["generation"].clone(), line["reason"].clone()))
        .collect();
    assert_eq!(
        unanswered,
        [(3.into(), "anchor_generation_not_held".into())]
    );
}

/// Issue #61 (design rev 4 m-c): after boot a copy is not trimmed until it has adopted the
/// partition's newest lineage, or the inventory pin's time has passed. Generation 1 holds seqs
/// 1-6, trimmed to 2 before this boot. The restarted host refuses a trim to 4
/// `inventory_pinned` (logged `history_trim_pinned`) and the floor stays 2. Once it lands a
/// newer generation the pin is released and the same trim goes on; on a second host the pin
/// expires instead (`inventory_pin_expired`) and the trim goes on too.
/// Cut: `Host::new` seeds no boot pin, red (the first trim goes on). Unit.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn t61_a_restarted_holder_does_not_trim_before_adopting() {
    const METHOD: &str = "t61_a_restarted_holder_does_not_trim_before_adopting";
    let p = PartitionId(1);
    let rt = current_thread();
    for release in ["landed", "expired"] {
        let dir = config_testkit::fs::temp_dir();
        let path = dir.path().join("node");
        {
            let mut engine = RocksEngine::open(&path).expect("open engine");
            write_chain(&mut engine, Generation(1), 6);
            trimmed_to(&mut engine, Generation(1), 2);
        }
        let mut host = host_over(&path, &rt);
        assert_eq!(
            host.trim_history_through(p, Seq(4)),
            Err("inventory_pinned".to_owned()),
            "{release}: pinned after boot"
        );
        assert_eq!(host.engine.floor_seq(p, Generation(1)), Seq(2));
        if release == "landed" {
            host.landed.insert((p, Generation(2)));
        } else {
            host.inventory_pin_millis = 0;
        }
        assert_eq!(
            host.trim_history_through(p, Seq(4)),
            Ok(Seq(4)),
            "{release}: released"
        );
        assert_eq!(host.engine.floor_seq(p, Generation(1)), Seq(4));
    }
    assert_eq!(logged!(METHOD, "history_trim_pinned").len(), 2);
    assert_eq!(logged!(METHOD, "inventory_pin_expired").len(), 1);
    assert_eq!(logged!(METHOD, "host_history_trimmed").len(), 2);
}

/// Issue #61 S2 (A8a, lead ruling 1, the transfer branch): a primary trimmed to its head holds a
/// ladder as dense as its engine, so R1's `SnapshotCatchupRequired` for node 3's copy opens a transfer
/// at its applied seq `S` (`snapshot_transfer_opened{at = S, floor = F}`) and refills nothing.
/// While it is open the transfer pins `S`: with node 3's links held, two more puts land and a
/// trim past `S` is refused `transfer_pinned` (`history_trim_transfer_pinned{pin = S}`). Then
/// the heal ends the transfer one of two ways, whichever lands first: the offer reaches copy 3,
/// which already applied `S` and refuses `not_ahead` (`why = refused`), or R1's cursor sends it
/// the two puts and its `CopyCaughtUp` releases the pin (`why = caught_up`). Either way the
/// transfer is released once and the same trim goes through. Accepting either ending is sound
/// only because S2-R5 (`t61_s2_r5_an_offer_from_a_deposed_or_stale_primary_is_refused_untouched`)
/// pins the `not_ahead` refusal on its own; drop that row and this one must pin it.
/// Replaces A5's `t61_a_trimmed_copy_behind_the_floor_is_refused_below_floor`, whose refusal
/// A8a deletes.
/// Cuts: the refill branch always taken, red (no transfer); the pin ignored, red (the trim
/// past `S` lands); the transfer never released, red (the trim stays pinned).
/// Integration (~3 s): bootstrap's discovery window, a hold and a heal.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn t61_a_trimmed_primary_opens_a_transfer_and_pins_its_seq() {
    const METHOD: &str = "t61_a_trimmed_primary_opens_a_transfer_and_pins_its_seq";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    let p = PartitionId(1);
    trio.bootstrap();
    trio.ready();
    for request in 1..=3 {
        assert!(matches!(
            trio.ask(super::seal_tests::put(request, b"k", 5_000)),
            Answer::Txn { .. }
        ));
    }
    let head = trio.head().expect("node 1 holds a head");
    trio.until("node 3 applied the head", |trio| {
        trio.nodes[2]
            .0
            .replication
            .receiver(NodeId(3), p)
            .is_some_and(|receiver| receiver.applied_head().seq.0 >= head)
            .then_some(())
    });
    assert_eq!(
        super::trio::trim(&mut trio, 0, head),
        Ok(Seq(head)),
        "node 1 trimmed to its head"
    );
    let three = trio.nodes[2]
        .0
        .replication
        .receiver(NodeId(3), p)
        .expect("node 3's receiver")
        .copy();
    trio.links.hold(NodeId(1), NodeId(3));
    trio.links.hold(NodeId(2), NodeId(3));
    let site = Site {
        partition: p,
        correlation: CorrelationId(u64::MAX),
    };
    let host = &mut trio.nodes[0].0;
    assert_eq!(
        host.kernel(
            ModuleName::Replication,
            &KernelEffect::SnapshotCatchupRequired {
                copy: three,
                barrier: Seq(head),
            },
            site,
            &[],
            &StepFences::default(),
        ),
        Ok(())
    );
    assert_eq!(host.fault, None);
    assert!(
        host.transfers.contains_key(&(p, three)),
        "a transfer opened"
    );
    for request in 4..=5 {
        assert!(matches!(
            trio.ask(super::seal_tests::put(request, b"k", 5_000)),
            Answer::Txn { .. }
        ));
    }
    let past = trio.head().expect("node 1 holds a head");
    assert!(past > head, "the puts moved the head past S");
    assert_eq!(
        super::trio::trim(&mut trio, 0, past),
        Err("transfer_pinned".to_owned()),
        "a trim past S is refused while the transfer is open"
    );
    trio.links.heal_all();
    trio.until("the refusal frees the pin", |trio| {
        (!trio.nodes[0].0.transfers.contains_key(&(p, three))).then_some(())
    });
    assert_eq!(super::trio::trim(&mut trio, 0, past), Ok(Seq(past)));
    let opened = logged!(METHOD, "snapshot_transfer_opened");
    assert_eq!(opened.len(), 1, "{opened:?}");
    assert_eq!(
        (&opened[0]["copy"], &opened[0]["at"], &opened[0]["floor"]),
        (
            &serde_json::json!(three.0),
            &serde_json::json!(head),
            &serde_json::json!(head)
        )
    );
    assert!(
        logged!(METHOD, "snapshot_catchup_served").is_empty(),
        "nothing refilled"
    );
    let pinned = logged!(METHOD, "history_trim_transfer_pinned");
    assert_eq!(pinned.len(), 1, "{pinned:?}");
    assert_eq!(pinned[0]["pin"], head);
    let released = logged!(METHOD, "snapshot_transfer_released");
    assert_eq!(released.len(), 1, "{released:?}");
    assert!(
        released[0]["why"] == "refused" || released[0]["why"] == "caught_up",
        "{released:?}"
    );
    for refused in logged!(METHOD, "snapshot_offer_refused") {
        assert_eq!(refused["reason"], "not_ahead", "{refused:?}");
    }
}

/// Issue #61: put on whichever trio node admits writes (as `Db` routes a put to the owner)
/// until one lands, retrying any refusal (a pause, a re-plan) for [`Trio::PATIENCE`]. Answers
/// the head that node holds after it.
pub(super) fn lands(trio: &mut Trio, request: u64) -> u64 {
    let patience = test_patience(Trio::PATIENCE);
    let deadline = Instant::now() + patience;
    loop {
        let owner = (0..trio.nodes.len()).find(|at| trio.status(*at).admits == Some(true));
        if let Some(at) = owner {
            let (reply, answer) = mpsc::channel();
            trio.nodes[at]
                .1
                .send(Msg::Client(Client {
                    partition: PartitionId(1),
                    call: super::seal_tests::put(request, b"k", 2_000),
                    reply,
                }))
                .expect("the node's mailbox");
            if matches!(trio.answer(&answer), Answer::Txn { .. }) {
                return trio.status(at).holds.map_or(0, |held| held.applied);
            }
        }
        assert!(
            Instant::now() < deadline,
            "no put landed within {patience:?}"
        );
        for _ in 0..20 {
            trio.step();
        }
    }
}

/// Issue #61 Trio rows: `note_tests::tune` on the regular rejoin path (no restart shadow).
pub(super) fn trim_tuned(host: &mut Host) {
    super::note_tests::tune(host);
    host.restart_shadow = false;
}

/// Issue #61: every trio node's `history_floor` and R1 ladder floor, in node order.
pub(super) fn floors(trio: &Trio) -> Vec<(u64, Option<Seq>)> {
    trio.nodes
        .iter()
        .map(|(host, _, _)| {
            (
                host.status(PartitionId(1))
                    .lengths
                    .map_or(0, |lengths| lengths.history_floor),
                super::trio::ladder_floor(host),
            )
        })
        .collect()
}

/// Issue #61 S1-R1 (design section 2, A5): write, trim every copy, restart every node. Puts
/// land, each copy is trimmed to `head - 4` (`Msg::TrimHistory`), and every copy then reports
/// `history_floor` = its R1 ladder's first rung = F. Nodes 3 and then 2 restart one at a
/// time (a restarted owner stays out until the folder is opened again, `Db::restart_node`, so
/// the owner's half is `s1_r1_a_trimmed_folder_reopens_and_serves`, a reopen of every node).
/// Each restarted copy reports its inventory from its engine, the ladder rebuilt from F
/// (the root rung, then F through its head: `inventory_ladder{rungs = head - F + 2}`), never
/// from `(0, ROOT)`, and a put lands after every restart. Every copy keeps `history_floor` F and
/// no R1 ladder holds a rung below F. After a re-plan the receivers of the new generation start
/// at its cutoff, above F (A3's `on_recovered`), so "first rung = F" is asserted right after
/// the trim and "no rung below F" after each restart. The design's 1,000 writes and F = 800
/// are 12 and 8 here: the claim is about where the ladder starts, not how long it is.
/// Right after the trim, every node's full ladder read (`dormant_rungs`, what a dormant restore
/// gives its receiver) starts at F with the digest kept there; that clause is function level,
/// since no row restores a dormant copy of a trimmed generation. The inventory builds its own
/// base (`inventory_ladder`), so the restarts do not reach `first_rung`.
/// Cut: `first_rung` ignores the floor (`(0, ROOT)`), red (the full read is refused
/// `below_floor`).
/// Integration (~2 s): bootstrap, two restarts and their re-plans.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn s1_r1_a_trimmed_trio_restarts_and_serves_with_its_ladders_at_the_floor() {
    const METHOD: &str = "s1_r1_a_trimmed_trio_restarts_and_serves_with_its_ladders_at_the_floor";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = super::note_tests::formed(dir.path(), trim_tuned);
    let mut request = 1;
    let mut head = 0;
    while head < 12 {
        head = lands(&mut trio, request);
        request += 1;
    }
    let through = head - 4;
    // Every copy must hold `through` before it is trimmed there.
    trio.until("every copy applied the head", |trio| {
        (0..3)
            .all(|at| {
                trio.status(at)
                    .holds
                    .is_some_and(|held| held.applied >= head)
            })
            .then_some(())
    });
    for at in 0..3 {
        assert_eq!(
            super::trio::trim(&mut trio, at, through),
            Ok(Seq(through)),
            "node {} trimmed",
            at + 1
        );
    }
    assert_eq!(floors(&trio), vec![(through, Some(Seq(through))); 3]);
    // Function level: no row here restores a dormant copy of the trimmed generation, the one
    // caller that reads `first_rung` there, so the full read it would restore is checked direct.
    for (at, (host, _, _)) in trio.nodes.iter().enumerate() {
        let generation = host.newest_held(PartitionId(1)).expect("a held generation");
        let first = host
            .dormant_rungs(PartitionId(1), generation)
            .map(|rungs| rungs.first().copied());
        let at_floor = host
            .digest_at(PartitionId(1), generation, Seq(through))
            .expect("the record at the floor is kept");
        assert_eq!(
            first,
            Ok(Some((Seq(through), at_floor))),
            "node {}'s full read starts at F",
            at + 1
        );
    }
    for n in [3, 2] {
        // (base, head, rungs) of each inventory this node reported: its ladder holds the root
        // and then F through its head when its base is below F, else its base through its head.
        let ladders = |node: u32| -> Vec<(u64, u64, u64)> {
            let rungs = logged!(METHOD, "inventory_ladder")
                .into_iter()
                .filter(|line| line["node"] == node)
                .map(|line| line["rungs"].as_u64().expect("rungs"));
            logged!(METHOD, "inventory_reported")
                .into_iter()
                .filter(|line| line["node"] == node)
                .map(|line| {
                    (
                        line["base"].as_u64().expect("base"),
                        line["head"].as_u64().expect("head"),
                    )
                })
                .zip(rungs)
                .map(|((base, head), rungs)| (base, head, rungs))
                .collect()
        };
        let before = ladders(n).len();
        super::note_tests::restart(&mut trio, dir.path(), n, trim_tuned);
        head = lands(&mut trio, request);
        request += 1;
        trio.until("every copy holds a ladder", |trio| {
            trio.nodes
                .iter()
                .all(|(host, _, _)| super::trio::ladder_floor(host).is_some())
                .then_some(())
        });
        let (base, held, rungs) = ladders(n).get(before).copied().expect("an inventory");
        let expected = if base < through {
            held - through + 2
        } else {
            held - base + 1
        };
        assert_eq!(
            rungs, expected,
            "node {n}'s inventory ladder after its restart, base {base}, head {held}"
        );
        if n == 3 {
            assert!(base < through, "the first restart reports below the floor");
        }
        for (node, (floor, ladder)) in floors(&trio).into_iter().enumerate() {
            assert_eq!(floor, through, "node {}'s history_floor", node + 1);
            assert!(
                ladder.is_some_and(|first| first >= Seq(through)),
                "node {}'s ladder starts at {ladder:?}, below {through}",
                node + 1
            );
        }
    }
    assert!(
        logged!(METHOD, "inventory_failed").is_empty(),
        "every inventory answered"
    );
    tracing::info!(head, through, floors = ?floors(&trio), "s1_r1_done");
}

/// Issue #61 (t3's minor, design rev 5): with live re-add off, a refused receiver re-anchors on
/// its highest rung at or below the cutoff; the host keeps its ladder floor at the engine's trim
/// floor, so for a cutoff below F there is none and it anchors at the root (A3's
/// `on_recovered`). Node 2, a secondary of a formed trio, applied 8 or more records and holds a
/// live receiver whose ladder is dense from the root. The host trims node 2 to `head - 2`: the
/// engine and the ladder both start there. Node 2 then hears generation `g + 1` committed
/// `ReadOnly` at cutoff 2 (from node 1's commit), a prefix it holds past: its landing is
/// refused `held_past_base`, and with re-add off its receiver stays, unmarked. The `Recovered`
/// finds no rung at or below 2: the receiver anchors at the new generation's root `(0, ROOT)`,
/// never on rung 2, whose History is gone.
/// Cut: the host's trim leaves R1's ladder whole (no `Replication::trim_history`, no
/// `align_ladders`), red (the receiver anchors at rung 2). The design's cut, `align_ladders`
/// skipped after `Recovered`, is not observable in one step: that alignment runs after the
/// step that re-anchors, so it guards the next `Recovered`, not this one.
/// Integration (~2 s): bootstrap, writes, then direct calls on node 2.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn t61_refused_receiver_without_live_readd_anchors_at_root_below_floor() {
    let p = PartitionId(1);
    let dir = config_testkit::fs::temp_dir();
    let mut trio = super::note_tests::formed(dir.path(), trim_tuned);
    let mut head = 0;
    let mut request = 1;
    while head < 8 {
        head = lands(&mut trio, request);
        request += 1;
    }
    trio.until("node 2 applied the head", |trio| {
        trio.status(1)
            .holds
            .is_some_and(|held| held.applied >= head)
            .then_some(())
    });
    let generation = trio.status(1).holds.expect("node 2 holds").generation;
    assert_eq!(
        super::trio::ladder_floor(&trio.nodes[1].0),
        Some(Seq::ZERO),
        "node 2's ladder is dense from the root"
    );
    let cutoff_digest = trio.nodes[1]
        .0
        .digest_at(p, generation, Seq(2))
        .expect("node 2 holds seq 2");
    let mut result = (*trio.nodes[0].0.committed[&p]).clone();
    result.new_generation = Generation(generation.0 + 1);
    result.selected.root.generation = result.new_generation;
    result.selected.cutoff_seq = Seq(2);
    result.selected.cutoff_digest = cutoff_digest;
    result.retained_status_map.predecessor_generation = generation;
    result.mode = PartitionMode::ReadOnly;
    let through = head - 2;
    assert_eq!(super::trio::trim(&mut trio, 1, through), Ok(Seq(through)));
    let host = &mut trio.nodes[1].0;
    host.live_readd.on = false;
    assert_eq!(
        super::trio::ladder_floor(host),
        Some(Seq(through)),
        "the ladder is trimmed too"
    );
    assert_eq!(
        host.land_link(p, &result),
        Ok(None),
        "the landing is refused"
    );
    host.push(
        Site {
            partition: p,
            correlation: CorrelationId::default(),
        },
        EventKind::Kernel(KernelEvent::Recovered(Box::new(result.clone()))),
        true,
        None,
    );
    host.drain();
    assert_eq!(host.fault, None);
    let receiver = host
        .replication
        .receiver(NodeId(2), p)
        .expect("the receiver stays");
    assert_eq!(receiver.lineage().generation, result.new_generation);
    assert_eq!(
        receiver.applied_head(),
        Head {
            seq: Seq::ZERO,
            digest: Digest::ROOT
        },
        "anchored at the root, not at rung 2"
    );
}

/// Issue #61 S2 (A8a): the chunk chain's known answer, so the sim's twin and any later change to
/// the encoding are caught: two pages chained from a barrier digest, pinned in hex. Each part's
/// change moves the answer: the label, `prev`, a namespace, a key, a value, a version and a
/// generation. Cut: drop any one part from [`super::snapshot::chunk_chain`], red (the pin).
/// Unit.
#[test]
fn t61_chunk_chain_known_answer() {
    use super::snapshot::chunk_chain;
    let hex = |digest: Digest| -> String { digest.0.iter().map(|b| format!("{b:02x}")).collect() };
    let barrier = Digest::of(rdb_core::contracts::digest::Domain::Record, &[b"barrier"]);
    let entry = |ns, key: &'static [u8], value: &'static [u8], version, generation| {
        (
            ns,
            Bytes::from_static(key),
            Bytes::from_static(value),
            version,
            Generation(generation),
        )
    };
    let first = [
        entry(Namespace::User, b"a", b"1", 7, 2),
        entry(Namespace::Dedup, b"b", b"", 8, 3),
    ];
    let second = [entry(Namespace::Progress, b"c", b"3", 9, 3)];
    let one = chunk_chain(barrier, &first);
    let two = chunk_chain(one, &second);
    assert_eq!(
        (hex(one), hex(two), hex(chunk_chain(barrier, &[]))),
        (
            "0e0607de51f50f0fcd0ce19c419996f45880bf894411fb348032f30ac9c5fc75".to_owned(),
            "164ff67cfb79c6342b15d1979f39cc50e3236363bd94e89cfc375a05466aa64e".to_owned(),
            "cebb77f7a3478e41f2072ae75121888594fd5b50dbfeecf20da013017ef16be8".to_owned()
        ),
        "the chunk chain's encoding moved"
    );
    let moved = [
        chunk_chain(Digest::ROOT, &first),
        chunk_chain(
            barrier,
            &[
                entry(Namespace::Progress, b"a", b"1", 7, 2),
                first[1].clone(),
            ],
        ),
        chunk_chain(
            barrier,
            &[entry(Namespace::User, b"x", b"1", 7, 2), first[1].clone()],
        ),
        chunk_chain(
            barrier,
            &[entry(Namespace::User, b"a", b"2", 7, 2), first[1].clone()],
        ),
        chunk_chain(
            barrier,
            &[entry(Namespace::User, b"a", b"1", 6, 2), first[1].clone()],
        ),
        chunk_chain(
            barrier,
            &[entry(Namespace::User, b"a", b"1", 7, 1), first[1].clone()],
        ),
        chunk_chain(barrier, &first[..1]),
    ];
    for (at, digest) in moved.iter().enumerate() {
        assert_ne!(*digest, one, "change {at} left the chain unmoved");
    }
}

/// Issue #61 lead ruling 1, density: `snapshot::ladder_dense` holds only for a ladder with a
/// rung at every seq from the engine floor through its highest. `fresh_primary`'s
/// `{(0, ROOT), cutoff}` on an untrimmed engine is not dense (a gap), nor is a ladder whose
/// first rung is above the floor or below it, nor an empty one. The real promote path is
/// `seal_tests::t61_a_copy_behind_a_promote_on_an_untrimmed_engine_is_caught_up_from_a_refilled_ladder`.
/// Cut: the count check dropped (floor equality only), red (`{0, 3}` reads dense).
/// Unit.
#[test]
fn t61_ladder_dense_needs_every_rung_from_the_engine_floor() {
    use super::snapshot::ladder_dense;
    use rdb_core::contracts::digest::Domain;
    use rdb_core::replication::progress::DigestLadder;
    let ladder = |seqs: &[u64]| {
        let mut ladder = DigestLadder::new();
        for &seq in seqs {
            ladder.insert(Seq(seq), Digest::of(Domain::Record, &[&seq.to_le_bytes()]));
        }
        ladder
    };
    let cases = [
        (&[0, 1, 2, 3][..], 0, true),
        (&[0, 3][..], 0, false),
        (&[2, 3][..], 0, false),
        (&[2, 3][..], 2, true),
        (&[0, 1, 2, 3][..], 2, false),
        (&[2, 4][..], 2, false),
        (&[][..], 0, false),
    ];
    let got: Vec<_> = cases
        .iter()
        .map(|&(seqs, floor, _)| ladder_dense(&ladder(seqs), Seq(floor)))
        .collect();
    let want: Vec<_> = cases.iter().map(|&(_, _, dense)| dense).collect();
    assert_eq!(got, want, "{cases:?}");
}

/// Issue #61 S2-R5 (design section 2 step 2, rev 5 M1): an offer the target did not ask for is
/// refused before anything is written. Node 3, a live copy at `applied`, is handed offers
/// built from its own adopted view with one thing wrong each: the owner epoch below its own
/// (a deposed primary of the same generation), the generation below its own (an older offer
/// replayed), the primary's boot changed, `S` at its own applied seq, and a barrier that names
/// another seq. Each logs `snapshot_offer_refused` with its reason, in that order
/// (`not_adopted`, `not_adopted`, `not_primary`, `not_ahead`, `bad_barrier`), no install
/// starts, and node 3's engine is record-for-record what it was (`rdb_storage::dump`).
/// Cut: the authority check skipped, red (the first offer is accepted and pulls).
/// Integration (~2 s): bootstrap's discovery window, then direct calls on node 3.
#[config_log::retcd_test]
fn t61_s2_r5_an_offer_from_a_deposed_or_stale_primary_is_refused_untouched() {
    const METHOD: &str = "t61_s2_r5_an_offer_from_a_deposed_or_stale_primary_is_refused_untouched";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    let p = PartitionId(1);
    trio.bootstrap();
    trio.ready();
    for request in 1..=3 {
        assert!(matches!(
            trio.ask(super::seal_tests::put(request, b"k", 5_000)),
            Answer::Txn { .. }
        ));
    }
    let head = trio.head().expect("node 1 holds a head");
    trio.until("node 3 applied the head", |trio| {
        trio.nodes[2]
            .0
            .replication
            .receiver(NodeId(3), p)
            .is_some_and(|receiver| receiver.applied_head().seq.0 >= head)
            .then_some(())
    });
    let barrier_at = |seq: u64| {
        trio.nodes[0]
            .0
            .engine
            .history_at(
                p,
                trio.nodes[0]
                    .0
                    .replication
                    .primary(NodeId(1), p)
                    .expect("primary")
                    .tracker()
                    .lineage()
                    .generation,
                Seq(seq),
            )
            .expect("read")
            .expect("held")
            .0
    };
    let barrier = barrier_at(head);
    let host = &mut trio.nodes[2].0;
    let receiver = host
        .replication
        .receiver(NodeId(3), p)
        .expect("node 3's receiver");
    let lineage = receiver.lineage();
    let applied = receiver.applied_head().seq;
    let primary = *receiver
        .config()
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .expect("a primary");
    assert!(
        lineage.generation.0 > 0 && lineage.owner_epoch.0 > 0,
        "{lineage:?}"
    );
    let good = SnapshotOffer {
        lineage,
        config_version: receiver.config().config_version,
        from: primary.node,
        boot: primary.boot,
        copy: receiver.copy(),
        at: Seq(applied.0 + 5),
        root: (Seq::ZERO, Digest::ROOT),
        barrier: barrier.clone(),
    };
    let offers = [
        SnapshotOffer {
            lineage: Lineage {
                owner_epoch: OwnerEpoch(lineage.owner_epoch.0 - 1),
                ..lineage
            },
            ..good.clone()
        },
        SnapshotOffer {
            lineage: Lineage {
                generation: Generation(lineage.generation.0 - 1),
                ..lineage
            },
            ..good.clone()
        },
        SnapshotOffer {
            boot: BootId(primary.boot.0 + 1),
            ..good.clone()
        },
        SnapshotOffer {
            at: applied,
            ..good.clone()
        },
        good,
    ];
    let before = format!(
        "{:?}",
        rdb_storage::dump(dir.path().join("3")).expect("dump")
    );
    for offer in offers {
        host.snapshot_offer(offer);
        assert!(host.installs.is_empty(), "an install started");
        assert_eq!(host.fault, None);
    }
    let after = format!(
        "{:?}",
        rdb_storage::dump(dir.path().join("3")).expect("dump")
    );
    assert!(before == after, "node 3's engine changed");
    let reasons: Vec<_> = logged!(METHOD, "snapshot_offer_refused")
        .iter()
        .map(|line| line["reason"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(
        reasons,
        [
            "not_adopted",
            "not_adopted",
            "not_primary",
            "not_ahead",
            "bad_barrier"
        ]
    );
    assert!(logged!(METHOD, "snapshot_offer_accepted").is_empty());
}

/// Issue #61 S2 (A8a, lead ruling on the re-seed, condition 1): a re-seeded receiver ACKs `S`
/// only when it is durable through `S`. Over node 3's adopted config, copy and lineage, a
/// receiver at head `S` with `durable = S - 1` gets no ACK from [`super::snapshot::reseed_ack`]
/// (the re-seed would log `snapshot_reseed_ack_withheld` and wait for its flush); with
/// `durable = S` it gets an ACK at `S`, durable `S`, with the barrier's digest.
/// Cut: the durable check dropped, red (the first receiver ACKs `S`).
/// Integration (~2 s): bootstrap's discovery window, then direct calls.
#[config_log::retcd_test]
fn t61_a_reseed_below_its_durable_does_not_ack_s() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let held = trio.nodes[2]
        .0
        .replication
        .receiver(NodeId(3), PartitionId(1))
        .expect("node 3's receiver");
    let at = Seq(7);
    let digest = Digest::of(rdb_core::contracts::digest::Domain::Record, &[b"barrier"]);
    let reseeded = |durable: u64| {
        AppendReceiver::new(ReceiverInit {
            config: held.config().clone(),
            own: held.copy(),
            lineage: held.lineage(),
            head: Head { seq: at, digest },
            durable: DurableSeq(durable),
        })
        .expect("a live receiver")
    };
    assert_eq!(super::snapshot::reseed_ack(&reseeded(at.0 - 1), at), None);
    let ack = super::snapshot::reseed_ack(&reseeded(at.0), at).expect("an ACK at S");
    assert_eq!(
        (
            ack.progress.buffered_applied.0,
            ack.progress.durable.0,
            ack.digest_at_buffered
        ),
        (at.0, at.0, digest)
    );
}

/// What a tail held `Unchecked` by check B must still be, whichever path held it (PR #156
/// review, #153 rule). Its status crosses the wire: a peer's look decodes it, so the link
/// stays up. A bare `forget-tail --through` over it, on the pinned owner in this process,
/// still needs `--give-up-live-copy`. And the state is `Unchecked`. Checked in that order, so
/// a `Refused` state fails on the wire first.
fn held_unchecked(host: &Host) {
    use crate::net::wire::{decode, encode, Frame, WireNodeStatus, WireReply};
    let p = PartitionId(1);
    let status = host.status(p);
    let held = status.tail_refused.expect("the status names the held tail");
    let bytes = encode(&Frame::Reply {
        correlation: 1,
        reply: WireReply::Status(Box::new(WireNodeStatus::from(&status))),
    });
    let Ok(Frame::Reply {
        reply: WireReply::Status(back),
        ..
    }) = decode(&bytes)
    else {
        panic!("not a status reply");
    };
    let back = NodeStatus::try_from(*back).unwrap_or_else(|error| {
        panic!("a peer cannot decode the status, so the link closes: {error}")
    });
    assert_eq!(
        back.tail_refused.map(|tail| tail.adoption),
        Some(held.adoption)
    );
    needs_live_copy_consent(held.adoption);
    assert_eq!(held.adoption, TailAdoption::Unchecked, "{held:?}");
}

/// A pinned owner, in this process, holding a tail in `adoption` above its cutoff as its own:
/// a bare `forget-tail --through` over it is refused, naming `--give-up-live-copy` and saying
/// the tail was not checked.
fn needs_live_copy_consent(adoption: TailAdoption) {
    use rdb_core::contracts::errors::ErrorKind;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let p = PartitionId(1);
    let (generation, cutoff) = {
        let committed = &trio.nodes[0].0.committed[&p];
        (committed.new_generation, committed.selected.cutoff_seq)
    };
    let owner = &mut trio.nodes[0].0;
    owner.tail_refused.insert(
        p,
        TailRefused {
            generation,
            head: Seq(cutoff.0 + 3),
            base: cutoff,
            reason: "tail_on_older_lineage",
            adoption,
        },
    );
    let through = TailRelease {
        through: Some(Seq(cutoff.0 + 5)),
        no_intact_copy: false,
    };
    let error = owner
        .forget_tail_asked(p, through, false)
        .expect_err("a bare forget-tail over the held tail needs --give-up-live-copy");
    assert_eq!(error.kind, ErrorKind::InvalidArgument, "{error:?}");
    assert!(error.no_mutation, "{error:?}");
    assert!(
        error.detail.contains("add --give-up-live-copy")
            && error.detail.contains("not been checked"),
        "{error:?}"
    );
}

/// PR #156 review, #153 rule, check B at a landing's scan, the missing floor: generation 2
/// holds a tail at seqs 2 and 3 with no seq 1 (as in
/// [`i121_adoption_refuses_a_tail_holed_below_its_base`]). Generation 3's landing at cutoff 2,
/// with L = 2, scans the tail and its adoption answers `tail_has_no_floor`, a reason outside
/// the wire vocabulary. The landing holds the tail `Unchecked`, as the inventory does: the
/// status decodes on a peer and a bare `forget-tail` needs `--give-up-live-copy`.
/// Cut: `TailAdoption::of` answers `Refused` for every reason past the not-ready ones (the
/// classification before this row); red, the status does not decode.
/// Unit.
#[config_log::retcd_test]
fn pr156_a_landing_holds_a_tail_with_no_floor_unchecked() {
    const METHOD: &str = "pr156_a_landing_holds_a_tail_with_no_floor_unchecked";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = write_chain_after(
        &mut RocksEngine::open(&path).expect("open engine"),
        Generation(2),
        vec![Digest::ROOT],
        2,
    );
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let mut landing = landing_at(2, digests[1]);
    landing.new_generation = Generation(3);
    landing.selected.root.generation = Generation(3);
    landing.ledger.last_writable = Some(Generation(2));
    host.landing_scan(p, &landing);
    let said: Vec<_> = logged!(METHOD, "land_tail_not_adoptable")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(said, [serde_json::Value::from("tail_has_no_floor")]);
    held_unchecked(&host);
}

/// PR #156 review, #153 rule, check B at a landing's scan, the unreadable floor: generation 1
/// holds seqs 1 to 3, its base (seq 2) and head (seq 3) read and seq 1 does not
/// ([`floor_unreadable`]). Generation 2's landing at cutoff 2, with L = 1, scans the tail; its
/// adoption logs `adopt_floor_read_failed` with the fault and answers
/// `tail_history_read_failed`, a reason outside the wire vocabulary. The landing holds the tail
/// `Unchecked`: the status decodes on a peer and a bare `forget-tail` needs
/// `--give-up-live-copy`.
/// Cut: as [`pr156_a_landing_holds_a_tail_with_no_floor_unchecked`]'s; red, the status does not
/// decode.
/// Unit.
#[config_log::retcd_test]
fn pr156_a_landing_holds_a_tail_whose_floor_read_fails_unchecked() {
    const METHOD: &str = "pr156_a_landing_holds_a_tail_whose_floor_read_fails_unchecked";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = floor_unreadable(&path);
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let p = PartitionId(1);
    let mut landing = landing_at(2, digests[1]);
    landing.ledger.last_writable = Some(Generation(1));
    host.landing_scan(p, &landing);
    let said: Vec<_> = logged!(METHOD, "land_tail_not_adoptable")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(said, [serde_json::Value::from("tail_history_read_failed")]);
    assert_eq!(floor_read_failures(METHOD), [(1.into(), 1.into(), true)]);
    held_unchecked(&host);
}

/// `adopt_floor_read_failed` lines: partition, the tail's generation, and whether the fault
/// names `Corrupt`. Each must be an error.
fn floor_read_failures(method: &str) -> Vec<(serde_json::Value, serde_json::Value, bool)> {
    logged!(method, "adopt_floor_read_failed")
        .into_iter()
        .map(|line| {
            assert_eq!(line["@l"], "Error", "{line}");
            (
                line["partition"].clone(),
                line["from"].clone(),
                line["fault"]
                    .as_str()
                    .is_some_and(|f| f.contains("Corrupt")),
            )
        })
        .collect()
}

/// PR #156 review, #121 check B at adoption, the read-error arm. Generation 1 holds seqs 1 to
/// 3; its base (seq 2) and head (seq 3) read and seq 1 does not ([`floor_unreadable`]). An ask
/// at anchor 2, base 2, with L = 1 passes every proof above the base, then check B's seq 1 read
/// faults: the inventory answers `TailHeld`, the tail held `Unchecked`, `tail_not_adoptable`
/// names `tail_history_read_failed`, `adopt_floor_read_failed` logs the fault at error, and
/// nothing is written: lineages, generation 2's link and the cached view are unchanged.
/// Cut: swallow the read error in `Host::adoption`'s check B (`Err(_) => {}`); red, the ask
/// links generation 2 and reports.
/// Unit.
#[config_log::retcd_test]
fn pr156_an_adoption_whose_floor_read_fails_holds_the_tail_unchecked() {
    const METHOD: &str = "pr156_an_adoption_whose_floor_read_fails_holds_the_tail_unchecked";
    let p = PartitionId(1);
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let digests = floor_unreadable(&path);
    let mut host = host_over(&path, &rt);
    host.refresh_view(p, Generation(1)).expect("a step view");
    let before = untouched(&host);
    assert!(before.view.is_some(), "a cached view");
    let answer = host.inventory(
        p,
        CopyId(1),
        anchor_at(2, 2, digests[1]),
        Some(Generation(1)),
        None,
    );
    assert!(
        matches!(answer, RecoveryEvent::TailHeld { .. }),
        "a tail whose floor cannot be read is held: {answer:?}"
    );
    assert_eq!(untouched(&host), before, "the refusal wrote nothing");
    assert!(adoptions(METHOD).is_empty(), "{:?}", adoptions(METHOD));
    let refused: Vec<_> = logged!(METHOD, "tail_not_adoptable")
        .into_iter()
        .map(|line| line["reason"].clone())
        .collect();
    assert_eq!(
        refused,
        [serde_json::Value::from("tail_history_read_failed")]
    );
    assert_eq!(floor_read_failures(METHOD), [(1.into(), 1.into(), true)]);
    assert_eq!(
        host.tail_refused.get(&p).map(|refused| refused.adoption),
        Some(TailAdoption::Unchecked)
    );
}
