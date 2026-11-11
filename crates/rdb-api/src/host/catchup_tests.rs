//! Host catch-up and landing rows: inventory, `land_link`, the missed-generation links and
//! the D7 catch-up wiring. Moved out of `host.rs` (M10 S2 event 0).

use super::trio::{
    current_thread, host_over, logged, write_chain, write_chain_after, write_chain_in, Trio,
};
use super::*;

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

/// H2 (A-R223, A-R254): R1's `SnapshotCatchupRequired` on the primary is served from its
/// engine, never by faulting the node. The first ask restores the primary's ladder at its
/// generation and logs `snapshot_catchup_served` with every rung, root through the head; a
/// repeat `(copy, barrier)` is dropped; a new ask at that generation logs
/// `snapshot_catchup_unservable`; and a node that hosts no primary logs
/// `snapshot_catchup_unserved{reason=no_primary}`. Before H2 every one of these faulted the
/// node `host_unsupported: kernel_effect SnapshotCatchupRequired`.
/// Cuts: the arm removed, red (a fault); the dedup removed, red (the repeat logs
/// `unservable`); the ladder check removed, red (the new ask is served again).
/// Integration (~2 s): bootstrap's discovery window, then direct calls.
#[config_log::retcd_test]
fn h2_a_snapshot_catch_up_is_served_once_from_the_primarys_engine() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let head = trio.head().expect("node 1 holds a head");
    let site = Site {
        partition: PartitionId(1),
        correlation: CorrelationId(u64::MAX),
    };
    let ask = |copy: u8, barrier: u64| KernelEffect::SnapshotCatchupRequired {
        copy: CopyId(copy),
        barrier: Seq(barrier),
    };
    for (node, copy, barrier) in [(0, 2, head), (0, 2, head), (0, 3, head), (1, 3, head)] {
        let host = &mut trio.nodes[node].0;
        assert_eq!(
            host.kernel(ModuleName::Replication, &ask(copy, barrier), site, &[]),
            Ok(()),
            "node {} asked for copy {copy} at {barrier}",
            host.node.0
        );
        assert_eq!(host.fault, None, "node {} faulted", host.node.0);
    }
    let generation = trio.nodes[0]
        .0
        .replication
        .primary(NodeId(1), PartitionId(1))
        .expect("node 1 is primary")
        .tracker()
        .lineage()
        .generation;
    assert!(
        trio.nodes[0]
            .0
            .primary_ladders
            .contains(&(PartitionId(1), generation)),
        "node 1's primary holds its ladder at generation {}",
        generation.0
    );
    let lines: Vec<_> = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "h2_a_snapshot_catch_up_is_served_once_from_the_primarys_engine",
    )
    .into_iter()
    .filter(|line| {
        line["@m"]
            .as_str()
            .is_some_and(|m| m.starts_with("snapshot_catchup_"))
    })
    .map(|line| {
        (
            line["@m"].as_str().unwrap_or_default().to_owned(),
            line["node"].as_u64(),
            line["copy"].as_u64(),
            line["rungs"].as_u64(),
            line["reason"].as_str().map(str::to_owned),
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
                None
            ),
            (
                "snapshot_catchup_unservable".to_owned(),
                Some(1),
                Some(3),
                None,
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
        "served once, the repeat dropped, a new ask unservable, no primary unserved"
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
            host.kernel(ModuleName::Replication, &ahead, site, &[]),
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

/// Issue #105 unit 2 (lead ruling on the forget-tail narrowing, architect section 14 item 2):
/// `--give-up-live-copy` is needed only for a live tail that could come back. Planted on the
/// owner's status, one state at a time: `Adoptable`, `NotReady` and `Unchecked` each refuse a
/// bare `--through` by name. A `Refused` own tail beside a member's `NotReady` hold is refused
/// too, naming that member. A `Refused` tail alone (a failed proof, here `base_digest_differs`)
/// is accepted with no flag, and the owner re-plans with the release.
/// Cuts: every live tail needs the flag (unit 1's rule), red (the `Refused` ask is refused);
/// `TailAdoption::could_return` true only for `Adoptable`, red (the `NotReady` ask is accepted).
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
    let needs_flag = |answer: Result<String, ApiError>, node: u64, state: &str| {
        let error = answer.expect_err(state);
        assert_eq!(error.kind, ErrorKind::InvalidArgument, "{state}: {error:?}");
        assert!(error.no_mutation, "{state}: {error:?}");
        assert!(
            error.detail.contains("add --give-up-live-copy")
                && error.detail.contains(&format!("node {node} holds seqs")),
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
        [(32, 33, "link".to_owned()), (33, 34, "link".to_owned())]
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
    assert!(copying.could_return(), "forget-tail needs the flag");

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
/// child flattened (`copied` at base = head 3), and the copy reports head 3.
/// Cut c6 (refuse at the ceiling, `NotReady`): red, `TailHeld`. Red under c1 too, as R7-3.
#[config_log::retcd_test]
fn u2_r7_9_a_tail_at_the_depth_ceiling_is_adopted_by_a_flatten() {
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
}

/// R7-10 (design section 16, G4): the flatten of R7-9 faults on its copy batch, so generation 33
/// is left staging a copy of the whole tail, and the node restarts.
/// (a) The same ask again: adoption resumes the copy into 33 (`tail_adopted from=32 to=33`), and
/// 33 is `copied` at base = head 3.
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
            (32, 33, "link".to_owned())
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
