//! Host catch-up and landing rows: inventory, `land_link`, the missed-generation links and
//! the D7 catch-up wiring. Moved out of `host.rs` (M10 S2 event 0).

use super::trio::{current_thread, host_over, logged, write_chain, Trio};
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
    let report =
        |trio: &mut Trio, anchor| trio.nodes[1].0.inventory(PartitionId(1), CopyId(1), anchor);

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
    match host.inventory(PartitionId(1), CopyId(1), anchor_at(2, 2, digests[1])) {
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
        host.land_link(PartitionId(1), &result),
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
        assert_eq!(host.land_link(PartitionId(1), result), None);
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
        host.land_link(PartitionId(1), &result),
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

/// review-r185 ADV2 and ADV3: the refusals no row reached, each by name and at its level.
/// A copy holding no older generation has nothing to link, which is expected on a first
/// landing, so it logs at info. One already holding the new generation, or one the pin
/// does not name, logs at warn. None links, installs a receiver or faults the host. Cut
/// (every reason at warn): red. Unit.
#[config_log::retcd_test]
fn a_landing_with_nothing_to_link_is_refused_by_name_at_its_level() {
    const METHOD: &str = "a_landing_with_nothing_to_link_is_refused_by_name_at_its_level";
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
        assert_eq!(host.land_link(PartitionId(1), result), None);
        assert_eq!(host.engine.lineages(), held, "nothing linked");
        assert!(host
            .replication
            .receiver(NodeId(2), PartitionId(1))
            .is_none());
        assert_eq!(host.fault, None);
    };
    refuses(&mut fresh, &result);
    refuses(&mut holding, &result);
    refuses(&mut fresh, &outside);
    let refused: Vec<_> = logged!(METHOD, "land_link_refused")
        .into_iter()
        .map(|line| (line["reason"].clone(), line["@l"].clone()))
        .collect();
    assert_eq!(
        refused,
        [
            ("anchor_generation_absent", "Information"),
            ("held_at_or_above_generation", "Warning"),
            ("not_a_member", "Warning"),
        ]
        .map(|(reason, level)| (
            serde_json::Value::from(reason),
            serde_json::Value::from(level)
        ))
    );
}

/// A-R177 rows 3-5 (critic-f5 C2): the guards refuse before anything is written, each by
/// name: the digest at the base differs, the copy holds past the base (uncertain data an
/// ask must not truncate), or below it.
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
            host.survivor(PartitionId(1), CopyId(1), anchor).map(drop),
            Err(reason)
        );
        assert_eq!(
            host.inventory(PartitionId(1), CopyId(1), anchor),
            RecoveryEvent::InventoryFailed { copy: CopyId(1) }
        );
        assert_eq!(host.engine.lineages(), held, "{reason}: nothing written");
    }
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
        host.survivor(PartitionId(1), CopyId(1), anchor).map(drop),
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
        assert!(host.survivor(PartitionId(1), CopyId(1), anchor).is_ok());
    }
    let mut host = host_over(&path, &rt);
    match host.inventory(PartitionId(1), CopyId(1), anchor) {
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
    let effect = RecoveryEffect::CatchUp {
        from: CopyId(1),
        to: CopyId(2),
        through,
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
            host.kernel(ModuleName::Replication, &ask(copy, barrier), site),
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
            host.kernel(ModuleName::Replication, &ahead, site),
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
