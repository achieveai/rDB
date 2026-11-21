//! Issue #61 S3 (PR-B) host rows: the History retention duty (`host::retention`).

use super::catchup_tests::{regular_rejoin, restart_three};
use super::trio::{current_thread, host_over, logged, write_chain, Trio};
use super::*;
use rdb_core::contracts::errors::RetryRule;
use rdb_core::replication::retention::{RetentionDecision, RetentionHold};

const P: PartitionId = PartitionId(1);

/// Stage a trim of partition 1's `generation` to `seq`, its mark rooted at `(0, ROOT)`, so the
/// host knows the lineage's root without a commit.
fn trimmed_to(engine: &mut RocksEngine, generation: Generation, seq: u64) {
    engine
        .stage_trim_history(
            P,
            generation,
            TrimFloor {
                seq: Seq(seq),
                root_base: Seq::ZERO,
                root_digest: Digest::ROOT,
            },
        )
        .expect("the trim is staged");
}

/// Sync partition 1's `generation` through `through`, so the engine's `durable` reaches it.
fn synced(engine: &mut RocksEngine, generation: Generation, through: u64) {
    engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: P,
            generation,
            through: AppliedSeq(through),
        }])
        .expect("the sync");
}

/// Issue #61 S3 (design 3.1, 3.5): on a node holding no R1 side, the duty keeps `history_keep`
/// of History and trims the rest, and holds while the boot inventory pin does. Generation 1
/// holds seqs 1-6, trimmed to 1 and synced. With `history_keep` = 1 s and no byte cap:
///
/// Times are from the host clock's start.
///
/// 1. At 10 s the boot pin holds the round (`InventoryPinned`) and the floor stays 1.
/// 2. With the pin window at 0, the round at 10.5 s drops the pin (`inventory_pin_expired`);
///    no sample is 1 s old yet, so `keep` is 0 and the floor stays 1.
/// 3. At 11.5 s the first sample (10 s, head 6) is a window old: the floor moves to 6 in the
///    engine, and `host_history_trimmed` is logged once.
///
/// Issue #149: the ladder duty first warms node 2's inventory ladder, reported as
/// `IndexLengths::inventory_rungs`; after step 3 it holds the floor's rung only, and the block
/// cache's usage is reported.
///
/// Cuts: the inventory-pin gate off, red (step 1 trims); `keep` ignored, red (step 2 trims);
/// the kept ladders left untrimmed, red (every rung stays).
/// Unit.
#[config_log::retcd_test]
fn t61_s3_the_duty_keeps_the_window_and_waits_for_the_boot_pin() {
    const METHOD: &str = "t61_s3_the_duty_keeps_the_window_and_waits_for_the_boot_pin";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain(&mut engine, Generation(1), 6);
        trimmed_to(&mut engine, Generation(1), 1);
        synced(&mut engine, Generation(1), 6);
    }
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    host.history_retention = HistoryRetention {
        keep_millis: 1_000,
        budget_bytes: u64::MAX,
    };
    host.ladder_duty();
    let lengths = host.status(P).lengths.expect("node 2's lengths");
    assert!(
        lengths.inventory_rungs > 1,
        "the warmed ladder: {lengths:?}"
    );
    let base = host.clock.now();
    let at = |millis: u64| base.plus_millis(millis);
    assert_eq!(
        host.retention_round(P, at(10_000)),
        Ok(RetentionDecision::Held(RetentionHold::InventoryPinned)),
        "the boot pin holds the first round"
    );
    assert_eq!(host.engine.floor_seq(P, Generation(1)), Seq(1));
    host.inventory_pin_millis = 0;
    let round = host.retention_round(P, at(10_500));
    assert!(
        matches!(&round, Ok(RetentionDecision::Floor(f)) if f.floor == Seq(1)),
        "no sample is a window old yet: {round:?}"
    );
    assert_eq!(host.engine.floor_seq(P, Generation(1)), Seq(1));
    let round = host.retention_round(P, at(11_500));
    assert!(
        matches!(&round, Ok(RetentionDecision::Floor(f)) if f.floor == Seq(6)),
        "the window has passed the head: {round:?}"
    );
    assert_eq!(host.engine.floor_seq(P, Generation(1)), Seq(6));
    let lengths = host.status(P).lengths.expect("node 2's lengths");
    assert_eq!(
        (lengths.history_floor, lengths.inventory_rungs),
        (6, 1),
        "the trim bounds the kept ladder to the floor: {lengths:?}"
    );
    assert!(lengths.block_cache_bytes.is_some(), "{lengths:?}");
    assert_eq!(logged!(METHOD, "inventory_pin_expired").len(), 1);
    assert_eq!(logged!(METHOD, "host_history_trimmed").len(), 1);
}

/// Issue #61 S3 (lead note a): a newer generation starts the samples afresh, logged
/// `retention_samples_reset{from_gen, to_gen}`, so a head below the old generation's is not
/// refused `applied_backwards`. Generation 1 holds seqs 1-6; a round samples head 6. Then the
/// engine gains generation 2 holding seqs 1-3, and the next round samples head 3 without
/// error. A third round in generation 2 logs no second reset.
///
/// Cut: no reset, red (the second round fails `applied_backwards`). Unit.
#[config_log::retcd_test]
fn t61_s3_a_newer_generation_resets_the_retention_samples() {
    const METHOD: &str = "t61_s3_a_newer_generation_resets_the_retention_samples";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain(&mut engine, Generation(1), 6);
    }
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    host.inventory_pin_millis = 0;
    let base = host.clock.now();
    let round = host.retention_round(P, base.plus_millis(1_000));
    assert!(round.is_ok(), "generation 1: {round:?}");
    write_chain(&mut host.engine, Generation(2), 3);
    let round = host.retention_round(P, base.plus_millis(2_000));
    assert!(round.is_ok(), "generation 2: {round:?}");
    let round = host.retention_round(P, base.plus_millis(3_000));
    assert!(round.is_ok(), "generation 2 again: {round:?}");
    let resets = logged!(METHOD, "retention_samples_reset");
    assert_eq!(resets.len(), 1, "{resets:?}");
    assert_eq!(resets[0]["from_gen"], 1);
    assert_eq!(resets[0]["to_gen"], 2);
}

/// The open grace of `host`'s partition 1, its generation.
fn grace_of(host: &Host) -> Option<Generation> {
    host.retention
        .get(&P)
        .and_then(|state| state.grace)
        .map(|(generation, _)| generation)
}

/// [`regular_rejoin`], and the retention duty never runs by itself: the row calls each round,
/// so nothing ends a grace between its steps.
fn no_duty(host: &mut Host) {
    regular_rejoin(host);
    host.next_retention = Tick(u64::MAX);
}

/// Step the trio, and run one retention round on node 1 after each step, until `done` holds
/// for node 1's host. Each round must be answered.
fn rounds_until(trio: &mut Trio, what: &str, done: impl Fn(&Host) -> bool) {
    let deadline = Instant::now() + test_patience(Trio::PATIENCE);
    loop {
        trio.step();
        let now = trio.clock.now();
        let round = trio.nodes[0].0.retention_round(P, now);
        assert!(round.is_ok(), "{what}: {round:?}");
        if done(&trio.nodes[0].0) {
            return;
        }
        assert!(Instant::now() < deadline, "not within patience: {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Issue #61 S3 (design 3.1 m2, lead rulings R23 and R24): grace on the host. Bootstrap's
/// `Recovered` leaves node 1 a primary, so it begins a grace in generation 1
/// (`history_grace_begun`). Node 3 restarts and the owner re-plans: the new generation's
/// `Recovered` ends the open grace `superseded` (generation 1) and begins one in generation 2.
/// Node 2 and node 3 hold no primary and begin none. Then node 1's retention rounds end the
/// generation-2 grace `acked` once both secondaries have an admitted ack.
///
/// Cuts: no grace begun at `Recovered`, red (no `history_grace_begun`); a newer generation's
/// `Recovered` keeps the open grace, red (no `superseded`, grace stays in generation 1); the
/// ack check dropped, red (the grace never ends `acked`).
/// Integration (~4 s): bootstrap's discovery window, node 3's restart and the re-plan.
#[config_log::retcd_test]
fn t61_s3_a_newer_generation_supersedes_an_open_grace_and_acks_end_the_new_one() {
    const METHOD: &str =
        "t61_s3_a_newer_generation_supersedes_an_open_grace_and_acks_end_the_new_one";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), no_duty);
    trio.bootstrap();
    trio.ready();
    let first = trio.status(0).recovered.expect("bootstrap committed");
    assert_eq!(grace_of(&trio.nodes[0].0), Some(first), "node 1's grace");
    restart_three(&mut trio, dir.path(), no_duty);
    let second = trio.until("node 1 commits the re-plan", |trio| {
        let committed = trio.nodes[0].0.committed.get(&P)?;
        (committed.new_generation > first).then_some(committed.new_generation)
    });
    trio.until("node 1's grace is in the new generation", |trio| {
        (grace_of(&trio.nodes[0].0) == Some(second)).then_some(())
    });
    rounds_until(
        &mut trio,
        "node 1's retention round ends the grace",
        |host| grace_of(host).is_none(),
    );
    let begun: Vec<_> = logged!(METHOD, "history_grace_begun")
        .into_iter()
        .map(|line| (line["node"].clone(), line["generation"].clone()))
        .collect();
    assert_eq!(
        begun,
        [(1.into(), first.0.into()), (1.into(), second.0.into())],
        "one grace per generation, on node 1 only"
    );
    let ends: Vec<_> = logged!(METHOD, "history_grace_ended")
        .into_iter()
        .map(|line| (line["generation"].clone(), line["why"].clone()))
        .collect();
    assert_eq!(
        ends,
        [
            (first.0.into(), "superseded".into()),
            (second.0.into(), "acked".into())
        ]
    );
}

/// Node 1's round on partition 1 at the trio's clock, which must be answered with a floor.
fn floor_round(trio: &mut Trio, what: &str) -> rdb_core::replication::retention::RetentionFloor {
    let now = trio.clock.now();
    match trio.nodes[0].0.retention_round(P, now) {
        Ok(RetentionDecision::Floor(floor)) => floor,
        other => panic!("{what}: {other:?}"),
    }
}

/// Issue #61 S3-R2 and S3-R4 (design 3.1, lead ruling R23): the byte cap passes a lagging copy
/// and an open transfer's pin. Node 3 applies the trio's first three puts, node 1 trims to that
/// head `H` and node 3's links are held. A `SnapshotCatchupRequired` opens a transfer at `H`
/// and three more puts land, to `P`, which node 1 knows node 2 durable through. With
/// `history_budget_bytes` = 0 (only the head record fits) and a `history_keep` no sample
/// reaches:
///
/// 1. The first round trims past `H`: the pin is reported `history_budget_overran_pin{pin = H}`
///    and its transfer ended `snapshot_transfer_abandoned{why = budget_overran}`.
/// 2. The next round finds copy 3 below the floor: `history_copy_behind_floor` once, and once
///    only over a third round.
/// 3. Healed, node 3 comes back past the trim by a snapshot and applies `P`.
///
/// Cuts: overran pins left open, red (the trim is refused `transfer_pinned`); the behind warning
/// on every round, red (two warnings). Integration (~4 s): bootstrap, a hold and a heal.
#[config_log::retcd_test]
fn t61_s3_the_byte_cap_passes_a_lagging_copy_and_an_open_transfer() {
    const METHOD: &str = "t61_s3_the_byte_cap_passes_a_lagging_copy_and_an_open_transfer";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        no_duty(host);
        host.inventory_pin_millis = 0;
    });
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
            .receiver(NodeId(3), P)
            .is_some_and(|receiver| receiver.applied_head().seq.0 >= head)
            .then_some(())
    });
    rounds_until(&mut trio, "node 1's grace ends", |host| {
        grace_of(host).is_none()
    });
    assert_eq!(
        trio.nodes[0].0.trim_history_through(P, Seq(head)),
        Ok(Seq(head)),
        "node 1 trimmed to H"
    );
    let three = trio.nodes[2]
        .0
        .replication
        .receiver(NodeId(3), P)
        .expect("node 3's receiver")
        .copy();
    trio.links.hold(NodeId(1), NodeId(3));
    trio.links.hold(NodeId(2), NodeId(3));
    let site = Site {
        partition: P,
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
    assert!(
        host.transfers.contains_key(&(P, three)),
        "a transfer opened"
    );
    for request in 4..=6 {
        assert!(matches!(
            trio.ask(super::seal_tests::put(request, b"k", 5_000)),
            Answer::Txn { .. }
        ));
    }
    let past = trio.head().expect("node 1 holds a head");
    trio.until("node 1 is durable through P", |trio| {
        let host = &trio.nodes[0].0;
        let generation = host.newest_held(P)?;
        (host.engine.durable(P, generation).0 >= past).then_some(())
    });
    let two = trio.nodes[1]
        .0
        .replication
        .receiver(NodeId(2), P)
        .expect("node 2's receiver")
        .copy();
    trio.until("node 1 knows copy 2 durable through P", |trio| {
        (known_durable(trio, two) >= past).then_some(())
    });
    trio.nodes[0].0.history_retention = HistoryRetention {
        keep_millis: u64::MAX,
        budget_bytes: 0,
    };
    let first = floor_round(&mut trio, "the capped round");
    assert_eq!(
        (first.floor, first.overran_pins.clone()),
        (Seq(past), vec![Seq(head)]),
        "the cap keeps only the head and passes the pin"
    );
    assert!(
        !trio.nodes[0].0.transfers.contains_key(&(P, three)),
        "the overran transfer ended"
    );
    let generation = trio.nodes[0].0.newest_held(P).expect("node 1's lineage");
    assert_eq!(trio.nodes[0].0.engine.floor_seq(P, generation), Seq(past));
    let second = floor_round(&mut trio, "the round after");
    assert_eq!(second.behind, vec![three], "copy 3 is below the floor");
    let third = floor_round(&mut trio, "a third round");
    assert_eq!(third.behind, vec![three], "copy 3 is still below");
    let overran = logged!(METHOD, "history_budget_overran_pin");
    assert_eq!(overran.len(), 1, "{overran:?}");
    assert_eq!(
        (&overran[0]["copy"], &overran[0]["pin"]),
        (&serde_json::json!(three.0), &serde_json::json!(head))
    );
    let abandoned = logged!(METHOD, "snapshot_transfer_abandoned");
    assert_eq!(abandoned.len(), 1, "{abandoned:?}");
    assert_eq!(abandoned[0]["why"], "budget_overran");
    let behind = logged!(METHOD, "history_copy_behind_floor");
    assert_eq!(behind.len(), 1, "warned once: {behind:?}");
    assert_eq!(behind[0]["copy"], three.0);
    trio.links.heal_all();
    trio.until("node 3 applies P after the heal", |trio| {
        trio.nodes[2]
            .0
            .replication
            .receiver(NodeId(3), P)
            .is_some_and(|receiver| receiver.applied_head().seq.0 >= past)
            .then_some(())
    });
}

/// The durable seq node 1's tracker knows for `copy` on partition 1.
fn known_durable(trio: &Trio, copy: CopyId) -> u64 {
    trio.nodes[0]
        .0
        .replication
        .primary(NodeId(1), P)
        .and_then(|primary| primary.tracker().known(copy))
        .map_or(0, |known| known.durable.0)
}

/// Issue #61 S3-R3 (design 3.1, `need`): a lagging member within budget holds `F`. Node 3
/// applies the trio's first three puts, node 1 knows it durable through them (`H`), and node 1's
/// grace ends; then node 3's links are held
/// and three more puts land, to `P`, which node 1 knows node 2 durable through, so copy 3 is the
/// only copy behind. With no byte cap and `history_keep` = 0, so `keep` reaches
/// node 1's head after a round has sampled it, node 1's second round stops at copy 3's durable
/// as node 1's tracker knows it, below `P`: copy 3 is counted, not behind, and nothing is
/// trimmed past it. Healed, node 3 catches up from History, with no snapshot transfer.
///
/// Cut: `need` ignores member durable, red (the floor reaches `P`).
/// Integration (~3 s): bootstrap, a hold and a heal.
#[config_log::retcd_test]
fn t61_s3_r3_a_lagging_member_within_budget_holds_the_floor() {
    const METHOD: &str = "t61_s3_r3_a_lagging_member_within_budget_holds_the_floor";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        no_duty(host);
        host.inventory_pin_millis = 0;
    });
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
            .receiver(NodeId(3), P)
            .is_some_and(|receiver| receiver.applied_head().seq.0 >= head)
            .then_some(())
    });
    rounds_until(&mut trio, "node 1's grace ends", |host| {
        grace_of(host).is_none()
    });
    let three = trio.nodes[2]
        .0
        .replication
        .receiver(NodeId(3), P)
        .expect("node 3's receiver")
        .copy();
    trio.until("node 1 knows copy 3 durable through H", |trio| {
        (known_durable(trio, three) >= head).then_some(())
    });
    trio.links.hold(NodeId(1), NodeId(3));
    trio.links.hold(NodeId(2), NodeId(3));
    for request in 4..=6 {
        assert!(matches!(
            trio.ask(super::seal_tests::put(request, b"k", 5_000)),
            Answer::Txn { .. }
        ));
    }
    let past = trio.head().expect("node 1 holds a head");
    trio.until("node 1 is durable through P", |trio| {
        let host = &trio.nodes[0].0;
        let generation = host.newest_held(P)?;
        (host.engine.durable(P, generation).0 >= past).then_some(())
    });
    let two = trio.nodes[1]
        .0
        .replication
        .receiver(NodeId(2), P)
        .expect("node 2's receiver")
        .copy();
    trio.until("node 1 knows copy 2 durable through P", |trio| {
        (known_durable(trio, two) >= past).then_some(())
    });
    trio.nodes[0].0.history_retention = HistoryRetention {
        keep_millis: 0,
        budget_bytes: u64::MAX,
    };
    let _sampled = floor_round(&mut trio, "the round that samples P");
    trio.step();
    let held = floor_round(&mut trio, "the round with keep at P");
    let durable = known_durable(&trio, three);
    assert!(durable < past, "copy 3 lags: {durable} against {past}");
    assert_eq!(
        (held.floor, held.behind.clone()),
        (Seq(durable), Vec::new()),
        "copy 3 within budget holds the floor at its durable"
    );
    let generation = trio.nodes[0].0.newest_held(P).expect("node 1's lineage");
    assert_eq!(
        trio.nodes[0].0.engine.floor_seq(P, generation),
        Seq(durable)
    );
    trio.links.heal_all();
    trio.until("node 3 applies P after the heal", |trio| {
        trio.nodes[2]
            .0
            .replication
            .receiver(NodeId(3), P)
            .is_some_and(|receiver| receiver.applied_head().seq.0 >= past)
            .then_some(())
    });
    let opened = logged!(METHOD, "snapshot_transfer_opened");
    assert!(opened.is_empty(), "History served copy 3: {opened:?}");
}

/// S3-R5's failover: node 3's links held, node 1's control store cut, node 2 promoted. Node 1
/// is left out and copy 3 is unreachable, so node 2's commit is `ReadOnly` and takes no writes.
/// Returns once node 2 has landed the new generation (which drops its inventory pin) with a
/// grace open in it, and the generation. No retention duty runs by itself.
fn failed_over_with_copy_3_held(dir: &std::path::Path) -> (Trio, Generation) {
    let (mut trio, faults) = super::seal_tests::formed(dir);
    for (host, _, _) in &mut trio.nodes {
        host.next_retention = Tick(u64::MAX);
    }
    trio.links.hold(NodeId(1), NodeId(3));
    trio.links.hold(NodeId(2), NodeId(3));
    assert!(matches!(
        trio.ask(super::seal_tests::put(1, b"a", 5_000)),
        Answer::Txn { .. }
    ));
    super::seal_tests::renewal_landed_then_no_more(&mut trio);
    faults.apply("cut-control");
    let promote = super::seal_tests::spawn_promote(&trio, 2);
    let committed = trio.until("node 2 committed the new generation", |trio| {
        trio.nodes[1]
            .0
            .committed
            .get(&P)
            .filter(|result| result.new_generation > Generation(1))
            .cloned()
    });
    trio.until("the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");
    let second = committed.new_generation;
    trio.until("node 2 landed the new generation", |trio| {
        trio.nodes[1].0.landed.contains(&(P, second)).then_some(())
    });
    assert_eq!(grace_of(&trio.nodes[1].0), Some(second), "node 2's grace");
    (trio, second)
}

/// Issue #61 S3-R5 (design 3.1 m2, lead ruling R23): after a failover with `history_keep` = 0,
/// the new primary does not trim past its secondaries before their first acks. Node 3's links
/// are held and node 1's control store is cut, then node 2 is promoted: its `Recovered` begins
/// a grace in the new generation. Node 1 is left out and copy 3 is unreachable, so the commit
/// is `ReadOnly` and takes no writes; its lineage holds History below the cutoff. Node 2 lands
/// that generation (which drops its inventory pin). With no byte cap, every round on node 2 is
/// held `Grace` and its floor stays where the commit left it, while copy 3 has no ack. Healed,
/// copy 3 acks and the grace ends `acked`.
///
/// Cut: no grace begun at `Recovered`, red (node 2's first round trims to its head).
/// Integration (~5 s): bootstrap, a promote and a heal.
#[config_log::retcd_test]
fn t61_s3_r5_after_a_failover_with_no_keep_the_new_primary_waits_for_first_acks() {
    const METHOD: &str =
        "t61_s3_r5_after_a_failover_with_no_keep_the_new_primary_waits_for_first_acks";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, second) = failed_over_with_copy_3_held(dir.path());
    let node2 = &mut trio.nodes[1].0;
    node2.history_retention = HistoryRetention {
        keep_millis: 0,
        budget_bytes: u64::MAX,
    };
    node2.inventory_pin_millis = 600_000;
    let before = node2.engine.floor_seq(P, second);
    let head = Seq(node2.engine.buffered_applied(P, second).0);
    assert!(
        before < head,
        "there is History to trim: {before:?} {head:?}"
    );
    for round in 0..3 {
        let now = trio.clock.now();
        assert_eq!(
            trio.nodes[1].0.retention_round(P, now),
            Ok(RetentionDecision::Held(RetentionHold::Grace)),
            "round {round} waits for copy 3's first ack"
        );
        assert_eq!(trio.nodes[1].0.engine.floor_seq(P, second), before);
        trio.step();
    }
    trio.links.heal_all();
    let deadline = Instant::now() + test_patience(Trio::PATIENCE);
    loop {
        trio.step();
        let now = trio.clock.now();
        let round = trio.nodes[1].0.retention_round(P, now);
        assert!(round.is_ok(), "{round:?}");
        if grace_of(&trio.nodes[1].0).is_none() {
            break;
        }
        assert!(Instant::now() < deadline, "node 2's grace never ended");
        std::thread::sleep(Duration::from_millis(1));
    }
    let ends: Vec<_> = logged!(METHOD, "history_grace_ended")
        .into_iter()
        .filter(|line| line["node"] == 2)
        .map(|line| (line["generation"].clone(), line["why"].clone()))
        .collect();
    assert_eq!(ends, [(second.0.into(), "acked".into())]);
}

/// Issue #61 S3 (design 3.1 m2, lead ruling R23): a copy that never acks does not pin History
/// for ever. After [`failed_over_with_copy_3_held`], copy 3 stays held. With `history_keep` =
/// 1 s, `inventory_pin_millis` = 4 s and no byte cap, node 2's grace waits for the longer, 4 s
/// from its start. Node 2's lineage is first trimmed by one seq and synced through its head, as
/// a gateway taking writes is (the floor never passes this node's own durable). On an
/// untrimmed lineage (`F_prev` = 0) copy 3's durable 0 is not below the floor, so `need` counts
/// it and only the byte cap passes it.
///
/// 1. At start + 1.5 s, past `history_keep` but inside the pin window, the round is held
///    `Grace` and the floor stays.
/// 2. At start + 4 s the grace ends `history_grace_ended{why = expired}`: copy 3 is behind the
///    floor and the round trims above it.
///
/// Cuts: the grace never expires, red (step 2 is held); it expires at `history_keep` alone,
/// red (step 1 ends it). Integration (~4 s): bootstrap and a promote.
#[config_log::retcd_test]
fn t61_s3_a_grace_no_copy_acks_ends_expired_at_the_longer_of_keep_and_the_pin_window() {
    const METHOD: &str =
        "t61_s3_a_grace_no_copy_acks_ends_expired_at_the_longer_of_keep_and_the_pin_window";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, second) = failed_over_with_copy_3_held(dir.path());
    let node2 = &mut trio.nodes[1].0;
    node2.history_retention = HistoryRetention {
        keep_millis: 1_000,
        budget_bytes: u64::MAX,
    };
    node2.inventory_pin_millis = 4_000;
    let (_, since) = node2
        .retention
        .get(&P)
        .and_then(|state| state.grace)
        .expect("node 2's grace");
    let untrimmed = node2.engine.floor_seq(P, second);
    let head = Seq(node2.engine.buffered_applied(P, second).0);
    assert!(untrimmed.0 + 1 < head.0, "{untrimmed:?} {head:?}");
    let before = Seq(untrimmed.0 + 1);
    assert_eq!(node2.trim_history_through(P, before), Ok(before));
    synced(&mut node2.engine, second, head.0);
    assert_eq!(
        node2.retention_round(P, since.plus_millis(1_500)),
        Ok(RetentionDecision::Held(RetentionHold::Grace)),
        "past history_keep, inside the pin window"
    );
    assert_eq!(node2.engine.floor_seq(P, second), before);
    let round = node2.retention_round(P, since.plus_millis(4_000));
    assert!(
        matches!(&round, Ok(RetentionDecision::Floor(f)) if f.floor > before && !f.behind.is_empty()),
        "the grace expired, the copy with no ack is behind and the floor moves: {round:?}"
    );
    assert_eq!(grace_of(node2), None);
    assert!(node2.engine.floor_seq(P, second) > before);
    let ends: Vec<_> = logged!(METHOD, "history_grace_ended")
        .into_iter()
        .filter(|line| line["node"] == 2)
        .map(|line| (line["generation"].clone(), line["why"].clone()))
        .collect();
    assert_eq!(ends, [(second.0.into(), "expired".into())]);
}

/// Issue #61 S3 (lead ruling R23): a grace on a node that holds no primary ends
/// `history_grace_ended{why = no_primary}` at the next round. The grace is planted: node 2 here
/// holds generation 1 of seqs 1-3 and no R1 side, which is what a demoted primary is left
/// with; demoting one for real takes a second re-plan.
///
/// Cut: the `no_primary` arm keeps the grace, red (it stays open). Unit.
#[config_log::retcd_test]
fn t61_s3_a_grace_on_a_node_with_no_primary_ends_no_primary() {
    const METHOD: &str = "t61_s3_a_grace_on_a_node_with_no_primary_ends_no_primary";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain(&mut engine, Generation(1), 3);
    }
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let now = host.clock.now();
    host.retention.entry(P).or_default().grace = Some((Generation(1), now));
    let round = host.retention_round(P, now.plus_millis(1));
    assert!(round.is_ok(), "{round:?}");
    assert_eq!(grace_of(&host), None, "the grace ended");
    let ends: Vec<_> = logged!(METHOD, "history_grace_ended")
        .into_iter()
        .map(|line| line["why"].clone())
        .collect();
    assert_eq!(ends, ["no_primary"]);
}

/// Ask node 1 to serve copy `three` a snapshot at node 1's head, as R1 does for a copy past the
/// trim. Answers whether a transfer is open for it afterwards.
fn offer_snapshot(trio: &mut Trio, three: CopyId) -> bool {
    let head = trio.head().expect("node 1 holds a head");
    let site = Site {
        partition: P,
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
    host.transfers.contains_key(&(P, three))
}

/// One put through node 1, then wait until node 1 is durable through the new head. A refusal
/// that wrote nothing and may be retried (a protection pause under a loaded host, say) is sent
/// again, within the trio's patience; any other answer fails the row.
fn put_durable(trio: &mut Trio, request: u64) {
    let deadline = Instant::now() + test_patience(Trio::PATIENCE);
    loop {
        match trio.ask(super::seal_tests::put(request, b"k", 5_000)) {
            Answer::Txn { .. } => break,
            Answer::Error { error, .. }
                if error.no_mutation
                    && !matches!(
                        error.retry,
                        RetryRule::Definitive | RetryRule::Quarantine | RetryRule::NotWired
                    )
                    && Instant::now() < deadline =>
            {
                trio.step();
                std::thread::sleep(Duration::from_millis(10));
            }
            answer => panic!("put {request}: {answer:?}"),
        }
    }
    let head = trio.head().expect("node 1 holds a head");
    trio.until("node 1 is durable through the put", |trio| {
        let host = &trio.nodes[0].0;
        let generation = host.newest_held(P)?;
        (host.engine.durable(P, generation).0 >= head).then_some(())
    });
}

/// Node 1's overruns in a row for `copy`.
fn overruns_of(trio: &Trio, copy: CopyId) -> u32 {
    trio.nodes[0]
        .0
        .retention
        .get(&P)
        .and_then(|state| state.overruns.get(&copy).copied())
        .unwrap_or(0)
}

/// Issue #61 S3-R4 (b) (owner ruling 2026-11-20, option 1; lead rulings R27 and 2026-11-21):
/// node 1 gives up offering copy 3 after three overruns **in a row**, and clears the give-up when
/// the budget rises. Node 3 is held after the trio's first three puts and node 1's grace ends.
/// With `history_budget_bytes` = 0, each overrun is: an offer opens a transfer at the head, one
/// put moves the head past it, and the round's cap passes the pin.
///
/// 1. Overrun, overrun (count 2), then a round within budget: the open transfer's pin is at the
///    floor, and the count resets to 0. One more overrun is count 1, and the next ask is
///    re-offered: a transfer opens.
/// 2. That transfer overruns (count 2). Then a held round (an inventory pin, planted) with the
///    head past the pin: no floor is decided, so the count stays 2. Its next unheld round is
///    the third overrun in a row: `history_budget_gave_up` once, and `NodeStatus::needs_operator`
///    carries `needs_history_budget`. The next ask is refused `snapshot_catchup_given_up`, and
///    no transfer opens.
/// 3. The budget rises: the next round logs `history_budget_give_up_cleared{why = budget_raised}`,
///    `needs_operator` clears, and the next ask opens a transfer. Healed, copy 3 is caught up
///    from it. (The caught-up reset of a count above 0 is
///    `t61_s3_r4b_a_caught_up_release_resets_the_count_and_the_give_up`: a further put here
///    holds copy 3 out long enough, on a loaded host, for the owner to drop it.)
///
/// Cuts: no reset on a within-budget round, red (step 1 gives up); a held round counts, red
/// (step 2 gives up at the held round); a held round resets, red (step 2 never gives up); no
/// give-up gate on the offer, red (step 2's ask opens a transfer); no `budget_raised` clear, red
/// (step 3 is still given up). Integration (~5 s): bootstrap, nine puts and a heal.
#[config_log::retcd_test]
fn t61_s3_r4b_three_overruns_in_a_row_give_up_the_copy_until_the_budget_rises() {
    const METHOD: &str =
        "t61_s3_r4b_three_overruns_in_a_row_give_up_the_copy_until_the_budget_rises";
    let dir = config_testkit::fs::temp_dir();
    // L1 at the spec's ages, not `fast_pause`'s: copy 3 is held for the whole row, and a pause
    // under a loaded host lets the owner re-plan it out of the configuration
    // (`snapshot_catchup_unserved{reason = copy_not_in_config}`), which no offer survives.
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.restart_shadow = false;
        host.next_retention = Tick(u64::MAX);
        host.inventory_pin_millis = 0;
    });
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
            .receiver(NodeId(3), P)
            .is_some_and(|receiver| receiver.applied_head().seq.0 >= head)
            .then_some(())
    });
    rounds_until(&mut trio, "node 1's grace ends", |host| {
        grace_of(host).is_none()
    });
    let three = trio.nodes[2]
        .0
        .replication
        .receiver(NodeId(3), P)
        .expect("node 3's receiver")
        .copy();
    trio.links.hold(NodeId(1), NodeId(3));
    trio.links.hold(NodeId(2), NodeId(3));
    trio.nodes[0].0.history_retention = HistoryRetention {
        keep_millis: u64::MAX,
        budget_bytes: 0,
    };
    let mut request = 4;
    let mut overrun = |trio: &mut Trio, expect: u32, what: &str| {
        if !trio.nodes[0].0.transfers.contains_key(&(P, three)) {
            assert!(offer_snapshot(trio, three), "{what}: a transfer opens");
        }
        put_durable(trio, request);
        request += 1;
        let round = floor_round(trio, what);
        assert!(round.overran_pins.len() == 1, "{what}: {round:?}");
        assert_eq!(overruns_of(trio, three), expect, "{what}");
    };

    // 1. Overrun, overrun, within budget, overrun: still re-offered.
    overrun(&mut trio, 1, "overrun 1");
    overrun(&mut trio, 2, "overrun 2");
    assert!(offer_snapshot(&mut trio, three), "a transfer at the head");
    let within = floor_round(&mut trio, "the round within budget");
    assert!(within.overran_pins.is_empty(), "{within:?}");
    assert_eq!(overruns_of(&trio, three), 0, "a round within budget resets");
    overrun(&mut trio, 1, "overrun after the reset");
    assert!(offer_snapshot(&mut trio, three), "re-offered after count 1");

    // 2. Overrun, a held round, overrun: the third in a row gives up.
    overrun(&mut trio, 2, "overrun 2 again");
    assert!(offer_snapshot(&mut trio, three), "re-offered after count 2");
    put_durable(&mut trio, 100);
    let generation = trio.nodes[0].0.newest_held(P).expect("node 1's lineage");
    let now = trio.clock.now();
    let host = &mut trio.nodes[0].0;
    host.inventory_pin_millis = 600_000;
    host.inventory_pins.insert(
        P,
        InventoryPin {
            generation,
            since: now,
        },
    );
    assert_eq!(
        host.retention_round(P, now),
        Ok(RetentionDecision::Held(RetentionHold::InventoryPinned))
    );
    assert_eq!(
        overruns_of(&trio, three),
        2,
        "a held round neither counts nor resets"
    );
    let host = &mut trio.nodes[0].0;
    host.inventory_pins.remove(&P);
    host.inventory_pin_millis = 0;
    let third = floor_round(&mut trio, "the third overrun in a row");
    assert_eq!(third.overran_pins.len(), 1, "{third:?}");
    assert_eq!(overruns_of(&trio, three), 3);
    let needs = rdb_core::replication::retention::needs_history_budget(three, NodeId(1), 3);
    assert_eq!(trio.status(0).needs_operator, Some(needs.clone()));
    let gave_up: Vec<_> = logged!(METHOD, "history_budget_gave_up")
        .into_iter()
        .map(|line| (line["copy"].clone(), line["needs_operator"].clone()))
        .collect();
    assert_eq!(gave_up, [(three.0.into(), needs.into())], "once");
    assert!(!offer_snapshot(&mut trio, three), "given up: no transfer");
    assert_eq!(logged!(METHOD, "snapshot_catchup_given_up").len(), 1);

    // 3. A raised budget clears the give-up; the caught-up release resets the count.
    trio.nodes[0].0.history_retention.budget_bytes = u64::MAX;
    let _ = floor_round(&mut trio, "the round at the raised budget");
    let cleared: Vec<_> = logged!(METHOD, "history_budget_give_up_cleared")
        .into_iter()
        .map(|line| line["why"].clone())
        .collect();
    assert_eq!(cleared, ["budget_raised"]);
    assert_eq!(trio.status(0).needs_operator, None);
    assert!(offer_snapshot(&mut trio, three), "offered again");
    trio.links.heal_all();
    let past = trio.head().expect("node 1 holds a head");
    trio.until("node 3 applies the head after the heal", |trio| {
        trio.nodes[2]
            .0
            .replication
            .receiver(NodeId(3), P)
            .is_some_and(|receiver| receiver.applied_head().seq.0 >= past)
            .then_some(())
    });
    trio.until("copy 3's transfer is released caught up", |trio| {
        (!trio.nodes[0].0.transfers.contains_key(&(P, three))).then_some(())
    });
    assert_eq!(overruns_of(&trio, three), 0);
}

/// Issue #61 S3-R4 (b) (lead ruling R27): a give-up recorded in an older generation is cleared
/// by the first round in a newer one, logged `history_budget_give_up_cleared{why =
/// new_generation}`, with its count. The give-up is planted at generation 0 on a host holding
/// generation 1; making one for real and then re-planning takes a failover (the simulator's
/// `t61_sim_s3_r4b_a_new_generation_clears_the_give_up` does). A give-up at the same generation
/// and budget stands.
///
/// Cut: no `new_generation` clear, red (the give-up stands). Unit.
#[config_log::retcd_test]
fn t61_s3_r4b_a_newer_generation_clears_a_give_up() {
    const METHOD: &str = "t61_s3_r4b_a_newer_generation_clears_a_give_up";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain(&mut engine, Generation(1), 3);
    }
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    host.history_retention.budget_bytes = 1_000;
    let given = |generation| super::retention::GivenUp {
        budget_bytes: 1_000,
        generation,
        attempts: 3,
    };
    let state = host.retention.entry(P).or_default();
    state.given_up.insert(CopyId(1), given(Generation(1)));
    state.given_up.insert(CopyId(2), given(Generation(0)));
    state.overruns.insert(CopyId(2), 3);
    let now = host.clock.now();
    assert!(host.retention_round(P, now).is_ok());
    let state = &host.retention[&P];
    assert_eq!(
        (
            state.given_up.keys().copied().collect::<Vec<_>>(),
            state.overruns.get(&CopyId(2))
        ),
        (vec![CopyId(1)], None),
        "only the older generation's give-up is cleared"
    );
    let cleared: Vec<_> = logged!(METHOD, "history_budget_give_up_cleared")
        .into_iter()
        .map(|line| (line["copy"].clone(), line["why"].clone()))
        .collect();
    assert_eq!(cleared, [(2.into(), "new_generation".into())]);
}

/// Issue #61 S3-R4 (b) (owner ruling 2026-11-20, option 1): a caught-up release of a copy,
/// `end_transfer(.., "caught_up", ..)` as `CopyCaughtUp` calls it, resets that copy's count
/// and ends its give-up, whether or not a transfer is open; another copy's stays. The count
/// and give-up are planted on a host holding generation 1.
///
/// Cut: no reset on a caught-up release, red (copy 2's count stays). Unit.
#[config_log::retcd_test]
fn t61_s3_r4b_a_caught_up_release_resets_the_count_and_the_give_up() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain(&mut engine, Generation(1), 3);
    }
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let given = super::retention::GivenUp {
        budget_bytes: 0,
        generation: Generation(1),
        attempts: 3,
    };
    let state = host.retention.entry(P).or_default();
    for copy in [CopyId(1), CopyId(2)] {
        state.overruns.insert(copy, 3);
        state.given_up.insert(copy, given);
    }
    assert!(host.needs_operator(P).is_some());
    host.end_transfer(P, CopyId(2), "caught_up", "snapshot_transfer_released");
    let state = &host.retention[&P];
    assert_eq!(
        (
            state.overruns.keys().copied().collect::<Vec<_>>(),
            state.given_up.keys().copied().collect::<Vec<_>>()
        ),
        (vec![CopyId(1)], vec![CopyId(1)]),
        "copy 2's count and give-up end; copy 1's stay"
    );
    host.end_transfer(
        P,
        CopyId(1),
        "budget_overran",
        "snapshot_transfer_abandoned",
    );
    assert_eq!(
        host.retention[&P].overruns.get(&CopyId(1)),
        Some(&3),
        "only caught_up resets"
    );
}
