//! M10 S2 part (b): a live owner re-plans a partition without a copy that has held its writes
//! up for `peer_drop_after_millis` (rulings A-R277, A-R281, A-R282).

use super::trio::{logged, Trio};
use super::*;
use rdb_core::contracts::errors::RetryRule;
use rdb_core::contracts::ids::{ClientId, RequestId, TenantId};
use rdb_value::delta::Op;

/// The rows' drop clock: past [`Trio::fast_pause`]'s pause age and resume hold, short enough
/// that a row waits on it in about a second.
const DROP_AFTER_MILLIS: u64 = 1_000;

/// More puts than land in [`Trio::fast_pause`]'s pause age with one copy silent (b1 and b4 see
/// tens), so a row whose copy was dropped early fails here instead of putting until overloaded.
const PUTS_BEFORE_PAUSE: u64 = 2_000;

fn tuned(host: &mut Host) {
    Trio::fast_pause(host);
    host.peer_drop_after_millis = DROP_AFTER_MILLIS;
}

fn identity(request: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(request),
    }
}

/// A put of key `k<request>`.
fn put(request: u64) -> ClientCall {
    put_within(request, 5_000)
}

/// [`put`] with a deadline of `millis`.
fn put_within(request: u64, millis: u64) -> ClientCall {
    ClientCall::Put {
        identity: identity(request),
        object: key(request),
        change: Change::Doc(Delta(vec![Op::Replace(Value::Bytes(b"v".to_vec()))])),
        expect: None,
        remaining_millis: millis,
    }
}

fn key(request: u64) -> Bytes {
    Bytes::from(format!("k{request}"))
}

/// Whether node 1 reads key `k<request>` back, asked under `asked_as`.
fn present(trio: &mut Trio, request: u64, asked_as: u64) -> bool {
    let get = ClientCall::Get {
        identity: identity(asked_as),
        object: key(request),
        look: Look::Doc,
    };
    match trio.ask(get) {
        Answer::Read { seen, .. } => !matches!(seen, Seen::Absent),
        other => panic!("read of k{request}: {other:?}"),
    }
}

/// Node 1's last commit: its generation, mode and member nodes.
fn commit_of(trio: &Trio) -> Option<(Generation, PartitionMode, Vec<u32>)> {
    trio.nodes[0]
        .0
        .committed
        .get(&PartitionId(1))
        .map(|result| {
            (
                result.new_generation,
                result.mode.clone(),
                result
                    .committed
                    .pinned_config
                    .members
                    .iter()
                    .map(|member| member.node.0)
                    .collect(),
            )
        })
}

/// Lose everything between `node` and `others`, as if its process died.
fn kill(trio: &Trio, node: u32, others: &[u32]) {
    for other in others {
        trio.links.drop_direction(NodeId(node), NodeId(*other));
        trio.links.drop_direction(NodeId(*other), NodeId(node));
    }
}

/// Hold node 3's links: it stays up and linked, and hears nothing until a heal.
fn hold_three(trio: &Trio) {
    trio.links.hold(NodeId(1), NodeId(3));
    trio.links.hold(NodeId(2), NodeId(3));
}

/// Put from `request` on until one is refused `PROTECTION_PAUSED`. Every put before the
/// refusal must land, and L1 must pause within [`PUTS_BEFORE_PAUSE`] puts. Returns the next
/// request id and how many landed.
fn put_until_paused(trio: &mut Trio, mut request: u64) -> (u64, u64) {
    let mut landed = 0;
    loop {
        assert!(
            landed < PUTS_BEFORE_PAUSE,
            "{landed} puts landed and L1 never paused"
        );
        let answer = trio.ask(put(request));
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

/// Step until `millis` of host time have passed.
fn pass(trio: &mut Trio, millis: u64) {
    let until = trio.clock.now().plus_millis(millis);
    trio.until("host time to pass", |trio| {
        (trio.clock.now() >= until).then_some(())
    });
}

/// Bootstrapped, admitting, and committed with all three.
fn started(dir: &std::path::Path, tune: impl Fn(&mut Host)) -> (Trio, Generation) {
    let mut trio = Trio::new(dir, tune);
    trio.bootstrap();
    trio.admits_for(4 * Trio::PAUSE_AGE_MILLIS);
    let (generation, _, members) = commit_of(&trio).expect("bootstrap committed");
    assert_eq!(members, vec![1, 2, 3]);
    (trio, generation)
}

/// Wait for node 1's commit without node 3: the next generation, `DegradedRf2`, nodes 1 and 2.
fn dropped_three(trio: &mut Trio, before: Generation) -> Generation {
    let (after, mode, members) = trio.until("node 1 committed without node 3", |trio| {
        commit_of(trio).filter(|(_, _, members)| members.len() == 2)
    });
    assert_eq!(
        (after, mode, members),
        (before.next(), PartitionMode::DegradedRf2, vec![1, 2])
    );
    after
}

/// b1, the drop half (A-R281 M1): node 3 dies, puts land until L1 pauses on it, and once L1
/// has refused writes on its account for `peer_drop_after_millis` the owner re-plans to the
/// next generation with nodes 1 and 2. That commit is `DegradedRf2`, and writes land again.
/// The plan F1 gets (`rejoin_plan`) follows the revocation persisted and A1's `DrainProof`.
/// Cuts: no drop duty in `run_due`, red (no commit without node 3 within the patience);
/// `revoke` plans at once instead of asking A1 (plan before `DrainProof`), red (the order).
/// Integration (~3 s): bootstrap's discovery window, the drop clock, the re-plan's window.
#[config_log::retcd_test]
fn m10_s2_b1_a_dead_copy_is_dropped_and_writes_go_on_with_two() {
    const METHOD: &str = "m10_s2_b1_a_dead_copy_is_dropped_and_writes_go_on_with_two";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);

    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    let after = dropped_three(&mut trio, before);
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after),
        other => panic!("a put after the drop: {other:?}"),
    }

    let planned = logged!(METHOD, "membership_drop_planned");
    assert_eq!(planned.len(), 1, "one drop: {planned:?}");
    assert_eq!(planned[0]["dropped"], 3);
    assert!(planned[0]["blamed_ms"].as_u64() >= Some(DROP_AFTER_MILLIS));

    // A-R289: the epoch's revocation is durable, and A1 has proved the drain, before the plan
    // goes to F1. `membership_drop_planned` is the decision and sends only the revocation.
    let order: Vec<String> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["node"] == 1)
        .filter_map(|line| match line["@m"].as_str() {
            Some("epoch_revocation_persisted") => Some("revoked".to_owned()),
            Some("authority_fact") if line["fact"] == "DrainProof" => Some("drained".to_owned()),
            Some("rejoin_plan") => Some("planned".to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        ["revoked", "drained", "planned"],
        "node 1's re-plan, in order"
    );
}

/// b2 (spec §8.3, no one-copy fallback): after node 3 is dropped, node 2 dies too. Writes
/// pause and stay paused for three drop clocks: the owner never plans one copy, and says why
/// once.
/// Cut: `kept < 2` as `kept < 1` in `drop_copy`, red (node 1 commits alone).
/// Integration (~6 s): b1's drop, then three drop clocks of watching.
#[config_log::retcd_test]
fn m10_s2_b2_with_two_copies_the_owner_never_drops_to_one() {
    const METHOD: &str = "m10_s2_b2_with_two_copies_the_owner_never_drops_to_one";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    let after = dropped_three(&mut trio, before);
    trio.ready();

    kill(&trio, 2, &[1]);
    // No put lands from here on. One sent before L1 pauses waits for node 2 past its
    // deadline and has an unknown outcome; the rest are refused.
    let mut request = request;
    let mut unknown = 0;
    loop {
        let answer = trio.ask(put_within(request, DROP_AFTER_MILLIS));
        request += 1;
        match answer {
            Answer::Error { error, .. } if error.name() == "PROTECTION_PAUSED" => break,
            Answer::Error { error, .. } if error.kind == ErrorKind::UnknownOutcome => unknown += 1,
            other => panic!("put {} with node 2 dead: {other:?}", request - 1),
        }
    }
    pass(&mut trio, 3 * DROP_AFTER_MILLIS);
    assert_eq!(
        commit_of(&trio).map(|(generation, _, members)| (generation, members)),
        Some((after, vec![1, 2])),
        "no re-plan to one copy"
    );
    assert_eq!(trio.status(0).admits, Some(false));
    match trio.ask(put(request)) {
        Answer::Error { error, .. } => assert_eq!(error.name(), "PROTECTION_PAUSED"),
        other => panic!("a put with one copy left: {other:?}"),
    }
    let refused = logged!(METHOD, "membership_drop_refused");
    assert_eq!(refused.len(), 1, "said once: {refused:?}");
    assert_eq!(refused[0]["holder"], "Some(2)");
    assert_eq!(refused[0]["why"], "a plan keeps at least two copies");
    assert_eq!(logged!(METHOD, "membership_drop_planned").len(), 1);
    tracing::info!(unknown, "b2_puts_unknown_after_node_2_died");
}

/// b3 (A-R281 M1 flapping): node 3's links are held three times, each time long enough for L1
/// to pause writes but healed after half the drop clock. An admit starts the clock again, so
/// no flap drops it. Held past the clock, it is dropped exactly once, and writes resume.
/// Cut: `drop_duty` keeps the blame while L1 admits, red (a flap drops node 3).
/// Integration (~7 s): three flaps, then b1's drop.
#[config_log::retcd_test]
fn m10_s2_b3_a_flapping_copy_is_dropped_once_and_only_when_down_past_the_clock() {
    const METHOD: &str =
        "m10_s2_b3_a_flapping_copy_is_dropped_once_and_only_when_down_past_the_clock";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    let mut request = 1;
    for _ in 0..3 {
        hold_three(&trio);
        (request, _) = put_until_paused(&mut trio, request);
        pass(&mut trio, DROP_AFTER_MILLIS * 6 / 10);
        trio.links.heal_all();
        trio.ready();
    }
    assert_eq!(
        logged!(METHOD, "membership_drop_planned").len(),
        0,
        "a flap dropped node 3"
    );
    assert_eq!(commit_of(&trio).map(|(g, ..)| g), Some(before));

    hold_three(&trio);
    (request, _) = put_until_paused(&mut trio, request);
    let after = dropped_three(&mut trio, before);
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after),
        other => panic!("a put after the drop: {other:?}"),
    }
    assert_eq!(logged!(METHOD, "membership_drop_planned").len(), 1);
}

/// b4 (A-R281 A2): puts sent every 50 ms of host time from node 3's death until writes land
/// again with two copies. Each one either landed and reads back, was refused having changed
/// nothing and reads absent, or had an unknown outcome (or a generation to reconcile) that a
/// status query settles.
/// Integration (~3 s): b1's drop with puts in flight throughout.
#[config_log::retcd_test]
fn m10_s2_b4_puts_in_flight_across_the_drop_land_or_are_refused_cleanly() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    let mut sent = Vec::new();
    let mut request = 1;
    let mut next = trio.clock.now();
    let patience = Instant::now() + test_patience(Trio::PATIENCE);
    loop {
        trio.step();
        let dropped = commit_of(&trio).is_some_and(|(_, _, members)| members.len() == 2);
        if dropped && trio.status(0).admits == Some(true) {
            break;
        }
        assert!(Instant::now() < patience, "no drop within the patience");
        if trio.clock.now() >= next {
            sent.push((request, trio.call(put(request))));
            request += 1;
            next = trio.clock.now().plus_millis(50);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let after = dropped_three(&mut trio, before);
    let (mut landed, mut refused, mut settled) = (0, 0, 0);
    let mut read_as = 10_000;
    for (request, answer) in sent {
        read_as += 1;
        let landed_here = match trio.answer(&answer) {
            Answer::Txn { result, .. } => {
                assert!(
                    result.generation == before || result.generation == after,
                    "put {request} at {result:?}"
                );
                landed += 1;
                true
            }
            Answer::Error { error, .. } if error.no_mutation => {
                refused += 1;
                false
            }
            Answer::Error { error, .. } => {
                // `Reconcile` only as `GENERATION_CHANGED`: a put queued behind the re-plan and
                // stranded (spec §8.1), never replayed, settled like the rest (A-R296).
                assert!(
                    matches!(
                        error.retry,
                        RetryRule::QueryStatus | RetryRule::RetryAfterRecovery
                    ) || (error.retry == RetryRule::Reconcile
                        && error.kind == ErrorKind::GenerationChanged),
                    "put {request}: {error:?}"
                );
                settled += 1;
                let status = ClientCall::Status {
                    identity: identity(request),
                    generation: None,
                };
                matches!(trio.ask(status), Answer::Status(TxnStatus::Resolved(_)))
            }
            other => panic!("put {request}: {other:?}"),
        };
        assert_eq!(
            present(&mut trio, request, read_as),
            landed_here,
            "put {request}"
        );
    }
    tracing::info!(landed, refused, settled, "b4_answers");
    assert!(
        landed > 0 && refused > 0,
        "landed {landed}, refused {refused}"
    );
}

/// b7 (A-R281): node 3 stays up and linked but hears nothing (its links are held). It is
/// dropped as a dead one is, and writes land again. Healed, node 3 hears the old traffic of a
/// generation it is no longer in, and no node faults.
/// Integration (~3 s): b1's drop, then the heal.
#[config_log::retcd_test]
fn m10_s2_b7_a_linked_copy_that_stalls_is_dropped() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    hold_three(&trio);
    let (request, _) = put_until_paused(&mut trio, 1);
    let after = dropped_three(&mut trio, before);
    trio.ready();
    trio.links.heal_all();
    for request in request..request + 3 {
        match trio.ask(put(request)) {
            Answer::Txn { result, .. } => assert_eq!(result.generation, after),
            other => panic!("a put after the drop and heal: {other:?}"),
        }
    }
}

/// b8 (A-R282 N1): node 3's links are held until L1 pauses and most of the drop clock has
/// run, then healed. Node 3 catches up while L1's resume hold, longer than what was left of
/// the clock, keeps writes refused. Its durable sequence advancing starts the clock again, so
/// it is not dropped, and writes resume with all three.
/// Cut: the clock from the last admit only (`Blame::next` keeps `since` whatever the copy and
/// its durable sequence), red (node 3 dropped during the resume hold). Comparing the copy alone
/// stays green here: once node 3 is heard, keepalives move `stalest_copy` between nodes 2 and
/// 3, and each move starts the clock again. The durable rule is pinned by
/// [`m10_s2_b8_the_clock_starts_again_while_the_blamed_copy_advances`].
/// Integration (~3 s): bootstrap's window, the hold, the resume hold.
#[config_log::retcd_test]
fn m10_s2_b8_a_copy_catching_up_past_the_clock_is_not_dropped() {
    const METHOD: &str = "m10_s2_b8_a_copy_catching_up_past_the_clock_is_not_dropped";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), |host| {
        tuned(host);
        host.budgets.resume_hold_millis = DROP_AFTER_MILLIS * 6 / 10;
    });
    hold_three(&trio);
    let (request, _) = put_until_paused(&mut trio, 1);
    pass(&mut trio, DROP_AFTER_MILLIS * 8 / 10);
    trio.links.heal_all();
    trio.ready();
    assert_eq!(
        logged!(METHOD, "membership_drop_planned").len(),
        0,
        "a copy catching up was dropped"
    );
    assert_eq!(
        commit_of(&trio),
        Some((before, PartitionMode::Active, vec![1, 2, 3]))
    );
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, before),
        other => panic!("a put after the catch-up: {other:?}"),
    }
}

/// b8's durable rule (A-R282 N1) where nothing else starts the clock again: the same copy,
/// blamed at check after check, keeps its clock while its durable sequence stands, and gets a
/// new one when it advances. A different copy gets a new clock too.
/// Cut: `Blame::next` compares the copy only, red.
#[test]
fn m10_s2_b8_the_clock_starts_again_while_the_blamed_copy_advances() {
    let first = Blame::next(None, CopyId(3), DurableSeq(5), Tick(100));
    assert_eq!(first.since, Tick(100));
    let stood = Blame::next(Some(&first), CopyId(3), DurableSeq(5), Tick(900));
    assert_eq!(stood.since, Tick(100), "a copy that stands keeps its clock");
    let advanced = Blame::next(Some(&stood), CopyId(3), DurableSeq(6), Tick(1_000));
    assert_eq!(
        advanced.since,
        Tick(1_000),
        "a copy that advances starts it again"
    );
    let other = Blame::next(Some(&advanced), CopyId(2), DurableSeq(6), Tick(1_100));
    assert_eq!((other.copy, other.since), (CopyId(2), Tick(1_100)));
}

/// b9 (critic-degraded D1): node 2 stays linked and acks everything it is sent, but its
/// durable sequence never moves (its flush duty is stopped). L1 pauses on it. The owner blames
/// the regular copy with the lowest durable sequence below the resume barrier, node 2, and
/// drops it after the clock; writes land again with nodes 1 and 3.
/// Cut: blame `stalest_copy` alone, red. L1's lag is time since a copy was last heard, not how
/// far behind it is: node 2 is heard as often as node 3, so `stalest_copy` names node 3 (ties
/// go to the higher copy) or moves between the two, and node 2 is not the one dropped.
/// Integration (~3 s): bootstrap's window, the drop clock, the re-plan's window.
#[config_log::retcd_test]
fn m10_s2_b9_a_copy_that_acks_but_stays_behind_is_dropped() {
    const METHOD: &str = "m10_s2_b9_a_copy_that_acks_but_stays_behind_is_dropped";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    trio.nodes[1].0.next_flush = Tick(u64::MAX);
    let (request, _) = put_until_paused(&mut trio, 1);
    let committed = trio.until("node 1 committed with two copies", |trio| {
        commit_of(trio).filter(|(_, _, members)| members.len() == 2)
    });
    let after = before.next();
    let planned = logged!(METHOD, "membership_drop_planned");
    assert_eq!(
        committed,
        (after, PartitionMode::DegradedRf2, vec![1, 3]),
        "{planned:?}"
    );
    assert_eq!(planned.len(), 1, "{planned:?}");
    assert_eq!(planned[0]["dropped"], 2);
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after),
        other => panic!("a put after the drop: {other:?}"),
    }
}

/// D2 (critic-degraded): L1 blocked for an operator (`DivergenceRequiresOperator`) is never
/// dropped past, however long it refuses writes and however far a copy is behind. Node 3 is
/// held and behind when node 1's L1 is blocked; three drop clocks later nothing is planned,
/// writes are still refused for the operator, and the commit is the one with all three.
/// Cut: `holding_up` without its `reason != ProtectionPaused` guard, red (node 3 dropped).
/// Integration (~4 s): bootstrap's window, three drop clocks.
#[config_log::retcd_test]
fn m10_s2_d2_a_pause_for_divergence_is_never_dropped_past() {
    const METHOD: &str = "m10_s2_d2_a_pause_for_divergence_is_never_dropped_past";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    hold_three(&trio);
    for request in 1..=3 {
        match trio.ask(put(request)) {
            Answer::Txn { .. } => {}
            other => panic!("put {request} with node 3 held: {other:?}"),
        }
    }
    let reason = rdb_core::contracts::authority::BlockReason::DivergenceRequiresOperator {
        diverged: vec![CopyId(3)],
    };
    trio.nodes[0].0.push(
        Site {
            partition: PartitionId(1),
            correlation: CorrelationId::default(),
        },
        EventKind::Kernel(KernelEvent::BlockPartition(reason)),
        false,
        None,
    );
    pass(&mut trio, 3 * DROP_AFTER_MILLIS);
    assert_eq!(
        logged!(METHOD, "membership_drop_planned").len(),
        0,
        "a partition blocked for an operator was re-planned"
    );
    match trio.ask(put(4)) {
        Answer::Error { error, .. } => assert_eq!(error.name(), "DIVERGENCE_REQUIRES_OPERATOR"),
        other => panic!("a put while blocked: {other:?}"),
    }
    assert_eq!(
        commit_of(&trio),
        Some((before, PartitionMode::Active, vec![1, 2, 3]))
    );
}

/// b10 (A-R306; tester-s2's drop half, step 2): node 3 misses puts, then restarts, so its
/// link reports a newer boot and the owner re-plans to link it again. Node 3 answers F1's
/// inventory, so F1 needs it, and is then held: its links stay up but nothing crosses them, so
/// its catch-up and barrier proof never arrive. This does not lean on W6's `NotASource`
/// refusal: the hold starts once F1 is synchronizing, whichever way the catch-up would go.
/// The rejoin is abandoned without node 3: the next generation commits `DegradedRf2` with
/// nodes 1 and 2 within the later of `peer_drop_after_millis` and F1's
/// barrier wait (7 windows), plus a margin for the re-plan's own window and CAS, and writes
/// land again. Every `rejoin_abandoned` names node 3, the copy that never advanced.
/// Only F1's block ends the rejoin; no clock does (ruling A-R316).
/// Cut: `rejoin_blocked` returns at once, red (node 1's F1 stays blocked).
/// Integration (~5 s): bootstrap's window, puts until L1 pauses, F1's sync and barrier
/// waits, the abandon's re-plan.
#[config_log::retcd_test]
fn m10_s2_b10_a_rejoin_whose_barrier_never_completes_is_abandoned() {
    const METHOD: &str = "m10_s2_b10_a_rejoin_whose_barrier_never_completes_is_abandoned";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    trio.short_window_after_commit();

    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    trio.links.heal_all();
    // Its restart: the link reports a newer boot, and nodes 1 and 2 hear it.
    let reboot = BootId(trio.nodes[2].0.boot.0 + 1);
    let mailbox = trio.nodes[2].1.clone();
    trio.links.register(NodeId(3), mailbox, reboot);
    let rebooted = trio.clock.now();
    for node in 0..2 {
        trio.nodes[node]
            .1
            .send(Msg::PeerBooted {
                node: NodeId(3),
                boot: reboot,
            })
            .expect("node up");
    }

    // Node 3 answered the inventory and is behind, so F1 needs it. From here it hears nothing
    // and says nothing, but its links stay up: no catch-up, no barrier proof, no `CopyLost`.
    trio.until("node 1 synchronizes node 3", |trio| {
        let phase = trio.nodes[0]
            .0
            .recoveries
            .get(&PartitionId(1))
            .map(Recovery::phase);
        (phase == Some(RecoveryPhase::Synchronizing)).then_some(())
    });
    hold_three(&trio);

    let after = dropped_three(&mut trio, before);
    let waited = HostClock::millis_until(rebooted, trio.clock.now());
    let window = test_discovery_window_millis();
    let bound = DROP_AFTER_MILLIS.max(7 * window) + 4 * window + 1_000;
    assert!(
        waited <= bound,
        "writes held {waited} ms by a rejoin that never completes; bound {bound} ms"
    );
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after),
        other => panic!("a put after the abandon: {other:?}"),
    }

    let plans = logged!(METHOD, "rejoin_plan");
    assert_eq!(
        plans[0]["dropped"], "None",
        "the rejoin links node 3: {plans:?}"
    );
    assert_eq!(plans[0]["rejoiner"], "Some(3)", "{plans:?}");
    assert_eq!(
        plans.last().map(|plan| plan["dropped"].clone()),
        Some("Some(3)".into()),
        "the last plan leaves node 3 out: {plans:?}"
    );
    let abandoned = logged!(METHOD, "rejoin_abandoned");
    assert!(!abandoned.is_empty(), "no rejoin_abandoned");
    assert!(
        abandoned.iter().all(|line| line["dropped"] == 3),
        "only node 3 is blamed: {abandoned:?}"
    );
}

/// A-R316 (critic-degraded A-1): a block on a re-plan leaves a member out only when it names
/// that member's copy. The rejoiner goes when the barrier misses it, among others or alone;
/// another member goes when its copy is the only one missing; anything else names nobody, so
/// the plan is retried with the same members. A plan that already leaves node 3 out never
/// leaves out a second member: the commit's members less node 2 would put node 3 back.
/// Cuts: `named_by_block` falls back to the rejoiner for every reason, red (`CasContention`);
/// `block_leaves_out` without its drop check, red (node 2 for the drop plan).
#[test]
fn a_block_leaves_out_only_the_member_it_names() {
    let members: Vec<Member> = (1..=3u8)
        .map(|n| Member {
            copy: CopyId(n - 1),
            node: NodeId(u32::from(n)),
            boot: BOOT,
            role: if n == 1 {
                ReplicaRole::Primary
            } else {
                ReplicaRole::RegularSecondary
            },
        })
        .collect();
    let barrier = |missing: &[u8]| BlockReason::BarrierIncomplete {
        missing: missing.iter().map(|copy| CopyId(*copy)).collect(),
    };
    let replan = |drop: Option<u32>, rejoiner: Option<u32>| Replan {
        drop: drop.map(NodeId),
        rejoiner: rejoiner.map(NodeId),
        since: Tick(0),
    };
    let rejoin = replan(None, Some(3));
    let out = |reason: &BlockReason, plan: Replan| block_leaves_out(reason, plan, &members);
    assert_eq!(out(&barrier(&[2]), rejoin), Some(NodeId(3)));
    assert_eq!(out(&barrier(&[1, 2]), rejoin), Some(NodeId(3)));
    assert_eq!(out(&barrier(&[1]), rejoin), Some(NodeId(2)));
    assert_eq!(out(&barrier(&[0, 1]), rejoin), None);
    assert_eq!(out(&barrier(&[1]), replan(None, None)), Some(NodeId(2)));
    assert_eq!(out(&barrier(&[1]), replan(Some(3), None)), None);
    for reason in [
        BlockReason::CasContention,
        BlockReason::OvertakenByPeer,
        BlockReason::ControlUnavailable,
        BlockReason::ControlUnknown,
        BlockReason::NoEligibleRegular,
    ] {
        assert_eq!(out(&reason, rejoin), None, "{reason:?}");
    }
}

/// b11 (A-R316, critic-degraded A-2): A1's `DrainProof` for b1's drop is lost once, as if A1
/// never answered the revocation. The owner asks again once the revocation is
/// `peer_drop_after_millis` old, A1 answers the repeated request, and the drop commits; writes
/// land again.
/// Cut: `retry_blocked_rejoin` leaves a `Revoking` alone, red (no commit without node 3 within
/// the patience).
/// Integration (~4 s): b1's drop, one more drop clock for the second ask.
#[config_log::retcd_test]
fn m10_s2_b11_a_revocation_a1_never_answers_is_asked_again() {
    const METHOD: &str = "m10_s2_b11_a_revocation_a1_never_answers_is_asked_again";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    trio.nodes[0].0.drain_proofs_to_lose = 1;

    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    let after = dropped_three(&mut trio, before);
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after),
        other => panic!("a put after the drop: {other:?}"),
    }

    assert_eq!(logged!(METHOD, "drain_proof_lost_for_test").len(), 1);
    let retries = logged!(METHOD, "rejoin_revoke_retry");
    assert_eq!(retries.len(), 1, "asked again once: {retries:?}");
    assert_eq!(retries[0]["dropped"], "Some(3)");
    let waited = retries[0]["waited_ms"].as_u64().expect("waited_ms");
    assert!(
        (DROP_AFTER_MILLIS..DROP_AFTER_MILLIS + 2 * DROP_CHECK_EVERY_MILLIS).contains(&waited),
        "asked again after {waited} ms"
    );
    assert_eq!(logged!(METHOD, "membership_drop_planned").len(), 1);
}

/// Node 1's committed owner epoch.
fn epoch_of(trio: &Trio) -> OwnerEpoch {
    trio.nodes[0].0.committed[&PartitionId(1)]
        .selected
        .root
        .owner_epoch
}

/// b12 (A-R321, critic-degraded A-4): a re-asked revocation persists twice, and the second
/// persist can land after the plan it fed has committed, while the owner revokes the next
/// epoch. A1 answers it with a `DrainProof` like any other. The row stands that late persist
/// in by hand: after b1's drop, node 1 hears the first epoch's revocation persisted again just
/// before it asks A1 to revoke the current one. The stale proof plans nothing; the plan waits
/// for the current epoch's revocation and its own proof, and commits the next generation.
/// Cut: `drained` ignores the epoch the proof is for, red (the plan precedes the revocation).
/// Integration (~3 s): b1's drop, then one re-plan.
#[config_log::retcd_test]
fn m10_s2_b12_a_drain_proof_for_an_older_epoch_plans_nothing() {
    const METHOD: &str = "m10_s2_b12_a_drain_proof_for_an_older_epoch_plans_nothing";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    let stale = epoch_of(&trio);

    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    let after = dropped_three(&mut trio, before);
    trio.ready();
    let current = epoch_of(&trio);
    assert_ne!(stale, current, "the drop moved the epoch");

    let host = &mut trio.nodes[0].0;
    let site = Site {
        partition: PartitionId(1),
        correlation: REJOIN,
    };
    let late = AuthorityEvent::EpochRevocationPersisted {
        partition: PartitionId(1),
        epoch: stale,
    };
    host.push(
        site,
        EventKind::Kernel(KernelEvent::Authority(late)),
        true,
        None,
    );
    host.revoke(PartitionId(1), current, None, None);
    let replanned = trio.until("node 1 re-planned", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > after)
    });
    assert_eq!(
        replanned,
        (after.next(), PartitionMode::DegradedRf2, vec![1, 2])
    );
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after.next()),
        other => panic!("a put after the re-plan: {other:?}"),
    }

    let order: Vec<String> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["node"] == 1)
        .filter_map(|line| match line["@m"].as_str() {
            Some("epoch_revocation_persisted") => Some(format!("revoked {}", line["epoch"])),
            Some("authority_fact") if line["fact"] == "DrainProof" => Some("drained".to_owned()),
            Some("drain_proof_stale") => Some("stale".to_owned()),
            Some("rejoin_plan") => Some("planned".to_owned()),
            _ => None,
        })
        .collect();
    let (revoked_stale, revoked_current) = (
        format!("revoked {}", stale.0),
        format!("revoked {}", current.0),
    );
    assert_eq!(
        order,
        [
            revoked_stale.as_str(),
            "drained",
            "planned",
            "drained",
            "stale",
            revoked_current.as_str(),
            "drained",
            "planned"
        ],
        "node 1's two re-plans, in order"
    );
    let stale_lines = logged!(METHOD, "drain_proof_stale");
    assert_eq!(stale_lines.len(), 1, "{stale_lines:?}");
    assert_eq!(stale_lines[0]["proved"], stale.0);
    assert_eq!(stale_lines[0]["revoking"], current.0);
}

/// [`Trio::until`] for a row that waits through an F1 block: steps by hand, so a block does
/// not fail the row, and still fails on a fault or after [`Trio::PATIENCE`].
fn until_through_blocks<T>(trio: &mut Trio, what: &str, done: impl Fn(&Trio) -> Option<T>) -> T {
    let deadline = Instant::now() + test_patience(Trio::PATIENCE);
    loop {
        for (host, _, rx) in &mut trio.nodes {
            host.run_due();
            host.drain();
            while let Ok(msg) = rx.try_recv() {
                host.handle(msg);
            }
            assert_eq!(host.fault, None, "node {} faulted", host.node.0);
        }
        if let Some(found) = done(trio) {
            return found;
        }
        assert!(Instant::now() < deadline, "not within the patience: {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// b13 (A-R321, critic-degraded A-5): a block that names no member is retried with the same
/// members. b1's drop is planned, and the control store goes down before F1 can commit it, so
/// F1 ends `Blocked(ControlUnavailable)`, which names nobody. The store comes back; once the
/// plan is `peer_drop_after_millis` old the owner sends it again, and it commits without node 3.
/// Cut: `retry_blocked_rejoin` leaves a blocked `Planned` alone, red (F1 stays blocked).
/// Integration (~4 s): b1's drop, one more drop clock for the retry.
#[config_log::retcd_test]
fn m10_s2_b13_a_plan_blocked_by_a_control_outage_is_sent_again() {
    const METHOD: &str = "m10_s2_b13_a_plan_blocked_by_a_control_outage_is_sent_again";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);

    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    trio.until("node 1 planned the drop", |trio| {
        matches!(
            trio.nodes[0].0.rejoins.get(&PartitionId(1)),
            Some(Rejoin::Planned { .. })
        )
        .then_some(())
    });
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    let phase = until_through_blocks(&mut trio, "F1 blocked on node 1", |trio| {
        trio.status(0)
            .recovery
            .filter(|phase| phase.starts_with("Blocked("))
    });
    assert_eq!(phase, "Blocked(ControlUnavailable)");
    trio.mem.stop_failing();

    let after = until_through_blocks(&mut trio, "node 1 committed without node 3", |trio| {
        commit_of(trio).filter(|(_, _, members)| members.len() == 2)
    });
    assert_eq!(
        after,
        (before.next(), PartitionMode::DegradedRf2, vec![1, 2])
    );
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after.0),
        other => panic!("a put after the drop: {other:?}"),
    }

    let blocked = logged!(METHOD, "rejoin_blocked");
    assert!(!blocked.is_empty(), "no rejoin_blocked");
    assert!(
        blocked
            .iter()
            .all(|line| line["why"] == "the block names no member to leave out"),
        "{blocked:?}"
    );
    let retries = logged!(METHOD, "rejoin_retry");
    assert!(!retries.is_empty(), "never sent again");
    assert!(
        retries.iter().all(|line| line["dropped"] == "Some(3)"
            && line["waited_ms"].as_u64() >= Some(DROP_AFTER_MILLIS)),
        "{retries:?}"
    );
    assert!(logged!(METHOD, "rejoin_abandoned").is_empty());
}

/// b14 (A-R325, critic-degraded A-9): b12 without the next revocation. After b1's drop, node 1
/// hears the first epoch's revocation persisted again, late, and nothing is being revoked. A1
/// records it and fences nothing, since it serves a later epoch; its drain proof plans
/// nothing; the next put lands at the drop's generation.
/// Host-level: A1 is the kernel the sim runs too, so the fix holds for both. Its kernel twin is
/// rdb-core `a_late_revocation_of_an_older_epoch_leaves_the_served_epoch_admitting`.
/// Cut: A1's completion arm fences whatever epoch it serves, red (the put is refused).
/// Integration (~3 s): b1's drop.
#[config_log::retcd_test]
fn m10_s2_b14_a_late_revocation_of_an_older_epoch_leaves_the_served_one_writable() {
    const METHOD: &str =
        "m10_s2_b14_a_late_revocation_of_an_older_epoch_leaves_the_served_one_writable";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    let stale = epoch_of(&trio);

    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    let after = dropped_three(&mut trio, before);
    trio.ready();
    assert_ne!(stale, epoch_of(&trio), "the drop moved the epoch");

    let site = Site {
        partition: PartitionId(1),
        correlation: REJOIN,
    };
    let late = AuthorityEvent::EpochRevocationPersisted {
        partition: PartitionId(1),
        epoch: stale,
    };
    trio.nodes[0].0.push(
        site,
        EventKind::Kernel(KernelEvent::Authority(late)),
        true,
        None,
    );
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after),
        other => panic!("a put after the late revocation: {other:?}"),
    }
    assert_eq!(trio.status(0).admits, Some(true));

    let unasked = logged!(METHOD, "drain_proof_unasked");
    assert_eq!(unasked.len(), 1, "the late proof was heard: {unasked:?}");
    let fences: Vec<_> = logged!(METHOD, "authority_fence")
        .into_iter()
        .filter(|line| line["node"] == 1 && line["reason"] == "EpochRevoked")
        .collect();
    assert_eq!(
        fences.len(),
        1,
        "only the drop's own revocation fenced: {fences:?}"
    );
}

/// b6 (critic-degraded M3, A-R282 M3 wording): node 3 dies and the owner plans without it, but
/// the link 1-2 is held through that plan's discovery, so node 2 is never heard and the commit
/// is `ReadOnly`, nodes 1 and 2 pinned, node 2 outside its barrier. Once the link heals and
/// stays up for the resume hold, the owner re-plans the same two members, and the next
/// generation is `DegradedRf2` and takes writes again. Nothing else would end the read-only
/// commit: node 2 is pinned, at the boot it was pinned at, and nothing holds writes up.
/// Cuts: `repair_duty` returns at once, red (no `DegradedRf2` within the patience); no wait for
/// the resume hold, red (`linked_ms` below it); no `linked` filter, red (repaired
/// while node 2 is held).
/// Integration (~4 s): b1's drop, the read-only commit, the repair's re-plan.
#[config_log::retcd_test]
fn m10_s2_b6_a_read_only_commit_is_repaired_when_its_member_links_again() {
    const METHOD: &str = "m10_s2_b6_a_read_only_commit_is_repaired_when_its_member_links_again";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    // Half way through node 3's drop clock, so the hold covers the whole of the drop's plan.
    trio.until("node 3 blamed for half the drop clock", |trio| {
        let now = trio.clock.now();
        trio.nodes[0]
            .0
            .blamed
            .get(&PartitionId(1))
            .filter(|blame| HostClock::millis_until(blame.since, now) >= DROP_AFTER_MILLIS / 2)
            .map(|_| ())
    });
    trio.links.hold(NodeId(1), NodeId(2));
    let read_only = trio.until("node 1 committed without node 3", |trio| {
        commit_of(trio).filter(|(_, _, members)| members.len() == 2)
    });
    assert_eq!(
        read_only,
        (before.next(), PartitionMode::ReadOnly, vec![1, 2]),
        "node 2 unheard through the drop's discovery"
    );

    // Still held: several repair checks past the resume hold repair nothing, since node 2 is
    // not linked.
    let until = trio
        .clock
        .now()
        .plus_millis(3 * DROP_CHECK_EVERY_MILLIS + trio.nodes[0].0.budgets.resume_hold_millis);
    trio.until("repair checks to pass while held", |trio| {
        (trio.clock.now() >= until).then_some(())
    });
    assert_eq!(
        commit_of(&trio),
        Some(read_only.clone()),
        "repaired while node 2 was held"
    );
    let planned = logged!(METHOD, "membership_repair_planned");
    assert!(
        planned.is_empty(),
        "repaired while node 2 was held: {planned:?}"
    );

    trio.links.heal(NodeId(1), NodeId(2));
    let healed = trio.clock.now();
    let (repaired, mode, members) = trio.until("node 1 repaired the read-only commit", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > read_only.0)
    });
    assert_eq!(
        (repaired, mode, members),
        (read_only.0.next(), PartitionMode::DegradedRf2, vec![1, 2])
    );
    let waited = HostClock::millis_until(healed, trio.clock.now());
    let host = &trio.nodes[0].0;
    // The resume hold, up to two drop checks to see it out, and the re-plan's own window.
    let bound = host.budgets.resume_hold_millis
        + 2 * DROP_CHECK_EVERY_MILLIS
        + host.budgets.discovery_window_millis
        + 1_000;
    assert!(
        waited <= bound,
        "repaired {waited} ms after the heal; bound {bound} ms"
    );
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, repaired),
        other => panic!("a put after the repair: {other:?}"),
    }
    let planned = logged!(METHOD, "membership_repair_planned");
    assert_eq!(planned.len(), 1, "one repair: {planned:?}");
    assert_eq!(planned[0]["outside"], "[2]");
    let hold = trio.nodes[0].0.budgets.resume_hold_millis;
    assert!(
        planned[0]["linked_ms"].as_u64() >= Some(hold),
        "repaired before the link held up for {hold} ms: {planned:?}"
    );
    tracing::info!(waited, "b6_repair_after_heal_ms");
}

/// A-R319 defect 2 (W5's probe, k546): a `ReadOnly` commit takes no write whatever L1 says,
/// so its rebuild check is never quiet on L1 admitting. Before, the check read L1 alone, and a
/// read-only partition whose L1 had resumed was silent. Every other mode stays quiet while L1
/// admits (K1 condition 6). A unit row: no host reaches a read-only commit with L1 admitting
/// on demand; the promote row `m10_s2_a_promote_that_missed_node_3_takes_writes_once_node_3_is_back`
/// walks it end to end.
/// Cut: `rebuild_quiet` reads L1 alone, red (read-only and admitting reads as quiet).
#[test]
fn a_read_only_commit_is_never_quiet_on_l1_admitting() {
    assert!(!rebuild_quiet(&PartitionMode::ReadOnly, Some(true)));
    assert!(!rebuild_quiet(&PartitionMode::ReadOnly, Some(false)));
    assert!(!rebuild_quiet(&PartitionMode::DegradedRf2, Some(false)));
    assert!(!rebuild_quiet(&PartitionMode::DegradedRf2, None));
    assert!(rebuild_quiet(&PartitionMode::DegradedRf2, Some(true)));
}

/// b17 (A-R319): a repair whose own plan is still running is not repaired again. b6's
/// read-only commit, the link heals, and the repair's plan is with F1 when the control store
/// goes down, so F1 blocks and the read-only commit stays the last one, with node 2 linked.
/// Over several repair checks no second repair is planned: the running re-plan is retried
/// as it is ([`Host::retry_blocked_rejoin`]). The store comes back and the plan commits.
/// Cut: `read_only_outside` ignores a running re-plan, red (a second `membership_repair_planned`).
/// Integration (~4 s): b6's drop, the repair, one blocked plan.
#[config_log::retcd_test]
fn m10_s2_b17_a_repair_whose_plan_is_running_is_not_repaired_again() {
    const METHOD: &str = "m10_s2_b17_a_repair_whose_plan_is_running_is_not_repaired_again";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    trio.until("node 3 blamed for half the drop clock", |trio| {
        let now = trio.clock.now();
        trio.nodes[0]
            .0
            .blamed
            .get(&PartitionId(1))
            .filter(|blame| HostClock::millis_until(blame.since, now) >= DROP_AFTER_MILLIS / 2)
            .map(|_| ())
    });
    trio.links.hold(NodeId(1), NodeId(2));
    let read_only = trio.until("node 1 committed without node 3", |trio| {
        commit_of(trio).filter(|(_, _, members)| members.len() == 2)
    });
    assert_eq!(read_only.1, PartitionMode::ReadOnly);

    trio.links.heal(NodeId(1), NodeId(2));
    trio.until("node 1 planned the repair", |trio| {
        matches!(
            trio.nodes[0].0.rejoins.get(&PartitionId(1)),
            Some(Rejoin::Planned { .. })
        )
        .then_some(())
    });
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    until_through_blocks(&mut trio, "F1 blocked on node 1", |trio| {
        trio.status(0)
            .recovery
            .filter(|phase| phase.starts_with("Blocked("))
    });
    // Several repair checks with node 2 linked and the read-only commit still the last one,
    // short of the blocked plan's own retry.
    let until = trio
        .clock
        .now()
        .plus_millis(3 * DROP_CHECK_EVERY_MILLIS + trio.nodes[0].0.budgets.resume_hold_millis);
    until_through_blocks(&mut trio, "repair checks to pass", |trio| {
        (trio.clock.now() >= until).then_some(())
    });
    assert_eq!(commit_of(&trio), Some(read_only.clone()), "still read-only");
    let planned = logged!(METHOD, "membership_repair_planned");
    assert_eq!(
        planned.len(),
        1,
        "repaired again while its plan ran: {planned:?}"
    );

    trio.mem.stop_failing();
    let repaired =
        until_through_blocks(&mut trio, "node 1 repaired the read-only commit", |trio| {
            commit_of(trio).filter(|(generation, _, _)| *generation > read_only.0)
        });
    assert_eq!(
        (repaired.1, repaired.2),
        (PartitionMode::DegradedRf2, vec![1, 2])
    );
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, repaired.0),
        other => panic!("a put after the repair: {other:?}"),
    }
}

/// A stopped node: its host, which nothing steps, and its mailbox, where everything sent to it
/// waits, as the real link queues it for the next process (`net_link_up queued=1396`).
type Dead = (Host, Sender<Msg>, Receiver<Msg>);

/// D8's first half, after [`two_down`].
struct TwoDown {
    trio: Trio,
    /// The member the drop kept, stopped, and its node.
    kept: (Dead, u32),
    /// The dropped member, stopped for good. Held so its mail still has somewhere to wait.
    _gone: Dead,
    /// The `ReadOnly` generation the drop committed.
    dropped: Generation,
    /// The plan anchor node 1 asked the kept member about for that generation.
    asked: LineageAnchor,
    /// The next request id.
    request: u64,
}

/// D8's first half: puts land on all three, then nodes 2 and 3 stop ([`Dead`]). A put lands
/// on node 1 only, writes stop, and the owner drops one of them: the next generation keeps
/// node 1 and the other, and commits `ReadOnly`, since that one never answers.
fn two_down(dir: &std::path::Path) -> TwoDown {
    let (mut trio, before) = started(dir, tuned);
    let mut request = 1;
    for _ in 0..3 {
        match trio.ask(put(request)) {
            Answer::Txn { .. } => request += 1,
            other => panic!("put {request} with all three up: {other:?}"),
        }
    }
    let three = trio.nodes.remove(2);
    let two = trio.nodes.remove(1);
    loop {
        let answer = trio.ask(put_within(request, DROP_AFTER_MILLIS));
        request += 1;
        match answer {
            // The first waits for an ack that never comes. Then L1 pauses, or the grant
            // lapses around the drop's fence, as the real walk saw (tester-s2 D8).
            Answer::Error { error, .. } if error.kind == ErrorKind::UnknownOutcome => {}
            Answer::Error { error, .. } => {
                tracing::info!(name = error.name(), "d8_put_refused_with_two_down");
                break;
            }
            other => panic!("put {} with two down: {other:?}", request - 1),
        }
    }
    let (dropped, mode, members) = trio.until("node 1 drops a copy", |trio| {
        commit_of(trio).filter(|(_, _, members)| members.len() == 2)
    });
    assert_eq!(
        (dropped, mode),
        (before.next(), PartitionMode::ReadOnly),
        "one dropped, the other silent: {members:?}"
    );
    let (kept, gone) = match members[..] {
        [1, 2] => ((two, 2), three),
        [1, 3] => ((three, 3), two),
        _ => panic!("the drop kept {members:?}"),
    };
    let asked = trio.nodes[0].0.plans[&PartitionId(1)].anchor;
    TwoDown {
        trio,
        kept,
        _gone: gone,
        dropped,
        asked,
        request,
    }
}

/// The kept member starts again, as `Db::restart_node` restarts a node: a new host opens its
/// directory at the next boot and takes the mail that waited, in order, and node 1 hears the
/// new boot. Returns when.
fn restart(trio: &mut Trio, ((host, tx, rx), node): (Dead, u32)) -> Tick {
    let node = NodeId(node);
    let boot = host.boot;
    let reboot = BootId(boot.0 + 1);
    let control = Arc::clone(&host.control);
    let path = host.engine.path().to_owned();
    drop(host);
    control.forget_node(node, boot);
    let engine = RocksEngine::open(path).expect("reopen the node");
    let mut host = Host::new(node, engine, Arc::clone(&trio.links), control, trio.clock);
    host.boot = reboot;
    tuned(&mut host);
    trio.links.register(node, tx.clone(), reboot);
    trio.nodes.push((host, tx, rx));
    trio.nodes[0]
        .1
        .send(Msg::PeerBooted { node, boot: reboot })
        .expect("node 1 up");
    trio.clock.now()
}

/// D8's second half: after `dropped`, node 1 commits `DegradedRf2` with itself and node `kept`
/// within the bound from `returned`, and a put lands there. That is the rejoin's commit, or,
/// when the kept copy reached the cutoff only after it answered, the commit of the repair
/// re-plan that follows the ReadOnly commit (`repair_duty`, A-R319). No answer was read as
/// `StaleLineage`.
fn rejoined_and_writes_land(
    trio: &mut Trio,
    method: &str,
    (dropped, kept): (Generation, u32),
    returned: Tick,
    request: u64,
) {
    let (after, members) = trio.until("node 1 commits DegradedRf2 with both", |trio| {
        commit_of(trio)
            .filter(|(generation, mode, _)| {
                *generation > dropped && *mode == PartitionMode::DegradedRf2
            })
            .map(|(generation, _, members)| (generation, members))
    });
    let waited = HostClock::millis_until(returned, trio.clock.now());
    assert_eq!(members, vec![1, kept], "{waited} ms after the return");
    // At most two runs of the spec's window: the rejoin, and the repair re-plan a ReadOnly
    // commit gets, each with its fence.
    let window = trio.nodes[0].0.budgets.discovery_window_millis;
    assert!(
        waited <= 2 * window + DROP_AFTER_MILLIS,
        "DegradedRf2 committed {waited} ms after the return"
    );
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, after),
        other => panic!("a put after the return: {other:?}"),
    }
    let stale: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == "recovery_fact")
        .filter(|line| {
            line["effect"]
                .as_str()
                .is_some_and(|effect| effect.contains("StaleLineage"))
        })
        .collect();
    assert!(
        stale.is_empty(),
        "a late answer was read as stale: {stale:?}"
    );
}

/// D8 (S2-5, two down and one back), as the walk saw it: [`two_down`], then the kept member
/// starts again. Its new host takes the mail that waited, including the dropped generation's
/// inventory ask, and its new boot starts a rejoin. Writes must land again at `DegradedRf2`
/// with both. Before the fix the partition stayed `ReadOnly` for good: the late answer was
/// read as `StaleLineage`, and a copy that reached the cutoff just after its answer had no
/// way back in.
/// Cuts: no `inventory_answer_late` check in `process`, red (`StaleLineage`); no
/// `repair_duty`, red when the ask beats the frames (stays `ReadOnly`).
/// Integration (~5 s): bootstrap, the drop clock, the drop's window, one or two re-plans.
#[config_log::retcd_test]
fn m10_s2_d8_two_down_return_commits_rf2_and_writes_land() {
    const METHOD: &str = "m10_s2_d8_two_down_return_commits_rf2_and_writes_land";
    let dir = config_testkit::fs::temp_dir();
    let mut down = two_down(dir.path());
    // Fix 3 (A-R337 item 3): the ReadOnly commit set up no rebuild, and its stall line says
    // so instead of "never pinned". Asked for directly, as host/tests.rs asks for M27's.
    // Cut: `unpinned_cause` always "never pinned", red.
    let host = &mut down.trio.nodes[0].0;
    host.rebuild_check(PartitionId(1), down.dropped);
    let stalled = host.status(PartitionId(1)).stalled.expect("the stall line");
    assert!(
        stalled.contains("phase=Committed")
            && stalled.ends_with(
                "F1 set up no rebuild, so the partition stays ReadOnly and does not activate"
            ),
        "{stalled}"
    );
    host.stalled.remove(&PartitionId(1));
    let kept = down.kept.1;
    let returned = restart(&mut down.trio, down.kept);
    rejoined_and_writes_land(
        &mut down.trio,
        METHOD,
        (down.dropped, kept),
        returned,
        down.request,
    );
    let late = logged!(METHOD, "inventory_answer_late");
    assert!(
        late.iter()
            .any(|line| line["node"] == 1 && line["seen_gen"] == down.asked.lineage.generation.0),
        "node 1 dropped the late answer: {late:?}"
    );
}

/// D8, critic-d8 finding 1: a late answer that is `InventoryFailed` (a copy answering an old
/// ask after it linked a newer generation says so) must not drop the copy from the current
/// run either. [`two_down`], then the kept member starts again with its link to node 1 held.
/// Once node 1's rejoin plan is in place, an `InventoryFailed` for the dropped generation's
/// ask reaches it; then the link heals and the copy answers the current ask.
/// Cut: no `inventory_answer_late` check in `process`, red (the copy dropped, `ReadOnly`).
/// Integration (~5 s): as the row above.
#[config_log::retcd_test]
fn m10_s2_d8_a_late_inventory_failed_does_not_drop_the_copy() {
    const METHOD: &str = "m10_s2_d8_a_late_inventory_failed_does_not_drop_the_copy";
    let dir = config_testkit::fs::temp_dir();
    let mut down = two_down(dir.path());
    let kept = down.kept.1;
    let copy = copy_of(&down.trio, kept);
    down.trio.links.hold(NodeId(1), NodeId(kept));
    let returned = restart(&mut down.trio, down.kept);
    let dropped = down.dropped;
    down.trio.until("node 1 plans the rejoin", |trio| {
        let plan = trio.nodes[0].0.plans.get(&PartitionId(1))?;
        (plan.anchor.lineage.generation == dropped).then_some(())
    });
    down.trio.nodes[0]
        .1
        .send(Msg::Answered {
            partition: PartitionId(1),
            correlation: REJOIN,
            asked: down.asked,
            kind: KernelEvent::Recovery(RecoveryEvent::InventoryFailed { copy }),
        })
        .expect("node 1 up");
    pass(&mut down.trio, 50);
    down.trio.links.heal(NodeId(1), NodeId(kept));
    rejoined_and_writes_land(
        &mut down.trio,
        METHOD,
        (dropped, kept),
        returned,
        down.request,
    );
    let late = logged!(METHOD, "inventory_answer_late");
    assert!(
        late.iter().any(|line| line["node"] == 1
            && line["seen_gen"] == down.asked.lineage.generation.0
            && line["failed"] == true),
        "node 1 dropped the late InventoryFailed: {late:?}"
    );
}

/// Node `node`'s copy in node 1's last commit.
fn copy_of(trio: &Trio, node: u32) -> CopyId {
    trio.nodes[0].0.committed[&PartitionId(1)]
        .committed
        .pinned_config
        .members
        .iter()
        .find(|member| member.node == NodeId(node))
        .map(|member| member.copy)
        .expect("a member")
}
