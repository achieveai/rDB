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
/// Since A-R336, node 3's newer boot also makes the drop's commit re-add it at once as a shadow
/// (b15), which L1 never counts: the put lands at the drop's generation or that one.
/// Cut: `rejoin_blocked` returns at once, red (node 1's F1 stays blocked).
/// Integration (~5 s): bootstrap's window, puts until L1 pauses, F1's sync and barrier
/// waits, the abandon's re-plan.
#[config_log::retcd_test]
fn m10_s2_b10_a_rejoin_whose_barrier_never_completes_is_abandoned() {
    const METHOD: &str = "m10_s2_b10_a_rejoin_whose_barrier_never_completes_is_abandoned";
    let dir = config_testkit::fs::temp_dir();
    // D9 (A-R398): the regular rejoin path, which a restart from an `Active` commit no longer takes.
    let (mut trio, before) = started(dir.path(), |host| {
        tuned(host);
        host.restart_shadow = false;
    });
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
    // ADV-4 (A-R406): name the path when the restart was shadowed after all.
    let plans = logged!(METHOD, "rejoin_plan");
    assert!(
        plans.iter().all(|plan| plan["shadowed"] == "None"),
        "b10 is F1's regular rejoin, which `restart_shadow = false` keeps; a plan shadowed \
         node 3: {plans:?}"
    );
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
    // At the drop's generation, or at the shadow re-add's that follows it (A-R336).
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert!(result.generation >= after, "{result:?}"),
        other => panic!("a put after the abandon: {other:?}"),
    }

    let plans = logged!(METHOD, "rejoin_plan");
    assert_eq!(
        plans[0]["dropped"], "None",
        "the rejoin links node 3: {plans:?}"
    );
    assert_eq!(plans[0]["rejoiner"], "Some(3)", "{plans:?}");
    let left_out = plans
        .iter()
        .position(|plan| plan["dropped"] == "Some(3)")
        .unwrap_or_else(|| panic!("a plan leaves node 3 out: {plans:?}"));
    assert!(
        plans[left_out + 1..]
            .iter()
            .all(|plan| plan["readded"] == "Some(3)"),
        "after node 3 is left out, a plan only re-adds it as a shadow: {plans:?}"
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
        promote: false,
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
/// the resume hold, red (`linked_ms` below it). A held link is never probed into a repair
/// either (b20; the `linked` filter's cut is b19's).
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

/// b26 (A-R381, A-R383, parked by A-R386): a `ReadOnly` commit drops nothing. Every regular
/// copy is outside its barrier, so a blame there picks a copy almost at random, the healthy one
/// included. The trio starts with node 1's links to nodes 2 and 3 held, so F1 commits
/// `ReadOnly` on nodes 1, 2 and 3 at cutoff 0. Node 2 then dies and the link 1-3 heals: node 3
/// answers, and the commit is repaired to `DegradedRf2` on the same members with node 3 in the
/// barrier. Only there does the drop clock run, and it drops node 2, the copy really missing.
///
/// Known gap, so ignored (issue #69): a copy missed at formation never lands. Node 3 holds no
/// generation 0, so it logs `land_link_refused{reason=anchor_generation_absent}`, its post-commit sync stays
/// `sync_deferred`, and every repair's inventory fails the same way and commits `ReadOnly`
/// again. The landing fix (critic-w9 "A-R383 re-plan", option (c)): treat an empty copy as head
/// 0 at the root, as `inventory_reported_empty` already does. This body is the acceptance row
/// for that fix, together with A-R381's `ReadOnly` gate in `holding_up` and A-R383's repair
/// past a pinned rebuild that only unanswering members hold.
/// Cuts, once it runs: `holding_up` blames in `ReadOnly`, red (a drop while `ReadOnly`, node 3
/// dropped, seen 2026-10-24); the repair skipped whenever the rebuild is pinned, red (no repair).
/// Integration (~5 s): the held bootstrap, three drop clocks of `ReadOnly`, the repair, the drop.
#[config_log::retcd_test]
#[ignore = "known gap: a copy missed at formation never lands; achieveai/rDB#69"]
fn m10_s2_b26_a_read_only_commit_drops_nothing_until_repaired() {
    const METHOD: &str = "m10_s2_b26_a_read_only_commit_drops_nothing_until_repaired";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), tuned);
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    trio.bootstrap();
    let read_only = trio.until("node 1 committed", commit_of);
    assert_eq!(
        (read_only.1.clone(), read_only.2.clone()),
        (PartitionMode::ReadOnly, vec![1, 2, 3]),
        "nodes 2 and 3 unheard through the bootstrap's discovery"
    );
    kill(&trio, 2, &[1, 3]);
    trio.links.heal(NodeId(1), NodeId(3));

    // Three drop clocks of `ReadOnly` with node 2 gone: nothing is dropped.
    let until = trio.clock.now().plus_millis(3 * DROP_AFTER_MILLIS);
    let repaired = trio.until("three drop clocks, or the repair", |trio| {
        let commit = commit_of(trio)?;
        (commit.1 != PartitionMode::ReadOnly || trio.clock.now() >= until).then_some(commit)
    });
    let dropped = logged!(METHOD, "membership_drop_planned");
    assert!(dropped.is_empty(), "a drop while ReadOnly: {dropped:?}");
    let (repaired, mode, members) = if repaired.1 == PartitionMode::ReadOnly {
        trio.until("node 1 repaired the read-only commit", |trio| {
            commit_of(trio).filter(|(_, mode, _)| *mode != PartitionMode::ReadOnly)
        })
    } else {
        repaired
    };
    assert_eq!(
        (mode, members),
        (PartitionMode::DegradedRf2, vec![1, 2, 3]),
        "repaired with node 3 in the barrier"
    );
    let dropped = logged!(METHOD, "membership_drop_planned");
    assert!(dropped.is_empty(), "a drop while ReadOnly: {dropped:?}");
    let planned = logged!(METHOD, "membership_repair_planned");
    assert_eq!(planned.len(), 1, "one repair: {planned:?}");

    // `DegradedRf2`: L1 blames node 2, and the drop clock drops it.
    let (after, mode, members) = trio.until("node 1 committed without node 2", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > repaired)
    });
    assert_eq!(
        (after, mode, members),
        (repaired.next(), PartitionMode::DegradedRf2, vec![1, 3])
    );
    let dropped = logged!(METHOD, "membership_drop_planned");
    assert_eq!(dropped.len(), 1, "one drop: {dropped:?}");
    assert_eq!(dropped[0]["dropped"], 2, "node 2 dropped: {dropped:?}");
}

/// A-R319 defect 2 (W5's probe, k546): a `ReadOnly` commit takes no write whatever L1 says,
/// so its rebuild check is never quiet on L1 admitting. Before, the check read L1 alone, and a
/// read-only partition whose L1 had resumed was silent. Every other mode stays quiet while L1
/// admits (K1 condition 6). A unit row: no host reaches a read-only commit with L1 admitting
/// on demand; the promote row `m10_s2_a_promote_that_missed_node_3_takes_writes_once_node_3_is_back`
/// walks it end to end.
/// Cut: `rebuild_quiet` reads L1 alone, red (read-only and admitting reads as quiet).
/// A-R366 (critic Q3): a `ReadOnly` commit's repair is for its regular secondaries only. The
/// primary is not outside, and a shadow is never repaired: [`Host::shadow_duty`] promotes it.
/// Cut: `read_only_outside` keeps any role (r346d2), red (node 3 the shadow, node 1).
#[test]
fn a_read_only_repair_counts_regular_secondaries_only() {
    let member = |n: u8, role| Member {
        copy: CopyId(n - 1),
        node: NodeId(u32::from(n)),
        boot: BOOT,
        role,
    };
    let members = [
        member(1, ReplicaRole::Primary),
        member(2, ReplicaRole::RegularSecondary),
        member(3, ReplicaRole::Shadow),
    ];
    assert_eq!(regular_secondaries(&members), vec![NodeId(2)]);
}

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
fn restart_dead(trio: &mut Trio, ((host, tx, rx), node): (Dead, u32)) -> Tick {
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
    tracing::info!(waited, method, "d8_rf2_after_return_ms");
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
        stalled.contains("phase=Committed: F1 set up no rebuild")
            && !stalled.contains("host_catch_up")
            && stalled.ends_with(
                "F1 set up no rebuild, so the partition stays ReadOnly and does not activate"
            ),
        "{stalled}"
    );
    host.stalled.remove(&PartitionId(1));
    let kept = down.kept.1;
    let returned = restart_dead(&mut down.trio, down.kept);
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
/// Cut: no `inventory_answer_late` check in `process`, red: F1 drops the copy as a source
/// (`RecordSourceUnavailable`) before its real answer arrives.
/// Integration (~5 s): as the row above.
#[config_log::retcd_test]
fn m10_s2_d8_a_late_inventory_failed_does_not_drop_the_copy() {
    const METHOD: &str = "m10_s2_d8_a_late_inventory_failed_does_not_drop_the_copy";
    let dir = config_testkit::fs::temp_dir();
    let mut down = two_down(dir.path());
    let kept = down.kept.1;
    let copy = copy_of(&down.trio, kept);
    down.trio.links.hold(NodeId(1), NodeId(kept));
    let returned = restart_dead(&mut down.trio, down.kept);
    let dropped = down.dropped;
    down.trio.until("node 1 plans the rejoin", |trio| {
        let plan = trio.nodes[0].0.plans.get(&PartitionId(1))?;
        (plan.anchor.lineage.generation == dropped).then_some(())
    });
    let before = unavailable(METHOD, copy);
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
    // The link is still held, so nothing the copy sent has reached node 1: F1 drops no
    // source on the late Failed alone (critic-d8r M1).
    assert_eq!(
        unavailable(METHOD, copy),
        before,
        "the late InventoryFailed dropped copy {copy:?} before its real answer"
    );
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

/// How many times node 1's F1 has dropped `copy` as a record source so far in this test.
fn unavailable(method: &str, copy: CopyId) -> usize {
    let fact = format!("RecordSourceUnavailable {{ copy: {copy:?},");
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == "recovery_fact" && line["node"] == 1)
        .filter(|line| line["effect"].as_str().is_some_and(|e| e.contains(&fact)))
        .count()
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

/// b19 (A-R346 M1): a repair that cannot take its member in is not repeated until that member
/// moves. b6's read-only commit {1, 2}, node 2 outside, but node 2 now refuses every inventory
/// F1 asks of it, as a copy that missed a generation below the base does (#59). Once node 2
/// links again the owner repairs once; F1 leaves node 2 out, so the repair commits `ReadOnly`
/// again. Over three more holds, with node 2 linked throughout and nothing about it moved, no
/// second repair runs: one `membership_repair_held` line, and the commit stays where the repair
/// left it. Node 2's link going down and up again is a move: the next repair runs at once, and
/// ends read-only again. Then node 2 stops refusing, with no flap and nothing moved (A-R353):
/// the held repair runs again once its wait has passed, and the partition takes writes.
/// Cuts: no bound in `repair_duty`, red (a second `membership_repair_planned` while held); no
/// `linked` filter, or a link going down that keeps the mark, red (no repair after the new link);
/// no timed retry, red (still read-only after the refusal clears).
/// Integration (~12 s): b1's drop, three repairs, three holds, one 5 s wait.
#[config_log::retcd_test]
fn m10_s2_b19_a_repair_that_ends_read_only_again_waits_for_its_member_to_move() {
    const METHOD: &str =
        "m10_s2_b19_a_repair_that_ends_read_only_again_waits_for_its_member_to_move";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    put_until_paused(&mut trio, 1);
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

    trio.nodes[1].0.refuse_inventories = true;
    trio.links.heal(NodeId(1), NodeId(2));
    let again = trio.until("node 1 repaired, and the repair ended read-only", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > read_only.0)
    });
    assert_eq!(
        (again.1.clone(), again.2.clone()),
        (PartitionMode::ReadOnly, vec![1, 2]),
        "{again:?}"
    );
    let holds = |trio: &mut Trio, label: &str| {
        let until = trio.clock.now().plus_millis(
            3 * trio.nodes[0].0.budgets.resume_hold_millis + 3 * DROP_CHECK_EVERY_MILLIS,
        );
        trio.until(label, |trio| (trio.clock.now() >= until).then_some(()));
    };
    holds(&mut trio, "three more holds with node 2 linked");
    assert_eq!(commit_of(&trio), Some(again.clone()), "repaired again");
    let planned = logged!(METHOD, "membership_repair_planned");
    assert_eq!(
        planned.len(),
        1,
        "one repair while nothing moved: {planned:?}"
    );
    let held = logged!(METHOD, "membership_repair_held");
    assert_eq!(held.len(), 1, "the held repair is logged once: {held:?}");

    // A new link is a move.
    trio.links.hold(NodeId(1), NodeId(2));
    holds(&mut trio, "node 2 down for a while");
    trio.links.heal(NodeId(1), NodeId(2));
    let flapped = trio.until("node 1 repaired after the new link", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > again.0)
    });
    assert_eq!(
        (flapped.1.clone(), flapped.2.clone()),
        (PartitionMode::ReadOnly, vec![1, 2]),
        "{flapped:?}"
    );
    let planned = logged!(METHOD, "membership_repair_planned");
    assert_eq!(planned.len(), 2, "repaired after the new link: {planned:?}");

    // A-R353: the refusal clears with nothing moved; the held repair runs once its wait passed.
    trio.nodes[1].0.refuse_inventories = false;
    let repaired = trio.until("node 1 repaired once its wait passed", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > flapped.0)
    });
    assert_eq!(
        (repaired.1, repaired.2),
        (PartitionMode::DegradedRf2, vec![1, 2])
    );
    let planned = logged!(METHOD, "membership_repair_planned");
    assert_eq!(planned.len(), 3, "one timed retry: {planned:?}");
    let wait = REPAIR_RETRY_FIRST_MILLIS;
    assert_eq!(planned[1]["next_wait_ms"], wait, "{planned:?}");
    assert_eq!(planned[2]["next_wait_ms"], 2 * wait, "{planned:?}");
    let held = logged!(METHOD, "membership_repair_held");
    assert_eq!(held.len(), 2, "each wait logged once: {held:?}");
}

/// b20 (A-R350): a half-open member is never repaired, so reads are never fenced for it. b6's
/// read-only commit {1, 2}, node 2 outside; the hold lifts, but everything node 1 sends node 2
/// is lost, as on a connection that is up and answers nothing. Node 2 counts as linked, so
/// each check past the resume hold probes it with an `Inspect`, and no answer comes: over three
/// holds there is no repair, no fence and no new generation. Once node 1's frames reach node 2
/// again, a probe is answered and the repair runs.
/// Cut: a linked member counts as answered, red (a repair while node 2 answers nothing).
/// Integration (~5 s): b1's drop, three holds, one repair.
#[config_log::retcd_test]
fn m10_s2_b20_a_member_that_answers_no_probe_is_not_repaired() {
    half_open_is_not_repaired(
        "m10_s2_b20_a_member_that_answers_no_probe_is_not_repaired",
        (NodeId(1), NodeId(2)),
    );
}

/// b21 (A-R353, critic A4): b20 with the other half lost: node 2 hears node 1, and nothing it
/// sends back arrives. In this process the probe's answer comes back over its own channel, not
/// over the links, so the owner must not count it: else it repairs, F1's inventory of node 2 is
/// lost, and reads are fenced once for nothing.
/// Cut: drop the dropped-direction check from the probe's answers, red (a repair while node 2's
/// answers are lost).
/// Integration (~5 s): b1's drop, three holds, one repair.
#[config_log::retcd_test]
fn m10_s2_b21_a_member_whose_answers_are_lost_is_not_repaired() {
    half_open_is_not_repaired(
        "m10_s2_b21_a_member_whose_answers_are_lost_is_not_repaired",
        (NodeId(2), NodeId(1)),
    );
}

/// b23 (A-R353, critic A7): a held repair's wait doubles from the first and stops at the cap,
/// so a persistent F1 miss fences reads at most once a minute: 5, 10, 20, 40, then 60 s for
/// every retry after.
/// Cut: `next_wait` without the cap, red (80 s at the fifth retry).
/// Unit: no trio.
#[test]
fn m10_s2_b23_a_held_repairs_wait_doubles_up_to_the_cap() {
    let mut held = RepairHeld {
        marks: Vec::new(),
        at: Tick(0),
        wait_millis: REPAIR_RETRY_FIRST_MILLIS,
        logged: false,
    };
    let mut waits = vec![held.wait_millis];
    for _ in 0..6 {
        held.wait_millis = held.next_wait();
        waits.push(held.wait_millis);
    }
    assert_eq!(
        waits,
        [5_000, 10_000, 20_000, 40_000, 60_000, 60_000, 60_000]
    );
    assert_eq!(REPAIR_RETRY_CAP_MILLIS, 60_000);
}

/// b20's and b21's row, with `lost` the direction whose frames are dropped.
fn half_open_is_not_repaired(method: &'static str, lost: (NodeId, NodeId)) {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    put_until_paused(&mut trio, 1);
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
    let fenced = logged!(method, "authority_fence").len();

    trio.links.heal(NodeId(1), NodeId(2));
    trio.links.drop_direction(lost.0, lost.1);
    let until = trio
        .clock
        .now()
        .plus_millis(3 * trio.nodes[0].0.budgets.resume_hold_millis + 3 * DROP_CHECK_EVERY_MILLIS);
    trio.until("three holds with one direction lost", |trio| {
        (trio.clock.now() >= until).then_some(())
    });
    assert_eq!(
        commit_of(&trio),
        Some(read_only.clone()),
        "repaired a half-open member"
    );
    let planned = logged!(method, "membership_repair_planned");
    assert!(
        planned.is_empty(),
        "repaired a half-open member: {planned:?}"
    );
    let fences = logged!(method, "authority_fence");
    assert_eq!(
        fences.len(),
        fenced,
        "fenced for a half-open member: {fences:?}"
    );

    trio.links.heal(NodeId(1), NodeId(2));
    let repaired = trio.until("node 1 repaired once node 2 answers", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > read_only.0)
    });
    assert_eq!(
        (repaired.1, repaired.2),
        (PartitionMode::DegradedRf2, vec![1, 2])
    );
    let planned = logged!(method, "membership_repair_planned");
    assert_eq!(planned.len(), 1, "one repair: {planned:?}");
}

/// Node `n` restarts: its host closes, and a new one opens its engine in `dir` with the next
/// boot, as `spawn_on` and `run` open one. Its links come back registered at that boot, and
/// the other nodes hear `PeerBooted`. A new host, not new links alone: every frame it sends
/// carries its own boot, which the next commit pins.
fn restart(trio: &mut Trio, dir: &std::path::Path, n: u32) -> BootId {
    restart_as(trio, dir, n, false)
}

/// [`restart`], with node `n`'s data folder removed first when `wipe`: a new process on an
/// empty disk, as `lr start N --wipe`.
fn restart_as(trio: &mut Trio, dir: &std::path::Path, n: u32, wipe: bool) -> BootId {
    let at = trio
        .nodes
        .iter()
        .position(|(host, _, _)| host.node == NodeId(n))
        .expect("node in the trio");
    let (old, _, _) = trio.nodes.remove(at);
    let boot = BootId(old.boot.0 + 1);
    let control = Arc::clone(&old.control);
    drop(old);
    if wipe {
        std::fs::remove_dir_all(dir.join(n.to_string())).expect("wipe the node's folder");
    }
    let engine = RocksEngine::open(dir.join(n.to_string())).expect("reopen engine");
    let (tx, rx) = mpsc::channel();
    trio.links.register(NodeId(n), tx.clone(), boot);
    let mut host = Host::new(
        NodeId(n),
        engine,
        Arc::clone(&trio.links),
        control,
        trio.clock,
    );
    host.boot = boot;
    tuned(&mut host);
    host.restore_revocations();
    trio.nodes.insert(at, (host, tx, rx));
    for (host, mailbox, _) in &trio.nodes {
        if host.node != NodeId(n) {
            mailbox
                .send(Msg::PeerBooted {
                    node: NodeId(n),
                    boot,
                })
                .expect("node up");
        }
    }
    boot
}

/// b22 (A-R353): a restart of the member a held repair was for is a move, so the partition is
/// re-planned at once, well inside the wait. b19's held repair, node 2 refusing; then node 2
/// restarts (its new process refuses nothing). Within the first wait the commit pins node 2 at
/// its new boot, `DegradedRf2`, and takes writes.
/// Cut: the boot mark, see the dev notes for what it shows (`rejoin_if_stale` re-plans a
/// restarted pinned member too).
/// Integration (~6 s): b1's drop, one repair, one restart re-plan.
#[config_log::retcd_test]
fn m10_s2_b22_a_restarted_member_is_repaired_at_once() {
    const METHOD: &str = "m10_s2_b22_a_restarted_member_is_repaired_at_once";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    put_until_paused(&mut trio, 1);
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
    trio.nodes[1].0.refuse_inventories = true;
    trio.links.heal(NodeId(1), NodeId(2));
    let again = trio.until("node 1 repaired, and the repair ended read-only", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > read_only.0)
    });
    assert_eq!(again.1, PartitionMode::ReadOnly, "{again:?}");
    trio.until("the next repair held", |_| {
        (!logged!(METHOD, "membership_repair_held").is_empty()).then_some(())
    });

    let restarted = trio.clock.now();
    let boot = restart(&mut trio, dir.path(), 2);
    let (generation, mode, members) =
        trio.until("node 1 re-planned with node 2's new boot", |trio| {
            let pinned = trio.nodes[0]
                .0
                .committed
                .get(&PartitionId(1))?
                .committed
                .pinned_config
                .members
                .iter()
                .any(|member| member.node == NodeId(2) && member.boot == boot);
            pinned.then(|| commit_of(trio)).flatten()
        });
    let waited = HostClock::millis_until(restarted, trio.clock.now());
    assert!(waited < REPAIR_RETRY_FIRST_MILLIS, "waited {waited} ms");
    assert!(generation > again.0, "{generation:?}");
    assert_eq!((mode, members), (PartitionMode::DegradedRf2, vec![1, 2]));
    trio.ready();
    match trio.ask(put(1_000)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, generation),
        other => panic!("a put after the restart: {other:?}"),
    }
}

/// Node 3's role in node 1's commit, when the commit pins it.
fn role_of_three(trio: &Trio) -> Option<ReplicaRole> {
    trio.nodes[0]
        .0
        .committed
        .get(&PartitionId(1))?
        .committed
        .pinned_config
        .members
        .iter()
        .find(|member| member.node == NodeId(3))
        .map(|member| member.role)
}

/// b15 (A-R329, A-R336 option 3): node 3 restarts after node 1 has decided to drop it. The
/// drop commits without node 3, and since it left out a member that has started again while
/// the commit pins two regular copies, node 1 re-plans with node 3 as a shadow. L1 never
/// counts it, so writes go on at rf2 while R1 catches it up off the stream. Once it has applied
/// the cutoff, node 1 promotes it: the next commit pins it as a regular secondary, synced in
/// the barrier, and a put lands there. It is never dropped again. Node 3 restarts as a new
/// host ([`restart`]), so the frames it sends carry the boot the commit pins.
/// The catch-up (A-R361): node 1 still holds every record node 3 lacks, so R1's shadow cursor
/// sends them (`copy_caught_up`); H2's snapshot serves only a record no longer held. The row
/// takes either, and asserts no `snapshot_catchup_unservable`. The H2-only path is owed with
/// History trim (#61): nothing trims records in this tree, so no row can reach it yet.
/// (e) A-R366: node 3's links stay held from its restart, so it is a shadow that is behind.
/// It restarts again; the restart's re-plan pins it again as a shadow at its new boot, and
/// nothing promotes it until the links heal and it catches up.
/// Deterministic: node 3 boots only once node 1 runs the drop's re-plan.
/// Cuts: `drained` re-adds as `RegularSecondary`, red (L1 pauses on it, dropped again);
/// `shadow_duty` never promotes, red (node 3 stays a shadow); `drained` promotes any
/// rejoiner shadow, red (e).
/// Integration (~6 s): b1's drop, the re-add's re-plan, the restart's, the promote's.
#[config_log::retcd_test]
fn m10_s2_b15_a_dropped_copy_that_restarts_is_added_back() {
    const METHOD: &str = "m10_s2_b15_a_dropped_copy_that_restarts_is_added_back";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);

    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    trio.until("node 1 decided to drop node 3", |trio| {
        matches!(
            trio.nodes[0].0.rejoins.get(&PartitionId(1)),
            Some(Rejoin::Revoking { replan, .. } | Rejoin::Planned { replan, .. })
                if replan.drop == Some(NodeId(3))
        )
        .then_some(())
    });
    trio.links.heal_all();
    // Node 3 hears nothing, and is heard by nobody, until (e) heals it: a shadow that is behind.
    trio.links.hold(NodeId(1), NodeId(3));
    trio.links.hold(NodeId(2), NodeId(3));
    restart(&mut trio, dir.path(), 3);

    // (a) Re-added as a shadow: L1 admits writes with node 3 counted for nothing.
    let (shadowed, mode, members) = trio.until("node 1 pinned node 3 as a shadow", |trio| {
        (role_of_three(trio) == Some(ReplicaRole::Shadow))
            .then(|| commit_of(trio))
            .flatten()
    });
    assert!(shadowed > before.next(), "{shadowed:?}");
    assert_eq!((mode, members), (PartitionMode::DegradedRf2, vec![1, 2, 3]));
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert!(result.generation >= shadowed),
        other => panic!("a put with node 3 a shadow: {other:?}"),
    }

    // (e) The behind shadow restarts: pinned again as a shadow at its new boot, not promoted.
    trio.until("the re-add's re-plan ended", |trio| {
        (!trio.nodes[0].0.rejoins.contains_key(&PartitionId(1))).then_some(())
    });
    let reboot = restart(&mut trio, dir.path(), 3);
    let (repinned, role) = trio.until("node 1 pinned node 3 at its new boot", |trio| {
        let committed = trio.nodes[0].0.committed.get(&PartitionId(1))?;
        let three = committed
            .committed
            .pinned_config
            .members
            .iter()
            .find(|member| member.node == NodeId(3) && member.boot == reboot)?;
        Some((committed.new_generation, three.role))
    });
    assert!(repinned > shadowed, "{repinned:?}");
    assert_eq!(role, ReplicaRole::Shadow, "a restart promoted the shadow");
    assert!(
        logged!(METHOD, "shadow_promote").is_empty(),
        "promoted while behind"
    );
    trio.links.heal_all();

    // (c) Promoted: a regular secondary again, at or past the cutoff, and a put lands there.
    let (promoted, mode, members) = trio.until("node 1 pinned node 3 as regular", |trio| {
        (role_of_three(trio) == Some(ReplicaRole::RegularSecondary))
            .then(|| commit_of(trio))
            .flatten()
    });
    assert!(promoted > shadowed, "{promoted:?}");
    assert_eq!((mode, members), (PartitionMode::Active, vec![1, 2, 3]));
    let host = &trio.nodes[0].0;
    let cutoff = host.committed[&PartitionId(1)].selected.cutoff_seq;
    let known = host
        .replication
        .primary(NodeId(1), PartitionId(1))
        .and_then(|primary| primary.tracker().known(CopyId(2)))
        .expect("node 1 tracks copy 2");
    assert!(known.durable.0 >= cutoff.0, "{known:?} below {cutoff:?}");
    trio.ready();
    match trio.ask(put(request + 1)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, promoted),
        other => panic!("a put after the promote: {other:?}"),
    }

    // (b) The shadow caught up, from records or from H2, then node 1 promoted it, once, at or
    // past the cutoff (A-R361).
    let mut caught_up: Vec<_> = logged!(METHOD, "copy_caught_up")
        .into_iter()
        .chain(logged!(METHOD, "snapshot_catchup_served"))
        .filter(|line| line["copy"] == 2)
        .collect();
    caught_up.sort_by(|a, b| a["@t"].as_str().cmp(&b["@t"].as_str()));
    assert!(
        logged!(METHOD, "snapshot_catchup_unservable").is_empty(),
        "a catch-up node 1 could not serve"
    );
    let promotes = logged!(METHOD, "shadow_promote");
    assert_eq!(promotes.len(), 1, "one promote: {promotes:?}");
    assert_eq!(promotes[0]["shadow"], 3);
    assert!(
        promotes[0]["acked"].as_u64() >= promotes[0]["cutoff"].as_u64(),
        "promoted below the cutoff: {promotes:?}"
    );
    assert!(
        caught_up
            .first()
            .is_some_and(|first| first["@t"].as_str() < promotes[0]["@t"].as_str()),
        "promoted before any catch-up: {caught_up:?} {promotes:?}"
    );
    // (d) Dropped once, never again.
    let planned = logged!(METHOD, "membership_drop_planned");
    assert_eq!(planned.len(), 1, "one drop: {planned:?}");
    let plans = logged!(METHOD, "rejoin_plan");
    assert_eq!(
        plans.first().map(|plan| plan["dropped"].clone()),
        Some("Some(3)".into()),
        "the drop came first: {plans:?}"
    );
    assert!(
        plans
            .iter()
            .any(|plan| plan["readded"] == "Some(3)" && plan["rejoiner"] == "Some(3)"),
        "node 3 was planned back: {plans:?}"
    );
    assert!(logged!(METHOD, "rejoin_readd_abandoned").is_empty());
}

/// b25 (A-R369): a promote whose `DrainProof` is lost is asked again as a promote. b15's
/// behind shadow; node 1 loses the next drain proof (b11's hook), then node 3's links heal.
/// It catches up, node 1 starts the promote, the proof is lost, and the revocation is asked
/// again once `peer_drop_after_millis` old. The first commit after the shadow's pins node 3
/// as a regular secondary, from one `shadow_promote`.
/// Cut: a retry drops `promote` (r366b), red (the retry pins node 3 as a shadow again).
/// Integration (~5 s): b1's drop, the re-add, the promote and its one retry.
#[config_log::retcd_test]
fn m10_s2_b25_a_promote_whose_drain_proof_is_lost_is_asked_again_as_a_promote() {
    const METHOD: &str =
        "m10_s2_b25_a_promote_whose_drain_proof_is_lost_is_asked_again_as_a_promote";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    put_until_paused(&mut trio, 1);
    trio.until("node 1 decided to drop node 3", |trio| {
        matches!(
            trio.nodes[0].0.rejoins.get(&PartitionId(1)),
            Some(Rejoin::Revoking { replan, .. } | Rejoin::Planned { replan, .. })
                if replan.drop == Some(NodeId(3))
        )
        .then_some(())
    });
    trio.links.heal_all();
    trio.links.hold(NodeId(1), NodeId(3));
    trio.links.hold(NodeId(2), NodeId(3));
    restart(&mut trio, dir.path(), 3);
    let (shadowed, _, _) = trio.until("node 1 pinned node 3 as a shadow", |trio| {
        (role_of_three(trio) == Some(ReplicaRole::Shadow))
            .then(|| commit_of(trio))
            .flatten()
    });
    trio.until("the re-add's re-plan ended", |trio| {
        (!trio.nodes[0].0.rejoins.contains_key(&PartitionId(1))).then_some(())
    });
    trio.nodes[0].0.drain_proofs_to_lose = 1;
    trio.links.heal_all();

    let (next, role) = trio.until("node 1 committed past the shadow's", |trio| {
        let (generation, _, _) = commit_of(trio)?;
        (generation > shadowed).then(|| (generation, role_of_three(trio)))
    });
    assert_eq!(next, shadowed.next(), "one commit after the shadow's");
    assert_eq!(
        role,
        Some(ReplicaRole::RegularSecondary),
        "the retried promote pinned node 3 as {role:?}"
    );
    let retries = logged!(METHOD, "rejoin_revoke_retry");
    assert_eq!(retries.len(), 1, "one retry: {retries:?}");
    let promotes = logged!(METHOD, "shadow_promote");
    assert_eq!(promotes.len(), 1, "one promote: {promotes:?}");
}

/// b24 (A-R336, critic A6, A-R366): a `ReadOnly` commit that pins a shadow is promoted, not
/// repaired. b6's read-only drop, node 2 held, with node 3 restarted while node 1 drops it:
/// the re-add pins node 3 as a shadow, and with node 2 still unheard the commit is `ReadOnly`
/// {1, 2, 3}. Node 3 catches up and node 1 starts its promote, which waits on node 2. While
/// that re-plan runs, over three holds, no repair is planned; once node 2 links again, the
/// promote commits with node 3 regular. Its mode is `Active` when node 2 proves the cutoff in
/// that barrier, else `DegradedRf2`. Both are legal: the `ReadOnly` commits took no write, so
/// node 2 already holds the cutoff, and only timing decides whether its proof is in time
/// (int gate k648 saw `Active`).
/// It does not reach the role filter: the running promote masks `read_only_outside`. That
/// filter's row is `a_read_only_repair_counts_regular_secondaries_only`.
/// Integration (~6 s): b1's drop, the re-add, three holds, the promote.
#[config_log::retcd_test]
fn m10_s2_b24_a_read_only_shadow_commit_is_promoted_not_repaired() {
    const METHOD: &str = "m10_s2_b24_a_read_only_shadow_commit_is_promoted_not_repaired";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    put_until_paused(&mut trio, 1);
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
    trio.until("node 1 decided to drop node 3", |trio| {
        matches!(
            trio.nodes[0].0.rejoins.get(&PartitionId(1)),
            Some(Rejoin::Revoking { replan, .. } | Rejoin::Planned { replan, .. })
                if replan.drop == Some(NodeId(3))
        )
        .then_some(())
    });
    trio.links.heal(NodeId(1), NodeId(3));
    trio.links.heal(NodeId(2), NodeId(3));
    restart(&mut trio, dir.path(), 3);

    let (shadowed, mode, members) = trio.until("node 1 pinned node 3 as a shadow", |trio| {
        (role_of_three(trio) == Some(ReplicaRole::Shadow))
            .then(|| commit_of(trio))
            .flatten()
    });
    assert_eq!(
        (mode, members),
        (PartitionMode::ReadOnly, vec![1, 2, 3]),
        "node 2 unheard through the re-add"
    );
    let until = trio
        .clock
        .now()
        .plus_millis(3 * trio.nodes[0].0.budgets.resume_hold_millis + 3 * DROP_CHECK_EVERY_MILLIS);
    trio.until("three holds with node 2 held", |trio| {
        (trio.clock.now() >= until).then_some(())
    });
    let planned = logged!(METHOD, "membership_repair_planned");
    assert!(planned.is_empty(), "repaired for the shadow: {planned:?}");
    assert_eq!(commit_of(&trio).map(|(g, _, _)| g), Some(shadowed));

    // The shadow's promote, waiting on node 2, commits once node 2 links: no repair at all.
    trio.links.heal(NodeId(1), NodeId(2));
    let (_, mode, members) = trio.until("node 1 committed once node 2 linked", |trio| {
        commit_of(trio).filter(|(generation, _, _)| *generation > shadowed)
    });
    assert!(
        matches!(mode, PartitionMode::Active | PartitionMode::DegradedRf2),
        "{mode:?}"
    );
    assert_eq!(members, vec![1, 2, 3]);
    assert_eq!(role_of_three(&trio), Some(ReplicaRole::RegularSecondary));
    let planned = logged!(METHOD, "membership_repair_planned");
    assert!(planned.is_empty(), "{planned:?}");
}

/// D9 (A-R398, arch-s2 S2-4 wiped): a secondary that restarts on an empty disk under a live,
/// `Active` owner is re-pinned as a shadow, so writes go on with two copies while it catches
/// up; once it has, node 1 promotes it and the partition is `Active` on three again. Before
/// D9 the restart's re-plan pinned it regular: L1 counted a copy that held nothing and
/// refused writes until the whole copy was rebuilt (7.6-7.9 s at 10,000 records).
/// Cut: `restart_shadowed` returns `None`, red (node 3 pinned regular at its new boot).
/// Integration (~3 s): bootstrap's window, the restart's re-plan, the promote's.
#[config_log::retcd_test]
fn m10_s2_d9_a_wiped_secondary_rejoins_as_a_shadow_and_writes_go_on() {
    const METHOD: &str = "m10_s2_d9_a_wiped_secondary_rejoins_as_a_shadow_and_writes_go_on";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    for request in 1..=20 {
        match trio.ask(put(request)) {
            Answer::Txn { .. } => {}
            other => panic!("put {request} with every copy up: {other:?}"),
        }
    }

    let boot = restart_as(&mut trio, dir.path(), 3, true);
    let (shadowed, role, mode) = trio.until("node 1 pinned node 3 at its new boot", |trio| {
        let committed = trio.nodes[0].0.committed.get(&PartitionId(1))?;
        let three = committed
            .committed
            .pinned_config
            .members
            .iter()
            .find(|member| member.node == NodeId(3) && member.boot == boot)?;
        Some((committed.new_generation, three.role, committed.mode.clone()))
    });
    assert!(shadowed > before, "{shadowed:?}");

    // Writes go on while the new copy cannot catch up: the 1-3 link is held from the
    // restart's commit, and a put lands at that generation on nodes 1 and 2. Asked before the
    // role, so the cut fails on the write itself.
    trio.links.hold(NodeId(1), NodeId(3));
    trio.ready();
    match trio.ask(put(21)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, shadowed),
        other => panic!("a put while node 3 is held off after its restart: {other:?}"),
    }
    assert_eq!(
        (role, mode),
        (ReplicaRole::Shadow, PartitionMode::DegradedRf2),
        "the wiped copy is pinned as a shadow, not counted"
    );
    assert_eq!(
        role_of_three(&trio),
        Some(ReplicaRole::Shadow),
        "node 3 was promoted while held off"
    );
    trio.links.heal(NodeId(1), NodeId(3));

    let (promoted, mode, members) = trio.until("node 1 pinned node 3 as regular", |trio| {
        (role_of_three(trio) == Some(ReplicaRole::RegularSecondary))
            .then(|| commit_of(trio))
            .flatten()
            .filter(|(generation, mode, _)| {
                *generation > shadowed && *mode == PartitionMode::Active
            })
    });
    assert_eq!((mode, members), (PartitionMode::Active, vec![1, 2, 3]));
    trio.ready();
    match trio.ask(put(22)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, promoted),
        other => panic!("a put after the promote: {other:?}"),
    }
    let promotes = logged!(METHOD, "shadow_promote");
    assert_eq!(promotes.len(), 1, "one promote: {promotes:?}");
    assert!(
        promotes[0]["acked"].as_u64() >= promotes[0]["cutoff"].as_u64(),
        "promoted below the cutoff: {promotes:?}"
    );
    let plans = logged!(METHOD, "rejoin_plan");
    assert!(
        plans.iter().any(|plan| plan["shadowed"] == "Some(3)"),
        "the restart's plan shadowed node 3: {plans:?}"
    );
    assert!(logged!(METHOD, "membership_drop_planned").is_empty());
}

/// review-s2 F-007 (D9, S2-4 wiped): writes go on *while* a wiped copy catches up as a shadow,
/// with no link held. 1,000 records, node 3 restarted on an empty disk, then puts in a loop
/// until node 1's promote of the shadow commits. Some put lands while node 3's applied position
/// is below the cutoff `shadow_promote` names and still rising, and no run of refusals lasts
/// longer than the accepted pause (~2 s, scaled). D9's own row holds link 1-3 before it writes,
/// which stops the catch-up, so it never showed this.
/// Cut: L1 counts the shadow (drop the `Shadow` filter where the host builds L1's members), red
/// (writes refused until the copy is rebuilt).
/// The stepped trio catches the shadow up ~1.7 records per put it lands, so 3,000 records
/// needed ~4,500 puts and outran the deadline on a loaded host (2 of 4 runs).
/// Integration (~20 s): 1,000 puts, the restart's re-plan, ~1,500 puts of catch-up.
#[config_log::retcd_test]
fn m10_s2_f007_writes_land_while_a_wiped_shadow_catches_up() {
    const METHOD: &str = "m10_s2_f007_writes_land_while_a_wiped_shadow_catches_up";
    const SEED: u64 = 1_000;
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _) = started(dir.path(), tuned);
    for request in 1..=SEED {
        match trio.ask(put(request)) {
            Answer::Txn { .. } => {}
            other => panic!("put {request} with every copy up: {other:?}"),
        }
    }
    restart_as(&mut trio, dir.path(), 3, true);
    trio.until("node 1 pinned node 3 as a shadow", |trio| {
        (role_of_three(trio) == Some(ReplicaRole::Shadow)).then_some(())
    });

    let pause_bound = test_patience(Duration::from_millis(2_500));
    let deadline = Instant::now() + test_patience(Duration::from_secs(30));
    // Node 3's applied position at each put that landed while it was still a shadow.
    let mut landed_at: Vec<u64> = Vec::new();
    let mut refused_since: Option<Tick> = None;
    let mut request = SEED + 1;
    while role_of_three(&trio) == Some(ReplicaRole::Shadow) {
        assert!(Instant::now() < deadline, "node 3 was never promoted");
        let applied = trio.status(2).holds.map_or(0, |held| held.applied);
        let answer = trio.ask(put(request));
        request += 1;
        let now = trio.clock.now();
        match answer {
            Answer::Txn { .. } => {
                refused_since = None;
                landed_at.push(applied);
            }
            Answer::Error { error, .. } => {
                let since = *refused_since.get_or_insert(now);
                let refused_for = HostClock::millis_until(since, now);
                assert!(
                    u128::from(refused_for) <= pause_bound.as_millis(),
                    "puts refused for {refused_for} ms while node 3 caught up, last: {error:?}"
                );
            }
            other => panic!("put {}: {other:?}", request - 1),
        }
    }

    let promotes = logged!(METHOD, "shadow_promote");
    assert_eq!(promotes.len(), 1, "one promote: {promotes:?}");
    let cutoff = promotes[0]["cutoff"]
        .as_u64()
        .expect("the promote's cutoff");
    let below: Vec<u64> = landed_at
        .iter()
        .copied()
        .filter(|applied| *applied < cutoff)
        .collect();
    assert!(
        !below.is_empty(),
        "no put landed while node 3 was below the cutoff {cutoff}: {landed_at:?}"
    );
    assert!(
        below.iter().max() > below.iter().min(),
        "node 3's progress did not rise while puts landed: {below:?}"
    );
}

/// D9's setup up to the shadow commit: 20 puts, node 3 restarted wiped, and nodes 2 and 3 both
/// landed the commit that pins node 3 a shadow. Nothing is held: the trio is stepped by hand,
/// so nothing moves while a row then runs a promote.
fn shadow_landed(dir: &std::path::Path) -> Trio {
    let (mut trio, _) = started(dir, tuned);
    for request in 1..=20 {
        match trio.ask(put(request)) {
            Answer::Txn { .. } => {}
            other => panic!("put {request} with every copy up: {other:?}"),
        }
    }
    restart_as(&mut trio, dir, 3, true);
    trio.until("nodes 2 and 3 landed node 3 as a shadow", |trio| {
        [1, 2]
            .iter()
            .all(|&at| {
                trio.status(at)
                    .pinned
                    .iter()
                    .any(|member| member.node == NodeId(3) && member.role == ReplicaRole::Shadow)
            })
            .then_some(())
    });
    trio
}
/// `partitions/1` as the trio's control store holds it, rewritten as the layout-3 record a
/// `Db` writes (the trio's hosts write no root, which `admin::promote` needs), in a store of
/// its own so the trio's hosts never see it; `bump` moves its `config_version` on, as a
/// configuration `to` has not landed would.
fn rooted_store(trio: &Trio, bump: u64) -> Arc<dyn config_core::ConfigStore> {
    use rdb_core::authority::partition::{PartitionRecord, PartitionRoot, INSTANCE_LEN};
    let key =
        Bytes::from(rdb_core::contracts::control::ControlKey::Partition(PartitionId(1)).encode());
    let found = trio
        .rt
        .block_on(trio.store.get(config_core::GetRequest { key: key.clone() }))
        .expect("read partitions/1")
        .record
        .expect("partitions/1");
    let mut record = PartitionRecord::decode(&found.value).expect("a partition record");
    record.config_version.0 += bump;
    let root = PartitionRoot {
        mode: rdb_core::authority::partition::RecordMode::Active,
        owner_boot: trio
            .links
            .boot_of(record.owner)
            .expect("the owner is linked"),
        base_seq: rdb_core::contracts::ids::Seq(0),
        base_digest: rdb_core::contracts::digest::Digest::ROOT,
        predecessor_generation: record.generation,
        predecessor_cutoff: rdb_core::contracts::ids::Seq(0),
        instance: [7; INSTANCE_LEN],
        run: rdb_core::contracts::ids::Revision(1),
    };
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    trio.rt
        .block_on(store.put(config_core::PutRequest {
            key,
            value: record.encode_with(&root),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put partitions/1");
    store
}

/// A promote of `to` as `Db::promote` runs it: members from `to`'s own live status, then
/// `admin::promote` against `store`. `send` reaches nobody.
fn promote_in(
    trio: &Trio,
    store: &Arc<dyn config_core::ConfigStore>,
    to: u32,
    within: Duration,
) -> Result<crate::admin::Promoted, crate::admin::PromoteError> {
    let at = usize::try_from(to - 1).expect("a trio node");
    let (members, members_version) =
        crate::admin::promote_members(NodeId(to), Some(trio.status(at)), Duration::from_secs(1))?;
    let links = Arc::clone(&trio.links);
    let ctx = crate::admin::PromoteCtx {
        store: Arc::clone(store),
        budgets: trio.nodes[0].0.budgets,
        clock: trio.clock,
        to: NodeId(to),
        drop: None,
        within,
        members,
        members_version,
        member_boot: Arc::new(move |node: NodeId| links.boot_of(node)),
        send: Arc::new(|node: NodeId, _: Msg| Err(NodeStopped(node))),
    };
    trio.rt.block_on(crate::admin::promote(ctx))
}

/// `partitions/1`'s `mod_revision` in `store` now.
fn partition_revision(trio: &Trio, store: &Arc<dyn config_core::ConfigStore>) -> u64 {
    trio.rt
        .block_on(store.get(config_core::GetRequest {
            key: Bytes::from(
                rdb_core::contracts::control::ControlKey::Partition(PartitionId(1)).encode(),
            ),
        }))
        .expect("read partitions/1")
        .record
        .expect("partitions/1")
        .mod_revision
}

/// review-s2 F-004 (A-R412): `lr promote 3` while node 3 is a live shadow is refused
/// `INVALID_ARGUMENT` before anything is written, naming node 2 instead. The roles come from
/// node 3's own status, as `Db::promote` reads them. Before the fix the fixed rf3 list held no
/// shadow, so node 3 was fenced in and pinned primary over a copy that held nothing.
/// Cuts: `promote_members` returns `admin::members()`, red (no `Shadow` refusal, partitions/1
/// fenced); the host's status leaves `pinned` empty, red (`PeersUnknown`).
/// Integration (~3 s): bootstrap's window, the restart's re-plan.
#[config_log::retcd_test]
fn m10_s2_f004_a_promote_of_a_live_shadow_is_refused_before_it_writes() {
    let dir = config_testkit::fs::temp_dir();
    let trio = shadow_landed(dir.path());
    let store = rooted_store(&trio, 0);
    let before = partition_revision(&trio, &store);
    let refused = promote_in(&trio, &store, 3, Duration::from_secs(1)).expect_err("a shadow `to`");
    assert!(
        matches!(
            refused,
            crate::admin::PromoteError::Shadow {
                node: 3,
                instead: 2
            }
        ),
        "{refused:?}"
    );
    let error = ApiError::from(refused);
    assert_eq!(
        (error.kind, error.no_mutation),
        (ErrorKind::InvalidArgument, true),
        "{error:?}"
    );
    assert!(error.detail.contains("node 2"), "{error:?}");
    assert_eq!(
        partition_revision(&trio, &store),
        before,
        "partitions/1 was written"
    );
}

/// review-s2 F-004 (A-R412): a promote of the regular node 2 while node 3 is a live shadow
/// keeps node 3 a shadow in the members it plans (`promote_members_kept`). It is cut short
/// after it plans them: nothing answers `send`, and its time runs out in the expiry wait.
/// Cuts: `promote_members` returns `admin::members()`, red (node 3 kept `RegularSecondary`);
/// the host's status leaves `pinned` empty, red (`PeersUnknown`, nothing kept).
/// Integration (~4 s): bootstrap's window, the restart's re-plan, one second of promote.
#[config_log::retcd_test]
fn m10_s2_f004_a_promote_of_a_regular_node_keeps_the_shadow_a_shadow() {
    const METHOD: &str = "m10_s2_f004_a_promote_of_a_regular_node_keeps_the_shadow_a_shadow";
    let dir = config_testkit::fs::temp_dir();
    let trio = shadow_landed(dir.path());
    let store = rooted_store(&trio, 0);
    let stopped = promote_in(&trio, &store, 2, Duration::from_secs(1));
    assert!(stopped.is_err(), "nothing answers the plan: {stopped:?}");
    let kept = logged!(METHOD, "promote_members_kept");
    assert_eq!(kept.len(), 1, "{kept:?}");
    let members = kept[0]["members"].as_str().unwrap_or_default();
    assert!(
        members.contains("(2, ") && members.contains("Primary"),
        "node 2 planned primary: {members}"
    );
    assert!(
        members.contains("(3, ") && members.contains("Shadow)"),
        "node 3 kept a shadow: {members}"
    );
    assert!(!members.contains("RegularSecondary"), "{members}");
}

/// A-R415 C-2: `partitions/1` is one configuration past the one node 2 landed, as after a heal
/// node 2 missed. Its roles may be out of date, so a promote of node 2 is refused
/// `StaleMembers` naming both versions, with nothing written. Cut: no version check in
/// `admin::promote` → it plans node 2's stale roles and fences partitions/1 (revision moves).
/// Integration (~3 s): bootstrap's window, the restart's re-plan.
#[config_log::retcd_test]
fn m10_s2_c2_a_promote_from_an_older_configuration_is_refused_before_it_writes() {
    let dir = config_testkit::fs::temp_dir();
    let trio = shadow_landed(dir.path());
    let held = trio.status(1).config_version.expect("node 2 landed one").0;
    let store = rooted_store(&trio, 1);
    let before = partition_revision(&trio, &store);
    let refused = promote_in(&trio, &store, 2, Duration::from_secs(1)).expect_err("stale");
    assert!(
        matches!(
            refused,
            crate::admin::PromoteError::StaleMembers { node: 2, held: h, current: c }
                if h == held && c == held + 1
        ),
        "{refused:?}"
    );
    let error = ApiError::from(refused);
    assert_eq!(
        (error.kind, error.no_mutation),
        (ErrorKind::Unavailable, true),
        "{error:?}"
    );
    assert_eq!(
        partition_revision(&trio, &store),
        before,
        "partitions/1 was written"
    );
}

/// ADV-2 (A-R406): a restart under an `Active` owner is shadowed only when two *linked*
/// regular members are current. With node 2 held off node 1, a restarted node 3 is not
/// pinned a shadow: a shadow plan would leave node 1 the only counted copy it can reach, and
/// the app would read `RECOVERY_READ_ONLY` though node 3 is back.
/// Cut: drop `restart_shadowed`'s `linked` filter, red (node 3 shadowed, `ReadOnly`).
/// Integration (~3 s): bootstrap's window, the restart's re-plan.
#[config_log::retcd_test]
fn m10_s2_adv2_a_restart_beside_an_unlinked_member_is_not_shadowed() {
    const METHOD: &str = "m10_s2_adv2_a_restart_beside_an_unlinked_member_is_not_shadowed";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    trio.links.hold(NodeId(1), NodeId(2));
    let boot = restart_as(&mut trio, dir.path(), 3, false);
    let (generation, role, mode) = trio.until("node 1 pinned node 3 at its new boot", |trio| {
        let committed = trio.nodes[0].0.committed.get(&PartitionId(1))?;
        let three = committed
            .committed
            .pinned_config
            .members
            .iter()
            .find(|member| member.node == NodeId(3) && member.boot == boot)?;
        Some((committed.new_generation, three.role, committed.mode.clone()))
    });
    assert!(generation > before, "{generation:?}");
    assert_eq!(
        (role, mode),
        (ReplicaRole::RegularSecondary, PartitionMode::DegradedRf2),
        "node 3 shadowed beside an unlinked node 2"
    );
    let plans = logged!(METHOD, "rejoin_plan");
    assert!(
        plans.iter().all(|plan| plan["shadowed"] != "Some(3)"),
        "the restart's plan shadowed node 3: {plans:?}"
    );
}

/// ADV-3 (A-R406): D9's promote whose barrier never completes. A wiped node 3 restarts under
/// an `Active` owner and is pinned a shadow; it catches up and node 1 starts its promote. Once
/// F1 is past the inventory (synchronizing or at the barrier), node 3 is held: its links stay
/// up but nothing crosses them, so it proves nothing. F1 commits the promote without node 3's
/// proof, `DegradedRf2`, so L1 still counts nodes 1 and 2 only, and writes land there. While
/// node 3 stays held, through the drop clock and past it, no commit is ever `Active`: a copy
/// that has proved nothing is never counted, and the drop clock leaves it out (b7): nodes 1
/// and 2, `DegradedRf2`.
/// Cut: `restart_shadowed` returns `None`, red (node 3 is never pinned a shadow).
/// Integration (~6 s): bootstrap's window, the restart's re-plan, the promote's, the drop
/// clock while held.
#[config_log::retcd_test]
fn m10_s2_adv3_a_shadow_promote_whose_barrier_never_completes_counts_only_two() {
    const METHOD: &str =
        "m10_s2_adv3_a_shadow_promote_whose_barrier_never_completes_counts_only_two";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _) = started(dir.path(), tuned);
    for request in 1..=20 {
        match trio.ask(put(request)) {
            Answer::Txn { .. } => {}
            other => panic!("put {request} with every copy up: {other:?}"),
        }
    }
    restart_as(&mut trio, dir.path(), 3, true);
    let shadowed = trio.until("node 1 pinned node 3 as a shadow", |trio| {
        (role_of_three(trio) == Some(ReplicaRole::Shadow))
            .then(|| commit_of(trio))
            .flatten()
            .map(|(generation, _, _)| generation)
    });

    trio.until("node 1 waits on node 3 for its promote", |trio| {
        let phase = trio.nodes[0]
            .0
            .recoveries
            .get(&PartitionId(1))
            .map(Recovery::phase);
        let promoting = !logged!(METHOD, "shadow_promote").is_empty();
        let past_inventory = matches!(
            phase,
            Some(RecoveryPhase::Synchronizing | RecoveryPhase::Barrier)
        );
        (promoting && past_inventory).then_some(())
    });
    hold_three(&trio);

    let (promoted, mode) = trio.until("node 1 committed the promote", |trio| {
        commit_of(trio)
            .filter(|(generation, _, _)| *generation > shadowed)
            .map(|(generation, mode, _)| (generation, mode))
    });
    assert_eq!(
        mode,
        PartitionMode::DegradedRf2,
        "the promote counted node 3 before it proved anything"
    );
    trio.ready();
    match trio.ask(put(21)) {
        Answer::Txn { result, .. } => assert!(result.generation >= promoted, "{result:?}"),
        other => panic!("a put while node 3 is held after its promote: {other:?}"),
    }

    // Held through the drop clock and a margin: no commit counts node 3.
    let until = trio
        .clock
        .now()
        .plus_millis(DROP_AFTER_MILLIS + 4 * test_discovery_window_millis());
    let mut active = None;
    trio.until("the drop clock to pass while node 3 is held", |trio| {
        if let Some(commit) = commit_of(trio).filter(|(_, mode, _)| *mode == PartitionMode::Active)
        {
            active.get_or_insert(commit);
        }
        (trio.clock.now() >= until).then_some(())
    });
    assert_eq!(active, None, "a commit counted node 3 while it was held");
    trio.ready();
    match trio.ask(put(22)) {
        Answer::Txn { result, .. } => assert!(result.generation >= promoted, "{result:?}"),
        other => panic!("a put after the drop clock, node 3 held: {other:?}"),
    }

    // The stalled regular copy is dropped by the clock (b7), and nothing else: nodes 1 and 2.
    assert_eq!(
        commit_of(&trio).map(|(_, mode, members)| (mode, members)),
        Some((PartitionMode::DegradedRf2, vec![1, 2])),
        "node 3, held, was not dropped"
    );
    let dropped = logged!(METHOD, "rejoin_plan");
    assert!(
        dropped.iter().any(|plan| plan["dropped"] == "Some(3)"),
        "{dropped:?}"
    );
}

/// #59 item 3, the past half (A-R185 C2 "past the cutoff"): node 3 holds a tail of the old
/// generation past every later cutoff, as an old owner's unacknowledged writes would be. It is
/// dropped (b1), restarts, and is re-added as a shadow (b15). Its landing of that commit links
/// nothing (`land_link_refused{held_past_base}`), so the tail is never made part of the new
/// generation. The writes do not stall until a reopen: L1 never counts the shadow, R1 walks it
/// up from the root, node 1 promotes it, and the next commit is `Active` with all three, node 3
/// durable through its cutoff, and a put lands there.
/// Cut: `shadow_duty` returns at once, red (node 3 stays a shadow; no `Active` commit).
/// Integration (~6 s): b1's drop, the re-add's re-plan, the catch-up, the promote's.
#[config_log::retcd_test]
fn m10_s2_59_a_copy_past_the_cutoff_is_added_back_and_ends_caught_up() {
    const METHOD: &str = "m10_s2_59_a_copy_past_the_cutoff_is_added_back_and_ends_caught_up";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, before) = started(dir.path(), tuned);
    kill(&trio, 3, &[1, 2]);
    let (request, _) = put_until_paused(&mut trio, 1);
    trio.until("node 1 decided to drop node 3", |trio| {
        matches!(
            trio.nodes[0].0.rejoins.get(&PartitionId(1)),
            Some(Rejoin::Revoking { replan, .. } | Rejoin::Planned { replan, .. })
                if replan.drop == Some(NodeId(3))
        )
        .then_some(())
    });
    // The tail: 500 records of node 3's newest generation after its head, which no other copy
    // holds, so node 3 is past the cutoff of every commit this row makes.
    let three = &mut trio.nodes[2].0;
    let held = three
        .engine
        .lineages()
        .into_iter()
        .filter(|(p, _)| *p == PartitionId(1))
        .map(|(_, g)| g)
        .max()
        .expect("node 3 holds a generation");
    let head = Seq(three.engine.buffered_applied(PartitionId(1), held).0);
    let mut digests = vec![Digest::ROOT; usize::try_from(head.0).expect("a small head")];
    if let Some(last) = digests.last_mut() {
        *last = three
            .checked(PartitionId(1), held, head)
            .expect("its head")
            .1;
    }
    super::trio::write_chain_after(&mut three.engine, held, digests, 500);
    trio.links.heal_all();
    restart(&mut trio, dir.path(), 3);

    let shadowed = trio.until("node 1 pinned node 3 as a shadow", |trio| {
        (role_of_three(trio) == Some(ReplicaRole::Shadow))
            .then(|| commit_of(trio).map(|(generation, _, _)| generation))
            .flatten()
    });
    assert!(shadowed > before.next(), "{shadowed:?}");
    let (promoted, mode, members) =
        trio.until("node 1 committed Active with node 3 regular", |trio| {
            (role_of_three(trio) == Some(ReplicaRole::RegularSecondary))
                .then(|| commit_of(trio))
                .flatten()
                .filter(|(generation, mode, _)| {
                    *generation > shadowed && *mode == PartitionMode::Active
                })
        });
    assert_eq!((mode, members), (PartitionMode::Active, vec![1, 2, 3]));
    let host = &trio.nodes[0].0;
    let cutoff = host.committed[&PartitionId(1)].selected.cutoff_seq;
    let known = host
        .replication
        .primary(NodeId(1), PartitionId(1))
        .and_then(|primary| primary.tracker().known(CopyId(2)))
        .expect("node 1 tracks copy 2");
    assert!(known.durable.0 >= cutoff.0, "{known:?} below {cutoff:?}");
    trio.ready();
    match trio.ask(put(request)) {
        Answer::Txn { result, .. } => assert_eq!(result.generation, promoted),
        other => panic!("a put after the promote: {other:?}"),
    }

    // The landing that met the tail linked nothing, by name, at the shadow's generation.
    let refused: Vec<_> = logged!(METHOD, "land_link_refused")
        .into_iter()
        .filter(|line| line["node"] == 3)
        .map(|line| (line["reason"].clone(), line["generation"].clone()))
        .collect();
    assert_eq!(
        refused,
        [("held_past_base".into(), shadowed.0.into())],
        "node 3 met the cutoff with its tail"
    );
    // So the tail never joined the new generation: it is not linked onto the tail's.
    assert!(
        trio.nodes[2]
            .0
            .engine
            .link(PartitionId(1), shadowed)
            .is_none_or(|link| link.parent != held),
        "generation {shadowed:?} linked onto the tail's {held:?}"
    );
    let promotes = logged!(METHOD, "shadow_promote");
    assert_eq!(promotes.len(), 1, "one promote: {promotes:?}");
    assert!(
        promotes[0]["acked"].as_u64() >= promotes[0]["cutoff"].as_u64(),
        "promoted below the cutoff: {promotes:?}"
    );
}
