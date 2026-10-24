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
/// nothing and reads absent, or had an unknown outcome that a status query settles.
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
                assert!(
                    matches!(
                        error.retry,
                        RetryRule::QueryStatus | RetryRule::RetryAfterRecovery
                    ),
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
