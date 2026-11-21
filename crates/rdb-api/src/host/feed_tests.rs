//! #170 review r2: a secondary's change feed against its control read of `partitions/1`, on
//! three hosts stepped by hand. The trio's clock is held and moved to the millisecond, node 2
//! alone is stepped, and its control-read answers (`Msg::FeedRead`) are kept back until a row
//! hands them over, so each row says exactly when the read lands.

use super::barrier_tests::written;
use super::trio::Trio;
use super::*;
use rdb_core::contracts::ids::ClientId;
use rdb_core::RetryRule;

const P: PartitionId = PartitionId(1);

fn changes(request: u64) -> ClientCall {
    ClientCall::Changes {
        identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(request),
        },
        from: FeedCursor::START,
        max: 10,
    }
}

/// Whether `host` serves the feed as a regular secondary at its newest generation, holding
/// `through`.
fn serves(host: &Host, through: Seq) -> bool {
    let Some(result) = host.committed.get(&P) else {
        return false;
    };
    let newest = result.new_generation;
    let regular = result
        .committed
        .pinned_config
        .members
        .iter()
        .any(|member| member.node == host.node && member.role == ReplicaRole::RegularSecondary);
    regular
        && host.engine.staging(P, newest).is_none()
        && host
            .replication
            .receiver(host.node, P)
            .is_some_and(|receiver| receiver.lineage().generation == newest)
        && host.engine.buffered_applied(P, newest).0 >= through.0
}

/// A ready trio with one put, held by node 2 as a regular secondary, and that put.
fn serving(dir: &std::path::Path) -> (Trio, Generation, Seq) {
    let mut trio = Trio::new(dir, Trio::fast);
    trio.bootstrap();
    trio.ready();
    let (generation, seq) = written(&mut trio, 1, b"k1");
    trio.until("node 2 serves the feed, holding k1", |trio| {
        serves(&trio.nodes[1].0, seq).then_some(())
    });
    (trio, generation, seq)
}

/// One round of node 2 alone, as [`Trio::step`] runs it, its control-read answers kept in
/// `held` instead of handled.
fn step_two(trio: &mut Trio, held: &mut Vec<Msg>) {
    let (host, _, rx) = &mut trio.nodes[1];
    host.run_due();
    host.drain();
    while let Ok(msg) = rx.try_recv() {
        if matches!(msg, Msg::FeedRead { .. }) {
            held.push(msg);
        } else {
            host.handle(msg);
        }
    }
}

/// Step node 2 until `n` control-read answers are held. Real time passes (the read runs on the
/// runtime); the held clock does not.
fn until_held(trio: &mut Trio, held: &mut Vec<Msg>, n: usize) {
    let deadline = Instant::now() + test_patience(Trio::PATIENCE);
    while held.len() < n {
        assert!(
            Instant::now() < deadline,
            "{} reads held, not {n}",
            held.len()
        );
        step_two(trio, held);
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// A feed call sent to node 2.
fn call_two(trio: &Trio, call: ClientCall) -> Receiver<Answer> {
    let (reply, answer) = mpsc::channel();
    trio.nodes[1]
        .1
        .send(Msg::Client(Client {
            partition: P,
            call,
            reply,
        }))
        .expect("node 2's mailbox");
    answer
}

fn feed_state(trio: &Trio) -> (bool, usize) {
    trio.nodes[1]
        .0
        .feed_reads
        .get(&P)
        .map_or((false, 0), |entry| (entry.in_flight, entry.waiting.len()))
}

/// Review r2 (thread 4647108722): a feed call parked on node 2's control read, which does not
/// land, is answered by node 2's duty at exactly 500 ms of its clock: nothing at 499, the
/// refusal "call again" (`UNAVAILABLE`, `BoundedJitter`, no mutation) at 500. Node 2 is
/// stepped by hand on a held clock; the read's answer is kept back.
/// Cuts: `FEED_READ_WAIT_MILLIS` 501, red (no answer at 500); 499, red (answered at 499).
/// Integration (~2 s): bootstrap's discovery window, then steps on a held clock.
#[config_log::retcd_test]
fn f87_2_r2_a_parked_feed_call_is_refused_at_exactly_500_ms() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _, _) = serving(dir.path());
    let hold = trio.clock.hold();
    let mut held = Vec::new();
    let parked_at = trio.clock.now();
    let answer = call_two(&trio, changes(10));
    step_two(&mut trio, &mut held);
    assert_eq!(feed_state(&trio), (true, 1), "one call parked on one read");

    hold.advance(499);
    step_two(&mut trio, &mut held);
    assert_eq!(trio.clock.now().0 - parked_at.0, 499);
    assert!(
        matches!(answer.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "answered before 500 ms"
    );
    assert_eq!(feed_state(&trio), (true, 1), "still parked at 499 ms");

    hold.advance(1);
    step_two(&mut trio, &mut held);
    assert_eq!(trio.clock.now().0 - parked_at.0, 500);
    match answer.try_recv() {
        Ok(Answer::Error { error, .. }) => {
            assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
            assert_eq!(error.retry, RetryRule::BoundedJitter, "{error:?}");
            assert!(error.no_mutation, "{error:?}");
            assert!(error.detail.contains("call again"), "{error:?}");
        }
        other => panic!("not refused at 500 ms: {other:?}"),
    }
    assert_eq!(feed_state(&trio), (true, 0), "the read is still in flight");
}

/// Review r2 (the half-grant blocker): node 2 holds a control read begun at t0. A call before
/// t0 + grant/2 is answered from it and starts no read; a call at exactly t0 + grant/2 is
/// answered from it at once, in the same step, and starts the next read, which then lands
/// stamped with that tick.
/// Cuts: refresh at the full grant (`age >= grant`), red (no read in flight at the half);
/// fresh only below half the grant, red (the caller at the half is not answered).
/// Integration (~2 s): bootstrap's discovery window, then steps on a held clock.
#[config_log::retcd_test]
fn f87_2_r2_at_half_the_grant_the_next_read_starts_and_the_caller_is_served_cached() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _, _) = serving(dir.path());
    let hold = trio.clock.hold();
    let mut held = Vec::new();
    let t0 = trio.clock.now();
    let first = call_two(&trio, changes(10));
    until_held(&mut trio, &mut held, 1);
    for msg in held.drain(..) {
        trio.nodes[1].0.handle(msg);
    }
    assert!(
        matches!(first.try_recv(), Ok(Answer::Changes(_))),
        "the first call is served once its read lands"
    );
    assert_eq!(feed_state(&trio), (false, 0));
    let grant = trio.nodes[1].0.budgets.grant_millis;

    hold.advance(grant / 2 - 1);
    let before = call_two(&trio, changes(11));
    step_two(&mut trio, &mut held);
    assert!(
        matches!(before.try_recv(), Ok(Answer::Changes(_))),
        "served from the cache before the half"
    );
    assert_eq!(feed_state(&trio), (false, 0), "no read before the half");

    hold.advance(1);
    let at_half = call_two(&trio, changes(12));
    step_two(&mut trio, &mut held);
    assert!(
        matches!(at_half.try_recv(), Ok(Answer::Changes(_))),
        "served from the cache at the half, in the step that starts the next read"
    );
    assert_eq!(feed_state(&trio), (true, 0), "the next read is in flight");
    until_held(&mut trio, &mut held, 1);
    match &held[0] {
        Msg::FeedRead { started, read, .. } => {
            assert_eq!(started.0 - t0.0, grant / 2, "the read began at the half");
            assert!(read.is_ok(), "{read:?}");
        }
        other => panic!("held {other:?}"),
    }
}

/// Review r2 (the two-caller damage blocker): node 2's record is cut one byte short, and two
/// feed calls are parked on one control read, counted on node 2 itself (two waiters), not
/// guessed with a sleep. When the read lands, the first answer faults the node and the
/// second caller, still parked, hears the same: both `CORRUPT_HISTORY` with the feed's step
/// once, claiming no mutation. Then a later feed call hears it with the step and a status
/// call without it.
/// Cuts: `answer_feed_waiters` dropping the rest at the fault, red (caller 1 hears nothing);
/// no `feed_reads` drain in `Host::set_fault`, red (the same).
/// Integration (~2 s): bootstrap's discovery window, then steps on a held clock.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn f87_2_r2_two_parked_feed_callers_both_hear_the_damage_fault() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, generation, seq) = serving(dir.path());
    trio.nodes[1]
        .0
        .engine
        .damage_record_as(
            P,
            generation,
            Namespace::History,
            &seq.0.to_be_bytes(),
            rdb_storage::RecordDamage::Truncate,
        )
        .expect("node 2's record cut short");
    let _hold = trio.clock.hold();
    let mut held = Vec::new();
    let callers = [call_two(&trio, changes(10)), call_two(&trio, changes(11))];
    step_two(&mut trio, &mut held);
    assert_eq!(
        feed_state(&trio),
        (true, 2),
        "two callers parked on one read"
    );
    until_held(&mut trio, &mut held, 1);
    for msg in held.drain(..) {
        trio.nodes[1].0.handle(msg);
    }
    assert!(trio.nodes[1].0.fault.is_some(), "node 2 faulted");
    let typed = |who: &str, answer: Result<Answer, mpsc::TryRecvError>| match answer {
        Ok(Answer::Error { error, .. }) => {
            assert_eq!(error.kind, ErrorKind::CorruptHistory, "{who}: {error:?}");
            assert!(
                error.detail.ends_with(FEED_HISTORY_STEP),
                "{who}: {error:?}"
            );
            assert_eq!(error.detail.matches("next:").count(), 1, "{who}: {error:?}");
            assert!(error.no_mutation, "{who}: {error:?}");
        }
        other => panic!("{who}: {other:?}"),
    };
    for (index, caller) in callers.iter().enumerate() {
        typed(&format!("caller {index}"), caller.try_recv());
    }
    let later = call_two(&trio, changes(12));
    step_two(&mut trio, &mut held);
    typed("a later feed call", later.try_recv());
    let status = call_two(
        &trio,
        ClientCall::Status {
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(13),
            },
            generation: None,
        },
    );
    step_two(&mut trio, &mut held);
    match status.try_recv() {
        Ok(Answer::Error { error, .. }) => {
            assert_eq!(error.kind, ErrorKind::CorruptHistory, "{error:?}");
            assert!(!error.detail.contains(FEED_HISTORY_STEP), "{error:?}");
        }
        other => panic!("the status call: {other:?}"),
    }
}

/// Review r3/r4 (threads 4654331881, 4654676431): a feed call parked on node 2's control read,
/// which never lands, is answered by node 2's clean stop: `UNAVAILABLE`, `BoundedJitter`,
/// nothing written, the feed's other-copy step; node 2 is not faulted, and logs
/// `feed_waiter_stopped`. Runs the run loop's own stop tail, [`Host::stop_tail`]. Not shown here:
/// that the answer precedes the stop's disk work (this trio's final marks write nothing).
/// Cut: no `stop_feed_waiters` in the stop tail, red (the call is never answered).
/// Integration (~2 s): bootstrap's discovery window, then one stop.
#[config_log::retcd_test]
fn f87_3_a_clean_stop_answers_a_parked_feed_call() {
    const METHOD: &str = "f87_3_a_clean_stop_answers_a_parked_feed_call";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _, _) = serving(dir.path());
    let _hold = trio.clock.hold();
    let mut held = Vec::new();
    let answer = call_two(&trio, changes(11));
    step_two(&mut trio, &mut held);
    assert_eq!(feed_state(&trio), (true, 1), "one call parked on one read");

    let (host, _, rx) = &mut trio.nodes[1];
    host.stop_tail(rx);
    assert!(
        host.fault.is_none(),
        "a clean stop faulted node 2: {:?}",
        host.fault
    );
    match answer.try_recv() {
        Ok(Answer::Error { error, request }) => {
            assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
            assert_eq!(error.retry, RetryRule::BoundedJitter, "{error:?}");
            assert!(error.no_mutation, "{error:?}");
            assert!(request.is_none(), "{request:?}");
            assert!(error.detail.contains("node 2 is stopping"), "{error:?}");
            assert!(error.detail.ends_with(FEED_OTHER_COPY_STEP), "{error:?}");
        }
        other => panic!("the stop left the parked call unanswered: {other:?}"),
    }
    assert_eq!(feed_state(&trio).1, 0, "nothing still waits");

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    assert!(
        lines
            .iter()
            .any(|line| line["@m"] == "feed_waiter_stopped" && line["node"] == 2),
        "feed_waiter_stopped is not logged"
    );
}

/// Hand node 2 its held control-read answers, each replaced by `read` when that is given.
fn land(trio: &mut Trio, held: &mut Vec<Msg>, read: Option<Result<FeedControlRead, String>>) {
    for mut msg in held.drain(..) {
        if let (Msg::FeedRead { read: landed, .. }, Some(read)) = (&mut msg, &read) {
            *landed = read.clone();
        }
        trio.nodes[1].0.handle(msg);
    }
}

/// The failure a refusal repeats, between "the last read failed: " and its step.
fn failure_named(answer: Result<Answer, mpsc::TryRecvError>) -> String {
    match answer {
        Ok(Answer::Error { error, .. }) => {
            assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
            assert!(error.detail.contains("so call again"), "{error:?}");
            error
                .detail
                .split("the last read failed: ")
                .nth(1)
                .and_then(|rest| rest.split("; next: ").next())
                .unwrap_or_else(|| panic!("names no failure: {error:?}"))
                .to_owned()
        }
        other => panic!("not refused: {other:?}"),
    }
}

/// Review r5 (thread 4655340435): a failed control read's error is repeated to the parked
/// feed call as its first [`FEED_READ_FAILURE_CHARS`] characters, counted as characters, not
/// bytes, then "..." when anything was left out. 200 two-byte `é` then 50 `x` give the 200 `é`
/// and "..."; exactly 200 `ü` give all 200 and no "...".
/// Cuts: no `take` (the whole error), red (the `x`s are repeated); `...` always appended, red
/// (the 200-character error gains one).
/// Integration (~2 s): bootstrap's discovery window, then steps on a held clock.
#[config_log::retcd_test]
fn f87_4_a_failed_read_is_repeated_as_at_most_200_characters() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _, _) = serving(dir.path());
    let _hold = trio.clock.hold();
    let mut held = Vec::new();

    let long = format!("{}{}", "é".repeat(200), "x".repeat(50));
    let answer = call_two(&trio, changes(10));
    until_held(&mut trio, &mut held, 1);
    land(&mut trio, &mut held, Some(Err(long)));
    assert_eq!(
        failure_named(answer.try_recv()),
        format!("{}...", "é".repeat(200))
    );

    let exact = "ü".repeat(200);
    let answer = call_two(&trio, changes(11));
    until_held(&mut trio, &mut held, 1);
    land(&mut trio, &mut held, Some(Err(exact.clone())));
    assert_eq!(failure_named(answer.try_recv()), exact);
}
