//! Issue #80 S5 (R4-1): a copy that answered a recovery inventory refuses the generation it
//! reported, and acknowledges nothing more of it (design section 5.4, rows 26 to 30).

use super::trio::{logged, Trio};
use super::*;
use crate::net::faults::{CutStore, Faults};
use rdb_core::contracts::ids::{ClientId, RequestId, TenantId};
use rdb_core::contracts::recovery::RecoveryResult;
use rdb_core::contracts::txn::Outcome;
use rdb_value::delta::Op;

const INSTANCE: [u8; INSTANCE_LEN] = [7; INSTANCE_LEN];
/// Node 1's tick behind everyone else's: past the 2(ε + δ) = 400 ms limit, so A1 on node 1
/// still admits after node 2's proof. A held-back clock is one way the design's frozen-tick
/// condition (a VM pause, or skew past the limit) can be reached.
const A_LAG_MILLIS: u64 = 1_000;
/// Short enough that the row waits on the grant in about a second; long enough that node 1
/// still admits for about 600 ms after the proof, at this lag.
const GRANT_MILLIS: u64 = 1_500;
const RENEW_MILLIS: u64 = 300;
/// Long enough that no renewal fires for the rest of the row (design row 26, R7).
const NO_RENEWAL_MILLIS: u64 = 3_600_000;

fn identity(request: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(request),
    }
}

pub(super) fn put(request: u64, object: &'static [u8], millis: u64) -> ClientCall {
    ClientCall::Put {
        identity: identity(request),
        object: Bytes::from_static(object),
        change: Change::Doc(Delta(vec![Op::Replace(Value::Bytes(b"v".to_vec()))])),
        expect: None,
        remaining_millis: millis,
    }
}

/// Bound to an instance, so F1 writes the rooted layout `admin::promote` reads; node 1 lagging.
fn tuned(host: &mut Host) {
    Trio::fast(host);
    host.engine.bind_instance(INSTANCE).expect("bind instance");
    host.instance = Some(INSTANCE);
    host.budgets.grant_millis = GRANT_MILLIS;
    host.budgets.renew_millis = RENEW_MILLIS;
    if host.node == NodeId(1) {
        host.clock = host.clock.lagging(A_LAG_MILLIS);
    }
}

/// One round for the hosts at `nodes` only: the others hear nothing and run nothing.
fn step_only(trio: &mut Trio, nodes: &[usize]) {
    for &at in nodes {
        let (host, _, rx) = &mut trio.nodes[at];
        host.run_due();
        host.drain();
        while let Ok(msg) = rx.try_recv() {
            host.handle(msg);
        }
        assert_eq!(host.fault, None, "node {} faulted", host.node.0);
    }
}

/// Step `nodes` until `done` answers, or fail naming `what` after [`Trio::PATIENCE`].
pub(super) fn only_until<T>(
    trio: &mut Trio,
    nodes: &[usize],
    what: &str,
    mut done: impl FnMut(&mut Trio) -> Option<T>,
) -> T {
    let patience = test_patience(Trio::PATIENCE);
    let deadline = Instant::now() + patience;
    loop {
        step_only(trio, nodes);
        if let Some(found) = done(trio) {
            return found;
        }
        assert!(Instant::now() < deadline, "not within {patience:?}: {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The promote run for real, on the Trio's runtime, as `Db::promote` runs it.
pub(super) fn spawn_promote(
    trio: &Trio,
    to: u32,
) -> tokio::task::JoinHandle<Result<crate::admin::Promoted, crate::admin::PromoteError>> {
    spawn_promote_dropping(trio, to, None)
}

/// [`spawn_promote`], dropping `drop` from the membership.
pub(super) fn spawn_promote_dropping(
    trio: &Trio,
    to: u32,
    drop: Option<NodeId>,
) -> tokio::task::JoinHandle<Result<crate::admin::Promoted, crate::admin::PromoteError>> {
    let at = usize::try_from(to - 1).expect("a trio node");
    let (members, members_version) = crate::admin::promote_members(
        PartitionId(1),
        NodeId(to),
        Some(trio.status(at)),
        Duration::from_secs(1),
    )
    .expect("members");
    let links = Arc::clone(&trio.links);
    let posted = Arc::clone(&trio.links);
    let ctx = crate::admin::PromoteCtx {
        store: Arc::clone(&trio.store),
        budgets: trio.nodes[0].0.budgets,
        clock: trio.clock,
        partition: PartitionId(1),
        to: NodeId(to),
        drop,
        owner_must_be_dead: false,
        look_within: Duration::from_secs(1),
        within: test_patience(Duration::from_secs(10)),
        members,
        members_version,
        member_boot: Arc::new(move |node: NodeId| links.boot_of(node)),
        send: Arc::new(move |node: NodeId, msg: Msg| {
            posted.post_local(node, msg).map_err(|_| NodeStopped(node))
        }),
    };
    trio.rt.spawn(crate::admin::promote(ctx))
}

/// Put node 1's control store behind a cut the row can apply, as `lr cut-control 1` does.
pub(super) fn control_behind_a_cut(trio: &mut Trio) -> Arc<Faults> {
    let faults = Faults::new();
    let cut: Arc<dyn config_core::ConfigStore> =
        CutStore::new(Arc::clone(&trio.store), Arc::clone(&faults));
    trio.nodes[0].0.control =
        ControlAdapter::new(cut, trio.rt.handle().clone(), Arc::clone(&trio.links));
    faults
}

/// Three bootstrapped hosts with node 1 lagging, and its control store behind a cut.
fn formed(dir: &std::path::Path) -> (Trio, Arc<Faults>) {
    let mut trio = Trio::new(dir, tuned);
    let faults = control_behind_a_cut(&mut trio);
    trio.rt
        .block_on(crate::admin::count_open(
            &*trio.store,
            &INSTANCE,
            true,
            crate::admin::COUNT_WITHIN,
        ))
        .expect("the open counter");
    trio.bootstrap();
    trio.ready();
    (trio, faults)
}

/// Let node 1's next renewal land, then stop its renewals (row 26's R7), so its grant has a
/// fresh window when the row's schedule starts.
///
/// The renewal is awaited under the normal budget and the budget raised only after it lands.
/// Raising it first (PR #104 review, B1) lost the precondition whenever a read of `grants/1`
/// came back before the armed renewal fired: the trio's store serves no watches, so A1 reads the
/// record after each refused watch, and a record no newer than the one held re-arms `Renew` at
/// `renewed_at + renew_millis` (`Authority::on_held_grant_record`) with the raised budget. No
/// renewal landed, the old wait was then met only by the `Expired` fence's view (its
/// `valid_through` is one tick before the fence), and the promote started after node 1 had
/// already fenced. Every s5_26, s5_26b and s5d_39 failure in the it8 runs had that read.
fn renewal_landed_then_no_more(trio: &mut Trio) {
    let renewed_at = |trio: &Trio| trio.nodes[0].0.authority.view().renewed_at;
    let before = renewed_at(trio);
    trio.until("node 1's next renewal lands", |trio| {
        (renewed_at(trio) > before).then_some(())
    });
    trio.nodes[0].0.budgets.renew_millis = NO_RENEWAL_MILLIS;
    assert_eq!(
        trio.status(0).admits,
        Some(true),
        "node 1 still admits after its last renewal"
    );
    tracing::info!(
        grant_left_ms = grant_left(trio),
        "seal_row_last_renewal_admits"
    );
}

/// Counted milliseconds left on node 1's grant, by node 1's own clock.
fn grant_left(trio: &Trio) -> Option<u64> {
    let host = &trio.nodes[0].0;
    let now = host.clock.now().0;
    host.authority_views
        .get(&PartitionId(1))
        .map(|view| view.valid_through_tick.0.saturating_sub(now))
}

/// The row 26 schedule's result, everything F1 and the promote have finished.
struct Late {
    trio: Trio,
    /// The client's answer to the put node 1 took after node 3 answered the inventory.
    answer: Answer,
    /// Node 2's commit of the new generation.
    committed: RecoveryResult,
    /// The checkpoints A1 on node 1 admitted, oldest first, from the put's dispatch on.
    admitted: Vec<String>,
    /// Node 3's applied head when the put had its answer, or was refused.
    copy_applied: Option<u64>,
}

/// Whether node 3 logged refusing a plain append because it is sealed.
fn copy_refused(method: &str) -> bool {
    logged!(method, "r1.append_refused_sealed")
        .iter()
        .any(|line| line["from"] == 1)
}

/// Issue #80 R4-1, on three real hosts. Node 1's tick lags past the limit, and its armed renewal
/// has landed, so it has grant left while node 2's proof holds. It is cut from the store, so
/// it cannot learn of the freeze: a paused or partitioned owner. Node 2 promotes for real and
/// node 3 answers (or, on `refuse`, fails) the inventory. Only then does a put reach node 1,
/// with only nodes 1 and 3 stepped, so node 3 is the one copy that can acknowledge it.
fn late_put(method: &'static str, dir: &std::path::Path, refuse: bool) -> Late {
    let (mut trio, faults) = formed(dir);
    assert!(
        matches!(trio.ask(put(1, b"a", 5_000)), Answer::Txn { .. }),
        "the first put is answered before the schedule starts"
    );
    renewal_landed_then_no_more(&mut trio);
    faults.apply("cut-control");
    trio.nodes[2].0.refuse_inventories = refuse;

    let promote = spawn_promote(&trio, 2);
    let reported = if refuse {
        "inventory_failed"
    } else {
        "inventory_reported"
    };
    only_until(&mut trio, &[1, 2], "node 3 answered the inventory", |_| {
        logged!(method, reported)
            .iter()
            .any(|line| line["node"] == 3)
            .then_some(())
    });
    let admitted_before = checks_admitted(method).len();

    let answer = trio.call(put(2, b"b", 1_000));
    let early = only_until(
        &mut trio,
        &[0, 2],
        "node 1 answered the put, or node 3 refused it",
        |_| match answer.try_recv() {
            Ok(got) => Some(Some(got)),
            Err(_) if copy_refused(method) => Some(None),
            Err(_) => None,
        },
    );
    let copy_applied = trio.status(2).holds.map(|held| held.applied);
    let answer = early.unwrap_or_else(|| trio.answer(&answer));
    trio.nodes[2].0.refuse_inventories = false;

    let committed = trio.until("node 2 committed the new generation", |trio| {
        trio.nodes[1]
            .0
            .committed
            .get(&PartitionId(1))
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
    let admitted = checks_admitted(method).split_off(admitted_before);
    Late {
        trio,
        answer,
        committed: *committed,
        admitted,
        copy_applied,
    }
}

/// Row 39 of #80 S5d, on row 26's schedule: node 1's clock lags and it still holds grant, node 3
/// has answered the recovery's inventory, and node 1's barrier over the first put is never `Ok`,
/// because node 3 refuses to prove more once it is sealed.
/// Integration (~4 s): the promote waits out the grant.
#[config_log::retcd_test]
fn s5d_39_a_barrier_from_the_old_owner_after_the_seal_is_never_ok() {
    const METHOD: &str = "s5d_39_a_barrier_from_the_old_owner_after_the_seal_is_never_ok";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, faults) = formed(dir.path());
    let (generation, through) = super::barrier_tests::written(&mut trio, 1, b"a");
    renewal_landed_then_no_more(&mut trio);
    faults.apply("cut-control");

    let promote = spawn_promote(&trio, 2);
    only_until(&mut trio, &[1, 2], "node 3 answered the inventory", |_| {
        logged!(METHOD, "inventory_reported")
            .iter()
            .any(|line| line["node"] == 3)
            .then_some(())
    });

    // The precondition is a deadline on the counted clock: node 1 admits only until its grant
    // runs out, about 570 ms after node 3's inventory (567 to 574 ms in 22 runs, PR #104 it10).
    // Held, the clock reads one tick until node 1 has taken the call, so the admission checked
    // here is A1's verdict at the barrier, however long the thread stalls in between.
    let hold = trio.clock.hold();
    assert_eq!(
        trio.status(0).admits,
        Some(true),
        "precondition lost: node 1 stopped admitting before the barrier ({:?} ms of grant left)",
        grant_left(&trio)
    );
    tracing::info!(grant_left_ms = grant_left(&trio), "s5d_39_barrier_admits");
    let answer = trio.call(super::barrier_tests::wait_durable(generation, through, 400));
    step_only(&mut trio, &[0]);
    assert!(
        logged!(METHOD, "durable_barrier_started")
            .iter()
            .any(|line| line["node"] == 1),
        "node 1 started the barrier at the tick it admitted at"
    );
    drop(hold);
    let got = only_until(&mut trio, &[0, 2], "node 1 answered the barrier", |_| {
        answer.try_recv().ok()
    });
    assert!(!matches!(got, Answer::Durable), "{got:?}");
    // PR #104 review: a node 1 whose grant already stopped admitting refuses the barrier
    // before asking anyone, so node 3 never sees it. That is row 26's precondition lost, not
    // this row's claim failing. `renewal_landed_then_no_more` closes one way to lose it: no
    // renewal landing before the schedule, so the grant ran out before the promote. The hold
    // above closes the other: the time from node 3's inventory to node 1 taking the call.
    if let Answer::Error { error, .. } = &got {
        assert!(
            !error.detail.contains("no barrier was started"),
            "precondition lost: node 1's grant stopped admitting before the barrier, so node 3 \
             was never asked: {got:?}"
        );
    }
    let lines = logged!(METHOD, "durable_through_refused");
    assert!(
        lines
            .iter()
            .any(|line| line["node"] == 3 && line["reason"] == "sealed"),
        "{lines:?}"
    );
    trio.until("the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");
}

/// The checkpoints A1 on node 1 admitted so far, oldest first.
fn checks_admitted(method: &str) -> Vec<String> {
    logged!(method, "authority_check_admitted")
        .iter()
        .filter(|line| line["node"] == 1)
        .map(|line| line["checkpoint"].as_str().expect("a name").to_owned())
        .collect()
}

/// Read object `object` from node 2, the new owner, once it admits.
fn read_on_new_owner(trio: &mut Trio, object: &'static [u8]) -> Answer {
    trio.until("node 2 admits", |trio| {
        (trio.status(1).admits == Some(true)).then_some(())
    });
    let (reply, answer) = mpsc::channel();
    trio.nodes[1]
        .1
        .send(Msg::Client(Client {
            partition: PartitionId(1),
            call: ClientCall::Get {
                identity: identity(9),
                group: AffinityId(1),
                object: Bytes::from_static(object),
                look: Look::Doc,
            },
            reply,
        }))
        .expect("node 2's mailbox");
    trio.answer(&answer)
}

/// What rows 26 and 26b assert of the schedule's result.
fn assert_refused_and_nothing_lost(method: &str, late: Late) {
    let Late {
        mut trio,
        answer,
        committed,
        admitted,
        copy_applied,
    } = late;
    let published = matches!(
        &answer,
        Answer::Txn { result, .. } if result.outcome == Outcome::Published
    );
    let after = read_on_new_owner(&mut trio, b"b");
    let absent = matches!(
        after,
        Answer::Read {
            seen: Seen::Absent,
            ..
        }
    );
    assert!(
        !published,
        "node 1's client heard success (absent on the new owner: {absent}), cutoff {:?}, A1 \
         admitted {admitted:?}: {answer:?}",
        committed.selected.cutoff_seq
    );
    assert!(
        matches!(&answer, Answer::Error { error, .. } if error.kind == ErrorKind::UnknownOutcome),
        "node 1 can only say it does not know: {answer:?}"
    );
    assert!(
        copy_refused(method),
        "node 3 was delivered node 1's frame and refused it as sealed"
    );
    assert_eq!(
        copy_applied,
        Some(2),
        "node 3 applied nothing of the old generation after it answered"
    );
    assert_eq!(
        admitted.first().map(String::as_str),
        Some("StorageDispatch"),
        "A1 was still admitting after node 2's proof, so the seal is what refused it: {admitted:?}"
    );
    assert!(
        !admitted
            .iter()
            .any(|check| check == "Publication" || check == "Reply"),
        "with no qualifying ack, no publication or reply check was admitted: {admitted:?}"
    );
    assert!(
        logged!(method, "authority_check_denied")
            .iter()
            .all(|line| line["checkpoint"] != "StorageDispatch"),
        "A1 never denied the dispatch"
    );
    assert!(
        absent,
        "the put was never acknowledged and is not in the new history: {after:?}"
    );
}

/// Row 26 (R4-1), end to end. A copy that has answered the new owner's inventory refuses the
/// old generation, so the old owner cannot get a client success for a record the new history
/// drops. The schedule reaches A1 admitting after node 2's proof; without the seal node 3
/// acknowledges, A1 admits `Publication` and `Reply`, the client hears `Published`, and F1's
/// cutoff drops the record. Integration, ~4 s.
#[config_log::retcd_test]
fn s5_26_a_copy_that_answered_an_inventory_refuses_the_old_generation() {
    const METHOD: &str = "s5_26_a_copy_that_answered_an_inventory_refuses_the_old_generation";
    let dir = config_testkit::fs::temp_dir();
    let late = late_put(METHOD, dir.path(), false);
    assert_refused_and_nothing_lost(METHOD, late);
}

/// Row 26's cut in row form: the seal runs before the head read, so an inventory that fails
/// seals too. Red with the seal moved into `Host::inventory`'s `Ok` arm. Integration, ~4 s.
#[config_log::retcd_test]
fn s5_26b_a_copy_whose_inventory_failed_refuses_the_old_generation_too() {
    const METHOD: &str = "s5_26b_a_copy_whose_inventory_failed_refuses_the_old_generation_too";
    let dir = config_testkit::fs::temp_dir();
    let late = late_put(METHOD, dir.path(), true);
    assert_refused_and_nothing_lost(METHOD, late);
}

/// Row 28 on real hosts: node 3 is behind when it answers the inventory, so it is sealed and F1
/// must catch it up three records through recovery appends. Those pass, it acknowledges them,
/// and F1 lands at the longest head with node 3 holding it. Cut: the seal check above the
/// credential branch, red (F1 blocks). A withheld credentialed ACK does not stop F1 here, so
/// only the receiver row `s5_28_*` sees `on_committed` passing `false`.
/// Integration, ~4 s.
#[config_log::retcd_test]
fn s5_28b_recovery_catches_up_a_sealed_copy_and_lands() {
    const METHOD: &str = "s5_28b_recovery_catches_up_a_sealed_copy_and_lands";
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, faults) = formed(dir.path());
    trio.links.hold(NodeId(1), NodeId(3));
    for (request, object) in [(1, b"a"), (2, b"b"), (3, b"c")] {
        let object: &'static [u8] = object;
        assert!(matches!(
            trio.ask(put(request, object, 5_000)),
            Answer::Txn { .. }
        ));
    }
    renewal_landed_then_no_more(&mut trio);
    faults.apply("cut-control");

    let promote = spawn_promote(&trio, 2);
    let committed = trio.until("node 2 committed the new generation", |trio| {
        trio.nodes[1]
            .0
            .committed
            .get(&PartitionId(1))
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
    let behind = logged!(METHOD, "inventory_reported")
        .iter()
        .find(|line| line["node"] == 3)
        .map(|line| line["head"].clone());
    assert_eq!(
        behind,
        Some(serde_json::json!(1)),
        "node 3 reported a head behind"
    );
    assert_eq!(committed.selected.cutoff_seq, Seq(4));
    trio.until("node 3 caught up in the new generation", |trio| {
        trio.status(2)
            .holds
            .filter(|held| held.generation == committed.new_generation && held.applied == 4)
            .map(|_| ())
    });
}

/// Row 30: the owner loses its grant and re-acquires it, then re-plans for a copy that
/// restarted. Its inventory seals both copies, the plan commits the next generation on the same
/// node, and writes resume. Cut: `sealed()` reading only that a seal was set, red (no copy
/// acknowledges the new generation). Integration, ~5 s.
#[config_log::retcd_test]
fn s5_30_a_same_node_recovery_after_a_fence_seals_the_copies_and_writes_resume() {
    const METHOD: &str =
        "s5_30_a_same_node_recovery_after_a_fence_seals_the_copies_and_writes_resume";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    let faults = control_behind_a_cut(&mut trio);
    trio.bootstrap();
    trio.ready();
    assert!(matches!(trio.ask(put(1, b"a", 5_000)), Answer::Txn { .. }));

    faults.apply("cut-control");
    trio.until("node 1 fenced", |trio| trio.status(0).fence.map(|_| ()));
    faults.apply("");
    trio.ready();
    let before = trio.status(0).holds.expect("node 1 holds").generation;

    // Node 3 restarts: the link reports a newer boot, and the owner re-plans.
    let reboot = BootId(trio.nodes[2].0.boot.0 + 1);
    let mailbox = trio.nodes[2].1.clone();
    trio.links.register(NodeId(3), mailbox, reboot);
    for node in 0..2 {
        trio.nodes[node]
            .1
            .send(Msg::PeerBooted {
                node: NodeId(3),
                boot: reboot,
            })
            .expect("node up");
    }
    trio.until("node 2 answered the owner's inventory and sealed", |_| {
        logged!(METHOD, "inventory_reported")
            .iter()
            .any(|line| line["node"] == 2)
            .then_some(())
    });
    let committed = trio.until("node 1 committed the next generation", |trio| {
        trio.nodes[0]
            .0
            .committed
            .get(&PartitionId(1))
            .filter(|result| result.new_generation > before)
            .cloned()
    });
    trio.ready();
    match trio.ask(put(2, b"b", 5_000)) {
        Answer::Txn { result, .. } => {
            assert!(result.generation >= committed.new_generation, "{result:?}");
            assert_eq!(result.outcome, Outcome::Published);
        }
        other => panic!("a put after the recovery: {other:?}"),
    }
    let node2 = trio.nodes[1]
        .0
        .replication
        .receiver(NodeId(2), PartitionId(1));
    assert!(
        node2.is_some_and(|receiver| !receiver.sealed()),
        "the commit moved node 2 above its seal"
    );
}

/// Row 30, the order the design's words name: the copies are already sealed when the owner loses
/// and re-acquires its grant. Node 1's re-plan has node 2's inventory answer in hand, node 3's
/// still held, when node 1 is cut from the store; its grant runs out and it fences. The cut
/// heals, it re-acquires, and the next generation still commits on node 1 and writes resume.
/// Integration, ~6 s.
#[config_log::retcd_test]
fn s5_30b_copies_sealed_before_the_owner_re_acquires_do_not_stop_the_next_commit() {
    const METHOD: &str =
        "s5_30b_copies_sealed_before_the_owner_re_acquires_do_not_stop_the_next_commit";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    let faults = control_behind_a_cut(&mut trio);
    trio.bootstrap();
    trio.ready();
    assert!(matches!(trio.ask(put(1, b"a", 5_000)), Answer::Txn { .. }));
    let before = trio.status(0).holds.expect("node 1 holds").generation;

    trio.links.hold(NodeId(3), NodeId(1));
    let reboot = BootId(trio.nodes[2].0.boot.0 + 1);
    let mailbox = trio.nodes[2].1.clone();
    trio.links.register(NodeId(3), mailbox, reboot);
    for node in 0..2 {
        trio.nodes[node]
            .1
            .send(Msg::PeerBooted {
                node: NodeId(3),
                boot: reboot,
            })
            .expect("node up");
    }
    trio.until("node 2 answered the owner's inventory", |_| {
        logged!(METHOD, "inventory_reported")
            .iter()
            .any(|line| line["node"] == 2)
            .then_some(())
    });
    faults.apply("cut-control");
    trio.until("node 1 fenced", |trio| trio.status(0).fence.map(|_| ()));
    let sealed = trio.nodes[1]
        .0
        .replication
        .receiver(NodeId(2), PartitionId(1))
        .is_some_and(AppendReceiver::sealed);
    assert!(sealed, "node 2 is sealed when the owner re-acquires");
    assert_eq!(
        trio.status(0).holds.expect("node 1 holds").generation,
        before,
        "the next generation had not committed when node 1 fenced"
    );

    faults.apply("");
    trio.links.heal_all();
    let committed = trio.until("node 1 committed the next generation", |trio| {
        trio.nodes[0]
            .0
            .committed
            .get(&PartitionId(1))
            .filter(|result| result.new_generation > before)
            .cloned()
    });
    trio.ready();
    match trio.ask(put(2, b"b", 5_000)) {
        Answer::Txn { result, .. } => {
            assert!(result.generation >= committed.new_generation, "{result:?}");
            assert_eq!(result.outcome, Outcome::Published);
        }
        other => panic!("a put after the recovery: {other:?}"),
    }
}
