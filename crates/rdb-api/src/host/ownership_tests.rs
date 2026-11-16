//! #84 host rows: `still_owner` against the host's own write admission, on the trio's capped
//! clock. Each row reads `still_owner` and submits a put while the clock is held, so both are
//! judged at one tick.

use super::trio::{self, current_thread, Trio};
use super::*;
use crate::ownership::{OwnerToken, OwnerViews};

const P1: PartitionId = PartitionId(1);

fn identity(request: u64) -> RequestIdentity {
    use rdb_core::contracts::ids::{ClientId, TenantId};
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(request),
    }
}

fn put(request: u64) -> ClientCall {
    use rdb_value::delta::Op;
    ClientCall::Put {
        identity: identity(request),
        object: Bytes::from_static(b"o"),
        change: Change::Doc(Delta(vec![Op::Replace(Value::Bytes(
            request.to_le_bytes().to_vec(),
        ))])),
        expect: None,
        remaining_millis: 5_000,
    }
}

/// A short grant, so a lapse comes within a second of the cut.
fn short_grant(host: &mut Host) {
    Trio::fast(host);
    host.budgets.grant_millis = 600;
    host.budgets.renew_millis = 100;
}

/// Node 1's token, once it admits.
fn token(trio: &Trio) -> OwnerToken {
    trio.nodes[0]
        .0
        .owner_views
        .ownership(P1)
        .expect("node 1 owns partition 1 while it admits")
        .token
}

fn still_owner(trio: &Trio, token: OwnerToken) -> bool {
    trio.nodes[0].0.owner_views.still_owner(P1, token)
}

/// The trio time between two probes, at least: the clock is held during each one.
const PROBE_STEP: Duration = Duration::from_millis(2);

/// One probe: with the clock held, `still_owner` is read, a put is submitted and stepped, and
/// `still_owner` is read again, all at one tick. The hosts step while the put waits, so a control
/// answer can fence or re-grant in between (own84_3 saw a `Frozen` fence land there, own84_4 a
/// self-replace): `owner_after` is what `still_owner` said at the end of the held tick.
#[derive(Debug)]
struct Probe {
    at: Tick,
    owner: bool,
    /// `None` when the put was admitted: published, or applied and answered `UNKNOWN_OUTCOME`
    /// (applied before a fence, its answer waits on the post-apply deadline). The error kind
    /// of a refusal otherwise.
    refused: Option<ErrorKind>,
    owner_after: bool,
}

impl Probe {
    /// Never admitted unless `still_owner` read true at the tick, and never refused while it read
    /// true both before and after: a refusal while `still_owner` read true is a fence that came
    /// between, which `still_owner` must already show at the end of the tick.
    fn agrees(&self) -> bool {
        match self.refused {
            None => self.owner || self.owner_after,
            Some(_) => !(self.owner && self.owner_after),
        }
    }
}

/// Steps a probe takes at the held tick, at most, before it has the put's admission verdict.
const HELD_STEPS: usize = 200;

/// Whether node 1's T1 has admitted `identity`: it waits in the queue, is the transaction past
/// it, or a batch was reserved since `seq_before` (only the probe's put writes in these rows).
fn admitted(trio: &Trio, identity: RequestIdentity, seq_before: Option<Seq>) -> bool {
    let Some(kernel) = trio.nodes[0].0.transaction.kernel(NodeId(1), P1) else {
        return false;
    };
    let inflight = match kernel.inflight() {
        Some(
            Inflight::AwaitingDispatchCheck { admitted, .. }
            | Inflight::Dispatched { admitted, .. },
        ) => admitted.req.identity == identity,
        None => false,
    };
    inflight
        || kernel.queued().any(|queued| queued == identity)
        || seq_before.is_some_and(|before| kernel.next_seq() > before)
}

fn probe(trio: &mut Trio, token: OwnerToken, request: u64) -> Probe {
    let hold = trio.clock.hold();
    let at = trio.clock.now();
    let owner = still_owner(trio, token);
    let seq_before = trio.nodes[0]
        .0
        .transaction
        .kernel(NodeId(1), P1)
        .map(TxnKernel::next_seq);
    let answer = trio.call(put(request));
    // The verdict is taken under the hold: an answer at this tick, or T1 holding the put as
    // admitted. An answer that comes after the hold drops is a later step's (the post-apply
    // deadline of a put applied before a fence), not admission's (#84 review).
    let mut verdict = None;
    for _ in 0..HELD_STEPS {
        trio.step();
        if let Ok(found) = answer.try_recv() {
            verdict = Some(Some(found));
            break;
        }
        if admitted(trio, identity(request), seq_before) {
            verdict = Some(None);
            break;
        }
    }
    let owner_after = still_owner(trio, token);
    assert_eq!(trio.clock.now(), at, "the held clock moved");
    let verdict = verdict.unwrap_or_else(|| {
        panic!("put {request}: no admission verdict in {HELD_STEPS} steps at the held tick")
    });
    drop(hold);
    let refused = match verdict {
        None => {
            // Admitted at the held tick; its answer comes when it comes.
            let _ = trio.answer(&answer);
            None
        }
        Some(Answer::Txn { .. }) => None,
        Some(Answer::Error { error, .. }) if error.kind == ErrorKind::UnknownOutcome => None,
        Some(Answer::Error { error, .. }) => Some(error.kind),
        Some(other) => panic!("put {request}: {other:?}"),
    };
    // The held clock counts nothing, so let it count a step before the next probe.
    std::thread::sleep(PROBE_STEP);
    Probe {
        at,
        owner,
        refused,
        owner_after,
    }
}

/// own84_2, #84's acceptance row: under a control cut-off on a stepped clock, `still_owner`
/// turns false no later than the first write refused `LEASE_EXPIRED`, and no write is admitted
/// at a tick where it read false. Every probe reads `still_owner` and submits a put at one held
/// tick, so the comparison is exact: a put refused for the grant while `still_owner` read true
/// is a lagging `still_owner`.
/// Cuts: `owns_at` judging `now - 100` (a δ late), red; judging `now - 1` (one tick late),
/// red 3/3: each a put refused `LEASE_EXPIRED` at a tick `still_owner` read true.
/// Integration (~2 s): bootstrap's discovery window, then one lapse of a 600 ms grant.
#[config_log::retcd_test]
fn own84_2_still_owner_turns_false_no_later_than_writes_stop_under_a_cut_off() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), short_grant);
    trio.bootstrap();
    trio.ready();
    let token = token(&trio);
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    let mut probes = Vec::new();
    let mut request = 1;
    let deadline = Instant::now() + crate::host::test_patience(Trio::PATIENCE);
    // Until five probes after the first refused one, so the row also sees what follows.
    while probes
        .iter()
        .filter(|probe: &&Probe| probe.refused.is_some())
        .count()
        < 5
    {
        assert!(Instant::now() < deadline, "no refusal: {probes:?}");
        probes.push(probe(&mut trio, token, request));
        request += 1;
        trio.step();
    }
    trio.mem.stop_failing();
    assert!(
        probes
            .iter()
            .any(|probe| probe.owner && probe.refused.is_none()),
        "precondition: some put was admitted before the lapse: {probes:?}"
    );
    for probe in &probes {
        assert!(
            probe.agrees(),
            "still_owner disagrees with admission: {probe:?}"
        );
    }
    let first_false = probes
        .iter()
        .find(|probe| !probe.owner || !probe.owner_after)
        .map(|p| p.at);
    let first_refused = probes
        .iter()
        .find(|probe| probe.refused.is_some())
        .expect("a refusal");
    assert_eq!(
        first_refused.refused,
        Some(ErrorKind::LeaseExpired),
        "the cut ends writes on the grant: {first_refused:?}"
    );
    assert!(
        first_false.is_some_and(|t_s| t_s <= first_refused.at),
        "t_s {first_false:?} after t_w {:?}: {probes:?}",
        first_refused.at
    );
}

/// own84_3: a frozen grant (a promote froze it) ends `still_owner` before the host admits
/// another write, and the token is never owned again: a frozen record is never replaced.
/// The freeze is written while the store answers; A1's next renewal finds the record changed,
/// reads it frozen and fences.
/// Integration (~2.5 s): bootstrap's discovery window, then two grant lengths after the freeze.
#[config_log::retcd_test]
fn own84_3_a_frozen_grant_ends_still_owner_before_the_next_admission() {
    use config_core::{ConfigStore as _, GetRequest, MutationOutcome, PutRequest};
    use rdb_core::authority::grant::GrantRecord;
    use rdb_core::contracts::control::ControlKey;

    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), short_grant);
    trio.bootstrap();
    trio.ready();
    let token = token(&trio);
    let key = Bytes::from(ControlKey::Grant(NodeId(1)).encode());
    let found = trio
        .rt
        .block_on(trio.mem.get(GetRequest { key: key.clone() }))
        .expect("get grants/1")
        .record
        .expect("grants/1 exists");
    let mut record = GrantRecord::decode(&found.value).expect("a grant record");
    record.frozen = true;
    let put_frozen = trio
        .rt
        .block_on(trio.mem.put(PutRequest {
            key,
            value: record.encode(),
            expected_mod_revision: Some(found.mod_revision),
            dedup: None,
        }))
        .expect("put grants/1 frozen");
    assert_eq!(put_frozen.outcome, MutationOutcome::Applied);

    let mut probes = Vec::new();
    let mut request = 1;
    let deadline = Instant::now() + crate::host::test_patience(Trio::PATIENCE);
    while !probes.iter().any(|probe: &Probe| probe.refused.is_some()) {
        assert!(Instant::now() < deadline, "no refusal: {probes:?}");
        probes.push(probe(&mut trio, token, request));
        request += 1;
        trio.step();
    }
    for probe in &probes {
        assert!(
            probe.agrees(),
            "still_owner disagrees with admission: {probe:?}"
        );
    }
    // Two grant lengths on: the frozen record is never replaced, so the token stays unowned.
    let until = trio.clock.now().plus_millis(1_200);
    trio.until("two grant lengths after the fence", |trio| {
        assert!(!still_owner(trio, token), "owned again after a freeze");
        assert!(trio.nodes[0].0.owner_views.ownership(P1).is_none());
        (trio.clock.now() >= until).then_some(())
    });
    let after = probe(&mut trio, token, request);
    assert!(
        !after.owner && !after.owner_after && after.refused.is_some(),
        "nothing admitted after a freeze: {after:?}"
    );
}

/// own84_4: a paused process. Node 1 is not stepped while its peers run for twice its grant of
/// the trio's time, the clock moving as they read it. When it runs again, the first read is not
/// owner, and its first put is refused at that tick. With no promote, the node replaces its own
/// grant and the same token is owned again exactly when admission admits again: the two never
/// disagree at one tick. "Never owned again" holds only after a freeze (own84_3).
/// Integration (~2.5 s): bootstrap's discovery window, the pause, then the self-replace.
#[config_log::retcd_test]
fn own84_4_a_paused_process_resumes_not_owning() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), short_grant);
    trio.bootstrap();
    trio.ready();
    let token = token(&trio);
    assert!(
        still_owner(&trio, token),
        "precondition: owned before the pause"
    );
    let resume = trio.clock.now().plus_millis(1_200);
    let deadline = Instant::now() + crate::host::test_patience(Trio::PATIENCE);
    while trio.clock.now() < resume {
        assert!(Instant::now() < deadline, "the pause did not end");
        for (host, _, rx) in &mut trio.nodes[1..] {
            host.run_due();
            host.drain();
            while let Ok(msg) = rx.try_recv() {
                host.handle(msg);
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let first = probe(&mut trio, token, 1);
    assert!(
        !first.owner && !first.owner_after && first.refused.is_some(),
        "the first read after the pause is not owner, and nothing is admitted then: {first:?}"
    );
    let mut request = 2;
    let deadline = Instant::now() + crate::host::test_patience(Trio::PATIENCE);
    loop {
        assert!(Instant::now() < deadline, "node 1 never admitted again");
        trio.step();
        let probe = probe(&mut trio, token, request);
        request += 1;
        assert!(
            probe.agrees(),
            "still_owner disagrees with admission: {probe:?}"
        );
        if probe.refused.is_none() {
            break;
        }
    }
    assert_eq!(
        trio.nodes[0].0.owner_views.ownership(P1).map(|o| o.token),
        Some(token),
        "no promote: the same token is owned again"
    );
}

/// A live view of partition 1 under `token`, through `valid_through`.
fn live_view(token: OwnerToken, valid_through: Tick) -> AuthorityView {
    live_view_of(P1, token, valid_through)
}

/// A live view of `partition` under `token`, through `valid_through`.
fn live_view_of(partition: PartitionId, token: OwnerToken, valid_through: Tick) -> AuthorityView {
    use rdb_core::contracts::authority::DenyReason;
    use rdb_core::contracts::ids::{AuthorityGeneration, BootId, ConfigVersion, GrantId};
    AuthorityView {
        lineage: token.lineage(partition),
        grant_id: GrantId(1),
        boot_id: BootId(1),
        authority_generation: AuthorityGeneration(1),
        config_version: ConfigVersion(1),
        authority_seq: 1,
        valid_through_tick: valid_through,
        past_horizon: DenyReason::Expired,
    }
}

/// own84_6: a terminal fault revokes the host's ownership before any caller hears of it. A
/// waiter answered by `fail_waiter` (a compile or snapshot fault, answered before the node's
/// `set_fault`) already reads both readers false when its answer arrives; `set_fault`, first or
/// repeated, leaves them false; and a view A1 reports after the fault is not kept. Cut: no
/// revoke in `fail_waiter`, red at the first assertion. Removing only `set_fault`'s revoke
/// cannot turn this row red, since `fail_waiter` has already revoked the map: `own84_6b` is that
/// cut's row. The order (revoked before the waiter can read the fault) is `own84_6c`'s.
/// Unit (one host, no cluster).
#[config_log::retcd_test]
fn own84_6_a_terminal_fault_revokes_ownership_before_it_is_answered() {
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let mut host = trio::host_over(dir.path(), &rt);
    let held = OwnerToken {
        generation: Generation(2),
        owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
    };
    let views = Arc::clone(&host.owner_views);
    let live = live_view(held, host.clock.now().plus_millis(60_000));

    // `fail_waiter`, alone: the reply path a compile fault takes before `set_fault`.
    host.stepped_at = host.clock.now();
    host.report_view(&live, false);
    assert!(views.still_owner(P1, held), "precondition: owned");
    let (reply, answer) = mpsc::channel();
    let fault = fail_waiter(&host.owner_views, &reply, "a compile fault".to_owned());
    assert!(
        matches!(answer.try_recv(), Ok(Answer::Error { .. })),
        "the waiter is answered"
    );
    assert!(!views.still_owner(P1, held), "still_owner after the reply");
    assert!(views.ownership(P1).is_none(), "ownership after the reply");

    // `set_fault`, first and repeated, and a view reported after it.
    host.set_fault(fault);
    host.set_fault(Fault::from("a second fault".to_owned()));
    host.report_view(&live, false);
    assert!(!views.still_owner(P1, held), "owned after the fault");
    assert!(views.ownership(P1).is_none());
}

/// own84_6b: `set_fault` alone (a duty or a step that faults, no `fail_waiter` first) revokes
/// ownership. Cut: no revoke in `set_fault`, red. Unit.
#[config_log::retcd_test]
fn own84_6b_set_fault_alone_revokes_ownership() {
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let mut host = trio::host_over(dir.path(), &rt);
    let held = OwnerToken {
        generation: Generation(2),
        owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
    };
    host.stepped_at = host.clock.now();
    host.report_view(
        &live_view(held, host.clock.now().plus_millis(60_000)),
        false,
    );
    assert!(
        host.owner_views.still_owner(P1, held),
        "precondition: owned"
    );
    host.set_fault(Fault::from("a flush fault".to_owned()));
    assert!(!host.owner_views.still_owner(P1, held));
    assert!(host.owner_views.ownership(P1).is_none());
}

/// One probe that must find the partition owned and its write refused at one held tick: what
/// `still_owner` promises not to cover (checks 7 and 8, a read-only recovery). Returns the
/// refusal's kind.
fn owned_but_refused(trio: &mut Trio, token: OwnerToken, request: u64) -> ErrorKind {
    let probe = probe(trio, token, request);
    assert!(
        probe.owner && probe.owner_after,
        "still_owner reads true while the view admits: {probe:?}"
    );
    probe
        .refused
        .unwrap_or_else(|| panic!("the write is refused: {probe:?}"))
}

/// own84_7a: a frozen queue under a live view. A put is applied and left unpublished (both
/// copies' ACKs dropped), a control outage fences node 1, and A1 replaces its own grant: the
/// view admits again but T1's queue still waits on the put (check 7). `still_owner` reads true
/// and the write is refused, at one held tick. Integration (~3 s): bootstrap, then one fence on
/// a 600 ms grant and its self-replace.
#[config_log::retcd_test]
fn own84_7a_a_frozen_queue_under_a_live_view_is_owned_and_refuses_writes() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), short_grant);
    trio.bootstrap();
    trio.ready();
    let token = token(&trio);
    trio.links.drop_direction(NodeId(2), NodeId(1));
    trio.links.drop_direction(NodeId(3), NodeId(1));
    let unacknowledged = trio.call(put(1));
    trio.until("node 1 applies seq 2", |trio| {
        (trio.head() == Some(2)).then_some(())
    });
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    trio.until("node 1 fenced", |trio| trio.status(0).fence.map(|_| ()));
    trio.mem.stop_failing();
    trio.until("node 1 owns again, its queue waiting on seq 2", |trio| {
        // A1 holds before its new view reaches T1 and the map, so wait for the view too.
        let status = trio.status(0);
        (status.authority.starts_with("held(")
            && status.waits_on == Some(Seq(2))
            && still_owner(trio, token))
        .then_some(())
    });
    let kind = owned_but_refused(&mut trio, token, 2);
    assert_eq!(kind, ErrorKind::ProtectionPaused, "check 7's refusal");
    drop(unacknowledged);
}

/// own84_7b: an L1-paused partition under a live grant. Both links from node 1 are held and a
/// put stays unacknowledged past the pause age, so L1 pauses writes (check 8) while A1 still
/// renews. `still_owner` reads true and the write is refused, at one held tick.
/// Integration (~2.5 s): bootstrap, then the 400 ms pause age.
#[config_log::retcd_test]
fn own84_7b_an_l1_paused_partition_under_a_live_grant_is_owned_and_refuses_writes() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast_pause);
    trio.bootstrap();
    trio.ready();
    let token = token(&trio);
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    let unacknowledged = trio.call(put(1));
    trio.until("L1 pauses writes", |trio| {
        (trio.status(0).admits == Some(false)).then_some(())
    });
    let kind = owned_but_refused(&mut trio, token, 2);
    assert_eq!(kind, ErrorKind::ProtectionPaused, "check 8's refusal");
    drop(unacknowledged);
}

/// own84_7c: a read-only recovery under a live view. One host bootstraps with both peers
/// unreachable, so F1 commits `ReadOnly` at cutoff 0, as walk A10 does; A1 holds the grant and
/// publishes a live view. `still_owner` reads true and a write is refused, at one held tick.
/// Integration (~2.5 s): bootstrap's discovery window on one host.
#[config_log::retcd_test]
fn own84_7c_a_read_only_recovery_under_a_live_view_is_owned_and_refuses_writes() {
    let dir = config_testkit::fs::temp_dir();
    let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let links = Links::new();
    let (tx, rx) = mpsc::channel();
    links.register(NodeId(1), tx.clone(), crate::host::BOOT);
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(1), NodeId(3));
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let control = ControlAdapter::new(Arc::clone(&store), rt.handle().clone(), Arc::clone(&links));
    let clock = HostClock::capped(Trio::CLOCK_STEP_CAP_MILLIS);
    let mut host = Host::new(NodeId(1), engine, links, control, clock);
    host.peer_drop_after_millis = 600_000;
    host.unreachable_drop_after_millis = 600_000;
    rt.block_on(crate::admin::bootstrap(&store, clock.now(), |msg| {
        tx.send(msg).map_err(|_| NodeStopped(NodeId(1)))
    }))
    .expect("bootstrap");
    let step = |host: &mut Host| {
        host.run_due();
        host.drain();
        assert_eq!(host.fault, None, "node 1 faulted");
        while let Ok(msg) = rx.try_recv() {
            host.handle(msg);
        }
    };
    let deadline = Instant::now() + crate::host::test_patience(Trio::PATIENCE);
    let token = loop {
        step(&mut host);
        let read_only = host
            .committed
            .get(&P1)
            .is_some_and(|result| result.mode == PartitionMode::ReadOnly);
        if let Some(owned) = host.owner_views.ownership(P1).filter(|_| read_only) {
            break owned.token;
        }
        assert!(Instant::now() < deadline, "no read-only commit owned");
        std::thread::sleep(Duration::from_millis(1));
    };

    let hold = clock.hold();
    let owner = host.owner_views.still_owner(P1, token);
    let (reply, answer) = mpsc::channel();
    host.handle(Msg::Client(Client {
        partition: P1,
        call: put(1),
        reply,
    }));
    let mut refused = None;
    for _ in 0..HELD_STEPS {
        step(&mut host);
        if let Ok(found) = answer.try_recv() {
            refused = Some(found);
            break;
        }
    }
    let owner_after = host.owner_views.still_owner(P1, token);
    drop(hold);
    assert!(owner && owner_after, "owned while the view admits");
    match refused {
        Some(Answer::Error { error, .. }) => {
            assert_eq!(error.kind, ErrorKind::RecoveryReadOnly, "{error:?}");
        }
        other => panic!("a read-only partition refuses the write at the held tick: {other:?}"),
    }
}

/// The token the rows below find owned before their fault.
const HELD: OwnerToken = OwnerToken {
    generation: Generation(2),
    owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
};

/// Both readers own [`HELD`] on partition 1.
fn assert_owned(views: &OwnerViews, what: &str) {
    assert!(views.still_owner(P1, HELD), "{what}: still_owner");
    assert_eq!(
        views.ownership(P1).map(|owned| owned.token),
        Some(HELD),
        "{what}: ownership"
    );
}

/// Neither reader owns [`HELD`] on partition 1.
fn assert_unowned(views: &OwnerViews, what: &str) {
    assert!(!views.still_owner(P1, HELD), "{what}: still_owner");
    assert!(views.ownership(P1).is_none(), "{what}: ownership");
}

/// A host over a fresh folder whose owner map holds a live view of partition 1 under
/// [`HELD`], owned by both readers.
fn owned_host(dir: &std::path::Path, rt: &tokio::runtime::Runtime) -> Host {
    let mut host = trio::host_over(dir, rt);
    host.stepped_at = host.clock.now();
    host.report_view(
        &live_view(HELD, host.clock.now().plus_millis(60_000)),
        false,
    );
    assert_owned(&host.owner_views, "precondition");
    host
}

/// How long [`answered_only_after_revoke`] lets a fault path run before calling it stuck: it
/// has neither begun to revoke nor returned. Only a hang guard; the order check itself waits
/// on no clock.
const ORDER_STUCK: Duration = Duration::from_secs(10);

/// Run `fault` on another thread while this one holds `views` for reading. `OwnerViews::revoke`
/// takes the write lock, so a path that revokes before it replies is stuck at the revoke until
/// the reader lets go. This thread waits until the path has begun to revoke
/// (`OwnerViews::revokes_begun` moves) or has returned, and only then looks at the waiter: a
/// path that replied before it called revoke has already sent, so the waiter holds the answer
/// and the row is red. No timer decides it, so a fault thread that starts late changes nothing.
/// Then the reader lets go, the answer is the fault, and both readers read false.
/// Returns that answer.
pub(super) fn answered_only_after_revoke(
    name: &str,
    views: &OwnerViews,
    answer: &Receiver<Answer>,
    fault: impl FnOnce() + Send,
) -> Answer {
    std::thread::scope(|scope| {
        let readers = views.hold_readers();
        let begun = views.revokes_begun();
        let faulting = scope.spawn(fault);
        let stuck = Instant::now() + crate::host::test_patience(ORDER_STUCK);
        while views.revokes_begun() == begun && !faulting.is_finished() {
            assert!(
                Instant::now() < stuck,
                "{name}: the fault path neither began to revoke nor returned"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let early = answer.try_recv();
        assert!(
            matches!(early, Err(mpsc::TryRecvError::Empty)),
            "{name}: the waiter heard of the fault before ownership was revoked: {early:?}"
        );
        drop(readers);
        faulting.join().expect("the fault path panicked");
    });
    let heard = match answer.try_recv() {
        Ok(heard @ Answer::Error { .. }) => heard,
        other => panic!("{name}: the waiter is answered with the fault: {other:?}"),
    };
    assert_unowned(views, name);
    heard
}

/// A waiter under request `request`, `kind`, answering on `reply`.
fn pending(request: u64, kind: PendingKind, reply: Sender<Answer>) -> (RequestIdentity, Pending) {
    (
        identity(request),
        Pending {
            reply,
            kind,
            event: EventId(0),
            since: Tick(0),
        },
    )
}

/// own84_6c (#84 review): both fault paths revoke ownership before the waiter can read the
/// fault, not merely by the time the call returns. `fail_waiter` alone, and `set_fault`
/// answering a pending call, each run while the map is held for reading: the waiter hears
/// nothing until revoke has run, then hears the fault with both readers false.
/// Cuts: `fail_waiter` sending before it revokes, red ("heard of the fault before ownership
/// was revoked"); `set_fault` revoking after its pending loop, red the same way.
/// Unit (one host each, no cluster).
#[config_log::retcd_test]
fn own84_6c_both_fault_paths_revoke_before_the_waiter_can_read_the_fault() {
    let rt = current_thread();

    let dir = config_testkit::fs::temp_dir();
    let host = owned_host(dir.path(), &rt);
    let views = Arc::clone(&host.owner_views);
    let (reply, answer) = mpsc::channel();
    answered_only_after_revoke("fail_waiter", &views, &answer, || {
        fail_waiter(&views, &reply, "a compile fault".to_owned());
    });

    let dir = config_testkit::fs::temp_dir();
    let mut host = owned_host(dir.path(), &rt);
    let views = Arc::clone(&host.owner_views);
    let (reply, answer) = mpsc::channel();
    let root = root_key(TenantId(1), AFFINITY, b"a");
    let (id, waiter) = pending(1, PendingKind::Read(root, Look::Doc), reply);
    host.pending.insert(id, waiter);
    answered_only_after_revoke("set_fault", &views, &answer, || {
        host.set_fault(Fault::from("a flush fault".to_owned()));
    });
}

/// own84_6d (#84 review): every concrete caller of `fail_waiter` outside a compile passes the
/// host's own map, and revokes it before its waiter hears the fault: a put's expiry meeting
/// another call, a status query no module answers, a read whose step view is missing, a reply
/// to a call it does not answer, and a previous-view answer to a read or under an unbound
/// handle. Each case faults a fresh host whose map owns [`HELD`], through
/// [`answered_only_after_revoke`]. The two compile callers in `Host::client` are pinned in
/// `own84_6f_each_compile_caller_in_client_revokes_before_it_answers` (`tests.rs`).
/// Cut: no revoke in `fail_waiter`, red on the first case.
/// Unit (one host per case, no cluster).
#[config_log::retcd_test]
fn own84_6d_each_fail_waiter_caller_revokes_before_it_answers() {
    type Case = fn(&mut Host, Sender<Answer>) -> Result<(), Fault>;
    fn read_kind() -> PendingKind {
        PendingKind::Read(root_key(TenantId(1), AFFINITY, b"a"), Look::Doc)
    }
    fn previous_kind() -> PendingKind {
        PendingKind::Previous(root_key(TenantId(1), AFFINITY, b"a"))
    }
    fn served_read() -> ReplyEffect {
        ReplyEffect::Read {
            identity: identity(1),
            outcome: ReadServiceOutcome::Served,
            value: None,
        }
    }
    fn waiting(host: &mut Host, kind: PendingKind, reply: Sender<Answer>) {
        let (id, waiter) = pending(1, kind, reply);
        host.pending.insert(id, waiter);
    }
    let cases: [(&str, Case); 6] = [
        ("a put's expiry meets a read", |host, reply| {
            waiting(host, read_kind(), reply);
            host.expire_txn(identity(1), EventId(0), Tick(0))
        }),
        ("a status query no module answers", |host, reply| {
            // Forged, as F-001's rows forge replies: a status waiter on a stale timer's step,
            // which no module answers.
            let site = Site {
                partition: P1,
                correlation: CorrelationId(1),
            };
            let fire = EventKind::Timer(TimerFired {
                id: TimerId(u64::MAX),
                version: TimerVersion(1),
                scheduled_at: Tick(0),
            });
            let queued = host.queued(site, fire, false, None);
            host.statuses.insert(queued.event.id, (identity(1), reply));
            let fault = host.process(queued).expect_err("no module answered");
            assert!(fault.detail.contains("status query"), "{fault:?}");
            Err(fault)
        }),
        ("a read with no step view", |host, reply| {
            waiting(host, read_kind(), reply);
            host.reply(P1, &served_read())
        }),
        ("a read reply to a previous-view call", |host, reply| {
            waiting(host, previous_kind(), reply);
            host.reply(P1, &served_read())
        }),
        ("a previous view to a read", |host, reply| {
            waiting(host, read_kind(), reply);
            host.previous_answer(P1, identity(1), Ok(SnapshotHandle(7)))
        }),
        ("an unbound previous view", |host, reply| {
            waiting(host, previous_kind(), reply);
            host.previous_answer(P1, identity(1), Ok(SnapshotHandle(7)))
        }),
    ];
    let rt = current_thread();
    for (name, case) in cases {
        let dir = config_testkit::fs::temp_dir();
        let mut host = owned_host(dir.path(), &rt);
        let views = Arc::clone(&host.owner_views);
        let (reply, answer) = mpsc::channel();
        answered_only_after_revoke(name, &views, &answer, || {
            case(&mut host, reply).expect_err(name);
        });
    }
}

/// own84_6g (#84 review 4): a previous-view answer under a handle storage **did** bind, whose
/// view meets a read fault, revokes before its waiter hears the fault. own84_6d's previous-view
/// cases stop at a wrong kind or an unbound handle; this one reaches `object_answer`'s error in
/// `Host::previous_answer`. The root record of `a` in partition 1, generation 1 is cut to 3
/// bytes on disk (shorter than a frame), as the A-R164 rows cut values, so the bound view's
/// read records `StorageFault::Corrupt`. Run through [`answered_only_after_revoke`]; then both
/// readers are false and the fault names the storage read fault.
/// Cut: `previous_answer`'s `object_answer` arm sending the fault before `fail_waiter`'s
/// revoke, red ("heard of the fault before ownership was revoked").
/// Unit (one host, no cluster).
#[config_log::retcd_test]
fn own84_6g_a_previous_view_read_fault_revokes_before_it_answers() {
    use rdb_core::contracts::storage::Namespace;
    let dir = config_testkit::fs::temp_dir();
    let root = root_key(TenantId(1), AFFINITY, b"a");
    // The engine makes its column families; then its root record is written as no frame reads.
    drop(RocksEngine::open(dir.path()).expect("open engine"));
    {
        let raw = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            dir.path(),
            rdb_storage::keys::COLUMN_FAMILIES,
        )
        .expect("raw open");
        let cf = raw
            .cf_handle(rdb_storage::keys::cf_for(Namespace::User))
            .expect("user column family");
        let key =
            rdb_storage::keys::encode_key(P1, Generation(1), Namespace::User, root.as_bytes());
        raw.put_cf(cf, key, [0, 0, 0]).expect("raw put");
    }
    let rt = current_thread();
    let mut host = owned_host(dir.path(), &rt);
    let handle = SnapshotHandle(7);
    let view = host
        .engine
        .snapshot(P1, Generation(1), handle)
        .expect("a view of generation 1");
    host.bound.insert(handle, view);
    let (reply, answer) = mpsc::channel();
    let (id, waiter) = pending(1, PendingKind::Previous(root), reply);
    host.pending.insert(id, waiter);
    let views = Arc::clone(&host.owner_views);
    let mut returned = None;
    let heard = answered_only_after_revoke(
        "a bound previous view's read fault",
        &views,
        &answer,
        || {
            returned = Some(host.previous_answer(P1, identity(1), Ok(handle)));
        },
    );
    let fault = returned
        .expect("previous_answer ran")
        .expect_err("a read fault is a host fault");
    assert!(
        fault.detail.contains("storage read fault"),
        "the bound view was read, not refused as unbound: {fault:?}"
    );
    match heard {
        Answer::Error {
            error,
            request: None,
        } => assert_eq!(error.detail, format!("host fault: {}", fault.detail)),
        other => panic!("the caller is told the host faulted: {other:?}"),
    }
    assert!(!views.still_owner(P1, HELD), "still_owner after the fault");
    assert!(views.ownership(P1).is_none(), "ownership after the fault");
}

/// own84_6e (#84 review): a waiter that already gave up (its receiver dropped) does not undo
/// the revoke. `fail_waiter` still revokes and returns the fault; faulting again, through
/// `fail_waiter` and `set_fault`, and a view reported late leave both readers false.
/// Cut: `fail_waiter` revoking only when its send succeeds, red. Unit (one host, no cluster).
#[config_log::retcd_test]
fn own84_6e_a_dropped_waiter_does_not_undo_the_revoke() {
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let mut host = owned_host(dir.path(), &rt);
    let (reply, answer) = mpsc::channel();
    drop(answer);
    let fault = fail_waiter(&host.owner_views, &reply, "a compile fault".to_owned());
    assert_eq!(fault.detail, "a compile fault", "the fault is returned");
    assert_unowned(&host.owner_views, "after a reply nobody read");
    let again = fail_waiter(&host.owner_views, &reply, "a second fault".to_owned());
    host.set_fault(fault);
    host.set_fault(again);
    host.report_view(
        &live_view(HELD, host.clock.now().plus_millis(60_000)),
        false,
    );
    assert_unowned(&host.owner_views, "after repeated faults and a late view");
}

/// An authority effect of partition 1, as A1 emits it.
fn from_a1(kind: AuthorityEffect) -> Effect {
    Effect {
        correlation: CorrelationId::default(),
        from: ModuleName::Authority,
        partition: P1,
        kind: EffectKind::Kernel(KernelEffect::Authority(kind)),
    }
}

/// own84_9 (#84 review): a fence at tick 0. A1's fence view there has its horizon saturated to
/// 0, the fence tick itself, which check 6 still admits (writes stay frozen by check 7). The
/// host names the view a fence's from its step, so both readers read false at once and stay
/// false. Outside a fence, a view whose last admitting tick is now is still owned (equality,
/// `remaining` zero), and a regrant after the fence is owned again. Both scopes of fence, on a
/// guarded clock (the one `rdb_node --clock-step-file` steps) stepped back to read tick 0.
/// Cut: `publish` ignoring `fenced`, red at "the fence's step" for the first scope.
/// Unit (one host, no cluster).
#[config_log::retcd_test]
fn own84_9_a_fence_at_tick_zero_ends_ownership() {
    use rdb_core::contracts::authority::DenyReason;
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let mut host = trio::host_over(dir.path(), &rt);
    let clock = HostClock::guarded(0);
    clock.step_back(u64::MAX);
    assert_eq!(clock.now(), Tick(0), "precondition: the clock reads tick 0");
    host.clock = clock;
    host.stepped_at = clock.now();
    let view = |valid_through| {
        from_a1(AuthorityEffect::PublishAuthorityView(live_view(
            HELD,
            Tick(valid_through),
        )))
    };
    for scope in [FenceScope::Partition(P1), FenceScope::Node] {
        host.deliver(&[view(60_000)]).expect("a live view");
        assert_owned(&host.owner_views, "a live view");
        host.deliver(&[view(0)]).expect("a view through now");
        assert_owned(&host.owner_views, "a view through now, outside a fence");
        assert_eq!(
            host.owner_views.ownership(P1).map(|owned| owned.remaining),
            Some(Duration::ZERO)
        );
        let fence = from_a1(AuthorityEffect::Fence {
            scope,
            reason: DenyReason::Frozen,
        });
        host.deliver(&[fence, view(0)]).expect("the fence's step");
        assert_unowned(&host.owner_views, &format!("the fence's step, {scope:?}"));
        assert_eq!(clock.now(), Tick(0), "still tick 0");
        assert_unowned(&host.owner_views, &format!("read again, {scope:?}"));
        host.deliver(&[view(60_000)]).expect("a regrant");
        assert_owned(&host.owner_views, &format!("a regrant, {scope:?}"));
    }
}

/// own84_9b (#84 review): a node fence A1 sites at partition 0, at tick 0, with views of two
/// served partitions in the same step. Partition 0 is served by nobody, so the fence's site is
/// neither view's partition: a node fence is every partition's, wherever it is sited. Both
/// views have the zero horizon check 6 still admits at tick 0, so only the fence can end
/// ownership. Both readers read false for both partitions, at once and read again; a regrant
/// of the same token is owned again on both.
/// Cut: `StepFences::of` counting a node fence for its own site's partition only, red at
/// "the fence's step"; own84_9 stays green under it.
/// Unit (one host, no cluster).
#[config_log::retcd_test]
fn own84_9b_a_node_fence_sited_elsewhere_ends_ownership_of_every_partition() {
    use rdb_core::contracts::authority::DenyReason;
    const SITE: PartitionId = PartitionId(0);
    const SERVED: [PartitionId; 2] = [P1, PartitionId(2)];
    let dir = config_testkit::fs::temp_dir();
    let rt = current_thread();
    let mut host = trio::host_over(dir.path(), &rt);
    let clock = HostClock::guarded(0);
    clock.step_back(u64::MAX);
    assert_eq!(clock.now(), Tick(0), "precondition: the clock reads tick 0");
    host.clock = clock;
    host.stepped_at = clock.now();
    let at_site = |kind| Effect {
        correlation: CorrelationId::default(),
        from: ModuleName::Authority,
        partition: SITE,
        kind: EffectKind::Kernel(KernelEffect::Authority(kind)),
    };
    let views = |valid_through| {
        SERVED.map(|partition| {
            at_site(AuthorityEffect::PublishAuthorityView(live_view_of(
                partition,
                HELD,
                Tick(valid_through),
            )))
        })
    };
    let owned = |host: &Host, what: &str| {
        for partition in SERVED {
            assert!(
                host.owner_views.still_owner(partition, HELD),
                "{what}, {partition:?}: still_owner"
            );
            assert_eq!(
                host.owner_views.ownership(partition).map(|o| o.token),
                Some(HELD),
                "{what}, {partition:?}: ownership"
            );
        }
    };
    let unowned = |host: &Host, what: &str| {
        for partition in SERVED {
            assert!(
                !host.owner_views.still_owner(partition, HELD),
                "{what}, {partition:?}: still_owner"
            );
            assert!(
                host.owner_views.ownership(partition).is_none(),
                "{what}, {partition:?}: ownership"
            );
        }
    };
    host.deliver(&views(0)).expect("views through now");
    owned(&host, "views through now, outside a fence");
    let fence = at_site(AuthorityEffect::Fence {
        scope: FenceScope::Node,
        reason: DenyReason::Frozen,
    });
    let [first, second] = views(0);
    host.deliver(&[fence, first, second])
        .expect("the fence's step");
    unowned(&host, "the fence's step");
    assert_eq!(clock.now(), Tick(0), "still tick 0");
    unowned(&host, "read again");
    host.deliver(&views(60_000)).expect("a regrant");
    owned(&host, "a regrant of the same token");
}

/// own84_7d (#84 review): the other L1 refusal. L1 blocked for an operator
/// (`DivergenceRequiresOperator`) under a live grant: `still_owner` reads true and the write is
/// refused `DIVERGENCE_REQUIRES_OPERATOR`, at one held tick.
/// Integration (~2 s): bootstrap's discovery window, then the steps to the block.
#[config_log::retcd_test]
fn own84_7d_a_partition_blocked_for_an_operator_is_owned_and_refuses_writes() {
    use rdb_core::contracts::authority::BlockReason;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let token = token(&trio);
    trio.nodes[0].0.push(
        Site {
            partition: P1,
            correlation: CorrelationId::default(),
        },
        EventKind::Kernel(KernelEvent::BlockPartition(
            BlockReason::DivergenceRequiresOperator {
                diverged: vec![CopyId(3)],
            },
        )),
        false,
        None,
    );
    trio.until("L1 blocks writes", |trio| {
        (trio.status(0).admits == Some(false)).then_some(())
    });
    let kind = owned_but_refused(&mut trio, token, 1);
    assert_eq!(
        kind,
        ErrorKind::DivergenceRequiresOperator,
        "check 8's refusal"
    );
}
