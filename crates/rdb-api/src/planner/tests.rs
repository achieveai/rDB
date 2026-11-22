//! #81 C1: [`choose_target`], pure. Each row builds the looks a sweep would gather.

use super::*;

use rdb_core::contracts::ids::OwnerEpoch;
use rdb_core::contracts::membership::CopyId;

use crate::host::Holding;

const DEAD: NodeId = NodeId(1);

fn member(copy: u8, node: u32, role: ReplicaRole) -> RecordMember {
    RecordMember {
        copy: CopyId(copy),
        node: NodeId(node),
        role,
    }
}

/// Node 1 the dead primary, nodes 2 and 3 regular secondaries.
fn rf3() -> Vec<RecordMember> {
    vec![
        member(0, 1, ReplicaRole::Primary),
        member(1, 2, ReplicaRole::RegularSecondary),
        member(2, 3, ReplicaRole::RegularSecondary),
    ]
}

fn status(node: u32, holds: Option<(u64, u64)>) -> NodeStatus {
    NodeStatus {
        node: NodeId(node),
        boot: BootId(1),
        fault: None,
        holds: holds.map(|(generation, applied)| Holding {
            generation: Generation(generation),
            owner_epoch: OwnerEpoch(0),
            applied,
            durable: applied,
        }),
        authority: "none".to_owned(),
        fence: None,
        fence_polling: false,
        replace_stopped: false,
        recovery: None,
        recovered: None,
        role: "secondary",
        protection: None,
        admits: None,
        published: None,
        in_flight: None,
        waits_on: None,
        stalled: None,
        reload_pending: None,
        lengths: None,
        pinned: Vec::new(),
        config_version: None,
        qualifying_copy: None,
        clock_bound_lost: false,
        committed_without: rdb_core::contracts::recovery::NoteState::Unknown,
        tail_refused: None,
        members_revision: None,
    }
}

fn looks(entries: &[(u32, Option<(u64, u64)>)]) -> Vec<(NodeId, Option<NodeStatus>)> {
    entries
        .iter()
        .map(|(node, holds)| (NodeId(*node), Some(status(*node, *holds))))
        .collect()
}

fn choose(
    partition: u32,
    members: &[RecordMember],
    looks: &[(NodeId, Option<NodeStatus>)],
) -> Result<NodeId, NoEligible> {
    choose_target(
        PartitionId(partition),
        members,
        DEAD,
        looks,
        &BTreeSet::new(),
    )
}

/// V1: unequal prefixes in one generation. The copy holding the acknowledged write wins
/// whichever node id it has, in both pairings. Cut: pick the lowest node id, red.
#[test]
fn own81_v1_picks_the_secondary_holding_the_acked_write() {
    assert_eq!(
        choose(1, &rf3(), &looks(&[(2, Some((3, 10))), (3, Some((3, 11)))])),
        Ok(NodeId(3)),
        "node 2, the lower id, lacks seq 11"
    );
    assert_eq!(
        choose(1, &rf3(), &looks(&[(2, Some((3, 11))), (3, Some((3, 10)))])),
        Ok(NodeId(2)),
    );
}

/// V1 (critic): one copy holds an old-generation tail above the new base, the other holds
/// new-generation acknowledged writes. The generation is compared first. Cut: compare
/// `applied` first, red.
#[test]
fn own81_v1_a_newer_generation_beats_a_longer_old_generation_tail() {
    assert_eq!(
        choose(1, &rf3(), &looks(&[(2, Some((2, 900))), (3, Some((3, 5)))])),
        Ok(NodeId(3)),
    );
}

/// V2: a shadow is never chosen, even holding the most, and it does not count toward the two
/// answering copies. Cut: drop the shadow filter, red on both asserts.
#[test]
fn own81_v2_shadow_never_chosen() {
    let members = vec![
        member(0, 1, ReplicaRole::Primary),
        member(1, 2, ReplicaRole::RegularSecondary),
        member(2, 3, ReplicaRole::RegularSecondary),
        member(3, 4, ReplicaRole::Shadow),
    ];
    let all = looks(&[(2, Some((3, 10))), (3, Some((3, 9))), (4, Some((3, 50)))]);
    assert_eq!(choose(1, &members, &all), Ok(NodeId(2)));
    let one_regular = looks(&[(2, Some((3, 10))), (4, Some((3, 50)))]);
    assert_eq!(
        choose(1, &members, &one_regular),
        Err(NoEligible::LoneSurvivor { answering: 1 })
    );
}

/// V3: one answering copy is a lone survivor (customer answer 1): no target. Also when the
/// second copy answered with a fault, holds no lineage, or did not answer.
/// Cut: require one candidate, red.
#[test]
fn own81_v3_lone_survivor_not_promoted() {
    let lone = Err(NoEligible::LoneSurvivor { answering: 1 });
    let mut silent = looks(&[(2, Some((3, 10)))]);
    silent.push((NodeId(3), None));
    assert_eq!(choose(1, &rf3(), &silent), lone);
    let mut faulted = looks(&[(2, Some((3, 10))), (3, Some((3, 11)))]);
    faulted[1].1.as_mut().expect("a status").fault = Some("sync_stuck".to_owned());
    assert_eq!(choose(1, &rf3(), &faulted), lone);
    assert_eq!(
        choose(1, &rf3(), &looks(&[(2, Some((3, 10))), (3, None)])),
        lone
    );
    assert_eq!(
        choose(1, &rf3(), &[]),
        Err(NoEligible::LoneSurvivor { answering: 0 })
    );
}

/// A copy on a frozen grant could never admit (`refuse_frozen_target`), so it is no candidate.
#[test]
fn own81_v3_a_frozen_copy_is_no_candidate() {
    let frozen = BTreeSet::from([NodeId(3)]);
    assert_eq!(
        choose_target(
            PartitionId(1),
            &rf3(),
            DEAD,
            &looks(&[(2, Some((3, 10))), (3, Some((3, 11)))]),
            &frozen,
        ),
        Err(NoEligible::LoneSurvivor { answering: 1 })
    );
}

/// The dead owner is never chosen, even when its look answered (a look races its death).
#[test]
fn own81_the_dead_owner_is_never_chosen() {
    let all = looks(&[(1, Some((3, 99))), (2, Some((3, 10))), (3, Some((3, 9)))]);
    assert_eq!(choose(1, &rf3(), &all), Ok(NodeId(2)));
}

/// Ruling C-6: idle tied partitions spread over the tied nodes by partition number, instead of
/// all landing on the lowest id. Cut: index 0, red.
#[test]
fn own81_ties_spread_by_partition() {
    let tied = looks(&[(2, Some((3, 0))), (3, Some((3, 0)))]);
    let chosen: Vec<u32> = (1..=4)
        .map(|p| choose(p, &rf3(), &tied).expect("two answer").0)
        .collect();
    assert_eq!(chosen, [3, 2, 3, 2]);
}

/// Links on which `node` is a healthy local host: it answers every look, with no fault, until
/// it is sent `Msg::Stop`, and then its mailbox is closed. A planner looks at its own host
/// before it holds, and a closed or absent mailbox releases the grant.
fn healthy(node: u32) -> (Arc<Links>, std::thread::JoinHandle<()>) {
    let links = Links::new();
    let (tx, rx) = std::sync::mpsc::channel::<Msg>();
    links.register(NodeId(node), tx, BootId(1));
    let host = std::thread::spawn(move || {
        for msg in rx {
            match msg {
                Msg::Inspect { reply, .. } => {
                    let _ = reply.send(status(node, None));
                }
                Msg::Stop => return,
                _ => {}
            }
        }
    });
    (links, host)
}

fn healthy_links(node: u32) -> Arc<Links> {
    healthy(node).0
}

/// A planner for `node` over `store`, on a clock no `Net` is attached to (so its bound holds)
/// unless a row attaches one. `grant_millis` short enough for a row to wait out.
fn planner(node: u32, store: &Arc<dyn ConfigStore>, links: Arc<Links>) -> Planner {
    let budgets = Budgets {
        grant_millis: 300,
        clock_error_millis: 20,
        dispatch_margin_millis: 20,
        ..crate::host::test_budgets()
    };
    Planner {
        node: NodeId(node),
        boot: BootId(1),
        store: Arc::clone(store),
        budgets,
        clock: links.clock(),
        links,
        failover: Failover::default(),
        look_within: Duration::from_millis(100),
        promote_within: Duration::from_secs(1),
    }
}

/// A held test clock and its first tick: every planner given `clock` reads exactly the tick
/// the row set with `hold.advance`, so an expiry boundary is asserted to the millisecond.
fn held_clock() -> (HostClock, crate::clock::ClockHold, u64) {
    let clock = HostClock::capped(1);
    let hold = clock.hold();
    let start = clock.now().0;
    (clock, hold, start)
}

/// `G + eps + delta`: how long after a grant write's dispatch tick its expiry is proven past
/// (`expiry_proven` wants the authority time strictly above `E + eps + delta`, `E = t + G`).
const fn takeover_floor(budgets: &Budgets) -> u64 {
    budgets.grant_millis + budgets.clock_error_millis + budgets.dispatch_margin_millis
}

/// `tick + G`, the `expiry_utc_ms` a grant write dispatched at `tick` carries on a held clock.
fn expiry_from(tick: u64, budgets: &Budgets) -> i64 {
    i64::try_from(tick + budgets.grant_millis).expect("in range")
}

fn planner_grant(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
) -> Option<GrantRecord> {
    rt.block_on(store.get(GetRequest {
        key: Bytes::from(ControlKey::PlannerGrant.encode()),
    }))
    .expect("the store answers")
    .record
    .map(|found| GrantRecord::decode(&found.value).expect("a grant record"))
}

fn messages(method: &str, message: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == message)
        .collect()
}

/// R3, the planner's half: two planners racing for a free `planner/grant` give one holder, by
/// the create-only CAS. The loser stays out while the winner's grant runs. Then the winner
/// stops renewing, and the loser takes it over only after the expiry is proven, with the next
/// grant id; the old holder logs the loss on its next pass. On a held clock the takeover
/// boundary is exact: refused at `E + eps + delta` after the winner's write, taken 1 ms later.
/// The race here is not forced, so writing the grant unconditionally does not reliably go red
/// in this row (issue #133); [`expired_grant_takeovers_race`] forces it.
/// Cut: take over once `E` alone has passed (drop `eps + delta`); taken at the boundary, red.
#[config_log::retcd_test]
fn own81_r3_two_planners_hold_one_grant_and_take_over_only_after_expiry() {
    const METHOD: &str = "own81_r3_two_planners_hold_one_grant_and_take_over_only_after_expiry";
    let rt = crate::db::reopen_tests::runtime();
    // Every put waits at the gate, so both planners read "no grant" before either writes: the
    // race the CAS has to settle, not one hold after the other.
    let gated = Arc::new(crate::test_store::Scripted::gated());
    let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
    let (clock, hold, written) = held_clock();
    let (a, b) = (
        Planner {
            clock,
            ..planner(2, &store, healthy_links(2))
        },
        Planner {
            clock,
            ..planner(3, &store, healthy_links(3))
        },
    );
    let (mut ma, mut mb) = (Memory::default(), Memory::default());
    let (held_a, held_b, ()) = rt.block_on(async {
        tokio::join!(a.hold(&mut ma), b.hold(&mut mb), async {
            tokio::task::yield_now().await;
            gated.release();
            gated.release();
        })
    });
    // From here on puts pass the gate.
    gated.rescript(crate::test_store::Script {
        pass_gate: u32::MAX,
        ..crate::test_store::Script::default()
    });
    assert!(held_a ^ held_b, "exactly one holds: {held_a} {held_b}");
    let (winner, mut mw, loser, mut ml) = if held_a {
        (a, ma, b, mb)
    } else {
        (b, mb, a, ma)
    };
    let budgets = loser.budgets;
    let first = planner_grant(&rt, &store).expect("a grant was written");
    assert_eq!(
        (first.node, first.grant, first.expiry_utc_ms),
        (winner.node, GrantId(1), expiry_from(written, &budgets))
    );
    assert!(!rt.block_on(loser.hold(&mut ml)), "the grant is live");

    let floor = takeover_floor(&budgets);
    hold.advance(floor);
    assert!(
        !rt.block_on(loser.hold(&mut ml)),
        "at exactly G+eps+delta={floor} ms after the winner's write the expiry is not proven"
    );
    assert_eq!(planner_grant(&rt, &store), Some(first), "nothing written");
    hold.advance(1);
    assert!(
        rt.block_on(loser.hold(&mut ml)),
        "{} ms after the winner's write the expiry is proven",
        floor + 1
    );
    let taken = planner_grant(&rt, &store).expect("a grant");
    assert_eq!(
        (taken.node, taken.grant, taken.expiry_utc_ms),
        (
            loser.node,
            GrantId(2),
            expiry_from(written + floor + 1, &budgets)
        )
    );
    assert!(!rt.block_on(winner.hold(&mut mw)), "the old holder lost it");
    let lost = messages(METHOD, "planner_grant_lost");
    assert!(
        lost.iter().any(|line| line["node"] == winner.node.0),
        "{lost:?}"
    );
    assert_eq!(messages(METHOD, "planner_grant_acquired").len(), 2);
}

/// `planner/grant`'s stored bytes and `mod_revision`, as they are.
async fn planner_grant_raw(store: &Arc<dyn ConfigStore>) -> Option<(Bytes, u64)> {
    store
        .get(GetRequest {
            key: Bytes::from(ControlKey::PlannerGrant.encode()),
        })
        .await
        .expect("the store answers")
        .record
        .map(|found| (found.value, found.mod_revision))
}

/// Issue #133, PR #119 review round 3 (`expired_grant_takeovers_race`): node 4's grant has
/// expired, and two planners of other identities (nodes 2 and 3) read it at the same nonzero
/// revision and both decide to take it over. The order is forced: each one's write is held at
/// the store's gate before either applies, and the gate lets one write through per release
/// (`one_shot_gate`). Node 2's is let through first and holds; its record is snapshotted while
/// node 3's write is still parked. Then node 3's stale write is let through: its CAS at the
/// old revision conflicts, so it holds nothing, its `Memory` stays empty, and node 2's record
/// is byte for byte and revision for revision as it was. The grant id rose once, 1 to 2. Unit,
/// held clock.
/// Cut: write the grant unconditionally (`expected_mod_revision: None`); node 3 overwrites
/// node 2's record and holds too, red. Cut: on a conflict, rewrite the record found as it is
/// (same bytes, a new revision); that write waits at the gate for a release that never comes,
/// and the bounded join fails the row naming the parked write. Before the one-shot gate, with
/// a few yields before the snapshot, node 3's writes went through the recycled permit first and
/// that cut passed.
#[config_log::retcd_test]
fn expired_grant_takeovers_race() {
    let rt = crate::db::reopen_tests::runtime();
    // Node 4's first write passes the gate; every write after it waits there for a release of
    // its own. A permit is not handed on when its write ends, so node 3's write cannot follow
    // node 2's before the snapshot (PR #119 review).
    let gated = Arc::new(crate::test_store::Scripted::gated_with(
        crate::test_store::Script {
            pass_gate: 1,
            one_shot_gate: true,
            ..crate::test_store::Script::default()
        },
    ));
    let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
    let (clock, hold, start) = held_clock();
    let [old, first, stale] = [4, 2, 3].map(|node| Planner {
        clock,
        ..planner(node, &store, healthy_links(node))
    });
    let budgets = old.budgets;
    let mut mo = Memory::default();
    assert!(rt.block_on(old.hold(&mut mo)), "node 4 holds");
    let (expired, expired_at) = planner_grant_at(&rt, &store).expect("node 4's grant");
    assert_eq!((expired.node, expired.grant), (NodeId(4), GrantId(1)));
    assert!(expired_at > 0, "a nonzero revision: {expired_at}");

    hold.advance(takeover_floor(&budgets) + 1);
    let (mut m2, mut m3) = (Memory::default(), Memory::default());
    let (stale_started, first_done) = (tokio::sync::Notify::new(), tokio::sync::Notify::new());
    // The race needs two writes and two releases. A third write (a conflict that rewrites the
    // record it found, say) parks at the one-shot gate for a release that never comes, so the
    // join is bounded and a stuck race fails the row rather than hanging it (PR #119 review).
    let patience = crate::host::test_patience(Duration::from_millis(10 * budgets.grant_millis));
    let (held_first, held_stale, snapshot) = rt
        .block_on(async {
            // Built inside the runtime: a timer needs its reactor.
            tokio::time::timeout(patience, async {
                tokio::join!(
                    async {
                        let held = first.hold(&mut m2).await;
                        first_done.notify_one();
                        held
                    },
                    async {
                        // Node 2's write is parked at the gate; only now does node 3 read,
                        // so it reads the same expired record at the same revision.
                        gated.put_arrived.notified().await;
                        stale_started.notify_one();
                        stale.hold(&mut m3).await
                    },
                    async {
                        stale_started.notified().await;
                        // Node 3's write is parked behind node 2's. One release lets node
                        // 2's through alone; node 3's stays parked until the second, after
                        // the snapshot.
                        gated.put_arrived.notified().await;
                        gated.release();
                        first_done.notified().await;
                        let snapshot = planner_grant_raw(&store).await.expect("node 2's write");
                        gated.release();
                        snapshot
                    }
                )
            })
            .await
        })
        .unwrap_or_else(|_| {
            panic!(
                "the takeover race did not finish within {patience:?}: a write beyond the two \
                 released (likely an extra write after the conflict) is parked at the one-shot \
                 gate"
            )
        });
    assert!(held_first, "node 2, released first, holds");
    assert!(!held_stale, "node 3's stale CAS holds nothing");
    assert!(
        m3.held.is_none(),
        "the loser's Memory is empty: {:?}",
        m3.held
    );
    let (bytes, revision) = snapshot;
    assert!(revision > expired_at, "{expired_at} -> {revision}");
    assert_eq!(
        rt.block_on(planner_grant_raw(&store)),
        Some((bytes, revision)),
        "node 2's record is not overwritten by the stale write"
    );
    let (record, _) = planner_grant_at(&rt, &store).expect("a grant");
    assert_eq!(
        (record.node, record.grant, record.expiry_utc_ms),
        (
            NodeId(2),
            GrantId(2),
            expiry_from(start + takeover_floor(&budgets) + 1, &budgets)
        ),
        "one takeover: the grant id rose once"
    );
    assert_eq!(
        m2.held.map(|held| held.grant),
        Some(GrantId(2)),
        "node 2 holds what it wrote"
    );
}

/// C1, the planner's half: with no clock bound the planner writes nothing, not even its own
/// grant, so it can never reach a `Fencing` CAS. Logged once.
/// Cut: skip the `e_new` check (write with the last expiry), red on the store read.
#[config_log::retcd_test]
fn own81_c1_no_clock_bound_no_grant_written() {
    const METHOD: &str = "own81_c1_no_clock_bound_no_grant_written";
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let links = healthy_links(2);
    // A Net attached in a cluster of three, and no peer ever sampled: no bound.
    links.clock().guard().expect("a guarded clock").attach(3);
    let unbounded = planner(2, &store, links);
    let mut memory = Memory::default();
    for _ in 0..3 {
        assert!(!rt.block_on(unbounded.hold(&mut memory)));
    }
    assert_eq!(planner_grant(&rt, &store), None, "nothing written");
    assert_eq!(messages(METHOD, "planner_clock_unbounded").len(), 1);
}

/// Lead ruling 3 on PR #119, pure: one cause is said at once, held back for [`REPEAT_EVERY`]
/// with a count, then said again with that count; another cause has its own window.
/// Cut: say every occurrence (`Some(0)` always); red at the second note.
#[test]
fn own81_repeats_say_once_per_window_with_the_count_held_back() {
    let start = Instant::now();
    let mut repeats = Repeats::default();
    assert_eq!(repeats.note("unavailable", start), Some(0));
    for second in 1..=9 {
        let now = start + Duration::from_secs(second);
        assert_eq!(repeats.note("unavailable", now), None, "{second} s");
    }
    assert_eq!(
        repeats.note("not_leader", start + Duration::from_secs(9)),
        Some(0)
    );
    assert_eq!(repeats.note("unavailable", start + REPEAT_EVERY), Some(9));
    assert_eq!(
        repeats.note("unavailable", start + REPEAT_EVERY + Duration::from_secs(1)),
        None
    );
}

/// Lead ruling 3 on PR #119, through `hold`: five reads of `planner/grant` fail in a row,
/// well inside one window, and `planner_grant_read_failed` is said once, holding back none
/// yet. Before the ruling it was said on every poll, twice a second in the walk.
/// Cut: drop the `Repeats` check; five lines, red.
#[config_log::retcd_test]
fn own81_a_failing_grant_read_is_warned_once_per_window() {
    const METHOD: &str = "own81_a_failing_grant_read_is_warned_once_per_window";
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(crate::test_store::Scripted::new(
        crate::test_store::Script {
            fail_gets: 5,
            ..crate::test_store::Script::default()
        },
    ));
    let cut = planner(2, &store, healthy_links(2));
    let mut memory = Memory::default();
    for _ in 0..5 {
        assert!(!rt.block_on(cut.hold(&mut memory)));
    }
    let said = messages(METHOD, "planner_grant_read_failed");
    assert_eq!(said.len(), 1, "{said:?}");
    assert_eq!(said[0]["suppressed"], 0, "{said:?}");
    assert!(rt.block_on(cut.hold(&mut memory)), "the sixth read answers");
}

/// PR #119 review (`stopped_host_releases`, the planner's half): the planner holds its grant,
/// then its own host's thread ends and its mailbox closes. The next hold releases the grant
/// (`planner_grant_released_host_stopped`, said once; `planner_grant_lost`, `host_stopped`),
/// and later holds write nothing. Before the review a closed mailbox read as "no answer", the
/// same as a stalled host, and the planner kept renewing.
/// Cut: treat `Asked::Stopped` as no answer in `hold`; the first hold after the stop renews, red.
#[config_log::retcd_test]
fn own81_a_planner_whose_host_mailbox_closed_releases_its_grant() {
    const METHOD: &str = "own81_a_planner_whose_host_mailbox_closed_releases_its_grant";
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let (links, host) = healthy(2);
    let stopping = planner(2, &store, Arc::clone(&links));
    let mut memory = Memory::default();
    assert!(
        rt.block_on(stopping.hold(&mut memory)),
        "a healthy host holds"
    );
    links
        .post(NodeId(2), Msg::Stop)
        .expect("the host takes the stop");
    host.join().expect("the host thread ends");
    let renewed = planner_grant(&rt, &store).expect("the grant written while healthy");
    for _ in 0..3 {
        assert!(
            !rt.block_on(stopping.hold(&mut memory)),
            "a stopped host holds nothing"
        );
    }
    assert_eq!(
        planner_grant(&rt, &store),
        Some(renewed),
        "no renewal after the stop"
    );
    let released = messages(METHOD, "planner_grant_released_host_stopped");
    assert_eq!(released.len(), 1, "said once: {released:?}");
    let lost = messages(METHOD, "planner_grant_lost");
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert_eq!(lost[0]["reason"], "host_stopped", "{lost:?}");
}

/// The control for the row above: a host whose mailbox is open but that never answers (a
/// stalled thread, V6) is no answer, not a stop, and the planner keeps its grant.
/// Cut: treat `Asked::NoAnswer` as a stop in `hold`; red at the first hold.
#[config_log::retcd_test]
fn own81_a_planner_whose_host_does_not_answer_keeps_its_grant() {
    const METHOD: &str = "own81_a_planner_whose_host_does_not_answer_keeps_its_grant";
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let links = Links::new();
    let (tx, _unread) = std::sync::mpsc::channel::<Msg>();
    links.register(NodeId(2), tx, BootId(1));
    let stalled = planner(2, &store, links);
    let mut memory = Memory::default();
    for _ in 0..2 {
        assert!(
            rt.block_on(stalled.hold(&mut memory)),
            "a host that does not answer in time still renews"
        );
    }
    assert!(messages(METHOD, "planner_grant_released_host_stopped").is_empty());
}

/// A script whose put steps apply to `planner/grant` only: every other key's put goes straight
/// through (the fault is scoped to the planner's grant).
fn on_planner_grant(script: crate::test_store::Script) -> crate::test_store::Script {
    crate::test_store::Script {
        only_prefix: Some(ControlKey::PlannerGrant.encode().into_bytes()),
        ..script
    }
}

/// `planner/grant`'s record and its store revision.
fn planner_grant_at(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
) -> Option<(GrantRecord, u64)> {
    rt.block_on(store.get(GetRequest {
        key: Bytes::from(ControlKey::PlannerGrant.encode()),
    }))
    .expect("the store answers")
    .record
    .map(|found| {
        (
            GrantRecord::decode(&found.value).expect("a grant record"),
            found.mod_revision,
        )
    })
}

/// Put `grants/9` through `store`: a put on another key, which a scoped fault must not take.
fn put_other_key(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>) {
    let put = rt
        .block_on(store.put(PutRequest {
            key: Bytes::from_static(b"grants/9"),
            value: Bytes::from_static(b"other"),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("a put on another key is not faulted");
    assert_eq!(put.outcome, MutationOutcome::Applied);
}

/// PR #119 review (`lost_grant_reply_is_not_held`): the planner's grant write applies and its
/// reply is lost. The hold answers false, and holds nothing, although the record is in the
/// store; the next hold renews it with a fresh CAS at that record's exact revision and only
/// then holds. The fault is scoped to `planner/grant`: a put on another key passes first.
/// Cut: hold on a failed write as on an applied one (`memory.held` set before the put); the
/// first hold answers true, red.
#[config_log::retcd_test]
fn own81_lost_grant_reply_is_not_held() {
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(crate::test_store::Scripted::new(on_planner_grant(
        crate::test_store::Script {
            lose_put_replies: 1,
            ..crate::test_store::Script::default()
        },
    )));
    put_other_key(&rt, &store);
    let lost = planner(2, &store, healthy_links(2));
    let mut memory = Memory::default();
    assert!(
        !rt.block_on(lost.hold(&mut memory)),
        "a lost reply holds nothing"
    );
    assert!(memory.held.is_none(), "{:?}", memory.held);
    let (applied, at) = planner_grant_at(&rt, &store).expect("the write applied");
    assert_eq!((applied.node, applied.boot), (NodeId(2), BootId(1)));
    assert!(rt.block_on(lost.hold(&mut memory)), "a fresh CAS holds");
    let (renewed, renewed_at) = planner_grant_at(&rt, &store).expect("renewed");
    assert!(renewed_at > at, "a new write: {at} -> {renewed_at}");
    assert_eq!(
        renewed.grant, applied.grant,
        "the same grant, renewed in place"
    );
    assert!(renewed.expiry_utc_ms >= applied.expiry_utc_ms);
    let held = memory.held.expect("held");
    assert_eq!(
        held.expiry_utc_ms, renewed.expiry_utc_ms,
        "held at the fresh write"
    );
}

/// PR #119 review (a competing renewal after a lost reply): the planner's grant write applies
/// and its reply is lost, so it holds nothing. Its next hold reads its own record at revision
/// `r1` and CASes a renewal at `r1`; while that put waits, a second renewal of the same grant
/// (node 2, boot 1, its own `Memory`) CASes at the same nonzero `r1` first and holds. The
/// waiting renewal then loses its CAS: it holds nothing and overwrites nothing, so the record
/// is the competitor's, at the competitor's revision and expiry: its bytes and revision are
/// snapshotted before the late renewal is let go and are the same afterwards. Unit, held clock.
/// Cut: renew unconditionally (`expected_mod_revision: None`); the late renewal overwrites
/// the record and holds, red.
#[config_log::retcd_test]
fn own81_a_competing_renewal_after_a_lost_reply_wins_and_is_not_overwritten() {
    let rt = crate::db::reopen_tests::runtime();
    let gated = Arc::new(crate::test_store::Scripted::gated_with(on_planner_grant(
        crate::test_store::Script {
            pass_gate: 1,
            lose_put_replies: 1,
            ..crate::test_store::Script::default()
        },
    )));
    let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
    let (clock, hold, start) = held_clock();
    let lost = Planner {
        clock,
        ..planner(2, &store, healthy_links(2))
    };
    let competitor = Planner {
        clock,
        ..planner(2, &store, healthy_links(2))
    };
    let budgets = lost.budgets;
    let mut memory = Memory::default();
    assert!(
        !rt.block_on(lost.hold(&mut memory)),
        "a lost reply holds nothing"
    );
    let (_, r1) = planner_grant_at(&rt, &store).expect("the write applied");
    assert!(r1 > 0, "a nonzero revision: {r1}");

    hold.advance(10);
    let mut theirs = Memory::default();
    let (held, (competed, snapshot)) = rt.block_on(async {
        tokio::join!(lost.hold(&mut memory), async {
            gated.put_arrived.notified().await;
            // The late renewal waits at the gate, its CAS at `r1`. The competitor's passes.
            hold.advance(10);
            gated.rescript(on_planner_grant(crate::test_store::Script {
                pass_gate: 1,
                ..crate::test_store::Script::default()
            }));
            let competed = competitor.hold(&mut theirs).await;
            // The competitor's record as it stands before the late renewal is let go.
            let snapshot = planner_grant_raw(&store)
                .await
                .expect("the competitor's write");
            gated.release();
            (competed, snapshot)
        })
    });
    assert!(competed, "the competing renewal at r1 holds");
    assert_eq!(
        rt.block_on(planner_grant_raw(&store)),
        Some(snapshot.clone()),
        "the competitor's bytes and revision are unchanged after the late renewal"
    );
    assert!(
        snapshot.1 > r1,
        "the competitor wrote: {r1} -> {}",
        snapshot.1
    );
    assert!(!held, "the renewal that lost its CAS holds nothing");
    assert!(memory.held.is_none(), "{:?}", memory.held);
    let (record, at) = planner_grant_at(&rt, &store).expect("a record");
    assert!(at > r1, "the competitor wrote: {r1} -> {at}");
    assert_eq!(
        (record.node, record.boot, record.grant, record.expiry_utc_ms),
        (
            NodeId(2),
            BootId(1),
            GrantId(1),
            expiry_from(start + 20, &budgets)
        ),
        "the competitor's record, not overwritten by the late renewal (dispatched at +10)"
    );
    assert_eq!(
        theirs.held.map(|held| held.expiry_utc_ms),
        Some(record.expiry_utc_ms)
    );
}

/// PR #119 review (a delayed renewal that succeeds): the planner holds, then renews, and the
/// renewal's reply is held 100 ms on the clock, inside the write's local horizon. The hold
/// answers true, and what it holds is the renewal's dispatch: `dispatched` is the tick read
/// before the put, not the tick at the reply, and the expiry is `dispatch + G`. Both of A1's
/// validity guards in `hold` are unchanged. Unit, held clock.
/// Cut: take `dispatched` at the reply (`self.clock.now()` after the put); red on the tick.
#[config_log::retcd_test]
fn own81_a_delayed_renewal_keeps_its_dispatch_tick() {
    let rt = crate::db::reopen_tests::runtime();
    let scripted = Arc::new(crate::test_store::Scripted::new(on_planner_grant(
        crate::test_store::Script {
            pass_puts: 1,
            hold_put_replies: 1,
            ..crate::test_store::Script::default()
        },
    )));
    let store: Arc<dyn ConfigStore> = Arc::clone(&scripted) as Arc<dyn ConfigStore>;
    let (clock, hold, start) = held_clock();
    let delayed = Planner {
        clock,
        ..planner(2, &store, healthy_links(2))
    };
    let budgets = delayed.budgets;
    let mut memory = Memory::default();
    assert!(
        rt.block_on(delayed.hold(&mut memory)),
        "the first write holds"
    );
    hold.advance(10);
    let dispatched = start + 10;
    let (held, ()) = rt.block_on(async {
        tokio::join!(delayed.hold(&mut memory), async {
            scripted.reply_held.notified().await;
            hold.advance(100);
            scripted.release_replies();
        })
    });
    assert!(held, "a reply 100 ms late, inside the horizon, holds");
    let held = memory.held.expect("held");
    assert_eq!(
        (held.dispatched, held.expiry_utc_ms),
        (Tick(dispatched), expiry_from(dispatched, &budgets)),
        "the renewal's dispatch tick, not the reply's ({})",
        dispatched + 100
    );
    let (record, _) = planner_grant_at(&rt, &store).expect("renewed");
    assert_eq!(record.expiry_utc_ms, held.expiry_utc_ms);
}

/// PR #119 review (`dropped_grant_write_is_not_held`): two grant writes in a row fail without
/// applying. Neither makes the planner hold, nothing is in the store, and the third, which
/// applies, holds.
/// Cut: as the lost-reply row; the first hold answers true, red.
#[config_log::retcd_test]
fn own81_dropped_grant_write_is_not_held() {
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(crate::test_store::Scripted::new(on_planner_grant(
        crate::test_store::Script {
            drop_puts: 2,
            ..crate::test_store::Script::default()
        },
    )));
    put_other_key(&rt, &store);
    let dropped = planner(2, &store, healthy_links(2));
    let mut memory = Memory::default();
    for attempt in 1..=2 {
        assert!(!rt.block_on(dropped.hold(&mut memory)), "write {attempt}");
        assert!(memory.held.is_none(), "write {attempt}: {:?}", memory.held);
        assert_eq!(planner_grant_at(&rt, &store), None, "write {attempt}");
    }
    assert!(
        rt.block_on(dropped.hold(&mut memory)),
        "the third write applies"
    );
    assert_eq!(
        planner_grant_at(&rt, &store).map(|(grant, _)| grant.grant),
        Some(GrantId(1))
    );
}

/// PR #119 review (`cancel_before_grant_write`): the hold is cancelled while its grant write
/// waits at a gate before it applies. Nothing is in the store and nothing is held. A planner
/// that restarts with a fresh `Memory` then acquires `GrantId(1)` at once: no record stands in
/// its way.
/// Cut: `memory.held` set before the put; the cancelled hold leaves one held, red.
#[config_log::retcd_test]
fn own81_cancel_before_grant_write() {
    let rt = crate::db::reopen_tests::runtime();
    let gated = Arc::new(crate::test_store::Scripted::gated());
    let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
    let cancelled = planner(2, &store, healthy_links(2));
    let mut memory = Memory::default();
    rt.block_on(async {
        tokio::select! {
            held = cancelled.hold(&mut memory) => panic!("the write is gated: {held}"),
            () = gated.put_arrived.notified() => {}
        }
    });
    assert!(memory.held.is_none(), "{:?}", memory.held);
    assert_eq!(planner_grant_at(&rt, &store), None, "nothing applied");

    gated.rescript(crate::test_store::Script {
        pass_gate: u32::MAX,
        ..crate::test_store::Script::default()
    });
    let mut fresh = Memory::default();
    assert!(
        rt.block_on(cancelled.hold(&mut fresh)),
        "a restart acquires"
    );
    assert_eq!(
        planner_grant_at(&rt, &store).map(|(grant, _)| grant.grant),
        Some(GrantId(1))
    );
}

/// PR #119 review (`cancel_after_grant_write`): the planner holds, then a renewal is cancelled
/// after it applied and before its reply came back. The cancelled renewal does not extend what
/// the planner holds: its `Held` is still the first write's. A fresh boot of the same node is
/// refused until the applied record's expiry is proven past, `G + eps + delta` after the
/// cancelled write, not after the first. On a held clock that boundary is exact: refused at
/// `G + eps + delta` after the cancelled write's dispatch tick, taken 1 ms later.
/// Cut: `memory.held` set before the put; the cancelled renewal moves the held horizon, red.
/// Cut: take over once `E` alone has passed (drop `eps + delta`); taken at the boundary, red.
#[config_log::retcd_test]
fn own81_cancel_after_grant_write() {
    let rt = crate::db::reopen_tests::runtime();
    let scripted = Arc::new(crate::test_store::Scripted::new(on_planner_grant(
        crate::test_store::Script {
            pass_puts: 1,
            hold_put_replies: 1,
            ..crate::test_store::Script::default()
        },
    )));
    let store: Arc<dyn ConfigStore> = Arc::clone(&scripted) as Arc<dyn ConfigStore>;
    let (clock, hold, start) = held_clock();
    let old = Planner {
        clock,
        ..planner(2, &store, healthy_links(2))
    };
    let budgets = old.budgets;
    let mut memory = Memory::default();
    assert!(rt.block_on(old.hold(&mut memory)), "the first write holds");
    let first = memory.held.expect("held");
    hold.advance(50);
    let written = start + 50;
    rt.block_on(async {
        tokio::select! {
            held = old.hold(&mut memory) => panic!("the reply is held: {held}"),
            () = scripted.reply_held.notified() => {}
        }
    });
    let held = memory.held.expect("still the first write's");
    assert_eq!(
        (held.expiry_utc_ms, held.dispatched),
        (first.expiry_utc_ms, first.dispatched),
        "the cancelled renewal extended the held horizon"
    );
    let (applied, _) = planner_grant_at(&rt, &store).expect("the renewal applied");
    assert_eq!(
        (first.expiry_utc_ms, applied.expiry_utc_ms),
        (expiry_from(start, &budgets), expiry_from(written, &budgets)),
        "{applied:?}"
    );

    let fresh = Planner {
        boot: BootId(2),
        clock,
        ..planner(2, &store, healthy_links(2))
    };
    let mut fresh_memory = Memory::default();
    assert!(
        !rt.block_on(fresh.hold(&mut fresh_memory)),
        "a fresh boot refuses before the expiry is proven"
    );
    let floor = takeover_floor(&budgets);
    hold.advance(floor);
    assert!(
        !rt.block_on(fresh.hold(&mut fresh_memory)),
        "at exactly G+eps+delta={floor} ms after the cancelled write the expiry is not proven \
         (the first write's was proven 50 ms ago)"
    );
    hold.advance(1);
    assert!(
        rt.block_on(fresh.hold(&mut fresh_memory)),
        "{} ms after the cancelled write the expiry is proven",
        floor + 1
    );
    let taken = planner_grant_at(&rt, &store).expect("taken").0;
    assert_eq!(
        (taken.boot, taken.grant, taken.expiry_utc_ms),
        (
            BootId(2),
            GrantId(applied.grant.0 + 1),
            expiry_from(written + floor + 1, &budgets)
        )
    );
}

/// PR #119 review (`late_grant_reply_keeps_dispatch_horizon`): a grant write applies, and its
/// reply comes back only after the write's local horizon (`G - delta` from its dispatch) has
/// passed. The hold answers false: the horizon runs from the dispatch, not from the reply. The
/// next hold's fresh CAS holds.
/// Cut: hold whatever was last written (drop the validity filter after the write); the late
/// hold answers true, red. Taking only `dispatched` at the reply stays green: the UTC rule on
/// the written expiry refuses too, and is the stricter of A1's two by `eps`.
#[config_log::retcd_test]
fn own81_late_grant_reply_keeps_dispatch_horizon() {
    const METHOD: &str = "own81_late_grant_reply_keeps_dispatch_horizon";
    let rt = crate::db::reopen_tests::runtime();
    let scripted = Arc::new(crate::test_store::Scripted::new(on_planner_grant(
        crate::test_store::Script {
            hold_put_replies: 1,
            ..crate::test_store::Script::default()
        },
    )));
    let store: Arc<dyn ConfigStore> = Arc::clone(&scripted) as Arc<dyn ConfigStore>;
    let late = planner(2, &store, healthy_links(2));
    let horizon = Duration::from_millis(late.budgets.grant_millis);
    let mut memory = Memory::default();
    let (held, ()) = rt.block_on(async {
        tokio::join!(late.hold(&mut memory), async {
            scripted.reply_held.notified().await;
            tokio::time::sleep(horizon).await;
            scripted.release_replies();
        })
    });
    assert!(!held, "a reply after the horizon holds nothing");
    assert!(messages(METHOD, "planner_grant_acquired").is_empty());
    assert!(planner_grant_at(&rt, &store).is_some(), "the write applied");
    assert!(rt.block_on(late.hold(&mut memory)), "a fresh CAS holds");
}

/// Write `value` at `key` straight into `store`.
fn put_raw(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>, key: &str, value: &[u8]) {
    rt.block_on(store.put(PutRequest {
        key: Bytes::from(key.to_owned()),
        value: Bytes::copy_from_slice(value),
        expected_mod_revision: None,
        dedup: None,
    }))
    .expect("the raw put lands");
}

fn delete_raw(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>, key: &str) {
    rt.block_on(store.delete(config_core::DeleteRequest {
        key: Bytes::from(key.to_owned()),
        expected_mod_revision: None,
        dedup: None,
    }))
    .expect("the raw delete lands");
}

/// The one `planner_warn_recovered` line for `warn`, with how many it held back.
fn recovered_once(method: &str, warn: &str, suppressed: u64) {
    let lines: Vec<_> = messages(method, "planner_warn_recovered")
        .into_iter()
        .filter(|line| line["warn"] == warn)
        .collect();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["suppressed"], suppressed, "{lines:?}");
}

/// `n` sweeps by `planner`, each refused: nothing promoted.
fn sweeps_refused(rt: &tokio::runtime::Runtime, planner: &Planner, memory: &mut Memory, n: u32) {
    for _ in 0..n {
        assert!(rt.block_on(planner.sweep(memory)).is_empty());
    }
}

/// PR #119 review (`bad_planner_grant_warns_once`): `planner/grant` holds bytes that are no
/// grant record. Five holds in a row, well inside one window, say `planner_grant_undecodable`
/// once and hold nothing; each still retries the read. Once the record is gone the next hold
/// acquires and says `planner_warn_recovered` with the 4 held back.
/// Cut: `Repeats::note` says every occurrence; five lines, red.
#[config_log::retcd_test]
fn own81_bad_planner_grant_warns_once() {
    const METHOD: &str = "own81_bad_planner_grant_warns_once";
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    put_raw(
        &rt,
        &store,
        &ControlKey::PlannerGrant.encode(),
        b"not a grant",
    );
    let bad = planner(2, &store, healthy_links(2));
    let mut memory = Memory::default();
    for _ in 0..5 {
        assert!(!rt.block_on(bad.hold(&mut memory)));
    }
    let said = messages(METHOD, "planner_grant_undecodable");
    assert_eq!(said.len(), 1, "{said:?}");
    delete_raw(&rt, &store, &ControlKey::PlannerGrant.encode());
    assert!(
        rt.block_on(bad.hold(&mut memory)),
        "acquired once it decodes"
    );
    recovered_once(METHOD, "planner_grant_undecodable", 4);
}

/// PR #119 review (`grant_write_warns_once`): five grant writes in a row fail. Each hold still
/// writes again, and `planner_grant_write_failed` is said once; the sixth write applies, and
/// `planner_warn_recovered` names the 4 held back.
/// Cut: `Repeats::note` says every occurrence; five lines, red.
#[config_log::retcd_test]
fn own81_grant_write_warns_once() {
    const METHOD: &str = "own81_grant_write_warns_once";
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(crate::test_store::Scripted::new(on_planner_grant(
        crate::test_store::Script {
            drop_puts: 5,
            ..crate::test_store::Script::default()
        },
    )));
    let failing = planner(2, &store, healthy_links(2));
    let mut memory = Memory::default();
    for _ in 0..5 {
        assert!(!rt.block_on(failing.hold(&mut memory)));
    }
    let said = messages(METHOD, "planner_grant_write_failed");
    assert_eq!(said.len(), 1, "{said:?}");
    assert!(
        rt.block_on(failing.hold(&mut memory)),
        "the sixth write applies"
    );
    recovered_once(METHOD, "planner_grant_write_failed", 4);
}

/// [`list_warns_once`] and [`bad_entry_warns_once`]: five sweeps on `store` are refused and say
/// `planner_sweep_unread` with `cause` once; `heal` then clears it, and the next sweep says
/// `planner_warn_recovered` with the 4 held back.
fn sweep_unread_once(
    method: &str,
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    cause: &str,
    heal: impl FnOnce(),
) {
    let planner = planner(2, store, healthy_links(2));
    let mut memory = Memory::default();
    sweeps_refused(rt, &planner, &mut memory, 5);
    let said = messages(method, "planner_sweep_unread");
    assert_eq!(said.len(), 1, "{said:?}");
    assert_eq!(said[0]["cause"], cause, "{said:?}");
    heal();
    assert!(rt.block_on(planner.sweep(&mut memory)).is_empty());
    recovered_once(method, "planner_sweep_unread", 4);
    assert_eq!(messages(method, "planner_sweep_unread").len(), 1);
}

/// Five sweeps whose `prefix` list fails, then lists that answer.
fn list_warns_once(method: &str, prefix: &str, cause: &str) {
    let rt = crate::db::reopen_tests::runtime();
    let cut = crate::test_store::PrefixCut::new(
        Arc::new(config_testkit::MemStore::new()),
        prefix,
        crate::test_store::CutCalls::Lists,
    );
    let store: Arc<dyn ConfigStore> = Arc::clone(&cut) as Arc<dyn ConfigStore>;
    sweep_unread_once(method, &rt, &store, cause, || cut.lift());
}

/// Five sweeps that find bytes at `key` that are no record of its family, then none.
fn bad_entry_warns_once(method: &str, key: &str, cause: &str) {
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    put_raw(&rt, &store, key, b"not a record");
    sweep_unread_once(method, &rt, &store, cause, || delete_raw(&rt, &store, key));
}

/// PR #119 review (`grant_list_warns_once`): the `grants/` list fails on five sweeps; each is
/// refused and the warn said once. Recovery is said.
/// Cut: `Repeats::note` says every occurrence; five lines, red.
#[config_log::retcd_test]
fn own81_grant_list_warns_once() {
    list_warns_once(
        "own81_grant_list_warns_once",
        ControlPrefix::Grants.encode(),
        "grants_list_failed",
    );
}

/// PR #119 review (`partition_list_warns_once`): as above, the `partitions/` list.
/// Cut: as above.
#[config_log::retcd_test]
fn own81_partition_list_warns_once() {
    list_warns_once(
        "own81_partition_list_warns_once",
        ControlPrefix::Partitions.encode(),
        "partitions_list_failed",
    );
}

/// PR #119 review (`bad_grant_entry_warns_once`): `grants/5` is no grant record; five sweeps
/// are refused whole and the warn said once. Recovery is said.
/// Cut: as above.
#[config_log::retcd_test]
fn own81_bad_grant_entry_warns_once() {
    bad_entry_warns_once(
        "own81_bad_grant_entry_warns_once",
        &ControlKey::Grant(NodeId(5)).encode(),
        "grant_entry_undecodable",
    );
}

/// PR #119 review (`bad_partition_entry_warns_once`): as above, `partitions/5`.
/// Cut: as above.
#[config_log::retcd_test]
fn own81_bad_partition_entry_warns_once() {
    bad_entry_warns_once(
        "own81_bad_partition_entry_warns_once",
        &ControlKey::Partition(PartitionId(5)).encode(),
        "partition_entry_undecodable",
    );
}

/// #80 S3 B1: [`write_outage_bound`] is the documented sum. At the spec defaults, with the
/// default `Timeouts` and `Failover` and the probe's 1 s put: owner 12.05 s, kill copy 13.45 s,
/// silent copy 15.65 s (critic B-1). Each input moves the terms that name it by exactly its own
/// change, and no other. `acceptance()` is ruling Q1's 15 s and, under the owner's B-1 ruling
/// (a), gates the owner and kill-copy classes only, each of which fits under it; the silent
/// class has the stated 17 s and the 25 s hard ceiling of `silent()`. Each
/// expected budget sits inside the code ceiling it stands in for (critic S-8). Unit.
///
/// Cut (a): drop `failover.poll` from the owner sum; red at the owner default (11.55 s, not
/// 12.05 s), observed 2026-11-20. Cut (b): `secondary_silent` set to the kill term, leaving the
/// silent clock out; red at the silent default (13.45 s, not 15.65 s), observed 2026-11-20.
/// Cut (c): `SecondarySilent` added to `acceptance()`'s gated list; red at the gated-list
/// assertion, observed 2026-11-20.
#[test]
fn s80s3_write_outage_bound_is_the_documented_sum() {
    let ms = Duration::from_millis;
    let budgets = Budgets::SPEC_DEFAULTS;
    let timeouts = crate::db::Timeouts::default();
    let failover = Failover::default();
    let put = PROBE_PUT_TIMEOUT;
    let bound = write_outage_bound(&budgets, &timeouts, &failover, put);
    assert_eq!(bound.owner, ms(12_050), "owner");
    assert_eq!(bound.secondary_kill, ms(13_450), "secondary_kill");
    assert_eq!(bound.secondary_silent, ms(15_650), "secondary_silent");
    assert_eq!(bound.retry, ms(1_250), "retry");

    // Each input moves exactly the terms that name it, by its own change: (owner, kill, silent).
    let moved = |changed: OutageBound, by: (u64, u64, u64), input: &str| {
        let delta = |after: Duration, before: Duration| {
            u64::try_from(after.saturating_sub(before).as_millis()).unwrap_or(u64::MAX)
        };
        assert_eq!(
            (
                delta(changed.owner, bound.owner),
                delta(changed.secondary_kill, bound.secondary_kill),
                delta(changed.secondary_silent, bound.secondary_silent),
            ),
            by,
            "{input}"
        );
    };
    let with = |b: Budgets| write_outage_bound(&b, &timeouts, &failover, put);
    moved(
        with(Budgets {
            grant_millis: budgets.grant_millis + 7,
            ..budgets
        }),
        (7, 0, 0),
        "grant",
    );
    moved(
        with(Budgets {
            clock_error_millis: budgets.clock_error_millis + 7,
            ..budgets
        }),
        (14, 0, 0),
        "epsilon, twice",
    );
    moved(
        with(Budgets {
            dispatch_margin_millis: budgets.dispatch_margin_millis + 7,
            ..budgets
        }),
        (7, 0, 0),
        "delta",
    );
    moved(
        with(Budgets {
            pause_age_millis: budgets.pause_age_millis + 7,
            ..budgets
        }),
        (0, 0, 7),
        "pause age",
    );
    let slower_poll = Failover {
        poll: failover.poll + ms(7),
        ..failover
    };
    moved(
        write_outage_bound(&budgets, &timeouts, &slower_poll, put),
        (7, 0, 0),
        "poll",
    );
    let slower = |t: crate::db::Timeouts| write_outage_bound(&budgets, &t, &failover, put);
    moved(
        slower(crate::db::Timeouts {
            read: timeouts.read + ms(7),
            ..timeouts
        }),
        (7, 0, 0),
        "read",
    );
    moved(
        slower(crate::db::Timeouts {
            unreachable_drop_after: timeouts.unreachable_drop_after + ms(7),
            ..timeouts
        }),
        (0, 7, 0),
        "unreachable_drop_after",
    );
    moved(
        write_outage_bound(&budgets, &timeouts, &failover, put + ms(7)),
        (7, 7, 7),
        "put",
    );
    // The fixed terms, by name: a dropped one is red at the defaults above as well.
    assert_eq!(
        bound.secondary_silent,
        ms(budgets.pause_age_millis)
            + ms(crate::host::DROP_CHECK_EVERY_MILLIS)
            + ms(crate::host::PEER_DROP_AFTER_MILLIS)
            + ms(crate::host::DROP_CHECK_EVERY_MILLIS)
            + REPLAN_BUDGET
            + bound.retry,
        "the silent sum by name"
    );

    // Ruling Q1, under B-1 choice (a).
    let (limit, gates) = bound.acceptance();
    assert_eq!(limit, ms(15_000), "ruling Q1");
    assert_eq!(
        gates,
        [OutageClass::Owner, OutageClass::SecondaryKill],
        "B-1 (a): a kill is gated, silence is reported"
    );
    for class in gates {
        assert!(
            bound.of(*class) <= limit,
            "{}: {:?} over the {limit:?} it is gated by",
            class.word(),
            bound.of(*class)
        );
    }
    assert!(
        bound.secondary_silent > limit,
        "B-1 is moot if the silent class fits: {:?}",
        bound.secondary_silent
    );
    // The B-1 ruling, option (a): the silent class reported against about 17 s, failed above a
    // loose 25 s, and its expected sum under both.
    let (stated, ceiling) = bound.silent();
    assert_eq!(
        (stated, ceiling),
        (ms(17_000), ms(25_000)),
        "B-1 (a) figures"
    );
    assert!(bound.secondary_silent <= stated && stated < ceiling);

    // Critic S-8: each expected budget inside the code ceiling it stands in for.
    assert_eq!(bound.ceilings.promote_within, ms(16_200), "heal_within");
    assert!(PROMOTE_BUDGET <= bound.ceilings.promote_within, "promote");
    assert!(bound.retry >= bound.ceilings.put, "retry");
    assert_eq!(bound.ceilings.put, put, "the probe's put timeout");
}

/// #81 X2: `grants/1` as a frozen record of node 1's boot `boot`, its expiry long past.
fn frozen_grant(boot: u64) -> Bytes {
    GrantRecord {
        grant: GrantId(1),
        node: NodeId(1),
        boot: BootId(boot),
        authority_generation: AuthorityGeneration::default(),
        expiry_utc_ms: 1,
        frozen: true,
    }
    .encode()
}

/// `grants/1`'s record and its store revision.
fn grant_1_at(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
) -> Option<(GrantRecord, u64)> {
    rt.block_on(store.get(GetRequest {
        key: Bytes::from(ControlKey::Grant(NodeId(1)).encode()),
    }))
    .expect("the store answers")
    .record
    .map(|found| {
        (
            GrantRecord::decode(&found.value).expect("a grant record"),
            found.mod_revision,
        )
    })
}

/// The #81 X2 setting over `store`: a frozen `grants/1` of node 1's boot 1, which is gone (the
/// links know node 1 at boot 2), and node 1 owning nothing. Every guard of
/// `clear_frozen_verdict` lets the record go. Returns links on which node 2 is a healthy host,
/// and the frozen record's revision.
fn x2_setting(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>) -> (Arc<Links>, u64) {
    let (links, _host) = healthy(2);
    let (tx, _mail) = std::sync::mpsc::channel::<Msg>();
    links.register(NodeId(1), tx, BootId(2));
    put_raw(
        rt,
        store,
        &ControlKey::Grant(NodeId(1)).encode(),
        &frozen_grant(1),
    );
    let (_, revision) = grant_1_at(rt, store).expect("grants/1 is written");
    (links, revision)
}

/// [`x2_setting`], with node 2's planner holding `planner/grant`: a sweep clears `grants/1`
/// unless a row's store changes something in between. Returns the planner, its memory, and the
/// frozen record's revision.
fn x2_held(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>) -> (Planner, Memory, u64) {
    let (links, revision) = x2_setting(rt, store);
    let planner = planner(2, store, links);
    let mut memory = Memory::default();
    assert!(
        rt.block_on(planner.hold(&mut memory)),
        "node 2's planner holds"
    );
    (planner, memory, revision)
}

/// #81 X2, PR #171 review (stale snapshot in the clear): the sweep's snapshot names node 1 a
/// candidate (frozen `grants/1` of a gone boot, owning nothing). Right after the clear reads
/// `grants/1`, `partitions/7` comes to name node 1 owner. The clear reads `partitions/` after
/// the grant, finds it, and deletes nothing: `grants/1` stays at its revision, and no delete is
/// even sent (`planner_frozen_grant_kept`, `OwnsPartition`, is a debug line). Unit
/// (in-memory store).
/// Cut: judge the grant against the sweep's snapshot, read before it (the old order);
/// `grants/1` is deleted, red.
#[config_log::retcd_test]
fn own81_x2_a_partition_naming_the_node_after_the_grant_read_keeps_the_freeze() {
    const METHOD: &str =
        "own81_x2_a_partition_naming_the_node_after_the_grant_read_keeps_the_freeze";
    let rt = crate::db::reopen_tests::runtime();
    let owning = PartitionRecord {
        partition: PartitionId(7),
        owner: NodeId(1),
        ..PartitionRecord::default()
    };
    let after = crate::test_store::PutAfterGet::new(
        Arc::new(config_testkit::MemStore::new()),
        &ControlKey::Grant(NodeId(1)).encode(),
        &ControlKey::Partition(PartitionId(7)).encode(),
        owning.encode(),
    );
    let store: Arc<dyn ConfigStore> = Arc::clone(&after) as Arc<dyn ConfigStore>;
    let (planner, mut memory, revision) = x2_held(&rt, &store);
    after.arm();
    assert!(rt.block_on(planner.sweep(&mut memory)).is_empty());
    assert_eq!(messages(METHOD, "test_put_after_get").len(), 1);
    assert_eq!(
        grant_1_at(&rt, &store).map(|(_, at)| at),
        Some(revision),
        "grants/1 is not deleted"
    );
    assert!(messages(METHOD, "planner_frozen_grant_cleared").is_empty());
    assert!(
        messages(METHOD, "planner_frozen_grant_not_cleared").is_empty(),
        "kept by the verdict, not by a conflict on the delete"
    );
}

/// #81 X2, PR #171 review: right after the clear reads `grants/1`, a later ownership cycle
/// freezes node 1's live boot 2 there. The verdict judged boot 1's record, and the delete goes
/// at that read's revision, so it conflicts and deletes nothing: boot 2's frozen record stays
/// (`planner_frozen_grant_not_cleared`). The next sweep finds boot 2's record and keeps it
/// (`BootNotOlder`). Unit (in-memory store).
/// Cut: the delete at no expected revision; boot 2's freeze is deleted, red.
#[config_log::retcd_test]
fn own81_x2_a_grant_refrozen_after_the_read_is_not_deleted() {
    const METHOD: &str = "own81_x2_a_grant_refrozen_after_the_read_is_not_deleted";
    let rt = crate::db::reopen_tests::runtime();
    let after = crate::test_store::PutAfterGet::new(
        Arc::new(config_testkit::MemStore::new()),
        &ControlKey::Grant(NodeId(1)).encode(),
        &ControlKey::Grant(NodeId(1)).encode(),
        frozen_grant(2),
    );
    let store: Arc<dyn ConfigStore> = Arc::clone(&after) as Arc<dyn ConfigStore>;
    let (planner, mut memory, revision) = x2_held(&rt, &store);
    after.arm();
    for _ in 0..2 {
        assert!(rt.block_on(planner.sweep(&mut memory)).is_empty());
    }
    let (record, at) = grant_1_at(&rt, &store).expect("grants/1 is not deleted");
    assert!(record.frozen && record.boot == BootId(2), "{record:?}");
    assert!(at > revision, "boot 2's freeze, written after the read");
    assert!(messages(METHOD, "planner_frozen_grant_cleared").is_empty());
    assert_eq!(
        messages(METHOD, "planner_frozen_grant_not_cleared").len(),
        1,
        "the first sweep's delete conflicts"
    );
}

/// #81 X2, PR #171 review (fail closed): the clear's own `partitions/` read, made after its
/// `grants/1` read, fails. The clear is refused (`planner_frozen_grant_partitions_unread`) and
/// `grants/1` stays. The clear is called as the sweep calls it, so only its own read fails.
/// Unit.
/// Cut: judge on an empty list when the read fails; `grants/1` is deleted, red.
#[config_log::retcd_test]
fn own81_x2_a_failed_partitions_read_refuses_the_clear() {
    const METHOD: &str = "own81_x2_a_failed_partitions_read_refuses_the_clear";
    let rt = crate::db::reopen_tests::runtime();
    let scripted = Arc::new(crate::test_store::Scripted::new(
        crate::test_store::Script::default(),
    ));
    let store: Arc<dyn ConfigStore> = Arc::clone(&scripted) as Arc<dyn ConfigStore>;
    let (planner, memory, revision) = x2_held(&rt, &store);
    scripted.rescript(crate::test_store::Script {
        fail_lists: 1,
        ..crate::test_store::Script::default()
    });
    rt.block_on(planner.clear_frozen(NodeId(1), memory.held));
    let unread = messages(METHOD, "planner_frozen_grant_partitions_unread");
    assert_eq!(unread.len(), 1, "{unread:?}");
    assert_eq!(unread[0]["cause"], "partitions_list_failed", "{unread:?}");
    assert_eq!(
        grant_1_at(&rt, &store).map(|(_, at)| at),
        Some(revision),
        "grants/1 stays"
    );
    assert!(messages(METHOD, "planner_frozen_grant_cleared").is_empty());
}

/// #81 X2, PR #171 review (re-check the planner's grant): node 2's planner holds on a held
/// clock, and its grant runs out after the sweep started and before the clear deletes. The
/// clear checks the grant again ([`Planner::still_held`]) and deletes nothing
/// (`planner_frozen_grant_skipped_grant_expired`). Unit.
/// Cut: no re-check before the delete; `grants/1` is deleted, red.
#[config_log::retcd_test]
fn own81_x2_a_clear_after_the_planner_grant_ran_out_deletes_nothing() {
    const METHOD: &str = "own81_x2_a_clear_after_the_planner_grant_ran_out_deletes_nothing";
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let (links, revision) = x2_setting(&rt, &store);
    let (clock, hold, _) = held_clock();
    let planner = Planner {
        clock,
        ..planner(2, &store, links)
    };
    // Past the frozen record's `E + eps + delta` (E = 1), so only the planner's grant is left.
    hold.advance(takeover_floor(&planner.budgets));
    let mut memory = Memory::default();
    assert!(
        rt.block_on(planner.hold(&mut memory)),
        "holds on the held clock"
    );
    hold.advance(planner.budgets.grant_millis);
    rt.block_on(planner.clear_frozen(NodeId(1), memory.held));
    assert_eq!(
        messages(METHOD, "planner_frozen_grant_skipped_grant_expired").len(),
        1
    );
    assert_eq!(
        grant_1_at(&rt, &store).map(|(_, at)| at),
        Some(revision),
        "grants/1 stays"
    );
    assert!(messages(METHOD, "planner_frozen_grant_cleared").is_empty());
}

/// #81 X2, PR #171 review (CAS conflict): the frozen `grants/1` is written again, unchanged,
/// right after the clear reads it, so the delete at the read's revision conflicts. The clear
/// is refused and logged (`planner_frozen_grant_not_cleared`), and nothing is deleted; the next
/// sweep reads the new revision and clears it (`planner_frozen_grant_cleared`). Unit.
/// Cut: the delete at no expected revision; the first sweep clears, red.
#[config_log::retcd_test]
fn own81_x2_a_conflicting_clear_is_refused_then_retried() {
    const METHOD: &str = "own81_x2_a_conflicting_clear_is_refused_then_retried";
    let rt = crate::db::reopen_tests::runtime();
    let after = crate::test_store::PutAfterGet::new(
        Arc::new(config_testkit::MemStore::new()),
        &ControlKey::Grant(NodeId(1)).encode(),
        &ControlKey::Grant(NodeId(1)).encode(),
        frozen_grant(1),
    );
    let store: Arc<dyn ConfigStore> = Arc::clone(&after) as Arc<dyn ConfigStore>;
    let (planner, mut memory, revision) = x2_held(&rt, &store);
    after.arm();
    assert!(rt.block_on(planner.sweep(&mut memory)).is_empty());
    let refused = messages(METHOD, "planner_frozen_grant_not_cleared");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0]["revision"], revision, "{refused:?}");
    let (_, moved) = grant_1_at(&rt, &store).expect("nothing deleted on the conflict");
    assert!(moved > revision);
    assert!(messages(METHOD, "planner_frozen_grant_cleared").is_empty());
    assert!(rt.block_on(planner.sweep(&mut memory)).is_empty());
    assert_eq!(grant_1_at(&rt, &store), None, "the retry clears it");
    let cleared = messages(METHOD, "planner_frozen_grant_cleared");
    assert_eq!(cleared.len(), 1, "{cleared:?}");
    assert_eq!(cleared[0]["revision"], moved, "{cleared:?}");
}

/// #81 X2, PR #171 review (delete error): the clear's delete fails `Unavailable`. It is warned
/// (`planner_frozen_grant_clear_failed`) and nothing is deleted; the next sweep clears it. Unit.
/// Cut: the error arm logs nothing; red at the warn.
#[config_log::retcd_test]
fn own81_x2_a_failed_clear_is_warned_then_retried() {
    const METHOD: &str = "own81_x2_a_failed_clear_is_warned_then_retried";
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(crate::test_store::Scripted::new(
        crate::test_store::Script {
            fail_delete_with: Some(ConfigError::Unavailable {
                reason: "scripted: the delete failed".to_owned(),
            }),
            ..crate::test_store::Script::default()
        },
    ));
    let (planner, mut memory, revision) = x2_held(&rt, &store);
    assert!(rt.block_on(planner.sweep(&mut memory)).is_empty());
    let failed = messages(METHOD, "planner_frozen_grant_clear_failed");
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0]["revision"], revision, "{failed:?}");
    assert_eq!(
        grant_1_at(&rt, &store).map(|(_, at)| at),
        Some(revision),
        "nothing deleted on the error"
    );
    assert!(rt.block_on(planner.sweep(&mut memory)).is_empty());
    assert_eq!(grant_1_at(&rt, &store), None, "the retry clears it");
    assert_eq!(messages(METHOD, "planner_frozen_grant_cleared").len(), 1);
}

/// #127: a panic in the planner is logged as `planner_panicked` at error, naming the node and
/// the panic's message, for a `&str` panic and a formatted one. A planner that ends normally
/// logs nothing.
/// Cut: `panics_logged` awaits the future with no `catch_unwind`; the panic escapes the row, red.
#[config_log::retcd_test]
fn i127_a_planner_panic_is_logged() {
    const METHOD: &str = "i127_a_planner_panic_is_logged";
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    rt.block_on(panics_logged(NodeId(2), async { panic!("i127 static") }));
    rt.block_on(panics_logged(NodeId(3), async {
        panic!("i127 formatted {}", 7)
    }));
    rt.block_on(panics_logged(NodeId(4), async {}));
    let lines = messages(METHOD, "planner_panicked");
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert_eq!(lines[0]["@l"], "Error", "{lines:?}");
    assert_eq!(lines[0]["node"], 2, "{lines:?}");
    assert_eq!(lines[0]["panic"], "i127 static", "{lines:?}");
    assert_eq!(lines[1]["node"], 3, "{lines:?}");
    assert_eq!(lines[1]["panic"], "i127 formatted 7", "{lines:?}");
}

/// #122 cost, bounded: an owner whose grant ran out is looked at before it is judged, and an
/// owner that never answers holds its judge for the whole look. A sweep judges every owner at
/// once, so four such owners cost one look, not four: a dead owner's failover is not held
/// back by other owners that are slow in the same sweep (t1 S3, 15 s bound).
///
/// The proof is that all four looks are outstanding at the same time, not a wall-clock cap
/// (PR #156 review: a cap fails on a loaded host even when judging is parallel). The row holds
/// every look it receives, unanswered, until it holds all four; only then does it let them go,
/// by dropping their reply channels. Each look waits longer than the row's patience, so a
/// look still held when the next arrives was outstanding beside it.
/// Cut: judge the owners in turn in `judge_all`; the second look never comes while the first
/// is held, red.
#[config_log::retcd_test]
fn i122_owners_are_judged_at_once() {
    let rt = crate::db::reopen_tests::runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(crate::test_store::Scripted::new(
        crate::test_store::Script::default(),
    ));
    let links = Links::new();
    let mut mailboxes = Vec::new();
    for node in 1..=4u32 {
        let (tx, rx) = std::sync::mpsc::channel::<Msg>();
        links.register(NodeId(node), tx, BootId(1));
        mailboxes.push(rx);
    }
    let patience = crate::host::test_patience(Duration::from_secs(5));
    let look_within = patience * 2;
    let planner = Planner {
        look_within,
        ..planner(9, &store, links)
    };
    // Takes each owner's look and keeps it, reply channel and all, until it holds all four or
    // runs out of patience; then drops them, which answers each judge at once.
    let holder = std::thread::spawn(move || {
        let started = Instant::now();
        let deadline = started + patience;
        let mut held = Vec::new();
        for mailbox in &mailboxes {
            match mailbox.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(look @ Msg::Inspect { .. }) => held.push(look),
                Ok(other) => panic!("not a look: {other:?}"),
                Err(_) => break,
            }
        }
        (held.len(), started.elapsed())
    });
    let verdicts = rt.block_on(
        planner.judge_all((1..=4u32).map(|node| (NodeId(node), None, PartitionId(node)))),
    );
    let (outstanding, took) = holder.join().expect("the holder ends");
    assert_eq!(
        outstanding, 4,
        "all four looks were outstanding at once within {patience:?}"
    );
    assert!(
        took < look_within,
        "held before the first look gave up: {took:?}"
    );
    assert_eq!(verdicts, vec![Verdict::Dead("silent_and_expired"); 4]);
}
