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
/// grant id; the old holder logs the loss on its next pass.
/// Cut: write the grant unconditionally (`expected_mod_revision: None`), both hold, red.
#[config_log::retcd_test]
fn own81_r3_two_planners_hold_one_grant_and_take_over_only_after_expiry() {
    const METHOD: &str = "own81_r3_two_planners_hold_one_grant_and_take_over_only_after_expiry";
    let rt = crate::db::reopen_tests::runtime();
    // Every put waits at the gate, so both planners read "no grant" before either writes: the
    // race the CAS has to settle, not one hold after the other.
    let gated = Arc::new(crate::test_store::Scripted::gated());
    let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
    let (a, b) = (
        planner(2, &store, healthy_links(2)),
        planner(3, &store, healthy_links(3)),
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
    let first = planner_grant(&rt, &store).expect("a grant was written");
    assert_eq!((first.node, first.grant), (winner.node, GrantId(1)));
    assert!(!rt.block_on(loser.hold(&mut ml)), "the grant is live");

    let started = std::time::Instant::now();
    let deadline = started + Duration::from_secs(5);
    while !rt.block_on(loser.hold(&mut ml)) {
        assert!(std::time::Instant::now() < deadline, "no takeover");
        std::thread::sleep(Duration::from_millis(10));
    }
    let taken = planner_grant(&rt, &store).expect("a grant");
    assert_eq!((taken.node, taken.grant), (loser.node, GrantId(2)));
    let budgets = loser.budgets;
    let floor = budgets.grant_millis + budgets.clock_error_millis + budgets.dispatch_margin_millis;
    let waited = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    assert!(
        waited + 50 >= floor,
        "taken over {waited} ms after the last renewal, before G+eps+delta={floor}"
    );
    assert!(!rt.block_on(winner.hold(&mut mw)), "the old holder lost it");
    let lost = messages(METHOD, "planner_grant_lost");
    assert!(
        lost.iter().any(|line| line["node"] == winner.node.0),
        "{lost:?}"
    );
    assert_eq!(messages(METHOD, "planner_grant_acquired").len(), 2);
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
