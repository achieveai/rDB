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
        planner(2, &store, Links::new()),
        planner(3, &store, Links::new()),
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
    let links = Links::new();
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
