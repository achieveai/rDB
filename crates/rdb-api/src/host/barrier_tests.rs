//! Issue #90 (#80 S5d): the durability barrier on three real hosts, design section 5.6, rows 37
//! to 47.

use super::note_tests::{formed, restart, tune};
use super::seal_tests::{put, spawn_promote, spawn_promote_dropping};
use super::trio::{logged, Trio};
use super::*;

const P: PartitionId = PartitionId(1);

/// How long these rows give a barrier that is expected to finish.
const PATIENT_MILLIS: u64 = 5_000;

pub(super) fn wait_durable(
    generation: Generation,
    through: Seq,
    remaining_millis: u64,
) -> ClientCall {
    ClientCall::WaitDurable {
        generation,
        through,
        remaining_millis,
    }
}

/// One put on a ready trio, and the position it landed at.
pub(super) fn written(trio: &mut Trio, request: u64, object: &'static [u8]) -> (Generation, Seq) {
    match trio.ask(put(request, object, 5_000)) {
        Answer::Txn { result, .. } => (result.generation, result.seq),
        other => panic!("the put was not answered: {other:?}"),
    }
}

/// Holds every background flush off, so only a barrier's own sync can move `durable`.
fn no_background_flush(trio: &mut Trio) {
    for (host, _, _) in &mut trio.nodes {
        host.next_flush = host.clock.now().plus_millis(600_000);
    }
}

/// Row 37: a barrier over three copies forces a sync on each, and answers before the 100 ms
/// flush could have run: the flush is held off, so `durable` can only have moved for the
/// barrier. Cut: remove the ask, and the barrier times out.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_37_a_barrier_syncs_every_copy_and_answers_ok() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    no_background_flush(&mut trio);

    let (generation, through) = written(&mut trio, 1, b"a");
    let answer = trio.ask(wait_durable(generation, through, PATIENT_MILLIS));
    assert!(matches!(answer, Answer::Durable), "{answer:?}");

    for (host, _, _) in &trio.nodes {
        let durable = host.engine.durable(P, generation);
        assert!(
            durable.0 >= through.0,
            "node {}: durable {} is below {}",
            host.node.0,
            durable.0,
            through.0
        );
    }
}

fn fast_trio(dir: &std::path::Path) -> Trio {
    let mut trio = Trio::new(dir, Trio::fast);
    trio.bootstrap();
    trio.ready();
    trio
}

/// The refusal in `answer`, or a panic naming what came instead.
fn refused(answer: Answer) -> ApiError {
    match answer {
        Answer::Error { error, .. } => error,
        other => panic!("the barrier was not refused: {other:?}"),
    }
}

fn unconfirmed_naming(error: &ApiError, node: u32, others: &[u32]) {
    assert_eq!(error.kind, ErrorKind::DurabilityUnconfirmed, "{error:?}");
    assert!(
        error.detail.contains(&format!("on node {node}")),
        "{error:?}"
    );
    for other in others {
        assert!(
            !error.detail.contains(&format!("on node {other}")),
            "{error:?}"
        );
    }
}

/// Row 38: a copy that cannot be reached fails the barrier, by name; once it catches up, the
/// same barrier answers `Ok`.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_38_a_copy_out_of_reach_is_named_and_catching_up_gives_ok() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    trio.links.hold(NodeId(1), NodeId(3));
    let (generation, through) = written(&mut trio, 1, b"a");

    let error = refused(trio.ask(wait_durable(generation, through, 400)));
    unconfirmed_naming(&error, 3, &[1, 2]);
    assert_eq!(error.retry, rdb_core::RetryRule::BoundedJitter, "{error:?}");
    assert!(!error.no_mutation, "{error:?}");

    trio.links.heal_all();
    let answer = trio.ask(wait_durable(generation, through, PATIENT_MILLIS));
    assert!(matches!(answer, Answer::Durable), "{answer:?}");
}

/// Row 39, the copy's half: a copy that answered a recovery's inventory is sealed and proves
/// nothing more. Cut: remove the sealed check in `durable_refused`, and this is `Ok`.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_39_a_sealed_copy_proves_nothing() {
    const METHOD: &str = "s5d_39_a_sealed_copy_proves_nothing";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    let (generation, through) = written(&mut trio, 1, b"a");
    trio.until("node 3 has applied it", |trio| {
        trio.nodes[0]
            .0
            .replication
            .primary(NodeId(1), P)
            .and_then(|primary| {
                let applied = primary.tracker().peer(CopyId(2))?.progress.buffered_applied;
                (applied.0 >= through.0).then_some(())
            })
    });
    trio.nodes[2].0.replication.seal(NodeId(3), P, generation);

    let error = refused(trio.ask(wait_durable(generation, through, 400)));
    unconfirmed_naming(&error, 3, &[1, 2]);
    let lines = logged!(METHOD, "durable_through_refused");
    assert!(
        lines
            .iter()
            .any(|line| line["node"] == 3 && line["reason"] == "sealed"),
        "{lines:?}"
    );
}

/// Row 39, the restart's half: a restarted copy holds no copy until a catch-up restores a
/// dormant receiver, and refuses in both states, so no barrier gets `Ok` from it.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_39_a_restarted_copy_refuses_until_it_rejoins() {
    const METHOD: &str = "s5d_39_a_restarted_copy_refuses_until_it_rejoins";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    let (generation, through) = written(&mut trio, 1, b"a");
    restart(&mut trio, dir.path(), 3, Trio::fast);

    let answer = trio.ask(wait_durable(generation, through, 400));
    assert!(!matches!(answer, Answer::Durable), "{answer:?}");
    let lines = logged!(METHOD, "durable_through_refused");
    assert!(
        lines
            .iter()
            .any(|line| line["node"] == 3 && line["reason"] == "no_copy"),
        "{lines:?}"
    );

    let committed = trio.nodes[0].0.committed[&P].clone();
    let seed = DormantSeed {
        config: committed.committed.pinned_config.clone(),
        lineage: Lineage {
            partition: P,
            generation,
            owner_epoch: OwnerEpoch(1),
        },
        revision: Revision(1),
    };
    let three = &mut trio.nodes[2].0;
    three.restore_dormant(P, &seed, false);
    assert_eq!(three.durable_refused(P, generation), Some("dormant"));
}

/// Row 40: a promote that keeps the old owner lands on it, and `Host::land` ends the barrier in
/// `GENERATION_CHANGED` at once, far before its own timeout.
/// Integration (~4 s): the promote waits out the grant.
#[config_log::retcd_test]
fn s5d_40_a_promote_keeping_the_old_owner_ends_the_barrier_at_once() {
    const METHOD: &str = "s5d_40_a_promote_keeping_the_old_owner_ends_the_barrier_at_once";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    trio.links.drop_direction(NodeId(1), NodeId(3));
    let (generation, through) = written(&mut trio, 1, b"a");
    let answer = trio.call(wait_durable(generation, through, 600_000));
    trio.until("the barrier is waiting", |trio| {
        (!trio.nodes[0].0.barriers.is_empty()).then_some(())
    });

    let promote = spawn_promote_dropping(&trio, 2, Some(NodeId(3)));
    let error = refused(trio.answer(&answer));
    assert_eq!(error.kind, ErrorKind::GenerationChanged, "{error:?}");
    assert_eq!(
        error.retry,
        rdb_core::RetryRule::RetryAfterRecovery,
        "{error:?}"
    );
    let lines = logged!(METHOD, "durable_barrier_generation_changed");
    assert!(!error.no_mutation, "{error:?}");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["node"], 1, "{lines:?}");
    trio.until("the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");
}

/// Row 40, PR #104 review blocker 1: a promote that drops the old owner commits a newer
/// generation, and node 1, fenced, reads `partitions/1` after its fence (A-R399). Once that read
/// names node 2 at the newer generation, node 1 ends its barrier in `GENERATION_CHANGED`, long
/// before the barrier's own timeout. Cut: take `end_barriers_below` out of the `OwnerLearned`
/// arm, and the barrier waits out its 600 s timeout, so `answer` gives up.
/// Integration (~6 s): the promote waits out the grant, then the after-fence read.
#[config_log::retcd_test]
fn s5d_40_a_promote_dropping_the_old_owner_ends_the_barrier_in_generation_changed() {
    const METHOD: &str =
        "s5d_40_a_promote_dropping_the_old_owner_ends_the_barrier_in_generation_changed";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    trio.links.drop_direction(NodeId(1), NodeId(3));
    let (generation, through) = written(&mut trio, 1, b"a");
    let answer = trio.call(wait_durable(generation, through, 600_000));
    trio.until("the barrier is waiting", |trio| {
        (!trio.nodes[0].0.barriers.is_empty()).then_some(())
    });

    let promote = spawn_promote(&trio, 2);
    let error = refused(trio.answer(&answer));
    assert_eq!(error.kind, ErrorKind::GenerationChanged, "{error:?}");
    assert_eq!(
        error.retry,
        rdb_core::RetryRule::RetryAfterRecovery,
        "{error:?}"
    );
    assert!(!error.no_mutation, "{error:?}");
    let lines = logged!(METHOD, "durable_barrier_generation_changed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["node"], 1, "{lines:?}");
    assert!(
        lines[0]["new"].as_u64() > Some(generation.0),
        "ended by a newer generation: {lines:?}"
    );
    trio.until("the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");
}

/// PR #104 review blocker 3: after that promote, node 1 still holds a primary tracker at the old
/// generation, fenced. A new barrier there is refused the way a write is, and once node 1 has
/// read the new owner the refusal is `NOT_PRIMARY` naming it, which `Db` re-reads the route on
/// (`rereads_owner`), so the retry leaves node 1. Cut: drop the A1 check in `barrier_copies`,
/// and the barrier starts on the stale tracker and ends `DURABILITY_UNCONFIRMED`.
/// Integration (~6 s): the promote waits out the grant, then the after-fence read.
#[config_log::retcd_test]
fn s5d_40_a_fenced_stale_owner_refuses_a_new_barrier_with_the_new_owner() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    let (generation, through) = written(&mut trio, 1, b"a");

    let promote = spawn_promote(&trio, 2);
    trio.until("the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");
    trio.until("node 1 read node 2 as owner after its fence", |trio| {
        (trio.nodes[0].0.owner_after_fence.get(&P) == Some(&Some(NodeId(2)))).then_some(())
    });
    assert!(
        trio.nodes[0].0.replication.primary(NodeId(1), P).is_some(),
        "the row needs node 1's stale primary tracker"
    );

    let error = refused(trio.ask(wait_durable(generation, through, 400)));
    assert_eq!(error.kind, ErrorKind::NotPrimary, "{error:?}");
    assert!(error.detail.contains("names node 2"), "{error:?}");
    assert!(trio.nodes[0].0.barriers.is_empty());
}

/// PR #104 review iteration 5, blocker 1: the same stale tracker before node 1 has learned the
/// replacement. Node 1's control store is cut before the promote, so it never reads the new
/// owner and has no hint to name. A new barrier there is still refused by A1, as
/// `LEASE_EXPIRED` provably mutating nothing, and is never registered. Cut: drop the A1 check
/// in `barrier_copies`, and the barrier is registered on the stale tracker.
/// Integration (~6 s): the promote waits out the grant.
#[config_log::retcd_test]
fn s5d_40_a_fenced_stale_owner_with_no_owner_hint_refuses_a_new_barrier() {
    const METHOD: &str = "s5d_40_a_fenced_stale_owner_with_no_owner_hint_refuses_a_new_barrier";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), tune);
    let faults = super::seal_tests::control_behind_a_cut(&mut trio);
    trio.rt
        .block_on(crate::admin::count_open(
            &*trio.store,
            &super::note_tests::INSTANCE,
            true,
            crate::admin::COUNT_WITHIN,
        ))
        .expect("the open counter");
    trio.bootstrap();
    trio.ready();
    let (generation, through) = written(&mut trio, 1, b"a");

    faults.apply("cut-control");
    let promote = spawn_promote(&trio, 2);
    trio.until("the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");
    assert!(
        trio.nodes[0].0.replication.primary(NodeId(1), P).is_some(),
        "the row needs node 1's stale primary tracker"
    );

    let answer = trio.ask(wait_durable(generation, through, 400));
    assert!(
        !matches!(
            trio.nodes[0].0.owner_after_fence.get(&P),
            Some(Some(NodeId(2)))
        ),
        "node 1 must not have learned the new owner: {:?}",
        trio.nodes[0].0.owner_after_fence.get(&P)
    );
    let error = refused(answer);
    assert_eq!(error.kind, ErrorKind::LeaseExpired, "{error:?}");
    assert!(error.no_mutation, "{error:?}");
    assert!(error.detail.contains("no barrier was started"), "{error:?}");
    assert!(trio.nodes[0].0.barriers.is_empty());
    let lines = logged!(METHOD, "durable_barrier_refused");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["node"], 1, "{lines:?}");
    faults.apply("");
}

/// PR #104 review blocker 2: node 3's real proof of the write, held on its way to node 1 while
/// a promote drops node 1 and commits a newer generation, is delivered after that commit. Node 1
/// is cut from rEtcd, so it cannot read the new owner, and its grant no longer admits: the last
/// proof is credited but the barrier is never `Ok`. Once the cut ends, node 1 reads the new
/// owner and ends it in `GENERATION_CHANGED`. Cut: answer `Durable` in `durable_proved` without
/// the A1 check, and the late proof gives `Ok`.
/// Integration (~8 s): the promote waits out the grant, then the after-fence read.
#[config_log::retcd_test]
fn s5d_40_a_real_proof_held_across_a_promote_is_never_ok() {
    const METHOD: &str = "s5d_40_a_real_proof_held_across_a_promote_is_never_ok";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), tune);
    let faults = super::seal_tests::control_behind_a_cut(&mut trio);
    trio.rt
        .block_on(crate::admin::count_open(
            &*trio.store,
            &super::note_tests::INSTANCE,
            true,
            crate::admin::COUNT_WITHIN,
        ))
        .expect("the open counter");
    trio.bootstrap();
    trio.ready();
    let (generation, through) = written(&mut trio, 1, b"a");
    let answer = trio.call(wait_durable(generation, through, 600_000));
    let (barrier, copy) = trio.until("the barrier is waiting", |trio| {
        trio.nodes[0].0.barriers.iter().next().map(|(id, barrier)| {
            let copy = barrier
                .owed
                .iter()
                .find(|(_, node)| **node == NodeId(3))
                .map(|(copy, _)| *copy)
                .expect("node 3's copy is owed");
            (*id, copy)
        })
    });

    // Node 3's genuine answer, taken out of node 1's mailbox and kept.
    let mut held = None;
    let patience = test_patience(Trio::PATIENCE);
    let started = Instant::now();
    while held.is_none() {
        assert!(
            started.elapsed() < patience,
            "node 3 never proved the write"
        );
        for (host, _, rx) in &mut trio.nodes {
            host.run_due();
            host.drain();
            while let Ok(msg) = rx.try_recv() {
                let ours = matches!(
                    &msg,
                    Msg::DurableProved { copy: from, barrier: id, .. } if *from == copy && *id == barrier
                );
                if host.node == NodeId(1) && ours {
                    held = Some(msg);
                    continue;
                }
                host.handle(msg);
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    trio.until("every other copy proved it", |trio| {
        (trio.nodes[0].0.barriers[&barrier].owed.len() == 1).then_some(())
    });

    faults.apply("cut-control");
    let promote = spawn_promote(&trio, 2);
    trio.until("the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");

    trio.nodes[0].0.handle(held.expect("held above"));
    let until = Instant::now() + Duration::from_millis(500);
    while Instant::now() < until {
        trio.step();
        if let Ok(got) = answer.try_recv() {
            panic!("the barrier answered after the promote: {got:?}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        trio.nodes[0].0.barriers[&barrier].owed.is_empty(),
        "the late proof was credited"
    );
    assert_eq!(logged!(METHOD, "durable_barrier_withheld").len(), 1);

    faults.apply("");
    let error = refused(trio.answer(&answer));
    assert_eq!(error.kind, ErrorKind::GenerationChanged, "{error:?}");
}

/// Row 40: a faulted host answers its barriers instead of leaving the caller to its own timeout.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_40_a_host_fault_answers_a_waiting_barrier() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    trio.links.drop_direction(NodeId(1), NodeId(3));
    let (generation, through) = written(&mut trio, 1, b"a");
    let answer = trio.call(wait_durable(generation, through, 600_000));
    trio.until("the barrier is waiting", |trio| {
        (!trio.nodes[0].0.barriers.is_empty()).then_some(())
    });

    trio.nodes[0]
        .0
        .set_fault(Fault::from("test fault".to_owned()));
    assert!(trio.nodes[0].0.barriers.is_empty());
    let got = answer.try_recv().expect("set_fault answered it");
    let error = refused(got);
    assert!(error.detail.contains("test fault"), "{error:?}");
}

/// Row 40: a position from a generation this primary is not at ends at once.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_40_an_old_generation_on_arrival_is_generation_changed() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    let (generation, through) = written(&mut trio, 1, b"a");
    let stale = Generation(generation.0 - 1);
    let error = refused(trio.ask(wait_durable(stale, through, 600_000)));
    assert_eq!(error.kind, ErrorKind::GenerationChanged, "{error:?}");
    assert_eq!(
        error.retry,
        rdb_core::RetryRule::RetryAfterRecovery,
        "{error:?}"
    );
    assert!(!error.no_mutation, "{error:?}");
}

/// Row 40: a proof for another generation or a lower position credits nothing, and the one that
/// matches does, so the row's own proofs do address this barrier.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_40_a_proof_for_another_position_credits_nothing() {
    const METHOD: &str = "s5d_40_a_proof_for_another_position_credits_nothing";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    trio.links.drop_direction(NodeId(1), NodeId(3));
    let (generation, through) = written(&mut trio, 1, b"a");
    let answer = trio.call(wait_durable(generation, through, 600_000));
    let barrier = trio.until("only node 3 is still owed", |trio| {
        trio.nodes[0]
            .0
            .barriers
            .iter()
            .find(|(_, barrier)| barrier.owed.len() == 1)
            .map(|(id, _)| *id)
    });
    let copy = CopyId(2);
    let proof = |generation: Generation, through: Seq| Msg::DurableProved {
        partition: P,
        barrier,
        copy,
        generation,
        through,
    };
    trio.nodes[0]
        .0
        .handle(proof(Generation(generation.0 + 1), through));
    trio.nodes[0]
        .0
        .handle(proof(generation, Seq(through.0 - 1)));
    trio.nodes[0].0.handle(Msg::DurableProved {
        partition: PartitionId(2),
        barrier,
        copy,
        generation,
        through,
    });
    assert_eq!(trio.nodes[0].0.barriers[&barrier].owed.len(), 1);
    assert_eq!(logged!(METHOD, "durable_proof_not_credited").len(), 3);

    trio.nodes[0].0.handle(proof(generation, through));
    let got = trio.answer(&answer);
    assert!(matches!(got, Answer::Durable), "{got:?}");
}

/// S5d-B2: `committed` can be stale, so its `ReadOnly` refuses a barrier only when it is the
/// current generation's. Cut: drop the generation comparison, and the first barrier is refused.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_40_read_only_refuses_only_at_the_current_generation() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    let (generation, through) = written(&mut trio, 1, b"a");
    {
        let committed = trio.nodes[0].0.committed.get_mut(&P).expect("committed");
        committed.mode = PartitionMode::ReadOnly;
        committed.new_generation = Generation(generation.0 - 1);
    }
    let answer = trio.ask(wait_durable(generation, through, PATIENT_MILLIS));
    assert!(matches!(answer, Answer::Durable), "{answer:?}");

    trio.nodes[0]
        .0
        .committed
        .get_mut(&P)
        .expect("committed")
        .new_generation = generation;
    let error = refused(trio.ask(wait_durable(generation, through, PATIENT_MILLIS)));
    assert_eq!(error.kind, ErrorKind::RecoveryReadOnly, "{error:?}");
}

/// Row 43: a copy already durable at the position answers without syncing again, and a full
/// table refuses a new barrier.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_43_a_durable_copy_answers_at_once_and_a_full_table_is_overloaded() {
    const METHOD: &str = "s5d_43_a_durable_copy_answers_at_once_and_a_full_table_is_overloaded";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    let (generation, through) = written(&mut trio, 1, b"a");
    let once = trio.ask(wait_durable(generation, through, PATIENT_MILLIS));
    assert!(matches!(once, Answer::Durable), "{once:?}");
    let before = logged!(METHOD, "durable_through_fast").len();
    let again = trio.ask(wait_durable(generation, through, PATIENT_MILLIS));
    assert!(matches!(again, Answer::Durable), "{again:?}");
    assert_eq!(
        logged!(METHOD, "durable_through_fast").len(),
        before + 3,
        "all three answered from what they already held"
    );

    let host = &mut trio.nodes[0].0;
    let deadline = host.clock.now().plus_millis(600_000);
    let lineage = host
        .replication
        .primary(host.node, P)
        .expect("node 1 is primary")
        .tracker()
        .lineage();
    for id in 0..u64::try_from(MAX_BARRIERS).expect("fits") {
        let (reply, _) = mpsc::channel();
        host.barriers.insert(
            id + 1_000_000,
            Barrier {
                partition: P,
                generation,
                lineage,
                through,
                deadline,
                started: Instant::now(),
                copies: 1,
                // A copy no member holds: never asked and never proved, so each one stays.
                owed: BTreeMap::from([(CopyId(99), NodeId(99))]),
                asked: BTreeSet::new(),
                withheld: false,
                reply,
            },
        );
    }
    let error = refused(trio.ask(wait_durable(generation, through, PATIENT_MILLIS)));
    assert_eq!(error.kind, ErrorKind::Overloaded, "{error:?}");
}

/// Row 45: a copy held behind by a dropped direction fails the barrier, by name, even though the
/// other two have proved it. Cut: finish at a majority of proofs, and this is `Ok`.
/// Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn s5d_45_a_majority_of_proofs_is_not_enough() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = fast_trio(dir.path());
    trio.links.drop_direction(NodeId(1), NodeId(2));
    let (generation, through) = written(&mut trio, 1, b"a");
    let error = refused(trio.ask(wait_durable(generation, through, 600)));
    unconfirmed_naming(&error, 2, &[1, 3]);
}

/// Row 44's cut: each recorded WAL back to its last pre-fsync length, every other WAL gone.
fn cut_to_recorded(node_dir: &std::path::Path, recorded: &BTreeMap<PathBuf, u64>) {
    for entry in std::fs::read_dir(node_dir).expect("the node directory") {
        let path = entry.expect("a directory entry").path();
        if path.extension().is_none_or(|ext| ext != "log") {
            continue;
        }
        match recorded.get(&path) {
            Some(length) => std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("the WAL opens")
                .set_len(*length)
                .expect("the WAL is cut"),
            None => std::fs::remove_file(&path).expect("the unrecorded WAL is removed"),
        }
    }
}

/// Row 44: all three hosts lose everything past their last fsync, and the write that got `Ok`
/// is still there at a `Db` open. The background flush is held off, so only the barrier's own
/// sync can have covered it. Cut: the copy answers at once, past `sync_answer`, and the write
/// is cut away.
/// Integration (~4 s): a formed trio, then a `Db` open over its stores.
#[config_log::retcd_test]
fn s5d_44_a_barrier_ok_survives_every_copy_cut_at_its_last_fsync() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    let node_dirs: Vec<PathBuf> = (1..=3).map(|n| dir.path().join(n.to_string())).collect();
    for node_dir in &node_dirs {
        watch_wal_lengths(node_dir);
    }
    no_background_flush(&mut trio);
    let (generation, through) = written(&mut trio, 1, b"a");
    let answer = trio.ask(wait_durable(generation, through, PATIENT_MILLIS));
    assert!(matches!(answer, Answer::Durable), "{answer:?}");

    let store = Arc::clone(&trio.store);
    drop(trio);
    let reopened = dir.path().join("reopened");
    std::fs::create_dir_all(reopened.join("nodes")).expect("the nodes directory");
    for (n, node_dir) in node_dirs.iter().enumerate() {
        cut_to_recorded(node_dir, &watched_wal_lengths(node_dir));
        std::fs::rename(node_dir, reopened.join("nodes").join((n + 1).to_string()))
            .expect("the copy moves under the Db's layout");
    }

    let rt = crate::db::reopen_tests::runtime();
    let db = rt
        .block_on(crate::db::Db::open_with(
            crate::db::reopen_tests::config(&reopened),
            store,
            rt.handle().clone(),
            crate::db::reopen_tests::fast(),
        ))
        .expect("the reopen");
    db.wait_ready(test_patience(Duration::from_secs(10)))
        .expect("the reopen is ready");
    let got = db.get(b"a").expect("the read");
    let (_, value) = got.value.expect("the write that got Ok is present");
    assert_eq!(value.as_ref(), b"v");
}

/// Row 46: a held link leaves node 2 behind, and O and node 3 both proved the write. The new
/// generation is committed below it, and O's barrier is still never `Ok`: a copy is never out of
/// the owed set for being unreachable. It ends in `GENERATION_CHANGED` once O reads the new owner
/// after its fence. Cut: drop a copy from `owed` when `Host::linked` says it is down, and this
/// is `Ok`.
/// Integration (~6 s): the promote waits out the grant, then the after-fence read.
#[config_log::retcd_test]
fn s5d_46_a_network_cut_never_gives_ok_for_a_write_the_new_history_lacks() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    trio.links.hold(NodeId(1), NodeId(2));
    let (generation, through) = written(&mut trio, 1, b"a");
    let answer = trio.call(wait_durable(generation, through, 12_000));
    trio.until(
        "node 3's proof reached node 1, or the barrier ended",
        |trio| {
            trio.nodes[0]
                .0
                .barriers
                .values()
                .all(|barrier| barrier.owed.len() == 1 && barrier.owed.contains_key(&CopyId(1)))
                .then_some(())
        },
    );
    trio.nodes[2].0.refuse_inventories = true;

    let promote = spawn_promote(&trio, 2);
    trio.until("the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");
    let committed = trio.nodes[1]
        .0
        .committed
        .get(&P)
        .filter(|result| result.new_generation > generation)
        .cloned()
        .expect("node 2 committed the new generation");
    assert!(
        committed.selected.cutoff_seq.0 < through.0,
        "the new history must lack the write: cutoff {:?}",
        committed.selected.cutoff_seq
    );

    // Node 1 learns the promote from `partitions/1` after its fence and ends the barrier in
    // `GENERATION_CHANGED` (PR #104 review); before that, it named node 2's copy at the timeout.
    // Either way the claim is the same: never `Ok`.
    let error = refused(trio.answer(&answer));
    assert_eq!(error.kind, ErrorKind::GenerationChanged, "{error:?}");
    assert!(
        trio.nodes[0].0.barriers.is_empty(),
        "the barrier was ended, not left to its timeout"
    );
}
