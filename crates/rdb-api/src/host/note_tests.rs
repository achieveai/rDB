//! Issue #80 S5n: a recovery that commits without an old member says so, per partition, across
//! restarts (design section 5.4.1, rows 31 to 35). The F1 half of those rows is in
//! `rdb-core/tests/recovery_f1.rs`, the layout half in `authority::partition`, the `Fencing`
//! half in `admin`, and a reopen of every node in `db::reopen_tests`.

use super::seal_tests::{only_until, put, spawn_promote};
use super::trio::{logged, Trio};
use super::*;
use crate::net::faults::{CutStore, Faults};
use rdb_core::authority::partition::committed_without_of;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::recovery::WithoutEntry;

const P: PartitionId = PartitionId(1);
const INSTANCE: [u8; INSTANCE_LEN] = [9; INSTANCE_LEN];

fn tune(host: &mut Host) {
    Trio::fast(host);
    host.engine.bind_instance(INSTANCE).expect("bind instance");
    host.instance = Some(INSTANCE);
    host.budgets.grant_millis = 1_500;
    host.budgets.renew_millis = 300;
}

fn formed(dir: &std::path::Path, tuned: impl Fn(&mut Host)) -> Trio {
    let mut trio = Trio::new(dir, tuned);
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
    trio
}

/// `partitions/1` as the store holds it now: its revision and its bytes.
fn record(trio: &Trio) -> (u64, Bytes) {
    let found = trio
        .rt
        .block_on(trio.store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Partition(P).encode()),
        }))
        .expect("get partitions/1")
        .record
        .expect("partitions/1 exists");
    (found.mod_revision, found.value)
}

fn known(status: &NodeStatus) -> Option<(Generation, [u8; INSTANCE_LEN], CommittedWithout)> {
    match &status.committed_without {
        NoteState::Unknown => None,
        NoteState::Known {
            generation,
            instance,
            note,
        } => Some((*generation, *instance, note.clone())),
    }
}

fn committed_generation(trio: &Trio, at: usize) -> Option<Generation> {
    trio.nodes[at]
        .0
        .committed
        .get(&P)
        .map(|result| result.new_generation)
}

/// Node 1 is gone (never stepped again) and node 3 answers the inventory with a failure, so
/// node 2's promote commits on its own copy. Returns the generation committed.
fn lost_primary_and_silent_secondary(trio: &mut Trio) -> Generation {
    assert!(
        matches!(trio.ask(put(1, b"a", 5_000)), Answer::Txn { .. }),
        "the put before the loss is answered"
    );
    let before = committed_generation(trio, 0).expect("the bootstrap committed");
    trio.nodes[2].0.refuse_inventories = true;
    let promote = spawn_promote(trio, 2);
    let generation = only_until(trio, &[1, 2], "node 2 committed", |trio| {
        committed_generation(trio, 1).filter(|committed| *committed > before)
    });
    only_until(trio, &[1, 2], "the promote finished", |_| {
        promote.is_finished().then_some(())
    });
    trio.rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the promote commits");
    only_until(trio, &[1, 2], "node 2 serves what it committed", |trio| {
        known(&trio.status(1)).filter(|(held, _, _)| *held == generation)
    });
    generation
}

/// Row 31 and the non-owner half of row 34, on three real hosts. Cut: `note_after` returns the
/// prior note without the new entry: layout 4 and an empty status, red.
/// Integration (~3 s): the discovery window, once.
#[config_log::retcd_test]
fn s5n_31_34_a_recovery_without_the_primary_and_a_secondary_says_so_on_the_owner_only() {
    const METHOD: &str =
        "s5n_31_34_a_recovery_without_the_primary_and_a_secondary_says_so_on_the_owner_only";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);

    // Row 34: an owner whose commit left nobody out says so, and no other node claims to know.
    let (held, instance, note) = known(&trio.status(0)).expect("the bootstrap owner knows");
    assert_eq!(Some(held), committed_generation(&trio, 0));
    assert_eq!(instance, INSTANCE);
    assert!(note.is_empty(), "{note:?}");
    assert_eq!(known(&trio.status(1)), None, "a secondary does not know");
    assert_eq!(known(&trio.status(2)), None, "a secondary does not know");

    let generation = lost_primary_and_silent_secondary(&mut trio);

    let (_, body) = record(&trio);
    assert_eq!(body[0], 5, "the commit's record carries the note");
    let note = committed_without_of(&body).expect("a record");
    assert_eq!(note.entries.len(), 1, "{note:?}");
    assert_eq!(note.entries[0].generation, generation);
    assert_eq!(
        note.entries[0].copies,
        vec![(CopyId(0), NodeId(1)), (CopyId(2), NodeId(3))],
        "the lost primary and the silent secondary"
    );

    let (held, instance, status_note) = known(&trio.status(1)).expect("the new owner knows");
    assert_eq!((held, instance), (generation, INSTANCE));
    assert_eq!(status_note, note, "status reads what the record holds");
    assert_eq!(
        known(&trio.status(2)),
        None,
        "node 3 landed, and did not commit"
    );

    let lines = logged!(METHOD, "recovery_committed_without");
    let mut named: Vec<_> = lines
        .iter()
        .map(|line| {
            assert_eq!(line["@l"], "Error", "{line}");
            assert_eq!(line["generation"], generation.0, "{line}");
            (line["copy"].as_u64(), line["missing_node"].as_u64())
        })
        .collect();
    named.sort_unstable();
    assert_eq!(named, vec![(Some(0), Some(1)), (Some(2), Some(3))]);
}

/// Node `n` restarts: its host closes and a new one opens its engine in `dir`, as
/// `drop_tests::restart` does, tuned for these rows.
fn restart(trio: &mut Trio, dir: &std::path::Path, n: u32, tuned: impl Fn(&mut Host)) {
    let at = usize::try_from(n - 1).expect("a trio node");
    let (old, _, _) = trio.nodes.remove(at);
    let boot = BootId(old.boot.0 + 1);
    let control = Arc::clone(&old.control);
    drop(old);
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
}

/// Row 34: a restarted owner holds no note, so it says it does not know: never `Known` with an
/// empty note. Cut: `status` answers `Known` empty when nothing is held: red.
/// Integration (~3 s).
#[config_log::retcd_test]
fn s5n_34_a_restarted_owner_before_its_heal_does_not_claim_an_empty_note() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    lost_primary_and_silent_secondary(&mut trio);
    assert!(known(&trio.status(1)).is_some(), "the owner knows");
    restart(&mut trio, dir.path(), 2, tune);
    assert_eq!(known(&trio.status(1)), None, "a new host holds no note");
    for _ in 0..50 {
        trio.nodes[1].0.run_due();
        trio.nodes[1].0.drain();
        assert_eq!(known(&trio.status(1)), None, "no heal has run");
    }
}

/// Rows 32 and 33 on three real hosts: the note outlives the activation, and a move to another
/// owner keeps it and adds that move's own entry. Status reads and writes never touch the
/// record.
/// Integration (~8 s).
#[config_log::retcd_test]
fn s5n_32_33_the_note_survives_activation_and_a_move_and_nothing_else_writes_the_record() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    let generation = lost_primary_and_silent_secondary(&mut trio);
    let original = committed_without_of(&record(&trio).1).expect("a record");
    assert_eq!(original.entries.len(), 1);
    trio.nodes[2].0.refuse_inventories = false;

    // Node 3 comes back. Node 2 re-plans with it and writes again, as `lr resume 3` did by hand.
    restart(&mut trio, dir.path(), 3, tune);
    trio.until("node 2 re-planned with node 3 and admits writes", |trio| {
        let planned = committed_generation(trio, 1).is_some_and(|g| g > generation);
        (planned && trio.status(1).admits == Some(true)).then_some(())
    });
    let (_, body) = record(&trio);
    assert_eq!(body[0], 5, "the activation's record still carries the note");
    assert_eq!(
        committed_without_of(&body).expect("a record").entries[0],
        original.entries[0]
    );
    let held = known(&trio.status(1)).expect("the owner knows");
    assert_eq!(held.2.entries[0], original.entries[0], "{held:?}");
    assert!(held.0 >= generation);

    // Row 33: status reads and client writes do not move `partitions/1`.
    let (revision, _) = record(&trio);
    for request in 10..15 {
        let (reply, answer) = mpsc::channel();
        trio.nodes[1]
            .1
            .send(Msg::Client(Client {
                partition: P,
                call: put(request, b"b", 5_000),
                reply,
            }))
            .expect("node 2's mailbox");
        let got = trio.answer(&answer);
        assert!(matches!(got, Answer::Txn { .. }), "a put: {got:?}");
        let _ = (trio.status(0), trio.status(1), trio.status(2));
    }
    assert_eq!(record(&trio).0, revision, "nothing acknowledges through it");

    // Row 32: a move to node 3 keeps the earlier entry and adds its own.
    let promote = spawn_promote(&trio, 3);
    trio.until("the move finished", |_| promote.is_finished().then_some(()));
    let moved = trio
        .rt
        .block_on(promote)
        .expect("the promote's task")
        .expect("the move commits");
    trio.until("node 3 committed the move", |trio| {
        committed_generation(trio, 2).filter(|g| *g == moved.generation)
    });
    let (after, body) = record(&trio);
    assert!(after > revision, "the recovery moved the record");
    let note = committed_without_of(&body).expect("a record");
    assert!(note.entries.len() >= 2, "{note:?}");
    assert_eq!(note.entries[0], original.entries[0], "{note:?}");
    let (_, _, status) = trio.until("node 3 serves the move", |trio| {
        known(&trio.status(2)).filter(|(held, _, _)| *held == moved.generation)
    });
    assert_eq!(status, note);
}

fn no_shadow(host: &mut Host) {
    tune(host);
    host.restart_shadow = false;
}

/// A recovery that left node 1 and node 3 out, committed on node 2. Then what the row is about:
/// the note node 2 holds is for an older generation than its commit, so it holds no note it may
/// plan from. Returns the note on the record.
fn owner_behind_its_commit(trio: &mut Trio) -> (Generation, CommittedWithout) {
    let generation = lost_primary_and_silent_secondary(trio);
    trio.nodes[2].0.refuse_inventories = false;
    let note = committed_without_of(&record(trio).1).expect("a record");
    assert_eq!(note.entries.len(), 1);
    trio.nodes[1].0.without.insert(
        P,
        (
            Generation(generation.0 - 1),
            INSTANCE,
            CommittedWithout::default(),
        ),
    );
    (generation, note)
}

/// Row 35: with the held note behind the commit, the re-plan reads the record and carries the
/// record's note into the plan. Cut: `drained` uses the held note whatever its generation: the
/// earlier entry is lost, red.
/// Integration (~4 s).
#[config_log::retcd_test]
fn s5n_35_a_replan_behind_the_commit_rereads_the_record_and_keeps_its_note() {
    const METHOD: &str = "s5n_35_a_replan_behind_the_commit_rereads_the_record_and_keeps_its_note";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), no_shadow);
    let (generation, note) = owner_behind_its_commit(&mut trio);
    restart(&mut trio, dir.path(), 1, no_shadow);
    trio.until("node 2 commits the rejoin", |trio| {
        committed_generation(trio, 1).filter(|g| *g > generation)
    });
    assert_eq!(logged!(METHOD, "replan_note_reread").len(), 1);
    let after = committed_without_of(&record(&trio).1).expect("a record");
    assert_eq!(after.entries[0], note.entries[0], "{after:?}");
}

/// Node 2's control store behind a cut the row can apply, as `lr cut-control 2` does.
fn owner_control_behind_a_cut(trio: &mut Trio) -> Arc<Faults> {
    let faults = Faults::new();
    let cut: Arc<dyn config_core::ConfigStore> =
        CutStore::new(Arc::clone(&trio.store), Arc::clone(&faults));
    trio.nodes[1].0.control =
        ControlAdapter::new(cut, trio.rt.handle().clone(), Arc::clone(&trio.links));
    faults
}

/// Row 35: with the read failing no plan is built; once it answers, the plan goes on. Cut:
/// `drained` plans from the held note when the read fails: a `rejoin_plan` while cut, red.
/// Integration (~5 s).
#[config_log::retcd_test]
fn s5n_35_a_replan_whose_read_fails_builds_no_plan_until_it_has_the_note() {
    const METHOD: &str = "s5n_35_a_replan_whose_read_fails_builds_no_plan_until_it_has_the_note";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), no_shadow);
    let (generation, note) = owner_behind_its_commit(&mut trio);
    let faults = owner_control_behind_a_cut(&mut trio);
    faults.apply("cut-control");
    restart(&mut trio, dir.path(), 1, no_shadow);
    trio.until("the read fails", |_| {
        (!logged!(METHOD, "replan_note_read_failed").is_empty()).then_some(())
    });
    assert_eq!(
        logged!(METHOD, "rejoin_plan")
            .iter()
            .filter(|line| line["node"] == 2)
            .count(),
        0,
        "no note, no plan"
    );
    assert_eq!(committed_generation(&trio, 1), Some(generation));

    faults.apply("");
    trio.until("node 2 commits the rejoin", |trio| {
        committed_generation(trio, 1).filter(|g| *g > generation)
    });
    let after = committed_without_of(&record(&trio).1).expect("a record");
    assert_eq!(after.entries[0], note.entries[0], "{after:?}");
}

fn entry_at(generation: Generation) -> WithoutEntry {
    WithoutEntry {
        generation,
        cutoff: Seq(1),
        copies: vec![(CopyId(2), NodeId(3))],
    }
}

/// Row 35: a read that answers for another generation than the commit its re-plan waits on
/// builds nothing. Cut: `replan_note_read` does not compare the generation: a plan, red.
/// Integration (~2 s).
#[config_log::retcd_test]
fn s5n_35_a_replan_note_for_another_generation_builds_no_plan() {
    const METHOD: &str = "s5n_35_a_replan_note_for_another_generation_builds_no_plan";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    let generation = committed_generation(&trio, 0).expect("committed");
    let host = &mut trio.nodes[0].0;
    let epoch = host.committed[&P].selected.root.owner_epoch;
    let waiting = Rejoin::Revoking {
        epoch,
        replan: Replan {
            drop: None,
            rejoiner: None,
            promote: false,
            since: Tick(0),
        },
    };
    host.rejoins.insert(P, waiting);
    host.replan_note_read(
        P,
        epoch,
        Generation(generation.0 + 1),
        CommittedWithout::default(),
    )
    .expect("not a fault");
    assert_eq!(host.rejoins.get(&P), Some(&waiting), "no plan was built");
    assert_eq!(logged!(METHOD, "replan_note_stale").len(), 1);
}

/// Row 35: a re-plan of a quarantined run offers the fresh F1 the run's own prior. Cut:
/// `replan_quarantined` passes `PriorRecord::fresh()`: red.
/// Integration (~2 s).
#[config_log::retcd_test]
fn s5n_35_a_quarantined_replan_carries_the_plans_prior() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = formed(dir.path(), tune);
    let host = &mut trio.nodes[0].0;
    let mut plan = host.plans[&P].clone();
    plan.prior.note = CommittedWithout::default().with(entry_at(Generation(1)));
    let carried = plan.prior.clone();
    let fence = host.fences[&P].clone();
    host.replan_quarantined(P, NodeId(3), &plan, fence);
    host.drain();
    assert_eq!(host.plans[&P].prior, carried);
}
