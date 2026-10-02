//! Rows M7F-25, M7F-30, M7F-31, M7F-32, M7F-33, M7F-34 and M7F-35: `harness::replay::replay` is
//! unavailable and names itself, and all four of `harness::trace::validate`'s checks.
//!
//! | Row | Claim |
//! |---|---|
//! | M7F-25 | `replay(&Trace)` refuses by name, and the refusal excludes every `ReplayOutcome` |
//! | M7F-30 | the positive control: a well-formed trace is `Ok(())`, and it carries every check's subject |
//! | M7F-31 | a non-increasing `event_id` is rejected naming it; a repeating `logical_tick` is **legal** |
//! | M7F-32 | a `Capability` event after the first ordinary event is rejected, and so is a missing package; `C0` inside the opening block is accepted |
//! | M7F-33 | an event the liveness checker folds, before any `SchedulePhaseChanged{Healed}`, is rejected; one position after, accepted |
//! | M7F-34 | a `ReplicationAck` above the **emitting node's** last `BatchApply.seq` is rejected; at that seq, or from another node, accepted |
//! | M7F-35 | a recorded trace, the same trace through a JSONL file, and a hand-built one get one verdict from one validator — clean and defective alike |
//!
//! M7F-30, M7F-32 and M7F-35 landed 2026-09-22, under the foundation Manual Tester's gate and
//! lead rulings F-1, F-2 and F-2a. Nothing else belongs in this file without that gate.
//!
//! Replay is two operations and only one of them needs a runner: **produce** a second trace by
//! re-running the recorded one, and **judge** whether the two are the same run. The judgement
//! (`compare_traces`) landed; the production did not, and this row is the assertion that it has
//! not quietly started answering anyway.
//!
//! An `Identical` from a `replay` that re-ran nothing would be the most dangerous fake in this
//! crate, because every determinism claim in M7 rests on that one answer. `Unreplayable` is the
//! next most dangerous, because it reads like a considered verdict rather than a stub.
//!
//! Log fields are ids and counts; never a key or value byte.

mod support;

use config_log::retcd_test;
use config_log::testing::test_log_dir;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::ids::{
    BootId, ClientId, ConfigVersion, CorrelationId, EventId, Generation, NodeId, OwnerEpoch,
    PartitionId, ReplicaRole, RequestId, Seq, TenantId,
};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    ApplyOutcome, DurabilityClass, PackageId, Provenance, SchedulePhase, Trace, TraceEvent,
    TraceHeader, TraceKind,
};
use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;
use rdb_sim::harness::expected_capability_packages;
use rdb_sim::harness::manifest::resolve;
use rdb_sim::harness::replay::{replay, ReplayOutcome};
use rdb_sim::harness::run::{execute, RunPlan};
use rdb_sim::harness::trace::{read_jsonl, validate, write_jsonl, Recorder, Site, TraceDefect};
use rdb_sim::sim::cluster::ClusterConfig;
use rdb_sim::SimError;

/// The primary, and the two secondaries whose acknowledgements M7F-34 separates.
const PRIMARY: NodeId = NodeId(1);
const SECONDARY_B: NodeId = NodeId(2);
const SECONDARY_C: NodeId = NodeId(3);

/// A header for a one-partition run generated from seed 1.
fn header() -> TraceHeader {
    TraceHeader {
        schema_version: TRACE_SCHEMA_VERSION,
        generator_version: 1,
        provenance: Provenance::Generated { seed: 1 },
        config: resolve(&ClusterConfig::default(), 1_000, &[]).expect("a manifest"),
        partitions: 1,
        topology: Vec::new(),
        oracle_checkpoint_digest: Digest::ROOT,
    }
}

/// A handful of events, in `event_id` order.
///
/// The kind carries no capability state on purpose: `M7V-82` greps this crate's sources for a
/// `CapabilityState` literal outside the module that builds the capability report, and it is
/// right to — a literal there is how a landed package stays unavailable. Test data is not an
/// exception worth carving out.
fn trace() -> Trace {
    Trace {
        header: header(),
        events: (0..3)
            .map(|index| TraceEvent {
                event_id: EventId(index),
                logical_tick: index * 10,
                partition: PartitionId(1),
                node: NodeId(1),
                boot: BootId(1),
                correlation: CorrelationId(1),
                kind: TraceKind::SchedulePhaseChanged {
                    phase: SchedulePhase::Chaotic,
                    fair_delivery: false,
                    remaining_event_budget: 100 - u32::try_from(index).expect("small"),
                },
            })
            .collect(),
    }
}

/// M7F-25: `replay` refuses by name, and the refusal is asserted by a pattern that excludes
/// **every** [`ReplayOutcome`].
///
/// **What turns this red:** a `replay` that answers any `ReplayOutcome` at all without a step
/// loop behind it — including the `Unreplayable` that a hurried stub reaches for because it
/// reads like a decision — or a change to the seam string. The `match` has no `_` arm, so a
/// fourth outcome cannot be added without this row being re-read.
///
/// **Why it is not `m7f_26` again.** `m7f_26` drives `replay` over an **empty** `Trace` and
/// asserts only the seam string. This row hands it a populated trace, so a `replay` that
/// short-circuited on emptiness and answered for a real stream would pass there and fail here.
///
/// **What this row does not claim.** Nothing about determinism. A seed is not a reproducer; the
/// recorded stream is, and until `replay` exists nothing in M7 has shown the kernel
/// deterministic rather than merely usually the same. `compare_traces` answering `Identical` is
/// not that evidence and is deliberately not cited here.
#[retcd_test]
fn m7f_25_harness_replay_is_unavailable_and_names_itself() {
    support::preamble();
    let trace = trace();
    assert_eq!(
        trace.events.len(),
        3,
        "a populated stream, not an empty one"
    );

    match replay(&trace) {
        Err(SimError::Unavailable { seam }) => {
            assert_eq!(
                seam, "harness::replay::replay",
                "the seam string is the function's own path"
            );
            tracing::info!(seam, "m7f_25 seam");
        }
        Err(other) => panic!("replay must refuse as Unavailable, got {other:?}"),
        Ok(ReplayOutcome::Identical) => {
            panic!("an Identical from a replay that re-ran nothing is the fake this row exists for")
        }
        Ok(ReplayOutcome::Diverged { .. }) => {
            panic!("a divergence needs two traces, and there is no runner to produce the second")
        }
        Ok(ReplayOutcome::Unreplayable { .. }) => {
            panic!("Unreplayable reads like a considered verdict; there is nothing to consider")
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The validator's fixtures
// ---------------------------------------------------------------------------------------------

/// One ordinary event of a fixture: where it happens and what it is.
///
/// No `event_id`. [`trace_with`] assigns it by position, which is what makes "the same event one
/// position after the phase change" (M7F-33) a reordering of this list and nothing else — and it
/// is why M7F-31, whose whole subject *is* a wrong `event_id`, writes one by hand afterwards
/// instead of going through here.
struct Ordinary {
    tick: u64,
    node: NodeId,
    kind: TraceKind,
}

/// The nine-event capability preamble, taken from a real recorded run rather than written out.
///
/// Ruling F-2 forbids a hand-written list of nine in the validator **and** a second one in the
/// fixture. The validator derives its expected set from
/// [`expected_capability_packages`]; this takes the fixture's from the run loop, and then
/// asserts the two agree. So no `PackageId` and no `CapabilityState` is spelled in this file,
/// and a package added on one side with nothing on the other fails here rather than drifting.
fn capability_preamble() -> Vec<TraceEvent> {
    let (recorded, report) =
        execute(&RunPlan::new(ClusterConfig::default())).expect("an empty run still records");
    assert_eq!(
        report.events_consumed, 0,
        "an empty plan pops nothing, so the trace is the preamble and nothing else"
    );

    let packages: Vec<PackageId> = recorded
        .events
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::Capability { package, .. } => Some(package),
            _ => None,
        })
        .collect();
    assert_eq!(
        packages,
        expected_capability_packages().to_vec(),
        "the run loop's preamble and the derived expected set are one list, in one order"
    );
    assert_eq!(
        recorded.events.len(),
        packages.len(),
        "the preamble is capability events only"
    );
    tracing::info!(
        packages = format!("{packages:?}"),
        count = packages.len(),
        "derived capability packages"
    );
    recorded.events
}

/// A trace: the recorded capability preamble, then `tail` numbered on from where it stopped.
fn trace_with(tail: Vec<Ordinary>) -> Trace {
    let mut events = capability_preamble();
    for item in tail {
        let event_id = EventId(u64::try_from(events.len()).expect("a small fixture"));
        events.push(TraceEvent {
            event_id,
            logical_tick: item.tick,
            partition: PartitionId(1),
            node: item.node,
            boot: BootId(1),
            correlation: CorrelationId(1),
            kind: item.kind,
        });
    }
    Trace {
        header: header(),
        events,
    }
}

/// Renumber every `event_id` by position.
///
/// A fixture built by **moving** an event carries that event's old id to its new place, which
/// breaks the total order and makes check 1 — not the check under test — the one that rejects
/// it. Renumbering restores a strictly increasing order so the only thing left wrong with the
/// fixture is the thing that was moved. M7F-31 is the row that owns the total order and it
/// deliberately does **not** go through here.
fn renumber(trace: &mut Trace) {
    for (index, event) in trace.events.iter_mut().enumerate() {
        event.event_id = EventId(u64::try_from(index).expect("a small fixture"));
    }
}

/// A `Capability` event for `package`, carrying the state a recorded one already carries.
///
/// The state is lifted off a real recorded preamble rather than written down. `M7V-82` greps
/// this crate's sources for a `CapabilityState` literal, and while its rule is scoped to
/// `src/harness`, this file has spelled neither a `PackageId` nor a `CapabilityState` in its
/// capability fixtures so far and there is no reason to start: the state is not M7F-32's
/// subject. **Position and completeness are**, and `package` is the only thing a caller chooses.
fn capability(package: PackageId) -> TraceKind {
    let state = capability_preamble()
        .into_iter()
        .find_map(|event| match event.kind {
            TraceKind::Capability { state, .. } => Some(state),
            _ => None,
        })
        .expect("a recorded preamble is capability events and nothing else");
    TraceKind::Capability { package, state }
}

/// The phase change that arms the liveness checker. `contracts/trace.rs` calls
/// `SchedulePhaseChanged` "the only thing that arms the liveness checker", which is the finding
/// ruling F-1 rewrote M7F-33 around.
fn healed() -> TraceKind {
    TraceKind::SchedulePhaseChanged {
        phase: SchedulePhase::Healed,
        fair_delivery: true,
        remaining_event_budget: 200,
    }
}

/// **The folded event M7F-33 uses, and why it is this one.** Ruling F-1 leaves the folded set to
/// team verification (INV-LIVE) and tells this row to use one both plans already agree on and to
/// say which. `M7V-30` folds *"one `inflight` request that never reaches a terminal
/// `client_outcome`"*; [`TraceKind::ClientSubmit`] is that request arriving, and it is the only
/// variant either plan names as folded today. The validator's own `folds_into_liveness` says the
/// same thing in one place, so widening the set is one edit there and not a sweep of fixtures.
fn client_submit() -> TraceKind {
    TraceKind::ClientSubmit {
        request: RequestId(1),
        tenant: TenantId(1),
        client: ClientId(1),
        affinity: 1,
        expected_generation: Some(Generation(1)),
        request_digest: Digest::ROOT,
        deadline_remaining_ms: 1_000,
        mutation_keys: Vec::new(),
        condition_keys: Vec::new(),
    }
}

/// A batch reaching `seq` on whichever node the envelope names.
fn batch_apply(seq: u64) -> TraceKind {
    TraceKind::BatchApply {
        role: ReplicaRole::RegularSecondary,
        generation: Generation(1),
        seq: Seq(seq),
        predecessor_seq: Seq(seq - 1),
        predecessor_digest: Digest::ROOT,
        entry_digest: Digest::ROOT,
        batch: seq,
        key_versions: Vec::new(),
        outcome: ApplyOutcome::Applied,
    }
}

/// An acknowledgement of a contiguous prefix, generated at `from`.
///
/// `from_node` is the field the validator keys on — the contract calls it *"the acknowledging
/// node"* — and every caller here sets the envelope's `node` to the same value, because the
/// contract says a `ReplicationAck` is "emitted at the secondary, where the acknowledgement is
/// generated". The two are therefore the same node in any trace the runner could produce, and a
/// fixture that disagreed with itself would be testing
/// [`TraceDefect::AckEmitterDisagreesWithEnvelope`] instead of M7F-34's rule.
fn ack(from: NodeId, contiguous: u64) -> TraceKind {
    TraceKind::ReplicationAck {
        from_node: from,
        to_node: PRIMARY,
        peer_role: ReplicaRole::RegularSecondary,
        peer_boot: BootId(1),
        config_version: ConfigVersion(1),
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        contiguous_seq: Seq(contiguous),
        contiguous_digest: Digest::ROOT,
        durability_class: DurabilityClass::Durable,
        accepted: true,
        reject_reason: None,
    }
}

/// The well-formed trace M7F-31 mutates: the preamble, a healed phase change, two applies **at
/// one `logical_tick`**, an acknowledgement inside its node's applied prefix, and one client
/// submission after the phase change.
///
/// Every later check passes on it, so a rejection from a mutated copy is about the mutation.
fn well_formed() -> Trace {
    trace_with(well_formed_tail())
}

/// [`well_formed`]'s ordinary events, on their own, so M7F-32 can put a tenth capability event
/// in front of them without restating the list.
fn well_formed_tail() -> Vec<Ordinary> {
    vec![
        Ordinary {
            tick: 90,
            node: PRIMARY,
            kind: healed(),
        },
        Ordinary {
            tick: 100,
            node: SECONDARY_B,
            kind: batch_apply(10),
        },
        Ordinary {
            tick: 100,
            node: SECONDARY_C,
            kind: batch_apply(20),
        },
        Ordinary {
            tick: 110,
            node: SECONDARY_B,
            kind: ack(SECONDARY_B, 10),
        },
        Ordinary {
            tick: 120,
            node: PRIMARY,
            kind: client_submit(),
        },
    ]
}

/// M7F-30: the validator accepts a well-formed trace.
///
/// **The positive control the four negative rows rest on.** Without it, M7F-31, M7F-32, M7F-33
/// and M7F-34 all pass on a `validate` that rejects everything it is handed — four green rows
/// over a validator that has never said `Ok` in its life.
///
/// The fixture is the one the plan names: a header, the **nine** `Capability` events (ruling
/// F-2 — nine module-bearing packages, `C0` permitted and not required), a
/// `SchedulePhaseChanged`, then ordinary events in `event_id` order.
///
/// **A control has to be more than `Ok(())`, or it controls nothing.** A fixture that happened
/// to contain no acknowledgement would be accepted by a check 4 that was deleted, and this row
/// would still be green. So the assertions below say that the fixture actually carries each
/// check's subject — a complete opening block, a healed phase change with a folded event after
/// it, two applies and an acknowledgement — before asserting the verdict. Each of those is a
/// thing one of the negative rows mutates, so what is accepted here is the same shape that is
/// rejected there with one field changed.
#[retcd_test]
fn m7f_30_the_validator_accepts_a_well_formed_trace() {
    support::preamble();

    let trace = well_formed();
    let expected = expected_capability_packages();

    // Check 1's subject: one strictly increasing total order.
    let ids: Vec<EventId> = trace.events.iter().map(|event| event.event_id).collect();
    assert!(
        ids.windows(2).all(|pair| pair[0] < pair[1]),
        "the control is in event_id order, {ids:?}"
    );

    // Check 2's subject: the opening block, complete, and nothing after the first ordinary event.
    let opening: Vec<PackageId> = trace
        .events
        .iter()
        .map_while(|event| match event.kind {
            TraceKind::Capability { package, .. } => Some(package),
            _ => None,
        })
        .collect();
    assert_eq!(
        opening,
        expected.to_vec(),
        "the opening block is every expected package, in the recorded order"
    );
    assert_eq!(opening.len(), 9, "nine, and ruling F-2 says why it is nine");
    assert!(
        !trace.events[opening.len()..]
            .iter()
            .any(|event| matches!(event.kind, TraceKind::Capability { .. })),
        "no capability event after the block"
    );

    // Check 3's subject: a healed phase change, and a folded event strictly after it.
    let healed_at = trace
        .events
        .iter()
        .position(|event| matches!(event.kind, TraceKind::SchedulePhaseChanged { .. }))
        .expect("the fixture states a schedule phase");
    let folded_at = trace
        .events
        .iter()
        .position(|event| matches!(event.kind, TraceKind::ClientSubmit { .. }))
        .expect("the fixture carries an event the liveness checker folds");
    assert_eq!(
        healed_at,
        opening.len(),
        "the phase change is the first ordinary event"
    );
    assert!(
        folded_at > healed_at,
        "the folded event is inside the stated phase, {folded_at} after {healed_at}"
    );

    // Check 4's subject: applies on two nodes, and an acknowledgement inside its own prefix.
    let applies = trace
        .events
        .iter()
        .filter(|event| matches!(event.kind, TraceKind::BatchApply { .. }))
        .count();
    let acks = trace
        .events
        .iter()
        .filter(|event| matches!(event.kind, TraceKind::ReplicationAck { .. }))
        .count();
    assert_eq!(applies, 2, "two applies, on two different nodes");
    assert_eq!(
        acks, 1,
        "one acknowledgement for check 4 to have an opinion on"
    );

    assert_eq!(
        validate(&trace),
        Ok(()),
        "a trace the runner could have produced is accepted, or every negative row below is \
         green over a validator that refuses everything"
    );

    tracing::info!(
        events = trace.events.len(),
        packages = opening.len(),
        applies,
        acks,
        "m7f_30 well-formed control"
    );
}

/// M7F-31: the validator rejects a non-increasing `event_id`, and a repeating `logical_tick` is
/// legal.
///
/// Design §4.10: *"`event_id` is a single strictly-increasing total order, so the oracle is a
/// left-to-right fold that never sorts"*. Two arms, because the two ways to break it are not the
/// same edit: a **swap** produces a decrease, a **duplicate** produces equality, and a check
/// written with `<` rather than `<=` accepts the second while rejecting the first.
///
/// **The near-miss twin is the point of the row.** The clean control at the top is a trace whose
/// last three events sit on two `logical_tick` values, two of them sharing one. A validator that
/// reached for time instead of for the total order — the obvious wrong implementation, since
/// `logical_tick` is right there on the envelope — rejects that and fails the first assertion
/// below, before either mutation is applied. Several events at one tick is the normal case: the
/// nine capability lines are all at tick zero.
#[retcd_test]
fn m7f_31_the_validator_rejects_a_non_increasing_event_id() {
    support::preamble();

    let base = well_formed();
    let ticks: Vec<u64> = base.events.iter().map(|event| event.logical_tick).collect();
    assert!(
        ticks.windows(2).any(|pair| pair[0] == pair[1]),
        "the control carries two events at one logical_tick, {ticks:?}"
    );
    assert_eq!(
        validate(&base),
        Ok(()),
        "a repeating logical_tick is legal: the total order is over event_id, not over time"
    );

    // Arm 1: two events swapped. Their ids travel with them, so the order decreases.
    let mut swapped = well_formed();
    let last = swapped.events.len() - 1;
    swapped.events.swap(last - 1, last);
    assert_eq!(
        validate(&swapped),
        Err(TraceDefect::EventIdNotIncreasing {
            previous: EventId(u64::try_from(last).expect("a small fixture")),
            found: EventId(u64::try_from(last - 1).expect("a small fixture")),
        }),
        "rejected, naming the offending event_id and the one it failed to follow"
    );

    // Arm 2: an id repeated. Not a decrease — a check spelled `<` accepts this one.
    let mut duplicated = well_formed();
    duplicated.events[last].event_id = duplicated.events[last - 1].event_id;
    assert_eq!(
        validate(&duplicated),
        Err(TraceDefect::EventIdNotIncreasing {
            previous: EventId(u64::try_from(last - 1).expect("a small fixture")),
            found: EventId(u64::try_from(last - 1).expect("a small fixture")),
        }),
        "two events at one event_id are two events the fold cannot order"
    );

    tracing::info!(events = base.events.len(), "m7f_31 event_id order");
}

/// M7F-32: the validator rejects a capability block that is not first, and one that is not
/// complete.
///
/// Design §4.10 says `TraceKind::Capability` is *"emitted once per package at trace start"*, and
/// M7V-88 rests on it: a checker that meets a capability state it has not already folded cannot
/// honestly report `Unavailable{Capability(p)}` for the events it read before it. So two things
/// have to hold, and they fail differently — **position** and **completeness** — which is why
/// this row is four fixtures and not one.
///
/// **What "complete" means is ruling F-2's, not this row's.** The nine module-bearing packages
/// are required; `PackageId::C0` is permitted and not required, because the contracts crate has
/// no module and emits no capability line. Both halves are asserted: a trace carrying C0 inside
/// the block is **accepted** at ten events, and a trace carrying C0 *after* an ordinary event is
/// rejected. Without the first, "reject any C0 anywhere" passes this row while rejecting
/// M7V-03(b)'s ten-package fixture; without the second, the position rule looks like a
/// completeness rule that happens to be spelled in order.
///
/// **The expected set is derived and never enumerated** (F-2, F-2a). The missing-package arm
/// walks [`expected_capability_packages`] and removes each package in turn, so a tenth
/// module-bearing package added to the run loop is nine-plus-one arms here automatically. No
/// `PackageId` is written in this row except `C0`, which is the one the rule names by name.
///
/// **What turns this red.** Deleting the completeness loop in `capability_block` leaves the
/// missing-package arm green-to-red on all nine packages; deleting the `seen_ordinary` guard
/// leaves both not-first arms red. Neither mutation touches the other's arms, which is the
/// evidence that the two halves of the check are two checks.
#[retcd_test]
fn m7f_32_the_validator_rejects_a_capability_block_that_is_not_first() {
    support::preamble();

    let expected = expected_capability_packages();
    assert_eq!(
        validate(&well_formed()),
        Ok(()),
        "the clean control: nine packages in the opening block is well-formed"
    );

    // Control, and the other half of F-2: `C0` is permitted inside the block, not required.
    let mut with_c0 = well_formed_tail();
    with_c0.insert(
        0,
        Ordinary {
            tick: 0,
            node: PRIMARY,
            kind: capability(PackageId::C0),
        },
    );
    let ten = trace_with(with_c0);
    let in_block = ten
        .events
        .iter()
        .filter(|event| matches!(event.kind, TraceKind::Capability { .. }))
        .count();
    assert_eq!(in_block, 10, "the nine required packages and C0");
    assert_eq!(
        validate(&ten),
        Ok(()),
        "C0 inside the opening block is permitted: M7V-03(b)'s fixture carries ten"
    );

    // Arm 1: one **required** package's capability event moved after the first ordinary event.
    // This is literally M7F-30's trace with one event moved, which is what the plan row says.
    let mut moved = well_formed();
    let lifted = moved.events.remove(0);
    let lifted_package = match &lifted.kind {
        TraceKind::Capability { package, .. } => *package,
        other => panic!("the preamble opens with a capability event, got {other:?}"),
    };
    assert_eq!(
        lifted_package, expected[0],
        "the event lifted is the first expected package's, derived and not named"
    );
    assert!(
        !matches!(moved.events[8].kind, TraceKind::Capability { .. }),
        "with one lifted the block is eight long, so index 8 is the first ordinary event"
    );
    moved.events.insert(9, lifted);
    // The moved event carries its old id, which would make check 1 the one that rejects this.
    renumber(&mut moved);
    assert_eq!(
        validate(&moved),
        Err(TraceDefect::CapabilityOutsideOpeningBlock {
            package: lifted_package,
            event_id: EventId(9),
        }),
        "a required package reporting after an ordinary event is still outside the block"
    );

    // Arm 2: `C0` — permitted, and therefore never the completeness loop's business — after an
    // ordinary event. A validator that enforced position only through the completeness list
    // accepts this one.
    let late_c0 = trace_with(vec![
        Ordinary {
            tick: 90,
            node: PRIMARY,
            kind: healed(),
        },
        Ordinary {
            tick: 100,
            node: SECONDARY_B,
            kind: batch_apply(10),
        },
        Ordinary {
            tick: 110,
            node: PRIMARY,
            kind: capability(PackageId::C0),
        },
    ]);
    assert_eq!(
        validate(&late_c0),
        Err(TraceDefect::CapabilityOutsideOpeningBlock {
            package: PackageId::C0,
            event_id: EventId(11),
        }),
        "permitted says nothing about position"
    );

    // Arm 3: a package missing. Every one of the nine, in turn, because the completeness loop
    // reports the first absence in the derived order and an arm on one package would only ever
    // exercise that package's position in it.
    let intact = well_formed().events.len();
    for package in expected {
        let mut incomplete = well_formed();
        incomplete.events.retain(
            |event| !matches!(event.kind, TraceKind::Capability { package: p, .. } if p == package),
        );
        assert_eq!(
            incomplete.events.len(),
            intact - 1,
            "exactly one capability event removed for {package:?}"
        );
        assert_eq!(
            validate(&incomplete),
            Err(TraceDefect::CapabilityMissing { package }),
            "a package that never reported is named, not silently tolerated"
        );
    }

    tracing::info!(
        packages = expected.len(),
        in_block,
        lifted = ?lifted_package,
        "m7f_32 capability block"
    );
}

/// M7F-33: the validator rejects an event the liveness checker folds when it is placed before
/// any `SchedulePhaseChanged{Healed}`, and accepts the same event one position after.
///
/// **Rewritten by lead ruling F-1 (2026-09-22).** The row named "the liveness-arming event"
/// until then, and there was no such thing on disk: `SchedulePhaseChanged` **is** the arming
/// event, so "X before any `SchedulePhaseChanged`" had no X. The rationale was always sound and
/// is the claim asserted here — *liveness is only claimed inside a stated phase* (spike §6
/// forbids calling an unhealed partition a liveness failure).
///
/// The folded event is [`TraceKind::ClientSubmit`]; [`client_submit`] carries the reasoning.
///
/// **The two arms differ in one thing: position.** Same two kinds, same two ticks, same nodes —
/// the tail list is reversed and the ids follow the positions. So nothing but the ordering rule
/// can separate the verdicts, which is what stops the row passing on a validator that rejects
/// every `ClientSubmit` outright.
///
/// **Inert on recorded traces today, and F-1 says so in advance.** `run::execute` emits neither
/// kind, so no recorded trace can trip this yet. The first time the loop can emit a folded event
/// with no phase change in front of it, every recorded trace goes invalid at once and this will
/// look like the row being too strict. It will not be.
#[retcd_test]
fn m7f_33_the_validator_rejects_a_liveness_folded_event_before_the_arming_phase_change() {
    support::preamble();

    let before = trace_with(vec![
        Ordinary {
            tick: 90,
            node: PRIMARY,
            kind: client_submit(),
        },
        Ordinary {
            tick: 90,
            node: PRIMARY,
            kind: healed(),
        },
    ]);
    let folded_at = before.events[before.events.len() - 2].event_id;
    assert_eq!(
        validate(&before),
        Err(TraceDefect::LivenessFoldedBeforeHealed {
            event_id: folded_at
        }),
        "a folded event before any healed phase arms the checker outside a stated phase"
    );

    // The near-miss twin: the same folded event, one position later.
    let after = trace_with(vec![
        Ordinary {
            tick: 90,
            node: PRIMARY,
            kind: healed(),
        },
        Ordinary {
            tick: 90,
            node: PRIMARY,
            kind: client_submit(),
        },
    ]);
    assert_eq!(
        validate(&after),
        Ok(()),
        "inside a stated phase the same event is exactly what the checker is for"
    );
    assert_eq!(
        before.events.len(),
        after.events.len(),
        "the two fixtures differ in order and in nothing else"
    );

    tracing::info!(?folded_at, "m7f_33 liveness arming order");
}

/// M7F-34: the validator rejects a `ReplicationAck` whose `contiguous_seq` is above the
/// **emitting node's** last `BatchApply.seq`, and accepts it at exactly that seq or from another
/// node that did apply it.
///
/// M7V-88's realizability rule, quoted in the verification plan: *"`replication_ack.contiguous_seq`
/// never above the emitting node's last `batch_apply.seq`"*. This is the check that catches a
/// fixture the runner could never produce.
///
/// **Two twins, and the second is the one with teeth.** All three arms share the same two
/// applies — node B at seq 10, node C at seq 20 — and differ only in who acknowledges what.
///
/// * B acknowledging 20 is rejected: B applied 10.
/// * B acknowledging 10 is accepted: exactly its own last apply, so the boundary is `>` and not
///   `>=`.
/// * **C** acknowledging 20 is accepted, from the identical trace. A validator that compared
///   against the highest `BatchApply` in the *trace* rather than the emitting node's accepts all
///   three and fails the first assertion; one that compared against the lowest rejects all three
///   and fails the third. Only the per-node rule gives these three verdicts.
///
/// **A fourth arm, added 2026-09-22 with the lead's ruling on which field names the emitter.**
/// The three above cannot tell `from_node` from the envelope's `node`, because each gives its
/// node its own apply and sets both fields to it — which is how the check came to be keyed on
/// the envelope, and how it stayed that way through this row being accepted. The fourth arm sets
/// them to different nodes and asserts the trace is refused for *that*, by name.
#[retcd_test]
fn m7f_34_the_validator_rejects_an_ack_above_the_emitting_nodes_last_batch_apply() {
    support::preamble();

    let applies = || {
        vec![
            Ordinary {
                tick: 90,
                node: PRIMARY,
                kind: healed(),
            },
            Ordinary {
                tick: 100,
                node: SECONDARY_B,
                kind: batch_apply(10),
            },
            Ordinary {
                tick: 100,
                node: SECONDARY_C,
                kind: batch_apply(20),
            },
        ]
    };
    let with_ack = |node: NodeId, contiguous: u64| {
        let mut tail = applies();
        tail.push(Ordinary {
            tick: 110,
            node,
            kind: ack(node, contiguous),
        });
        trace_with(tail)
    };

    assert_eq!(
        validate(&with_ack(SECONDARY_B, 20)),
        Err(TraceDefect::AckAboveLastApply {
            node: SECONDARY_B,
            contiguous_seq: Seq(20),
            last_apply: Seq(10),
        }),
        "rejected, naming the node and both sequences: B never applied past 10"
    );
    assert_eq!(
        validate(&with_ack(SECONDARY_B, 10)),
        Ok(()),
        "at exactly its own last apply, an acknowledgement is realizable"
    );
    assert_eq!(
        validate(&with_ack(SECONDARY_C, 20)),
        Ok(()),
        "the same seq from the node that did apply it: the rule is per emitting node"
    );

    // The lead's 2026-09-22 ruling on the key, and the only arm of this row it added. The three
    // above are untouched by it — each gives its node its own apply, so they read the same
    // whether the check keys on `from_node` or on the envelope, which is exactly why the wrong
    // key survived here. This arm is the one that can tell them apart: a trace whose two names
    // for the acknowledging node disagree is unrealizable, and it now says so instead of
    // reporting the other node's applies.
    let mut disagreeing = with_ack(SECONDARY_C, 20);
    let last = disagreeing.events.len() - 1;
    disagreeing.events[last].kind = ack(SECONDARY_B, 20);
    assert_eq!(
        disagreeing.events[last].node, SECONDARY_C,
        "the envelope still says C; only the acknowledgement's own from_node moved to B"
    );
    assert_eq!(
        validate(&disagreeing),
        Err(TraceDefect::AckEmitterDisagreesWithEnvelope {
            event_id: disagreeing.events[last].event_id,
            envelope: SECONDARY_C,
            from_node: SECONDARY_B,
        }),
        "two names for one acknowledging node is its own defect, not an apply comparison"
    );

    tracing::info!(
        rejected = ?SECONDARY_B,
        accepted = ?SECONDARY_C,
        "m7f_34 ack against applies"
    );
}

/// M7F-35: a recorded trace and a hand-built one go through one validator.
///
/// M7V-88 clause 2 says a trace is checked *"through I1's validator"*, singular. A second code
/// path for hand-built traces would let a fixture be well-formed for the oracle and unrealizable
/// by the runner — which is the thing M7V-88 exists to catch, arriving through the door M7V-88
/// came in by. So this row asserts the singular twice over: once on disk, and once on the answer.
///
/// **On disk:** `grep -c 'fn validate' crates/rdb-sim/src/harness` is 1. `sim::cluster` has a
/// `validate` of its own and is deliberately out of scope — the claim is about the trace
/// validator, not about the word.
///
/// **On the answer:** three values that must be one trace, and one verdict between them.
///
/// * `hand_built` — the M7F-30 fixture, written in this file.
/// * `recorded` — every event of it pushed through the harness's own [`Recorder`], which
///   assigns the `event_id`s itself. If the recorder numbered differently, or dropped a field
///   off the [`Site`], the values stop comparing equal here.
/// * `round_tripped` — `recorded` through [`write_jsonl`] and back through [`read_jsonl`].
///
/// Then the same three with **one defect injected**: the acknowledgement raised above its own
/// node's last apply. Both directions matter. Equal-and-`Ok` alone would pass on a `validate`
/// that answers `Ok(())` to everything, and equal-and-rejected alone would pass on one that
/// answers the same defect to everything; the two halves together need a validator that reads
/// the trace.
///
/// **What this row does not claim.** Nothing about `replay`, and nothing about determinism. The
/// recorded half here is a re-recording of a fixture, not a second run of a plan — M7F-05 owns
/// the byte-identical-twice claim and this row deliberately does not restate it.
#[retcd_test]
fn m7f_35_a_recorded_trace_and_a_hand_built_one_go_through_one_validator() {
    support::preamble();

    let definitions = validate_definitions();
    assert_eq!(
        definitions.len(),
        1,
        "one validator under src/harness, found {definitions:?}"
    );

    let hand_built = well_formed();
    let recorded = re_record(&hand_built);
    let round_tripped = through_a_file(&recorded, "clean");
    assert_eq!(
        recorded, hand_built,
        "the recorder assigns the same order the fixture wrote by hand"
    );
    assert_eq!(
        round_tripped, hand_built,
        "and the file gives it back unchanged"
    );
    assert_eq!(validate(&hand_built), Ok(()));
    assert_eq!(validate(&recorded), Ok(()));
    assert_eq!(
        validate(&round_tripped),
        Ok(()),
        "one verdict for one trace, however it was built"
    );

    // The same three with one defect: B acknowledges 20 having applied 10.
    let mut hand_built = well_formed();
    let ack_at = hand_built
        .events
        .iter()
        .position(|event| matches!(event.kind, TraceKind::ReplicationAck { .. }))
        .expect("the fixture carries one acknowledgement");
    hand_built.events[ack_at].kind = ack(SECONDARY_B, 20);
    let recorded = re_record(&hand_built);
    let round_tripped = through_a_file(&recorded, "defective");
    assert_eq!(recorded, hand_built, "the defect survives the recorder");
    assert_eq!(round_tripped, hand_built, "and the file");

    let defect = Err(TraceDefect::AckAboveLastApply {
        node: SECONDARY_B,
        contiguous_seq: Seq(20),
        last_apply: Seq(10),
    });
    assert_eq!(validate(&hand_built), defect);
    assert_eq!(validate(&recorded), defect);
    assert_eq!(
        validate(&round_tripped),
        defect,
        "one defect for one trace, however it was built"
    );

    tracing::info!(
        validators = definitions.len(),
        events = hand_built.events.len(),
        "m7f_35 one validator"
    );
}

/// Every `fn validate` under `crates/rdb-sim/src/harness`, as `file:line`.
///
/// The plan's clause is a `grep -c`, so this is a grep: read the sources, count the lines that
/// carry the token. It reports *where* rather than just how many, because a failure here means
/// somebody added a second validator and the useful thing to print is which file it is in.
///
/// `harness.rs` itself is included alongside the directory. The plan names the directory; a
/// second validator in the parent module would be the same defect with a different path, and
/// there is none there today, so including it costs nothing and closes the gap.
fn validate_definitions() -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sources: Vec<std::path::PathBuf> = vec![root.join("harness.rs")];
    let mut stack = vec![root.join("harness")];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).expect("the harness source directory is readable");
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                sources.push(path);
            }
        }
    }
    assert!(
        sources.len() > 1,
        "an empty source list would make this row pass by finding nothing"
    );

    let mut found = Vec::new();
    for path in sources {
        let text = std::fs::read_to_string(&path).expect("a harness source is readable");
        for (index, line) in text.lines().enumerate() {
            if line.contains("fn validate") {
                found.push(format!("{}:{}", path.display(), index + 1));
            }
        }
    }
    found
}

/// Push every event of `trace` through the harness's own [`Recorder`].
///
/// The recorder assigns the `event_id` and nothing else does, so this is the recorded half of
/// M7F-35: if it numbered differently from the fixture, or the [`Site`] lost a field on the way
/// through, the two values stop comparing equal.
fn re_record(trace: &Trace) -> Trace {
    let mut recorder = Recorder::new();
    recorder.begin(trace.header.clone()).expect("begin");
    for event in &trace.events {
        recorder
            .record(
                Site {
                    at: Tick(event.logical_tick),
                    node: event.node,
                    boot: event.boot,
                    partition: event.partition,
                    correlation: event.correlation,
                },
                event.kind.clone(),
            )
            .expect("record");
    }
    recorder.finish().expect("finish")
}

/// Write `trace` as JSONL under this row's log directory and read it back.
fn through_a_file(trace: &Trace, name: &str) -> Trace {
    let dir = test_log_dir().join("m7f_35");
    std::fs::create_dir_all(&dir).expect("log dir");
    let path = dir.join(format!("{name}.jsonl"));
    write_jsonl(trace, &path).expect("write");
    read_jsonl(&path).expect("read")
}
