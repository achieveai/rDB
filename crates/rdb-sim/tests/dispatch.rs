//! Rows M7F-09, M7F-11, M7F-19 and M7F-21: package I1, the dispatcher, the trace file and the
//! manifest.
//!
//! | Row | Claim |
//! |---|---|
//! | M7F-09 | before any `AdoptAuthority` the dispatcher fills `StepCtx` with the zero triple; after `AdoptAuthority { g, e, c }` the next `StepCtx` for that partition carries exactly `(g, e, c)`; another partition's is unchanged (K-F-05, F-R10) |
//! | M7F-11 | a `TraceHeader` with each `Provenance`, a `RunManifest`, `partitions` and `topology` round-trips through `write_jsonl`/`read_jsonl` byte-identically; an unknown header field is refused (K-F-09) |
//! | M7F-19 | `manifest::resolve` with one override lists exactly that `BudgetName`; with none, `overridden` is empty and `budgets == SPEC_DEFAULTS` (K-F-27) |
//! | M7F-21 | an effect emitted at tick *t* completes at tick *t*; with `DelayCompletion { by_millis: 50 }` at exactly *t + 50*; the harness spends none of `HOP_BUDGET_MILLIS` (B-R23) |
//!
//! Log fields are ticks, revisions and counts; never a key or value byte.

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use config_log::testing::test_log_dir;
use rdb_core::contracts::control::{ControlEffect, ControlEvent, ControlKey};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, EventKind, KernelEffect, ModuleName,
};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, EventId, Generation, MessageId, NodeId, OwnerEpoch,
    PartitionId, ReplicaRole, ScenarioId, Seq, SnapshotHandle, TimerId, TimerVersion,
};
use rdb_core::contracts::qualification::{
    QualificationCause, QualificationChanged, QualificationDirection,
};
use rdb_core::contracts::storage::StoreEffect;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    BudgetName, CapabilityState, DispatchOutcome, KernelNote, PackageId, Provenance, RunManifest,
    TopologyEntry, Trace, TraceHeader, TraceKind,
};
use rdb_core::contracts::transport::{Frame, SendEffect};
use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;
use rdb_sim::harness::dispatch::{Adopted, Dispatcher, HOP_BUDGET_MILLIS};
use rdb_sim::harness::manifest::{resolve, BudgetOverride};
use rdb_sim::harness::replay::replay;
use rdb_sim::harness::run::{execute, RunPlan, SeedEvent};
use rdb_sim::harness::trace::{read_jsonl, write_jsonl, Recorder, Site};
use rdb_sim::sim::clock::Clock;
use rdb_sim::sim::cluster::{Cluster, ClusterConfig};
use rdb_sim::sim::control::{ControlOp, ControlStore};
use rdb_sim::sim::network::Network;
use rdb_sim::sim::scheduler::Scheduler;
use rdb_sim::SimError;

const NODE: NodeId = NodeId(1);
const BOOT: BootId = BootId(1);

/// Q-62's line: one per manifest resolved (`docs/testing/m7-log-fields.md`, tier 3).
///
/// `overridden` is logged as JSON rather than as a count, because Q-62 asserts it is `[]` for
/// the defaults case and exactly `["PauseAge"]` for the one-override case — a count of 1 is true
/// of every single override and would pass on the wrong budget. `tracing` renders anything
/// composite through `Debug`, which would give `[PauseAge]`, so the JSON is built here.
fn log_manifest(manifest: &RunManifest) {
    let overridden = serde_json::to_string(&manifest.overridden).expect("budget names serialise");
    tracing::info!(
        overridden = %overridden,
        nodes = manifest.nodes,
        event_cap = manifest.event_cap,
        "m7f_19 manifest"
    );
}

/// Q-64's line: the tick an effect was emitted at and the tick its completion landed on.
///
/// Both ticks, not the difference: Q-64 computes `completion_tick - emitted_tick` itself and
/// asserts it is exactly 0 or exactly [`HOP_BUDGET_MILLIS`], never 49 or 51. Logging a
/// pre-computed hop would let a wrong pair of ticks produce a right difference.
fn log_hop(emitted: Tick, completion: Tick) {
    tracing::info!(
        emitted_tick = emitted.0,
        completion_tick = completion.0,
        hop_budget_millis = HOP_BUDGET_MILLIS,
        "m7f_21 hop"
    );
}

fn adopt(partition: u32, generation: u64, owner_epoch: u64, config_version: u64) -> Effect {
    Effect {
        correlation: CorrelationId(1),
        from: ModuleName::Authority,
        partition: PartitionId(partition),
        kind: EffectKind::AdoptAuthority {
            partition: PartitionId(partition),
            generation: Generation(generation),
            owner_epoch: OwnerEpoch(owner_epoch),
            config_version: ConfigVersion(config_version),
        },
    }
}

#[retcd_test]
fn m7f_09_the_dispatcher_fills_the_authority_triple_from_the_last_adoption() {
    support::preamble();
    let mut dispatcher = Dispatcher::new();
    let mut control = ControlStore::new();
    let mut scheduler = Scheduler::new();
    let base = support::ctx();

    // Before any adoption: the zero triple, whatever the base context claimed.
    let before = dispatcher.ctx_for(&base);
    assert_eq!(before.generation, Generation(0));
    assert_eq!(before.owner_epoch, OwnerEpoch(0));
    assert_eq!(before.config_version, ConfigVersion(0));
    assert_eq!(dispatcher.adopted(NODE, PartitionId(1)), Adopted::default());

    dispatcher
        .deliver(
            NODE,
            BOOT,
            vec![adopt(1, 7, 3, 11)],
            &mut control,
            &mut scheduler,
        )
        .expect("adopt is absorbed");

    let after = dispatcher.ctx_for(&base);
    tracing::info!(
        generation = after.generation.0,
        owner_epoch = after.owner_epoch.0,
        config_version = after.config_version.0,
        "m7f_09 adopted"
    );
    assert_eq!(after.generation, Generation(7));
    assert_eq!(after.owner_epoch, OwnerEpoch(3));
    assert_eq!(after.config_version, ConfigVersion(11));
    assert_eq!(after.node, base.node);
    assert_eq!(after.now, base.now);

    // Another partition on the same node is untouched, and so is the same partition on another
    // node.
    let mut other_partition = support::ctx();
    other_partition.partition = PartitionId(2);
    assert_eq!(
        dispatcher.ctx_for(&other_partition).generation,
        Generation(0)
    );
    assert_eq!(
        dispatcher.adopted(NodeId(2), PartitionId(1)),
        Adopted::default()
    );
    assert_eq!(scheduler.queued(), 0, "adopting schedules nothing");

    // The latest adoption wins.
    dispatcher
        .deliver(
            NODE,
            BOOT,
            vec![adopt(1, 8, 4, 11)],
            &mut control,
            &mut scheduler,
        )
        .expect("adopt again");
    assert_eq!(dispatcher.ctx_for(&base).generation, Generation(8));
    assert_eq!(dispatcher.ctx_for(&base).owner_epoch, OwnerEpoch(4));
}

fn header(provenance: Provenance) -> TraceHeader {
    TraceHeader {
        schema_version: TRACE_SCHEMA_VERSION,
        generator_version: 1,
        provenance,
        config: resolve(
            &support::cluster(),
            1_000,
            &[BudgetOverride {
                name: BudgetName::Grant,
                millis: 4_000,
            }],
        )
        .expect("manifest"),
        partitions: 1,
        topology: vec![
            TopologyEntry {
                partition: PartitionId(1),
                node: NodeId(1),
                role: ReplicaRole::Primary,
                config_version: ConfigVersion(1),
            },
            TopologyEntry {
                partition: PartitionId(1),
                node: NodeId(2),
                role: ReplicaRole::RegularSecondary,
                config_version: ConfigVersion(1),
            },
        ],
        oracle_checkpoint_digest: support::ROOT_DIGEST,
    }
}

#[retcd_test]
fn m7f_11_trace_header_round_trips_through_jsonl_for_every_provenance() {
    support::preamble();
    let dir = test_log_dir().join("m7f_11");
    std::fs::create_dir_all(&dir).expect("log dir");

    for (index, provenance) in [
        Provenance::Generated { seed: 42 },
        Provenance::Reduced {
            parent: ScenarioId(7),
        },
        Provenance::Authored {
            case: "m7f_11".to_string(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let mut recorder = Recorder::new();
        recorder.begin(header(provenance)).expect("begin");
        for package in [PackageId::H1, PackageId::M1, PackageId::I1] {
            recorder
                .record(
                    Site {
                        at: Tick::ZERO,
                        node: NODE,
                        boot: BOOT,
                        partition: PartitionId(1),
                        correlation: CorrelationId(0),
                    },
                    TraceKind::Capability {
                        package,
                        state: CapabilityState::Unavailable,
                    },
                )
                .expect("record");
        }
        let trace = recorder.finish().expect("finish");

        let path = dir.join(format!("{index}.jsonl"));
        write_jsonl(&trace, &path).expect("write");
        let first_bytes = std::fs::read(&path).expect("read bytes");
        let back = read_jsonl(&path).expect("read");
        assert_eq!(back, trace, "the trace survives the file");

        write_jsonl(&back, &path).expect("write again");
        let second_bytes = std::fs::read(&path).expect("read bytes again");
        tracing::info!(index, bytes = first_bytes.len(), "m7f_11 round trip");
        assert_eq!(
            first_bytes, second_bytes,
            "byte-identical on the second write"
        );
    }
}

#[retcd_test]
fn m7f_11_an_unknown_header_field_and_a_foreign_schema_are_refused() {
    support::preamble();
    let dir = test_log_dir().join("m7f_11_refusals");
    std::fs::create_dir_all(&dir).expect("log dir");

    let mut value =
        serde_json::to_value(header(Provenance::Generated { seed: 1 })).expect("to value");
    value["seed"] = serde_json::Value::from(1);
    let unknown = dir.join("unknown.jsonl");
    std::fs::write(&unknown, format!("{value}\n")).expect("write");
    assert_eq!(
        read_jsonl(&unknown).expect_err("unknown field"),
        SimError::Malformed { line: 1 }
    );

    let mut value =
        serde_json::to_value(header(Provenance::Generated { seed: 1 })).expect("to value");
    value["schema_version"] = serde_json::Value::from(TRACE_SCHEMA_VERSION + 1);
    let foreign = dir.join("foreign.jsonl");
    std::fs::write(&foreign, format!("{value}\n")).expect("write");
    assert_eq!(
        read_jsonl(&foreign).expect_err("foreign schema"),
        SimError::Config {
            field: "schema_version"
        }
    );

    let empty = dir.join("empty.jsonl");
    std::fs::write(&empty, "").expect("write");
    assert_eq!(
        read_jsonl(&empty).expect_err("empty"),
        SimError::Malformed { line: 1 }
    );
}

#[retcd_test]
fn m7f_19_the_manifest_lists_exactly_the_overridden_budgets() {
    support::preamble();
    let cluster = support::cluster();

    let defaults = resolve(&cluster, 500, &[]).expect("defaults");
    log_manifest(&defaults);
    assert_eq!(defaults.budgets, Budgets::SPEC_DEFAULTS);
    assert!(defaults.overridden.is_empty());
    assert_eq!(defaults.nodes, 4);
    assert_eq!(defaults.event_cap, 500);

    let one = resolve(
        &cluster,
        500,
        &[BudgetOverride {
            name: BudgetName::PauseAge,
            millis: 9_999,
        }],
    )
    .expect("one override");
    log_manifest(&one);
    assert_eq!(one.overridden, vec![BudgetName::PauseAge]);
    assert_eq!(one.budgets.pause_age_millis, 9_999);
    let mut expected = Budgets::SPEC_DEFAULTS;
    expected.pause_age_millis = 9_999;
    assert_eq!(one.budgets, expected, "only that budget moved");

    // Setting a budget to its default is not an override.
    let noop = resolve(
        &cluster,
        500,
        &[BudgetOverride {
            name: BudgetName::Grant,
            millis: Budgets::SPEC_DEFAULTS.grant_millis,
        }],
    )
    .expect("noop override");
    assert!(noop.overridden.is_empty());

    // Two values for one budget is two different runs.
    let twice = resolve(
        &cluster,
        500,
        &[
            BudgetOverride {
                name: BudgetName::Grant,
                millis: 1,
            },
            BudgetOverride {
                name: BudgetName::Grant,
                millis: 2,
            },
        ],
    );
    assert_eq!(twice, Err(SimError::Config { field: "overrides" }));
}

#[retcd_test]
fn m7f_21_the_effect_to_event_hop_costs_zero_ticks_and_a_delay_costs_exactly_the_delay() {
    support::preamble();
    let mut dispatcher = Dispatcher::new();
    let mut control = ControlStore::new();
    let mut scheduler = Scheduler::new();
    let key = ControlKey::Grant(NODE);
    let create = |correlation: u64| {
        support::control_effect(
            correlation,
            ControlEffect::Cas {
                key,
                expected: None,
                value: Some(Bytes::from_static(b"g")),
            },
        )
    };

    // Move logical time to t by draining a placeholder event scheduled there.
    let t = Tick(1_000);
    scheduler
        .schedule(rdb_core::contracts::event::Event {
            at: t,
            ..support::probe_event()
        })
        .expect("placeholder");
    let placeholder = scheduler.pop().expect("placeholder pops");
    assert_eq!(placeholder.at, t);
    assert_eq!(scheduler.now(), t);

    // An effect at t completes at t.
    dispatcher
        .deliver(NODE, BOOT, vec![create(1)], &mut control, &mut scheduler)
        .expect("deliver");
    let completion = scheduler.pop().expect("the completion is queued");
    log_hop(t, completion.at);
    assert_eq!(completion.at, t, "zero-tick hop");
    assert_eq!(completion.node, NODE);
    assert_eq!(completion.boot, BOOT);
    assert_eq!(completion.correlation, CorrelationId(1));
    assert!(matches!(
        completion.kind,
        EventKind::Control(ControlEvent::CasResult { .. })
    ));
    assert!(HOP_BUDGET_MILLIS >= completion.at.millis_until(scheduler.now()));

    // With a planned delay of HOP_BUDGET_MILLIS, exactly t + HOP_BUDGET_MILLIS: the harness
    // added nothing of its own.
    control
        .inject(ControlOp::DelayCompletion {
            node: NODE,
            by_millis: HOP_BUDGET_MILLIS,
        })
        .expect("delay");
    dispatcher
        .deliver(NODE, BOOT, vec![create(2)], &mut control, &mut scheduler)
        .expect("deliver again");
    let delayed = scheduler.pop().expect("the delayed completion is queued");
    log_hop(t, delayed.at);
    assert_eq!(delayed.at, t.plus_millis(HOP_BUDGET_MILLIS));
    assert_eq!(delayed.correlation, CorrelationId(2));
    assert_eq!(scheduler.now(), delayed.at, "time jumped to the deadline");
    assert_eq!(scheduler.queued(), 0);
}

/// An effect whose provider is not wired is refused by name, after everything before it was
/// carried out; nothing is dropped silently.
///
/// Carried by a `Store` effect since 2026-09-22 (lead ruling A-R40 / L-R142). It was a
/// `TimerEffect::Cancel` until the timer wheel was wired, and the row is re-pointed rather than
/// deleted: the claim is that an **unwired** provider refuses *by its own name* after the
/// effects before it land, and `Timer` was only the example that carried it. `Store` and `Send`
/// are still owed, so the claim is still owed a subject. Deleting the row because its example
/// got built is how the guarantee for the remaining seams quietly stops being tested.
#[retcd_test]
fn m7f_21_an_unwired_provider_is_refused_by_name_after_earlier_effects_land() {
    support::preamble();
    let mut dispatcher = Dispatcher::new();
    let mut control = ControlStore::new();
    let mut scheduler = Scheduler::new();
    let store = Effect {
        correlation: CorrelationId(1),
        from: ModuleName::Authority,
        partition: PartitionId(1),
        kind: EffectKind::Store(store_effect()),
    };

    let error = dispatcher
        .deliver(
            NODE,
            BOOT,
            vec![adopt(1, 1, 1, 1), store],
            &mut control,
            &mut scheduler,
        )
        .expect_err("the store is not wired");

    assert_eq!(
        error,
        SimError::Unavailable {
            seam: "harness::dispatch::deliver::store"
        }
    );
    assert_eq!(
        dispatcher.adopted(NODE, PartitionId(1)).generation,
        Generation(1),
        "the adoption before the refused effect was carried out"
    );
    tracing::info!(seam = "harness::dispatch::deliver::store", "m7f_21 seam");
}

/// Every distinct seam string literal this crate's `src` contains, in sorted order.
///
/// Clause 3 of `M7F-26`, landed 2026-09-22. The plan wrote that clause as
/// `grep -c 'SimError::unavailable(' crates/rdb-sim/src` equalling the size of the row's list,
/// and **that comparison could never have passed**: `grep -c` counts *call sites*, not distinct
/// seams. It is 10 today against a list of 6, and it was 11 against 7 before the timer wheel was
/// wired. One site passes a variable rather than a literal (`RunReport::into_result` re-raises
/// the seam it was handed), three sit in `#[cfg(test)]` scaffolding inside `src`, and
/// `harness::dispatch::deliver::send` alone is spelled at four of them.
///
/// So the clause is landed as the relation that is both true and anti-drift: the **set** of
/// distinct seam literals in `crates/rdb-sim/src` equals the set this row declares. A new seam
/// added anywhere in the crate without a row fails here; a seam that is wired and whose literal
/// disappears fails here too. That is the drift the clause exists to catch, and a count of call
/// sites would not have caught either — adding a fourth `send` call site moves a count and
/// changes no set.
///
/// Extraction, rather than a regex crate: for each marker (`SimError::unavailable(` or `seam:`)
/// take the next non-whitespace character; a `"` starts a literal and anything else is a
/// variable and is skipped.
fn seam_literals_in_src() -> Vec<String> {
    fn visit(dir: &std::path::Path, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).expect("the crate's own src is readable") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                visit(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let text = std::fs::read_to_string(&path).expect("a source file");
                for marker in ["SimError::unavailable(", "seam:"] {
                    for (at, _) in text.match_indices(marker) {
                        let rest = text[at + marker.len()..].trim_start();
                        if let Some(literal) = rest.strip_prefix('"') {
                            let end = literal.find('"').expect("a closing quote");
                            out.push(literal[..end].to_owned());
                        }
                    }
                }
            }
        }
    }

    let mut found = Vec::new();
    visit(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut found,
    );
    found.sort_unstable();
    found.dedup();
    found
}

/// M7F-26: every unbuilt seam refuses by its own name, logs that name, and the set does not
/// drift from the code.
///
/// Three claims in one row. The assertion half is that nothing is owed silently: each seam
/// returns [`SimError::Unavailable`] carrying the string a reader can grep for, rather than a
/// bare error or a fake success. The log half is Q-61's only source — it asserts the **set** of
/// distinct `seam` values, so a seam with no row fails it and a row that stopped asserting its
/// seam fails it too. The third is [`seam_literals_in_src`], which pins that set against the
/// crate's sources.
///
/// Before this row existed the `seam` field appeared on no line anywhere, so Q-61 was not
/// returning zero rows — it was a binder error on a column that had no source.
///
/// **The set is six, not seven, since 2026-09-22** (lead ruling A-R40 / L-R142): the `timer`
/// seam under `harness::dispatch::deliver` is gone, because [`Dispatcher::deliver`] routes an
/// `EffectKind::Timer` to the clock's wheel rather than refusing it. Named the long way round
/// on purpose, so that a grep for the live seam vocabulary does not hit a sentence about a seam
/// that no longer exists. The row keeps
/// every other member and its shape — the claim is about seams that are still owed, and `Send`,
/// `Store` and `Kernel` still are.
///
/// **The `kernel` seam narrowed on 2026-09-22 and stayed in the set** (lead ruling A-R46). It
/// used to refuse every `EffectKind::Kernel`; it now refuses every arm whose consumer is not
/// routed: `Recovered`, `QualificationChanged`, R1's and F1's outputs, and every `Authority` arm
/// but `Fact`. Recorded instead: `Ignored` and `Alert` (A-R46), `Authority`'s `Fact` (A-R49), and
/// L1's `SetAdmission` and `ProtectionWarn` (B-R42). All refusals share the one literal, so the
/// count stays six.
///
/// The row's `Kernel` example was an `Ignored`, then a `SetAdmission`, and is now a
/// `QualificationChanged`: each time the arm it used started being recorded. Re-pointed, not
/// deleted: the claim is that the seam refuses by its own name, and the arm is only the example
/// that carries it. **The example must be an arm with no routed consumer**, so when L1 or P1
/// routes `QualificationChanged`, re-point it again rather than delete it. What a recorded arm
/// now does is asserted by the `i1_scaffolding_*` and `h1_scaffolding_*` rows in
/// `tests/harness.rs`, and by `harness::dispatch`'s own scaffolding. None carries this row's id,
/// because none asserts this row's claim.
#[retcd_test]
fn m7f_26_every_unbuilt_seam_refuses_by_its_own_name() {
    support::preamble();
    let mut seams: Vec<&'static str> = Vec::new();

    // I1: replay is owed.
    let trace = Trace {
        header: header(Provenance::Generated { seed: 1 }),
        events: Vec::new(),
    };
    seams.push(seam_of(replay(&trace).map(|_| ())));

    // H1: the network and the cluster lifecycle are owed.
    let mut network = Network::new();
    seams.push(seam_of(
        network.send(NodeId(1), NodeId(2), frame()).map(|_| ()),
    ));
    let mut cluster = Cluster::new(support::cluster()).expect("a four-node cluster");
    seams.push(seam_of(cluster.suspend(NodeId(1), 10)));

    // I1: the three effect kinds the dispatcher has no provider for. `EffectKind::Timer` was the
    // fourth until A-R40 wired it. What it does instead is asserted in `harness::dispatch`'s and
    // `harness::run`'s own scaffolding — a wired kind's claim is what it *does*, and this row is
    // about what is still owed. For `Kernel`, "owed" means an arm whose consumer is not routed
    // (A-R46, B-R42); the recorded arms are not owed, so the example is a `QualificationChanged`.
    for effect in [
        EffectKind::Send(send_effect()),
        EffectKind::Store(store_effect()),
        EffectKind::Kernel(KernelEffect::QualificationChanged(qualification_edge())),
    ] {
        let mut dispatcher = Dispatcher::new();
        let mut control = ControlStore::new();
        let mut scheduler = Scheduler::new();
        let refused = dispatcher.deliver(
            NODE,
            BOOT,
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Authority,
                partition: PartitionId(1),
                kind: effect,
            }],
            &mut control,
            &mut scheduler,
        );
        seams.push(seam_of(refused));
        assert_eq!(scheduler.queued(), 0, "a refused effect queues nothing");
    }

    for seam in &seams {
        tracing::info!(seam, "m7f_26 seam");
    }

    /// The seams still owed, sorted. Six since 2026-09-22; seven before, when `Timer` was one.
    const OWED_SEAMS: [&str; 6] = [
        "harness::dispatch::deliver::kernel",
        "harness::dispatch::deliver::send",
        "harness::dispatch::deliver::store",
        "harness::replay::replay",
        "sim::cluster::Cluster::suspend",
        "sim::network::Network::send",
    ];

    let mut distinct = seams.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct, OWED_SEAMS,
        "the seam vocabulary Q-61 pins, sorted"
    );

    // Clause 3: the list above is the crate's, not just this row's.
    assert_eq!(
        seam_literals_in_src(),
        OWED_SEAMS,
        "a seam string lives in crates/rdb-sim/src that this row does not exercise, or a seam \
         this row declares is no longer spelled anywhere in the crate"
    );
}

/// One frame to nobody in particular. The body is empty: a seam row proves the refusal, and a
/// payload would be a payload in a test that has no use for one.
fn frame() -> Frame {
    Frame {
        id: MessageId(1),
        protocol: 1,
        config: ConfigVersion(1),
        body: bytes::Bytes::new(),
    }
}

fn send_effect() -> SendEffect {
    SendEffect::Unicast {
        to: NodeId(2),
        frame: frame(),
    }
}

fn store_effect() -> StoreEffect {
    StoreEffect::Release {
        handle: SnapshotHandle(1),
    }
}

/// R1's qualification edge: the payload of the `QualificationChanged` that carries `M7F-26`'s
/// `kernel` example, an arm whose consumers (L1, P1) are not routed. It replaced a
/// `SetAdmission`, which lead ruling B-R42 records. The values are irrelevant to a seam row; the
/// arm is the point.
fn qualification_edge() -> QualificationChanged {
    QualificationChanged {
        lineage: rdb_core::contracts::authority::Lineage {
            partition: PartitionId(1),
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
        },
        config_version: ConfigVersion(1),
        at_seq: Seq(0),
        direction: QualificationDirection::Gained,
        qualified_copies: vec![rdb_core::contracts::membership::CopyId(2)],
        qualified_ack_count: 1,
        cause: QualificationCause::AckAdvanced,
        tick: Tick::ZERO,
    }
}

// `timer_effect()` lived here until 2026-09-22. It was a fixture for `M7F-26`'s fourth refusable
// kind and had no other caller, and `EffectKind::Timer` is no longer refusable (A-R40). Removed
// rather than left dead: it is a fixture, not an assertion. What a `TimerEffect` now *does* is
// asserted in `harness::dispatch` and `harness::run` scaffolding; `M7F-43`'s clock clauses are
// unaffected and still below.

/// The seam an owed call refused with. Fails the row on a call that unexpectedly succeeded,
/// because a seam that quietly started working is how a row stops testing anything.
fn seam_of(result: Result<(), SimError>) -> &'static str {
    match result.expect_err("an unbuilt seam must refuse") {
        SimError::Unavailable { seam } => seam,
        other => panic!("an unbuilt seam must refuse as Unavailable, got {other:?}"),
    }
}

/// M7F-47, second row — two events at one tick pop in ascending `EventId` order.
///
/// Manual-tester finding F2, 2026-09-21. `sim/scheduler.rs`'s own doc states the contract —
/// ordered by `(tick, event_id)`, and "the id is not decoration": spike §6 needs equal-time
/// events to have a stable order, because H1's acceptance claim is that one event log replays to
/// a byte-identical trace. Nothing scheduled two events at the same tick and checked the order.
/// Inverting the tie-break to `(at, u64::MAX - id)` left the whole workspace green, 147/147.
///
/// Scheduling the higher id first is the point: a queue that happens to preserve insertion order
/// would pass a test that inserted them in the order it expected back.
#[retcd_test]
fn m7f_47_two_events_at_one_tick_pop_in_ascending_event_id_order() {
    support::preamble();
    let mut scheduler = Scheduler::new();
    let at = Tick(500);

    for id in [EventId(9), EventId(2), EventId(5)] {
        scheduler
            .schedule(rdb_core::contracts::event::Event {
                id,
                at,
                ..support::probe_event()
            })
            .expect("three distinct ids at one tick are three events");
    }

    let popped: Vec<EventId> = std::iter::from_fn(|| scheduler.pop())
        .map(|e| e.id)
        .collect();
    assert_eq!(
        popped,
        vec![EventId(2), EventId(5), EventId(9)],
        "equal-tick events are ordered by id, ascending — they were scheduled 9, 2, 5, so an \
         insertion-ordered queue would answer 9, 2, 5 and a descending tie-break 9, 5, 2"
    );
}

/// M7F-43, second function — a timer re-armed at the same or a lower version is refused.
///
/// Renamed from `m7f_47_…` on 2026-09-22 by the lead. It landed under the M7F-47 prefix on
/// 2026-09-21, but the clause it asserts is written verbatim in **M7F-43**'s assertion column
/// ("`arm` at a version at or below the armed one is `SimError::Config { field: \"version\" }`"),
/// and M7F-47 is the scheduler-ordering row. The rename was checked to be free: no §12 query
/// names a test function — they all group by `testMethod` and filter on `@m` or `seam` — and a
/// repository-wide search found the old name only at this definition and in `sim.rs`'s prose.
///
/// Manual-tester finding F3, 2026-09-21, and the wider finding is the one worth keeping: **no
/// test file in this crate referenced `Clock`, `.arm(`, `.due(` or `.cancel(` at all**, so
/// H1's "stale timer version is ignored" claim had no subject. Deleting `arm`'s version guard
/// outright left the workspace green.
///
/// Half of H1's claim is the kernel's ("the kernel ignores a stale fire") and cannot be tested
/// while every kernel module is unwired — `m7f_01` is the row that says so. This row asserts the
/// half foundation owns: the environment must not let a stale re-arm overwrite a live one, or
/// the fire the kernel is supposed to reject never carries a stale version in the first place.
#[retcd_test]
fn m7f_43_a_timer_rearmed_at_the_same_or_lower_version_is_refused() {
    support::preamble();
    let mut clock = Clock::new(0);
    let id = TimerId(1);
    let armed = Tick(100);

    clock
        .arm(NODE, id, TimerVersion(2), armed)
        .expect("the first arm of a timer is always accepted");

    for stale in [TimerVersion(2), TimerVersion(1)] {
        assert_eq!(
            clock.arm(NODE, id, stale, Tick(900)),
            Err(rdb_sim::SimError::Config { field: "version" }),
            "a re-arm at version {stale:?} is not above the armed version 2, and accepting it \
             would let a fire carrying a stale version pass the kernel's own check"
        );
    }

    let fired = clock.due(Tick(1_000));
    assert_eq!(
        fired.len(),
        1,
        "one timer was armed, so exactly one fires — a refused re-arm must not queue a second"
    );
    assert_eq!(
        (fired[0].1.version, fired[0].1.scheduled_at),
        (TimerVersion(2), armed),
        "the surviving arm is the original: version 2 at tick 100, not either refused re-arm"
    );
}

/// Scaffolding, not an M7 row (lead ruling A-R49): A1's `AuthorityEffect::Fact` reaches the trace
/// **through the run loop** as a `KernelNoted`, attributed to the event whose offer produced it.
///
/// The drive is the one `an_injected_control_fault_and_a_clean_run_diverge` uses for its `Fact`
/// (lead ruling A-R47): the seed is A1's first `AcquireDue`, A1 issues the create-only grant CAS,
/// and `PlanCas { Conflict }` makes it lose, so A1 emits `Fact(AcquireLost)`. A **lost**
/// acquisition, because under A-R47 a clean one emits no `Fact` at all: it adopts. Since finding
/// F2 the adoption publishes no view; the first view is the partitions install's, and only over
/// a store seeded with a record naming the node owner (lead ruling B-R39) does the run then stop
/// `Refused` on it (A-R49). Until A-R47 this row hand-carried a committed CAS, which A1 now
/// treats as an unmatched completion.
///
/// What the run does *after* the `Fact` is not this row's claim, and it is deliberately not
/// asserted. The stop is logged, so a reader can see where it landed without the row pinning a
/// kernel that another team is still changing.
#[retcd_test]
fn i1_scaffolding_an_authority_fact_reaches_the_trace_through_the_loop() {
    use rdb_core::authority::AuthorityTimer;
    use rdb_core::contracts::control::CasOutcome;
    use rdb_core::contracts::ids::Revision;
    use rdb_core::contracts::time::TimerFired;

    support::preamble();
    let mut plan = RunPlan::new(ClusterConfig::default());
    plan.seed = vec![SeedEvent {
        at: Tick(10),
        node: NODE,
        boot: BOOT,
        partition: PartitionId(1),
        correlation: CorrelationId(1),
        kind: EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: TimerVersion(0),
            scheduled_at: Tick(10),
        }),
    }];
    plan.control_ops = vec![ControlOp::PlanCas {
        node: NODE,
        outcome: CasOutcome::Conflict {
            exists: true,
            current: Revision(1),
        },
    }];
    let (trace, report) = execute(&plan).expect("a run");

    let (at, record, event) = trace
        .events
        .iter()
        .enumerate()
        .find_map(|(at, record)| match &record.kind {
            TraceKind::KernelNoted {
                event,
                module: ModuleName::Authority,
                note: KernelNote::AuthorityFact { .. },
            } => Some((at, record, *event)),
            _ => None,
        })
        .expect("A1's Fact is recorded in the trace, not refused and not absorbed");
    tracing::info!(
        trace_index = at,
        event_id = event.0,
        recorded = trace.events.len(),
        stop = ?report.stop,
        "i1 authority fact noted"
    );

    let (dispatch_at, dispatch) = trace
        .events
        .iter()
        .enumerate()
        .find(|(_, candidate)| {
            matches!(
                candidate.kind,
                TraceKind::ModuleDispatch {
                    event: offered,
                    module: ModuleName::Authority,
                    ..
                } if offered == event
            )
        })
        .expect("the offer that produced the Fact is recorded");
    assert!(
        dispatch_at < at,
        "the note follows its offer's dispatch record ({dispatch_at} < {at})"
    );
    assert_eq!(
        dispatch.logical_tick, record.logical_tick,
        "one pop, one tick"
    );
    assert!(
        matches!(
            dispatch.kind,
            TraceKind::ModuleDispatch {
                outcome: DispatchOutcome::Answered { effects: 1.. },
                ..
            }
        ),
        "A1 answered that offer with at least the Fact: {:?}",
        dispatch.kind
    );
}
