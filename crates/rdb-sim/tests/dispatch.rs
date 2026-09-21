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
use rdb_core::contracts::event::{Budgets, Effect, EffectKind, EventKind, ModuleName};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, Generation, NodeId, OwnerEpoch, PartitionId, ReplicaRole,
    ScenarioId,
};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    BudgetName, CapabilityState, PackageId, Provenance, TopologyEntry, TraceHeader, TraceKind,
};
use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;
use rdb_sim::harness::dispatch::{Adopted, Dispatcher, HOP_BUDGET_MILLIS};
use rdb_sim::harness::manifest::{resolve, BudgetOverride};
use rdb_sim::harness::trace::{read_jsonl, write_jsonl, Recorder, Site};
use rdb_sim::sim::control::{ControlOp, ControlStore};
use rdb_sim::sim::scheduler::Scheduler;
use rdb_sim::SimError;

const NODE: NodeId = NodeId(1);
const BOOT: BootId = BootId(1);

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
    tracing::info!(overridden = one.overridden.len(), "m7f_19");
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
    tracing::info!(
        t = t.0,
        delayed_at = delayed.at.0,
        hop_budget_millis = HOP_BUDGET_MILLIS,
        "m7f_21"
    );
    assert_eq!(delayed.at, t.plus_millis(HOP_BUDGET_MILLIS));
    assert_eq!(delayed.correlation, CorrelationId(2));
    assert_eq!(scheduler.now(), delayed.at, "time jumped to the deadline");
    assert_eq!(scheduler.queued(), 0);
}

/// An effect whose provider is not wired is refused by name, after everything before it was
/// carried out; nothing is dropped silently.
#[retcd_test]
fn m7f_21_an_unwired_provider_is_refused_by_name_after_earlier_effects_land() {
    support::preamble();
    let mut dispatcher = Dispatcher::new();
    let mut control = ControlStore::new();
    let mut scheduler = Scheduler::new();
    let timer = Effect {
        correlation: CorrelationId(1),
        from: ModuleName::Authority,
        partition: PartitionId(1),
        kind: EffectKind::Timer(rdb_core::contracts::time::TimerEffect::Cancel {
            id: rdb_core::contracts::ids::TimerId(1),
            version: rdb_core::contracts::ids::TimerVersion(1),
        }),
    };

    let error = dispatcher
        .deliver(
            NODE,
            BOOT,
            vec![adopt(1, 1, 1, 1), timer],
            &mut control,
            &mut scheduler,
        )
        .expect_err("timers are not wired");

    assert_eq!(
        error,
        SimError::Unavailable {
            seam: "harness::dispatch::deliver::timer"
        }
    );
    assert_eq!(
        dispatcher.adopted(NODE, PartitionId(1)).generation,
        Generation(1),
        "the adoption before the refused effect was carried out"
    );
}
