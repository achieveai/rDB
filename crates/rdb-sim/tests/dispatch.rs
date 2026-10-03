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
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, EventKind, KernelEffect, ModuleName,
};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, ControlRequestId, CorrelationId, EventId, Generation, MessageId, NodeId,
    OwnerEpoch, PartitionId, ReplicaRole, ScenarioId, Seq, SnapshotHandle, TimerId, TimerVersion,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::recovery::{LineageAnchor, RecoveryEffect, SurvivorInventory};
use rdb_core::contracts::storage::StorageFault;
use rdb_core::contracts::storage::StoreEffect;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    BudgetName, CapabilityState, DispatchOutcome, KernelNote, PackageId, Provenance, RunManifest,
    TopologyEntry, Trace, TraceHeader, TraceKind,
};
use rdb_core::contracts::transport::{Frame, PeerLabel};
use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;
use rdb_sim::harness::dispatch::{Adopted, Dispatcher, DropReason, Dropped, HOP_BUDGET_MILLIS};
use rdb_sim::harness::manifest::{resolve, BudgetOverride};
use rdb_sim::harness::replay::replay;
use rdb_sim::harness::run::{execute, RunLimits, RunPlan, Runner, SeedEvent};
use rdb_sim::harness::trace::{read_jsonl, write_jsonl, Recorder, Site};
use rdb_sim::sim::clock::Clock;
use rdb_sim::sim::cluster::ClusterConfig;
use rdb_sim::sim::control::{ControlOp, ControlStore};
use rdb_sim::sim::network::{Network, NetworkOp};
use rdb_sim::sim::scheduler::Scheduler;
use rdb_sim::storage::StorageOp;
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
    // Registered under the boot it delivers under: only `restart` gives a node a boot (V-R36).
    dispatcher.register_node(NODE, BOOT);
    let mut control = ControlStore::new();
    let mut scheduler = Scheduler::new();
    let key = ControlKey::Grant(NODE);
    let create = |correlation: u64| {
        support::control_effect(
            correlation,
            ControlEffect::Cas {
                request: ControlRequestId(correlation),
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
/// **Re-pointed 2026-10-02 (team i1)**, the fourth time: carried by F1's `QuarantineSuffix`
/// under `harness::dispatch::deliver::recovery`, because `ProbeDigestAt` got its provider. From
/// 2026-09-26 (lead ruling A-R61) it was carried by `ProbeDigestAt`. It was a `Store` effect from
/// 2026-09-22 (A-R40 / L-R142) until the store was wired to the memory engine, and a
/// `TimerEffect::Cancel` before that. The claim is that an **unwired** provider refuses *by its
/// own name* after the effects before it land; the example is only what carries it. Keeping a
/// quarantined suffix is a request to the environment that no provider answers yet, which is
/// exactly that claim.
///
/// The `Store` that used to carry it is now carried out, and is the positive control here: it is
/// delivered between the adoption and the request, and its completion is queued.
#[retcd_test]
fn m7f_21_an_unwired_provider_is_refused_by_name_after_earlier_effects_land() {
    support::preamble();
    let mut dispatcher = Dispatcher::new();
    let mut control = ControlStore::new();
    let mut scheduler = Scheduler::new();
    let effect = |kind: EffectKind| Effect {
        correlation: CorrelationId(1),
        from: ModuleName::Recovery,
        partition: PartitionId(1),
        kind,
    };
    let snapshot = effect(EffectKind::Store(StoreEffect::Snapshot {
        handle: SnapshotHandle(1),
        partition: PartitionId(1),
    }));
    let probe = effect(EffectKind::Kernel(KernelEffect::Recovery(
        RecoveryEffect::QuarantineSuffix {
            copy: rdb_core::contracts::membership::CopyId(2),
            from: Seq(1),
            until: Tick(1),
        },
    )));

    let error = dispatcher
        .deliver(
            NODE,
            BOOT,
            vec![adopt(1, 1, 1, 1), snapshot, probe],
            &mut control,
            &mut scheduler,
        )
        .expect_err("no provider keeps a quarantined suffix");

    assert_eq!(
        error,
        SimError::Unavailable {
            seam: "harness::dispatch::deliver::recovery"
        }
    );
    assert_eq!(
        dispatcher.adopted(NODE, PartitionId(1)).generation,
        Generation(1),
        "the adoption before the refused effect was carried out"
    );
    assert_eq!(
        scheduler.queued(),
        1,
        "the store effect before it was carried out too, and completed"
    );
    tracing::info!(seam = "harness::dispatch::deliver::recovery", "m7f_21 seam");
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
/// **Re-pointed 2026-09-26 (lead ruling A-R61): the set is seven, and four of them are new.**
/// `send` and `store` under `harness::dispatch::deliver`, and `Network`'s `send`, are gone:
/// a `Send` goes through the controlled network and a `Store` through the memory engine. Named
/// the long way round on purpose, so a grep for the live seam vocabulary does not hit a sentence
/// about seams that no longer exist. What replaced them is what those providers still cannot do,
/// each refused under its own name:
///
/// * `deliver::crash` — a planned crash fired; restarting the node was owed. **Closed
///   2026-10-02 (team i1):** a crash is a fault the run goes on through. What met it is dropped
///   as the dead process's (`NodeDown`), and a `Restart` step brings the node back
///   (`crash_a_run_goes_on_through_a_crash_and_a_restart_step_brings_the_node_back`). The set
///   is four.
/// * `deliver::recovery` — one of F1's requests with no provider, `QuarantineSuffix` and
///   `RebuildFromAuthoritative` (lead ruling A-R64). `ProbeDigestAt` was the third until
///   2026-10-02, when team i1 gave it a provider
///   (`probe_a_sparse_ladder_is_answered_from_the_holders_engine_and_recovery_commits`).
/// * `run::route` — a routed kernel fact whose named, wired consumer declined it. **Closed
///   2026-10-02 (team i1):** the run still stops there, because continuing would drop the fact
///   (B-R28), but the stop is `StopReason::Declined`, carrying the consumer's own answer. A
///   decline is a module's reply, not a delivery the harness cannot make, so it is no seam. The
///   set is three.
/// * `Network::forge_ack` — the network does not rewrite reply bodies. **Narrowed 2026-09-26
///   (lead ruling L-R177do):** a forgery the frame can carry — a forged label, authenticated or
///   not — is delivered, and R1 refuses it itself. Only an authenticated forgery whose lie is a
///   role inside the body was still refused here. **Closed 2026-10-02 (team h1):** that one is
///   delivered too, with the claimed role written into the body, and R1's role rule refuses it.
///   The set is five: `Cluster::suspend` closed the same day.
///
/// The `kernel` seam stays, narrowed again: it now refuses only arms with no consumer at all —
/// R1's `SnapshotCatchupRequired` and `CopyAheadOnControl`. Its example was an `Ignored`, a
/// `SetAdmission`, a `QualificationChanged`, a `SendEnvelopes` (given its provider by lead ruling
/// B-R57), and is now a `SnapshotCatchupRequired`: each time, the arm it used started being
/// recorded, routed or provided. `CopyAheadOnControl` is the second example since 2026-10-02
/// (tester-m7c-i1 A1), so absorbing either arm silently turns this row red.
/// **The example must be an arm with no consumer.** When one is given a provider, re-point it
/// again rather than delete it. What a routed arm does is asserted by the `route_*` scaffolding
/// at the end of this file, which carries no row id because none asserts this row's claim.
#[retcd_test]
fn m7f_26_every_unbuilt_seam_refuses_by_its_own_name() {
    support::preamble();
    let mut seams: Vec<&'static str> = Vec::new();

    // I1: replay from a bare trace refuses, by design and permanently.
    let trace = Trace {
        header: header(Provenance::Generated { seed: 1 }),
        events: Vec::new(),
    };
    seams.push(seam_of(replay(&trace).map(|_| ())));

    // H1 has no example since 2026-10-02 (team h1). A forged acknowledgement whose lie is inside
    // its body is delivered (`forge_ack_writes_the_claimed_role_into_an_authenticated_acknowledgement`)
    // and the cluster's suspend queues the resume (`m7f_24`, re-pointed).

    // I1: an unprovided F1 request and a kernel arm with no consumer, each delivered alone to a
    // fresh dispatcher. The crash had an example here until 2026-10-02, when its seam closed.
    let one = |kind: EffectKind| Effect {
        correlation: CorrelationId(1),
        from: ModuleName::Replication,
        partition: PartitionId(1),
        kind,
    };
    for effect in [
        EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::QuarantineSuffix {
            copy: CopyId(2),
            from: Seq(1),
            until: Tick(1),
        })),
        EffectKind::Kernel(KernelEffect::SnapshotCatchupRequired {
            copy: CopyId(2),
            barrier: Seq(1),
        }),
        EffectKind::Kernel(KernelEffect::CopyAheadOnControl { copy: CopyId(2) }),
    ] {
        let mut dispatcher = Dispatcher::new();
        let mut control = ControlStore::new();
        let mut scheduler = Scheduler::new();
        let refused =
            dispatcher.deliver(NODE, BOOT, vec![one(effect)], &mut control, &mut scheduler);
        seams.push(seam_of(refused));
        assert_eq!(scheduler.queued(), 0, "a refused effect queues nothing");
    }

    // Not a seam since 2026-10-02 (team i1): a routed fact whose named consumer declines stops
    // the run as `Declined`, with the consumer's answer, and refuses under no seam. R1 is the only
    // consumer of a divergence and has no primary installed, so it declines.
    let mut runner = Runner::new(&RunPlan::new(support::cluster())).expect("a runner");
    runner
        .carry_out(
            NODE,
            BOOT,
            vec![one(EffectKind::Kernel(KernelEffect::DivergenceDetected {
                copy: CopyId(2),
            }))],
        )
        .expect("a divergence is routed, not refused, at delivery");
    let declined = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail")
        .into_result()
        .map(drop)
        .expect_err("a declined routed fact still stops the run");
    assert!(
        matches!(declined, SimError::Kernel(RdbError::Unavailable { .. })),
        "the consumer's own answer, not a seam: {declined:?}"
    );

    for seam in &seams {
        tracing::info!(seam, "m7f_26 seam");
    }

    /// The seams still refused by name, sorted. Three since 2026-10-02, when H1's two closed
    /// (`Network::forge_ack` delivers, `Cluster::suspend` resumes) and I1's crash and route
    /// closed (a crash is a fault the run goes on through; a decline is `StopReason::Declined`);
    /// seven from 2026-09-26; six before.
    const OWED_SEAMS: [&str; 3] = [
        "harness::dispatch::deliver::kernel",
        "harness::dispatch::deliver::recovery",
        "harness::replay::replay",
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
        sender: send_lineage(),
        body: bytes::Bytes::new(),
    }
}

/// [`frame`] carrying an acknowledgement: R1's reply, `Accepted`, from node 1 as `role`.
fn ack_frame(role: ReplicaRole) -> Frame {
    use rdb_core::contracts::envelope::{AppendAck, AppendOutcome, ReplicaProgress};
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, ReceivedSeq};
    let lineage = send_lineage();
    Frame {
        body: rdb_core::replication::wire::encode_reply(&AppendOutcome::Accepted(AppendAck {
            partition: lineage.partition,
            generation: lineage.generation,
            owner_epoch: lineage.owner_epoch,
            config_version: ConfigVersion(1),
            from: NodeId(1),
            boot: BOOT,
            role,
            progress: ReplicaProgress {
                received: ReceivedSeq(1),
                buffered_applied: AppliedSeq(1),
                durable: DurableSeq(0),
            },
            digest_at_buffered: rdb_core::contracts::digest::Digest::ROOT,
        })),
        ..frame()
    }
}

// `send_effect()`, `store_effect()` and `qualification_edge()` lived here until 2026-09-26.
// They were fixtures for `M7F-26`'s `send`, `store` and `kernel` examples; the first two seams
// are wired and the third example is now a `SendEnvelopes` (A-R61). Removed rather than left
// dead: they are fixtures, not assertions.

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

// ------------------------------------------------------------------------------------------
// Scaffolding for the providers and the kernel routing (dev-sim-route, 2026-09-26). No row
// ids: each shows one wiring path (effect, provider, completion, module) is reachable, and
// none asserts a plan row's claim. Rows that do come after a manual tester's thumbs-up.
// ------------------------------------------------------------------------------------------

/// A second node, under a boot distinct from `BOOT`, so a delivery shows whose boot it carries.
const PEER: NodeId = NodeId(2);
const PEER_BOOT: BootId = BootId(7);

fn effect_from(module: ModuleName, kind: EffectKind) -> Effect {
    Effect {
        correlation: CorrelationId(1),
        from: module,
        partition: PartitionId(1),
        kind,
    }
}

/// `NODE` and `PEER`, both registered.
fn two_nodes() -> Dispatcher {
    let mut dispatcher = Dispatcher::new();
    dispatcher.register_node(NODE, BOOT);
    dispatcher.register_node(PEER, PEER_BOOT);
    dispatcher
}

fn deliver_on(
    dispatcher: &mut Dispatcher,
    scheduler: &mut Scheduler,
    node: NodeId,
    effect: Effect,
) -> Result<(), SimError> {
    let boot = if node == PEER { PEER_BOOT } else { BOOT };
    dispatcher.deliver(
        node,
        boot,
        vec![effect],
        &mut ControlStore::new(),
        scheduler,
    )
}

fn send_to(to: NodeId) -> Effect {
    use rdb_core::contracts::transport::SendEffect;
    effect_from(
        ModuleName::Replication,
        EffectKind::Send(SendEffect::Unicast { to, frame: frame() }),
    )
}

fn store(store: StoreEffect) -> Effect {
    effect_from(ModuleName::Transaction, EffectKind::Store(store))
}

#[retcd_test]
fn send_a_planned_delay_delivers_on_the_recipient_under_its_boot() {
    use rdb_core::contracts::transport::TransportEvent;
    use rdb_sim::sim::network::Delivery;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    dispatcher
        .inject_network(NetworkOp::PlanNext {
            from: NODE,
            to: PEER,
            delivery: Delivery::Deliver { delay_millis: 50 },
        })
        .expect("a plan between two nodes");

    deliver_on(&mut dispatcher, &mut scheduler, NODE, send_to(PEER)).expect("a send");

    assert_eq!(
        scheduler.queued(),
        1,
        "a delivered frame is scheduled, never dropped silently"
    );
    let arrival = scheduler.pop().expect("the frame arrives");
    assert_eq!(arrival.at, Tick(50), "now plus the planned delay");
    assert_eq!((arrival.node, arrival.boot), (PEER, PEER_BOOT));
    assert_eq!(
        arrival.kind,
        EventKind::Transport(TransportEvent::Delivered {
            from: PeerLabel {
                node: NODE,
                boot: BOOT,
                authenticated: true,
            },
            frame: frame(),
        })
    );
    assert_eq!(scheduler.queued(), 0, "one frame, one arrival");
}

#[retcd_test]
fn send_a_duplicate_arrives_twice_and_a_drop_schedules_nothing_but_is_recorded() {
    use rdb_sim::sim::network::Delivery;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    for delivery in [
        Delivery::Duplicate {
            delay_millis: 0,
            second_delay_millis: 20,
        },
        Delivery::Drop,
    ] {
        dispatcher
            .inject_network(NetworkOp::PlanNext {
                from: NODE,
                to: PEER,
                delivery,
            })
            .expect("a plan between two nodes");
    }

    deliver_on(&mut dispatcher, &mut scheduler, NODE, send_to(PEER)).expect("the duplicate");
    let ticks: Vec<Tick> = std::iter::from_fn(|| scheduler.pop())
        .map(|event| event.at)
        .collect();
    assert_eq!(
        ticks,
        vec![Tick(0), Tick(20)],
        "two arrivals, at their planned delays"
    );

    deliver_on(&mut dispatcher, &mut scheduler, NODE, send_to(PEER)).expect("the drop");
    assert_eq!(scheduler.queued(), 0, "a dropped frame schedules nothing");
    let last = dispatcher
        .network()
        .transmissions()
        .last()
        .copied()
        .expect("the drop is a transmission");
    assert_eq!((last.copies, last.partitioned), (0, false), "{last:?}");
    assert_eq!(dispatcher.network().transmissions().len(), 2);
}

#[retcd_test]
fn send_a_partition_or_an_unknown_node_fails_back_to_the_sender_now() {
    use rdb_core::contracts::transport::{LinkFault, TransportEvent};
    use rdb_sim::sim::network::LinkState;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    dispatcher
        .inject_network(NetworkOp::SetLink {
            a: NODE,
            b: PEER,
            state: LinkState::Partitioned,
        })
        .expect("a link between two nodes");

    for (to, fault) in [
        (PEER, LinkFault::Partitioned),
        (NodeId(9), LinkFault::Unreachable),
    ] {
        deliver_on(&mut dispatcher, &mut scheduler, NODE, send_to(to)).expect("a failed send");
        let failed = scheduler.pop().expect("the sender learns");
        assert_eq!((failed.at, failed.node, failed.boot), (Tick(0), NODE, BOOT));
        assert_eq!(
            failed.kind,
            EventKind::Transport(TransportEvent::SendFailed {
                id: MessageId(1),
                fault,
            }),
            "to {to:?}"
        );
    }
}

#[retcd_test]
fn store_a_commit_completes_and_a_planned_failure_says_so() {
    use rdb_core::contracts::ids::{AppliedSeq, BatchId};
    use rdb_core::contracts::storage::StorageEvent;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();

    let commit = store(StoreEffect::Commit(support::batch(1, 1, b"k", b"v")));
    deliver_on(&mut dispatcher, &mut scheduler, NODE, commit).expect("a commit");
    assert_eq!(
        scheduler.pop().map(|event| event.kind),
        Some(EventKind::Storage(StorageEvent::Committed {
            batch: BatchId(1),
            applied: AppliedSeq(1),
        }))
    );

    dispatcher
        .inject_storage(StorageOp::Fail {
            node: NODE,
            fault: StorageFault::WriteFailed,
        })
        .expect("a planned fault");
    let commit = store(StoreEffect::Commit(support::batch(1, 2, b"k", b"w")));
    deliver_on(&mut dispatcher, &mut scheduler, NODE, commit).expect("a failure completes");
    assert_eq!(
        scheduler.pop().map(|event| event.kind),
        Some(EventKind::Storage(StorageEvent::CommitFailed {
            batch: BatchId(2),
            fault: StorageFault::WriteFailed,
        }))
    );
}

/// The `Store` provider never marks unflushed data durable: every prefix a `Flushed` carries is
/// the engine's answer, and a short, false or failed flush moves the engine's own watermark no
/// further than it synced.
#[retcd_test]
fn store_a_flush_reports_only_what_the_engine_synced() {
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, FlushTicket};
    use rdb_core::contracts::storage::{CapturedPrefix, DurablePrefix, StorageEvent};
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    for seq in 1..=2 {
        let commit = store(StoreEffect::Commit(support::batch(1, seq, b"k", b"v")));
        deliver_on(&mut dispatcher, &mut scheduler, NODE, commit).expect("a commit");
        scheduler.pop();
    }
    let flush = |ticket: u64| {
        store(StoreEffect::Flush {
            ticket: FlushTicket(ticket),
            captured: vec![CapturedPrefix {
                partition: PartitionId(1),
                generation: Generation(1),
                through: AppliedSeq(2),
            }],
        })
    };
    let durable_through = |through: u64| DurablePrefix {
        partition: PartitionId(1),
        generation: Generation(1),
        through: DurableSeq(through),
    };

    let cases = [
        (
            Some(StorageOp::ShortFlush {
                node: NODE,
                through: AppliedSeq(1),
            }),
            StorageEvent::Flushed {
                ticket: FlushTicket(1),
                durable: vec![durable_through(1)],
            },
            1,
        ),
        (
            Some(StorageOp::FalseDurable {
                node: NODE,
                through: AppliedSeq(2),
            }),
            StorageEvent::Flushed {
                ticket: FlushTicket(2),
                durable: Vec::new(),
            },
            1,
        ),
        (
            Some(StorageOp::Fail {
                node: NODE,
                fault: StorageFault::FlushFailed,
            }),
            StorageEvent::FlushFailed {
                ticket: FlushTicket(3),
                fault: StorageFault::FlushFailed,
            },
            1,
        ),
        (
            None,
            StorageEvent::Flushed {
                ticket: FlushTicket(4),
                durable: vec![durable_through(2)],
            },
            2,
        ),
    ];
    for (ticket, (fault, answer, durable)) in (1..).zip(cases) {
        if let Some(op) = fault {
            dispatcher.inject_storage(op).expect("a planned fault");
        }
        deliver_on(&mut dispatcher, &mut scheduler, NODE, flush(ticket)).expect("a flush");
        assert_eq!(
            scheduler.pop().map(|event| event.kind),
            Some(EventKind::Storage(answer)),
            "flush {ticket}"
        );
        assert_eq!(
            dispatcher
                .engine(NODE)
                .expect("the node's engine")
                .durable(PartitionId(1), Generation(1)),
            DurableSeq(durable),
            "flush {ticket}: the engine's own watermark"
        );
    }
}

/// Lead ruling L-R177do, [`StorageOp::StallFlush`] on the engine alone. While it is planned,
/// every sync stalls: `stalled_sync` answers `true` for each, holds each capture, and nothing
/// becomes durable. No sync takes it. A crash image does not carry it: the reopened engine
/// syncs again.
#[retcd_test]
fn storage_a_stalled_flush_holds_every_sync_and_makes_nothing_durable() {
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq};
    use rdb_core::contracts::storage::CapturedPrefix;
    use rdb_sim::storage::crash_image::CrashImage;
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    let old = Generation(1);
    let mut engine = MemoryEngine::new(NODE);
    for batch in send_history(2).batches {
        engine.commit(batch).expect("a commit");
    }
    engine
        .inject(StorageOp::StallFlush { node: NODE })
        .expect("a planned stall");
    let capture = |through: u64| {
        vec![CapturedPrefix {
            partition: PartitionId(1),
            generation: old,
            through: AppliedSeq(through),
        }]
    };
    assert!(engine.stalled_sync(&capture(1)), "the first sync stalls");
    assert!(
        engine.stalled_sync(&capture(2)),
        "and so does the next: no sync takes a stall"
    );
    assert!(engine.has_planned(), "still planned");
    assert_eq!(
        engine.stalled_syncs(),
        &[capture(1), capture(2)][..],
        "each capture it held, in order"
    );
    assert_eq!(
        engine.durable(PartitionId(1), old),
        DurableSeq(0),
        "nothing durable"
    );
    let mut reopened = CrashImage::of(&engine, StorageFault::ProcessCrash)
        .expect("an image")
        .reopen(NODE);
    assert!(
        !reopened.stalled_sync(&capture(2)),
        "a restarted engine syncs again"
    );
    reopened.sync_wal_through(capture(2)).expect("a sync");
    assert_eq!(reopened.durable(PartitionId(1), old), DurableSeq(2));
}

/// Lead ruling L-R177do, through the `Store` provider: a flush on a stalled engine is never
/// answered — no `Flushed`, no `FlushFailed` — and is not silent: the engine holds its capture.
/// The same flush on the other node, which is not stalled, is answered as usual.
#[retcd_test]
fn store_a_stalled_flush_is_never_answered_and_its_capture_is_held() {
    use rdb_core::contracts::ids::{AppliedSeq, FlushTicket};
    use rdb_core::contracts::storage::{CapturedPrefix, StorageEvent};
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    let captured = vec![CapturedPrefix {
        partition: PartitionId(1),
        generation: Generation(1),
        through: AppliedSeq(1),
    }];
    let flush = store(StoreEffect::Flush {
        ticket: FlushTicket(1),
        captured: captured.clone(),
    });
    for node in [NODE, PEER] {
        let commit = store(StoreEffect::Commit(support::batch(1, 1, b"k", b"v")));
        deliver_on(&mut dispatcher, &mut scheduler, node, commit).expect("a commit");
        scheduler.pop();
    }
    dispatcher
        .inject_storage(StorageOp::StallFlush { node: NODE })
        .expect("a planned stall");
    deliver_on(&mut dispatcher, &mut scheduler, NODE, flush.clone()).expect("a flush");
    assert_eq!(scheduler.queued(), 0, "a stalled flush is never answered");
    assert_eq!(
        dispatcher
            .engine(NODE)
            .expect("the node's engine")
            .stalled_syncs(),
        &[captured][..],
        "and not silent: the engine holds its capture"
    );
    deliver_on(&mut dispatcher, &mut scheduler, PEER, flush).expect("a flush");
    assert!(
        matches!(
            scheduler.pop().map(|event| event.kind),
            Some(EventKind::Storage(StorageEvent::Flushed { .. }))
        ),
        "the node that is not stalled answers"
    );
}

#[retcd_test]
fn store_a_snapshot_is_bound_and_released_once() {
    use rdb_core::contracts::storage::StorageEvent;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    dispatcher
        .deliver(
            NODE,
            BOOT,
            vec![
                adopt(1, 1, 1, 1),
                store(StoreEffect::Commit(support::batch(1, 1, b"k", b"v"))),
            ],
            &mut ControlStore::new(),
            &mut scheduler,
        )
        .expect("an adoption and a commit");
    scheduler.pop();

    let snapshot = store(StoreEffect::Snapshot {
        handle: SnapshotHandle(4),
        partition: PartitionId(1),
    });
    deliver_on(&mut dispatcher, &mut scheduler, NODE, snapshot).expect("a snapshot");
    assert_eq!(
        scheduler.pop().map(|event| event.kind),
        Some(EventKind::Storage(StorageEvent::SnapshotReady {
            handle: SnapshotHandle(4),
            at: Seq(1),
        })),
        "bound at the adopted lineage"
    );

    let release = || {
        store(StoreEffect::Release {
            handle: SnapshotHandle(4),
        })
    };
    deliver_on(&mut dispatcher, &mut scheduler, NODE, release()).expect("a release");
    assert_eq!(scheduler.queued(), 0, "a release completes nothing");
    assert_eq!(
        deliver_on(&mut dispatcher, &mut scheduler, NODE, release()),
        Err(SimError::Config {
            field: "snapshot_handle"
        }),
        "a handle is released once"
    );
}

/// Coordinator, 2026-09-26: T1 and P1 read the stepping node's applied view; the other four
/// keep the caller's (empty) view. P1's reads are safe on it only because P1 serves nothing
/// from a view that is not at the published position.
#[retcd_test]
fn step_t1_and_p1_read_the_nodes_applied_view_and_no_other_module_does() {
    use rdb_core::contracts::storage::SnapshotRead;
    use rdb_sim::harness::dispatch::STEP_VIEW;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    assert!(
        dispatcher
            .step_view(ModuleName::Transaction, NODE, PartitionId(1))
            .is_none(),
        "no engine yet: the caller's view stands"
    );
    dispatcher
        .deliver(
            NODE,
            BOOT,
            vec![
                adopt(1, 1, 1, 1),
                store(StoreEffect::Commit(support::batch(1, 1, b"k", b"v"))),
                store(StoreEffect::Commit(support::batch(1, 2, b"k", b"w"))),
            ],
            &mut ControlStore::new(),
            &mut scheduler,
        )
        .expect("an adoption and two commits, never flushed");

    for module in ModuleName::ALL {
        let view = dispatcher.step_view(module, NODE, PartitionId(1));
        match module {
            ModuleName::Transaction | ModuleName::Publication => {
                let view = view.expect("T1 and P1 read storage");
                assert_eq!(view.at(), Seq(2), "applied, not durable: {module:?}");
                assert_eq!(view.generation(), Generation(1));
                assert_eq!(view.handle(), STEP_VIEW);
                assert_eq!(
                    view.get(rdb_core::contracts::storage::Namespace::User, b"k"),
                    Some(Bytes::from_static(b"w"))
                );
            }
            _ => assert!(view.is_none(), "{module:?} keeps the caller's view"),
        }
    }
    assert_eq!(
        dispatcher
            .step_view(ModuleName::Transaction, PEER, PartitionId(1))
            .map(|view| view.at()),
        None,
        "the peer has no engine: a node reads its own storage, never another's"
    );
}

#[retcd_test]
fn store_a_handle_still_bound_is_refused_not_rebound() {
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    let snapshot = || {
        store(StoreEffect::Snapshot {
            handle: SnapshotHandle(4),
            partition: PartitionId(1),
        })
    };
    deliver_on(&mut dispatcher, &mut scheduler, NODE, snapshot()).expect("a first bind");
    assert_eq!(scheduler.queued(), 1);

    assert_eq!(
        deliver_on(&mut dispatcher, &mut scheduler, NODE, snapshot()),
        Err(SimError::Config {
            field: "snapshot_handle"
        }),
        "a second bind of a live handle would replace a view its holder still reads"
    );
    assert_eq!(scheduler.queued(), 1, "the refused bind completes nothing");
    deliver_on(&mut dispatcher, &mut scheduler, PEER, snapshot())
        .expect("a handle is per node: the peer's is its own");
}

/// A-R69a: a crash drops every view the node held, the node refuses storage until it restarts,
/// and a handle reused after the restart opens fresh on the reopened engine. A host crash, so
/// "fresh" is visible: the unsynced commit the first view saw is gone.
#[retcd_test]
fn store_a_crash_drops_the_nodes_views_and_a_reused_handle_opens_fresh_after_restart() {
    use rdb_core::contracts::storage::StorageEvent;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    dispatcher
        .deliver(
            NODE,
            BOOT,
            vec![
                adopt(1, 1, 1, 1),
                store(StoreEffect::Commit(support::batch(1, 1, b"k", b"v"))),
            ],
            &mut ControlStore::new(),
            &mut scheduler,
        )
        .expect("an adoption and a commit");
    scheduler.pop();
    let snapshot = || {
        store(StoreEffect::Snapshot {
            handle: SnapshotHandle(4),
            partition: PartitionId(1),
        })
    };
    let release = || {
        store(StoreEffect::Release {
            handle: SnapshotHandle(4),
        })
    };
    let ready = |at: u64| {
        Some(EventKind::Storage(StorageEvent::SnapshotReady {
            handle: SnapshotHandle(4),
            at: Seq(at),
        }))
    };
    deliver_on(&mut dispatcher, &mut scheduler, NODE, snapshot()).expect("a bind");
    assert_eq!(scheduler.pop().map(|event| event.kind), ready(1));
    // The peer holds the same handle number; its view must outlive the other node's crash.
    deliver_on(&mut dispatcher, &mut scheduler, PEER, snapshot()).expect("the peer's bind");
    scheduler.pop();

    dispatcher
        .inject_storage(StorageOp::Crash {
            node: NODE,
            fault: StorageFault::HostCrash,
        })
        .expect("a planned crash");
    // Since 2026-10-02 (team i1) a crash is a fault the run goes on through, not a refusal: what
    // meets it is dropped as the dead process's.
    let drops = dispatcher.dropped().len();
    deliver_on(&mut dispatcher, &mut scheduler, NODE, release())
        .expect("the crash is taken at the next storage effect, which is dropped");
    assert!(dispatcher.is_down(NODE), "the node is down");
    deliver_on(&mut dispatcher, &mut scheduler, NODE, snapshot())
        .expect("and stays down until it restarts: what reaches it is dropped");
    assert_eq!(
        dispatcher.dropped()[drops..]
            .iter()
            .filter(|dropped| matches!(
                dropped,
                Dropped::Effects {
                    node: NODE,
                    reason: DropReason::NodeDown,
                    ..
                }
            ))
            .count(),
        2,
        "both are dropped as the dead process's, and recorded"
    );
    assert_eq!(scheduler.queued(), 0, "neither completes");
    assert_eq!(
        dispatcher.restart(PEER, PEER_BOOT),
        Err(SimError::Config { field: "restart" }),
        "only a crashed node restarts"
    );

    dispatcher
        .restart(NODE, BootId(2))
        .expect("the crashed node restarts");
    // The new process's effects carry its own boot: an effect under `BOOT` is the dead
    // process's, and `deliver` drops it (M7V-115).
    let mut deliver_restarted = |effect: Effect| {
        dispatcher.deliver(
            NODE,
            BootId(2),
            vec![effect],
            &mut ControlStore::new(),
            &mut scheduler,
        )
    };
    assert_eq!(
        deliver_restarted(release()),
        Err(SimError::Config {
            field: "snapshot_handle"
        }),
        "the crash dropped the view: there is nothing to release"
    );
    deliver_restarted(snapshot()).expect("a fresh bind");
    assert_eq!(
        scheduler.pop().map(|event| event.kind),
        ready(0),
        "the reused handle reads the reopened engine, not the lost buffer"
    );
    deliver_on(&mut dispatcher, &mut scheduler, PEER, release())
        .expect("the peer's view survived the other node's crash");
    assert_eq!(scheduler.queued(), 0);
}

/// Every `ModuleDispatch` a run recorded, in order.
fn dispatches(trace: &Trace) -> Vec<(ModuleName, DispatchOutcome)> {
    trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::ModuleDispatch {
                module, outcome, ..
            } => Some((*module, *outcome)),
            _ => None,
        })
        .collect()
}

/// A-R69a: `SnapshotReady` for a handle P1 minted is offered to P1 and to no other module.
/// The outcome is P1's business and is not asserted; the addressing is.
#[retcd_test]
fn route_a_snapshot_p1_minted_is_offered_to_p1_alone() {
    support::preamble();
    let mut runner = Runner::new(&RunPlan::new(support::cluster())).expect("a runner");
    runner
        .carry_out(
            NODE,
            BOOT,
            vec![effect_from(
                ModuleName::Publication,
                EffectKind::Store(StoreEffect::Snapshot {
                    handle: SnapshotHandle(9),
                    partition: PartitionId(1),
                }),
            )],
        )
        .expect("a bind");
    let report = runner.run(RunLimits::SMALL).expect("the run");
    let trace = runner.finish().expect("a trace");
    tracing::info!(stop = ?report.stop, "snapshot addressed to P1");

    let offered: Vec<ModuleName> = dispatches(&trace)
        .into_iter()
        .map(|(module, _)| module)
        .collect();
    assert_eq!(offered, vec![ModuleName::Publication]);
}

/// A-R69a: the addressed module's decline stops the run by name rather than drop the view it
/// asked for. A1 mints no snapshots, so it declines one addressed to it today and always will.
#[retcd_test]
fn route_an_addressed_snapshot_its_module_declines_stops_the_run_by_name() {
    use rdb_sim::harness::run::StopReason;
    support::preamble();
    let mut runner = Runner::new(&RunPlan::new(support::cluster())).expect("a runner");
    runner
        .carry_out(
            NODE,
            BOOT,
            vec![effect_from(
                ModuleName::Authority,
                EffectKind::Store(StoreEffect::Snapshot {
                    handle: SnapshotHandle(9),
                    partition: PartitionId(1),
                }),
            )],
        )
        .expect("a bind");
    let report = runner.run(RunLimits::SMALL).expect("the run");
    let trace = runner.finish().expect("a trace");

    assert!(
        matches!(
            report.stop,
            StopReason::Declined {
                module: ModuleName::Authority,
                error: RdbError::Unavailable { .. },
                ..
            }
        ),
        "{:?}",
        report.stop
    );
    assert_eq!(
        dispatches(&trace),
        vec![(ModuleName::Authority, DispatchOutcome::Declined)]
    );
}

#[retcd_test]
fn store_a_revocation_is_persisted_and_routed_to_a1() {
    use rdb_core::contracts::authority::AuthorityEvent;
    use rdb_core::contracts::event::KernelEvent;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    let revoke = effect_from(
        ModuleName::Authority,
        EffectKind::Store(StoreEffect::PersistEpochRevocation {
            partition: PartitionId(1),
            epoch: OwnerEpoch(3),
        }),
    );

    deliver_on(&mut dispatcher, &mut scheduler, NODE, revoke).expect("a revocation");

    assert!(dispatcher.epoch_revoked(NODE, PartitionId(1), OwnerEpoch(3)));
    assert!(!dispatcher.epoch_revoked(PEER, PartitionId(1), OwnerEpoch(3)));
    let persisted = scheduler.pop().expect("A1 learns");
    assert_eq!(
        persisted.kind,
        EventKind::Kernel(KernelEvent::Authority(
            AuthorityEvent::EpochRevocationPersisted {
                partition: PartitionId(1),
                epoch: OwnerEpoch(3),
            }
        ))
    );
    assert!(dispatcher.take_routed(persisted.id), "held to an answer");
    assert!(!dispatcher.take_routed(persisted.id), "asked once");
}

#[retcd_test]
fn store_a_host_flush_runs_at_its_tick_and_answers_with_the_engine() {
    use rdb_core::contracts::ids::{DurableSeq, FlushTicket};
    use rdb_core::contracts::storage::{DurablePrefix, StorageEvent};
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    let commit = store(StoreEffect::Commit(support::batch(1, 1, b"k", b"v")));
    deliver_on(&mut dispatcher, &mut scheduler, NODE, commit).expect("a commit");
    scheduler.pop();
    dispatcher.schedule_flush(Tick(5), NODE);

    assert_eq!(
        dispatcher.next_deadline(),
        Some(Tick(5)),
        "a flush is work still to do"
    );
    assert_eq!(
        dispatcher
            .fire_due_timers(Tick(4), &mut scheduler)
            .expect("nothing due"),
        0
    );
    assert_eq!(
        dispatcher
            .fire_due_timers(Tick(5), &mut scheduler)
            .expect("the flush"),
        1
    );
    let flushed = scheduler.pop().expect("the flush answers");
    assert_eq!((flushed.at, flushed.node), (Tick(5), NODE));
    assert_eq!(
        flushed.kind,
        EventKind::Storage(StorageEvent::Flushed {
            ticket: FlushTicket(0),
            durable: vec![DurablePrefix {
                partition: PartitionId(1),
                generation: Generation(1),
                through: DurableSeq(1),
            }],
        })
    );
}

/// The same-tick hop (a carried L-R175 item): a routed fact is scheduled at `now`, on the
/// emitting node, partition and correlation, and marked for the run loop.
#[retcd_test]
fn route_a_kernel_fact_is_scheduled_in_the_same_tick_on_the_same_site() {
    use rdb_core::contracts::event::KernelEvent;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    let applied = effect_from(
        ModuleName::Transaction,
        EffectKind::Kernel(KernelEffect::LocalApplied {
            seq: Seq(1),
            bytes: 10,
            record_digest: support::ROOT_DIGEST,
        }),
    );

    deliver_on(&mut dispatcher, &mut scheduler, NODE, applied).expect("a routed fact");

    let hop = scheduler.pop().expect("the fact is an event");
    assert_eq!(
        (hop.at, hop.node, hop.partition, hop.correlation),
        (Tick(0), NODE, PartitionId(1), CorrelationId(1))
    );
    assert_eq!(
        hop.kind,
        EventKind::Kernel(KernelEvent::LocalApplied {
            seq: Seq(1),
            bytes: 10,
            record_digest: support::ROOT_DIGEST,
        })
    );
    assert!(dispatcher.take_routed(hop.id));
}

/// Ruling B-R46e(2): a configuration change is offered to L1 before R1, as the trace shows. P1
/// is not a consumer (lead ruling A-R82).
#[retcd_test]
fn route_a_config_change_reaches_l1_before_r1_in_a_run() {
    use rdb_core::contracts::event::KernelEvent;
    support::preamble();
    let mut plan = RunPlan::new(support::cluster());
    plan.seed = vec![SeedEvent {
        at: Tick(1),
        node: NODE,
        boot: BOOT,
        partition: PartitionId(1),
        correlation: CorrelationId(1),
        kind: EventKind::Kernel(KernelEvent::ConfigChanged(support::rf3_config())),
    }];
    plan.limits.max_events = 1;
    let (trace, report) = execute(&plan).expect("a run");

    let order: Vec<ModuleName> = trace
        .events
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::ModuleDispatch { module, .. } => Some(module),
            _ => None,
        })
        .collect();
    tracing::info!(order = ?order, stop = ?report.stop, "route config order");
    assert!(order.len() >= 2, "both consumers were offered: {order:?}");
    assert_eq!(
        order[..2],
        [ModuleName::Protection, ModuleName::Replication],
        "consumers first, L1 before R1: {order:?}"
    );
}

#[retcd_test]
fn route_a_routed_fact_its_wired_consumer_declines_stops_the_run_by_name() {
    use rdb_sim::harness::run::StopReason;
    support::preamble();
    let mut runner = Runner::new(&RunPlan::new(support::cluster())).expect("a runner");
    runner
        .carry_out(
            NODE,
            BOOT,
            vec![effect_from(
                ModuleName::Replication,
                EffectKind::Kernel(KernelEffect::DivergenceDetected { copy: CopyId(2) }),
            )],
        )
        .expect("routed, not refused, at delivery");

    let report = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail");

    assert!(
        matches!(
            report.stop,
            StopReason::Declined {
                module: ModuleName::Replication,
                error: RdbError::Unavailable { .. },
                ..
            }
        ),
        "R1 holds no primary, so it declines, and the decline stops the run: {:?}",
        report.stop
    );
}

/// Lead rulings A-R62 condition 1, A-R65.2 and A-R82..A-R84: nothing is owed any more. This
/// used to hold every owed edge's consumer to `Unavailable`, so a capability flip forced the
/// table to be re-read. Since 2026-09-28 the table is empty: T1's and P1's seventeen edges left
/// together (fourteen answered, the answer arm split by checkpoint, the foreign fence answered
/// `Ignored(NotOurs)`, and `ConfigChanged` no longer offered to P1), and R1's one edge left with
/// B-R53. So the T1/P1 capability flip is unblocked by routing; what still gates it is kernel-a's
/// own manual-tester gate (A-R67.1), not this table. An edge added back fails here, and comes
/// back only with a ruling.
#[retcd_test]
fn route_nothing_is_owed_any_more() {
    use rdb_sim::harness::route::OWED_EDGES;
    support::preamble();
    let report = Dispatcher::new().capability_report();
    tracing::info!(edges = OWED_EDGES.len(), capability = ?report, "owed edges");
    assert!(
        OWED_EDGES.is_empty(),
        "an owed edge came back: {OWED_EDGES:?}"
    );
}

/// M7A-192. Lead ruling A-R84 (dev-edges defect D1, end to end). On a node serving `p1` and
/// `p2`, A1 steps only `p1`'s events, yet each view and each partition fence it emits is
/// delivered to the partition it concerns: the install's view of `p2` is offered at `(n1, p2)`,
/// and a revocation of `p2` requested on `p1` fences `(n1, p2)` and never `(n1, p1)`, and its
/// paired past view of `p2` (popped after that fence) is offered at `(n1, p2)` too. And the run
/// goes on, because P1 answers every fence and view it is offered. Before the fix both install
/// views popped at `p1`, `p2`'s fence popped at `p1`, and P1's decline of that foreign fence
/// stopped the run as a refusal once its edge left `OWED_EDGES`.
///
/// Pops are told apart by their offer order (`route::offer_order`): a view is offered to R1,
/// T1, P1 first, a fence to T1, P1 first.
#[retcd_test]
fn m7a_192_a_view_and_a_fence_for_p2_reach_p2_not_p1() {
    use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
    use rdb_core::authority::AuthorityTimer;
    use rdb_core::contracts::authority::AuthorityEvent;
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::time::TimerFired;
    use rdb_sim::harness::run::StopReason;
    use ModuleName::{Publication, Replication, Transaction};
    const P1: PartitionId = PartitionId(1);
    const P2: PartitionId = PartitionId(2);
    support::preamble();

    let record = |partition| {
        let record = PartitionRecord {
            partition,
            owner: NODE,
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
            config_version: ConfigVersion(1),
            lifecycle: PartitionLifecycle::Serving,
        };
        (ControlKey::Partition(partition), record.encode())
    };
    let seed = |at: u64, kind| SeedEvent {
        at: Tick(at),
        node: NODE,
        boot: BOOT,
        partition: P1,
        correlation: CorrelationId(at),
        kind,
    };
    let mut plan = RunPlan::new(support::cluster());
    plan.control_records = vec![record(P1), record(P2)];
    plan.seed = vec![
        seed(
            1,
            EventKind::Timer(TimerFired {
                id: AuthorityTimer::Acquire.id(),
                version: TimerVersion(0),
                scheduled_at: Tick(1),
            }),
        ),
        seed(
            20,
            EventKind::Kernel(KernelEvent::Authority(
                AuthorityEvent::RevokeEpochRequested {
                    partition: P2,
                    epoch: OwnerEpoch(1),
                },
            )),
        ),
    ];
    plan.limits = RunLimits {
        max_events: 200,
        deadline: Tick(60),
    };
    let (trace, report) = execute(&plan).expect("a run");

    // Each pop: its partition and tick, and its offers in order.
    let mut pops: std::collections::BTreeMap<u64, (PartitionId, u64, Vec<ModuleName>)> =
        std::collections::BTreeMap::new();
    for event in &trace.events {
        if let TraceKind::ModuleDispatch {
            event: id, module, ..
        } = &event.kind
        {
            pops.entry(id.0)
                .or_insert_with(|| (event.partition, event.logical_tick, Vec::new()))
                .2
                .push(*module);
        }
    }
    let sited = |prefix: &[ModuleName], tick: u64| -> Vec<PartitionId> {
        pops.values()
            .filter(|(_, at, order)| *at == tick && order.starts_with(prefix))
            .map(|(partition, _, _)| *partition)
            .collect()
    };
    let (install_views, fences) = (
        sited(&[Replication, Transaction, Publication], 1),
        sited(&[Transaction, Publication], 20),
    );
    // The revocation's paired past view: a view popped at tick 20 after the fence was.
    let fence_id = pops
        .iter()
        .find(|(_, (_, at, order))| *at == 20 && order.starts_with(&[Transaction, Publication]))
        .map(|(id, _)| *id);
    let past_views: Vec<PartitionId> = pops
        .iter()
        .filter(|(id, (_, at, order))| {
            *at == 20
                && Some(**id) > fence_id
                && order.starts_with(&[Replication, Transaction, Publication])
        })
        .map(|(_, (partition, _, _))| *partition)
        .collect();
    tracing::info!(
        stop = ?report.stop, ?install_views, ?fences, ?past_views, "m7a_192 sited pops"
    );

    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "M7A-192: no consumer declined a routed fence or view: {:?}",
        report.stop
    );
    assert_eq!(
        past_views,
        vec![P2],
        "M7A-192: the revocation's paired past view of p2 is sited at p2"
    );
    assert_eq!(
        install_views,
        vec![P1, P2],
        "M7A-192: one install view at each partition"
    );
    assert_eq!(
        fences,
        vec![P2],
        "M7A-192: p2's revocation fences p2, not p1"
    );
}

/// Lead ruling A-R64(b): none of F1's seven requests to the environment is recorded as a
/// `RecoveryFact`. Two are refused by name for want of a provider. `QueryInventory`,
/// `SyncWalThrough` and, since 2026-10-02 (team i1), `ProbeDigestAt` answer. `CatchUp` and `CatchUpBeforeGrant` have a provider (B-R59), but no
/// copy is placed here, so they are refused by the same name; the placed case is
/// `provider_catch_up_goes_to_the_node_holding_its_source`.
#[retcd_test]
fn provider_no_recovery_request_is_recorded_as_a_fact() {
    use rdb_core::contracts::authority::{FenceCredential, Lineage};
    use rdb_core::contracts::ids::Revision;
    use rdb_core::contracts::recovery::LineageAnchor;
    support::preamble();
    let credential = FenceCredential {
        partition: PartitionId(1),
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(1),
        control_revision: Revision(1),
        sender: CopyId(1),
    };
    let refused = Some("harness::dispatch::deliver::recovery");
    let requests = [
        (
            RecoveryEffect::QueryInventory {
                copies: vec![CopyId(1)],
            },
            None,
        ),
        (
            RecoveryEffect::SyncWalThrough {
                copy: CopyId(1),
                cutoff: Seq(1),
            },
            None,
        ),
        (
            RecoveryEffect::ProbeDigestAt {
                copy: CopyId(1),
                seq: Seq(1),
            },
            None,
        ),
        (
            RecoveryEffect::CatchUp {
                from: CopyId(1),
                to: CopyId(2),
                through: Seq(1),
                credential,
            },
            refused,
        ),
        (
            RecoveryEffect::CatchUpBeforeGrant {
                from: CopyId(1),
                to: CopyId(2),
                through: Seq(1),
                credential,
            },
            refused,
        ),
        (
            RecoveryEffect::QuarantineSuffix {
                copy: CopyId(1),
                from: Seq(2),
                until: Tick(10),
            },
            refused,
        ),
        (
            RecoveryEffect::RebuildFromAuthoritative {
                copy: CopyId(1),
                root: LineageAnchor {
                    lineage: Lineage {
                        partition: PartitionId(1),
                        generation: Generation(1),
                        owner_epoch: OwnerEpoch(1),
                    },
                    base_seq: Seq(0),
                    base_digest: support::ROOT_DIGEST,
                },
            },
            refused,
        ),
    ];
    for (request, refusal) in requests {
        let mut dispatcher = two_nodes();
        let mut scheduler = Scheduler::new();
        let answer = deliver_on(
            &mut dispatcher,
            &mut scheduler,
            NODE,
            effect_from(
                ModuleName::Recovery,
                EffectKind::Kernel(KernelEffect::Recovery(request.clone())),
            ),
        );
        match refusal {
            Some(seam) => assert_eq!(answer, Err(SimError::Unavailable { seam }), "{request:?}"),
            None => assert_eq!(answer, Ok(()), "{request:?} has a provider"),
        }
        let facts: Vec<KernelNote> = dispatcher
            .take_notes()
            .into_iter()
            .map(|(_, _, note)| note)
            .filter(|note| matches!(note, KernelNote::RecoveryFact { .. }))
            .collect();
        assert!(
            facts.is_empty(),
            "{request:?} was recorded as a fact: {facts:?}"
        );
    }
}

/// The probe scenario's other answer (team i1, 2026-10-02): a copy that cannot answer — never
/// placed, or placed on a node that has since crashed — answers `ProbeUnavailable`, routed back
/// to the asker, and never a digest the harness made up.
#[retcd_test]
fn provider_a_probe_no_holder_can_answer_is_unavailable() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::RecoveryEvent;
    support::preamble();
    let probe = |copy: CopyId| {
        effect_from(
            ModuleName::Recovery,
            EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::ProbeDigestAt {
                copy,
                seq: Seq(1),
            })),
        )
    };
    let unavailable = |copy: CopyId| {
        Some(EventKind::Kernel(KernelEvent::Recovery(
            RecoveryEvent::ProbeUnavailable { copy, seq: Seq(1) },
        )))
    };
    let mut dispatcher = with_survivor();
    let mut scheduler = Scheduler::new();

    deliver_on(&mut dispatcher, &mut scheduler, NODE, probe(CopyId(2))).expect("a probe");
    let answer = scheduler.pop().expect("an answer");
    assert_eq!(
        (answer.node, answer.partition),
        (NODE, PartitionId(1)),
        "to the asker"
    );
    assert!(dispatcher.take_routed(answer.id), "held to F1's answer");
    assert_eq!(
        Some(answer.kind),
        unavailable(CopyId(2)),
        "copy 2 was never placed"
    );

    dispatcher
        .inject_storage(StorageOp::Crash {
            node: PEER,
            fault: StorageFault::ProcessCrash,
        })
        .expect("a planned crash");
    let touch = store(StoreEffect::Commit(support::batch(1, 3, b"k", b"v")));
    deliver_on(&mut dispatcher, &mut scheduler, PEER, touch).expect("the crash is taken");
    deliver_on(&mut dispatcher, &mut scheduler, NODE, probe(CopyId(1))).expect("a probe");
    assert_eq!(
        scheduler.pop().map(|event| event.kind),
        unavailable(CopyId(1)),
        "copy 1's holder is down"
    );
}

/// Lead rulings A-R65.3 and A-R66: P1's output to the environment is recorded, not refused.
#[retcd_test]
fn provider_a_publication_output_is_recorded_as_a_fact() {
    use rdb_core::contracts::publication::PublicationEffect;
    support::preamble();
    let mut dispatcher = two_nodes();
    let mut scheduler = Scheduler::new();
    let quarantined = PublicationEffect::Quarantined {
        generation: Generation(1),
        seq: Seq(3),
    };

    deliver_on(
        &mut dispatcher,
        &mut scheduler,
        NODE,
        effect_from(
            ModuleName::Publication,
            EffectKind::Kernel(KernelEffect::Publication(quarantined.clone())),
        ),
    )
    .expect("recorded, not refused");

    assert_eq!(
        dispatcher.take_notes(),
        vec![(
            NODE,
            ModuleName::Publication,
            KernelNote::PublicationFact {
                effect: quarantined
            }
        )]
    );
    assert_eq!(
        scheduler.queued(),
        0,
        "no consumer, so nothing is scheduled"
    );
}

fn digest(seq: u64) -> rdb_core::contracts::digest::Digest {
    rdb_core::contracts::digest::Digest([u8::try_from(seq).expect("a small sequence"); 32])
}

/// Copy `copy` of partition 1 at generation 1, head `head`, with a rung at every sequence.
fn survivor(copy: u8, head: u64) -> rdb_core::contracts::recovery::SurvivorInventory {
    use rdb_core::contracts::authority::Lineage;
    use rdb_core::contracts::recovery::{LineageAnchor, SurvivorInventory};
    SurvivorInventory {
        copy: CopyId(copy),
        anchor_seen: LineageAnchor {
            lineage: Lineage {
                partition: PartitionId(1),
                generation: Generation(1),
                owner_epoch: OwnerEpoch(1),
            },
            base_seq: Seq(0),
            base_digest: support::ROOT_DIGEST,
        },
        head: (Seq(head), digest(head)),
        ladder: (1..=head).map(|seq| (Seq(seq), digest(seq))).collect(),
        quarantined: None,
    }
}

/// `two_nodes()`, with `PEER` holding sequences 1 and 2 of partition 1 and copy 1 placed there.
fn with_survivor() -> Dispatcher {
    let mut dispatcher = two_nodes();
    for seq in 1..=2 {
        dispatcher
            .preload(PEER, support::batch(1, seq, b"k", b"v"))
            .expect("a preload");
    }
    dispatcher
        .place_survivor(PEER, PartitionId(1), survivor(1, 2))
        .expect("a head the engine holds");
    dispatcher
}

/// A-R64(c), A-R67.3: `QueryInventory` answers each copy in order, from what the scenario
/// placed against the node's own engine, and a copy nobody can report for fails.
#[retcd_test]
fn provider_query_inventory_reports_placed_survivors_and_fails_the_rest() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::RecoveryEvent;
    support::preamble();
    let mut dispatcher = with_survivor();
    assert_eq!(
        dispatcher.take_notes(),
        vec![(
            PEER,
            ModuleName::Recovery,
            KernelNote::SurvivorPlaced {
                partition: PartitionId(1),
                inventory: Box::new(survivor(1, 2)),
            }
        )],
        "a placement is recorded as declared (A-R67.3b)"
    );
    assert_eq!(
        dispatcher.place_survivor(NodeId(3), PartitionId(1), survivor(2, 2)),
        Err(SimError::Config {
            field: "survivor_head"
        }),
        "a head no engine holds is refused (A-R67.3a)"
    );
    assert!(
        dispatcher.take_notes().is_empty(),
        "and a refused one is not recorded"
    );

    let query = |copies: Vec<CopyId>| {
        effect_from(
            ModuleName::Recovery,
            EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::QueryInventory {
                copies,
            })),
        )
    };
    let mut scheduler = Scheduler::new();
    let asked = query(vec![CopyId(1), CopyId(2)]);
    deliver_on(&mut dispatcher, &mut scheduler, NODE, asked).expect("a query");

    let mut answers = Vec::new();
    while let Some(event) = scheduler.pop() {
        assert_eq!(
            (event.node, event.partition),
            (NODE, PartitionId(1)),
            "to the asker"
        );
        assert!(dispatcher.take_routed(event.id), "held to F1's answer");
        answers.push(event.kind);
    }
    assert_eq!(
        answers,
        vec![
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::InventoryReported(
                Box::new(survivor(1, 2))
            ))),
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::InventoryFailed {
                copy: CopyId(2)
            })),
        ]
    );

    // A crashed holder cannot report.
    dispatcher
        .inject_storage(StorageOp::Crash {
            node: PEER,
            fault: StorageFault::ProcessCrash,
        })
        .expect("a planned crash");
    let touch = store(StoreEffect::Commit(support::batch(1, 3, b"k", b"v")));
    deliver_on(&mut dispatcher, &mut scheduler, PEER, touch)
        .expect("the crash is taken, a fault and not a refusal");
    assert!(dispatcher.is_down(PEER), "the holder is down");
    deliver_on(
        &mut dispatcher,
        &mut scheduler,
        NODE,
        query(vec![CopyId(1)]),
    )
    .expect("a query");
    assert_eq!(
        scheduler.pop().map(|event| event.kind),
        Some(EventKind::Kernel(KernelEvent::Recovery(
            RecoveryEvent::InventoryFailed { copy: CopyId(1) }
        )))
    );
}

/// Copy 1 of [`send_history`] as placed at generation 1: root to seq 3, a rung at every seq.
fn placed_at_gen1(prior: &rdb_sim::storage::history::CanonicalHistory) -> SurvivorInventory {
    SurvivorInventory {
        copy: CopyId(1),
        anchor_seen: LineageAnchor {
            lineage: send_lineage(),
            base_seq: Seq(0),
            base_digest: prior.digest(0),
        },
        head: (Seq(3), prior.digest(3)),
        ladder: (0..=3).map(|seq| (Seq(seq), prior.digest(seq))).collect(),
        quarantined: None,
    }
}

/// F1's result for generation 2 of partition 1, cut from generation 1 at seq 2 with
/// `cutoff_digest`, pinning copy 1 on `PEER` and copy 2 on `NODE`. No barrier, no loss.
fn gen2_result(cutoff_digest: Digest) -> rdb_core::contracts::recovery::RecoveryResult {
    use rdb_core::contracts::authority::{
        AuthorityView, DenyReason, FencingProof, Lineage, PartitionMode, Revocation,
    };
    use rdb_core::contracts::ids::{AuthorityGeneration, GrantId, Revision};
    use rdb_core::contracts::membership::{Member, PartitionConfig};
    use rdb_core::contracts::recovery::{
        CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap,
        SelectedLineage,
    };
    let (prior, cutoff) = (Generation(1), Seq(2));
    let root = Lineage {
        generation: Generation(2),
        ..send_lineage()
    };
    let member = |copy: u8, node: NodeId, boot: BootId, role| Member {
        copy: CopyId(copy),
        node,
        boot,
        role,
    };
    RecoveryResult {
        fenced_prior: FencingProof {
            partition: PartitionId(1),
            prior_generation: prior,
            prior_owner_epoch: OwnerEpoch(1),
            prior_grant_id: GrantId(1),
            prior_boot_id: BOOT,
            revocation: Revocation::DurableDrain {
                ack_revision: Revision(1),
            },
            control_revision: Revision(1),
            decision_tick: Tick::ZERO,
        },
        inventories: Vec::new(),
        selected: SelectedLineage {
            root,
            cutoff_seq: cutoff,
            cutoff_digest,
            source: CopyId(1),
        },
        new_generation: root.generation,
        mode: PartitionMode::Active,
        barrier: RecoveryBarrier::try_new(&[], &Default::default(), cutoff, cutoff_digest)
            .expect("an empty required set needs no proof"),
        loss: LossRecord {
            queried: Vec::new(),
            unavailable: Vec::new(),
            cutoff_seq: cutoff,
            highest_advertised_seq: Seq(3),
            uncertain: false,
        },
        committed: CommittedRoot {
            revision: Revision(2),
            pinned_config: PartitionConfig::new(
                PartitionId(1),
                ConfigVersion(1),
                vec![
                    member(1, PEER, PEER_BOOT, ReplicaRole::Primary),
                    member(2, NODE, BOOT, ReplicaRole::RegularSecondary),
                ],
            ),
            authority_view: AuthorityView {
                lineage: root,
                grant_id: GrantId(2),
                boot_id: PEER_BOOT,
                authority_generation: AuthorityGeneration(1),
                config_version: ConfigVersion(1),
                authority_seq: 1,
                valid_through_tick: Tick(u64::MAX),
                past_horizon: DenyReason::Expired,
            },
        },
        retained_status_map: RetainedStatusMap {
            predecessor_generation: prior,
            predecessor_cutoff: cutoff,
            retained_through: cutoff,
            discarded_from: Some(Seq(3)),
            uncertain: false,
        },
    }
}

/// `PEER` placed as copy 1 at generation 1 (seqs 1-3), then recovered by its own F1 into
/// generation 2 at cutoff 2 with `cutoff_digest`, adopting it and writing seq 3 of the new
/// lineage. What it holds is [`send_history`] to 2, then [`send_history_after`] from 2 to 3.
fn survivor_that_moved_on(cutoff_digest: Digest) -> Dispatcher {
    let prior = send_history(3);
    let mut dispatcher = two_nodes();
    for batch in prior.batches.clone() {
        dispatcher.preload(PEER, batch).expect("a preload");
    }
    dispatcher
        .place_survivor(PEER, PartitionId(1), placed_at_gen1(&prior))
        .expect("a head the engine holds");
    let mut scheduler = Scheduler::new();
    let recovered = effect_from(
        ModuleName::Recovery,
        EffectKind::Kernel(KernelEffect::Recovered(Box::new(gen2_result(
            cutoff_digest,
        )))),
    );
    deliver_on(&mut dispatcher, &mut scheduler, PEER, recovered).expect("F1's result");
    deliver_on(&mut dispatcher, &mut scheduler, PEER, adopt(1, 2, 1, 1)).expect("an adoption");
    for batch in send_history_after(2, &prior, 2, 3).batches {
        dispatcher
            .preload(PEER, batch)
            .expect("a write of the new lineage");
    }
    dispatcher
}

/// q1's request under the recovery seam (lead-approved 2026-10-02, team i1): a placed survivor
/// whose holder has since adopted a newer generation answers from **that** generation, as F1's
/// own committed root names it, and never from the placement it has outgrown. Both providers
/// that read a placement agree: `QueryInventory` and the pre-commit arm of `SyncWalThrough`.
///
/// Every digest is the holder engine's own stored one; a committed base the engine does not
/// hold is refused by name rather than reported.
#[retcd_test]
fn provider_a_survivor_that_adopted_a_newer_generation_answers_from_it() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::ids::DurableSeq;
    use rdb_core::contracts::recovery::{DurableProof, RecoveryEvent};
    support::preamble();
    let prior = send_history(3);
    let after = send_history_after(2, &prior, 2, 3);
    let effect = |kind: RecoveryEffect| {
        effect_from(
            ModuleName::Recovery,
            EffectKind::Kernel(KernelEffect::Recovery(kind)),
        )
    };
    let query = || {
        effect(RecoveryEffect::QueryInventory {
            copies: vec![CopyId(1)],
        })
    };

    let mut dispatcher = survivor_that_moved_on(prior.digest(2));
    let mut scheduler = Scheduler::new();
    deliver_on(&mut dispatcher, &mut scheduler, NODE, query()).expect("a query");
    let current = SurvivorInventory {
        copy: CopyId(1),
        anchor_seen: LineageAnchor {
            lineage: rdb_core::contracts::authority::Lineage {
                generation: Generation(2),
                ..send_lineage()
            },
            base_seq: Seq(2),
            base_digest: prior.digest(2),
        },
        head: (Seq(3), after.digest(3)),
        ladder: vec![(Seq(2), prior.digest(2)), (Seq(3), after.digest(3))],
        quarantined: None,
    };
    tracing::info!(
        anchor_generation = current.anchor_seen.lineage.generation.0,
        base_seq = current.anchor_seen.base_seq.0,
        head = current.head.0 .0,
        "survivor answers from its adopted generation"
    );
    assert_eq!(
        scheduler.pop().map(|event| event.kind),
        Some(EventKind::Kernel(KernelEvent::Recovery(
            RecoveryEvent::InventoryReported(Box::new(current))
        ))),
        "anchored at the gen-2 committed root, head and ladder as the engine stores them"
    );

    let mut scheduler = Scheduler::new();
    let sync = effect(RecoveryEffect::SyncWalThrough {
        copy: CopyId(1),
        cutoff: Seq(3),
    });
    deliver_on(&mut dispatcher, &mut scheduler, NODE, sync).expect("a sync");
    assert_eq!(
        scheduler.pop().map(|event| event.kind),
        Some(EventKind::Kernel(KernelEvent::Recovery(
            RecoveryEvent::DurableAt(DurableProof {
                copy: CopyId(1),
                partition: PartitionId(1),
                seq: DurableSeq(3),
                digest: after.digest(3),
            })
        ))),
        "the pre-commit sync proves seq 3 of generation 2, not the placed generation-1 record"
    );

    let mut dispatcher = survivor_that_moved_on(digest(9));
    let mut scheduler = Scheduler::new();
    assert_eq!(
        deliver_on(&mut dispatcher, &mut scheduler, NODE, query()),
        Err(SimError::Config {
            field: "survivor_base"
        }),
        "a committed base the holder's engine does not store is refused, never reported"
    );
}

/// q1's second-recovery scenario (L-R182z, team i1): a placed survivor whose holder is a
/// **secondary** of a recovery took no adoption, only a landing. When it is in that recovery's
/// barrier its first landing inherits the committed prefix (B-R58c), so it descends from the
/// committed root exactly as an adopter does, and a later `QueryInventory` answers from that
/// generation. Before the fix the provider read the holder's adoption alone and answered from
/// the outgrown placement, which F1 then judged `StaleLineage`.
///
/// A member outside the barrier lands empty and inherits nothing, so it still answers from its
/// placement: a landing alone proves nothing about the holder's storage.
#[retcd_test]
fn provider_a_survivor_that_landed_a_newer_generation_in_the_barrier_answers_from_it() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::ids::DurableSeq;
    use rdb_core::contracts::recovery::{DurableProof, RecoveryBarrier, RecoveryEvent};
    use rdb_sim::harness::dispatch::CONTROL_WATCH_MILLIS;
    use std::collections::BTreeSet;
    support::preamble();
    let prior = send_history(3);
    let landed_under = |required: bool, ask: RecoveryEffect| {
        let mut result = gen2_result(prior.digest(2));
        if required {
            let proof = DurableProof {
                copy: CopyId(1),
                partition: PartitionId(1),
                seq: DurableSeq(2),
                digest: prior.digest(2),
            };
            result.barrier = RecoveryBarrier::try_new(
                &[proof],
                &BTreeSet::from([CopyId(1)]),
                Seq(2),
                prior.digest(2),
            )
            .expect("copy 1 proves the cutoff");
        }
        let mut dispatcher = two_nodes();
        for batch in prior.batches.clone() {
            dispatcher.preload(PEER, batch).expect("a preload");
        }
        dispatcher
            .place_survivor(PEER, PartitionId(1), placed_at_gen1(&prior))
            .expect("a head the engine holds");
        let mut scheduler = Scheduler::new();
        let recovered = effect_from(
            ModuleName::Recovery,
            EffectKind::Kernel(KernelEffect::Recovered(Box::new(result))),
        );
        deliver_on(&mut dispatcher, &mut scheduler, NODE, recovered).expect("F1's result");
        dispatcher
            .fire_due_timers(Tick(CONTROL_WATCH_MILLIS), &mut scheduler)
            .expect("PEER's watch fires");
        assert_eq!(
            dispatcher.adopted(PEER, PartitionId(1)),
            Adopted::default(),
            "PEER only landed generation 2; it never adopted"
        );
        let mut scheduler = Scheduler::new();
        let ask = effect_from(
            ModuleName::Recovery,
            EffectKind::Kernel(KernelEffect::Recovery(ask)),
        );
        deliver_on(&mut dispatcher, &mut scheduler, NODE, ask).expect("answered");
        scheduler.pop().map(|event| event.kind)
    };
    let query = || RecoveryEffect::QueryInventory {
        copies: vec![CopyId(1)],
    };
    let answered = |inventory: SurvivorInventory| {
        Some(EventKind::Kernel(KernelEvent::Recovery(
            RecoveryEvent::InventoryReported(Box::new(inventory)),
        )))
    };

    let current = SurvivorInventory {
        copy: CopyId(1),
        anchor_seen: LineageAnchor {
            lineage: rdb_core::contracts::authority::Lineage {
                generation: Generation(2),
                ..send_lineage()
            },
            base_seq: Seq(2),
            base_digest: prior.digest(2),
        },
        head: (Seq(2), prior.digest(2)),
        ladder: vec![(Seq(2), prior.digest(2))],
        quarantined: None,
    };
    assert_eq!(
        landed_under(true, query()),
        answered(current),
        "in the barrier: anchored at the gen-2 committed root it inherited, head at the cutoff"
    );
    assert_eq!(
        landed_under(false, query()),
        answered(placed_at_gen1(&prior)),
        "outside the barrier: landed empty, so it still answers from its placement"
    );
    // A probe reads the same generation the query reports (tester-m7c-i1 A2). Generation 2
    // holds nothing past its cutoff at 2; the placed generation-1 record does hold seq 3.
    assert_eq!(
        landed_under(
            true,
            RecoveryEffect::ProbeDigestAt {
                copy: CopyId(1),
                seq: Seq(3),
            },
        ),
        Some(EventKind::Kernel(KernelEvent::Recovery(
            RecoveryEvent::ProbeUnavailable {
                copy: CopyId(1),
                seq: Seq(3),
            }
        ))),
        "in the barrier: probed in generation 2, never in the outgrown placement"
    );
}

/// A-R64(c), A-R67.4: `SyncWalThrough` proves exactly the cutoff when, and only when, the
/// holder's engine made it durable; every other outcome is a named `SyncWithheld`, never a
/// proof and never nothing. B-R55a: a proof is recorded as `SyncProven`, so every request below
/// is asserted to carry exactly one of the two notes.
#[retcd_test]
fn provider_sync_wal_through_proves_only_what_the_engine_made_durable() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq};
    use rdb_core::contracts::recovery::{DurableProof, RecoveryEvent};
    use rdb_core::contracts::trace::SyncWithheldReason;
    support::preamble();
    let sync = |copy: u8, cutoff: u64| {
        effect_from(
            ModuleName::Recovery,
            EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::SyncWalThrough {
                copy: CopyId(copy),
                cutoff: Seq(cutoff),
            })),
        )
    };
    let proof = |seq: u64| {
        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::DurableAt(
            DurableProof {
                copy: CopyId(1),
                partition: PartitionId(1),
                seq: DurableSeq(seq),
                digest: digest(seq),
            },
        )))
    };
    let withheld = |copy: u8, cutoff: u64, reason: SyncWithheldReason| {
        vec![(
            NODE,
            ModuleName::Recovery,
            KernelNote::SyncWithheld {
                copy: CopyId(copy),
                cutoff: Seq(cutoff),
                reason,
            },
        )]
    };

    // A clean sync proves the cutoff; a later, lower cutoff is proved from what is durable.
    let mut dispatcher = with_survivor();
    dispatcher.take_notes();
    let mut scheduler = Scheduler::new();
    for cutoff in [2, 1] {
        deliver_on(&mut dispatcher, &mut scheduler, NODE, sync(1, cutoff)).expect("a sync");
        let answer = scheduler.pop().expect("a proof");
        assert_eq!(answer.kind, proof(cutoff), "cutoff {cutoff}");
        assert_eq!((answer.node, answer.partition), (NODE, PartitionId(1)));
        assert!(dispatcher.take_routed(answer.id));
        assert_eq!(
            dispatcher.take_notes(),
            vec![(
                NODE,
                ModuleName::Recovery,
                KernelNote::SyncProven {
                    copy: CopyId(1),
                    cutoff: Seq(cutoff),
                    durable: DurableSeq(2),
                },
            )],
            "a proof is recorded as proven, once, with what the engine reported (B-R55a)"
        );
    }
    assert_eq!(
        dispatcher
            .engine(PEER)
            .expect("the holder's engine")
            .durable(PartitionId(1), Generation(1)),
        DurableSeq(2),
        "synced on the holder, not on the asker"
    );

    // Every way the proof can fail to exist, each against a fresh holder.
    let cases = [
        (
            Some(StorageOp::ShortFlush {
                node: PEER,
                through: AppliedSeq(1),
            }),
            1,
            2,
            SyncWithheldReason::Short {
                durable: DurableSeq(1),
            },
        ),
        (
            Some(StorageOp::FalseDurable {
                node: PEER,
                through: AppliedSeq(2),
            }),
            1,
            2,
            SyncWithheldReason::Short {
                durable: DurableSeq(0),
            },
        ),
        (
            Some(StorageOp::Fail {
                node: PEER,
                fault: StorageFault::FlushFailed,
            }),
            1,
            2,
            SyncWithheldReason::Failed(StorageFault::FlushFailed),
        ),
        (None, 9, 2, SyncWithheldReason::NotPlaced),
        (
            None,
            1,
            3,
            SyncWithheldReason::Short {
                durable: DurableSeq(2),
            },
        ),
    ];
    for (fault, copy, cutoff, reason) in cases {
        let mut dispatcher = with_survivor();
        dispatcher.take_notes();
        let mut scheduler = Scheduler::new();
        if let Some(op) = fault {
            dispatcher.inject_storage(op).expect("a planned fault");
        }
        deliver_on(&mut dispatcher, &mut scheduler, NODE, sync(copy, cutoff)).expect("a sync");
        assert_eq!(scheduler.queued(), 0, "{reason:?}: no proof");
        assert_eq!(dispatcher.take_notes(), withheld(copy, cutoff, reason));
    }

    // A durable cutoff the placed history has no digest for is not proved either.
    let mut dispatcher = two_nodes();
    for seq in 1..=2 {
        dispatcher
            .preload(PEER, support::batch(1, seq, b"k", b"v"))
            .expect("a preload");
    }
    let mut sparse = survivor(1, 2);
    sparse.ladder.clear();
    dispatcher
        .place_survivor(PEER, PartitionId(1), sparse)
        .expect("a head the engine holds");
    dispatcher.take_notes();
    let mut scheduler = Scheduler::new();
    deliver_on(&mut dispatcher, &mut scheduler, NODE, sync(1, 1)).expect("a sync");
    assert_eq!(scheduler.queued(), 0, "no digest, no proof");
    assert_eq!(
        dispatcher.take_notes(),
        withheld(1, 1, SyncWithheldReason::NoDigest)
    );
}

// ------------------------------------------------------------------------------------------
// The spine (dev-sim-route, 2026-09-26): one multi-node run through A1, F1, R1 and L1.
// ------------------------------------------------------------------------------------------

/// Partition 1's survivors hold sequences 1..=`SPINE_HEAD` of the prior lineage (1, 1).
const SPINE_HEAD: u64 = 2;
/// The partition A1 serves on node 1 while partition 1 recovers.
const SERVED: PartitionId = PartitionId(2);

/// The survivors' shared history: records `1..=SPINE_HEAD` of the prior lineage, as T1 commits
/// them, so `SendEnvelopes` reads and sends real envelopes (lead ruling B-R57, rule 3).
fn spine_history() -> rdb_sim::storage::history::CanonicalHistory {
    rdb_sim::storage::history::canonical_history(spine_prior(), ConfigVersion(1), SPINE_HEAD)
        .expect("a canonical history")
}

/// The digest a survivor's history holds at `seq`; the root (seq 0) is the anchor's digest.
fn spine_digest(seq: u64) -> rdb_core::contracts::digest::Digest {
    spine_history().digest(seq)
}

fn spine_prior() -> rdb_core::contracts::authority::Lineage {
    rdb_core::contracts::authority::Lineage {
        partition: PartitionId(1),
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

fn spine_anchor() -> rdb_core::contracts::recovery::LineageAnchor {
    rdb_core::contracts::recovery::LineageAnchor {
        lineage: spine_prior(),
        base_seq: Seq(0),
        base_digest: spine_digest(0),
    }
}

/// Copy `copy`'s report: the whole shared history, root to `SPINE_HEAD`.
fn spine_survivor(copy: u8) -> rdb_core::contracts::recovery::SurvivorInventory {
    rdb_core::contracts::recovery::SurvivorInventory {
        copy: CopyId(copy),
        anchor_seen: spine_anchor(),
        head: (Seq(SPINE_HEAD), spine_digest(SPINE_HEAD)),
        ladder: (0..=SPINE_HEAD)
            .map(|seq| (Seq(seq), spine_digest(seq)))
            .collect(),
        quarantined: None,
    }
}

/// `partitions/{partition}` at generation 1, epoch 1, naming `owner`.
fn spine_record(partition: PartitionId, owner: NodeId) -> (ControlKey, Bytes) {
    use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
    let record = PartitionRecord {
        partition,
        owner,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        lifecycle: PartitionLifecycle::Serving,
    };
    (ControlKey::Partition(partition), record.encode())
}

/// What F1 on node 1 is handed for partition 1: placement's plan over `rf3_config`.
fn spine_recovery_plan() -> rdb_core::contracts::recovery::RecoveryPlan {
    use rdb_core::contracts::authority::{AuthorityView, DenyReason};
    use rdb_core::contracts::ids::{AuthorityGeneration, GrantId};
    use rdb_core::contracts::recovery::{Candidate, RecoveryPlan};
    let config = support::rf3_config();
    let candidates = config
        .members
        .iter()
        .map(|member| Candidate {
            copy: member.copy,
            primary_eligible: true,
            healthy: true,
            within_capacity: true,
            has_valid_grant: true,
        })
        .collect();
    RecoveryPlan {
        anchor: spine_anchor(),
        config,
        candidates,
        rebuild_required: [CopyId(0), CopyId(1), CopyId(2)].into_iter().collect(),
        authority_view: AuthorityView {
            lineage: spine_prior(),
            grant_id: GrantId(1),
            boot_id: BOOT,
            authority_generation: AuthorityGeneration(1),
            config_version: ConfigVersion(1),
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: 1_000,
    }
}

/// The fence the prior owner (node 4) lost, read at the partition record's revision.
fn spine_fence(control_revision: u64) -> rdb_core::contracts::authority::FencingProof {
    use rdb_core::contracts::authority::{FencingProof, Revocation};
    use rdb_core::contracts::ids::{GrantId, Revision};
    FencingProof {
        partition: PartitionId(1),
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(1),
        prior_grant_id: GrantId(1),
        prior_boot_id: BOOT,
        revocation: Revocation::DurableDrain {
            ack_revision: Revision(control_revision),
        },
        control_revision: Revision(control_revision),
        decision_tick: Tick(1),
    }
}

fn spine_seed(at: u64, node: NodeId, partition: PartitionId, kind: EventKind) -> SeedEvent {
    SeedEvent {
        at: Tick(at),
        node,
        boot: BOOT,
        partition,
        correlation: CorrelationId(at),
        kind,
    }
}

/// The spine scenario (spike §6, F1/R1, as far as the landed kernels allow).
///
/// Four nodes. Node 1's A1 acquires its grant and serves partition 2 from the control plane.
/// Partition 1's prior owner (node 4) is gone: its survivors on nodes 1, 2 and 3 hold the same
/// two records of lineage (1, 1), preloaded into each node's engine and placed for F1's
/// providers. F1 on node 1 is given placement's plan and the fence, queries the survivors,
/// selects, syncs each copy's WAL through the cutoff, and commits the new root by CAS.
fn spine_plan() -> RunPlan {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::RecoveryEvent;
    let mut plan = RunPlan::new(support::cluster());
    plan.provenance = Provenance::Authored {
        case: String::from("spine-f1-r1"),
    };
    // Partition 1 is revision 1, partition 2 revision 2.
    plan.control_records = vec![
        spine_record(PartitionId(1), NodeId(4)),
        spine_record(SERVED, NODE),
    ];
    for node in 1..=3 {
        for batch in spine_history().batches {
            plan.preloads.push((NodeId(node), batch));
        }
        plan.survivors.push((
            NodeId(node),
            PartitionId(1),
            spine_survivor(u8::try_from(node - 1).expect("small")),
        ));
    }
    plan.seed = vec![
        spine_seed(
            1,
            NODE,
            SERVED,
            EventKind::Timer(rdb_core::contracts::time::TimerFired {
                id: rdb_core::authority::AuthorityTimer::Acquire.id(),
                version: TimerVersion(0),
                scheduled_at: Tick(1),
            }),
        ),
        spine_seed(
            2,
            NODE,
            PartitionId(1),
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(Box::new(
                spine_recovery_plan(),
            )))),
        ),
        spine_seed(
            3,
            NODE,
            PartitionId(1),
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(Box::new(
                spine_fence(1),
            )))),
        ),
    ];
    plan.limits = RunLimits {
        max_events: 400,
        deadline: Tick(3_000),
    };
    plan
}

/// B-R56 with B-R57's provider and B-R58's R1: the spine with the members' fan-out on. Every
/// other member (two secondaries and the shadow) hears `Recovered` and builds its receiver
/// (B-R54). The two secondaries are in the barrier, so they start at the cutoff and ACK it at
/// once. The shadow is not, so it starts at the root and asks node 1 for
/// `NeedPrefix { have: 0, ROOT }`. Node 1's primary holds the anchor's rung (B-R58), so it vouches
/// for the root and sends records `1..=2` through the `SendEnvelopes` provider, and the shadow
/// takes them: the historical records it receives were sealed under the prior generation and
/// pass R1's historical rule. The run goes on to its deadline.
///
/// The fan-out is the **default** (B-R58b): `spine_plan` is `RunPlan::new` and nothing here sets
/// `member_watches`. With the default off, no member hears `Recovered`, no receiver is built, and
/// every head below is `None`.
///
/// Until 2026-09-27 this row pinned the opposite (the run stopped at the shadow's
/// `SnapshotCatchupRequired`), and it said it would turn red when R1 could vouch for the root. It
/// did, in the B-R58 export.
#[retcd_test]
fn fanout_the_spine_catches_the_shadow_up_from_the_root_by_default() {
    use rdb_sim::harness::run::StopReason;
    support::preamble();
    let plan = spine_plan();
    assert!(plan.member_watches, "the fan-out is on by default");
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the spine runs");
    tracing::info!(stop = ?report.stop, "spine with member watches");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );
    let history = spine_history();
    let heads: Vec<_> = (2..=4)
        .map(|node| {
            runner
                .dispatcher()
                .replication()
                .receiver(NodeId(node), PartitionId(1))
                .map(|receiver| receiver.applied_head())
                .map(|head| (head.seq.0, head.digest))
        })
        .collect();
    let at_head = Some((SPINE_HEAD, history.digest(SPINE_HEAD)));
    assert_eq!(
        heads,
        vec![at_head; 3],
        "the secondaries start at the cutoff; the shadow reaches it from the root"
    );
    let shadow = runner
        .dispatcher()
        .engine(NodeId(4))
        .expect("the shadow's engine");
    for (seq, batch) in (1..).map(Seq).zip(&history.batches) {
        let stored = shadow
            .history_at(PartitionId(1), Generation(2), seq)
            .map(|(record, _)| record);
        let sent = rdb_sim::storage::history::history_writes(batch, seq).map(|(record, _)| record);
        assert_eq!(
            stored, sent,
            "the shadow holds node 1's record {} unchanged",
            seq.0
        );
    }
    let trace = runner.finish().expect("a trace");
    let landed: Vec<NodeId> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredLanded { member, .. },
                ..
            } => Some(*member),
            _ => None,
        })
        .collect();
    assert_eq!(
        landed,
        vec![NodeId(2), NodeId(3), NodeId(4)],
        "both secondaries and the shadow heard it: every other member of the pin"
    );
}

/// The spine with ladders that cannot decide. The shadow (copy 3, node 4) also survived, lagging:
/// its engine holds record 1 and it reports head 1. Copy 2 reports head 2 with no rung at 1.
/// Selection cannot compare the two at 1, so F1 probes copy 2 there. The lagging copy is the
/// shadow because a shadow is outside the barrier: the commit does not wait on its catch-up.
fn sparse_ladder_plan() -> RunPlan {
    let mut plan = spine_plan();
    let rungs = |seqs: &[u64]| {
        seqs.iter()
            .map(|seq| (Seq(*seq), spine_digest(*seq)))
            .collect::<Vec<_>>()
    };
    for (_, _, inventory) in &mut plan.survivors {
        if inventory.copy == CopyId(2) {
            inventory.ladder = rungs(&[0, 2]);
        }
    }
    let shadow = NodeId(4);
    let first = spine_history()
        .batches
        .into_iter()
        .next()
        .expect("record 1");
    plan.preloads.push((shadow, first));
    plan.survivors.push((
        shadow,
        PartitionId(1),
        SurvivorInventory {
            head: (Seq(1), spine_digest(1)),
            ladder: rungs(&[0, 1]),
            ..spine_survivor(3)
        },
    ));
    plan
}

/// Scenario (team i1, 2026-10-02): survivors report ladders too sparse to compare, so F1 asks
/// the environment for one digest, and recovery goes on to commit.
///
/// The shadow lags at head 1; copy 2 is at head 2 but its ladder has no rung at 1. F1 probes
/// copy 2 at 1, and the environment answers from copy 2's holder's engine — the digest it stores, never
/// one the harness made up. F1 learns the rung, finds the pair compatible, selects head 2, and
/// commits. Copy 2 is never recorded as lost.
///
/// Red before the probe had a provider: the run stopped `Refused` at
/// `harness::dispatch::deliver::recovery`, with F1 still collecting.
#[retcd_test]
fn probe_a_sparse_ladder_is_answered_from_the_holders_engine_and_recovery_commits() {
    use rdb_sim::harness::run::StopReason;
    support::preamble();
    let plan = sparse_ladder_plan();
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner
        .run(plan.limits)
        .expect("the run itself does not fail");
    let phase = runner
        .dispatcher()
        .recovery(NODE, PartitionId(1))
        .map(rdb_core::recovery::Recovery::phase);
    let facts: Vec<RecoveryEffect> = runner
        .recorded()
        .iter()
        .filter_map(|record| match &record.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveryFact { effect },
                ..
            } => Some(effect.clone()),
            _ => None,
        })
        .collect();
    tracing::info!(stop = ?report.stop, ?phase, ?facts, "sparse ladder");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "the probe is answered, not refused: {:?}",
        report.stop
    );
    let selected: Vec<_> = facts
        .iter()
        .filter_map(|effect| match effect {
            RecoveryEffect::Selected(selected) => {
                Some((selected.cutoff_seq, selected.cutoff_digest))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        selected,
        vec![(Seq(SPINE_HEAD), spine_digest(SPINE_HEAD))],
        "the learned rung proved the pair compatible, so the longest prefix won"
    );
    assert!(
        !facts.iter().any(|effect| matches!(
            effect,
            RecoveryEffect::RecordSourceUnavailable {
                copy: CopyId(2),
                ..
            } | RecoveryEffect::Quarantine(_)
        )),
        "copy 2 was answered, not lost, and nothing diverged: {facts:?}"
    );
    assert_eq!(
        phase,
        Some(rdb_core::recovery::RecoveryPhase::Committed),
        "and F1 committed"
    );
}

/// What one spine run left behind.
struct SpineRun {
    trace: Trace,
    report: rdb_sim::harness::run::RunReport,
    /// F1's phase on node 1 for partition 1 when the loop stopped.
    phase: Option<rdb_core::recovery::RecoveryPhase>,
    /// Each survivor node's engine watermark for partition 1 at the prior generation.
    durable: Vec<u64>,
    /// The prefix node 1's engine inherited into generation 2 when F1 recovered.
    inherited: Seq,
    /// The primary R1 built on node 1 from `Recovered` (B-R54): its generation and head.
    primary: Option<(Generation, Seq)>,
}

/// One spine run: the plan, then the loop to its deadline. Nothing is installed by hand: R1
/// builds node 1's primary from F1's `Recovered` (lead ruling B-R54).
fn run_spine() -> SpineRun {
    let plan = spine_plan();
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the spine runs");
    let dispatcher = runner.dispatcher();
    let phase = dispatcher
        .recovery(NODE, PartitionId(1))
        .map(rdb_core::recovery::Recovery::phase);
    let durable = (1..=3)
        .map(|node| {
            dispatcher
                .engine(NodeId(node))
                .map_or(0, |engine| engine.durable(PartitionId(1), Generation(1)).0)
        })
        .collect();
    let inherited = dispatcher
        .engine(NODE)
        .map_or(Seq(0), |engine| engine.base(PartitionId(1), Generation(2)));
    let primary = dispatcher
        .replication()
        .primary(NODE, PartitionId(1))
        .map(|primary| {
            (
                primary.tracker().lineage().generation,
                primary.tracker().head(),
            )
        });
    let trace = runner.finish().expect("a trace");
    SpineRun {
        trace,
        report,
        phase,
        durable,
        inherited,
        primary,
    }
}

/// The spine: A1, F1, R1 and L1 in one four-node run that stops at its deadline, not at a
/// refusal, and the same plan run twice writes the same bytes.
///
/// Scaffolding, not a row: it asserts that the paths are joined up, and no plan row's claim.
/// What it does **not** reach, each owed and named in the handoff: A1's takeover emitting the
/// fence (the fence is seeded), and F1's `CatchUp` and `ProbeDigestAt` (every survivor holds the
/// cutoff, so neither is asked). The members' fan-out is on by default (B-R58b): every other
/// member of the pin lands `Recovered` and builds its receiver, and the shadow is caught up from
/// the root (`fanout_the_spine_catches_the_shadow_up_from_the_root_by_default`).
#[retcd_test]
fn spine_a1_f1_r1_l1_run_to_completion_and_replay_byte_identically() {
    use rdb_core::contracts::trace::ControlOpKind;
    use rdb_sim::harness::run::StopReason;
    use rdb_sim::harness::trace::validate;
    support::preamble();
    let run = run_spine();
    let trace = &run.trace;
    tracing::info!(stop = ?run.report.stop, events = run.report.events_consumed,
        phase = ?run.phase, durable = ?run.durable, "spine stop");

    assert!(
        matches!(run.report.stop, StopReason::DeadlineReached { .. }),
        "the spine runs to its deadline: {:?}",
        run.report.stop
    );
    validate(trace).expect("the spine's trace is well formed");

    // The scenario's placements are in the trace, one per survivor node.
    let placed: Vec<NodeId> = trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::KernelNoted {
                    note: KernelNote::SurvivorPlaced { .. },
                    ..
                }
            )
        })
        .map(|event| event.node)
        .collect();
    assert_eq!(placed, vec![NodeId(1), NodeId(2), NodeId(3)]);

    // F1: every survivor proved durable on its own node, no proof withheld, one CAS, committed.
    assert_eq!(
        run.phase,
        Some(rdb_core::recovery::RecoveryPhase::Committed)
    );
    assert_eq!(run.durable, vec![SPINE_HEAD; 3], "synced on each holder");
    assert_eq!(
        run.inherited,
        Seq(SPINE_HEAD),
        "the new generation starts at the cutoff on the recovering node"
    );
    // B-R54: R1 built the primary itself. A `NotRequired` answer to `Recovered` would pass every
    // dispatch assertion below without one, so its existence and position are asserted here.
    assert_eq!(
        run.primary,
        Some((Generation(2), Seq(SPINE_HEAD))),
        "R1 leads the new generation from the cutoff on node 1"
    );
    assert!(
        !trace.events.iter().any(|event| matches!(
            event.kind,
            TraceKind::KernelNoted {
                note: KernelNote::SyncWithheld { .. },
                ..
            }
        )),
        "no proof was withheld"
    );
    let proven = trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::KernelNoted {
                    note: KernelNote::SyncProven { .. },
                    ..
                }
            )
        })
        .count();
    assert_eq!(proven, 3, "one proof per survivor copy (B-R55a)");
    let recovery_cas: Vec<&TraceKind> = trace
        .events
        .iter()
        .map(|event| &event.kind)
        .filter(|kind| {
            matches!(
                kind,
                TraceKind::ControlInteraction {
                    op: ControlOpKind::Cas,
                    key: Some(ControlKey::Partition(PartitionId(1))),
                    ..
                }
            )
        })
        .collect();
    assert_eq!(recovery_cas.len(), 1, "{recovery_cas:?}");

    // `Recovered` reached R1 and then L1 on node 1, and both answered it.
    let dispatches: Vec<(EventId, ModuleName, DispatchOutcome)> = trace
        .events
        .iter()
        .filter(|event| event.partition == PartitionId(1) && event.node == NODE)
        .filter_map(|event| match &event.kind {
            TraceKind::ModuleDispatch {
                event,
                module,
                outcome,
            } => Some((*event, *module, *outcome)),
            _ => None,
        })
        .collect();
    let recovered = dispatches
        .iter()
        .find(|(_, module, outcome)| {
            *module == ModuleName::Replication
                && matches!(outcome, DispatchOutcome::Answered { .. })
        })
        .map(|(event, _, _)| *event)
        .expect("R1 answered F1's result");
    let offered: Vec<(ModuleName, bool)> = dispatches
        .iter()
        .filter(|(event, _, _)| *event == recovered)
        .map(|(_, module, outcome)| (*module, matches!(outcome, DispatchOutcome::Answered { .. })))
        .collect();
    assert_eq!(
        offered[..2],
        [
            (ModuleName::Replication, true),
            (ModuleName::Protection, true)
        ],
        "{offered:?}"
    );

    // B-R55b with B-R56: one `RecoveredFact` per `Recovered` F1 emits, and one `RecoveredLanded`
    // per member it reaches. A routed `Recovered` is the only event offered to R1 and then L1
    // first (`route::consumers`), so its offers count the routings without reading the notes.
    // The members' fan-out is on by default (B-R58b), so node 1's emission is routed once on
    // node 1 and once more on each member that lands it.
    let mut offers: std::collections::BTreeMap<EventId, Vec<ModuleName>> =
        std::collections::BTreeMap::new();
    for event in &trace.events {
        if let TraceKind::ModuleDispatch { event, module, .. } = &event.kind {
            offers.entry(*event).or_default().push(*module);
        }
    }
    let emitted = offers
        .values()
        .filter(|modules| modules.starts_with(&[ModuleName::Replication, ModuleName::Protection]))
        .count();
    let facts = trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::KernelNoted {
                    note: KernelNote::RecoveredFact { .. },
                    ..
                }
            )
        })
        .count();
    let landed: Vec<NodeId> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredLanded { member, .. },
                ..
            } => Some(*member),
            _ => None,
        })
        .collect();
    assert_eq!(facts, 1, "F1 emits one Recovered");
    assert_eq!(
        landed,
        vec![NodeId(2), NodeId(3), NodeId(4)],
        "every other member of the pin lands it"
    );
    assert_eq!(
        emitted,
        facts + landed.len(),
        "one routing per emission and per landing"
    );

    // A1 served partition 2 on node 1, and R1 answered its view there with nothing installed
    // for that partition (B-R53).
    assert!(trace.events.iter().any(|event| {
        event.partition == SERVED
            && matches!(
                event.kind,
                TraceKind::ModuleDispatch {
                    module: ModuleName::Replication,
                    outcome: DispatchOutcome::Answered { .. },
                    ..
                }
            )
    }));

    // Byte-identical, run twice. `.trace`, not `.jsonl`: the log relation's glob must not read
    // it (AGENTS.md, "the glob also catches JSONL that is not a log").
    let dir = test_log_dir().join("spine");
    std::fs::create_dir_all(&dir).expect("a fixture directory");
    let again = run_spine();
    let (first, second) = (dir.join("first.trace"), dir.join("second.trace"));
    write_jsonl(trace, &first).expect("write the first");
    write_jsonl(&again.trace, &second).expect("write the second");
    let (first, second) = (
        std::fs::read(first).expect("read the first"),
        std::fs::read(second).expect("read the second"),
    );
    tracing::info!(
        bytes = first.len(),
        events = trace.events.len(),
        "spine trace"
    );
    assert!(!first.is_empty());
    assert!(first == second, "the same plan wrote different bytes");
}

/// dev-t1's report, 2026-09-26: a recovery's new generation begins at the cutoff. The engine
/// inherits the predecessor's prefix through it as applied — so T1's step view reaches
/// `retained_through` — and never as durable. A process crash keeps the base; a host crash keeps
/// only what the predecessor itself had synced, since those are the bytes the base is made of.
#[retcd_test]
fn store_an_inherited_prefix_is_applied_never_durable_and_survives_only_what_was_synced() {
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq};
    use rdb_core::contracts::storage::CapturedPrefix;
    use rdb_sim::storage::crash_image::CrashImage;
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    let (old, new, part) = (Generation(1), Generation(2), PartitionId(1));
    let mut engine = MemoryEngine::new(NODE);
    for seq in 1..=3 {
        engine
            .commit(support::batch(1, seq, b"k", b"v"))
            .expect("a commit");
    }

    let mut short = engine.clone();
    short.inherit(part, old, new, Seq(5)).expect("inherit");
    assert_eq!(
        short.base(part, new),
        Seq(3),
        "an engine never inherits a prefix it lacks"
    );
    assert_eq!(
        engine.clone().inherit(part, new, old, Seq(1)),
        Err(SimError::Config { field: "inherit" }),
        "only from an earlier generation"
    );

    engine.inherit(part, old, new, Seq(2)).expect("inherit");
    assert_eq!(engine.base(part, new), Seq(2));
    assert_eq!(engine.parent(part, new), Some(old));
    assert_eq!(engine.buffered_applied(part, new), AppliedSeq(2));
    assert_eq!(
        engine.durable(part, new),
        DurableSeq(0),
        "inherited, not synced"
    );

    let process = CrashImage::of(&engine, StorageFault::ProcessCrash)
        .expect("an image")
        .reopen(NODE);
    assert_eq!(process.base(part, new), Seq(2));
    assert_eq!(process.parent(part, new), Some(old));
    assert_eq!(process.buffered_applied(part, new), AppliedSeq(2));
    let host = CrashImage::of(&engine, StorageFault::HostCrash)
        .expect("an image")
        .reopen(NODE);
    assert_eq!(
        host.base(part, new),
        Seq(0),
        "the predecessor never synced the base"
    );

    engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: part,
            generation: old,
            through: AppliedSeq(2),
        }])
        .expect("a sync");
    let host = CrashImage::of(&engine, StorageFault::HostCrash)
        .expect("an image")
        .reopen(NODE);
    assert_eq!(host.base(part, new), Seq(2), "synced, so it survives");
}

/// Lead ruling, 2026-09-26 (MATERIAL): a new generation's view shows the state as of the cutoff
/// and nothing the predecessor wrote above it. The predecessor writes `k` at the cutoff and
/// again at cutoff + 1, and `gone` only above it; after recovery the new generation reads the
/// cutoff's `k` and no `gone`, while a view of the old generation taken before still reads both.
#[retcd_test]
fn store_a_new_generations_view_hides_the_discarded_suffix() {
    use rdb_core::contracts::ids::{BatchId, SnapshotHandle};
    use rdb_core::contracts::storage::{Batch, Namespace, SnapshotRead, Write};
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    let (old, new, part) = (Generation(1), Generation(2), PartitionId(1));
    let write = |key: &'static [u8], value: Option<&'static [u8]>| Write {
        ns: Namespace::User,
        key: Bytes::from_static(key),
        value: value.map(Bytes::from_static),
    };
    let batch = |generation: Generation, seq: u64, writes: Vec<Write>| Batch {
        id: BatchId(seq),
        partition: part,
        generation,
        seq: Seq(seq),
        writes,
    };
    let mut engine = MemoryEngine::new(NODE);
    engine
        .commit(batch(old, 1, vec![write(b"k", Some(b"at-cutoff"))]))
        .expect("seq 1");
    engine
        .commit(batch(
            old,
            2,
            vec![write(b"k", Some(b"discarded")), write(b"gone", Some(b"x"))],
        ))
        .expect("seq 2");
    let before = engine.snapshot(part, old, SnapshotHandle(1));

    engine.inherit(part, old, new, Seq(1)).expect("cut at 1");
    let view = engine.snapshot(part, new, SnapshotHandle(2));
    assert_eq!(view.at(), Seq(1));
    assert_eq!(
        view.get(Namespace::User, b"k"),
        Some(Bytes::from_static(b"at-cutoff"))
    );
    assert_eq!(view.version(Namespace::User, b"k"), Some(1));
    assert_eq!(
        view.get(Namespace::User, b"gone"),
        None,
        "written only above the cutoff"
    );

    // The new generation's own writes land on top of the inherited state.
    engine
        .commit(batch(
            new,
            2,
            vec![write(b"k", None), write(b"n", Some(b"new"))],
        ))
        .expect("the new generation's first batch");
    let view = engine.snapshot(part, new, SnapshotHandle(3));
    assert_eq!(view.at(), Seq(2));
    assert_eq!(view.get(Namespace::User, b"k"), None, "deleted in g2");
    assert_eq!(view.get(Namespace::User, b"gone"), None);
    assert_eq!(
        view.get(Namespace::User, b"n"),
        Some(Bytes::from_static(b"new"))
    );

    assert_eq!(
        before.get(Namespace::User, b"k"),
        Some(Bytes::from_static(b"discarded")),
        "the old view is owned and unchanged"
    );
    assert_eq!(
        before.get(Namespace::User, b"gone"),
        Some(Bytes::from_static(b"x"))
    );
}

/// Lead ruling B-R55a (M7B-104): a durable watermark set at preload spends no planned fault.
/// `PEER` holds 50 applied and 45 durable; the `FalseDurable` planned after that is consumed by
/// F1's own sync, which therefore proves nothing and says the engine still stands at 45.
#[retcd_test]
fn preload_durable_leaves_a_planned_false_durable_for_f1s_sync() {
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq};
    use rdb_core::contracts::trace::SyncWithheldReason;
    support::preamble();
    let (part, gen) = (PartitionId(1), Generation(1));
    let false_durable = StorageOp::FalseDurable {
        node: PEER,
        through: AppliedSeq(50),
    };

    // Through a run plan: preloads, then the durable mark, then the planned fault.
    let mut plan = RunPlan::new(support::cluster());
    plan.preloads = (1..=50)
        .map(|seq| (PEER, support::batch(1, seq, b"k", b"v")))
        .collect();
    plan.preload_durable = vec![(PEER, part, gen, DurableSeq(45))];
    plan.storage_ops = vec![false_durable];
    plan.survivors = vec![(PEER, part, survivor(1, 50))];
    let mut runner = Runner::new(&plan).expect("the plan applies in order");
    let engine = runner.dispatcher().engine(PEER).expect("the holder");
    assert_eq!(engine.buffered_applied(part, gen), AppliedSeq(50));
    assert_eq!(engine.durable(part, gen), DurableSeq(45));
    assert!(engine.has_planned(), "the fault is still waiting");

    runner
        .carry_out(
            NODE,
            BOOT,
            vec![effect_from(
                ModuleName::Recovery,
                EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::SyncWalThrough {
                    copy: CopyId(1),
                    cutoff: Seq(50),
                })),
            )],
        )
        .expect("F1's sync");
    let engine = runner.dispatcher().engine(PEER).expect("the holder");
    assert_eq!(
        engine.false_claims(),
        &[AppliedSeq(50)],
        "F1's sync is the call that met the fault"
    );
    assert!(!engine.has_planned());
    assert_eq!(engine.durable(part, gen), DurableSeq(45), "nothing moved");
    let notes: Vec<KernelNote> = runner
        .dispatcher_mut()
        .take_notes()
        .into_iter()
        .map(|(_, _, note)| note)
        .filter(|note| {
            matches!(
                note,
                KernelNote::SyncWithheld { .. } | KernelNote::SyncProven { .. }
            )
        })
        .collect();
    assert_eq!(
        notes,
        vec![KernelNote::SyncWithheld {
            copy: CopyId(1),
            cutoff: Seq(50),
            reason: SyncWithheldReason::Short {
                durable: DurableSeq(45)
            },
        }]
    );

    // Directly: a durable preload refuses to run while a fault is planned, because it would
    // spend it.
    let mut dispatcher = two_nodes();
    dispatcher
        .preload(PEER, support::batch(1, 1, b"k", b"v"))
        .expect("a preload");
    dispatcher
        .inject_storage(false_durable)
        .expect("a planned fault");
    assert_eq!(
        dispatcher.preload_durable(PEER, part, gen, DurableSeq(1)),
        Err(SimError::Config {
            field: "preload_durable"
        })
    );
    assert!(dispatcher.engine(PEER).expect("the holder").has_planned());
    // And it cannot declare durable what was never applied.
    let mut dispatcher = two_nodes();
    dispatcher
        .preload(PEER, support::batch(1, 1, b"k", b"v"))
        .expect("a preload");
    assert_eq!(
        dispatcher.preload_durable(PEER, part, gen, DurableSeq(2)),
        Err(SimError::Config {
            field: "preload_durable"
        })
    );
}

/// Lead ruling B-R55 (M7B-96): a declared transfer is started by F1's own `QueryInventory`, and
/// every `TransferProgress` after the first comes from the transfer's own steps — none is seeded.
/// C advertises 150 from 100, moves 10 a step, and stops at tick 3000; A's copy reports as placed.
#[retcd_test]
fn provider_a_transfer_reports_progress_per_step_and_goes_silent_when_it_stops() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::RecoveryEvent;
    use rdb_sim::harness::transfer::TransferPlan;
    support::preamble();
    let plan = TransferPlan {
        copy: CopyId(2),
        holder: PEER,
        advertised: Seq(150),
        from: Seq(100),
        per_step: 10,
        step_millis: 1_000,
        stop_at: Some(Tick(3_000)),
        stall_at: None,
    };
    let progress = |received: u64| {
        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::TransferProgress {
            copy: CopyId(2),
            advertised_seq: Seq(150),
            received_seq: Seq(received),
        }))
    };
    let mut dispatcher = with_survivor();
    dispatcher.plan_transfer(PartitionId(1), plan);
    let mut scheduler = Scheduler::new();
    let query = effect_from(
        ModuleName::Recovery,
        EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::QueryInventory {
            copies: vec![CopyId(1), CopyId(2)],
        })),
    );
    deliver_on(&mut dispatcher, &mut scheduler, NODE, query).expect("a query");

    let mut seen: Vec<(u64, EventKind)> = Vec::new();
    let mut drain = |dispatcher: &mut Dispatcher, scheduler: &mut Scheduler| {
        while let Some(event) = scheduler.pop() {
            assert_eq!((event.node, event.partition), (NODE, PartitionId(1)));
            assert!(dispatcher.take_routed(event.id), "held to F1's answer");
            seen.push((event.at.0, event.kind));
        }
    };
    drain(&mut dispatcher, &mut scheduler);
    while let Some(at) = dispatcher.next_deadline() {
        dispatcher
            .fire_due_timers(at, &mut scheduler)
            .expect("a step");
        drain(&mut dispatcher, &mut scheduler);
    }

    assert_eq!(
        seen,
        vec![
            (
                0,
                EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::InventoryReported(
                    Box::new(survivor(1, 2))
                )))
            ),
            (0, progress(100)),
            (1_000, progress(110)),
            (2_000, progress(120)),
        ],
        "advertised at the query, one step a second, silent from 3000 on"
    );
}

/// B-R55: a transfer that reaches its advertised head answers the copy's inventory as a query
/// would, and a transfer whose holder is down moves nothing.
#[retcd_test]
fn provider_a_completed_transfer_answers_the_inventory_and_a_downed_holder_stalls_it() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::RecoveryEvent;
    use rdb_sim::harness::transfer::TransferPlan;
    support::preamble();
    let plan = TransferPlan {
        copy: CopyId(1),
        holder: PEER,
        advertised: Seq(2),
        from: Seq(1),
        per_step: 5,
        step_millis: 500,
        stop_at: None,
        stall_at: None,
    };
    let query = || {
        effect_from(
            ModuleName::Recovery,
            EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::QueryInventory {
                copies: vec![CopyId(1)],
            })),
        )
    };
    let progress = |received: u64| {
        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::TransferProgress {
            copy: CopyId(1),
            advertised_seq: Seq(2),
            received_seq: Seq(received),
        }))
    };

    let mut dispatcher = with_survivor();
    dispatcher.plan_transfer(PartitionId(1), plan);
    let mut scheduler = Scheduler::new();
    deliver_on(&mut dispatcher, &mut scheduler, NODE, query()).expect("a query");
    assert_eq!(scheduler.pop().map(|event| event.kind), Some(progress(1)));
    dispatcher
        .fire_due_timers(Tick(500), &mut scheduler)
        .expect("a step");
    let kinds: Vec<EventKind> = std::iter::from_fn(|| scheduler.pop())
        .map(|event| event.kind)
        .collect();
    assert_eq!(
        kinds,
        vec![
            progress(2),
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::InventoryReported(
                Box::new(survivor(1, 2))
            ))),
        ],
        "capped at the advertised head, then the placed inventory"
    );
    assert_eq!(dispatcher.next_deadline(), None, "nothing left to step");

    // The holder crashes between the advertisement and the first step.
    let mut dispatcher = with_survivor();
    dispatcher.plan_transfer(PartitionId(1), plan);
    let mut scheduler = Scheduler::new();
    deliver_on(&mut dispatcher, &mut scheduler, NODE, query()).expect("a query");
    assert_eq!(scheduler.pop().map(|event| event.kind), Some(progress(1)));
    dispatcher
        .inject_storage(StorageOp::Crash {
            node: PEER,
            fault: StorageFault::ProcessCrash,
        })
        .expect("a planned crash");
    let touch = store(StoreEffect::Commit(support::batch(1, 3, b"k", b"v")));
    deliver_on(&mut dispatcher, &mut scheduler, PEER, touch)
        .expect("the crash is taken, a fault and not a refusal");
    assert!(dispatcher.is_down(PEER), "the holder is down");
    dispatcher
        .fire_due_timers(Tick(500), &mut scheduler)
        .expect("a step");
    assert_eq!(scheduler.queued(), 0, "a downed holder sends nothing");
    assert_eq!(dispatcher.next_deadline(), None);
}

// ------------------------------------------------------------------------------------------
// `SendEnvelopes` (lead ruling B-R57): R1 names a range, the harness reads it from the primary's
// own engine and sends the stored bytes unchanged.
// ------------------------------------------------------------------------------------------

/// Partition 1's lineage (1, 1), the one the send rows serve.
fn send_lineage() -> rdb_core::contracts::authority::Lineage {
    rdb_core::contracts::authority::Lineage {
        partition: PartitionId(1),
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

/// Records `1..=n` of [`send_lineage`], as T1 commits them.
fn send_history(n: u64) -> rdb_sim::storage::history::CanonicalHistory {
    rdb_sim::storage::history::canonical_history(send_lineage(), ConfigVersion(1), n)
        .expect("a canonical history")
}

/// Records `cutoff + 1..=n` of [`send_lineage`] at `generation`, chained from `prior`'s digest
/// at `cutoff`: what a live T1 writes after inheriting `prior` there (B-R57b).
fn send_history_after(
    generation: u64,
    prior: &rdb_sim::storage::history::CanonicalHistory,
    cutoff: u64,
    n: u64,
) -> rdb_sim::storage::history::CanonicalHistory {
    rdb_sim::storage::history::canonical_history_from(
        rdb_core::contracts::authority::Lineage {
            generation: Generation(generation),
            ..send_lineage()
        },
        ConfigVersion(1),
        (Seq(cutoff), prior.digest(cutoff)),
        n,
    )
    .expect("a canonical history")
}

/// A runner with node 1 leading partition 1 at `head` of [`send_history`], its engine holding
/// `stored` (the batches it committed), and node 2 a receiver at the root. Seeded with node 2's
/// `NeedPrefix { have: 0 }` arriving at node 1, which starts copy 1's cursor.
fn send_runner(head: u64, stored: Vec<rdb_core::contracts::storage::Batch>) -> Runner {
    send_runner_on(
        head,
        stored.into_iter().map(|batch| (NODE, batch)).collect(),
        None,
    )
}

/// [`send_runner`], with each engine holding its `preloads`, and node 1's durable through
/// `durable` when given.
fn send_runner_on(
    head: u64,
    preloads: Vec<(NodeId, rdb_core::contracts::storage::Batch)>,
    durable: Option<u64>,
) -> Runner {
    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::envelope::{AppendOutcome, AppendReject, ReplicaProgress};
    use rdb_core::contracts::event::EventKind;
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, ReceivedSeq};
    use rdb_core::contracts::transport::TransportEvent;
    use rdb_core::contracts::version::ENVELOPE_VERSION;
    use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
    use rdb_core::replication::progress::{DigestLadder, ProgressTracker, TrackerInit};
    use rdb_core::replication::wire::encode_reply;

    let history = send_history(head);
    let mut plan = RunPlan::new(support::cluster());
    plan.preloads = preloads;
    if let Some(through) = durable {
        plan.preload_durable = vec![(NODE, PartitionId(1), Generation(1), DurableSeq(through))];
    }
    plan.seed = vec![SeedEvent {
        at: Tick(1),
        node: NODE,
        boot: BOOT,
        partition: PartitionId(1),
        correlation: CorrelationId(1),
        kind: EventKind::Transport(TransportEvent::Delivered {
            from: PeerLabel {
                node: NodeId(2),
                boot: BOOT,
                authenticated: true,
            },
            frame: Frame {
                id: MessageId(0),
                protocol: ENVELOPE_VERSION,
                config: ConfigVersion(1),
                // A reply carries the receiver's lineage: node 2 is at the sender's.
                sender: send_lineage(),
                body: encode_reply(&AppendOutcome::Rejected(AppendReject::NeedPrefix {
                    have: Seq(0),
                    head_digest: Digest::ROOT,
                })),
            },
        }),
    }];
    let mut runner = Runner::new(&plan).expect("a runner");
    let mut ladder = DigestLadder::new();
    for seq in 0..=head {
        ladder.insert(Seq(seq), history.digest(seq));
    }
    let tracker = ProgressTracker::new(TrackerInit {
        config: support::rf3_config(),
        own: CopyId(0),
        lineage: send_lineage(),
        history: ladder,
        local: ReplicaProgress {
            received: ReceivedSeq(head),
            buffered_applied: AppliedSeq(head),
            durable: DurableSeq(head),
        },
    })
    .expect("a primary at the head");
    let receiver = AppendReceiver::new(ReceiverInit {
        config: support::rf3_config(),
        own: CopyId(1),
        lineage: send_lineage(),
        head: Head {
            seq: Seq(0),
            digest: Digest::ROOT,
        },
        durable: DurableSeq(0),
    })
    .expect("a receiver at the root");
    let replication = runner.dispatcher_mut().replication_mut();
    replication.install_primary(tracker);
    replication.install_receiver(receiver);
    runner
}

/// The `History` bytes `node`'s engine shows for partition 1 at generation 1, `1..=n`.
fn stored_records(runner: &Runner, node: NodeId, n: u64) -> Vec<Option<Bytes>> {
    let engine = runner.dispatcher().engine(node).expect("an engine");
    (1..=n)
        .map(|seq| {
            engine
                .history_at(PartitionId(1), Generation(1), Seq(seq))
                .map(|(record, _)| record)
        })
        .collect()
}

/// A copy at the root is caught up to the primary's head by `SendEnvelopes` alone: R1 names one
/// record at a time, the harness reads it from node 1's engine and sends it, node 2's receiver
/// stages and applies it, and its ACK moves the cursor to the next. Node 2 ends holding the
/// same bytes node 1 committed, at node 1's head digest.
#[retcd_test]
fn send_envelopes_a_copy_at_the_root_is_caught_up_from_the_primarys_engine() {
    support::preamble();
    let history = send_history(3);
    let mut runner = send_runner(3, history.batches.clone());
    let report = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "send catch-up");

    let head = runner
        .dispatcher()
        .replication()
        .receiver(NodeId(2), PartitionId(1))
        .expect("node 2's receiver")
        .applied_head();
    assert_eq!(
        (head.seq, head.digest),
        (Seq(3), history.digest(3)),
        "node 2 applied through the primary's head, on the primary's history"
    );
    let sent = stored_records(&runner, NodeId(2), 3);
    assert_eq!(
        sent,
        stored_records(&runner, NODE, 3),
        "node 2 holds exactly the bytes node 1 committed"
    );
    assert!(sent.iter().all(Option::is_some), "{sent:?}");
    let acked = runner
        .dispatcher()
        .replication()
        .primary(NODE, PartitionId(1))
        .expect("node 1's primary")
        .tracker()
        .peer(CopyId(1))
        .expect("copy 1")
        .progress
        .buffered_applied;
    assert_eq!(acked.0, 3, "node 2's ACK reached the primary at the head");
}

/// Never fabricated (B-R57 rule 1): R1's ladder vouches for seq 3, but node 1's engine holds
/// only 1..=2. Records 1 and 2 are sent; 3 is not, and nothing stands in for it. The copy
/// stops at 2.
#[retcd_test]
fn send_envelopes_a_record_the_engine_does_not_hold_is_not_sent() {
    support::preamble();
    let mut stored = send_history(3).batches;
    stored.truncate(2);
    let mut runner = send_runner(3, stored);
    let report = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail");
    tracing::info!(stop = ?report.stop, "send with a missing record");

    let head = runner
        .dispatcher()
        .replication()
        .receiver(NodeId(2), PartitionId(1))
        .expect("node 2's receiver")
        .applied_head();
    assert_eq!(head.seq, Seq(2), "the copy took what the engine held");
    assert_eq!(
        stored_records(&runner, NodeId(2), 3)[2],
        None,
        "nothing was sent for the record the engine lacks"
    );
}

/// B-R57 rule 2: a stored record that disagrees with the progress record committed beside it
/// is not sent. Record 2's progress value is rewritten to another digest; 1 is sent, 2 and
/// everything after it are not.
#[retcd_test]
fn send_envelopes_a_record_that_fails_its_check_is_not_sent() {
    use rdb_core::contracts::storage::Namespace;
    support::preamble();
    let mut stored = send_history(3).batches;
    for write in &mut stored[1].writes {
        if write.ns == Namespace::Progress {
            let mut value = write.value.clone().expect("a progress value").to_vec();
            let last = value.len() - 1;
            value[last] ^= 0xff;
            write.value = Some(Bytes::from(value));
        }
    }
    let mut runner = send_runner(3, stored);
    let report = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail");
    tracing::info!(stop = ?report.stop, "send with a record failing its check");

    let head = runner
        .dispatcher()
        .replication()
        .receiver(NodeId(2), PartitionId(1))
        .expect("node 2's receiver")
        .applied_head();
    assert_eq!(head.seq, Seq(1), "only the record that passed was sent");
}

/// The provider's read (B-R57: "in the primary's lineage, through the inherited base"): at or
/// below the base, a new generation shows its predecessor's record; above it, only its own.
/// The predecessor's suffix past the cutoff is invisible from the new generation, though the
/// predecessor still shows it.
#[retcd_test]
fn send_envelopes_the_read_crosses_the_inherited_base_and_not_the_discarded_suffix() {
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    let prior = send_history(3);
    let mut engine = MemoryEngine::new(NODE);
    for batch in prior.batches.clone() {
        engine.commit(batch).expect("a commit");
    }
    engine
        .inherit(PartitionId(1), Generation(1), Generation(2), Seq(2))
        .expect("generation 2 starts at 2");
    let next = send_history_after(2, &prior, 2, 3);
    engine
        .commit(next.batch(3).clone())
        .expect("generation 2's own record 3");

    let record = |generation: u64, seq: u64| {
        engine
            .history_at(PartitionId(1), Generation(generation), Seq(seq))
            .map(|(record, _)| record)
    };
    let written = |history: &rdb_sim::storage::history::CanonicalHistory, seq: u64| {
        rdb_sim::storage::history::history_writes(history.batch(seq), Seq(seq))
            .map(|(record, _)| record)
    };
    assert_eq!(
        record(2, 1),
        written(&prior, 1),
        "below the base: the predecessor's"
    );
    assert_eq!(
        record(2, 2),
        written(&prior, 2),
        "at the base: the predecessor's"
    );
    assert_eq!(
        record(2, 3),
        written(&next, 3),
        "above the base: its own, not the suffix"
    );
    assert_eq!(
        record(1, 3),
        written(&prior, 3),
        "the predecessor still shows its suffix"
    );
    assert_eq!(record(2, 4), None, "nothing past the head");
}

// ------------------------------------------------------------------------------------------
// Placement as data after commit (lead ruling B-R55 item 3, M7B-137): a `SyncWalThrough` F1
// asks while `Rebuilding` is served by the pinned configuration's node for the copy, in the new
// generation, with the digest the holder's own engine stores at the cutoff. The harness reports
// that digest; F1 judges it.
// ------------------------------------------------------------------------------------------

/// The generation the spine's recovery creates.
const REBUILT: Generation = Generation(2);

/// The spine with copy 2 unable to report: F1 commits `DegradedRf2` over copies 0 and 1 and
/// starts rebuilding all three. Nothing of the new generation is installed by hand (B-R58b):
/// the members' fan-out is on by default, so every other member of the pin hears `Recovered`,
/// inherits what it holds of the predecessor through the cutoff (B-R56.4), and builds its
/// receiver. Copy 1 is in the barrier and starts at the cutoff. Copy 2 is not: it asks node 1
/// from the root, R1 catches it up, and R1's own `CopyCaughtUp` pins the rebuild point and
/// makes F1 sync every required copy.
///
/// Node 3, copy 2's holder, starts with `prior` as its predecessor history and nothing else:
/// F1 never inventories it (it is not a survivor), so what it holds reaches F1 only through the
/// holder's own engine at the rebuild's sync.
fn rebuild_plan(prior: Vec<rdb_core::contracts::storage::Batch>) -> RunPlan {
    let mut plan = spine_plan();
    plan.survivors.retain(|(node, _, _)| *node != NodeId(3));
    plan.preloads.retain(|(node, _)| *node != NodeId(3));
    for batch in prior {
        plan.preloads.push((NodeId(3), batch));
    }
    plan.limits = RunLimits {
        max_events: 600,
        deadline: Tick(4_000),
    };
    plan
}

/// What one rebuild run left behind.
struct RebuildRun {
    trace: Trace,
    phase: Option<rdb_core::recovery::RecoveryPhase>,
    /// Copy 2's receiver on node 3 at the end: its applied head, if R1 built one.
    copy_2_head: Option<(u64, rdb_core::contracts::digest::Digest)>,
    /// What node 3's engine shows for the new generation: its inherited base, and the `History`
    /// record at each seq `1..=SPINE_HEAD`.
    copy_2_base: Seq,
    copy_2_records: Vec<Option<Bytes>>,
}

fn run_rebuild(prior: Vec<rdb_core::contracts::storage::Batch>) -> RebuildRun {
    run_rebuild_faulted(prior, None)
}

/// [`run_rebuild`] with one at-rest fault planned on node 3's engine (lead ruling B-R58d).
fn run_rebuild_faulted(
    prior: Vec<rdb_core::contracts::storage::Batch>,
    fault: Option<StorageOp>,
) -> RebuildRun {
    let mut plan = rebuild_plan(prior);
    plan.storage_ops.extend(fault);
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the rebuild runs");
    let phase = runner
        .dispatcher()
        .recovery(NODE, PartitionId(1))
        .map(rdb_core::recovery::Recovery::phase);
    let copy_2_head = runner
        .dispatcher()
        .replication()
        .receiver(NodeId(3), PartitionId(1))
        .map(|receiver| receiver.applied_head())
        .map(|head| (head.seq.0, head.digest));
    let engine = runner
        .dispatcher()
        .engine(NodeId(3))
        .expect("node 3's engine");
    let copy_2_base = engine.base(PartitionId(1), REBUILT);
    tracing::info!(stop = ?report.stop, events = report.events_consumed, ?phase, ?copy_2_head,
        ?copy_2_base, "rebuild stop");
    let copy_2_records = (1..=SPINE_HEAD)
        .map(|seq| {
            engine
                .history_at(PartitionId(1), REBUILT, Seq(seq))
                .map(|(record, _)| record)
        })
        .collect();
    let trace = runner.finish().expect("a trace");
    RebuildRun {
        trace,
        phase,
        copy_2_head,
        copy_2_base,
        copy_2_records,
    }
}

impl RebuildRun {
    /// Every recovery note on node 1 about a sync or a rebuild fact, in trace order.
    fn sync_notes(&self) -> Vec<KernelNote> {
        self.trace
            .events
            .iter()
            .filter(|event| event.node == NODE)
            .filter_map(|event| match &event.kind {
                TraceKind::KernelNoted { note, .. } => Some(note.clone()),
                _ => None,
            })
            .filter(|note| {
                matches!(
                    note,
                    KernelNote::SyncWithheld { .. }
                        | KernelNote::SyncProven { .. }
                        | KernelNote::RecoveryFact { .. }
                )
            })
            .collect()
    }

    /// How many CASes wrote partition 1's record: the recovery's, then the activation's.
    fn partition_cas(&self) -> usize {
        use rdb_core::contracts::trace::ControlOpKind;
        self.trace
            .events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    TraceKind::ControlInteraction {
                        op: ControlOpKind::Cas,
                        key: Some(ControlKey::Partition(PartitionId(1))),
                        ..
                    }
                )
            })
            .count()
    }

    fn proven(&self) -> Vec<(CopyId, Seq)> {
        self.sync_notes()
            .into_iter()
            .filter_map(|note| match note {
                KernelNote::SyncProven { copy, cutoff, .. } => Some((copy, cutoff)),
                _ => None,
            })
            .collect()
    }
}

/// Scaffolding for M7B-137: each required copy is proved from the pinned configuration's node
/// in the new generation, copy 2 included though no survivor was ever placed for it. Rebuilding
/// closes: F1 proposes activation by a second CAS and ends `Committed`.
///
/// Copy 2's holder starts empty, so its new generation is filled by R1's catch-up alone, with
/// the members' fan-out on by default (B-R58b) and nothing seeded: the records node 3 stores
/// are node 1's, sent from the root, and R1's own `CopyCaughtUp` is what starts the rebuild's
/// syncs. With the fan-out off, node 3 hears nothing, holds nothing, and copy 2 is never proved.
#[retcd_test]
fn m7b_137_sync_after_commit_proves_each_pinned_copy_from_its_own_engine() {
    support::preamble();
    let run = run_rebuild(Vec::new());
    let history = spine_history();
    assert_eq!(
        run.copy_2_head,
        Some((SPINE_HEAD, history.digest(SPINE_HEAD))),
        "R1 caught copy 2 up to the cutoff"
    );
    assert_eq!(run.copy_2_base, Seq(0), "node 3 inherited nothing");
    let sent: Vec<Option<Bytes>> = (1..=SPINE_HEAD)
        .map(|seq| {
            rdb_sim::storage::history::history_writes(history.batch(seq), Seq(seq))
                .map(|(record, _)| record)
        })
        .collect();
    assert_eq!(
        run.copy_2_records, sent,
        "node 3 stores node 1's records in the new generation, each written by the catch-up"
    );
    let head = Seq(SPINE_HEAD);
    assert_eq!(
        run.proven(),
        vec![
            (CopyId(0), head),
            (CopyId(1), head),
            (CopyId(0), head),
            (CopyId(1), head),
            (CopyId(2), head),
        ],
        "two survivors before commit, then every required copy: {:?}",
        run.sync_notes()
    );
    assert_eq!(
        run.partition_cas(),
        2,
        "the recovery's CAS, then activation's"
    );
    assert_eq!(
        run.phase,
        Some(rdb_core::recovery::RecoveryPhase::Committed)
    );
}

/// The spine's history as another owner epoch wrote it: a record at every seq that passes its
/// own check, and a different digest at the cutoff.
fn another_epochs_history() -> rdb_sim::storage::history::CanonicalHistory {
    let other = rdb_sim::storage::history::canonical_history(
        rdb_core::contracts::authority::Lineage {
            owner_epoch: OwnerEpoch(2),
            ..spine_prior()
        },
        ConfigVersion(1),
        SPINE_HEAD,
    )
    .expect("a canonical history");
    assert_ne!(
        other.digest(SPINE_HEAD),
        spine_digest(SPINE_HEAD),
        "precondition: another record at the cutoff"
    );
    other
}

/// Lead ruling B-R58c: a member outside the barrier lands **empty** in the new generation. Node
/// 3 holds copy 2, which the barrier does not name, and it starts with another owner epoch's
/// predecessor history through the cutoff — content nobody verified. It inherits none of it: R1
/// catches it up from the root, and its engine reads exactly what R1 sent at every seq. F1 then
/// proves copy 2 like any other and activates. Inheriting that prefix would put R1's writes
/// under a base where no read sees them, and the copy would read the other epoch's records.
#[retcd_test]
fn fanout_a_member_outside_the_barrier_lands_empty_and_reads_what_r1_sent() {
    support::preamble();
    let run = run_rebuild(another_epochs_history().batches);
    assert_eq!(run.copy_2_base, Seq(0), "node 3 inherited nothing");
    let history = spine_history();
    assert_eq!(
        run.copy_2_head,
        Some((SPINE_HEAD, history.digest(SPINE_HEAD))),
        "R1 caught copy 2 up to the cutoff"
    );
    let sent: Vec<Option<Bytes>> = (1..=SPINE_HEAD)
        .map(|seq| {
            rdb_sim::storage::history::history_writes(history.batch(seq), Seq(seq))
                .map(|(record, _)| record)
        })
        .collect();
    assert_eq!(
        run.copy_2_records, sent,
        "node 3 reads what R1 sent at every seq, not the other epoch's records"
    );
    assert!(
        run.proven().contains(&(CopyId(2), Seq(SPINE_HEAD))),
        "F1 proved copy 2: {:?}",
        run.sync_notes()
    );
    assert_eq!(
        run.partition_cas(),
        2,
        "the recovery's CAS, then activation's"
    );
    assert_eq!(
        run.phase,
        Some(rdb_core::recovery::RecoveryPhase::Committed)
    );
}

/// One real sync of partition 1 at `generation` on `engine`, through `through`.
fn sync_send_lineage(
    engine: &mut rdb_sim::storage::memory::MemoryEngine,
    generation: Generation,
    through: u64,
) {
    engine
        .sync_wal_through(vec![rdb_core::contracts::storage::CapturedPrefix {
            partition: PartitionId(1),
            generation,
            through: rdb_core::contracts::ids::AppliedSeq(through),
        }])
        .expect("a sync");
}

/// Lead ruling B-R58d, [`StorageOp::LoseRecord`] on the engine alone. Planned before the record
/// exists, it waits. The first sync after the record is written loses it, and the sync still
/// reports the prefix durable: nothing reports a lost write. Its neighbour is untouched, and a
/// host crash does not bring it back.
#[retcd_test]
fn storage_a_lost_record_is_gone_from_the_first_sync_after_it_was_written() {
    use rdb_core::contracts::ids::DurableSeq;
    use rdb_core::contracts::storage::StorageFault;
    use rdb_sim::storage::crash_image::CrashImage;
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    let history = send_history(3);
    let old = Generation(1);
    let mut engine = MemoryEngine::new(NODE);
    engine
        .inject(StorageOp::LoseRecord {
            node: NODE,
            partition: PartitionId(1),
            generation: old,
            seq: Seq(3),
        })
        .expect("a planned loss");
    for seq in 1..=2 {
        engine.commit(history.batch(seq).clone()).expect("a commit");
    }
    sync_send_lineage(&mut engine, old, 2);
    assert!(engine.has_planned(), "no record at 3 yet: the loss waits");
    engine.commit(history.batch(3).clone()).expect("record 3");
    sync_send_lineage(&mut engine, old, 3);
    assert!(!engine.has_planned(), "taken by the sync after record 3");

    let record = |engine: &MemoryEngine, seq: u64| {
        engine
            .history_at(PartitionId(1), old, Seq(seq))
            .map(|(record, _)| record)
    };
    let written = |seq: u64| {
        rdb_sim::storage::history::history_writes(history.batch(seq), Seq(seq))
            .map(|(record, _)| record)
    };
    assert_eq!(record(&engine, 3), None, "the record is lost");
    assert_eq!(record(&engine, 2), written(2), "its neighbour is not");
    assert_eq!(
        engine.durable(PartitionId(1), old),
        DurableSeq(3),
        "the sync reported the prefix durable all the same"
    );
    let host = CrashImage::of(&engine, StorageFault::HostCrash)
        .expect("an image")
        .reopen(NODE);
    assert_eq!(record(&host, 3), None, "a crash does not bring it back");
}

/// Lead ruling B-R58d, [`StorageOp::MisfileRecord`] on the engine alone. Generation 2 inherits
/// generation 1 through 2; the misfile waits until generation 2 has written its own record 3,
/// and then generation 1's record 3 — which passes its own check — is what generation 2 reads
/// there. Generation 1 still reads its own, and record 2 is unchanged.
#[retcd_test]
fn storage_a_misfiled_record_shadows_the_one_its_target_wrote() {
    use rdb_sim::storage::history::{history_writes, verified_record};
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    let prior = send_history(3);
    let (old, new) = (Generation(1), Generation(2));
    let mut engine = MemoryEngine::new(NODE);
    for batch in prior.batches.clone() {
        engine.commit(batch).expect("a commit");
    }
    engine
        .inherit(PartitionId(1), old, new, Seq(2))
        .expect("generation 2 starts at 2");
    engine
        .inject(StorageOp::MisfileRecord {
            node: NODE,
            partition: PartitionId(1),
            from: old,
            to: new,
            seq: Seq(3),
        })
        .expect("a planned misfile");
    sync_send_lineage(&mut engine, new, 2);
    assert!(
        engine.has_planned(),
        "generation 2 has not written 3: the misfile waits"
    );
    let next = send_history_after(2, &prior, 2, 3);
    engine
        .commit(next.batch(3).clone())
        .expect("generation 2's 3");
    sync_send_lineage(&mut engine, new, 3);
    assert!(!engine.has_planned(), "taken by the sync after it");

    let read =
        |generation: Generation, seq: u64| engine.history_at(PartitionId(1), generation, Seq(seq));
    let written = |history: &rdb_sim::storage::history::CanonicalHistory, seq: u64| {
        history_writes(history.batch(seq), Seq(seq)).map(|(record, _)| record)
    };
    let (record, progress) = read(new, 3).expect("a record at 3");
    assert_eq!(Some(record.clone()), written(&prior, 3), "generation 1's");
    assert_ne!(written(&prior, 3), written(&next, 3), "precondition");
    assert!(
        verified_record(Seq(3), &record, progress.as_ref()).is_ok(),
        "it passes its own check"
    );
    assert_eq!(read(old, 3).map(|(record, _)| record), written(&prior, 3));
    assert_eq!(read(new, 2).map(|(record, _)| record), written(&prior, 2));
    assert_eq!(
        engine.inject(StorageOp::MisfileRecord {
            node: NODE,
            partition: PartitionId(1),
            from: new,
            to: new,
            seq: Seq(3),
        }),
        Err(SimError::Config { field: "fault" }),
        "a misfile into its own lineage is refused: it would change nothing"
    );
}

/// Lead ruling L-R177do, on the network alone: a `ForgeAck` plan waits for an acknowledgement.
/// A frame that is not one passes it by, under its own label. The next acknowledgement arrives
/// once, at once, under the forged label, with its body unchanged, and the plan is spent. An
/// authenticated forgery whose claimed role is the body's own is a frame-level lie too, and is
/// delivered the same way.
#[retcd_test]
fn forge_ack_waits_for_an_acknowledgement_and_delivers_it_under_the_forged_label() {
    use rdb_sim::sim::network::{Arrival, Fate};
    support::preamble();
    let label = PeerLabel {
        node: NodeId(1),
        boot: BOOT,
        authenticated: true,
    };
    let forge = |claimed_role: ReplicaRole, authenticated: bool| NetworkOp::ForgeAck {
        from: NodeId(1),
        to: NodeId(2),
        claimed_node: NodeId(3),
        claimed_role,
        authenticated,
    };
    let once = |label: PeerLabel, frame: Frame| {
        Ok(Fate::Delivered(vec![Arrival {
            delay_millis: 0,
            label,
            frame,
        }]))
    };
    let mut network = Network::new();
    network
        .inject(forge(ReplicaRole::RegularSecondary, false))
        .expect("a plan");
    assert_eq!(
        network.send(NodeId(1), NodeId(2), label, frame()),
        once(label, frame()),
        "not an acknowledgement: passed by under its own label"
    );
    assert_eq!(network.planned().len(), 1, "the plan still waits");
    let ack = ack_frame(ReplicaRole::Shadow);
    let forged = |authenticated: bool| PeerLabel {
        node: NodeId(3),
        boot: BOOT,
        authenticated,
    };
    assert_eq!(
        network.send(NodeId(1), NodeId(2), label, ack.clone()),
        once(forged(false), ack.clone()),
        "the acknowledgement, under the forged label, body unchanged"
    );
    assert!(network.planned().is_empty(), "the plan is spent");

    network
        .inject(forge(ReplicaRole::Shadow, true))
        .expect("a plan");
    assert_eq!(
        network.send(NodeId(1), NodeId(2), label, ack.clone()),
        once(forged(true), ack),
        "a stolen credential under another node's name: the role claimed is the body's"
    );
}

/// `Network::forge_ack`, on the network alone: spec §5.2's case. A shadow's acknowledgement,
/// forged under its own name with a real credential, claims `RegularSecondary`. It arrives once,
/// at once, under the forged label, and its body decodes as the same acknowledgement with **only**
/// the role changed. The frame's header is the sender's, and [`Network::frames`] keeps the frame
/// as it was sent. The plan is spent.
///
/// Near-miss twin, one fact changed (`authenticated: false`): the body arrives byte for byte as
/// sent, so the role lie is the credentialed forgery's and nobody else's.
#[retcd_test]
fn forge_ack_writes_the_claimed_role_into_an_authenticated_acknowledgement() {
    use rdb_core::contracts::envelope::AppendOutcome;
    use rdb_core::replication::wire::decode_reply;
    use rdb_sim::sim::network::Fate;
    support::preamble();
    let label = PeerLabel {
        node: NodeId(1),
        boot: BOOT,
        authenticated: true,
    };
    let send = |authenticated: bool| {
        let mut network = Network::new();
        network
            .inject(NetworkOp::ForgeAck {
                from: NodeId(1),
                to: NodeId(2),
                claimed_node: NodeId(1),
                claimed_role: ReplicaRole::RegularSecondary,
                authenticated,
            })
            .expect("a plan");
        let fate = network
            .send(NodeId(1), NodeId(2), label, ack_frame(ReplicaRole::Shadow))
            .expect("an acknowledgement on the link");
        assert!(network.planned().is_empty(), "the plan is spent");
        assert_eq!(
            network.frames(),
            [ack_frame(ReplicaRole::Shadow)],
            "the network records the frame as sent, before the forgery"
        );
        let Fate::Delivered(arrivals) = fate else {
            panic!("a forged acknowledgement is delivered: {fate:?}");
        };
        assert_eq!(arrivals.len(), 1, "once");
        arrivals.into_iter().next().expect("one arrival")
    };
    let decoded = |frame: &Frame| match decode_reply(&frame.body) {
        Ok(AppendOutcome::Accepted(ack)) => ack,
        other => panic!("an acknowledgement still decodes: {other:?}"),
    };
    let sent = decoded(&ack_frame(ReplicaRole::Shadow));

    let forged = send(true);
    assert_eq!(forged.delay_millis, 0, "at once");
    assert_eq!(
        forged.label, label,
        "under the sender's own, real, credential"
    );
    assert_eq!(
        decoded(&forged.frame),
        rdb_core::contracts::envelope::AppendAck {
            role: ReplicaRole::RegularSecondary,
            ..sent
        },
        "the body claims the role, and nothing else in it changed"
    );
    assert_eq!(
        Frame {
            body: bytes::Bytes::new(),
            ..forged.frame
        },
        frame(),
        "the header is the sender's"
    );

    let honest = send(false);
    assert_eq!(
        honest.frame,
        ack_frame(ReplicaRole::Shadow),
        "unauthenticated: the body is unchanged"
    );
}

/// Lead ruling L-R177do, through the run loop. In the rebuild run node 3 acknowledges R1's
/// catch-up to node 1. The forgery takes node 3's first acknowledgement and delivers it to node 1
/// as node 2's, `authenticated: false`, body unchanged. **R1's own check** refuses it — its
/// tracker's rule 1, `AckRejected(ForgedIdentity)` — on node 1, once. The same run without the
/// forgery notes no forged identity anywhere, so the refusal is the forgery's.
///
/// The forgery *steals* node 3's acknowledgement, so node 1 never counts it. R1's retransmit
/// (B-R67) re-sends the unacknowledged record on its timer, node 3 acknowledges again, and the
/// rebuild ends where the control's does: `Committed`. A refused forgery costs a retransmit
/// period, not the recovery. This row credits no M7B-32 (lead ruling).
#[retcd_test]
fn forge_ack_an_unauthenticated_acknowledgement_reaches_r1_and_r1_refuses_it() {
    use rdb_core::contracts::ignore::KernelIgnoredReason;
    use rdb_core::contracts::trace::AckRejectReason;
    use rdb_core::recovery::RecoveryPhase;
    support::preamble();
    let run = |op: Option<NetworkOp>| {
        let mut plan = rebuild_plan(Vec::new());
        plan.network_ops.extend(op);
        let mut runner = Runner::new(&plan).expect("a runner");
        let stop = runner.run(plan.limits).expect("the rebuild runs").stop;
        assert!(
            matches!(
                stop,
                rdb_sim::harness::run::StopReason::DeadlineReached { .. }
            ),
            "the rebuild runs to its deadline, never a refusal: {stop:?}"
        );
        let phase = runner
            .dispatcher()
            .recovery(NODE, PartitionId(1))
            .map(rdb_core::recovery::Recovery::phase);
        let waiting = runner.dispatcher().network().planned().to_vec();
        let trace = runner.finish().expect("a trace");
        let refused: Vec<(NodeId, ModuleName)> = trace
            .events
            .iter()
            .filter_map(|event| match &event.kind {
                TraceKind::KernelNoted {
                    module,
                    note:
                        KernelNote::Ignored {
                            reason:
                                KernelIgnoredReason::AckRejected(AckRejectReason::ForgedIdentity),
                        },
                    ..
                } => Some((event.node, *module)),
                _ => None,
            })
            .collect();
        (phase, waiting, refused)
    };
    let (control_phase, _, control) = run(None);
    assert!(
        control.is_empty(),
        "control: without the forgery nothing is refused as forged: {control:?}"
    );
    assert_eq!(
        control_phase,
        Some(RecoveryPhase::Committed),
        "control: the rebuild commits"
    );
    let (phase, waiting, refused) = run(Some(NetworkOp::ForgeAck {
        from: NodeId(3),
        to: NODE,
        claimed_node: NodeId(2),
        claimed_role: ReplicaRole::RegularSecondary,
        authenticated: false,
    }));
    assert!(
        waiting.is_empty(),
        "the forgery took an acknowledgement: {waiting:?}"
    );
    assert_eq!(
        refused,
        vec![(NODE, ModuleName::Replication)],
        "delivered to node 1, and refused there by R1's own check, once"
    );
    assert_eq!(
        phase, control_phase,
        "R1's retransmit recovers the stolen acknowledgement: the rebuild ends as the control's"
    );
}

/// One rebuild run with `op` planned on the network: where recovery ended, the network plans
/// still waiting, and every `(node, module)` that noted `AckRejected(reason)`. The run must reach
/// its deadline; a refusal stop fails the caller here.
fn rebuild_rejecting(
    op: Option<NetworkOp>,
    reason: rdb_core::contracts::trace::AckRejectReason,
) -> (
    Option<rdb_core::recovery::RecoveryPhase>,
    Vec<NetworkOp>,
    Vec<(NodeId, ModuleName)>,
) {
    use rdb_core::contracts::ignore::KernelIgnoredReason;
    let mut plan = rebuild_plan(Vec::new());
    plan.network_ops.extend(op);
    let mut runner = Runner::new(&plan).expect("a runner");
    let stop = runner.run(plan.limits).expect("the rebuild runs").stop;
    assert!(
        matches!(
            stop,
            rdb_sim::harness::run::StopReason::DeadlineReached { .. }
        ),
        "the rebuild runs to its deadline, never a refusal: {stop:?}"
    );
    let phase = runner
        .dispatcher()
        .recovery(NODE, PartitionId(1))
        .map(rdb_core::recovery::Recovery::phase);
    let waiting = runner.dispatcher().network().planned().to_vec();
    let trace = runner.finish().expect("a trace");
    let refused = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module,
                note:
                    KernelNote::Ignored {
                        reason: KernelIgnoredReason::AckRejected(noted),
                    },
                ..
            } if *noted == reason => Some((event.node, *module)),
            _ => None,
        })
        .collect();
    (phase, waiting, refused)
}

/// `Network::forge_ack`, through the run loop: the lie is in the body. Node 3 is a regular
/// secondary. The forgery takes its first acknowledgement to node 1 and delivers it under node 3's
/// own name, `authenticated: true`, with the body's role rewritten to `Primary`. Nothing outside
/// the body lies, so R1's identity, lineage and configuration rules pass it, and **R1's own role
/// rule** refuses it — rule 5, `AckRejected(RoleMismatch)`, because R1 holds node 3 to the role
/// its pinned configuration names, never the one the frame claims (spec §5.2). On node 1, once.
/// The same run without the forgery notes no role mismatch anywhere, so the refusal is the
/// forgery's. Before 2026-10-02 this forgery stopped the run as `Refused` at the
/// `sim::network::Network::forge_ack` seam.
///
/// As with the unauthenticated forgery above, the acknowledgement is stolen, R1's retransmit
/// re-sends, and the rebuild ends where the control's does.
#[retcd_test]
fn forge_ack_an_authenticated_role_lie_in_the_body_reaches_r1_and_r1_refuses_it() {
    use rdb_core::contracts::trace::AckRejectReason;
    use rdb_core::recovery::RecoveryPhase;
    support::preamble();
    let (control_phase, _, control) = rebuild_rejecting(None, AckRejectReason::RoleMismatch);
    assert!(
        control.is_empty(),
        "control: without the forgery no acknowledgement is refused for its role: {control:?}"
    );
    assert_eq!(
        control_phase,
        Some(RecoveryPhase::Committed),
        "control: the rebuild commits"
    );
    let (phase, waiting, refused) = rebuild_rejecting(
        Some(NetworkOp::ForgeAck {
            from: NodeId(3),
            to: NODE,
            claimed_node: NodeId(3),
            claimed_role: ReplicaRole::Primary,
            authenticated: true,
        }),
        AckRejectReason::RoleMismatch,
    );
    assert!(
        waiting.is_empty(),
        "the forgery took an acknowledgement: {waiting:?}"
    );
    assert_eq!(
        refused,
        vec![(NODE, ModuleName::Replication)],
        "delivered to node 1, and refused there by R1's role rule, once"
    );
    assert_eq!(
        phase, control_phase,
        "R1's retransmit recovers the stolen acknowledgement: the rebuild ends as the control's"
    );
}

/// `Delivery::OverstateDurable`, on the network alone. An acknowledgement planned to overstate
/// arrives once, after the planned delay, under the sender's own label, and its body decodes as
/// the same acknowledgement with **only** `durable` changed — one past `buffered_applied`. A
/// frame that is not an acknowledgement passes the plan by, so the plan waits for an
/// acknowledgement, as a `ForgeAck` does.
#[retcd_test]
fn overstate_durable_writes_durable_past_buffered_into_an_acknowledgement() {
    use rdb_core::contracts::envelope::{AppendOutcome, ReplicaProgress};
    use rdb_core::contracts::ids::DurableSeq;
    use rdb_core::replication::wire::decode_reply;
    use rdb_sim::sim::network::{Arrival, Delivery, Fate};
    support::preamble();
    let label = PeerLabel {
        node: NodeId(1),
        boot: BOOT,
        authenticated: true,
    };
    let mut network = Network::new();
    network
        .inject(NetworkOp::PlanNext {
            from: NodeId(1),
            to: NodeId(2),
            delivery: Delivery::OverstateDurable { delay_millis: 7 },
        })
        .expect("a plan");
    let passed = network
        .send(NodeId(1), NodeId(2), label, frame())
        .expect("a frame on the link");
    assert_eq!(
        passed,
        Fate::Delivered(vec![Arrival {
            delay_millis: 0,
            label,
            frame: frame(),
        }]),
        "a frame that is not an acknowledgement passes the plan by"
    );
    assert_eq!(network.planned().len(), 1, "and the plan still waits");

    let sent_frame = ack_frame(ReplicaRole::RegularSecondary);
    let fate = network
        .send(NodeId(1), NodeId(2), label, sent_frame.clone())
        .expect("an acknowledgement on the link");
    assert!(network.planned().is_empty(), "the plan is spent");
    let Fate::Delivered(arrivals) = fate else {
        panic!("an overstated acknowledgement is delivered: {fate:?}");
    };
    let [arrival] = arrivals.as_slice() else {
        panic!("once: {arrivals:?}");
    };
    assert_eq!(
        (arrival.delay_millis, arrival.label),
        (7, label),
        "after the planned delay, under the sender's own label"
    );
    let decoded = |frame: &Frame| match decode_reply(&frame.body) {
        Ok(AppendOutcome::Accepted(ack)) => ack,
        other => panic!("an acknowledgement still decodes: {other:?}"),
    };
    let sent = decoded(&sent_frame);
    assert_eq!(
        decoded(&arrival.frame),
        rdb_core::contracts::envelope::AppendAck {
            progress: ReplicaProgress {
                durable: DurableSeq(sent.progress.buffered_applied.0 + 1),
                ..sent.progress
            },
            ..sent
        },
        "durable is one past buffered_applied, and nothing else in the body changed"
    );
    assert_eq!(
        network.frames()[1],
        sent_frame,
        "the network records the frame as sent, before the lie"
    );
}

/// `Delivery::OverstateDurable`, through the run loop: the producer for the verification plan's
/// §15.1 cell 3. Node 3's first acknowledgement to node 1 arrives under node 3's own, real label
/// with `durable` one past `buffered_applied`. Identity, lineage, role and boot are all true, so
/// R1's rules 1–6 pass it, and **R1's own ordering rule** refuses it — rule 7,
/// `AckRejected(InconsistentProgress)` — on node 1, once. The same run without the lie notes no
/// inconsistent progress anywhere, so the refusal is the lie's; and, as with a stolen
/// acknowledgement, R1's retransmit re-sends and the rebuild ends where the control's does.
#[retcd_test]
fn overstate_durable_a_progress_lie_in_the_body_reaches_r1_and_r1_refuses_it() {
    use rdb_core::contracts::trace::AckRejectReason;
    use rdb_core::recovery::RecoveryPhase;
    use rdb_sim::sim::network::Delivery;
    support::preamble();
    let (control_phase, _, control) =
        rebuild_rejecting(None, AckRejectReason::InconsistentProgress);
    assert!(
        control.is_empty(),
        "control: without the lie no acknowledgement is refused for its progress: {control:?}"
    );
    assert_eq!(
        control_phase,
        Some(RecoveryPhase::Committed),
        "control: the rebuild commits"
    );
    let (phase, waiting, refused) = rebuild_rejecting(
        Some(NetworkOp::PlanNext {
            from: NodeId(3),
            to: NODE,
            delivery: Delivery::OverstateDurable { delay_millis: 0 },
        }),
        AckRejectReason::InconsistentProgress,
    );
    assert!(
        waiting.is_empty(),
        "the lie took an acknowledgement: {waiting:?}"
    );
    assert_eq!(
        refused,
        vec![(NODE, ModuleName::Replication)],
        "delivered to node 1, and refused there by R1's ordering rule, once"
    );
    assert_eq!(
        phase, control_phase,
        "R1's retransmit recovers the refused acknowledgement: the rebuild ends as the control's"
    );
}

/// What one rebuild run with planned replies left behind.
struct ReplyRun {
    stop: rdb_sim::harness::run::StopReason,
    /// Whether `RunReport::into_result` accepts the run: it was neither refused nor declined.
    bounded: bool,
    phase: Option<rdb_core::recovery::RecoveryPhase>,
    /// Copy 2's receiver on node 3 at the end: its applied head.
    copy_2_head: Option<(u64, rdb_core::contracts::digest::Digest)>,
    /// How many copies of each frame from node 3 to node 1 the network delivered, in send order.
    replies: Vec<u8>,
    /// Plans the run never spent.
    waiting: usize,
}

impl ReplyRun {
    /// Where the run ended: F1's phase on node 1, and copy 2's head.
    fn ends(
        &self,
    ) -> (
        Option<&rdb_core::recovery::RecoveryPhase>,
        Option<&(u64, rdb_core::contracts::digest::Digest)>,
    ) {
        (self.phase.as_ref(), self.copy_2_head.as_ref())
    }

    /// How this run failed to end as `control` did — a refusal or a decline, a plan left
    /// unspent, or another phase or head — or `None`.
    fn deviation(&self, control: &Self) -> Option<String> {
        (!self.bounded || self.waiting > 0 || self.ends() != control.ends()).then(|| {
            format!(
                "stop {:?}, {} plans unspent, ends {:?}",
                self.stop,
                self.waiting,
                self.ends()
            )
        })
    }
}

/// The rebuild run with node 3's replies to node 1 planned: reply `at` (in send order, from 0)
/// goes as its `Delivery`, and every reply up to the last planned one that is not named goes at
/// once, exactly as an unplanned frame would. `PlanNext` decides the next frame on the link, so
/// the replies in between must be planned through.
fn rebuild_with_replies(planned: &[(usize, rdb_sim::sim::network::Delivery)]) -> ReplyRun {
    use rdb_sim::sim::network::Delivery;
    let last = planned.iter().map(|(at, _)| *at).max();
    let ops = last.map_or_else(Vec::new, |last| {
        (0..=last)
            .map(|at| NetworkOp::PlanNext {
                from: NodeId(3),
                to: NODE,
                delivery: planned
                    .iter()
                    .find(|(named, _)| *named == at)
                    .map_or(Delivery::Deliver { delay_millis: 0 }, |(_, delivery)| {
                        *delivery
                    }),
            })
            .collect()
    });
    let mut plan = rebuild_plan(Vec::new());
    plan.network_ops.extend(ops);
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the rebuild runs");
    let bounded = report.clone().into_result().is_ok();
    let stop = report.stop;
    let dispatcher = runner.dispatcher();
    ReplyRun {
        stop,
        bounded,
        phase: dispatcher
            .recovery(NODE, PartitionId(1))
            .map(rdb_core::recovery::Recovery::phase),
        copy_2_head: dispatcher
            .replication()
            .receiver(NodeId(3), PartitionId(1))
            .map(|receiver| receiver.applied_head())
            .map(|head| (head.seq.0, head.digest)),
        replies: dispatcher
            .network()
            .transmissions()
            .iter()
            .filter(|sent| (sent.from, sent.to) == (NodeId(3), NODE))
            .map(|sent| sent.copies)
            .collect(),
        waiting: dispatcher.network().planned().len(),
    }
}

/// The rebuild run with nothing planned: it commits, and node 3 replies to node 1 and loses
/// nothing.
fn rebuild_control() -> ReplyRun {
    use rdb_core::recovery::RecoveryPhase;
    let control = rebuild_with_replies(&[]);
    assert!(
        control.bounded,
        "control: neither refused nor declined: {:?}",
        control.stop
    );
    assert_eq!(
        control.phase,
        Some(RecoveryPhase::Committed),
        "control: the rebuild commits"
    );
    assert!(
        !control.replies.is_empty() && control.replies.iter().all(|copies| *copies == 1),
        "control: node 3 replies to node 1 and loses nothing: {:?}",
        control.replies
    );
    control
}

/// Lead ruling B-R67, end to end (tester-kb-r1 B1, probe S3): one lost reply does not stall a
/// catch-up. In the rebuild run node 3 holds only a receiver, so its every frame to node 1 is a
/// reply to R1. The row loses each of them in turn, one per run, and every run ends where the
/// control's does: `Committed`, with copy 2 at the same head. Some of those replies acknowledge a
/// record R1's cursor still holds outstanding. Before B-R67 nothing re-sent such a record — the
/// keepalive skips a copy with one outstanding — so losing its reply idled the rebuild in
/// `Rebuilding` to any deadline. R1's retransmit re-sends it and node 3 acknowledges again.
#[retcd_test]
fn rebuild_a_lost_acknowledgement_is_recovered_by_r1s_retransmit() {
    use rdb_sim::sim::network::Delivery;
    support::preamble();
    let control = rebuild_control();
    for lost in 0..control.replies.len() {
        let run = rebuild_with_replies(&[(lost, Delivery::Drop)]);
        assert!(
            run.bounded,
            "reply {lost}: neither refused nor declined: {:?}",
            run.stop
        );
        assert_eq!(run.waiting, 0, "reply {lost}: every plan was spent");
        let dropped: Vec<usize> = (0..run.replies.len())
            .filter(|at| run.replies[*at] == 0)
            .collect();
        assert_eq!(
            dropped,
            vec![lost],
            "reply {lost} is lost, and nothing else is: {:?}",
            run.replies
        );
        assert_eq!(
            run.ends(),
            control.ends(),
            "reply {lost} of {} lost: R1 recovers it, and the rebuild ends as the control's",
            control.replies.len()
        );
    }
}

/// Lead rulings B-R67c and B-R67d, end to end (tester-kb-r1 B2, probe S4c): latency alone does
/// not escalate a rebuild. Nothing is lost and the network duplicates nothing. Reply 1 is slow
/// (300 ms, past R1's second retransmit fire), and one later reply `k` is slower (600 ms), for
/// each `k` in turn. R1 re-sends the record whose ACK is slow, so node 3 acknowledges it twice,
/// and the slow original can land while the cursor is mid-walk. The second ACK repeats what the
/// cursor already took and is split off before the in-flight check: at the mark it is progress,
/// below it `Recorded`, never a send or a snapshot. So every run ends where the control's does:
/// no refusal, `Committed`, copy 2 at the same head. Before the split a repeat reached the
/// in-flight check as `Unverifiable` and asked for a snapshot the sim does not build.
#[retcd_test]
fn rebuild_two_slow_replies_end_as_the_control() {
    use rdb_sim::sim::network::Delivery;
    support::preamble();
    let control = rebuild_control();
    let slow = |delay_millis: u64| Delivery::Deliver { delay_millis };
    // Reply 1 slow alone. The runs agree up to each one's reply `k`, so `k` ranges over the
    // replies this run sends before its deadline.
    let first = rebuild_with_replies(&[(1, slow(300))]);
    let mut deviations: Vec<(Option<usize>, String)> = first
        .deviation(&control)
        .map(|how| (None, how))
        .into_iter()
        .collect();
    for later in 2..first.replies.len() {
        let run = rebuild_with_replies(&[(1, slow(300)), (later, slow(600))]);
        assert!(
            run.replies.iter().all(|copies| *copies == 1),
            "reply {later}: nothing is lost or duplicated: {:?}",
            run.replies
        );
        deviations.extend(run.deviation(&control).map(|how| (Some(later), how)));
    }
    assert!(
        deviations.is_empty(),
        "reply 1 at 300 ms, alone (None) and with reply k at 600 ms, k in 2..{}: every rebuild \
         ends as the control's {:?}; these did not: {deviations:#?}",
        first.replies.len(),
        control.ends()
    );
}

/// Lead rulings B-R67c and B-R67d, end to end (tester-kb-r1 probe S4b): a reply the network
/// duplicates does not escalate a rebuild. Reply 1 arrives twice, the second copy 5 ms or 300 ms
/// behind the first. The second copy repeats an ACK the cursor already took, so it is split off
/// as progress or `Recorded`, and the run ends where the control's does. Before the split it was
/// refused `Unverifiable` and the rebuild stopped on the snapshot request (M7B-174, B-R58c).
#[retcd_test]
fn rebuild_a_duplicated_reply_ends_as_the_control() {
    use rdb_sim::sim::network::Delivery;
    support::preamble();
    let control = rebuild_control();
    let mut deviations = Vec::new();
    for second_delay_millis in [5, 300] {
        let run = rebuild_with_replies(&[(
            1,
            Delivery::Duplicate {
                delay_millis: 0,
                second_delay_millis,
            },
        )]);
        assert_eq!(
            run.replies.get(1),
            Some(&2),
            "reply 1 arrives twice: {:?}",
            run.replies
        );
        deviations.extend(
            run.deviation(&control)
                .map(|how| (second_delay_millis, how)),
        );
    }
    assert!(
        deviations.is_empty(),
        "reply 1 duplicated 5 or 300 ms apart: every rebuild ends as the control's {:?}; these \
         did not: {deviations:#?}",
        control.ends()
    );
}

/// Lead ruling B-R70 (superseding L-R177do's refusal), through F1's `SyncWalThrough` provider.
/// A stalled sync never answers, so the provider **withholds it as stalled**:
/// [`rdb_core::contracts::trace::SyncWithheldReason::Stalled`] on F1's request, no proof, and the
/// run goes on so F1's own sync timer can report it (B-R52). The capture stays held. Two paths:
/// node 2's survivor proof at the barrier, and node 3's rebuilt copy while rebuilding. The clean
/// run refuses nothing and withholds nothing as stalled.
#[retcd_test]
fn recovery_a_stalled_sync_is_withheld_as_stalled_not_refused() {
    use rdb_core::contracts::trace::SyncWithheldReason;
    use rdb_core::recovery::RecoveryPhase;
    use rdb_sim::harness::run::StopReason;
    support::preamble();
    let run = |stall: Option<(NodeId, CopyId)>| {
        let mut plan = rebuild_plan(Vec::new());
        plan.storage_ops
            .extend(stall.map(|(node, _)| StorageOp::StallFlush { node }));
        let mut runner = Runner::new(&plan).expect("a runner");
        let stop = runner.run(plan.limits).expect("the rebuild runs").stop;
        let phase = runner
            .dispatcher()
            .recovery(NODE, PartitionId(1))
            .map(rdb_core::recovery::Recovery::phase);
        let held = stall.map(|(node, _)| {
            runner
                .dispatcher()
                .engine(node)
                .expect("the node's engine")
                .stalled_syncs()
                .len()
        });
        let refused = matches!(stop, StopReason::Refused { .. });
        let trace = runner.finish().expect("a trace");
        // Every sync note on F1's node, as (copy, cutoff, stalled?, proven?).
        let fates: Vec<(CopyId, Seq, bool, bool)> = trace
            .events
            .iter()
            .filter(|event| event.node == NODE)
            .filter_map(|event| match &event.kind {
                TraceKind::KernelNoted {
                    note:
                        KernelNote::SyncWithheld {
                            copy,
                            cutoff,
                            reason,
                        },
                    ..
                } => Some((
                    *copy,
                    *cutoff,
                    *reason == SyncWithheldReason::Stalled,
                    false,
                )),
                TraceKind::KernelNoted {
                    note: KernelNote::SyncProven { copy, cutoff, .. },
                    ..
                } => Some((*copy, *cutoff, false, true)),
                _ => None,
            })
            .collect();
        (refused, phase, held, fates)
    };
    let (refused, _, _, fates) = run(None);
    assert!(!refused, "control: the clean run refuses nothing");
    assert!(
        fates.iter().all(|(_, _, stalled, _)| !stalled),
        "control: nothing withheld as stalled: {fates:?}"
    );
    for (node, copy, phase, path) in [
        (
            NodeId(2),
            CopyId(1),
            RecoveryPhase::Barrier,
            "a survivor's proof",
        ),
        (
            NodeId(3),
            CopyId(2),
            RecoveryPhase::Rebuilding,
            "the rebuilt copy's proof",
        ),
    ] {
        let (refused, at, held, fates) = run(Some((node, copy)));
        assert!(!refused, "{path} stalls: not refused");
        assert_eq!(
            at.as_ref(),
            Some(&phase),
            "{path} stalls: F1 stays in {phase:?}"
        );
        assert_eq!(held, Some(1), "{path} stalls: its capture held");
        let mine: Vec<(Seq, bool, bool)> = fates
            .iter()
            .filter(|(c, ..)| *c == copy)
            .map(|(_, cutoff, stalled, proven)| (*cutoff, *stalled, *proven))
            .collect();
        assert!(
            mine.contains(&(Seq(SPINE_HEAD), true, false)),
            "{path} stalls: withheld as stalled at the cutoff: {fates:?}"
        );
        assert!(
            mine.iter().all(|(_, _, proven)| !proven),
            "{path} stalls: never proven: {fates:?}"
        );
    }
}

/// Near miss (lead's condition on B-R55 item 3): copy 2's engine holds a record at the cutoff
/// that passes its own check but is not the committed history's — another owner epoch wrote
/// it. The harness proves durability and reports the digest it holds, without comparing it.
/// **F1** refuses it: `Rebuild::durable` judges the proof against the rebuild point and the
/// committed cutoff, and quarantines. Rebuilding never closes, and no activation CAS is written.
///
/// Planted by a misdirected write (lead ruling B-R58d): node 3 holds another epoch's
/// predecessor history, lands empty (B-R58c), and R1 catches it up with the committed records.
/// Then [`StorageOp::MisfileRecord`] writes the predecessor's record at the cutoff over R1's, at
/// the first sync after R1 wrote it.
#[retcd_test]
fn m7b_137_sync_after_commit_a_rebuilt_copy_holding_another_record_at_the_cutoff_is_quarantined_by_f1(
) {
    use rdb_core::contracts::recovery::{DivergenceEvidence, RecoveryEffect};
    support::preamble();
    let other = another_epochs_history();
    let run = run_rebuild_faulted(
        other.batches.clone(),
        Some(StorageOp::MisfileRecord {
            node: NodeId(3),
            partition: PartitionId(1),
            from: spine_prior().generation,
            to: REBUILT,
            seq: Seq(SPINE_HEAD),
        }),
    );
    assert_eq!(run.copy_2_base, Seq(0), "node 3 landed empty (B-R58c)");
    assert_eq!(
        run.copy_2_head,
        Some((SPINE_HEAD, spine_digest(SPINE_HEAD))),
        "R1 caught copy 2 up with the committed records"
    );
    let misfiled =
        rdb_sim::storage::history::history_writes(other.batch(SPINE_HEAD), Seq(SPINE_HEAD))
            .map(|(record, _)| record);
    assert_eq!(
        run.copy_2_records.last(),
        Some(&misfiled),
        "the other epoch's record now sits at the cutoff"
    );
    let notes = run.sync_notes();
    let proven_2 = notes
        .iter()
        .position(|note| {
            matches!(
                note,
                KernelNote::SyncProven {
                    copy: CopyId(2),
                    ..
                }
            )
        })
        .expect("the harness proved copy 2 durable and reported its digest");
    // F1's evidence names copy 2 at the cutoff with the digest its engine reported: against the
    // point its own catch-up pinned (`Pairwise`), or against the committed cutoff
    // (`RootMismatch`), whichever check F1 runs first.
    let reported = (CopyId(2), other.digest(SPINE_HEAD));
    let quarantine = notes
        .iter()
        .position(|note| match note {
            KernelNote::RecoveryFact {
                effect: RecoveryEffect::Quarantine(evidence),
            } => match evidence {
                DivergenceEvidence::Pairwise { seq, a, b } => {
                    *seq == Seq(SPINE_HEAD) && (*a == reported || *b == reported)
                }
                DivergenceEvidence::RootMismatch { copy, .. } => *copy == CopyId(2),
            },
            _ => false,
        })
        .unwrap_or_else(|| panic!("F1 quarantined copy 2: {notes:?}"));
    assert!(
        proven_2 < quarantine,
        "F1 judged the proof the harness routed"
    );
    let refused_by: Vec<(NodeId, ModuleName)> = run
        .trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module,
                note:
                    KernelNote::RecoveryFact {
                        effect: RecoveryEffect::Quarantine(_),
                    },
                ..
            } => Some((event.node, *module)),
            _ => None,
        })
        .collect();
    assert_eq!(refused_by, vec![(NODE, ModuleName::Recovery)], "F1, not R1");
    assert_eq!(run.partition_cas(), 1, "no activation");
    assert_eq!(
        run.phase,
        Some(rdb_core::recovery::RecoveryPhase::Quarantined)
    );
}

/// Never fabricated: copy 2's engine is applied through the cutoff in the new generation but
/// stores no `History` record there. Its sync is withheld with `NoDigest`, and Rebuilding stays
/// open with no activation.
///
/// Planted by a lost write (lead ruling B-R58d): node 3 lands empty (B-R58c) and R1 catches it
/// up, so its receiver holds the cutoff. Then [`StorageOp::LoseRecord`] loses the record R1
/// wrote there, at the first sync after it was written: applied, reported durable, not there.
#[retcd_test]
fn m7b_137_sync_after_commit_a_rebuilt_copy_with_no_record_at_the_cutoff_is_withheld() {
    use rdb_core::contracts::trace::SyncWithheldReason;
    support::preamble();
    let run = run_rebuild_faulted(
        Vec::new(),
        Some(StorageOp::LoseRecord {
            node: NodeId(3),
            partition: PartitionId(1),
            generation: REBUILT,
            seq: Seq(SPINE_HEAD),
        }),
    );
    assert_eq!(run.copy_2_base, Seq(0), "node 3 landed empty (B-R58c)");
    let history = spine_history();
    assert_eq!(
        run.copy_2_head,
        Some((SPINE_HEAD, history.digest(SPINE_HEAD))),
        "R1 caught copy 2 up through the cutoff"
    );
    let sent: Vec<Option<Bytes>> = (1..SPINE_HEAD)
        .map(|seq| {
            rdb_sim::storage::history::history_writes(history.batch(seq), Seq(seq))
                .map(|(record, _)| record)
        })
        .chain([None])
        .collect();
    assert_eq!(
        run.copy_2_records, sent,
        "every record R1 sent below the cutoff, and none at it"
    );
    let notes = run.sync_notes();
    assert!(
        notes.contains(&KernelNote::SyncWithheld {
            copy: CopyId(2),
            cutoff: Seq(SPINE_HEAD),
            reason: SyncWithheldReason::NoDigest,
        }),
        "{notes:?}"
    );
    assert!(
        !run.proven().contains(&(CopyId(2), Seq(SPINE_HEAD))),
        "no proof for copy 2"
    );
    assert_eq!(run.partition_cas(), 1, "no activation");
    assert_eq!(
        run.phase,
        Some(rdb_core::recovery::RecoveryPhase::Rebuilding)
    );
}

/// No borrowed durability (tester attack (c) on M7B-137): a new generation's sync through its
/// inherited base flushes that base where it lives, in the parent lineage on the **same**
/// engine. Here generation 1 committed 1..=3 and never synced; generation 2 inherits through 2,
/// commits its own 3, and syncs through 3. Generation 1 is then durable through 2, not 3: its
/// suffix above the base is not generation 2's to flush. After a host crash generation 2 still
/// reads its record at 2. Another
/// engine holding the same history is not touched.
#[retcd_test]
fn m7b_137_sync_after_commit_a_new_generation_sync_flushes_its_inherited_base_on_the_same_engine() {
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq};
    use rdb_core::contracts::storage::CapturedPrefix;
    use rdb_sim::storage::crash_image::CrashImage;
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    let prior = send_history(3);
    let mut engine = MemoryEngine::new(NODE);
    let mut other = MemoryEngine::new(NodeId(2));
    for batch in prior.batches.clone() {
        engine.commit(batch.clone()).expect("a commit");
        other.commit(batch).expect("a commit");
    }
    engine
        .inherit(PartitionId(1), Generation(1), Generation(2), Seq(2))
        .expect("generation 2 starts at 2");
    assert_eq!(
        engine.durable(PartitionId(1), Generation(1)),
        DurableSeq(0),
        "precondition: the parent never synced"
    );
    let next = send_history_after(2, &prior, 2, 3);
    engine
        .commit(next.batch(3).clone())
        .expect("generation 2's own record 3");
    engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: PartitionId(1),
            generation: Generation(2),
            through: AppliedSeq(3),
        }])
        .expect("a clean flush");
    assert_eq!(engine.durable(PartitionId(1), Generation(2)), DurableSeq(3));
    assert_eq!(
        engine.durable(PartitionId(1), Generation(1)),
        DurableSeq(2),
        "the base was flushed where it lives, and nothing above it"
    );
    assert_eq!(
        other.durable(PartitionId(1), Generation(1)),
        DurableSeq(0),
        "no other engine moved"
    );
    let reopened = CrashImage::of(&engine, StorageFault::HostCrash)
        .expect("a crash image")
        .reopen(NODE);
    assert_eq!(
        reopened.durable(PartitionId(1), Generation(2)),
        DurableSeq(3)
    );
    assert_eq!(
        reopened.base(PartitionId(1), Generation(2)),
        Seq(2),
        "a host crash keeps the whole base: the parent synced it"
    );
    assert!(
        reopened
            .history_at(PartitionId(1), Generation(2), Seq(2))
            .is_some(),
        "what generation 2 claims durable, it can still read"
    );
}

// ------------------------------------------------------------------------------------------
// B-R57a (tester-kb-r1's B-R57 gate): the crash seam on the send provider (F5), the helper's
// shape against a batch T1 really commits (F6), and rows for the guards only the tester's probes
// caught (D01, D03, D04, D05, H01, H02, M01). The probes s01-s13 are the templates.
// ------------------------------------------------------------------------------------------

/// This test's own log lines that contain `needle`.
fn own_log_lines(method: &str, needle: &str) -> Vec<String> {
    fn walk(dir: &std::path::Path, file: &str, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, file, out);
            } else if path.file_name().and_then(|name| name.to_str()) == Some(file) {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&test_log_dir(), &format!("{method}.jsonl"), &mut files);
    assert!(!files.is_empty(), "no log file for {method}");
    files
        .iter()
        .flat_map(|path| {
            std::fs::read_to_string(path)
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|line| line.contains(needle))
        .collect()
}

/// Node 2's applied head for partition 1.
fn node2_head(runner: &Runner) -> Seq {
    runner
        .dispatcher()
        .replication()
        .receiver(NodeId(2), PartitionId(1))
        .expect("node 2's receiver")
        .applied_head()
        .seq
}

/// The `History` record `batch` wrote at its own sequence.
fn record_of(batch: &rdb_core::contracts::storage::Batch) -> Bytes {
    rdb_sim::storage::history::history_writes(batch, batch.seq)
        .expect("a history write")
        .0
}

/// Replace `batch`'s `History` value and, when given, its `Progress` value.
fn rewrite_record(
    batch: &mut rdb_core::contracts::storage::Batch,
    record: &Bytes,
    progress: Option<Bytes>,
) {
    use rdb_core::contracts::storage::Namespace;
    for write in &mut batch.writes {
        if write.ns == Namespace::History {
            write.value = Some(record.clone());
        }
        if write.ns == Namespace::Progress {
            if let Some(progress) = &progress {
                write.value = Some(progress.clone());
            }
        }
    }
}

/// Hand node 1 `SendEnvelopes { copy 1, from..=through }` directly and return the frame bodies
/// scheduled for node 2, in order. R1 itself names one record at a time; a range pins the
/// provider's rule for when it does not.
fn sent_for(runner: &mut Runner, from: u64, through: u64) -> Vec<Bytes> {
    use rdb_core::contracts::transport::TransportEvent;
    let mut scheduler = Scheduler::new();
    runner
        .dispatcher_mut()
        .deliver(
            NODE,
            BOOT,
            vec![Effect {
                correlation: CorrelationId(9),
                from: ModuleName::Replication,
                partition: PartitionId(1),
                kind: EffectKind::Kernel(KernelEffect::SendEnvelopes {
                    copy: CopyId(1),
                    from: Seq(from),
                    through: Seq(through),
                }),
            }],
            &mut ControlStore::new(),
            &mut scheduler,
        )
        .expect("the provider answers");
    let mut bodies = Vec::new();
    while let Some(event) = scheduler.pop() {
        if let EventKind::Transport(TransportEvent::Delivered { frame, .. }) = event.kind {
            assert_eq!(event.node, NodeId(2));
            bodies.push(frame.body);
        }
    }
    bodies
}

/// F5 (s02b inverted): records 1..=3 committed on node 1, only 1..=2 synced, then a host crash
/// and no restart. The crash image keeps 1..=2. A crashed primary sends nothing: the runner does
/// not step a down node, so what is addressed to it is dropped, the provider is never reached,
/// and the pre-crash engine's record 3 is never served. Until M7V-114 the node was stepped and
/// the provider was refused at the crash seam instead.
#[retcd_test]
fn send_envelopes_a_crashed_primary_sends_nothing() {
    use rdb_core::contracts::storage::StoreEffect;
    support::preamble();
    let history = send_history(3);
    let preloads = history
        .batches
        .iter()
        .cloned()
        .map(|batch| (NODE, batch))
        .collect();
    let mut runner = send_runner_on(3, preloads, Some(2));
    runner
        .dispatcher_mut()
        .inject_storage(StorageOp::Crash {
            node: NODE,
            fault: StorageFault::HostCrash,
        })
        .expect("a planned crash");
    let taken = runner.dispatcher_mut().deliver(
        NODE,
        BOOT,
        vec![Effect {
            correlation: CorrelationId(9),
            from: ModuleName::Replication,
            partition: PartitionId(1),
            kind: EffectKind::Store(StoreEffect::Release {
                handle: SnapshotHandle(99),
            }),
        }],
        &mut ControlStore::new(),
        &mut Scheduler::new(),
    );
    taken.expect("the crash is taken, a fault and not a refusal");
    assert!(runner.dispatcher().is_down(NODE), "the primary is down");
    let report = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail");
    tracing::info!(stop = ?report.stop, "send from a crashed primary");
    let image = runner.dispatcher().crash_image(NODE).expect("still down");
    assert!(
        image
            .reopen(NODE)
            .history_at(PartitionId(1), Generation(1), Seq(3))
            .is_none(),
        "precondition: the crash image lost record 3"
    );
    let received = runner.dispatcher().engine(NodeId(2)).map_or(0, |engine| {
        (1..=3)
            .filter(|seq| {
                engine
                    .history_at(PartitionId(1), Generation(1), Seq(*seq))
                    .is_some()
            })
            .count()
    });
    assert_eq!(received, 0, "nothing was sent from the crashed node");
    assert_eq!(node2_head(&runner), Seq(0));
    // A down node is not stepped (M7V-114): what was addressed to it is dropped, so the provider
    // is never reached, and the run drains rather than stopping at the crash seam.
    assert!(
        runner.dispatcher().dropped().iter().any(|dropped| matches!(
            dropped,
            rdb_sim::harness::dispatch::Dropped::Event {
                event,
                reason: rdb_sim::harness::dispatch::DropReason::NodeDown,
            } if event.node == NODE
        )),
        "what reached the crashed node was dropped: {:?}",
        runner.dispatcher().dropped()
    );
    assert!(
        matches!(report.stop, rdb_sim::harness::run::StopReason::QueueEmpty),
        "the run drains; nothing steps the crashed node into its seam: {:?}",
        report.stop
    );
}

/// F5's twin, for a crash met by `SendEnvelopes` (tester D5): a crash planned on the primary and
/// not yet taken is taken when `SendEnvelopes` reaches it, before any record is read. F5 cannot
/// see this line, because its crash is taken by a storage effect first and the runner never
/// steps a down node.
///
/// Re-pointed 2026-10-02 (team i1, crash seam closed): the crash is taken by the delivery loop
/// before the provider runs, not inside it, and it is a fault rather than a refusal: the effect
/// is dropped as the dead process's. Renamed from `..._is_taken_by_the_provider_itself`, which
/// stopped being true.
#[retcd_test]
fn send_envelopes_a_planned_crash_is_taken_before_any_record_is_read() {
    support::preamble();
    let history = send_history(3);
    let preloads = history
        .batches
        .iter()
        .cloned()
        .map(|batch| (NODE, batch))
        .collect();
    let mut runner = send_runner_on(3, preloads, Some(2));
    runner
        .dispatcher_mut()
        .inject_storage(StorageOp::Crash {
            node: NODE,
            fault: StorageFault::HostCrash,
        })
        .expect("a planned crash");
    let mut scheduler = Scheduler::new();
    let taken = runner.dispatcher_mut().deliver(
        NODE,
        BOOT,
        vec![Effect {
            correlation: CorrelationId(9),
            from: ModuleName::Replication,
            partition: PartitionId(1),
            kind: EffectKind::Kernel(KernelEffect::SendEnvelopes {
                copy: CopyId(1),
                from: Seq(1),
                through: Seq(3),
            }),
        }],
        &mut ControlStore::new(),
        &mut scheduler,
    );
    taken.expect("the crash is taken, a fault and not a refusal");
    assert!(
        runner.dispatcher().crash_image(NODE).is_some(),
        "node 1 is down"
    );
    assert!(
        matches!(
            runner.dispatcher().dropped().last(),
            Some(Dropped::Effects {
                node: NODE,
                reason: DropReason::NodeDown,
                ..
            })
        ),
        "the send is dropped as the dead process's"
    );
    assert_eq!(scheduler.queued(), 0, "no frame was sent");
}

/// D01 (s01): node 3's engine holds record 3 and the primary's does not. Only the sending node's
/// engine is read, so the copy stops at 2 and the warn names node 1.
#[retcd_test]
fn send_envelopes_only_the_primarys_own_engine_is_read() {
    support::preamble();
    let history = send_history(3);
    let mut preloads: Vec<_> = history.batches[..2]
        .iter()
        .cloned()
        .map(|batch| (NODE, batch))
        .collect();
    preloads.extend(
        history
            .batches
            .iter()
            .cloned()
            .map(|batch| (NodeId(3), batch)),
    );
    let mut runner = send_runner_on(3, preloads, None);
    let report = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail");
    tracing::info!(stop = ?report.stop, "run stop");
    assert_eq!(node2_head(&runner), Seq(2));
    let warns = own_log_lines(
        "send_envelopes_only_the_primarys_own_engine_is_read",
        "no record at this sequence",
    );
    assert_repeats_one_refusal(&warns, (1, 3, None), &report);
}

/// B-R67b: R1's retransmit re-sends an unanswered record every `RETRANSMIT_MS` and is not
/// capped, so a record the provider cannot send is refused, and warned about, once per fire.
/// A row therefore cannot count one warn. It checks that there is at least one, that every one
/// is the same refusal (`node`, `seq`, and `fault`, or no `fault` for a missing record), and
/// that there are no more than one per retransmit period over the time the run took, plus the
/// first send. A provider or cursor that re-sends faster than R1's period breaks the bound.
fn assert_repeats_one_refusal(
    warns: &[String],
    (node, seq, fault): (u64, u64, Option<&str>),
    report: &rdb_sim::harness::run::RunReport,
) {
    use rdb_core::replication::catchup::RETRANSMIT_MS;
    assert!(!warns.is_empty(), "the refusal is warned at least once");
    for line in warns {
        let value: serde_json::Value = serde_json::from_str(line).expect("a JSON log line");
        assert_eq!(
            (
                value["node"].as_u64(),
                value["seq"].as_u64(),
                value["fault"].as_str()
            ),
            (Some(node), Some(seq), fault),
            "every warn is the same refusal: {line}"
        );
    }
    // The run's clock starts at tick 0 and a tick is a millisecond.
    let elapsed = report.last_tick.0;
    let bound = elapsed / RETRANSMIT_MS + 1;
    let count = u64::try_from(warns.len()).expect("a count");
    assert!(
        count <= bound,
        "{count} warns in {elapsed} ms: more than one per {RETRANSMIT_MS} ms retransmit, plus \
         the first send ({bound})"
    );
}

/// D03, D04 (s12): a range stops at the first record it cannot send (a hole at 2, or a record
/// failing its check at 2) though 3 is held and valid. A whole range sends every stored value
/// unchanged, byte for byte.
#[retcd_test]
fn send_envelopes_a_range_stops_at_a_hole_or_a_failed_record() {
    support::preamble();
    let history = send_history(3);
    let holed = vec![history.batches[0].clone(), history.batches[2].clone()];
    let bodies = sent_for(&mut send_runner(3, holed), 1, 3);
    assert_eq!(bodies, vec![record_of(&history.batches[0])], "a hole at 2");

    let mut failed = send_history(3).batches;
    let three = record_of(&failed[2]);
    rewrite_record(&mut failed[1], &three, None);
    let bodies = sent_for(&mut send_runner(3, failed.clone()), 1, 3);
    assert_eq!(bodies, vec![record_of(&failed[0])], "a failed record at 2");

    let bodies = sent_for(&mut send_runner(3, history.batches.clone()), 1, 3);
    let stored: Vec<Bytes> = history.batches.iter().map(record_of).collect();
    assert_eq!(bodies, stored, "the frame bodies are the stored values");
}

/// D05 (s13): the read is in the primary's own generation. Node 1's engine holds generations 1
/// and 2 at 1..=3 with different bytes; a primary leading generation 2 sends generation 2's.
/// Generation 2 here is committed without inheriting, so it is a lineage cut at the root, and
/// [`send_history`]'s root chain is the right start for it (B-R57b).
#[retcd_test]
fn send_envelopes_the_read_is_in_the_primarys_own_generation() {
    use rdb_core::contracts::envelope::ReplicaProgress;
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, ReceivedSeq};
    use rdb_core::replication::progress::{DigestLadder, ProgressTracker, TrackerInit};
    support::preamble();
    let second_lineage = rdb_core::contracts::authority::Lineage {
        generation: Generation(2),
        ..send_lineage()
    };
    let first = send_history(3);
    let second = rdb_sim::storage::history::canonical_history(second_lineage, ConfigVersion(1), 3)
        .expect("a canonical history");
    let mut stored = first.batches.clone();
    stored.extend(second.batches.clone());
    let mut runner = send_runner(3, stored);
    let mut ladder = DigestLadder::new();
    for seq in 0..=3 {
        ladder.insert(Seq(seq), second.digest(seq));
    }
    let tracker = ProgressTracker::new(TrackerInit {
        config: support::rf3_config(),
        own: CopyId(0),
        lineage: second_lineage,
        history: ladder,
        local: ReplicaProgress {
            received: ReceivedSeq(3),
            buffered_applied: AppliedSeq(3),
            durable: DurableSeq(3),
        },
    })
    .expect("a generation-2 primary");
    runner
        .dispatcher_mut()
        .replication_mut()
        .install_primary(tracker);
    let want: Vec<Bytes> = second.batches.iter().map(record_of).collect();
    assert_ne!(
        want,
        first.batches.iter().map(record_of).collect::<Vec<_>>(),
        "precondition: the generations' bytes differ"
    );
    assert_eq!(sent_for(&mut runner, 1, 3), want);
}

/// H01 (s03): record 2's user value is changed and re-encoded under its old carried digest, and
/// its progress record still names that digest. Only recomputing the digest refuses it.
#[retcd_test]
fn send_envelopes_a_tampered_value_under_its_old_digest_is_not_sent() {
    use rdb_core::contracts::envelope::ReplicationEnvelope;
    support::preamble();
    let mut stored = send_history(3).batches;
    let mut envelope = ReplicationEnvelope::decode(&record_of(&stored[1])).expect("decodes");
    envelope.mutations[0].value = Some(Bytes::from_static(b"tampered"));
    let bytes = envelope.encode().expect("encodes");
    rewrite_record(&mut stored[1], &bytes, None);
    let mut runner = send_runner(3, stored);
    let report = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail");
    tracing::info!(stop = ?report.stop, "run stop");
    assert_eq!(node2_head(&runner), Seq(1));
    let warns = own_log_lines(
        "send_envelopes_a_tampered_value_under_its_old_digest_is_not_sent",
        "failed its check",
    );
    assert_repeats_one_refusal(&warns, (1, 2, Some("DigestMismatch")), &report);
}

/// H02 (s04): key 2 holds record 3's bytes and its progress record is forged to (2, digest 3).
/// Only the sequence check refuses it.
#[retcd_test]
fn send_envelopes_a_record_under_the_wrong_key_is_not_sent() {
    support::preamble();
    let history = send_history(3);
    let mut stored = history.batches.clone();
    let three = record_of(&stored[2]);
    let mut forged = 2u64.to_le_bytes().to_vec();
    forged.extend_from_slice(&history.digest(3).0);
    rewrite_record(&mut stored[1], &three, Some(Bytes::from(forged)));
    let mut runner = send_runner(3, stored);
    let report = runner
        .run(RunLimits::SMALL)
        .expect("the run itself does not fail");
    tracing::info!(stop = ?report.stop, "run stop");
    assert_eq!(node2_head(&runner), Seq(1));
    let warns = own_log_lines(
        "send_envelopes_a_record_under_the_wrong_key_is_not_sent",
        "failed its check",
    );
    assert_repeats_one_refusal(&warns, (1, 2, Some("WrongSeq")), &report);
}

/// M01 (s07): across two inheritances (generation 2 from 1 at 2, generation 3 from 2 at 4), a
/// lineage shows nothing above its base until it writes its own record there, and never a
/// predecessor's discarded suffix.
#[retcd_test]
fn history_at_shows_nothing_above_a_base_through_two_inheritances() {
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    // Each generation chains from its predecessor's digest at its cutoff (B-R57b).
    let first = send_history(5);
    let second = send_history_after(2, &first, 2, 5);
    let third = send_history_after(3, &second, 4, 6);
    let mut engine = MemoryEngine::new(NODE);
    for batch in first.batches.iter().cloned() {
        engine.commit(batch).expect("generation 1");
    }
    engine
        .inherit(PartitionId(1), Generation(1), Generation(2), Seq(2))
        .expect("generation 2 at 2");
    let shows = |engine: &MemoryEngine, generation: u64, seq: u64| {
        engine
            .history_at(PartitionId(1), Generation(generation), Seq(seq))
            .map(|(record, _)| record)
    };
    for seq in 3..=5 {
        assert_eq!(shows(&engine, 2, seq), None, "generation 2 at {seq}");
    }
    for batch in second.batches.iter().cloned() {
        engine.commit(batch).expect("generation 2");
    }
    engine
        .inherit(PartitionId(1), Generation(2), Generation(3), Seq(4))
        .expect("generation 3 at 4");
    engine
        .commit(third.batch(6).clone())
        .expect("generation 3's own 6");
    let own = |history: &rdb_sim::storage::history::CanonicalHistory, seq: u64| {
        Some(record_of(history.batch(seq)))
    };
    assert_eq!(shows(&engine, 3, 1), own(&first, 1));
    assert_eq!(shows(&engine, 3, 2), own(&first, 2));
    assert_eq!(shows(&engine, 3, 3), own(&second, 3));
    assert_eq!(shows(&engine, 3, 4), own(&second, 4));
    assert_eq!(shows(&engine, 3, 5), None, "every discarded 5");
    assert_eq!(shows(&engine, 3, 6), own(&third, 6));
}

/// F6 (s09): `canonical_history` against a batch T1 really commits. One T1, live at
/// generation 7 cut at 0, is given the request the helper documents for record 1; its batch is
/// set beside the helper's record 1 for the same lineage and configuration. The writes (each
/// namespace, key and value, the History record's bytes included) are equal; the batch id
/// carries T1's tag. The request is built here, not taken from the helper, so the two cannot
/// drift together.
#[retcd_test]
fn canonical_history_writes_what_t1_commits() {
    use rdb_core::contracts::authority::{
        AuthorityDecision, AuthorityEvent, AuthorityView, Checkpoint, DenyReason, FencingProof,
        Lineage, PartitionMode, Revocation, Verdict,
    };
    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::event::{ClientEvent, Event, KernelEvent, Module, StepCtx};
    use rdb_core::contracts::ids::{
        AffinityId, AuthorityGeneration, ClientId, GrantId, RequestId, RequestIdentity, Revision,
        TenantId,
    };
    use rdb_core::contracts::membership::{Member, PartitionConfig};
    use rdb_core::contracts::protection::{AdmissionState, ReplicationLag};
    use rdb_core::contracts::recovery::{
        CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap,
        SelectedLineage,
    };
    use rdb_core::contracts::storage::{Namespace, SnapshotRead, StoreEffect};
    use rdb_core::contracts::time::ControlTime;
    use rdb_core::contracts::trace::Version;
    use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
    use rdb_core::contracts::version::API_VERSION;
    use rdb_core::transaction::{Transaction, BATCH_TAG};

    /// An empty partition: T1 seeds its dedup index from it and finds nothing.
    struct Empty;
    impl SnapshotRead for Empty {
        fn handle(&self) -> SnapshotHandle {
            SnapshotHandle(0)
        }
        fn at(&self) -> Seq {
            Seq(0)
        }
        fn generation(&self) -> Generation {
            Generation(7)
        }
        fn get(&self, _ns: Namespace, _key: &[u8]) -> Option<Bytes> {
            None
        }
        fn version(&self, _ns: Namespace, _key: &[u8]) -> Option<Version> {
            None
        }
        fn scan(&self, _ns: Namespace, _from: &[u8], _limit: usize) -> Vec<(Bytes, Bytes)> {
            Vec::new()
        }
    }

    support::preamble();
    const GEN: Generation = Generation(7);
    const C1: ConfigVersion = ConfigVersion(1);
    let lineage_at = |generation: Generation| Lineage {
        partition: PartitionId(1),
        generation,
        owner_epoch: OwnerEpoch(1),
    };
    // Grant 1, so T1's lease id is the helper's `LeaseId(1)`.
    let view = AuthorityView {
        lineage: lineage_at(GEN),
        grant_id: GrantId(1),
        boot_id: BOOT,
        authority_generation: AuthorityGeneration(1),
        config_version: C1,
        authority_seq: 1,
        valid_through_tick: Tick(u64::MAX),
        past_horizon: DenyReason::Expired,
    };
    let member = |copy: u8, node: u32, role| Member {
        copy: CopyId(copy),
        node: NodeId(node),
        boot: BOOT,
        role,
    };
    let config = PartitionConfig::new(
        PartitionId(1),
        C1,
        vec![
            member(1, 1, ReplicaRole::Primary),
            member(2, 2, ReplicaRole::RegularSecondary),
            member(3, 3, ReplicaRole::RegularSecondary),
        ],
    );
    let prior = Generation(GEN.0 - 1);
    let recovered = RecoveryResult {
        fenced_prior: FencingProof {
            partition: PartitionId(1),
            prior_generation: prior,
            prior_owner_epoch: OwnerEpoch(1),
            prior_grant_id: GrantId(1),
            prior_boot_id: BOOT,
            revocation: Revocation::DurableDrain {
                ack_revision: Revision(1),
            },
            control_revision: Revision(1),
            decision_tick: Tick::ZERO,
        },
        inventories: Vec::new(),
        selected: SelectedLineage {
            root: lineage_at(prior),
            cutoff_seq: Seq(0),
            cutoff_digest: Digest::ROOT,
            source: CopyId(1),
        },
        new_generation: GEN,
        mode: PartitionMode::Active,
        barrier: RecoveryBarrier::try_new(&[], &Default::default(), Seq(0), Digest::ROOT)
            .expect("an empty required set needs no proof"),
        loss: LossRecord {
            queried: Vec::new(),
            unavailable: Vec::new(),
            cutoff_seq: Seq(0),
            highest_advertised_seq: Seq(0),
            uncertain: false,
        },
        committed: CommittedRoot {
            revision: Revision(2),
            authority_view: view,
            pinned_config: config,
        },
        retained_status_map: RetainedStatusMap {
            predecessor_generation: prior,
            predecessor_cutoff: Seq(0),
            retained_through: Seq(0),
            discarded_from: None,
            uncertain: false,
        },
    };
    let event = |kind: EventKind| Event {
        id: EventId(0),
        at: Tick::ZERO,
        node: NODE,
        boot: BOOT,
        partition: PartitionId(1),
        correlation: CorrelationId(42),
        kind,
    };
    let budgets = Budgets::SPEC_DEFAULTS;
    let snapshot = Empty;
    let ctx = StepCtx {
        now: Tick(10),
        control_time: ControlTime {
            estimate: Tick(10),
            error_millis: 0,
            bound_established: true,
            sampled_at: Tick(10),
        },
        node: NODE,
        boot: BOOT,
        partition: PartitionId(1),
        generation: GEN,
        owner_epoch: OwnerEpoch(1),
        config_version: C1,
        snapshot: &snapshot,
        budgets: &budgets,
    };
    let mut t1 = Transaction::new();
    let mut step = |kind: EventKind| -> Vec<EffectKind> {
        t1.step(&ctx, &event(kind))
            .expect("a T1 input is never declined")
            .into_iter()
            .map(|effect| effect.kind)
            .collect()
    };
    assert_eq!(
        step(EventKind::Kernel(KernelEvent::Recovered(Box::new(
            recovered
        )))),
        vec![]
    );
    assert_eq!(
        step(EventKind::Kernel(KernelEvent::SetAdmission(
            AdmissionState {
                allow: true,
                reason: None,
                oldest_unsafe_age: 0,
                oldest_unsafe_seq: Seq::ZERO,
                replication_lag: ReplicationLag::ZERO,
                stalest_copy: None,
                lost_copies: Vec::new(),
                paused_prefix: Seq(4),
                resume_barrier: Seq::ZERO,
                required_config_versions: vec![C1],
                outstanding_unsafe_bytes: 0,
            }
        ))),
        vec![]
    );
    // Record 1 as the helper documents it: tenant 1, client 1, request 1, affinity 1, one put
    // of the sequence (big-endian) at the scoped key `k`.
    let request = TxnRequest {
        api_version: API_VERSION,
        identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(1),
        },
        affinity: AffinityId(1),
        expected_generation: None,
        remaining_millis: 1_000,
        conditions: Vec::new(),
        mutations: vec![Mutation::Put {
            key: scoped_key(TenantId(1), AffinityId(1), b"k"),
            value: Bytes::copy_from_slice(&1u64.to_be_bytes()),
            expected_version: None,
        }],
    };
    let effects = step(EventKind::Client(ClientEvent::Submit(request)));
    let [EffectKind::Kernel(KernelEffect::AuthorityCheck {
        checkpoint: Checkpoint::StorageDispatch,
        correlation,
        ..
    })] = effects.as_slice()
    else {
        panic!("one StorageDispatch check: {effects:?}");
    };
    let effects = step(EventKind::Kernel(KernelEvent::Authority(
        AuthorityEvent::Answer(AuthorityDecision {
            owner: NODE,
            boot: BOOT,
            grant: GrantId(1),
            authority_generation: AuthorityGeneration(1),
            lineage: lineage_at(GEN),
            expiry_utc_ms: 0,
            decided_at: Tick::ZERO,
            authority_seq: 1,
            checkpoint: Checkpoint::StorageDispatch,
            correlation: *correlation,
            verdict: Verdict::Admit,
        }),
    )));
    let [EffectKind::Store(StoreEffect::Commit(committed))] = effects.as_slice() else {
        panic!("one batch: {effects:?}");
    };

    let canonical = rdb_sim::storage::history::canonical_history(lineage_at(GEN), C1, 1)
        .expect("a canonical history")
        .batches
        .remove(0);
    let shape = |batch: &rdb_core::contracts::storage::Batch| {
        batch
            .writes
            .iter()
            .map(|write| (write.ns, write.key.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        shape(&canonical),
        shape(committed),
        "the same namespaces and keys, in the same order"
    );
    assert_eq!(
        canonical.writes, committed.writes,
        "the same values, the History record's bytes included"
    );
    assert_eq!(
        (canonical.partition, canonical.generation, canonical.seq),
        (committed.partition, committed.generation, committed.seq)
    );
    let tag = |id: rdb_core::contracts::ids::BatchId| id.0 >> 56;
    assert_eq!(tag(committed.id), BATCH_TAG >> 56);
    assert_eq!(tag(canonical.id), tag(committed.id), "T1's batch tag");
}

// ------------------------------------------------------------------------------------------
// B-R57b (tester-kb-r1 F7): a lineage inherited at a cutoff above 0 chains its first own record
// from the predecessor's digest at the cutoff. `canonical_history_from` takes that start; a
// history started anywhere else is refused by R1's own receiver, so the helper cannot pass a
// wrong chain off as a provider or R1 defect.

/// Node 2 (copy 1) receiving generation 2 of [`send_lineage`], its head at the inherited cutoff
/// `(2, digest)`: a real R1 receiver, as a member at the cutoff builds it.
fn receiver_at_cutoff(
    digest: rdb_core::contracts::digest::Digest,
) -> rdb_core::replication::append::AppendReceiver {
    use rdb_core::contracts::ids::DurableSeq;
    use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
    AppendReceiver::new(ReceiverInit {
        config: support::rf3_config(),
        own: CopyId(1),
        lineage: rdb_core::contracts::authority::Lineage {
            generation: Generation(2),
            ..send_lineage()
        },
        head: Head {
            seq: Seq(2),
            digest,
        },
        durable: DurableSeq(2),
    })
    .expect("a receiver")
}

/// `record` offered to [`receiver_at_cutoff`] by node 1, leading generation 2: the receiver's
/// accept head afterwards, and the effects it emitted.
fn offer_at_cutoff(
    cutoff: rdb_core::contracts::digest::Digest,
    record: Bytes,
) -> (rdb_core::replication::append::Head, Vec<EffectKind>) {
    let mut receiver = receiver_at_cutoff(cutoff);
    let frame = Frame {
        id: MessageId(1),
        protocol: rdb_core::contracts::version::ENVELOPE_VERSION,
        config: ConfigVersion(1),
        sender: rdb_core::contracts::authority::Lineage {
            generation: Generation(2),
            ..send_lineage()
        },
        body: record,
    };
    let from = PeerLabel {
        node: NODE,
        boot: BOOT,
        authenticated: true,
    };
    let effects = receiver.on_append(&from, &frame);
    (receiver.accept_head(), effects)
}

/// s15 (B-R57b): generation 2 inherits generation 1 at 2 and writes its own record 3 with
/// [`send_history_after`]. The record chains from generation 1's digest at 2, and a real receiver
/// whose head is that cutoff takes it.
#[retcd_test]
fn canonical_history_from_an_inherited_cutoff_is_taken_by_a_receiver_at_that_cutoff() {
    use rdb_core::contracts::envelope::ReplicationEnvelope;
    use rdb_core::replication::append::Head;
    support::preamble();
    let first = send_history(3);
    let next = send_history_after(2, &first, 2, 3);
    assert_eq!(next.start, Seq(2));
    assert_eq!(next.batches.len(), 1, "only generation 2's own record");
    let record = record_of(next.batch(3));
    let envelope = ReplicationEnvelope::decode(&record).expect("decodes");
    assert_eq!(
        envelope.prev_digest,
        first.digest(2),
        "chained from the predecessor's record at the cutoff"
    );
    assert_eq!(envelope.header.generation, Generation(2));
    let (head, effects) = offer_at_cutoff(first.digest(2), record);
    assert_eq!(
        head,
        Head {
            seq: Seq(3),
            digest: next.digest(3),
        },
        "{effects:?}"
    );
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, EffectKind::Kernel(KernelEffect::Alert { .. }))),
        "{effects:?}"
    );
}

/// The near miss for s15: the same generation-2 record 3, chained from generation 2's **own**
/// record 2 — what `canonical_history` built before B-R57b, a history started from the wrong
/// digest. Valid on its own terms (the provider's check passes it), and refused by the receiver:
/// `DivergentHistory { at: 3 }` with a `CorruptHistory` alert, its head left at the cutoff.
#[retcd_test]
fn canonical_history_from_the_wrong_digest_is_refused_by_a_receiver_at_that_cutoff() {
    use rdb_core::contracts::envelope::{AppendOutcome, AppendReject};
    use rdb_core::contracts::errors::ErrorKind;
    use rdb_core::contracts::transport::SendEffect;
    use rdb_core::replication::append::Head;
    support::preamble();
    let first = send_history(3);
    let wrong = rdb_sim::storage::history::canonical_history(
        rdb_core::contracts::authority::Lineage {
            generation: Generation(2),
            ..send_lineage()
        },
        ConfigVersion(1),
        3,
    )
    .expect("a canonical history");
    assert_ne!(
        wrong.digest(2),
        first.digest(2),
        "precondition: another chain start"
    );
    let (record, progress) =
        rdb_sim::storage::history::history_writes(wrong.batch(3), Seq(3)).expect("stored");
    assert!(
        rdb_sim::storage::history::verified_record(Seq(3), &record, progress.as_ref()).is_ok(),
        "precondition: the provider's check does not read the chain"
    );
    let (head, effects) = offer_at_cutoff(first.digest(2), record);
    assert_eq!(
        head,
        Head {
            seq: Seq(2),
            digest: first.digest(2),
        },
        "{effects:?}"
    );
    let replies: Vec<AppendOutcome> = effects
        .iter()
        .filter_map(|effect| match effect {
            EffectKind::Send(SendEffect::Unicast { frame, .. }) => {
                Some(rdb_core::replication::wire::decode_reply(&frame.body).expect("a reply"))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        replies,
        vec![AppendOutcome::Rejected(AppendReject::DivergentHistory {
            at: Seq(3)
        })]
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            EffectKind::Kernel(KernelEffect::Alert {
                reason: ErrorKind::CorruptHistory
            })
        )),
        "{effects:?}"
    );
}

/// Ruling B-R60, through the run loop: L1's `SetAdmission` is routed to R1 on the same node,
/// and R1's keepalive runs exactly while admission is rejected.
///
/// The spine. `Recovered` lands on node 1 and L1 goes live `Paused` — admission rejected at
/// birth. R1 took the `Recovered` first (it is offered to R1 ahead of L1), so a primary is there
/// to start the keepalive. From then on, every 100 ms, the primary sends its head to the two
/// regular secondaries, nodes 2 and 3, and never to the shadow, node 4. Their answers are what
/// let L1 resume. When L1 admits, R1 cancels the keepalive and node 1 sends nothing more.
///
/// Every event is offered to all six modules, so R1 would hear `SetAdmission` even unrouted.
/// What the route adds is the edge: R1 is a **named** consumer, offered straight after T1, so a
/// decline by R1 stops the run. The offer order is the part of that the trace shows, and it is
/// asserted here.
#[retcd_test]
fn route_l1s_admission_reaches_r1_whose_keepalive_runs_exactly_while_paused() {
    use rdb_core::replication::primary::KEEPALIVE_MS;
    support::preamble();
    let plan = spine_plan();
    let mut runner = Runner::new(&plan).expect("a runner");
    // (tick, keepalive version, node 1's sends so far), sampled every 5 ms.
    let mut samples = Vec::new();
    for tick in (0..=8_000).step_by(5) {
        let stop = runner
            .run(RunLimits {
                max_events: 100_000,
                deadline: Tick(tick),
            })
            .expect("the spine runs")
            .stop;
        assert!(
            matches!(
                stop,
                rdb_sim::harness::run::StopReason::DeadlineReached { .. }
            ),
            "the spine is work until each deadline, never a refusal: {stop:?}"
        );
        let dispatcher = runner.dispatcher();
        let keepalive = dispatcher
            .replication()
            .primary(NODE, PartitionId(1))
            .and_then(rdb_core::replication::primary::Primary::keepalive);
        samples.push((tick, keepalive, dispatcher.network().transmissions().len()));
    }
    let sent: Vec<(NodeId, NodeId)> = runner
        .dispatcher()
        .network()
        .transmissions()
        .iter()
        .map(|sent| (sent.from, sent.to))
        .collect();
    let trace = runner.finish().expect("a trace");

    let admissions: Vec<(u64, NodeId, bool)> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::SetAdmission { state },
                ..
            } => Some((event.logical_tick, event.node, state.allow)),
            _ => None,
        })
        .collect();
    let [(paused, NODE, false), (allowed, NODE, true)] = admissions[..] else {
        panic!("node 1 pauses at birth and later admits, once each: {admissions:?}");
    };

    // Each round: the first sample that shows a new version, and node 1's sends since the last.
    let mut rounds = Vec::new();
    for pair in samples.windows(2) {
        let [(_, before, from), (tick, after, to)] = pair else {
            unreachable!("windows of two");
        };
        if after.is_some() && after != before {
            let mut targets: Vec<NodeId> = sent[*from..*to]
                .iter()
                .filter(|(sender, _)| *sender == NODE)
                .map(|(_, target)| *target)
                .collect();
            targets.sort();
            rounds.push((*tick, *after, targets));
        }
    }
    assert!(rounds.len() >= 2, "the keepalive runs: {rounds:?}");
    assert!(
        rounds[0].0 >= paused && rounds[0].0 - paused < 5,
        "the first round is the pause itself: paused at {paused}, {rounds:?}"
    );
    for (index, (tick, version, targets)) in rounds.iter().enumerate() {
        assert_eq!(
            *version,
            Some(TimerVersion(u64::try_from(index + 1).expect("small"))),
            "one version per round"
        );
        assert_eq!(
            targets,
            &vec![NodeId(2), NodeId(3)],
            "round at {tick}: the head to each regular secondary, never to the shadow"
        );
        assert!(*tick <= allowed + 5, "no round after admission: {tick}");
    }
    assert!(
        rounds
            .windows(2)
            .all(|pair| pair[1].0 - pair[0].0 == KEEPALIVE_MS),
        "every {KEEPALIVE_MS} ms: {rounds:?}"
    );
    let after_allow: Vec<_> = samples
        .iter()
        .filter(|(tick, _, _)| *tick >= allowed + 5)
        .collect();
    assert!(
        after_allow
            .iter()
            .all(|(_, keepalive, _)| keepalive.is_none()),
        "admission cancels the keepalive"
    );
    let sends_then = after_allow.first().expect("samples after admission").2;
    assert!(
        sent[sends_then..].iter().all(|(sender, _)| *sender != NODE),
        "and node 1 sends nothing more: {:?}",
        &sent[sends_then..]
    );

    // The route: the two `SetAdmission` events are offered to T1, then R1, then the rest.
    let mut offers: std::collections::BTreeMap<EventId, (u64, Vec<(ModuleName, bool)>)> =
        std::collections::BTreeMap::new();
    for event in &trace.events {
        if let TraceKind::ModuleDispatch {
            event: id,
            module,
            outcome,
        } = &event.kind
        {
            offers
                .entry(*id)
                .or_insert_with(|| (event.logical_tick, Vec::new()))
                .1
                .push((*module, matches!(outcome, DispatchOutcome::Answered { .. })));
        }
    }
    let routed: Vec<(u64, bool)> = offers
        .values()
        .filter(|(_, offered)| {
            offered
                .iter()
                .map(|(module, _)| *module)
                .collect::<Vec<_>>()
                == [
                    ModuleName::Transaction,
                    ModuleName::Replication,
                    ModuleName::Authority,
                    ModuleName::Publication,
                    ModuleName::Protection,
                    ModuleName::Recovery,
                ]
        })
        .map(|(tick, offered)| (*tick, offered[1].1))
        .collect();
    assert_eq!(
        routed,
        vec![(paused, true), (allowed, true)],
        "each SetAdmission offered to T1 then R1, and R1 answered it"
    );
}

// ------------------------------------------------------------------------------------------
// F1's catch-up through a source that is not the primary (lead rulings B-R59, B-R59a): the
// request goes to the source's node, the source's records go out as recovery appends signed
// with the credential, and the source's `CopyCaughtUp` comes back to the F1 that asked.
// ------------------------------------------------------------------------------------------

/// A credential minted for copy `sender`, with a prior generation (2) and owner epoch (3) that no
/// lineage in these rows carries, so a frame signed from anything but the credential shows in
/// both fields. In a real recovery the source's receiver is at the credential's prior generation;
/// they differ here only to tell the two sources apart (tester-kb-r1 A2, survivor S03).
fn catch_up_credential(sender: u8) -> rdb_core::contracts::authority::FenceCredential {
    rdb_core::contracts::authority::FenceCredential {
        partition: PartitionId(1),
        prior_generation: Generation(2),
        prior_owner_epoch: OwnerEpoch(3),
        control_revision: rdb_core::contracts::ids::Revision(1),
        sender: CopyId(sender),
    }
}

/// B-R59 surface items 1 and 4. Both of F1's catch-up requests reach R1, as
/// `KernelEvent::CatchUp` with the same four fields, on the node the scenario placed the source
/// on, and are held to R1's answer. The source's `CopyCaughtUp` then goes back to the asking F1's
/// node, once; a second one from the same source has no asker left and stays on its own node.
/// A source the scenario never placed is refused by name, and nothing is scheduled.
#[retcd_test]
fn provider_catch_up_goes_to_the_node_holding_its_source() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::RecoveryEvent;
    support::preamble();
    let credential = catch_up_credential(1);
    let requests = [
        RecoveryEffect::CatchUp {
            from: CopyId(1),
            to: CopyId(2),
            through: Seq(2),
            credential,
        },
        RecoveryEffect::CatchUpBeforeGrant {
            from: CopyId(1),
            to: CopyId(2),
            through: Seq(2),
            credential,
        },
    ];
    for request in requests {
        let mut dispatcher = with_survivor();
        let mut scheduler = Scheduler::new();
        deliver_on(
            &mut dispatcher,
            &mut scheduler,
            NODE,
            effect_from(
                ModuleName::Recovery,
                EffectKind::Kernel(KernelEffect::Recovery(request.clone())),
            ),
        )
        .expect("a provider");
        let routed = scheduler.pop().expect("the request is routed");
        assert_eq!(
            (routed.at, routed.node, routed.boot, routed.partition),
            (Tick(0), PEER, PEER_BOOT, PartitionId(1)),
            "{request:?}: at once, to the source's node under its boot"
        );
        assert_eq!(routed.correlation, CorrelationId(1), "{request:?}");
        assert_eq!(
            routed.kind,
            EventKind::Kernel(KernelEvent::CatchUp {
                from: CopyId(1),
                to: CopyId(2),
                through: Seq(2),
                credential,
            }),
            "{request:?}"
        );
        assert!(dispatcher.take_routed(routed.id), "held to R1's answer");
        assert_eq!(scheduler.queued(), 0, "one request, one event");

        let caught_up = || {
            effect_from(
                ModuleName::Replication,
                EffectKind::Kernel(KernelEffect::CopyCaughtUp {
                    copy: CopyId(2),
                    head: Seq(2),
                    digest: digest(2),
                }),
            )
        };
        let answer = EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::CopyCaughtUp {
            copy: CopyId(2),
            head: Seq(2),
            digest: digest(2),
        }));
        deliver_on(&mut dispatcher, &mut scheduler, PEER, caught_up()).expect("routed");
        let back = scheduler.pop().expect("the answer is routed");
        assert_eq!(
            (back.node, back.boot, back.kind.clone()),
            (NODE, BOOT, answer.clone()),
            "{request:?}: to the F1 that asked"
        );
        assert!(dispatcher.take_routed(back.id), "held to F1's answer");
        deliver_on(&mut dispatcher, &mut scheduler, PEER, caught_up()).expect("routed");
        let again = scheduler.pop().expect("routed");
        assert_eq!(
            (again.node, again.kind),
            (PEER, answer),
            "{request:?}: the asker is answered once; a repeat stays on the source's node"
        );
    }

    let mut dispatcher = with_survivor();
    let mut scheduler = Scheduler::new();
    let unplaced = RecoveryEffect::CatchUp {
        from: CopyId(3),
        to: CopyId(2),
        through: Seq(2),
        credential: catch_up_credential(3),
    };
    assert_eq!(
        deliver_on(
            &mut dispatcher,
            &mut scheduler,
            NODE,
            effect_from(
                ModuleName::Recovery,
                EffectKind::Kernel(KernelEffect::Recovery(unplaced)),
            ),
        ),
        Err(SimError::Unavailable {
            seam: "harness::dispatch::deliver::recovery"
        }),
        "a source nobody placed has no node to go to"
    );
    assert_eq!(scheduler.queued(), 0, "and nothing is sent anywhere");
}

/// B-R59 surface item 2. R1's `SendRecoveryEnvelopes` on the source's node becomes one unicast
/// per record, to the node of the copy being caught up. Each body is
/// `wire::encode_recovery_append` over the credential and the source's own stored bytes, and
/// each `Frame.sender` is the credential's prior lineage, not the source receiver's. The records
/// are read in the source receiver's own generation. A range is inclusive: asking through 2 sends
/// 2 (tester-kb-r1 A1, survivor S04). A record the engine does not hold stops the range, as for
/// `SendEnvelopes`; a node with no receiver is refused by name.
#[retcd_test]
fn provider_send_recovery_envelopes_unicasts_stored_records_signed_by_the_credential() {
    use rdb_core::contracts::authority::Lineage;
    use rdb_core::contracts::ids::DurableSeq;
    use rdb_core::contracts::transport::TransportEvent;
    use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
    use rdb_core::replication::wire::{decode_recovery_append, RECOVERY_MAGIC};
    support::preamble();
    let history = send_history(2);
    let mut dispatcher = two_nodes();
    for batch in history.batches.clone() {
        dispatcher.preload(PEER, batch).expect("a preload");
    }
    let credential = catch_up_credential(1);
    let request = |through: u64| {
        effect_from(
            ModuleName::Replication,
            EffectKind::Kernel(KernelEffect::SendRecoveryEnvelopes {
                copy: CopyId(0),
                from: Seq(1),
                through: Seq(through),
                credential,
            }),
        )
    };
    let mut scheduler = Scheduler::new();
    assert_eq!(
        deliver_on(&mut dispatcher, &mut scheduler, PEER, request(2)),
        Err(SimError::Config {
            field: "send_recovery_envelopes"
        }),
        "no receiver on the source's node, so no configuration to address the copy by"
    );
    assert_eq!(scheduler.queued(), 0);

    dispatcher.replication_mut().install_receiver(
        AppendReceiver::new(ReceiverInit {
            config: support::rf3_config(),
            own: CopyId(1),
            lineage: send_lineage(),
            head: Head {
                seq: Seq(2),
                digest: history.digest(2),
            },
            durable: DurableSeq(2),
        })
        .expect("a receiver at 2"),
    );
    let stored: Vec<Bytes> = (1..=2)
        .map(|seq| {
            dispatcher
                .engine(PEER)
                .expect("an engine")
                .history_at(PartitionId(1), Generation(1), Seq(seq))
                .expect("a stored record")
                .0
        })
        .collect();
    let drain = |scheduler: &mut Scheduler| {
        let mut sent = Vec::new();
        while let Some(arrival) = scheduler.pop() {
            assert_eq!(
                (arrival.node, arrival.boot),
                (NODE, BOOT),
                "to copy 0's node"
            );
            let EventKind::Transport(TransportEvent::Delivered { from, frame }) = arrival.kind
            else {
                panic!("an arrival, not {:?}", arrival.kind);
            };
            assert_eq!((from.node, from.boot), (PEER, PEER_BOOT), "from the source");
            assert_eq!(
                frame.config,
                ConfigVersion(1),
                "the source receiver's configuration"
            );
            assert_eq!(
                frame.sender,
                Lineage {
                    partition: PartitionId(1),
                    generation: Generation(2),
                    owner_epoch: OwnerEpoch(3),
                },
                "signed with the credential's prior lineage"
            );
            assert_eq!(
                &frame.body[..4],
                RECOVERY_MAGIC,
                "a recovery append on the wire"
            );
            let (carried, envelope) =
                decode_recovery_append(&frame.body).expect("a recovery append");
            assert_eq!(carried, credential, "carrying the credential unchanged");
            sent.push(envelope);
        }
        sent
    };
    deliver_on(&mut dispatcher, &mut scheduler, PEER, request(2)).expect("served");
    assert_eq!(
        drain(&mut scheduler),
        stored,
        "through 2: records 1 and 2, in order, byte for byte, the last one included"
    );
    deliver_on(&mut dispatcher, &mut scheduler, PEER, request(3)).expect("served");
    assert_eq!(
        drain(&mut scheduler),
        stored,
        "through 3: the same two; nothing stands in for the missing 3"
    );
}

/// tester-kb-r1 A4 (S2): a catch-up R1 refuses leaves no asker behind. Node 2 holds copy 1's
/// placed survivor but serves no receiver, so R1 there refuses F1's `CatchUp` (`NotASource`)
/// and starts nothing. A later `CopyCaughtUp` from node 2 then answers no catch-up of F1 on
/// node 1, and goes to node 2's own F1 like any other.
#[retcd_test]
fn provider_a_refused_catch_up_leaves_no_asker_behind() {
    use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
    support::preamble();
    let mut plan = RunPlan::new(support::cluster());
    for seq in 1..=2 {
        plan.preloads
            .push((PEER, support::batch(1, seq, b"k", b"v")));
    }
    plan.survivors.push((PEER, PartitionId(1), survivor(1, 2)));
    let mut runner = Runner::new(&plan).expect("a runner");
    let kernel = |from: ModuleName, kind: KernelEffect| Effect {
        correlation: CorrelationId(1),
        from,
        partition: PartitionId(1),
        kind: EffectKind::Kernel(kind),
    };
    let limits = |deadline: u64| RunLimits {
        max_events: 50,
        deadline: Tick(deadline),
    };

    runner
        .carry_out(
            NODE,
            BOOT,
            vec![kernel(
                ModuleName::Recovery,
                KernelEffect::Recovery(RecoveryEffect::CatchUp {
                    from: CopyId(1),
                    to: CopyId(2),
                    through: Seq(2),
                    credential: catch_up_credential(1),
                }),
            )],
        )
        .expect("routed to node 2");
    let first = runner.run(limits(5)).expect("the run itself does not fail");
    assert!(
        matches!(first.stop, rdb_sim::harness::run::StopReason::QueueEmpty),
        "{:?}",
        first.stop
    );
    let refused = runner.recorded().iter().any(|event| {
        event.node == PEER
            && matches!(
                &event.kind,
                TraceKind::KernelNoted {
                    module: ModuleName::Replication,
                    note: KernelNote::Ignored {
                        reason: KernelIgnoredReason::Replica(ReplicaIgnoreReason::NotASource)
                    },
                    ..
                }
            )
    });
    assert!(refused, "precondition: R1 on node 2 refused the catch-up");

    let before = runner.recorded().len();
    runner
        .carry_out(
            PEER,
            BOOT,
            vec![kernel(
                ModuleName::Replication,
                KernelEffect::CopyCaughtUp {
                    copy: CopyId(2),
                    head: Seq(2),
                    digest: digest(2),
                },
            )],
        )
        .expect("routed");
    let second = runner
        .run(limits(10))
        .expect("the run itself does not fail");
    assert!(
        matches!(second.stop, rdb_sim::harness::run::StopReason::QueueEmpty),
        "{:?}",
        second.stop
    );
    let heard_on: std::collections::BTreeSet<NodeId> = runner.recorded()[before..]
        .iter()
        .filter(|event| matches!(event.kind, TraceKind::ModuleDispatch { .. }))
        .map(|event| event.node)
        .collect();
    assert_eq!(
        heard_on,
        [PEER].into_iter().collect(),
        "no asker is left for a refused catch-up: node 2's own F1 hears it"
    );
}

/// Node 2 serves copy 1 at 2 from an installed receiver, and holds copy 1's placed survivor. No
/// catch-up has been asked for yet.
fn catch_up_setup() -> (Runner, rdb_sim::storage::history::CanonicalHistory) {
    use rdb_core::contracts::ids::DurableSeq;
    use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
    let history = send_history(2);
    let mut plan = RunPlan::new(support::cluster());
    for batch in history.batches.clone() {
        plan.preloads.push((PEER, batch));
    }
    plan.survivors.push((PEER, PartitionId(1), survivor(1, 2)));
    let mut runner = Runner::new(&plan).expect("a runner");
    runner.dispatcher_mut().replication_mut().install_receiver(
        AppendReceiver::new(ReceiverInit {
            config: support::rf3_config(),
            own: CopyId(1),
            lineage: send_lineage(),
            head: Head {
                seq: Seq(2),
                digest: history.digest(2),
            },
            durable: DurableSeq(2),
        })
        .expect("a receiver at 2"),
    );
    (runner, history)
}

/// F1 on node 1 asks for copy 2 to be caught up through 2 from copy 1, which the provider routes
/// to node 2. Carried out, not yet run.
fn ask_for_catch_up(runner: &mut Runner) {
    let credential = rdb_core::contracts::authority::FenceCredential {
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(1),
        ..catch_up_credential(1)
    };
    runner
        .carry_out(
            NODE,
            BOOT,
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Recovery,
                partition: PartitionId(1),
                kind: EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::CatchUp {
                    from: CopyId(1),
                    to: CopyId(2),
                    through: Seq(2),
                    credential,
                })),
            }],
        )
        .expect("routed to node 2");
}

/// Node 3, copy 2's holder, answers node 2 with `outcome`. Carried out, not yet run.
fn node_3_answers(runner: &mut Runner, outcome: &rdb_core::contracts::envelope::AppendOutcome) {
    use rdb_core::contracts::transport::SendEffect;
    runner
        .carry_out(
            NodeId(3),
            BOOT,
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Replication,
                partition: PartitionId(1),
                kind: EffectKind::Send(SendEffect::Unicast {
                    to: PEER,
                    frame: Frame {
                        body: rdb_core::replication::wire::encode_reply(outcome),
                        ..frame()
                    },
                }),
            }],
        )
        .expect("sent to node 2");
}

/// A rejection that stops R1's cursor (`Stop::Refused`).
fn not_a_member() -> rdb_core::contracts::envelope::AppendOutcome {
    rdb_core::contracts::envelope::AppendOutcome::Rejected(
        rdb_core::contracts::envelope::AppendReject::NotAMember,
    )
}

/// Node 3's ACK of copy 2 at 2, with `digest` there: what finishes the catch-up.
fn caught_up_at_2(
    digest: rdb_core::contracts::digest::Digest,
) -> rdb_core::contracts::envelope::AppendOutcome {
    use rdb_core::contracts::envelope::{AppendAck, AppendOutcome, ReplicaProgress};
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, ReceivedSeq};
    let lineage = send_lineage();
    AppendOutcome::Accepted(AppendAck {
        partition: lineage.partition,
        generation: lineage.generation,
        owner_epoch: lineage.owner_epoch,
        config_version: ConfigVersion(1),
        from: NodeId(3),
        boot: BOOT,
        role: ReplicaRole::RegularSecondary,
        progress: ReplicaProgress {
            received: ReceivedSeq(2),
            buffered_applied: AppliedSeq(2),
            durable: DurableSeq(2),
        },
        digest_at_buffered: digest,
    })
}

/// Whether R1 on node 2 is running a source for copy 2.
fn source_running(runner: &Runner) -> bool {
    runner
        .dispatcher()
        .replication()
        .source(PEER, PartitionId(1), CopyId(2))
        .is_some()
}

/// A catch-up R1 on node 2 accepted and is running: F1 on node 1 asked it to catch copy 2 up
/// through 2, and R1 there started a source and sent node 3 a record. The source is still
/// running when this returns.
fn running_catch_up() -> (Runner, rdb_sim::storage::history::CanonicalHistory) {
    let (mut runner, history) = catch_up_setup();
    ask_for_catch_up(&mut runner);
    let first = runner
        .run(catch_up_limits(5))
        .expect("the run itself does not fail");
    assert!(
        first.clone().into_result().is_ok(),
        "bounded: neither refused nor declined: {:?}",
        first.stop
    );
    let sent = runner
        .dispatcher()
        .network()
        .transmissions()
        .iter()
        .filter(|sent| (sent.from, sent.to) == (PEER, NodeId(3)))
        .count();
    assert!(
        sent > 0,
        "precondition: R1 on node 2 accepted and started sending"
    );
    assert!(
        source_running(&runner),
        "precondition: the source is running"
    );
    (runner, history)
}

fn catch_up_limits(deadline: u64) -> RunLimits {
    RunLimits {
        max_events: 200,
        deadline: Tick(deadline),
    }
}

/// Run to `deadline`, never a refusal: the nodes whose modules ran from the first event recorded
/// after `before`.
fn heard_on_after(
    runner: &mut Runner,
    before: usize,
    deadline: u64,
) -> std::collections::BTreeSet<NodeId> {
    let report = runner
        .run(catch_up_limits(deadline))
        .expect("the run itself does not fail");
    assert!(
        report.clone().into_result().is_ok(),
        "bounded: neither refused nor declined: {:?}",
        report.stop
    );
    runner.recorded()[before..]
        .iter()
        .filter(|event| matches!(event.kind, TraceKind::ModuleDispatch { .. }))
        .map(|event| event.node)
        .collect()
}

/// Node 2's R1 reports copy 2 caught up at `digest`, carried out by hand, and the run carries
/// it: the nodes whose modules then ran.
fn copy_caught_up_heard_on(
    runner: &mut Runner,
    digest: rdb_core::contracts::digest::Digest,
    deadline: u64,
) -> std::collections::BTreeSet<NodeId> {
    let before = runner.recorded().len();
    runner
        .carry_out(
            PEER,
            BOOT,
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Replication,
                partition: PartitionId(1),
                kind: EffectKind::Kernel(KernelEffect::CopyCaughtUp {
                    copy: CopyId(2),
                    head: Seq(2),
                    digest,
                }),
            }],
        )
        .expect("routed");
    heard_on_after(runner, before, deadline)
}

/// The twin of `provider_a_refused_catch_up_leaves_no_asker_behind`: a catch-up R1 accepts
/// keeps its asker. Node 2 serves copy 1 at 2, so R1 there starts a source for copy 2, and a
/// later `CopyCaughtUp` from node 2 goes back to F1 on node 1.
#[retcd_test]
fn provider_an_accepted_catch_up_keeps_its_asker() {
    support::preamble();
    let (mut runner, history) = running_catch_up();
    let heard_on = copy_caught_up_heard_on(&mut runner, history.digest(2), 10);
    assert!(
        heard_on.contains(&NODE),
        "the asker, F1 on node 1, hears it: {heard_on:?}"
    );
}

/// A source that finishes reports to its asker, end to end. Node 3 ACKs copy 2 at 2, the
/// source's cursor reports `CopyCaughtUp`, and R1 drops the finished source **in the same step**.
/// The step's report is what keeps the asker until the report is carried out to F1 on node 1: a
/// source gone after the step is forgotten only when it reported nothing.
#[retcd_test]
fn provider_a_source_that_finishes_reports_to_its_asker() {
    support::preamble();
    let (mut runner, history) = running_catch_up();
    let before = runner.recorded().len();
    node_3_answers(&mut runner, &caught_up_at_2(history.digest(2)));
    let heard_on = heard_on_after(&mut runner, before, 10);
    assert!(
        !source_running(&runner),
        "precondition: R1 on node 2 dropped the finished source"
    );
    assert!(
        heard_on.contains(&NODE),
        "the asker, F1 on node 1, hears the source's report: {heard_on:?}"
    );
}

/// A catch-up still queued keeps its asker through an earlier R1 step on its node. Node 1's
/// append of record 1 to node 2's receiver is carried out first and F1's `CatchUp` second, so at
/// the same tick R1 on node 2 answers the append before it has started any source. That step ends
/// nothing it had not started: only a source running before a step, or the one the step starts,
/// can be forgotten by it. When node 3 later ACKs copy 2 at 2, F1 on node 1 hears the report.
#[retcd_test]
fn provider_a_queued_catch_up_keeps_its_asker_through_an_earlier_r1_step() {
    use rdb_core::contracts::transport::SendEffect;
    support::preamble();
    let (mut runner, history) = catch_up_setup();
    runner
        .carry_out(
            NODE,
            BOOT,
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Replication,
                partition: PartitionId(1),
                kind: EffectKind::Send(SendEffect::Unicast {
                    to: PEER,
                    frame: Frame {
                        body: record_of(&history.batches[0]),
                        ..frame()
                    },
                }),
            }],
        )
        .expect("sent to node 2");
    ask_for_catch_up(&mut runner);
    let first = runner
        .run(catch_up_limits(5))
        .expect("the run itself does not fail");
    assert!(
        first.clone().into_result().is_ok(),
        "bounded: neither refused nor declined: {:?}",
        first.stop
    );
    let answered: Vec<EventId> = runner
        .recorded()
        .iter()
        .filter(|event| event.node == PEER)
        .filter_map(|event| match event.kind {
            TraceKind::ModuleDispatch {
                event,
                module: ModuleName::Replication,
                outcome: DispatchOutcome::Answered { .. },
            } => Some(event),
            _ => None,
        })
        .collect();
    assert!(
        answered.len() >= 2,
        "precondition: R1 on node 2 answered the append, then the catch-up: {answered:?}"
    );
    assert!(
        source_running(&runner),
        "precondition: the catch-up started after the earlier step"
    );
    let before = runner.recorded().len();
    node_3_answers(&mut runner, &caught_up_at_2(history.digest(2)));
    let heard_on = heard_on_after(&mut runner, before, 10);
    assert!(
        heard_on.contains(&NODE),
        "the asker, F1 on node 1, hears the source's report: {heard_on:?}"
    );
}

/// tester-kb-r1 A3: a source that stops leaves no asker behind. The catch-up of
/// `running_catch_up` is running when node 3 answers it `NotAMember`, a rejection that stops
/// R1's cursor, and R1 drops the source. No `CopyCaughtUp` of that catch-up can follow, so a
/// later one from node 2 answers no catch-up of F1 on node 1 and goes to node 2's own F1 like any
/// other. A source dropped any other way — `Recovered` for its partition — is the same case to the
/// harness: the source was running before R1's step and is gone after it, with no
/// `CopyCaughtUp` among the step's effects.
#[retcd_test]
fn provider_a_stopped_source_leaves_no_asker_behind() {
    support::preamble();
    let (mut runner, history) = running_catch_up();
    node_3_answers(&mut runner, &not_a_member());
    let stopped = runner
        .run(catch_up_limits(10))
        .expect("the run itself does not fail");
    assert!(
        stopped.clone().into_result().is_ok(),
        "bounded: neither refused nor declined: {:?}",
        stopped.stop
    );
    assert!(
        !source_running(&runner),
        "precondition: R1 on node 2 dropped the stopped source"
    );
    let heard_on = copy_caught_up_heard_on(&mut runner, history.digest(2), 20);
    assert_eq!(
        heard_on,
        [PEER].into_iter().collect(),
        "no asker is left for a stopped source: node 2's own F1 hears it"
    );
}

// ------------------------------------------------------------------------------------------
// Semantic recording (step 3 of the sim-hooks brief): what each engine applied, synced and
// acknowledged, as the trace lines the oracle arms on. Scaffolding rows, not plan ids: they
// assert the harness records what the environment really did, and no invariant's verdict.
// ------------------------------------------------------------------------------------------

/// One `BatchApply` line on partition 1, flattened for comparison.
type ApplyLine = (
    NodeId,
    Generation,
    Seq,
    rdb_core::contracts::digest::Digest,
    rdb_core::contracts::digest::Digest,
    ReplicaRole,
    rdb_core::contracts::trace::ApplyOutcome,
);

/// `(node, generation, seq, predecessor_digest, entry_digest, role, outcome)` for every
/// `BatchApply` line on partition 1, in trace order.
fn applies(trace: &Trace) -> Vec<ApplyLine> {
    trace
        .events
        .iter()
        .filter(|event| event.partition == PartitionId(1))
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply {
                role,
                generation,
                seq,
                predecessor_digest,
                entry_digest,
                outcome,
                ..
            } => Some((
                event.node,
                *generation,
                *seq,
                *predecessor_digest,
                *entry_digest,
                *role,
                *outcome,
            )),
            _ => None,
        })
        .collect()
}

/// A commit is a `BatchApply` at the committing node, carrying the digests of the record the
/// batch wrote — the same bytes the engine then holds — and the role the node holds in its pinned
/// configuration. A preload is one too, at tick 0, so an acknowledgement of a preloaded prefix is
/// never above its node's last apply.
#[retcd_test]
fn recording_every_commit_is_a_batch_apply_line_with_the_records_own_digests() {
    use rdb_core::contracts::trace::ApplyOutcome;
    use rdb_sim::harness::trace::validate;
    support::preamble();
    let run = run_rebuild(Vec::new());
    validate(&run.trace).expect("the rebuild's trace is well formed");
    let applies = applies(&run.trace);
    tracing::info!(count = applies.len(), "batch_apply lines");

    // The preloads: nodes 1 and 2 hold the prior history, at tick 0.
    for node in [NodeId(1), NodeId(2)] {
        let preloaded: Vec<_> = applies
            .iter()
            .filter(|line| line.0 == node && line.1 == Generation(1))
            .map(|line| (line.2, line.3, line.4, line.6))
            .collect();
        assert_eq!(
            preloaded,
            (1..=SPINE_HEAD)
                .map(|seq| (
                    Seq(seq),
                    spine_digest(seq - 1),
                    spine_digest(seq),
                    ApplyOutcome::Applied
                ))
                .collect::<Vec<_>>(),
            "node {} preloaded the prior history",
            node.0
        );
    }
    let preload_ticks: Vec<u64> = run
        .trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::BatchApply {
                    generation: Generation(1),
                    ..
                }
            )
        })
        .map(|event| event.logical_tick)
        .collect();
    assert!(
        !preload_ticks.is_empty() && preload_ticks.iter().all(|tick| *tick == 0),
        "{preload_ticks:?}"
    );

    // R1 caught copy 2 up from the root: node 3 applied 1..=SPINE_HEAD of the new generation,
    // each with the digest its engine now holds, as the regular secondary the pin names.
    let caught_up: Vec<_> = applies
        .iter()
        .filter(|line| line.0 == NodeId(3) && line.1 == REBUILT)
        .map(|line| (line.2, line.3, line.4, line.5, line.6))
        .collect();
    assert_eq!(
        caught_up,
        (1..=SPINE_HEAD)
            .map(|seq| (
                Seq(seq),
                spine_digest(seq - 1),
                spine_digest(seq),
                ReplicaRole::RegularSecondary,
                ApplyOutcome::Applied
            ))
            .collect::<Vec<_>>()
    );
    for (seq, record) in (1..=SPINE_HEAD).zip(&run.copy_2_records) {
        let record = record.as_ref().expect("node 3 holds the record");
        let envelope = rdb_core::contracts::envelope::ReplicationEnvelope::decode(record)
            .expect("an envelope");
        assert_eq!(
            envelope.record_digest,
            spine_digest(seq),
            "the engine's own bytes"
        );
    }
}

/// An accepted append reply is a `ReplicationAck` recorded at the acknowledging node, at the
/// moment it answers: from that node, to the primary it answers, with the progress and role its
/// own acknowledgement carries. Buffered, because that is what `buffered_applied` is.
#[retcd_test]
fn recording_every_accepted_append_reply_is_a_replication_ack_line_at_its_acker() {
    use rdb_core::contracts::trace::DurabilityClass;
    use rdb_sim::harness::trace::validate;
    support::preamble();
    let run = run_rebuild(Vec::new());
    validate(&run.trace).expect("the rebuild's trace is well formed");
    let acks: Vec<_> = run
        .trace
        .events
        .iter()
        .filter(|event| matches!(event.kind, TraceKind::ReplicationAck { .. }))
        .collect();
    tracing::info!(count = acks.len(), "replication_ack lines");
    assert!(!acks.is_empty(), "the catch-up was acknowledged");

    let mut highest_from_3 = None;
    for event in &acks {
        let TraceKind::ReplicationAck {
            from_node,
            to_node,
            peer_role,
            peer_boot,
            generation,
            contiguous_seq,
            contiguous_digest,
            durability_class,
            accepted,
            reject_reason,
            ..
        } = &event.kind
        else {
            unreachable!("filtered to acks")
        };
        assert_eq!(*from_node, event.node, "recorded at the acker");
        assert_eq!(*peer_boot, event.boot, "under the acker's own boot");
        assert_eq!(*to_node, NODE, "answering node 1's primary");
        assert!(*accepted && reject_reason.is_none());
        assert_eq!(*durability_class, DurabilityClass::Buffered);
        // The acker's own apply of that position, with that digest, precedes the ack.
        if contiguous_seq.0 > 0 {
            let applied = run.trace.events.iter().any(|line| {
                line.event_id < event.event_id
                    && line.node == event.node
                    && matches!(
                        line.kind,
                        TraceKind::BatchApply { seq, entry_digest, .. }
                            if seq == *contiguous_seq && entry_digest == *contiguous_digest
                    )
            });
            assert!(
                applied,
                "node {} acked seq {} before any apply of it",
                from_node.0, contiguous_seq.0
            );
        }
        if *from_node == NodeId(3) && *generation == REBUILT {
            assert_eq!(*peer_role, ReplicaRole::RegularSecondary);
            highest_from_3 = Some(highest_from_3.unwrap_or(Seq(0)).max(*contiguous_seq));
        }
    }
    assert_eq!(
        highest_from_3,
        Some(Seq(SPINE_HEAD)),
        "copy 2 acknowledged the whole catch-up"
    );
}

/// One `DurabilityAdvance` line on partition 1, flattened for comparison.
type SyncLine = (
    NodeId,
    Generation,
    Seq,
    rdb_core::contracts::digest::Digest,
    rdb_core::contracts::trace::SyncOutcome,
);

/// `(node, generation, durable_seq, durable_digest, outcome)` for every `DurabilityAdvance` line
/// on partition 1, in trace order.
fn syncs(trace: &Trace) -> Vec<SyncLine> {
    trace
        .events
        .iter()
        .filter(|event| event.partition == PartitionId(1))
        .filter_map(|event| match &event.kind {
            TraceKind::DurabilityAdvance {
                generation,
                durable_seq,
                durable_digest,
                outcome,
                ..
            } => Some((
                event.node,
                *generation,
                *durable_seq,
                *durable_digest,
                *outcome,
            )),
            _ => None,
        })
        .collect()
}

/// A sync is a `DurabilityAdvance` at the node whose engine ran it: F1's `SyncWalThrough` on each
/// survivor is `Synced` at the head with the engine's digest there, and a planned `FalseDurable`
/// on node 2 makes node 2's a flush that completed and advanced nothing, which is `Partial` at
/// the watermark the engine still holds — never `Synced` at the cutoff.
///
/// Renamed from `..._synced_only_where_the_engine_moved` (tester-sim-hooks F2): a no-op sync,
/// whose watermark does not move, is still `Synced` at that watermark, so the old name claimed
/// more than the row asserts. A `ShortFlush` is `Partial` (V-R36), pinned by M7V-98 and M7V-99.
#[retcd_test]
fn recording_a_sync_is_synced_at_the_engines_watermark_and_a_false_durable_flush_is_partial() {
    use rdb_core::contracts::ids::AppliedSeq;
    use rdb_core::contracts::trace::SyncOutcome;
    use rdb_sim::harness::trace::validate;
    support::preamble();
    let run = run_spine();
    validate(&run.trace).expect("the spine's trace is well formed");
    let synced: Vec<_> = syncs(&run.trace)
        .into_iter()
        .filter(|line| line.1 == Generation(1))
        .collect();
    tracing::info!(?synced, "durability_advance lines");
    for node in 1..=3 {
        assert!(
            synced.contains(&(
                NodeId(node),
                Generation(1),
                Seq(SPINE_HEAD),
                spine_digest(SPINE_HEAD),
                SyncOutcome::Synced
            )),
            "node {node}'s survivor sync is recorded: {synced:?}"
        );
    }

    let mut plan = spine_plan();
    plan.storage_ops.push(StorageOp::FalseDurable {
        node: PEER,
        through: AppliedSeq(SPINE_HEAD),
    });
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the spine runs");
    tracing::info!(stop = ?report.stop, "spine under FalseDurable on node 2");
    assert_eq!(
        runner
            .dispatcher()
            .engine(PEER)
            .expect("node 2")
            .false_claims(),
        &[AppliedSeq(SPINE_HEAD)],
        "the fault was met"
    );
    let trace = runner.finish().expect("a trace");
    validate(&trace).expect("well formed");
    let on_peer: Vec<_> = syncs(&trace)
        .into_iter()
        .filter(|line| line.0 == PEER && line.1 == Generation(1))
        .collect();
    tracing::info!(
        ?on_peer,
        "node 2's durability_advance lines under FalseDurable"
    );
    assert_eq!(
        on_peer.first(),
        Some(&(
            PEER,
            Generation(1),
            Seq(0),
            rdb_core::contracts::digest::Digest::ROOT,
            SyncOutcome::Partial
        )),
        "the false flush advanced nothing, and says so: {on_peer:?}"
    );
}

/// The one fault hook on F1's sync path (L-R182m): a tagged `FalseDurable` on node 2, taken by
/// F1's `SyncWalThrough` in the spine (no host flush runs there), is recorded as exactly one
/// `fault_injected{Storage, 2, FalseDurableWatermark}` line carrying the step's op index, right
/// after the `Partial` line of the sync that took it and in that sync's tick. The host-flush
/// path is M7V-70's.
#[retcd_test]
fn a_tagged_false_durable_taken_by_an_f1_sync_is_one_fault_line_beside_that_sync() {
    use rdb_core::contracts::ids::AppliedSeq;
    use rdb_core::contracts::trace::{BoundaryId, FaultKind, SyncOutcome};
    use rdb_sim::harness::run::{FaultTag, ScenarioStep, StepAction};
    /// Any op index the spine does not use for anything else; nonzero, so it cannot pass for a
    /// default.
    const OP_INDEX: u32 = 7;
    support::preamble();
    let mut plan = spine_plan();
    plan.steps.push(ScenarioStep {
        at: Tick(0),
        node: PEER,
        partition: PartitionId(1),
        action: StepAction::Storage(StorageOp::FalseDurable {
            node: PEER,
            through: AppliedSeq(SPINE_HEAD),
        }),
        line: None,
        taken: Some(FaultTag {
            boundary: BoundaryId::FalseDurableWatermark,
            op_index: OP_INDEX,
        }),
    });
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the spine runs");
    tracing::info!(stop = ?report.stop, "spine under a tagged FalseDurable on node 2");
    let trace = runner.finish().expect("a trace");

    let faults: Vec<usize> = trace
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| matches!(event.kind, TraceKind::FaultInjected { .. }))
        .map(|(index, _)| index)
        .collect();
    tracing::info!(?faults, "fault_injected lines");
    let [at] = faults.as_slice() else {
        panic!("exactly one fault_injected line: {faults:?}");
    };
    let fault = &trace.events[*at];
    assert_eq!(
        (fault.node, fault.partition, &fault.kind),
        (
            PEER,
            PartitionId(1),
            &TraceKind::FaultInjected {
                fault_kind: FaultKind::Storage,
                target: PEER,
                boundary: BoundaryId::FalseDurableWatermark,
                scenario_op_index: OP_INDEX,
            }
        ),
        "the lie is recorded once, on node 2, with the step's op index"
    );
    let previous = &trace.events[at - 1];
    assert!(
        previous.node == PEER
            && previous.logical_tick == fault.logical_tick
            && matches!(
                previous.kind,
                TraceKind::DurabilityAdvance {
                    outcome: SyncOutcome::Partial,
                    ..
                }
            ),
        "the fault line follows node 2's Partial sync, in its tick: {previous:?} then {fault:?}"
    );
}

// ------------------------------------------------------------------------------------------
// M7V-92 — MUT-5's kernel half (lead ruling V-R25): a false durable watermark never becomes a
// durable position the kernel reports
// ------------------------------------------------------------------------------------------

/// The rebuild with node 3's engine lying on its next two syncs: `FalseDurable` completes the
/// flush, reports nothing durable, and moves nothing. One lie is met by F1's sync of copy 2, the
/// other by a host flush planned after the catch-up, so both of the kernel's durable inputs on
/// node 3 (R1's `Flushed` and F1's `DurableAt`) are offered the lie.
fn run_false_durable_rebuild() -> (
    RebuildRun,
    Vec<rdb_core::contracts::ids::AppliedSeq>,
    u64,
    u64,
) {
    use rdb_core::contracts::ids::AppliedSeq;
    let mut plan = rebuild_plan(Vec::new());
    for _ in 0..2 {
        plan.storage_ops.push(StorageOp::FalseDurable {
            node: NodeId(3),
            through: AppliedSeq(SPINE_HEAD),
        });
    }
    plan.flushes.push((Tick(3_000), NodeId(3)));
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the rebuild runs");
    let engine = runner.dispatcher().engine(NodeId(3)).expect("node 3");
    let claims = engine.false_claims().to_vec();
    let performed = engine.durable(PartitionId(1), REBUILT).0;
    let reported = runner
        .dispatcher()
        .replication()
        .receiver(NodeId(3), PartitionId(1))
        .map(|receiver| receiver.current_ack().progress.durable.0)
        .expect("R1 built copy 2's receiver");
    let phase = runner
        .dispatcher()
        .recovery(NODE, PartitionId(1))
        .map(rdb_core::recovery::Recovery::phase);
    tracing::info!(stop = ?report.stop, ?claims, performed, reported, ?phase,
        "rebuild under FalseDurable on node 3");
    let copy_2_head = runner
        .dispatcher()
        .replication()
        .receiver(NodeId(3), PartitionId(1))
        .map(|receiver| receiver.applied_head())
        .map(|head| (head.seq.0, head.digest));
    let copy_2_base = runner
        .dispatcher()
        .engine(NodeId(3))
        .expect("node 3")
        .base(PartitionId(1), REBUILT);
    let trace = runner.finish().expect("a trace");
    let run = RebuildRun {
        trace,
        phase,
        copy_2_head,
        copy_2_base,
        copy_2_records: Vec::new(),
    };
    (run, claims, performed, reported)
}

/// M7V-92. `StorageOp::FalseDurable` on a rebuilt copy's engine: the kernel never reports a
/// durable position above what M1 performed. R1's acknowledgement stays at the engine's real
/// watermark, the recorded `DurabilityAdvance` at the lie is `Partial` and never `Synced` above
/// it, and F1 withholds copy 2's proof as `Short` rather than proving the cutoff.
///
/// Its third clause, "no publish counts an ungrounded ack", needs a run that reaches P1's
/// `Publish`, and this spine never does (B-R60: L1 stays `Paused` on a recovered partition).
/// That clause is `publish_rows::m7v_92_a_publish_after_the_lie_counts_no_ungrounded_ack` in
/// `tests/scenarios.rs`, on the A1/P1 arming shape; this function asserts the spine still does
/// not publish rather than assuming it.
#[retcd_test]
fn m7v_92_false_durable_the_kernel_never_reports_durable_beyond_what_m1_performed() {
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq};
    use rdb_core::contracts::trace::{SyncOutcome, SyncWithheldReason};
    use rdb_sim::harness::trace::validate;
    use support::oracle::{Invariant, Oracle, Verdict};
    support::preamble();
    let (run, claims, performed, reported) = run_false_durable_rebuild();
    validate(&run.trace).expect("well formed");

    // The fault was met, twice: a row whose lie never reached a sync would pass vacuously.
    assert_eq!(
        claims,
        vec![AppliedSeq(SPINE_HEAD), AppliedSeq(SPINE_HEAD)],
        "both planned lies were told"
    );
    assert!(
        performed < SPINE_HEAD,
        "the lie is a lie: M1 performed {performed}, below the claimed {SPINE_HEAD}"
    );
    assert_eq!(
        run.copy_2_head.map(|(seq, _)| seq),
        Some(SPINE_HEAD),
        "copy 2 buffered the whole catch-up, so there was something to lie about"
    );

    // R1: the acknowledgement's durable position is the engine's, never the claim.
    assert!(
        reported <= performed,
        "R1 reports durable {reported} on node 3, above the {performed} M1 performed"
    );

    // The record: every `DurabilityAdvance` on node 3 in the new generation, and none `Synced`
    // above what was performed.
    let on_3: Vec<_> = syncs(&run.trace)
        .into_iter()
        .filter(|line| line.0 == NodeId(3) && line.1 == REBUILT)
        .collect();
    tracing::info!(?on_3, "node 3's durability_advance lines");
    assert_eq!(
        on_3.iter()
            .filter(|line| line.4 == SyncOutcome::Partial)
            .count(),
        2,
        "each lie is one Partial line: {on_3:?}"
    );
    for line in &on_3 {
        assert!(
            line.2 .0 <= performed,
            "a durability_advance above what M1 performed: {line:?}"
        );
    }

    // F1: copy 2 is withheld as short at the real watermark, never proven.
    let notes = run.sync_notes();
    tracing::info!(?notes, "node 1's sync notes");
    assert!(
        notes.contains(&KernelNote::SyncWithheld {
            copy: CopyId(2),
            cutoff: Seq(SPINE_HEAD),
            reason: SyncWithheldReason::Short {
                durable: DurableSeq(performed),
            },
        }),
        "{notes:?}"
    );
    assert!(
        !run.proven().iter().any(|(copy, _)| *copy == CopyId(2)),
        "no proof for copy 2: {:?}",
        run.proven()
    );

    // This spine never publishes (L1 stays Paused, B-R60). The publish clause runs on a
    // recovered-and-resumed run in `tests/scenarios.rs`,
    // `publish_rows::m7v_92_a_publish_after_the_lie_counts_no_ungrounded_ack`. Stated as a fact
    // about this run, so a spine that starts publishing is noticed here too.
    let publishes = run
        .trace
        .events
        .iter()
        .filter(|event| matches!(event.kind, TraceKind::Publish { .. }))
        .count();
    let inv_pub = Oracle::new()
        .judge(&run.trace)
        .verdict(Invariant::Pub)
        .clone();
    tracing::info!(publishes, ?inv_pub, "INV-PUB on the FalseDurable rebuild");
    assert!(!matches!(inv_pub, Verdict::Violated(_)), "{inv_pub:?}");
    assert_eq!(
        publishes, 0,
        "the rebuild spine now publishes: the publish clause in scenarios.rs still covers \
         M7V-92, but re-read this row's third clause against this run"
    );
}

/// Report a parked clause, and fail if its package has landed (as `scenarios.rs` does): read
/// from the landed capability table, never written down.
#[track_caller]
fn parked(row: &str, package: PackageId, what: &str) {
    let state = rdb_sim::harness::environment_capabilities()
        .into_iter()
        .find(|(candidate, _)| *candidate == package)
        .map(|(_, state)| state);
    assert_eq!(
        state,
        Some(CapabilityState::Unavailable),
        "{package:?} now reports Wired: {row}'s parked clause must be written"
    );
    println!("{row}: parked on {package:?} — {what}");
}

// ------------------------------------------------------------------------------------------
// M7V-80 — a recovery-path digest disagreement is F1's quarantine, and the trace says so
// ------------------------------------------------------------------------------------------

/// The copy whose survivor disagrees: copy 2, on node 3.
const DISAGREEING: NodeId = NodeId(3);

/// The spine with node 3's survivor reporting the head position (generation 1, seq 2) under a
/// digest nobody else holds. Every source is reachable, and the other two agree with each other.
fn disagreeing_spine_plan() -> RunPlan {
    let mut plan = spine_plan();
    let forked = rdb_core::contracts::digest::Digest([0xd1; 32]);
    for (node, _, inventory) in &mut plan.survivors {
        if *node == DISAGREEING {
            inventory.head = (Seq(SPINE_HEAD), forked);
            if let Some(rung) = inventory
                .ladder
                .iter_mut()
                .find(|(seq, _)| seq.0 == SPINE_HEAD)
            {
                rung.1 = forked;
            }
        }
    }
    plan
}

/// M7V-80. A reachable source whose reported digest differs at a recorded position is a
/// divergence spec §8.2 says never auto-merges, and deciding it is F1's, not the oracle's (the
/// kernel-facing third sub-case of M7V-19). The trace carries `recovery_decision{Quarantine}`
/// naming every queried source, the disagreeing one included, with **no** cutoff, digest or new
/// generation (the contract's `None` on a quarantine, L-R177gd), and `quarantine{DigestConflict}`
/// at that position naming both sides. INV-LIN does not fire on it: a quarantine selected no
/// cutoff, so `cutoff_below_an_available_recorded_prefix` has nothing to judge (read as 0, it
/// would fire on nodes 1 and 2, which report seq 2 under the recorded digest).
///
/// The plan's `(gen 7, seq 9)` is the spine's `(gen 1, seq 2)`: the position is illustrative, the
/// shape is not. Parked on its coverage and `Proven` clauses, which need recorder lines the
/// harness does not write (I1): see the `parked` call.
#[retcd_test]
fn m7v_80_recovery_path_digest_disagreement_yields_quarantine_from_the_kernel() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::{DivergenceEvidence, RecoveryEffect, RecoveryEvent};
    use rdb_core::contracts::trace::{QuarantineReason, QueriedSource, RecoveryMode};
    use rdb_sim::harness::trace::validate;
    use support::oracle::{Invariant, Oracle, Verdict};
    use support::scenarios::coverage::{recovery_mode_cell, RECOVERY_MODES};
    support::preamble();
    let plan = disagreeing_spine_plan();
    let fence_at = plan
        .seed
        .iter()
        .find(|seed| {
            matches!(
                seed.kind,
                EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(_)))
            )
        })
        .map(|seed| seed.at.0)
        .expect("the spine fences F1");
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the spine runs");
    let phase = runner
        .dispatcher()
        .recovery(NODE, PartitionId(1))
        .map(rdb_core::recovery::Recovery::phase);
    tracing::info!(stop = ?report.stop, ?phase, "spine with a disagreeing survivor");
    let trace = runner.finish().expect("a trace");
    validate(&trace).expect("well formed");

    // F1 decided it, over copy 2 at the head; the other side is copy 0 or copy 1.
    let evidence: Vec<(u64, Seq, CopyId, CopyId)> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note:
                    KernelNote::RecoveryFact {
                        effect:
                            RecoveryEffect::Quarantine(DivergenceEvidence::Pairwise { seq, a, b }),
                    },
                ..
            } => Some((event.logical_tick, *seq, a.0, b.0)),
            _ => None,
        })
        .collect();
    tracing::info!(?evidence, "F1's quarantine evidence");
    assert_eq!(evidence.len(), 1, "one quarantine: {evidence:?}");
    let (decided_at, seq, a, b) = evidence[0];
    assert_eq!(seq, Seq(SPINE_HEAD));
    assert!(a == CopyId(2) || b == CopyId(2), "copy 2 is a side");
    assert_eq!(phase, Some(rdb_core::recovery::RecoveryPhase::Quarantined));
    // The window closed at its deadline, in the step that decided.
    let closed_at: Vec<u64> = trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::KernelNoted {
                    note: KernelNote::RecoveryFact {
                        effect: RecoveryEffect::CloseWindow
                    },
                    ..
                }
            )
        })
        .map(|event| event.logical_tick)
        .collect();
    assert_eq!(
        closed_at,
        vec![decided_at],
        "one window, closed where F1 decided"
    );

    // The decision: at F1's node, mode Quarantine, nothing selected and nothing created.
    let decisions: Vec<_> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            kind @ TraceKind::RecoveryDecision { .. } => Some((event.node, kind.clone())),
            _ => None,
        })
        .collect();
    tracing::info!(?decisions, "recovery_decision lines");
    assert_eq!(
        decisions.len(),
        1,
        "one recovery_decision line: {decisions:?}"
    );
    let (at_node, decision) = decisions[0].clone();
    assert_eq!(at_node, NODE, "at F1's node");
    let TraceKind::RecoveryDecision {
        fenced_epoch,
        discovery_window_ticks,
        queried_sources,
        selected_source,
        selected_cutoff_seq,
        selected_digest,
        mode,
        loss_uncertainty,
        new_generation,
    } = decision
    else {
        unreachable!("filtered to decisions")
    };
    assert_eq!(mode, RecoveryMode::Quarantine);
    assert_eq!(
        (
            selected_source,
            selected_cutoff_seq,
            selected_digest,
            new_generation
        ),
        (None, None, None, None),
        "a quarantine selects no source, cutoff or digest and creates no lineage"
    );
    assert_eq!(fenced_epoch, spine_prior().owner_epoch, "the fence's epoch");
    assert_eq!(
        discovery_window_ticks,
        decided_at - fence_at,
        "from the fence's arrival to the window's close"
    );
    assert!(!loss_uncertainty, "a quarantine discards no suffix");
    // Every copy F1 queried, by its node: nodes 1 and 2 answered with the shared head, node 3
    // with the digest nobody else holds, and node 4 (the prior owner, no survivor placed) could
    // not answer, so it is unreachable and reports nothing.
    let forked = rdb_core::contracts::digest::Digest([0xd1; 32]);
    let placed: Vec<NodeId> = plan.survivors.iter().map(|(node, _, _)| *node).collect();
    assert_eq!(placed, vec![NodeId(1), NodeId(2), DISAGREEING]);
    let expected: Vec<QueriedSource> = support::rf3_config()
        .members
        .iter()
        .map(|member| {
            let reported = placed.contains(&member.node).then(|| {
                let digest = if member.node == DISAGREEING {
                    forked
                } else {
                    spine_digest(SPINE_HEAD)
                };
                (Generation(1), Seq(SPINE_HEAD), digest)
            });
            QueriedSource {
                node: member.node,
                boot: BOOT,
                role: member.role,
                reachable: reported.is_some(),
                reported_generation: reported.map(|(generation, _, _)| generation),
                reported_seq: reported.map(|(_, seq, _)| seq),
                reported_digest: reported.map(|(_, _, digest)| digest),
            }
        })
        .collect();
    assert_eq!(queried_sources, expected);
    assert_eq!(
        queried_sources.len(),
        4,
        "every member of the pin was queried"
    );
    // The guard cell this line hits.
    assert_eq!(
        RECOVERY_MODES[recovery_mode_cell(mode)],
        RecoveryMode::Quarantine
    );

    // The quarantine: one line, at the position, naming each side by the node F1's plan places
    // it on (copy n is on node n + 1 in the spine).
    let node_of = |copy: CopyId| NodeId(u32::from(copy.0) + 1);
    let quarantines: Vec<_> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::Quarantine {
                reason,
                generation,
                seq,
                sources,
            } => Some((event.node, *reason, *generation, *seq, sources.clone())),
            _ => None,
        })
        .collect();
    tracing::info!(?quarantines, "quarantine lines");
    assert_eq!(
        quarantines,
        vec![(
            NODE,
            QuarantineReason::DigestConflict,
            Generation(1),
            Seq(SPINE_HEAD),
            vec![node_of(a), node_of(b)]
        )],
        "at F1's node, in the prior lineage, naming both sides"
    );
    assert!(quarantines[0].4.contains(&DISAGREEING));

    // INV-LIN: nothing fires. `Proven` is the parked clause below.
    let inv_lin = Oracle::new().judge(&trace).verdict(Invariant::Lin).clone();
    tracing::info!(?inv_lin, "INV-LIN on the quarantined spine");
    assert!(!matches!(inv_lin, Verdict::Violated(_)), "{inv_lin:?}");
    parked(
        "M7V-80",
        PackageId::I1,
        "INV-LIN Proven (the recorder writes no lineage_root, so INV-LIN never arms on a \
         recorded run), the BoundaryId::Divergence cell (no fault_injected line) and the \
         AckRejectReason::Diverged cell (no rejected-ack line; a quarantine builds no receiver)",
    );
}

// ------------------------------------------------------------------------------------------
// Recorder truth: M7V-96..M7V-101 (tester-sim-hooks F1, MATERIAL). Each recorded line is checked
// against the engine's own state, never against the recorder's inputs, so a recorder that logs
// something that did not happen is killed by a row rather than by a probe. Adopted from the
// tester's probes p1, p1b, p3, p4, p5, p7 and p8 (`probe_tail.rs`, md5 b542b3d2).
// ------------------------------------------------------------------------------------------

/// A finished run, with each node's engine as it stood when the loop stopped.
struct TruthRun {
    trace: Trace,
    engines: std::collections::BTreeMap<NodeId, rdb_sim::storage::memory::MemoryEngine>,
    err: Option<String>,
}

/// Run `plan` to its end, keeping each engine before `finish` consumes the runner.
fn truth_run(plan: &RunPlan) -> TruthRun {
    let mut runner = Runner::new(plan).expect("a runner");
    let err = runner.run(plan.limits).err().map(|e| format!("{e:?}"));
    let engines = (1..=4)
        .filter_map(|n| {
            runner
                .dispatcher()
                .engine(NodeId(n))
                .map(|engine| (NodeId(n), engine.clone()))
        })
        .collect();
    let trace = runner.finish().expect("a trace");
    TruthRun {
        trace,
        engines,
        err,
    }
}

/// The digest `engine` holds at `(generation, seq)`, or the root where it holds no record.
fn truth_digest(
    engine: &rdb_sim::storage::memory::MemoryEngine,
    generation: Generation,
    seq: Seq,
) -> rdb_core::contracts::digest::Digest {
    engine
        .history_at(PartitionId(1), generation, seq)
        .and_then(|(record, _)| {
            rdb_core::contracts::envelope::ReplicationEnvelope::decode(&record).ok()
        })
        .map_or(rdb_core::contracts::digest::Digest::ROOT, |envelope| {
            envelope.record_digest
        })
}

/// Every `Applied` `BatchApply` names a record its node's engine holds, with those exact
/// digests. Returns `(applied, failed)` line counts.
fn truth_applies(run: &TruthRun) -> (usize, usize) {
    use rdb_core::contracts::trace::ApplyOutcome;
    let (mut applied, mut failed) = (0, 0);
    for event in &run.trace.events {
        let TraceKind::BatchApply {
            generation,
            seq,
            predecessor_digest,
            entry_digest,
            outcome,
            ..
        } = &event.kind
        else {
            continue;
        };
        if *outcome != ApplyOutcome::Applied {
            failed += 1;
            continue;
        }
        let (record, _) = run.engines[&event.node]
            .history_at(event.partition, *generation, *seq)
            .unwrap_or_else(|| {
                panic!(
                    "event {}: node {} logged Applied g{} s{}, which its engine does not hold",
                    event.event_id.0, event.node.0, generation.0, seq.0
                )
            });
        let envelope = rdb_core::contracts::envelope::ReplicationEnvelope::decode(&record)
            .expect("an envelope");
        assert_eq!(
            (envelope.prev_digest, envelope.record_digest),
            (*predecessor_digest, *entry_digest),
            "event {}: BatchApply digests differ from node {}'s stored record",
            event.event_id.0,
            event.node.0
        );
        applied += 1;
    }
    (applied, failed)
}

/// Every `Synced` line is at or below its node's final watermark (no crash in these runs, so the
/// watermark only rises), with the engine's own digest there. Returns `(synced, partial,
/// failed)` line counts.
fn truth_syncs(run: &TruthRun) -> (usize, usize, usize) {
    use rdb_core::contracts::trace::SyncOutcome;
    let (mut synced, mut partial, mut failed) = (0, 0, 0);
    for event in &run.trace.events {
        let TraceKind::DurabilityAdvance {
            generation,
            durable_seq,
            durable_digest,
            outcome,
            ..
        } = &event.kind
        else {
            continue;
        };
        let engine = &run.engines[&event.node];
        let held = Seq(engine.durable(event.partition, *generation).0);
        match outcome {
            SyncOutcome::Synced => {
                synced += 1;
                assert!(
                    *durable_seq <= held,
                    "event {}: Synced at {} above node {}'s watermark {}",
                    event.event_id.0,
                    durable_seq.0,
                    event.node.0,
                    held.0
                );
                assert_eq!(
                    *durable_digest,
                    truth_digest(engine, *generation, *durable_seq),
                    "event {}: the durable digest is the engine's",
                    event.event_id.0
                );
            }
            SyncOutcome::Partial => partial += 1,
            SyncOutcome::Failed => failed += 1,
        }
    }
    (synced, partial, failed)
}

/// Every `ReplicationAck` is accepted, `Buffered`, recorded at its acker under the acker's boot,
/// and names a position that node already `Applied` (not merely attempted) with that digest.
/// Unless `allow_zero`, no ack is at seq 0: in these runs nothing replies before its first
/// commit, so an ack at 0 is not a real `Accepted` reply (the tester's zero-seq check, which is
/// what catches a refusal logged as an ack on a run-level trace; M7V-100 catches it directly).
/// Returns the ack count.
fn truth_acks(run: &TruthRun, allow_zero: bool) -> usize {
    use rdb_core::contracts::trace::{ApplyOutcome, DurabilityClass};
    let mut acks = 0;
    for event in &run.trace.events {
        let TraceKind::ReplicationAck {
            from_node,
            peer_boot,
            contiguous_seq,
            contiguous_digest,
            durability_class,
            accepted,
            reject_reason,
            ..
        } = &event.kind
        else {
            continue;
        };
        acks += 1;
        assert!(*accepted && reject_reason.is_none());
        assert_eq!(*durability_class, DurabilityClass::Buffered);
        assert_eq!(
            *from_node, event.node,
            "event {}: at the acker",
            event.event_id.0
        );
        assert_eq!(*peer_boot, event.boot);
        assert!(
            allow_zero || contiguous_seq.0 > 0,
            "event {}: node {} logged an accepted ack at seq 0",
            event.event_id.0,
            event.node.0
        );
        if contiguous_seq.0 == 0 {
            continue;
        }
        let applied = run.trace.events.iter().any(|line| {
            line.event_id < event.event_id
                && line.node == event.node
                && matches!(
                    line.kind,
                    TraceKind::BatchApply { seq, entry_digest, outcome: ApplyOutcome::Applied, .. }
                        if seq == *contiguous_seq && entry_digest == *contiguous_digest
                )
        });
        assert!(
            applied,
            "event {}: node {} acked seq {} with no earlier Applied BatchApply of it",
            event.event_id.0, event.node.0, contiguous_seq.0
        );
    }
    acks
}

/// Every tick-0 `BatchApply` (a preload) carries the role its node holds in `plan`'s pin.
/// Returns how many were checked.
fn truth_preload_roles(plan: &RunPlan, run: &TruthRun) -> usize {
    let mut checked = 0;
    for event in &run.trace.events {
        let TraceKind::BatchApply { role, .. } = &event.kind else {
            continue;
        };
        if event.logical_tick != 0 {
            continue;
        }
        let pinned = plan
            .cluster
            .partitions
            .iter()
            .filter(|partition| partition.partition == event.partition)
            .flat_map(|partition| partition.config.members.iter())
            .find(|member| member.node == event.node)
            .map_or(ReplicaRole::RegularSecondary, |member| member.role);
        assert_eq!(
            *role, pinned,
            "event {}: preload role on node {}",
            event.event_id.0, event.node.0
        );
        checked += 1;
    }
    checked
}

/// Applies, syncs and acks against engine state, and no oracle violation on a correct kernel's
/// recorded run.
fn truth_all(name: &str, run: &TruthRun) {
    let applies = truth_applies(run);
    let syncs = truth_syncs(run);
    let acks = truth_acks(run, false);
    let report = support::oracle::Oracle::new().judge(&run.trace);
    tracing::info!(name, err = ?run.err, ?applies, ?syncs, acks, "recorder truth");
    assert!(
        report.violations().is_empty(),
        "{name}: the oracle found a violation on a correct kernel's run: {:?}",
        report.violations()
    );
}

/// The `DurabilityAdvance` lines on `node` in generation 1, as `(durable_seq, outcome)`.
fn truth_sync_lines(
    run: &TruthRun,
    node: NodeId,
) -> Vec<(u64, rdb_core::contracts::trace::SyncOutcome)> {
    run.trace
        .events
        .iter()
        .filter(|event| event.node == node)
        .filter_map(|event| match &event.kind {
            TraceKind::DurabilityAdvance {
                generation: Generation(1),
                durable_seq,
                outcome,
                ..
            } => Some((durable_seq.0, *outcome)),
            _ => None,
        })
        .collect()
}

/// M7V-96. A commit that fails is a `Failed` `BatchApply`, never `Applied`, and every ack on the
/// run rests on an earlier `Applied` line of its seq, so the failed attempt is never acked. The
/// spine with a planned `WriteFailed` on node 4 (tester probe p3; kills mutant t1, a failed
/// commit logged `Applied`).
#[retcd_test]
fn m7v_96_recording_a_failed_commit_is_a_failed_batch_apply_and_never_acked() {
    use rdb_core::contracts::trace::ApplyOutcome;
    support::preamble();
    let mut plan = spine_plan();
    plan.storage_ops.push(StorageOp::Fail {
        node: NodeId(4),
        fault: StorageFault::WriteFailed,
    });
    let run = truth_run(&plan);
    truth_all("spine + WriteFailed on node 4", &run);
    let on_4: Vec<_> = run
        .trace
        .events
        .iter()
        .filter(|event| event.node == NodeId(4))
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply {
                generation,
                seq,
                outcome,
                ..
            } => Some((generation.0, seq.0, *outcome)),
            _ => None,
        })
        .collect();
    tracing::info!(?on_4, "node 4's batch_apply lines");
    assert!(
        on_4.iter().any(|line| line.2 == ApplyOutcome::Failed),
        "the planned WriteFailed is a Failed line: {on_4:?}"
    );
}

/// M7V-97. A flush that fails is a `Failed` `DurabilityAdvance` at the watermark the engine still
/// holds, never `Synced`. The spine with a planned `FlushFailed` on node 2 (tester probe p4;
/// kills mutant t2, a `FlushFailed` sync logged `Synced`).
#[retcd_test]
fn m7v_97_recording_a_failed_flush_is_a_failed_durability_advance_never_synced() {
    use rdb_core::contracts::trace::SyncOutcome;
    support::preamble();
    let mut plan = spine_plan();
    plan.storage_ops.push(StorageOp::Fail {
        node: PEER,
        fault: StorageFault::FlushFailed,
    });
    let run = truth_run(&plan);
    truth_all("spine + FlushFailed on node 2", &run);
    let on_peer = truth_sync_lines(&run, PEER);
    tracing::info!(?on_peer, "node 2's durability_advance lines");
    assert_eq!(
        on_peer.first(),
        Some(&(0, SyncOutcome::Failed)),
        "{on_peer:?}"
    );
}

/// M7V-98. A `ShortFlush` completes with less durable than was captured, and the line is
/// `Partial` at the engine's short watermark: the `SyncOutcome` contract says only `Synced`
/// publishes the captured prefixes and `Partial` is a flush that completed partially (lead
/// ruling V-R36). The spine with `ShortFlush { through: 1 }` on node 2, whose F1 sync captures
/// the head at 2 (tester probe p5, finding F3; kills mutant t3, `Synced` at the captured seq).
#[retcd_test]
fn m7v_98_recording_a_short_flush_is_partial_at_the_engines_short_watermark() {
    use rdb_core::contracts::ids::AppliedSeq;
    use rdb_core::contracts::trace::SyncOutcome;
    support::preamble();
    let mut plan = spine_plan();
    plan.storage_ops.push(StorageOp::ShortFlush {
        node: PEER,
        through: AppliedSeq(1),
    });
    let run = truth_run(&plan);
    truth_all("spine + ShortFlush through 1 on node 2", &run);
    let first = run.trace.events.iter().find_map(|event| match &event.kind {
        TraceKind::DurabilityAdvance {
            durable_seq,
            outcome,
            captured,
            ..
        } if event.node == PEER => Some((durable_seq.0, *outcome, captured.clone())),
        _ => None,
    });
    tracing::info!(?first, "node 2's first durability_advance under ShortFlush");
    assert_eq!(
        first,
        Some((
            1,
            SyncOutcome::Partial,
            vec![(PartitionId(1), Seq(SPINE_HEAD))]
        )),
        "captured the head, flushed less, and says so"
    );
}

/// One sync of `through` on a bare engine, and the line [`semantic::durability_lines`] writes
/// for it: `(watermark before, watermark after, line's durable_seq, line's outcome)`.
fn truth_unit_sync(
    engine: &mut rdb_sim::storage::memory::MemoryEngine,
    through: u64,
) -> (u64, u64, u64, rdb_core::contracts::trace::SyncOutcome) {
    use rdb_core::contracts::ids::AppliedSeq;
    use rdb_core::contracts::storage::CapturedPrefix;
    let (partition, generation) = (PartitionId(1), Generation(1));
    let captured = vec![CapturedPrefix {
        partition,
        generation,
        through: AppliedSeq(through),
    }];
    let before = engine.durable(partition, generation).0;
    let synced = engine.sync_wal_through(captured.clone());
    let lines =
        rdb_sim::harness::semantic::durability_lines(engine, 7, &captured, synced.as_deref());
    let after = engine.durable(partition, generation).0;
    let (_, line) = lines
        .into_iter()
        .next()
        .expect("one line per captured prefix");
    let TraceKind::DurabilityAdvance {
        durable_seq,
        outcome,
        ..
    } = line
    else {
        unreachable!("durability_lines writes DurabilityAdvance")
    };
    tracing::info!(
        through,
        before,
        after,
        durable = durable_seq.0,
        ?outcome,
        "unit sync"
    );
    (before, after, durable_seq.0, outcome)
}

/// M7V-99. `durability_lines` over a bare engine names the engine's watermark and the sync's real
/// outcome, case by case: a first sync through 1 is `Synced` at 1; a `ShortFlush` of a capture
/// through 2 is `Partial` at its short watermark 1 (V-R36); the whole sync is `Synced` at 2; a
/// repeat, which moves nothing, is still `Synced` at 2 (tester F2: `Synced` names the watermark,
/// not a movement); a capture below the watermark is `Synced` at the engine's 2, not the
/// captured 1; a capture above what was applied gets only 2 and is `Partial` (V-R36);
/// `FalseDurable` is `Partial` at the unmoved watermark; `FlushFailed` is `Failed` (tester probe
/// p7; kills mutants t2 and t3).
#[retcd_test]
fn m7v_99_recording_a_bare_engines_syncs_names_its_watermark_and_the_real_outcome() {
    use rdb_core::contracts::ids::AppliedSeq;
    use rdb_core::contracts::trace::SyncOutcome::{Failed, Partial, Synced};
    use rdb_sim::storage::memory::MemoryEngine;
    support::preamble();
    let mut engine = MemoryEngine::new(PEER);
    for batch in spine_history().batches {
        engine.commit(batch).expect("commit");
    }
    assert_eq!(truth_unit_sync(&mut engine, 1), (0, 1, 1, Synced), "first");
    engine
        .inject(StorageOp::ShortFlush {
            node: PEER,
            through: AppliedSeq(1),
        })
        .expect("planned");
    assert_eq!(truth_unit_sync(&mut engine, 2), (1, 1, 1, Partial), "short");
    assert_eq!(truth_unit_sync(&mut engine, 2), (1, 2, 2, Synced), "whole");
    assert_eq!(truth_unit_sync(&mut engine, 2), (2, 2, 2, Synced), "repeat");
    assert_eq!(
        truth_unit_sync(&mut engine, 1),
        (2, 2, 2, Synced),
        "below the watermark"
    );
    assert_eq!(
        truth_unit_sync(&mut engine, 9),
        (2, 2, 2, Partial),
        "above applied"
    );
    engine
        .inject(StorageOp::FalseDurable {
            node: PEER,
            through: AppliedSeq(5),
        })
        .expect("planned");
    assert_eq!(
        truth_unit_sync(&mut engine, 2),
        (2, 2, 2, Partial),
        "false durable"
    );
    engine
        .inject(StorageOp::Fail {
            node: PEER,
            fault: StorageFault::FlushFailed,
        })
        .expect("planned");
    assert_eq!(truth_unit_sync(&mut engine, 2), (2, 2, 2, Failed), "failed");
}

/// A frame carrying `body`, as R1 on node 3 would send it.
fn truth_frame(body: Bytes) -> Frame {
    Frame {
        id: MessageId(1),
        protocol: rdb_core::contracts::version::ENVELOPE_VERSION,
        config: ConfigVersion(1),
        sender: spine_prior(),
        body,
    }
}

/// M7V-100. Only an `Accepted` reply is a `ReplicationAck`, and it carries that ack verbatim.
///
/// Two surfaces. `semantic::ack_line` classifies every reply kind and an append body (tester
/// probe p8). And `Semantic::record` is driven with R1's replies from node 3 **while node 3 holds a
/// real receiver** (the rebuild's copy 2): every refusal and the other non-acks write no line,
/// and the one `Accepted` writes one. That second half is the deterministic kill for mutant t4
/// (a refusal logged as the receiver's current ack), which the run-level rows catch only by the
/// zero-seq heuristic (tester F1's closure).
#[retcd_test]
fn m7v_100_recording_only_an_accepted_reply_is_a_replication_ack() {
    use rdb_core::contracts::envelope::{AppendAck, AppendOutcome, AppendReject, ReplicaProgress};
    use rdb_core::contracts::event::Event;
    use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, ReceivedSeq};
    use rdb_core::contracts::trace::DurabilityClass;
    use rdb_core::contracts::transport::SendEffect;
    use rdb_core::replication::wire::encode_reply;
    use rdb_sim::harness::semantic::{ack_line, Semantic};
    support::preamble();
    let ack = AppendAck {
        partition: PartitionId(1),
        generation: Generation(2),
        owner_epoch: OwnerEpoch(3),
        config_version: ConfigVersion(4),
        from: NodeId(3),
        boot: BootId(5),
        role: ReplicaRole::Shadow,
        progress: ReplicaProgress {
            received: ReceivedSeq(9),
            buffered_applied: AppliedSeq(7),
            durable: DurableSeq(6),
        },
        digest_at_buffered: spine_digest(1),
    };
    let accepted = truth_frame(encode_reply(&AppendOutcome::Accepted(ack)));
    assert_eq!(
        ack_line(NodeId(1), &accepted),
        Some(TraceKind::ReplicationAck {
            from_node: NodeId(3),
            to_node: NodeId(1),
            peer_role: ReplicaRole::Shadow,
            peer_boot: BootId(5),
            config_version: ConfigVersion(4),
            generation: Generation(2),
            owner_epoch: OwnerEpoch(3),
            contiguous_seq: Seq(7),
            contiguous_digest: spine_digest(1),
            durability_class: DurabilityClass::Buffered,
            accepted: true,
            reject_reason: None,
        })
    );
    let others: Vec<Frame> = [
        AppendOutcome::AlreadyHave,
        AppendOutcome::Busy {
            accepted_through: Seq(3),
        },
        AppendOutcome::ProbeDigestAt { seq: Seq(2) },
        AppendOutcome::Rejected(AppendReject::IncompatibleVersion),
    ]
    .iter()
    .map(|outcome| truth_frame(encode_reply(outcome)))
    .collect();
    for frame in &others {
        assert_eq!(ack_line(NodeId(1), frame), None, "{frame:?}");
    }
    // An append body, not a reply, is never an ack either.
    let batch = spine_history().batches[0].clone();
    let (record, _) = rdb_sim::storage::history::history_writes(&batch, Seq(1)).expect("a record");
    assert_eq!(ack_line(NodeId(1), &truth_frame(record)), None);

    // `Semantic::record`, against a dispatcher where node 3 holds copy 2's receiver.
    let plan = rebuild_plan(Vec::new());
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the rebuild runs");
    tracing::info!(stop = ?report.stop, "rebuild ran");
    let dispatcher = runner.dispatcher();
    assert!(
        dispatcher
            .replication()
            .receiver(NodeId(3), PartitionId(1))
            .is_some(),
        "node 3 holds a receiver, so a forged ack would have one to copy"
    );
    let site = Site {
        at: Tick(1),
        node: NodeId(3),
        boot: BOOT,
        partition: PartitionId(1),
        correlation: CorrelationId(1),
    };
    let event = Event {
        id: EventId(1),
        at: Tick(1),
        node: NodeId(3),
        boot: BOOT,
        partition: PartitionId(1),
        correlation: CorrelationId(1),
        kind: EventKind::Timer(rdb_core::contracts::time::TimerFired {
            id: TimerId(1),
            version: TimerVersion(0),
            scheduled_at: Tick(1),
        }),
    };
    let send = |frame: &Frame| Effect {
        correlation: CorrelationId(1),
        from: ModuleName::Replication,
        partition: PartitionId(1),
        kind: EffectKind::Send(SendEffect::Unicast {
            to: NODE,
            frame: frame.clone(),
        }),
    };
    let mut recorder = Recorder::new();
    recorder
        .begin(header(Provenance::Authored {
            case: String::from("m7v-100"),
        }))
        .expect("a header");
    let mut semantic = Semantic::default();
    let refusals: Vec<Effect> = others.iter().map(send).collect();
    semantic
        .record(
            &mut recorder,
            site,
            ModuleName::Replication,
            &event,
            &refusals,
            dispatcher,
            &Budgets::SPEC_DEFAULTS,
        )
        .expect("recorded");
    assert_eq!(
        recorder.events().len(),
        0,
        "no reply but Accepted is a line: {:?}",
        recorder.events()
    );
    semantic
        .record(
            &mut recorder,
            site,
            ModuleName::Replication,
            &event,
            &[send(&accepted)],
            dispatcher,
            &Budgets::SPEC_DEFAULTS,
        )
        .expect("recorded");
    let kinds: Vec<&TraceKind> = recorder.events().iter().map(|event| &event.kind).collect();
    assert_eq!(
        kinds,
        vec![&ack_line(NODE, &accepted).expect("an ack")],
        "the Accepted reply is one line, verbatim"
    );
}

/// M7V-101. On the spine and on the rebuild, every recorded line matches engine state: each
/// `Applied` apply is a record the engine holds with those digests, each `Synced` sync is at or
/// below the engine's watermark with its digest, each ack rests on an earlier `Applied` apply
/// and none is at seq 0, and each preload carries its node's pinned role (tester probes p1 and
/// p1b; kills mutant t5, a preload logged `Primary` on a secondary, and t4 by the zero-seq check).
#[retcd_test]
fn m7v_101_recording_every_line_matches_engine_state_on_the_spine_and_the_rebuild() {
    support::preamble();
    let rebuild = truth_run(&rebuild_plan(Vec::new()));
    truth_all("rebuild", &rebuild);
    assert!(rebuild.err.is_none(), "{:?}", rebuild.err);
    let (applied, _) = truth_applies(&rebuild);
    assert!(
        applied > 0 && truth_acks(&rebuild, false) > 0,
        "lines to check"
    );

    let plan = spine_plan();
    let spine = truth_run(&plan);
    truth_all("spine", &spine);
    assert!(spine.err.is_none(), "{:?}", spine.err);
    let roles = truth_preload_roles(&plan, &spine);
    tracing::info!(roles, "preload lines role-checked");
    assert!(roles > 0, "the spine preloads");
    // The pin gives the survivors three roles; a check that saw only one would pass a mutant
    // that forces every preload to it.
    let seen: std::collections::BTreeSet<ReplicaRole> = spine
        .trace
        .events
        .iter()
        .filter(|event| event.logical_tick == 0)
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply { role, .. } => Some(*role),
            _ => None,
        })
        .collect();
    assert!(seen.len() > 1, "more than one preload role: {seen:?}");
}
