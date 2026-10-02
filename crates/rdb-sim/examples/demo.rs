//! A small, honest demo of what the rDB simulator does today.
//!
//! ```text
//! cargo run -p rdb-sim --example demo -- <out.json>
//! node samples/rdb-demo/view.mjs <out.json> <out.html>
//! ```
//!
//! It runs ONE deterministic scenario on three nodes and one partition, through the public
//! `rdb_sim` API only (`RunPlan`, `Runner`, `SeedEvent`, the dispatcher and the control store):
//!
//! 1. Node 3 owns partition 1 at generation 1, epoch 1. Its A1 takes a grant.
//! 2. Node 3 crashes and restarts under boot 2. Its new process cannot take a grant, because the
//!    old boot's grant record is still held.
//! 3. Placement (seeded by this demo, because the planner does not exist yet) hands F1 a recovery
//!    plan and a fencing proof. F1 selects generation 2, epoch 2 from the survivors on nodes 1
//!    and 2. A1 on node 1 takes the grant. L1 pauses writes, then lifts the pause.
//! 4. A model of the planner's grant-clearing service removes node 3's stale grant once it is
//!    provably expired. Node 3 acquires, reloads the partition records, and is not the owner.
//! 5. A client write goes to node 1 (A1 answers three gates, P1 publishes, the client gets
//!    `Success`). A write sent to node 3 is refused. An event addressed to node 3's dead boot is
//!    dropped.
//!
//! What the printed story is made of:
//!
//! * Lines tagged `trace` are built from recorded trace events, one rule per event kind. Nothing
//!   in them is typed by hand about the run; counts, ticks, nodes and numbers come from the
//!   event. Ticks are the simulator's logical clock.
//! * Lines tagged `scenario` are things THIS demo did (crash, restart, seed a plan, call the grant
//!   service, send a write). They are not trace events and are labelled as such.
//! * Lines tagged `probe` read kernel state after the run. They are not trace events either.
//!
//! What this does NOT show: T1 reports `Unavailable` (held by lead ruling V-R40), so this is a
//! look at the simulator's write path and not a claim that client transactions ship. The clock is
//! logical; there is no network, no disk and no real process. There is no random seed: the
//! scenario is authored and the simulator draws no random numbers.

use std::collections::BTreeMap;
use std::process::ExitCode;

use bytes::Bytes;
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::AuthorityTimer;
use rdb_core::contracts::authority::{
    AuthorityView, DenyReason, FencingProof, Lineage, Revocation,
};
use rdb_core::contracts::control::{ControlKey, ReadOutcome};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{
    Budgets, ClientEvent, Effect, EffectKind, EventKind, KernelEvent, ModuleName,
};
use rdb_core::contracts::ids::{
    AffinityId, AuthorityGeneration, BootId, ClientId, ConfigVersion, CorrelationId, DurableSeq,
    Generation, GrantId, NodeId, OwnerEpoch, PartitionId, ReplicaRole, RequestId, RequestIdentity,
    Revision, Seq, SnapshotHandle, TenantId, TimerVersion,
};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::recovery::{
    Candidate, LineageAnchor, RecoveryEffect, RecoveryEvent, RecoveryPlan, SurvivorInventory,
};
use rdb_core::contracts::storage::{StorageFault, StoreEffect};
use rdb_core::contracts::time::{Tick, TimerFired};
use rdb_core::contracts::trace::{
    ControlOpKind, ControlOutcomeKind, KernelNote, PackageId, Provenance, TopologyEntry, Trace,
    TraceEvent, TraceKind,
};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::dispatch::Dropped;
use rdb_sim::harness::run::{RunLimits, RunPlan, Runner, SeedEvent, StopReason};
use rdb_sim::sim::cluster::{ClusterConfig, NodeSpec, PartitionSpec};
use rdb_sim::sim::grant_service::{clear_restarted_grant, Clearance};
use rdb_sim::storage::history::canonical_history;
use rdb_sim::storage::StorageOp;
use serde_json::{json, Value};

/// The one partition.
const PART: PartitionId = PartitionId(1);
/// The node that owns the partition at generation 1, then crashes.
const OLD_PRIMARY: NodeId = NodeId(3);
/// The node F1 recovers onto, and that A1 then grants.
const NEW_PRIMARY: NodeId = NodeId(1);
/// The survivor that keeps its history.
const SURVIVOR: NodeId = NodeId(2);
/// The node whose clock the modelled grant service reads: no process of ours runs on it.
const SERVICE: NodeId = NodeId(0);
/// The first boot of every node.
const BOOT1: BootId = BootId(1);
/// The boot node 3 comes back under.
const BOOT2: BootId = BootId(2);
/// How much history the survivors hold (seqs `1..=HEAD` of generation 1).
const HEAD: u64 = 10;
/// When placement hands F1 its plan. The fencing proof follows one tick later.
const PLAN_AT: u64 = 500;
/// How often the host flushes each node. Nothing in the kernel emits a flush, so the scenario
/// schedules them, as the verification bridge does.
const FLUSH_EVERY: u64 = 100;
/// How long after F1's survivor window closes the host first wakes A1 on the new primary.
/// Nothing arms the first `AcquireDue`, so the scenario seeds it.
const ACQUIRE_AFTER_CUTOFF: u64 = 500;
/// When node 3's new process first wakes A1.
const REBOOT_ACQUIRE_AT: u64 = 300;
/// How far ahead the host flushes are scheduled. Flushes past the end of the run stay queued.
const HORIZON: u64 = 14_000;
/// The most events one `run` call may pop.
const MAX_EVENTS: u32 = 50_000;

fn main() -> ExitCode {
    let Some(out) = std::env::args().nth(1) else {
        eprintln!("usage: cargo run -p rdb-sim --example demo -- <out.json>");
        return ExitCode::from(2);
    };
    match run_demo() {
        Ok(doc) => match std::fs::write(&out, doc) {
            Ok(()) => {
                println!("wrote {out}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("cannot write {out}: {error}");
                ExitCode::FAILURE
            }
        },
        Err(message) => {
            eprintln!("demo failed: {message}");
            ExitCode::FAILURE
        }
    }
}

/// One thing the demo itself did (or probed), placed in the story by trace position.
struct Step {
    /// How many trace events existed when it happened.
    after_events: usize,
    /// The simulator's logical tick when it happened.
    tick: u64,
    /// The node it concerns, if one.
    node: Option<NodeId>,
    /// `scenario` (something the demo did) or `probe` (a read of kernel state).
    kind: &'static str,
    /// What it was, in words.
    text: String,
}

/// Collects [`Step`]s.
struct Steps(Vec<Step>);

impl Steps {
    fn push(&mut self, runner: &Runner, node: Option<NodeId>, kind: &'static str, text: String) {
        self.0.push(Step {
            after_events: runner.recorded().len(),
            tick: runner.scheduler().now().0,
            node,
            kind,
            text,
        });
    }
}

fn members() -> Vec<Member> {
    [
        (0u8, 1u32, ReplicaRole::Primary),
        (1, 2, ReplicaRole::RegularSecondary),
        (2, 3, ReplicaRole::RegularSecondary),
    ]
    .into_iter()
    .map(|(copy, node, role)| Member {
        copy: CopyId(copy),
        node: NodeId(node),
        boot: BOOT1,
        role,
    })
    .collect()
}

fn seed(at: u64, node: NodeId, boot: BootId, correlation: u64, kind: EventKind) -> SeedEvent {
    SeedEvent {
        at: Tick(at),
        node,
        boot,
        partition: PART,
        correlation: CorrelationId(correlation),
        kind,
    }
}

/// A1's first `AcquireDue`: what a scenario seeds, because nothing arms it.
fn acquire_due(at: u64, node: NodeId, boot: BootId, correlation: u64) -> SeedEvent {
    seed(
        at,
        node,
        boot,
        correlation,
        EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: TimerVersion(0),
            scheduled_at: Tick(at),
        }),
    )
}

fn limits(deadline: u64) -> RunLimits {
    RunLimits {
        max_events: MAX_EVENTS,
        deadline: Tick(deadline),
    }
}

fn submit(request: u64) -> EventKind {
    EventKind::Client(ClientEvent::Submit(TxnRequest {
        api_version: API_VERSION,
        identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(request),
        },
        affinity: AffinityId(1),
        expected_generation: None,
        remaining_millis: 1_000,
        conditions: Vec::new(),
        mutations: vec![Mutation::Put {
            key: scoped_key(TenantId(1), AffinityId(1), &1u64.to_be_bytes()),
            value: Bytes::from_static(b"demo"),
            expected_version: None,
        }],
    }))
}

/// Run to `deadline`. Anything but reaching it (or an empty queue) is a failure: a refusal at an
/// unbuilt seam would otherwise end the story early and silently.
fn go(runner: &mut Runner, deadline: u64, what: &str) -> Result<(), String> {
    let report = runner
        .run(limits(deadline))
        .map_err(|error| format!("{what}: {error:?}"))?;
    match report.stop {
        StopReason::DeadlineReached { .. } | StopReason::QueueEmpty => Ok(()),
        other => Err(format!("{what}: run stopped early: {other:?}")),
    }
}

/// Whether the trace holds an L1 admission line that lets writes in.
fn writes_allowed(runner: &Runner) -> bool {
    runner.recorded().iter().any(|event| {
        matches!(
            &event.kind,
            TraceKind::KernelNoted {
                note: KernelNote::SetAdmission { state },
                ..
            } if state.allow
        )
    })
}

#[allow(clippy::too_many_lines)]
fn run_demo() -> Result<String, String> {
    let e = |what: &'static str| move |error: rdb_sim::SimError| format!("{what}: {error:?}");
    let prior = Lineage {
        partition: PART,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    };
    let next = Lineage {
        partition: PART,
        generation: Generation(2),
        owner_epoch: OwnerEpoch(2),
    };
    let config = PartitionConfig::new(PART, ConfigVersion(1), members());

    // ---- The plan: topology, survivors, and the partition record that names node 3. ----
    let cluster = ClusterConfig {
        nodes: (1u16..=3)
            .map(|node| NodeSpec {
                node: NodeId(u32::from(node)),
                boot: BOOT1,
                failure_domain: node,
                core_sets: 1,
            })
            .collect(),
        partitions: vec![PartitionSpec {
            partition: PART,
            config: config.clone(),
        }],
        initial_config_version: ConfigVersion(1),
    };
    let mut plan = RunPlan::new(cluster);
    plan.provenance = Provenance::Authored {
        case: String::from("demo-primary-crash-failover"),
    };
    plan.topology = config
        .members
        .iter()
        .map(|member| TopologyEntry {
            partition: PART,
            node: member.node,
            role: member.role,
            config_version: ConfigVersion(1),
        })
        .collect();
    let record = PartitionRecord {
        partition: PART,
        owner: OLD_PRIMARY,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        lifecycle: PartitionLifecycle::Serving,
    };
    plan.control_records = vec![(ControlKey::Partition(PART), record.encode())];
    let history = canonical_history(prior, ConfigVersion(1), HEAD).map_err(e("history"))?;
    for node in [NEW_PRIMARY, SURVIVOR] {
        for batch in &history.batches {
            plan.preloads.push((node, batch.clone()));
        }
        plan.preload_durable
            .push((node, PART, Generation(1), DurableSeq(HEAD)));
        let copy = config
            .members
            .iter()
            .find(|member| member.node == node)
            .map(|member| member.copy)
            .ok_or_else(|| String::from("survivor not in the configuration"))?;
        plan.survivors.push((
            node,
            PART,
            SurvivorInventory {
                copy,
                anchor_seen: LineageAnchor {
                    lineage: prior,
                    base_seq: Seq(0),
                    base_digest: Digest::ROOT,
                },
                head: (Seq(HEAD), history.digest(HEAD)),
                ladder: (0..=HEAD)
                    .map(|seq| (Seq(seq), history.digest(seq)))
                    .collect(),
                quarantined: None,
            },
        ));
    }
    plan.seed.push(acquire_due(1, OLD_PRIMARY, BOOT1, 1));
    plan.limits = limits(HORIZON);
    let budgets: Budgets = plan.header().map_err(e("header"))?.config.budgets;

    let mut runner = Runner::new(&plan).map_err(e("runner"))?;
    let mut steps = Steps(Vec::new());
    steps.push(
        &runner,
        None,
        "scenario",
        format!(
            "setup: 3 nodes, partition {} owned by node {} at generation {} epoch {}; nodes {} and {} hold seq 1..{HEAD} of generation 1",
            PART.0, OLD_PRIMARY.0, prior.generation.0, prior.owner_epoch.0, NEW_PRIMARY.0, SURVIVOR.0
        ),
    );
    steps.push(
        &runner,
        Some(OLD_PRIMARY),
        "scenario",
        String::from(
            "wake A1 on node 3 at t1 (nothing arms the first acquisition; the scenario seeds it)",
        ),
    );

    // ---- Act 1: node 3 takes its grant. ----
    go(&mut runner, 10, "act 1")?;

    // ---- Act 2: node 3 crashes and restarts under boot 2. ----
    // The crash is planned on the node's engine and taken by the next storage effect, the way
    // the restart rows take it. The restart puts a fresh process under boot 2.
    runner
        .dispatcher_mut()
        .inject_storage(StorageOp::Crash {
            node: OLD_PRIMARY,
            fault: StorageFault::ProcessCrash,
        })
        .map_err(e("plan crash"))?;
    let tripped = runner.carry_out(
        OLD_PRIMARY,
        BOOT1,
        vec![Effect {
            correlation: CorrelationId(9_000),
            from: ModuleName::Transaction,
            partition: PART,
            kind: EffectKind::Store(StoreEffect::Snapshot {
                handle: SnapshotHandle(9_000),
                partition: PART,
            }),
        }],
    );
    if tripped.is_ok() || !runner.dispatcher().is_down(OLD_PRIMARY) {
        return Err(String::from("the planned crash was not taken"));
    }
    steps.push(
        &runner,
        Some(OLD_PRIMARY),
        "scenario",
        String::from("crash node 3 (fault injected through the public storage API: ProcessCrash)"),
    );
    runner
        .dispatcher_mut()
        .restart(OLD_PRIMARY, BOOT2)
        .map_err(e("restart"))?;
    steps.push(
        &runner,
        Some(OLD_PRIMARY),
        "scenario",
        format!(
            "restart node 3 under boot {} (fresh kernel modules)",
            BOOT2.0
        ),
    );
    runner
        .queue(&acquire_due(REBOOT_ACQUIRE_AT, OLD_PRIMARY, BOOT2, 2))
        .map_err(e("queue"))?;
    steps.push(
        &runner,
        Some(OLD_PRIMARY),
        "scenario",
        format!("wake A1 on node 3 (boot 2) at t{REBOOT_ACQUIRE_AT}"),
    );

    // ---- Act 3: placement hands F1 a plan and a fencing proof. ----
    // The plan's pinned configuration names node 3 under the boot it now runs.
    let mut pinned = config.clone();
    for member in &mut pinned.members {
        if member.node == OLD_PRIMARY {
            member.boot = BOOT2;
        }
    }
    let recovery = RecoveryPlan {
        anchor: LineageAnchor {
            lineage: prior,
            base_seq: Seq(0),
            base_digest: Digest::ROOT,
        },
        candidates: pinned
            .members
            .iter()
            .map(|member| Candidate {
                copy: member.copy,
                primary_eligible: true,
                healthy: true,
                within_capacity: true,
                has_valid_grant: true,
            })
            .collect(),
        rebuild_required: pinned.members.iter().map(|member| member.copy).collect(),
        authority_view: AuthorityView {
            lineage: prior,
            grant_id: GrantId(1),
            boot_id: BOOT1,
            authority_generation: AuthorityGeneration(1),
            config_version: ConfigVersion(1),
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: 1_000,
        config: pinned,
    };
    let fence = FencingProof {
        partition: PART,
        prior_generation: prior.generation,
        prior_owner_epoch: prior.owner_epoch,
        prior_grant_id: GrantId(1),
        prior_boot_id: BOOT1,
        revocation: Revocation::DurableDrain {
            ack_revision: Revision(1),
        },
        control_revision: Revision(1),
        decision_tick: Tick(PLAN_AT + 1),
    };
    runner
        .queue(&seed(
            PLAN_AT,
            NEW_PRIMARY,
            BOOT1,
            101,
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(Box::new(
                recovery,
            )))),
        ))
        .map_err(e("queue plan"))?;
    runner
        .queue(&seed(
            PLAN_AT + 1,
            NEW_PRIMARY,
            BOOT1,
            102,
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(Box::new(
                fence,
            )))),
        ))
        .map_err(e("queue fence"))?;
    steps.push(
        &runner,
        Some(NEW_PRIMARY),
        "scenario",
        format!(
            "placement (seeded by the demo) sends F1 on node 1 a recovery plan at t{PLAN_AT} and a fencing proof for generation 1 epoch 1 at t{}",
            PLAN_AT + 1
        ),
    );
    let cutoff = PLAN_AT + 1 + budgets.discovery_window_millis;
    let mut at = cutoff;
    while at <= HORIZON {
        for node in [NEW_PRIMARY, SURVIVOR, OLD_PRIMARY] {
            runner.dispatcher_mut().schedule_flush(Tick(at), node);
        }
        at += FLUSH_EVERY;
    }
    let first_grant_at = cutoff + ACQUIRE_AFTER_CUTOFF;
    runner
        .queue(&acquire_due(first_grant_at, NEW_PRIMARY, BOOT1, 103))
        .map_err(e("queue acquire"))?;
    steps.push(
        &runner,
        Some(NEW_PRIMARY),
        "scenario",
        format!(
            "host flushes every node every {FLUSH_EVERY} ticks from t{cutoff} (the end of F1's survivor window) and wakes A1 on node 1 at t{first_grant_at}"
        ),
    );

    // ---- Act 4: the modelled grant service clears node 3's stale grant. ----
    let old_grant = match runner.control_mut().get(ControlKey::Grant(OLD_PRIMARY)) {
        ReadOutcome::Found { value, .. } => {
            GrantRecord::decode(&value).ok_or_else(|| String::from("undecodable grant record"))?
        }
        other => return Err(format!("node 3 has no grant record: {other:?}")),
    };
    let epsilon = runner
        .dispatcher()
        .clock()
        .control_time(SERVICE)
        .error_millis;
    let threshold = old_grant
        .expiry_utc_ms
        .checked_add_unsigned(epsilon + budgets.dispatch_margin_millis)
        .ok_or_else(|| String::from("grant expiry overflow"))?;
    let proven = u64::try_from(threshold).map_err(|error| error.to_string())? + 1;
    for service_at in [proven - 1, proven] {
        go(&mut runner, service_at, "run to service")?;
        let now = Tick(service_at);
        runner
            .dispatcher_mut()
            .clock_mut()
            .advance(now)
            .map_err(e("advance clock"))?;
        let sample = runner.dispatcher().clock().control_time(SERVICE);
        let clearance = clear_restarted_grant(
            runner.control_mut(),
            OLD_PRIMARY,
            BOOT2,
            sample,
            now,
            &budgets,
        )
        .map_err(e("grant service"))?;
        let verdict = match clearance {
            Clearance::Cleared(revision) => {
                format!(
                    "cleared node 3's old grant record (control revision {})",
                    revision.0
                )
            }
            other => format!("left node 3's old grant record in place: {other:?}"),
        };
        steps.push(
            &runner,
            Some(OLD_PRIMARY),
            "scenario",
            format!(
                "modelled grant service at t{service_at} (old grant expired at t{}, proof needs t{proven}): {verdict}",
                old_grant.expiry_utc_ms
            ),
        );
    }

    // ---- Act 5: wait for L1 to let writes in, then write. ----
    let mut waited = proven;
    while !writes_allowed(&runner) {
        waited += 500;
        if waited > HORIZON {
            return Err(String::from("L1 never allowed writes inside the horizon"));
        }
        go(&mut runner, waited, "wait for L1")?;
    }
    let write_at = waited + 1;
    runner
        .queue(&seed(write_at, NEW_PRIMARY, BOOT1, 200, submit(11)))
        .map_err(e("queue write"))?;
    runner
        .queue(&seed(write_at + 200, OLD_PRIMARY, BOOT2, 201, submit(12)))
        .map_err(e("queue write"))?;
    let stale_event = seed(write_at + 300, OLD_PRIMARY, BOOT1, 202, submit(13));
    let stale_correlation = stale_event.correlation;
    runner.queue(&stale_event).map_err(e("queue write"))?;
    steps.push(
        &runner,
        None,
        "scenario",
        format!(
            "L1 has allowed writes; client sends request 11 to node 1 at t{write_at}, request 12 to node 3 (boot 2) at t{}, request 13 addressed to node 3's dead boot 1 at t{}",
            write_at + 200,
            write_at + 300
        ),
    );
    go(&mut runner, write_at + 1_000, "act 5")?;

    // ---- Probes: kernel state after the run. Not trace events. ----
    let now = runner.dispatcher().clock().now();
    let probe_admit = |runner: &Runner, node: NodeId, lineage: Lineage| -> Result<String, String> {
        runner
            .dispatcher()
            .authority(node)
            .map(|authority| format!("{:?}", authority.may_admit_at(lineage, now, &budgets)))
            .ok_or_else(|| format!("no A1 on node {}", node.0))
    };
    let probes = [
        (
            OLD_PRIMARY,
            format!(
                "node 3 asked to admit the old lineage (generation 1, epoch 1): {}",
                probe_admit(&runner, OLD_PRIMARY, prior)?
            ),
        ),
        (
            NEW_PRIMARY,
            format!(
                "node 1 asked to admit the new lineage (generation 2, epoch 2): {}",
                probe_admit(&runner, NEW_PRIMARY, next)?
            ),
        ),
        (
            NEW_PRIMARY,
            format!(
                "node 1 asked to admit the old lineage (generation 1, epoch 1): {}",
                probe_admit(&runner, NEW_PRIMARY, prior)?
            ),
        ),
    ];
    for (node, text) in probes {
        steps.push(&runner, Some(node), "probe", text);
    }
    for node in [NEW_PRIMARY, OLD_PRIMARY] {
        let served: Vec<u32> = runner
            .dispatcher()
            .authority(node)
            .map(|authority| authority.view().served.keys().map(|p| p.0).collect())
            .ok_or_else(|| format!("no A1 on node {}", node.0))?;
        steps.push(
            &runner,
            Some(node),
            "probe",
            format!("partitions A1 on node {} serves: {served:?}", node.0),
        );
    }
    let dropped: Vec<String> = runner
        .dispatcher()
        .dropped()
        .iter()
        .filter_map(|dropped| match dropped {
            Dropped::Event { event, reason } if event.correlation == stale_correlation => {
                Some(format!("{reason:?}"))
            }
            _ => None,
        })
        .collect();
    steps.push(
        &runner,
        Some(OLD_PRIMARY),
        "probe",
        format!("request 13, addressed to node 3's dead boot 1, was dropped by the dispatcher: {dropped:?}"),
    );

    let trace = runner.finish().map_err(e("finish"))?;
    let story = narrate(&trace);

    // ---- Print the story: trace rows and scenario steps, interleaved by trace position. ----
    println!("rDB demo: a primary crashes, a survivor takes over, the old primary is fenced");
    println!("(ticks are logical; [trace] rows come from trace events; [scenario] and [probe] rows are not)");
    println!();
    let mut pending = steps.0.iter().peekable();
    for row in &story.rows {
        while let Some(step) = pending.next_if(|step| step.after_events <= row.index) {
            print_step(step);
        }
        println!("[trace]    {}", row.line());
    }
    for step in pending {
        print_step(step);
    }
    println!();
    println!(
        "{} trace events in all; the rows above stand for {} of them; the other events are plumbing: {}",
        trace.events.len(),
        story.shown_events,
        story
            .plumbing
            .iter()
            .map(|(name, count)| format!("{name} {count}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let capabilities: Vec<Value> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::Capability { package, state } => Some(json!({
                "package": package_name(*package),
                "state": format!("{state:?}"),
            })),
            _ => None,
        })
        .collect();
    let doc = json!({
        "format": "rdb-demo/1",
        "summary": {
            "case": "demo-primary-crash-failover",
            "provenance": trace.header.provenance,
            "seed": Value::Null,
            "seed_note": "authored scenario; the simulator draws no random numbers",
            "nodes": [1, 2, 3],
            "partition": PART.0,
            "old_primary": OLD_PRIMARY.0,
            "new_primary": NEW_PRIMARY.0,
            "event_cap": trace.header.config.event_cap,
            "trace_events": trace.events.len(),
            "capabilities": capabilities,
            "steps": steps.0.iter().map(|step| json!({
                "after_events": step.after_events,
                "tick": step.tick,
                "node": step.node.map(|node| node.0),
                "kind": step.kind,
                "text": step.text,
            })).collect::<Vec<_>>(),
            "timeline": story.rows.iter().map(Row::json).collect::<Vec<_>>(),
            "plumbing": story.plumbing,
        },
        "trace": trace,
    });
    let mut text = serde_json::to_string(&doc).map_err(|error| error.to_string())?;
    text.push('\n');
    Ok(text)
}

fn print_step(step: &Step) {
    let who = step
        .node
        .map_or_else(|| String::from("      "), |node| format!("node {}", node.0));
    println!("[{}] t{:<6} {who}  {}", step.kind, step.tick, step.text);
}

// ---------------------------------------------------------------------------------------------
// The narrator: trace events in, story rows out. One rule per kind of event.
// ---------------------------------------------------------------------------------------------

/// One row of the story, made from one or more trace events.
struct Row {
    /// Position of the first event in the trace.
    index: usize,
    first_event: u64,
    last_event: u64,
    tick: u64,
    last_tick: u64,
    node: u32,
    boot: u64,
    /// `A1`, `P1`, `F1`, `R1`, `L1`, `T1`, `H1`, `M1`, `client` or `env`.
    module: &'static str,
    text: String,
    count: u32,
}

impl Row {
    fn line(&self) -> String {
        let who = if self.node == 0 {
            String::from("         ")
        } else {
            format!("n{}/boot {}", self.node, self.boot)
        };
        let repeat = if self.count > 1 {
            format!(" (x{}, last at t{})", self.count, self.last_tick)
        } else {
            String::new()
        };
        format!(
            "t{:<6} {who:<9} {:<6} {}{repeat}",
            self.tick, self.module, self.text
        )
    }

    fn json(&self) -> Value {
        json!({
            "index": self.index,
            "first_event": self.first_event,
            "last_event": self.last_event,
            "tick": self.tick,
            "last_tick": self.last_tick,
            "node": self.node,
            "boot": self.boot,
            "module": self.module,
            "text": self.text,
            "count": self.count,
        })
    }
}

/// The finished story.
struct Story {
    rows: Vec<Row>,
    /// How many trace events the rows account for.
    shown_events: usize,
    /// How many events of each kind were left out as plumbing.
    plumbing: BTreeMap<String, u64>,
}

#[derive(Default)]
struct Narrator {
    rows: Vec<Row>,
    /// Repeats and runs, by key: which row they extend.
    by_key: BTreeMap<String, usize>,
    /// Sequence runs, by key: first seq, last seq and the row showing them.
    runs: BTreeMap<String, (u64, u64, usize)>,
    /// Copy lists, by key.
    lists: BTreeMap<String, Vec<u64>>,
    /// The highest seq each `(from node, generation)` has acknowledged.
    acked: BTreeMap<(u32, u64), u64>,
    plumbing: BTreeMap<String, u64>,
    shown_events: usize,
}

fn package_name(package: PackageId) -> String {
    format!("{package:?}")
}

fn variant_name(value: &Value) -> String {
    match value {
        Value::String(name) => name.clone(),
        Value::Object(map) => map.keys().next().cloned().unwrap_or_default(),
        other => other.to_string(),
    }
}

fn narrate(trace: &Trace) -> Story {
    let mut narrator = Narrator::default();
    for (index, event) in trace.events.iter().enumerate() {
        narrator.event(index, event);
    }
    Story {
        rows: narrator.rows,
        shown_events: narrator.shown_events,
        plumbing: narrator.plumbing,
    }
}

impl Narrator {
    fn plumb(&mut self, name: &str) {
        *self.plumbing.entry(name.to_owned()).or_insert(0) += 1;
    }

    /// Add a row, or extend the row already under `key`.
    fn add(
        &mut self,
        index: usize,
        event: &TraceEvent,
        module: &'static str,
        key: Option<String>,
        text: String,
    ) {
        self.shown_events += 1;
        if let Some(existing) = key.as_ref().and_then(|key| self.by_key.get(key)).copied() {
            let row = &mut self.rows[existing];
            row.count += 1;
            row.last_event = event.event_id.0;
            row.last_tick = event.logical_tick;
            return;
        }
        if let Some(key) = key {
            self.by_key.insert(key, self.rows.len());
        }
        self.rows.push(Row {
            index,
            first_event: event.event_id.0,
            last_event: event.event_id.0,
            tick: event.logical_tick,
            last_tick: event.logical_tick,
            node: event.node.0,
            boot: event.boot.0,
            module,
            text,
            count: 1,
        });
    }

    /// Add a row for a growing run (`seq a..b`), rewriting its text as the run grows.
    fn add_run(
        &mut self,
        index: usize,
        event: &TraceEvent,
        module: &'static str,
        key: String,
        seq: u64,
        render: &dyn Fn(u64, u64) -> String,
    ) {
        if let Some((first, last, row)) = self.runs.get(&key).copied() {
            if last + 1 == seq {
                self.runs.insert(key, (first, seq, row));
                self.rows[row].text = render(first, seq);
                self.rows[row].last_event = event.event_id.0;
                self.rows[row].last_tick = event.logical_tick;
                self.shown_events += 1;
                return;
            }
        }
        // A run that cannot be extended starts a new row.
        self.runs.insert(key, (seq, seq, self.rows.len()));
        self.add(index, event, module, None, render(seq, seq));
    }

    #[allow(clippy::too_many_lines)]
    fn event(&mut self, index: usize, event: &TraceEvent) {
        let node = event.node.0;
        let boot = event.boot.0;
        match &event.kind {
            TraceKind::Capability { .. } => {
                // One row for the nine lines, at the first of them.
                self.add(
                    index,
                    event,
                    "env",
                    Some(String::from("capability")),
                    String::from("capability preamble recorded (see the table)"),
                );
            }
            TraceKind::BatchApply {
                role,
                generation,
                seq,
                ..
            } => {
                let module = if *role == ReplicaRole::Primary {
                    "T1"
                } else {
                    "R1"
                };
                let generation = generation.0;
                if event.logical_tick == 0 {
                    let key = format!("preload/{node}/{generation}");
                    self.add_run(index, event, "M1", key, seq.0, &move |first, last| {
                        format!(
                            "holds generation {generation} seq {first}..{last} from the start (preloaded history)"
                        )
                    });
                } else {
                    let key = format!("apply/{node}/{boot}/{generation}/{}", event.logical_tick);
                    let role = *role;
                    self.add_run(index, event, module, key, seq.0, &move |first, last| {
                        format!("applied generation {generation} seq {first}..{last} as {role:?}")
                    });
                }
            }
            TraceKind::ReplicationAck {
                from_node,
                to_node,
                generation,
                contiguous_seq,
                accepted,
                reject_reason,
                ..
            } => {
                let progress = self.acked.entry((from_node.0, generation.0)).or_insert(0);
                if !*accepted {
                    let text = format!(
                        "node {} refused to acknowledge for node {}: {reject_reason:?}",
                        from_node.0, to_node.0
                    );
                    self.add(index, event, "R1", None, text);
                } else if contiguous_seq.0 > *progress {
                    *progress = contiguous_seq.0;
                    let key = format!(
                        "ack/{}/{}/{}",
                        from_node.0, generation.0, event.logical_tick
                    );
                    let (from, to) = (from_node.0, to_node.0);
                    self.add_run(
                        index,
                        event,
                        "R1",
                        key,
                        contiguous_seq.0,
                        &move |first, last| {
                            if first == last {
                                format!("node {from} acknowledged seq {last} to node {to}")
                            } else {
                                format!("node {from} acknowledged seq {first}..{last} to node {to}")
                            }
                        },
                    );
                } else {
                    self.plumb("ReplicationAck");
                }
            }
            TraceKind::AuthorityDecision {
                gate,
                owner_node,
                owner_epoch,
                generation,
                outcome,
                ..
            } => {
                let text = format!(
                    "{gate:?} gate {outcome:?}: owner node {}, epoch {}, generation {}",
                    owner_node.0, owner_epoch.0, generation.0
                );
                self.add(index, event, "A1", None, text);
            }
            TraceKind::Publish {
                generation,
                seq,
                ack_evidence,
                ..
            } => {
                let acks: Vec<String> = ack_evidence
                    .iter()
                    .map(|ack| format!("node {} ({:?})", ack.node.0, ack.durability))
                    .collect();
                let text = format!(
                    "published generation {} seq {}, acknowledged by {}",
                    generation.0,
                    seq.0,
                    acks.join(" and ")
                );
                self.add(index, event, "P1", None, text);
            }
            TraceKind::ClientOutcomeReported {
                request,
                outcome,
                generation,
                seq,
                delivered,
                ..
            } => {
                let at = seq.map_or_else(String::new, |seq| format!(" seq {}", seq.0));
                let text = format!(
                    "client request {}: {outcome:?} (generation {}{at}, delivered {delivered})",
                    request.0, generation.0
                );
                self.add(index, event, "client", None, text);
            }
            TraceKind::ProtectionState { phase, .. } => {
                let text = format!("protection phase: {phase:?}");
                let key = format!("phase/{node}/{phase:?}");
                self.add(index, event, "L1", Some(key), text);
            }
            TraceKind::ControlInteraction {
                op,
                key,
                prefix,
                outcome,
            } => {
                let scope =
                    prefix.map_or_else(|| String::from("control"), |prefix| format!("{prefix:?}"));
                match (op, key, outcome) {
                    (ControlOpKind::Cas, Some(ControlKey::Grant(owner)), outcome) => {
                        let what = match outcome {
                            ControlOutcomeKind::Committed => String::from("write committed"),
                            ControlOutcomeKind::Conflict => {
                                String::from("write lost (Conflict: the record is held)")
                            }
                            other => format!("write {other:?}"),
                        };
                        let text = format!("grant record for node {}: {what}", owner.0);
                        let key = format!("grant/{node}/{boot}/{outcome:?}");
                        self.add(index, event, "A1", Some(key), text);
                    }
                    (ControlOpKind::Cas, Some(ControlKey::Partition(partition)), outcome) => {
                        let text = format!("partition {} record: write {outcome:?}", partition.0);
                        self.add(index, event, "F1", None, text);
                    }
                    (ControlOpKind::Reload, _, outcome) => {
                        let text = format!("reloaded the {scope} records ({outcome:?})");
                        let key = format!("reload/{node}/{boot}");
                        self.add(index, event, "A1", Some(key), text);
                    }
                    (
                        ControlOpKind::Watch,
                        _,
                        ControlOutcomeKind::Terminated { termination, .. },
                    ) => {
                        let text = format!("control watch on {scope} ended ({termination:?})");
                        self.add(index, event, "H1", None, text);
                    }
                    _ => self.plumb("ControlInteraction"),
                }
            }
            TraceKind::KernelNoted {
                event: cause,
                module,
                note,
            } => {
                self.note(index, event, cause.0, *module, note);
            }
            TraceKind::ModuleDispatch { .. } => self.plumb("ModuleDispatch"),
            TraceKind::DurabilityAdvance { .. } => self.plumb("DurabilityAdvance"),
            other => {
                // Any kind without its own rule still appears, by name, so nothing is hidden.
                let name = serde_json::to_value(other)
                    .map(|value| variant_name(&value))
                    .unwrap_or_default();
                self.add(index, event, "env", None, format!("{name} (see the trace)"));
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn note(
        &mut self,
        index: usize,
        event: &TraceEvent,
        cause: u64,
        module: ModuleName,
        note: &KernelNote,
    ) {
        let node = event.node.0;
        let boot = event.boot.0;
        match note {
            KernelNote::Ignored { .. } => self.plumb("KernelNoted/Ignored"),
            KernelNote::AuthorityFact { fact } => {
                let name = format!("{fact:?}");
                if name == "RecoveryObserved" {
                    self.plumb("KernelNoted/RecoveryObserved");
                } else {
                    let key = format!("fact/{node}/{boot}/{name}");
                    self.add(index, event, "A1", Some(key), format!("A1 fact: {name}"));
                }
            }
            KernelNote::RecoveryFact { effect } => {
                let text = match effect {
                    RecoveryEffect::CloseWindow => String::from("survivor discovery window closed"),
                    RecoveryEffect::Selected(selected) => format!(
                        "chose root generation {}, epoch {}, cutoff seq {}",
                        selected.root.generation.0,
                        selected.root.owner_epoch.0,
                        selected.cutoff_seq.0
                    ),
                    RecoveryEffect::RecordSourceUnavailable { copy, reason } => {
                        format!("copy {} gave no inventory ({reason:?})", copy.0)
                    }
                    other => serde_json::to_value(other)
                        .map(|value| format!("F1 effect: {}", variant_name(&value)))
                        .unwrap_or_default(),
                };
                self.add(index, event, "F1", None, text);
            }
            KernelNote::RecoveredFact { result } => {
                let required: Vec<u8> = result
                    .barrier
                    .required()
                    .iter()
                    .map(|copy| copy.0)
                    .collect();
                let text = format!(
                    "recovery committed: new generation {}, mode {:?}, barrier copies {required:?}, fenced prior generation {} epoch {}",
                    result.new_generation.0,
                    result.mode,
                    result.fenced_prior.prior_generation.0,
                    result.fenced_prior.prior_owner_epoch.0
                );
                self.add(index, event, "F1", None, text);
            }
            KernelNote::RecoveredLanded {
                member,
                revision,
                emitter,
                ..
            } => {
                let text = format!(
                    "node {} learned the recovery (control revision {}, from node {})",
                    member.0, revision.0, emitter.0
                );
                self.add(index, event, "F1", None, text);
            }
            KernelNote::RecoveredDeferred { member, reason, .. } => {
                let text = format!(
                    "recovery notice for node {} deferred ({reason:?})",
                    member.0
                );
                self.add(index, event, "F1", None, text);
            }
            KernelNote::SyncProven { copy, cutoff, .. } => {
                let key = format!("sync/{node}/{cause}");
                let copies = self.lists.entry(key.clone()).or_default();
                copies.push(u64::from(copy.0));
                let text = format!("copies {copies:?} synced through cutoff seq {}", cutoff.0);
                if let Some(row) = self.by_key.get(&key).copied() {
                    self.rows[row].text = text;
                    self.rows[row].last_event = event.event_id.0;
                    self.shown_events += 1;
                } else {
                    self.add(index, event, "F1", Some(key), text);
                }
            }
            KernelNote::SurvivorPlaced { inventory, .. } => {
                let text = format!(
                    "survivor placed: copy {} with head seq {}",
                    inventory.copy.0, inventory.head.0 .0
                );
                self.add(index, event, "F1", None, text);
            }
            KernelNote::SetAdmission { state } => {
                let text = if state.allow {
                    String::from("L1 admission: writes allowed")
                } else {
                    format!("L1 admission: writes refused ({:?})", state.reason)
                };
                self.add(index, event, "L1", None, text);
            }
            KernelNote::PublicationFact { effect } => {
                let value = serde_json::to_value(effect).unwrap_or(Value::Null);
                let status = value.get("Status");
                let request = status
                    .and_then(|status| status.pointer("/request/request"))
                    .map_or_else(String::new, ToString::to_string);
                let outcome = status
                    .and_then(|status| status.get("outcome"))
                    .map(variant_name)
                    .unwrap_or_default();
                let text = if status.is_some() {
                    format!("status for request {request}: {outcome}")
                } else {
                    format!("P1 effect: {}", variant_name(&value))
                };
                self.add(index, event, "P1", None, text);
            }
            other => {
                let name = serde_json::to_value(other)
                    .map(|value| variant_name(&value))
                    .unwrap_or_default();
                self.add(
                    index,
                    event,
                    "env",
                    None,
                    format!("{module:?} note: {name}"),
                );
            }
        }
    }
}
