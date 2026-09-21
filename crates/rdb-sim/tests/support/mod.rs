//! The frozen test-support registry.
//!
//! Integration tests in Rust each compile as their own crate, so a shared helper has to be a
//! module every test file includes. This is that module, and its shape is fixed now so that four
//! teams can add files under it without colliding: one `pub mod` line per area, one owner per
//! area.
//!
//! | Module | Owner | What lives there |
//! |---|---|---|
//! | [`oracle`] | team verification (package O1) | the invariant checkers |
//! | [`scenarios`] | team verification (packages G1, Q1) | generators and the campaign runner |
//!
//! `#![allow(dead_code)]` because each test binary uses a different subset, and without it every
//! test file would warn about the helpers it happens not to call.
//!
//! Every row in this crate is a `#[retcd_test]` (finding K-F-30) and opens with [`preamble`],
//! so each row's JSONL file starts with the three environment `Capability` lines (Q-F-1). Log
//! fields are ids, counts and digests; never a key or value byte (team rules).

#![allow(dead_code)]

pub mod oracle;
pub mod scenarios;

use bytes::Bytes;
use rdb_core::contracts::control::ControlEffect;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{
    Budgets, ClientEvent, Effect, EffectKind, Event, EventKind, ModuleName, StepCtx,
};
use rdb_core::contracts::ids::{
    AffinityId, BatchId, BootId, ClientId, ConfigVersion, CorrelationId, EventId, Generation,
    NodeId, OwnerEpoch, PartitionId, ReplicaRole, RequestId, RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::storage::{Batch, Namespace, Write};
use rdb_core::contracts::time::{ControlTime, Tick};
use rdb_core::contracts::trace::{ControlOutcomeKind, TraceKind};
use rdb_core::contracts::txn::TxnRequest;
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::environment_capabilities;
use rdb_sim::sim::cluster::{ClusterConfig, NodeSpec, PartitionSpec};
use rdb_sim::storage::snapshot::EmptySnapshot;

/// The budgets every row runs under unless it says otherwise.
pub const BUDGETS: Budgets = Budgets::SPEC_DEFAULTS;

/// An empty snapshot of a fresh engine, for stepping a module with no data behind it.
pub const SNAPSHOT: EmptySnapshot = EmptySnapshot::new();

/// The all-zero digest, for rows that must pass a digest they do not care about.
pub const ROOT_DIGEST: Digest = Digest::ROOT;

/// Log the three environment packages' capability states, one line each, as the first thing a
/// row does. What Q-F-1 counts, and the row-level form of
/// [`rdb_core::contracts::trace::TraceKind::Capability`].
pub fn preamble() {
    for (package, state) in environment_capabilities() {
        tracing::info!(?package, ?state, "capability");
    }
}

/// Log one `control interaction` line per drained interaction, for Q-63.
///
/// Tier 3 of `docs/testing/m7-log-fields.md`: a test-local line, not a trace event, so it keeps
/// the spelling Q-63 string-matches. Note that `control interaction` here and
/// `control_interaction` in tier 1 are two different lines — the first is this fixture line, the
/// second is the serialised [`rdb_core::contracts::trace::TraceKind::ControlInteraction`]. Q-63
/// reads this one.
///
/// `termination` and `gap` are flattened out of
/// [`ControlOutcomeKind::Terminated`] rather than left inside the debug rendering of `outcome`,
/// because Q-63 groups by all four and asserts `gap = true` for exactly `RevisionCompacted` and
/// `ResourceExhaustedResumable`. A `gap` buried in a string cannot be grouped on.
pub fn log_control_interactions(interactions: &[TraceKind]) {
    for interaction in interactions {
        let TraceKind::ControlInteraction { op, outcome, .. } = interaction else {
            continue;
        };
        let (name, termination, gap) = match outcome {
            ControlOutcomeKind::Terminated { termination, gap } => {
                ("Terminated", Some(format!("{termination:?}")), Some(*gap))
            }
            other => (outcome_name(other), None, None),
        };
        tracing::info!(
            op = ?op,
            outcome = name,
            termination = termination.as_deref().unwrap_or(""),
            gap = gap.unwrap_or(false),
            "control interaction"
        );
    }
}

/// The bare variant name, so Q-63 can `GROUP BY` it.
const fn outcome_name(outcome: &ControlOutcomeKind) -> &'static str {
    match outcome {
        ControlOutcomeKind::Committed => "Committed",
        ControlOutcomeKind::Conflict => "Conflict",
        ControlOutcomeKind::Unknown => "Unknown",
        ControlOutcomeKind::Unavailable => "Unavailable",
        ControlOutcomeKind::Found => "Found",
        ControlOutcomeKind::Absent => "Absent",
        ControlOutcomeKind::Progress => "Progress",
        ControlOutcomeKind::Terminated { .. } => "Terminated",
    }
}

/// A minimal context on node 1, partition 1, generation 1, at tick zero, sampled now.
///
/// The one place a test builds a [`StepCtx`]. Borrows `SNAPSHOT` and `BUDGETS`, which are
/// `const`, so no test owns simulator state it did not ask for.
#[must_use]
pub fn ctx() -> StepCtx<'static> {
    StepCtx {
        now: Tick::ZERO,
        control_time: ControlTime {
            estimate: Tick::ZERO,
            error_millis: 0,
            bound_established: true,
            sampled_at: Tick::ZERO,
        },
        node: NodeId(1),
        boot: BootId(1),
        partition: PartitionId(1),
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        snapshot: &SNAPSHOT,
        budgets: &BUDGETS,
    }
}

/// A client submit carrying no conditions and no mutations.
///
/// Enough to be dispatched; deliberately not enough to be interesting. Rows that need a real
/// transaction build one from [`scenarios`], which is team verification's to write.
#[must_use]
pub fn probe_event() -> Event {
    Event {
        id: EventId(1),
        at: Tick::ZERO,
        node: NodeId(1),
        boot: BootId(1),
        partition: PartitionId(1),
        correlation: CorrelationId(1),
        kind: EventKind::Client(ClientEvent::Submit(TxnRequest {
            api_version: API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(1),
            },
            affinity: AffinityId(1),
            expected_generation: Some(Generation(1)),
            remaining_millis: 1_000,
            conditions: Vec::new(),
            mutations: Vec::new(),
        })),
    }
}

/// A control effect from the authority module on partition 1 under `correlation`.
#[must_use]
pub fn control_effect(correlation: u64, control: ControlEffect) -> Effect {
    Effect {
        correlation: CorrelationId(correlation),
        from: ModuleName::Authority,
        partition: PartitionId(1),
        kind: EffectKind::Control(control),
    }
}

/// One batch at `seq` in `generation` on partition 1, putting `key` to `value` in the user
/// namespace.
#[must_use]
pub fn batch(generation: u64, seq: u64, key: &'static [u8], value: &'static [u8]) -> Batch {
    Batch {
        id: BatchId(seq),
        partition: PartitionId(1),
        generation: Generation(generation),
        seq: Seq(seq),
        writes: vec![Write {
            ns: Namespace::User,
            key: Bytes::from_static(key),
            value: Some(Bytes::from_static(value)),
        }],
    }
}

/// An RF3 configuration for partition 1: node 1 primary, nodes 2 and 3 regular, node 4 shadow.
#[must_use]
pub fn rf3_config() -> PartitionConfig {
    PartitionConfig::new(
        PartitionId(1),
        ConfigVersion(1),
        vec![
            member(0, 1, ReplicaRole::Primary),
            member(1, 2, ReplicaRole::RegularSecondary),
            member(2, 3, ReplicaRole::RegularSecondary),
            member(3, 4, ReplicaRole::Shadow),
        ],
    )
}

/// A member in slot `copy` on node `node` at boot 1.
#[must_use]
pub const fn member(copy: u8, node: u32, role: ReplicaRole) -> Member {
    Member {
        copy: CopyId(copy),
        node: NodeId(node),
        boot: BootId(1),
        role,
    }
}

/// Four nodes, one RF3 partition. The topology most rows run on.
#[must_use]
pub fn cluster() -> ClusterConfig {
    ClusterConfig {
        nodes: (1..=4)
            .map(|node| NodeSpec {
                node: NodeId(node),
                boot: BootId(1),
                failure_domain: u16::try_from(node).expect("small"),
                core_sets: 1,
            })
            .collect(),
        partitions: vec![PartitionSpec {
            partition: PartitionId(1),
            config: rf3_config(),
        }],
        initial_config_version: ConfigVersion(1),
    }
}
