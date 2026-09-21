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

#![allow(dead_code)]

pub mod oracle;
pub mod scenarios;

use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{Budgets, ClientEvent, Event, EventKind, StepCtx};
use rdb_core::contracts::ids::{
    AffinityId, BootId, ClientId, ConfigVersion, CorrelationId, EventId, Generation, NodeId,
    OwnerEpoch, PartitionId, RequestId, RequestIdentity, TenantId,
};
use rdb_core::contracts::time::{ControlTime, Tick};
use rdb_core::contracts::txn::TxnRequest;
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::storage::snapshot::EmptySnapshot;

/// The budgets every row runs under unless it says otherwise.
pub const BUDGETS: Budgets = Budgets::SPEC_DEFAULTS;

/// An empty snapshot of a fresh engine, for stepping a module with no data behind it.
pub const SNAPSHOT: EmptySnapshot = EmptySnapshot::new();

/// A minimal context on node 1, partition 1, generation 1, at tick zero.
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

/// The all-zero digest, for rows that must pass a digest they do not care about.
pub const ROOT_DIGEST: Digest = Digest::ROOT;
