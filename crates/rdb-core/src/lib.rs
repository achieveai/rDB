//! rDB kernel contracts and kernel modules (M7): the partition database's protocol decisions,
//! and nothing else.
//!
//! rDB is the **data plane**. rEtcd (the `config-*` crates) is the control plane. The dependency
//! arrow points one way — `rdb-*` may use `config-*`, never the reverse (rdb ADR-0002).
//!
//! # What may not live here
//!
//! No clock, no randomness, no I/O, no network, no async runtime, no thread. A kernel module is
//! a synchronous function from an [`contracts::event::Event`] to a list of
//! [`contracts::event::Effect`]s, and everything it needs to decide arrives in the event or in
//! [`contracts::event::StepCtx`]. Same event log, same effects, byte for byte — that equality is
//! what lets a ten-thousand-history campaign hand back a reproducer instead of a shrug
//! (rdb ADR-0003).
//!
//! The one exception is reads: [`contracts::event::StepCtx::snapshot`] is a total, ordered,
//! side-effect-free view of an already-published prefix. It is a lookup, not I/O, and routing it
//! through the effect queue would buy no determinism.
//!
//! # Layout
//!
//! * [`contracts`] — the seams. One module per seam; see its table.
//! * [`authority`], [`transaction`], [`replication`], [`publication`], [`protection`],
//!   [`recovery`] — the six kernel modules, one per spike §5 package.
//!
//! # Seed state (2026-09-20)
//!
//! The contracts are real types. The six kernel modules and the codec functions are explicit
//! stubs that return [`contracts::errors::RdbError::Unavailable`]. Spike §8 permits an explicit
//! unavailable result for an unimplemented capability and forbids a fake success; nothing in
//! this crate ever calls `todo!()`, because a panic would abort the campaign runner instead of
//! letting it report which package is missing.
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod authority;
pub mod contracts;
pub mod protection;
pub mod publication;
pub mod recovery;
pub mod replication;
pub mod transaction;

pub use contracts::digest::{Digest, Domain};
pub use contracts::errors::{Capability, ErrorKind, RdbError, RetryRule};
pub use contracts::event::{
    Budgets, ClientEvent, Effect, EffectKind, Event, EventKind, Module, ModuleName, ReplyEffect,
    StepCtx,
};
pub use contracts::ids::{
    AffinityId, AppliedSeq, BatchId, BootId, ChoiceId, ClientId, ConfigVersion, CorrelationId,
    DurableSeq, EventId, FlushTicket, Generation, GrantId, LeaseId, MessageId, NodeId, OperationId,
    OwnerEpoch, PartitionId, RangeId, ReceivedSeq, ReplicaRole, RequestId, RequestIdentity,
    Revision, Seq, SnapshotHandle, TenantId, TimerId, TimerVersion,
};
pub use contracts::membership::{CopyId, Member, PartitionConfig};
pub use contracts::storage::{
    Batch, CapturedPrefix, DurablePrefix, Namespace, SnapshotRead, StorageEvent, StorageFault,
    StoreEffect, Write,
};
pub use contracts::time::{ClockVerdict, ControlTime, Deadline, Tick, TimerEffect, TimerFired};
pub use contracts::txn::{
    Condition, ConditionOutcome, Durability, Mutation, Outcome, TxnRequest, TxnResult, TxnStatus,
};
pub use contracts::version::{check_mandatory, VersionedArtifact};
