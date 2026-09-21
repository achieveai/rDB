//! Identities. Every value here is a dense integer newtype.
//!
//! Dense on purpose. The spike compares traces byte for byte and logs identities as structured
//! fields (ADR-0013), so a tenant is a number here rather than the string it will be in a real
//! deployment. What the contract fixes is *identity and ordering*, not the wire shape a later
//! milestone will give these fields.
//!
//! ## The three watermarks
//!
//! [`ReceivedSeq`], [`AppliedSeq`] and [`DurableSeq`] are separate types with **no conversion
//! between them** — no `From`, no `into_seq`, nothing. That is the whole mechanism (lead ruling
//! B-R13, 2026-09-20), and it is deliberately smaller than the alternatives: no sealed trait, no
//! hidden constructor, no proof object. Spike §7 names "mark buffered data durable" as a mutation
//! a test must catch; with three types, writing it means writing `DurableSeq(applied.0)`, which is
//! a line a reviewer sees and a grep finds, rather than a field assignment that reads correctly.

use serde::{Deserialize, Serialize};

/// Declares a dense identity newtype with the derives every identity in this crate needs.
///
/// `Ord` is not decoration: ordered maps are the only collections allowed on a trace path
/// (charter DO-NOT: no `HashMap` iteration order), so every key type must be orderable.
/// `Default` is zero for every identity, which is why [`Seq::ZERO`] and [`Digest::ROOT`] are
/// spelled out rather than left implicit: a zero identity is a real position, not "unset".
macro_rules! dense_id {
    ($($(#[doc = $doc:expr])+ $name:ident($ty:ty);)*) => {$(
        $(#[doc = $doc])+
        #[derive(
            Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize,
            Deserialize,
        )]
        pub struct $name(
            /// The underlying dense value.
            pub $ty,
        );
    )*};
}

dense_id! {
    /// Application tenant. Keys in one transaction must share it (spec §5.1).
    TenantId(u32);
    /// Application-selected affinity group. All atomically related keys share it; its hash
    /// selects the partition (spec §2).
    AffinityId(u64);
    /// Logical hash range, recovery lineage and replica-placement unit (spec §2).
    PartitionId(u32);
    /// A machine in the simulated cluster.
    NodeId(u32);
    /// One process lifetime of a node. A restart gets a new one, so a grant issued to an
    /// earlier boot can never be honoured by the new process (spec §7.2).
    BootId(u64);
    /// The client that issued a request; half of the dedup key (spec §5.3).
    ClientId(u32);
    /// Client-chosen request identity; the other half of the dedup key (spec §5.3).
    RequestId(u64);
    /// Monotonic partition-history incarnation. Callers compare it to detect possible
    /// rollback (spec §2).
    Generation(u64);
    /// Owner epoch inside a generation; bumped on every ownership transition (spec §7.3).
    OwnerEpoch(u64);
    /// Pins the required-copy set. Membership changes bump it so an old exposure predicate
    /// cannot be erased by renaming a replica (spec §6.2).
    ConfigVersion(u64);
    /// Identity of one fenced grant (spec §7.2).
    GrantId(u64);
    /// Identity of the lease backing a grant; carried in the replication envelope (spec §6.1).
    LeaseId(u64);
    /// Position in one partition's ordered transaction history.
    Seq(u64);
    /// Highest contiguous sequence a replica has *received*. Diagnostic only: it qualifies
    /// nothing, and spec §5.2 never counts it towards an acknowledgement.
    ReceivedSeq(u64);
    /// Highest sequence a replica has *applied to state*, not necessarily synced. What
    /// `BufferedOnTwo` rests on (spec §5.2), and the ceiling a flush may capture.
    AppliedSeq(u64);
    /// Highest sequence a replica has *synced*. What releases a protection pause and what
    /// `DurableOnRequiredCopies` rests on (spec §6.1, §6.2).
    DurableSeq(u64);
    /// Ties an effect to the events it causes and back to the request that started them.
    CorrelationId(u64);
    /// Total-order tiebreak for events that land on the same tick (spike §6).
    EventId(u64);
    /// One atomic storage batch. Whole batch or none (spike §5, M1).
    BatchId(u64);
    /// One transport frame. Delivery may duplicate it; the identity is what makes a duplicate
    /// recognisable (spike §4).
    MessageId(u32);
    /// A published, immutable read view. Reads bind to one of these, never to the raw
    /// applied prefix (spec §5.3).
    SnapshotHandle(u64);
    /// Identifies one `sync_wal_through` call so its completion can be matched to the
    /// prefixes it captured (spec §6.1).
    FlushTicket(u64);
    /// A timer the kernel armed.
    TimerId(u64);
    /// Generation counter for one `TimerId`. A fire carrying a stale version is ignored
    /// (spike §4, time seam).
    TimerVersion(u64);
    /// rEtcd control-store revision. Single-record CAS is expressed against it (spec §7.1).
    Revision(u64);
    /// A routing range, the unit of the `routes/{range}` control key family (spec §7.1).
    RangeId(u32);
    /// An idempotent control-plane operation record (spec §7.1).
    OperationId(u64);
    /// Identifies one generator choice point in a trace, so a reducer can shrink a specific
    /// decision rather than a whole seed (spike §4, trace seam).
    ChoiceId(u32);
}

impl Seq {
    /// The position before the first transaction of a lineage.
    pub const ZERO: Self = Self(0);

    /// The next position in the history. Sequences are assigned one at a time per partition
    /// (spec §8.2), so this is the only way a sequence advances.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Generation {
    /// The next history incarnation. Only an explicit recovery decision bumps it (spec §8.1).
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// The dedup identity of a request: `(tenant, client_id, request_id)`, scoped to its affinity
/// group and generation (spec §5.3).
///
/// The affinity group and generation are deliberately *not* fields. They are the scope the
/// identity is looked up in, not part of the identity, and folding them in here would make a
/// retry against a new generation look like a different request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RequestIdentity {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Issuing client.
    pub client: ClientId,
    /// Client-chosen request id.
    pub request: RequestId,
}

/// What a copy is allowed to do for the protection predicate.
///
/// The distinction is a safety rule, not bookkeeping: a shadow acknowledgement never qualifies
/// a write for success and a shadow never becomes primary (spec §2, §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ReplicaRole {
    /// Holds the grant and assigns sequences.
    Primary,
    /// Primary-eligible copy on a distinct machine. Counts toward protection and may ACK.
    RegularSecondary,
    /// Never-primary asynchronous copy. Never counts toward an ACK or readiness predicate.
    Shadow,
}

impl ReplicaRole {
    /// Whether an acknowledgement from this role may qualify a transaction for client success.
    ///
    /// Exists so no kernel module has to re-derive the rule, and so the one place that could get
    /// it wrong is covered by one test rather than six.
    #[must_use]
    pub const fn may_qualify_ack(self) -> bool {
        matches!(self, Self::Primary | Self::RegularSecondary)
    }
}
