//! [`ApiError`]: a failed call, with the §5.4 name and retry rule. Moved out of `db.rs`
//! (M10 S2 event 0).

use std::time::Duration;

use rdb_core::contracts::errors::{ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::ids::{NodeId, PartitionId};

/// A failed call, with the §5.4 name and what the caller may do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// The §5.4 kind.
    pub kind: ErrorKind,
    /// What the caller may do.
    pub retry: RetryRule,
    /// Whether this submission provably mutated nothing. It says nothing of an earlier send of
    /// the same request id.
    pub no_mutation: bool,
    /// Detail, for the log and the operator.
    pub detail: String,
    /// The soonest a retry can be admitted, when the host knows it (M10 S1-6, critic-d6b
    /// candidate 3): a lower bound, "not before", never a promise that it will be. `None` when
    /// nothing is known. Set it with [`Self::with_retry_after`], which also says it in `detail`.
    pub retry_after: Option<Duration>,
    /// The size limit a `TOO_LARGE` refusal broke, with the size found and the maximum (#148).
    /// Set on every `TOO_LARGE` the `Db` raises and on nothing else. The `Db` checks before it
    /// sends, so an answer that crossed a link never carries it. Set it with
    /// [`Self::too_large_refusal`].
    pub too_large: Option<TooLarge>,
    /// Whether the owner's node faulted, so a call can succeed once another node takes the
    /// partition over (#134). Set by [`Self::owner_fault`] only, and on an answer that crossed a
    /// link when it is `NotWired` and its detail starts with that constructor's
    /// `"host fault: "`: the wire carries the detail, not this field. Every other `NotWired`
    /// refusal, [`Self::host`]'s included, keeps "retrying will not help".
    pub(crate) owner_fault: bool,
}

/// Which fixed size limit a `TOO_LARGE` refusal broke (#148).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// An object id's raw bytes, over `MAX_OBJECT_ID`.
    ObjectId,
    /// A map key's or set member's bytes, over `MAX_KEY`.
    Key,
    /// One byte value of a put, over `MAX_VALUE`.
    Value,
    /// A whole byte write's upper-bound record size, over `MAX_WRITE`.
    Write,
    /// The ops of one change, or the puts of one transaction, over `MAX_OPS`.
    Ops,
    /// One blob chunk's bytes, over `rdb_value::blob::MAX_CHUNK`.
    Chunk,
}

/// What a `TOO_LARGE` refusal found: the limit, the size the call carried and the most that
/// limit allows. `size` is always above `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooLarge {
    /// The limit broken.
    pub limit: Limit,
    /// The size the call carried, in the limit's unit (bytes, or ops for [`Limit::Ops`]).
    pub size: usize,
    /// The most the limit allows.
    pub max: usize,
}

/// How the detail of an owner fault ([`ApiError::owner_fault`], or the host's typed damage)
/// starts. It is what marks one on the wire: an answer that crossed a link is an owner fault
/// when it is `NotWired` and its detail starts with this ([`ApiError::fault_from_wire`], #134).
/// Typed damage keeps its own kind and still gets [`owner_fault_step`] whatever that kind is
/// ([`ApiError::is_host_fault`], #170 review r4).
pub(crate) const HOST_FAULT: &str = "host fault: ";

/// How the detail of [`ApiError::host`] starts: a node error that no takeover fixes.
const HOST_ERROR: &str = "host error: ";

/// The tail every "open the folder again" step in `db.rs` ends with ([`super::REOPEN_STEP`] and
/// the stuck-recovery step). A detail that ends with it already says what to do next.
const OPEN_AGAIN: &str = "open the folder again";

/// What every `Err` of `Db::wait_durable` says about the write (#90): a barrier wrote nothing,
/// proves nothing about the write, and has no request to resend.
const BARRIER_NOTE: &str = "the barrier wrote nothing and says nothing about the write; there \
     is nothing to `Db::resend`";

/// The next step of a `Db::wait_durable` `Err` a later call can end (#90, S5d-9): it replaces
/// `step_for`, whose text for a kind with `no_mutation` false says to `Db::resend`. An `Err`
/// that needs something else first (a fixed argument, a report, a reopen) keeps that step
/// instead ([`ApiError::for_barrier`]).
pub(crate) const BARRIER_STEP: &str = "the barrier wrote nothing and says nothing about the \
     write; there is nothing to `Db::resend`; call `Db::wait_durable` again, or after \
     `GENERATION_CHANGED` read back and write what is missing as new writes";

/// The tail `admin.rs` ends a store error with when a delete may have applied: it already says
/// to run the call again, which a "retrying will not help" step would contradict (rdb_dev row
/// `an_admin_error_that_may_have_deleted_says_a_rerun_shows_whether_it_did` pins the text).
const RUN_AGAIN: &str = "run it again to read where it stands";

/// The tail of [`ApiError::owner_lost_admin`]: its detail names the step, and an admin call has
/// no request id for the `Db::resend` / `Db::status` step a lost write gets (PR #74 R2).
const ADMIN_LOST_STEP: &str = "so run `admin status`";

/// The tail every `admin rebuild` error ends with (#65): its detail already names the step
/// (stop it, send a write, promote first, restart it), which "retrying will not help" would
/// contradict (bb walk paper cut).
pub(crate) const REBUILD_STEP: &str = "the rebuild did nothing more than this says";

/// The tail of a `RECOVERY_READ_ONLY` refusal while the owner rebuilds a copy from a snapshot
/// (issue #61, design N3): the window ends by itself, and `retry_after` is a "not before" poll
/// hint, not an estimate.
pub(crate) const REBUILDING_STEP: &str = "reads still work and nothing was discarded; this \
    send wrote nothing: send the write again as a new write after `retry_after`, which is a poll \
    hint, not an estimate";

/// The tail every `admin move` refusal ends with (M11): its detail names the step.
pub(crate) const MOVE_STEP: &str = "the move did nothing more than this says";

/// What a refill refusal onto a node its owner holds as left out says (#80 S4); `rebalance`
/// skips that target on it and tries the next.
pub(crate) const REFILL_LEFT_OUT: &str = "comes back only by its own restart";

/// The tail every refill refusal ends with (#80 S4, A-R456 narrowed: refill): its detail names
/// the step.
pub(crate) const REFILL_STEP: &str = "the refill did nothing more than this says";

/// The tail every `admin forget-tail` refusal ends with (issue #105): its detail names the step,
/// and nothing was given up.
pub(crate) const FORGET_TAIL_STEP: &str = "nothing was given up";

/// The tail of a move's answer when the move ended by filling a free slot (A-R457 (a)): its
/// detail says what the move did and the step to move the source's copy.
pub(crate) const MOVE_ENDED: &str = "the move ended with that commit";

/// The tail of [`crate::admin::PromoteError::OwnerAlive`]: a designed refusal whose text names
/// the step, which "retrying will not help: report this" would contradict (#79 S3 P2).
pub(crate) const PROMOTE_ALIVE_STEP: &str = "stop that node first, then promote again";

/// The tails of the other `NotWired` promote refusals that name their own step (#134 audit):
/// "retrying will not help: report this" contradicted `Unanswered`'s "promote again" and
/// misdirected the rest. `Unanswered`, `Superseded`, `Unconfigured`, `NoControlStore`.
pub(crate) const PROMOTE_UNANSWERED_STEP: &str = "it may have written: promote again";
pub(crate) const PROMOTE_SUPERSEDED_STEP: &str = "run status before promoting again";
pub(crate) const PROMOTE_UNCONFIGURED_STEP: &str =
    "promote a node that has not restarted since that node stopped";
pub(crate) const PROMOTE_NO_STORE_STEP: &str = "open it with Db::open, which connects it to one";

/// The tails of a `RECOVERY_READ_ONLY` refusal whose repair is held on a copy's tail (#79 S4d).
/// "Writes return when the lost copies are back" is false there: the copy is back, and its tail
/// is why it cannot rejoin. One per way the refused send can be sent again, and (issue #105
/// unit 2, design section 8.3) two per whether the tail can come back on its own: the `HELD`
/// pair when one cannot, the `ADOPTABLE` pair when every copy named reports its tail `Adoptable`.
pub(crate) const TAIL_HELD_NEW_WRITE: &str = "reads still work and nothing was discarded; \
    writes stay refused until the copy named here returns with these seqs or an operator runs \
    `admin forget-tail`, which accepts losing these seqs; do not wipe or rebuild the node named \
    here, or those seqs may be gone; `Db::node_status` shows this as `tail_refused`; this send \
    wrote nothing: when writes return, send the write again as a new write, not with \
    `Db::resend`";
pub(crate) const TAIL_HELD_RESEND: &str = "reads still work and nothing was discarded; writes \
    stay refused until the copy named here returns with these seqs or an operator runs \
    `admin forget-tail`, which accepts losing these seqs; do not wipe or rebuild the node named \
    here, or those seqs may be gone; `Db::node_status` shows this as `tail_refused`; an earlier \
    send may have applied: when writes return, send the same request again with `Db::resend`, \
    or ask `Db::status`; never send it under a new request id";
pub(crate) const TAIL_ADOPTABLE_NEW_WRITE: &str = "reads still work and nothing was discarded; \
    the copy named here can restore these seqs, and writes will return on their own; do not \
    wipe or rebuild the node named here, or those seqs may be gone; `Db::node_status` shows \
    this as `tail_refused` with adoption `Adoptable`; this send wrote nothing: when writes return, send \
    the write again as a new write, not with `Db::resend`";
pub(crate) const TAIL_ADOPTABLE_RESEND: &str = "reads still work and nothing was discarded; \
    the copy named here can restore these seqs, and writes will return on their own; do not \
    wipe or rebuild the node named here, or those seqs may be gone; `Db::node_status` shows \
    this as `tail_refused` with adoption `Adoptable`; an earlier send may have applied: when writes \
    return, send the same request again with `Db::resend`, or ask `Db::status`; never send it \
    under a new request id";

/// The tail of [`super::Db::route`]'s `INCOMPATIBLE_VERSION` (#135): a control record this
/// build cannot decode. A newer build may have written it, or its bytes are damaged; either way
/// every retry decodes the same bytes, so the step is an upgrade, then corruption recovery, not
/// "try again" (PR #159 review).
pub(crate) const ROUTE_NEWER_RECORD_STEP: &str = "a newer build may have written this record, \
    or it is damaged; retrying will not help: run this app on the rDB build the database's \
    nodes run, or a newer one; if the nodes' own build cannot read it either, it is damaged: \
    stop sending this partition's calls, keep the store as it is, never edit the record by \
    hand, and report it with this message";

/// The tail of [`crate::admin::PromoteError::TargetFrozen`], for the same reason.
pub(crate) const PROMOTE_FROZEN_STEP: &str = "promote another node";

/// The tail of [`super::ReadyError::Quarantined`]'s text, which a refusal carries while the
/// owner's recovery is quarantined ([`super::Db::explained`]). It already says what to do, which
/// a "wait a few seconds and try again" step would contradict (review-s2 F-003).
pub(crate) const QUARANTINED_STEP: &str =
    "do not delete the folder; run `admin rebuild N` (`Db::rebuild`) for a copy whose history \
     disagrees: it takes that copy out of the partition and back, and a rebuild of a copy the \
     quarantine does not name is refused, naming the ones it does";

/// The step a damaged object's root (a document, or a map's or set's own root) ends its
/// `CORRUPT_HISTORY` with ([`corrupt_history`]), in place of the map or set entry's step: no
/// key can be written over, so only a clear helps (A-R420, tester-q G1/G2). It names the API
/// and the `rdb_dev` command, since the reader may be either.
pub(crate) const DAMAGED_OBJECT_STEP: &str =
    "the object's own stored value is damaged, so delete the whole object with `Change::Clear` \
     (in rdb_dev: `admin clear <object>`), then write it again";

/// The step of a feed read's `CORRUPT_HISTORY` (#87): the cursor's record has another digest
/// here, or a stored `User` key the call scans does not parse; or the copy's stored history is
/// missing or damaged (a record unreadable, not decoding, not matching its digest, naming
/// another seq, or missing between the retained floor and what the copy may serve), or a read
/// of its floor finds it corrupt, which faults the node, and then only feed replies carry this
/// step (review r3).
/// "Write over its key" is a map entry's step, which a feed caller cannot act on.
pub(crate) const FEED_HISTORY_STEP: &str = "the history at this copy is unreadable from this \
     cursor: read from another copy (the owner or another member), or rebuild from your own \
     source";

/// The step of a feed read refused by a copy that holds no current copy of the partition (#87):
/// left out of the generation `partitions/{p}` names, or never a member. Its rule is
/// `Definitive`: calling this copy again answers the same until it is a member again, so the
/// caller switches source rather than retrying here.
pub(crate) const FEED_OTHER_COPY_STEP: &str = "read from the owner or another member \
     (`Source::Owner`, or `Source::Node` of a member); calling this copy again will not help \
     while it holds no current copy";

/// The step of a feed call that a node stopping or stopped could not answer (#170 review r5): a
/// clean stop's answer to a call parked on its control read. Its rule is `RetryAfterRecovery`:
/// this node serves again once it has restarted, and another copy can serve now.
pub(crate) const FEED_STOPPED_STEP: &str = "read from the owner or another member now \
     (`Source::Owner`, or `Source::Node` of a member), or call this node again once it has \
     restarted";

/// The step of a feed call whose link to a node in another process closed before the answer
/// came (#170 review r7): the node may have read, but a feed call changes nothing. Its rule is
/// `RetryAfterRecovery`: another copy can serve now, and this node once its link is up again.
pub(crate) const FEED_LINK_STEP: &str = "read from the owner or another member now \
     (`Source::Owner`, or `Source::Node` of a member), or call this node again once its link \
     is up again";

impl std::fmt::Display for ApiError {
    /// `NAME (detail; next: step)`, or `NAME (detail)` when the detail already ends with the
    /// step. See [`ApiError::explain`].
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", wire_name(self.kind), self.explain())
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    /// An error of `kind` raised by the API itself, with a fixed retry rule and mutation proof
    /// per kind:
    ///
    /// | kind | `retry` | `no_mutation` |
    /// |---|---|---|
    /// | `ConditionFailed`, `InvalidArgument`, `TooLarge` | `Definitive` | `true` |
    /// | `UnknownOutcome` | `QueryStatus` | `false` |
    /// | `CorruptHistory`, `CorruptBlob`, `IncompatibleVersion` | `Quarantine` | `false` |
    /// | `UploadChunkConflict` | `Reconcile` | `true` |
    /// | any other | `BoundedJitter` | `false` |
    ///
    /// This is not what the kernel attaches to the same kind: for example a kernel
    /// `NotPrimary` carries `RefreshRoute`, and a kernel `BoundedJitter` proves this submission
    /// mutated nothing.
    /// Wrap a kernel error with [`Self::from_kernel`], never with this.
    #[must_use]
    pub fn new(kind: ErrorKind, detail: impl Into<String>) -> Self {
        let (retry, no_mutation) = match kind {
            ErrorKind::ConditionFailed | ErrorKind::InvalidArgument | ErrorKind::TooLarge => {
                (RetryRule::Definitive, true)
            }
            ErrorKind::UnknownOutcome => (RetryRule::QueryStatus, false),
            ErrorKind::CorruptHistory | ErrorKind::CorruptBlob | ErrorKind::IncompatibleVersion => {
                (RetryRule::Quarantine, false)
            }
            ErrorKind::UploadChunkConflict => (RetryRule::Reconcile, true),
            _ => (RetryRule::BoundedJitter, false),
        };
        Self {
            kind,
            retry,
            no_mutation,
            detail: detail.into(),
            retry_after: None,
            too_large: None,
            owner_fault: false,
        }
    }

    /// The kernel's own error, with its own retry rule and proof.
    #[must_use]
    pub fn from_kernel(error: &RdbError) -> Self {
        Self {
            kind: error.kind(),
            retry: error.retry_rule(),
            no_mutation: error.proves_no_mutation(),
            detail: error.to_string(),
            retry_after: None,
            too_large: None,
            owner_fault: false,
        }
    }

    /// A caller's argument the API refuses before the kernel sees it.
    #[must_use]
    pub fn invalid(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, detail)
    }

    /// `TOO_LARGE` for a call over a fixed size limit, refused before anything was sent
    /// (#148): `Definitive`, `no_mutation`, and [`Self::too_large`] set to `found`.
    #[must_use]
    pub fn too_large_refusal(found: TooLarge, detail: impl Into<String>) -> Self {
        Self {
            too_large: Some(found),
            ..Self::new(ErrorKind::TooLarge, detail)
        }
    }

    /// The size limit this error broke, when it is a `TOO_LARGE` the `Db` raised (#148).
    #[must_use]
    pub const fn too_large(&self) -> Option<TooLarge> {
        self.too_large
    }

    /// A put refused before it was sent: this node has adopted no generation yet, so there is
    /// none to fence it with (`expected_generation` is always set; ADR-0004 §8). This submission
    /// never left the host, so it mutated nothing, and it may be retried once a generation is
    /// adopted.
    #[must_use]
    pub fn not_adopted(node: NodeId, partition: PartitionId) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::BoundedJitter,
            no_mutation: true,
            detail: format!(
                "no generation adopted yet for partition {} on node {}; nothing was sent",
                partition.0, node.0
            ),
            retry_after: None,
            too_large: None,
            owner_fault: false,
        }
    }

    /// A call to the owner that runs in another process, refused before it was sent: no link
    /// to it is up, as when its process was killed (tester-s2 G walk). Not a host fault, so
    /// never "retrying will not help": a retry helps once that node is back or another node is
    /// promoted. Nothing was sent, so nothing was mutated.
    #[must_use]
    pub fn owner_unreachable(node: NodeId) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::RetryAfterRecovery,
            no_mutation: true,
            detail: format!(
                "node {}, the owner, is not reachable from this process: no link to it is up \
                 (its process may have stopped); nothing was sent; a retry can succeed once \
                 node {} is back or another node is promoted",
                node.0, node.0
            ),
            retry_after: None,
            too_large: None,
            owner_fault: false,
        }
    }

    /// A call to the owner that runs in another process, sent, whose answer never came back:
    /// the link dropped while it waited, as when that process is stopped (`lr down`, tester tc1
    /// D1, A-R357). Not a host fault: a retry helps once the node is back. The call was sent,
    /// so a write may have applied.
    #[must_use]
    pub fn owner_lost(node: NodeId) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::RetryAfterRecovery,
            no_mutation: false,
            // The step is not in the detail: [`ApiError::next_step`] picks it from the kind, the
            // rule and `no_mutation` (review-s2 F-003), so it is said once.
            detail: format!(
                "node {}, the owner, went out of reach while this call waited for its answer (its \
                 process may have stopped); a write sent here may have applied",
                node.0
            ),
            retry_after: None,
            too_large: None,
            owner_fault: false,
        }
    }

    /// [`Self::owner_lost`] for an admin move or rebuild (PR #74 F-004, R2). It may have
    /// started, so not `no_mutation`; but it carries no request id, so the step is to repeat
    /// the same admin call. A rebuild answers its own spare note again (a restarted owner has
    /// none and notes it afresh). A move answers where it stands only while the owner keeps
    /// running: `Host::moved` is memory only, so after a restart a repeat of a move that
    /// finished is refused by T1's "holds no copy" arm, which says to run `admin status`.
    #[must_use]
    pub fn owner_lost_admin(node: NodeId) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::RetryAfterRecovery,
            no_mutation: false,
            detail: format!(
                "node {}, the owner, went out of reach while this admin call waited for its \
                 answer (its process may have stopped); the move or rebuild may have started: \
                 once the owner is back, run the same `admin move` or `admin rebuild` again; a \
                 rebuild recognises its own note. A move answers where it stands only while the \
                 owner keeps running; after a restart a repeat of a finished move is refused, \
                 {ADMIN_LOST_STEP}",
                node.0
            ),
            retry_after: None,
            too_large: None,
            owner_fault: false,
        }
    }

    /// [`Self::owner_lost`] for a call that writes nothing: a read, a status look, or a write's
    /// compile, which answers before the write itself is sent (tester tc2 E1, A-R394). Nothing
    /// was applied, so `no_mutation`, and there is no request to resend.
    #[must_use]
    pub fn owner_lost_unsent(node: NodeId) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::RetryAfterRecovery,
            no_mutation: true,
            detail: format!(
                "node {}, the owner, went out of reach while this call waited for its answer (its \
                 process may have stopped); this call writes nothing (a read, a status look, or \
                 a write's first step, before the write is sent), so nothing was applied: retry \
                 it once node {} is back or another node is promoted",
                node.0, node.0
            ),
            retry_after: None,
            too_large: None,
            owner_fault: false,
        }
    }

    /// The owner's node faulted (§4.4): it stopped, dropped the call, or could not serve an
    /// effect, so it serves nothing more this boot. Retrying at it will not help, but another
    /// node taking its partitions over can serve the call, which [`Self::next_step`] says (#134).
    #[must_use]
    pub fn owner_fault(detail: &str) -> Self {
        Self {
            owner_fault: true,
            detail: format!("{HOST_FAULT}{detail}"),
            ..Self::host_error(String::new())
        }
    }

    /// A node error that is not an owner fault: an answer of the wrong shape, a call that
    /// compiled to nothing, a call the blocking pool did not run. No retry and no takeover fixes
    /// it, so its step is "retrying will not help: report this" (#134).
    #[must_use]
    pub fn host(detail: &str) -> Self {
        Self::host_error(format!("{HOST_ERROR}{detail}"))
    }

    const fn host_error(detail: String) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::NotWired,
            no_mutation: false,
            detail,
            retry_after: None,
            too_large: None,
            owner_fault: false,
        }
    }

    /// Whether an answer that crossed a link with this rule and detail is an owner fault: the
    /// wire carries [`Self::owner_fault`]'s detail prefix, not its field (#134).
    pub(crate) fn fault_from_wire(retry: RetryRule, detail: &str) -> bool {
        retry == RetryRule::NotWired && detail.starts_with(HOST_FAULT)
    }

    /// This error with a "not before" hint: a retry is not admitted sooner than `after` from
    /// now. Sets [`Self::retry_after`] and ends `detail` with `; not before ~Ns`, the seconds
    /// rounded up so the text never names a time earlier than the hint.
    #[must_use]
    pub fn with_retry_after(mut self, after: Duration) -> Self {
        let seconds = after.as_millis().div_ceil(1_000);
        self.detail = format!("{}; not before ~{seconds}s", self.detail);
        self.retry_after = Some(after);
        self
    }

    /// The soonest a retry can be admitted, when the host knows it.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Whether this is a host fault: the owner's node faulted ([`Self::owner_fault`], or a fault
    /// that names stored damage, whose detail starts the same way). Retrying at that node will
    /// not help, whatever the kind and retry rule, but a takeover can; [`Self::next_step`] says
    /// so (#170 review r4). [`Self::host`]'s errors are not host faults: their detail starts
    /// `"host error: "` and no takeover fixes them (#134).
    #[must_use]
    pub fn is_host_fault(&self) -> bool {
        self.owner_fault || self.detail.starts_with(HOST_FAULT)
    }

    /// The §5.4 wire name, `SCREAMING_SNAKE_CASE`.
    #[must_use]
    pub fn name(&self) -> String {
        wire_name(self.kind)
    }

    /// What the caller does next, in words, for this kind, retry rule and mutation proof. `None`
    /// when [`Self::detail`] already ends with its own step, which a second one would contradict:
    /// the host said to shut the `Db` down and open the folder again, an admin call said to run
    /// it again to read where it stands, or the owner's recovery is quarantined. A write's
    /// `UnknownOutcome` (`QueryStatus`) keeps its step after "open the folder again", because
    /// asking [`super::Db::status`] with the same id is still right after the new open.
    ///
    /// Where the step says to send a write again, `no_mutation` picks how (review-s2 F-002,
    /// F-003). True: this send wrote nothing and no earlier send exists, and [`super::Db`]'s ack
    /// floor counts the refusal as settled, so a later write can expire its id and a resend then
    /// answers `STATUS_EXPIRED` without applying it: send it again as a new write. False: an
    /// earlier send may have applied, so only a resend of the same request or a status look is
    /// safe; a new id could apply it twice.
    #[must_use]
    pub fn next_step(&self) -> Option<&'static str> {
        let write_unknown =
            self.kind == ErrorKind::UnknownOutcome && self.retry == RetryRule::QueryStatus;
        let reopen = self.detail.ends_with(OPEN_AGAIN) && !write_unknown;
        if reopen
            || self.detail.ends_with(RUN_AGAIN)
            || self.detail.ends_with(REBUILD_STEP)
            || self.detail.ends_with(REBUILDING_STEP)
            || self.detail.ends_with(BARRIER_STEP)
            || self.detail.ends_with(MOVE_STEP)
            || self.detail.ends_with(REFILL_STEP)
            || self.detail.ends_with(FORGET_TAIL_STEP)
            || self.detail.ends_with(MOVE_ENDED)
            || self.detail.ends_with(ADMIN_LOST_STEP)
            || self.detail.ends_with(QUARANTINED_STEP)
            || self.detail.ends_with(DAMAGED_OBJECT_STEP)
            || self.detail.ends_with(FEED_HISTORY_STEP)
            || self.detail.ends_with(FEED_OTHER_COPY_STEP)
            || self.detail.ends_with(FEED_STOPPED_STEP)
            || self.detail.ends_with(FEED_LINK_STEP)
            || self.detail.ends_with(PROMOTE_ALIVE_STEP)
            || self.detail.ends_with(PROMOTE_FROZEN_STEP)
            || self.detail.ends_with(PROMOTE_UNANSWERED_STEP)
            || self.detail.ends_with(PROMOTE_SUPERSEDED_STEP)
            || self.detail.ends_with(PROMOTE_UNCONFIGURED_STEP)
            || self.detail.ends_with(PROMOTE_NO_STORE_STEP)
            || self.detail.ends_with(ROUTE_NEWER_RECORD_STEP)
            || self.detail.ends_with(TAIL_HELD_NEW_WRITE)
            || self.detail.ends_with(TAIL_HELD_RESEND)
            || self.detail.ends_with(TAIL_ADOPTABLE_NEW_WRITE)
            || self.detail.ends_with(TAIL_ADOPTABLE_RESEND)
        {
            return None;
        }
        // #170 review r4: every host fault, typed damage included. A damaged node's fault is
        // not a damaged entry of the caller's (`step_for(CorruptHistory)` says to write over
        // its key): only a takeover helps, so it gets the owner-fault step whatever its kind.
        if self.is_host_fault() {
            return Some(owner_fault_step(self.no_mutation));
        }
        Some(step_for(self.kind, self.retry, self.no_mutation))
    }

    /// This error as `Db::wait_durable` answers it (#90): `no_mutation` false, since a barrier is
    /// not a write and proves nothing about one. A detail that already ends with its own step
    /// (a reopen, a quarantine) keeps it. An error no later call ends, a bad argument
    /// (`Definitive`) or a host fault (`NotWired`), says what the barrier did and keeps its own
    /// step, so "call `Db::wait_durable` again" never hides "fix the argument" or "report this".
    /// Anything else ends with [`BARRIER_STEP`].
    #[must_use]
    pub(crate) fn for_barrier(mut self) -> Self {
        self.no_mutation = false;
        if self.next_step().is_none() {
            return self;
        }
        let own_step = matches!(
            self.retry,
            RetryRule::Definitive | RetryRule::NotWired | RetryRule::Quarantine
        );
        let tail = if own_step { BARRIER_NOTE } else { BARRIER_STEP };
        self.detail = format!("{}; {tail}", self.detail);
        self
    }

    /// [`Self::detail`] with its next step: `detail; next: step`. This is the text to show an
    /// app's user or log; [`Display`](std::fmt::Display) is the wire name and this.
    #[must_use]
    pub fn explain(&self) -> String {
        match self.next_step() {
            Some(step) => format!("{}; next: {step}", self.detail),
            None => self.detail.clone(),
        }
    }
}

/// The next step of a host fault, in the gateway's words (`rdb_server::wire_step`, PR #129
/// review, #134). Retrying at this node will not help, but another node can take a faulted
/// owner's partition over (an operator promote, or the planner when `DbConfig::failover` is
/// configured), so the takeover clause opens "if the owner faulted": a host fault also covers
/// a node that answered nonsense. This `Db` keeps routing to the old owner until it reads the
/// owner again, which `Db::wait_ready` does, so that comes first. `no_mutation` true: the send wrote nothing, so a write goes again as a
/// new write. False: it may have applied, so a write asks its status first and a read, which
/// has no request, reads again.
const fn owner_fault_step(no_mutation: bool) -> &'static str {
    if no_mutation {
        "retrying at this node will not help: keep this Db's log and report this message; if \
         the owner faulted, once another node takes over (an operator promote does that), call \
         `Db::wait_ready` until it returns `Ok`; then a read reads again, and a write is sent \
         again as a new write (`Db::put`, `Db::txn` or `Db::write`), not with `Db::resend`: \
         this send wrote nothing"
    } else {
        "retrying at this node will not help: keep this Db's log and report this message; if \
         the owner faulted, once another node takes over (an operator promote does that), call \
         `Db::wait_ready` until it returns `Ok`; then, for a write, ask `Db::status` with its \
         request id before writing again, and never send it under a new request id; for a \
         read, read again"
    }
}

/// The next step for an error of `kind` with `retry` and `no_mutation`. Total over
/// [`ErrorKind`], so a new kind does not compile until it has a step. `NotWired` wins over the
/// kind: nothing in this open serves it, so no kind's retry advice applies. `no_mutation` picks
/// how a refused write is sent again ([`ApiError::next_step`]); it never turns a read's step
/// into a write's, and `Unavailable` keys on it only with `RetryAfterRecovery`, the owner-link
/// refusals, so a read's `Unavailable` keeps its plain words.
const fn step_for(kind: ErrorKind, retry: RetryRule, no_mutation: bool) -> &'static str {
    if matches!(retry, RetryRule::NotWired) {
        return "retrying will not help: keep this Db's log and report this message";
    }
    match kind {
        ErrorKind::NotPrimary | ErrorKind::RouteChanged if no_mutation => {
            "this send wrote nothing: send the write again as a new write (`Db::put`, `Db::txn` \
             or `Db::write`), not with `Db::resend`; if this repeats, `Db::node_status` shows \
             which node is primary"
        }
        ErrorKind::NotPrimary | ErrorKind::RouteChanged => {
            "an earlier send may have applied: send the same request again with `Db::resend`, \
             or ask `Db::status`; never send it under a new request id; if this repeats, \
             `Db::node_status` shows which node is primary"
        }
        ErrorKind::LeaseExpired if no_mutation => {
            "call `Db::wait_ready`; then a read reads again, and a write is sent again as a new \
             write (`Db::put`, `Db::txn` or `Db::write`), not with `Db::resend`: this send wrote \
             nothing"
        }
        ErrorKind::LeaseExpired => {
            "call `Db::wait_ready`; an earlier send may have applied: send the same request \
             again with `Db::resend`, or ask `Db::status`; never send it under a new request id"
        }
        ErrorKind::RecoveryReadOnly if no_mutation => {
            "reads still work; writes return when the lost copies are back, and \
             `Db::node_status` shows the recovery; this send wrote nothing: then send the write \
             again as a new write, not with `Db::resend`"
        }
        ErrorKind::RecoveryReadOnly => {
            "reads still work; writes return when the lost copies are back, and \
             `Db::node_status` shows the recovery; an earlier send may have applied: then send \
             the same request again with `Db::resend`, or ask `Db::status`; never send it under \
             a new request id"
        }
        ErrorKind::ProtectionPaused if no_mutation => {
            "wait; then a read reads again, and a write is sent again as a new write \
             (`Db::put`, `Db::txn` or `Db::write`), not with `Db::resend`: this send wrote \
             nothing; `Db::node_status` shows what writes are waiting on"
        }
        ErrorKind::ProtectionPaused => {
            "wait; an earlier send may have applied: send the same request again with \
             `Db::resend`, or ask `Db::status`; never send it under a new request id; \
             `Db::node_status` shows what writes are waiting on"
        }
        ErrorKind::ConditionFailed => "read the object again, then decide whether to write",
        ErrorKind::CrossAffinity => {
            "split the request: one request names objects of one affinity group only"
        }
        ErrorKind::InvalidArgument => {
            "fix the argument this message names; the same request fails the same way"
        }
        ErrorKind::Overloaded if no_mutation => {
            "wait for `retry_after`; then a read reads again, and a write is sent again as a new \
             write (`Db::put`, `Db::txn` or `Db::write`), not with `Db::resend`: this send wrote \
             nothing; a shorter `DbConfig::dedup_retention` raises the cap"
        }
        ErrorKind::Overloaded => {
            "wait for `retry_after`; an earlier send may have applied: send the same request \
             again with `Db::resend`, or ask `Db::status`; never send it under a new request id; \
             a shorter `DbConfig::dedup_retention` raises the cap"
        }
        ErrorKind::DeadlineBeforeAdmission if no_mutation => {
            "this send wrote nothing: send the write again as a new write with a longer \
             deadline (`Db::txn` takes one; `Timeouts::put` is a put's), not with \
             `Db::resend_within`"
        }
        ErrorKind::DeadlineBeforeAdmission => {
            "an earlier send may have applied: send the same request again with \
             `Db::resend_within` and a longer deadline, or ask `Db::status`; never send it under \
             a new request id"
        }
        // Only a write carries `QueryStatus`; a read has no request to ask about (tester-s2,
        // A-R230), so any other rule here is a read's.
        ErrorKind::UnknownOutcome if matches!(retry, RetryRule::QueryStatus) => {
            "ask `Db::status` with this request id; never send it under a new request id"
        }
        ErrorKind::UnknownOutcome => {
            "a read changes nothing, so read again; if this repeats, check that the database's \
             nodes and its rEtcd are running (on a local run, `scripts/local-rdb.sh status`)"
        }
        ErrorKind::GenerationChanged => {
            "this send wrote nothing; if the request was sent before, ask `Db::status` with \
             this request id whether an earlier send applied it; never replay it as it is"
        }
        ErrorKind::RequestIdReuse => {
            "use a new request id for a different write; this id stays bound to its first write"
        }
        ErrorKind::StaleContinuation => "start the read again from the beginning (`after: None`)",
        ErrorKind::CorruptHistory => {
            "a damaged map or set entry: write over its key (put or del) to keep the rest; \
             otherwise `Change::Clear` deletes the whole object (embed.md, Known limits)"
        }
        ErrorKind::IncompatibleVersion => "open the folder with the newer build that wrote it",
        ErrorKind::StatusExpired => {
            "the answer is no longer kept: read the object to see its value, and write again only \
             if that value is not the one you want"
        }
        ErrorKind::DivergenceRequiresOperator => {
            "an operator must act: keep this Db's log and report this message; retrying will \
             not help"
        }
        // The owner in another process is out of reach (`ApiError::owner_unreachable`,
        // `owner_lost`, `owner_lost_unsent`), keyed on the rule as well as the proof: a read's
        // `Unavailable` has `BoundedJitter` and keeps the plain words below (review-s2 F-003).
        ErrorKind::Unavailable if matches!(retry, RetryRule::RetryAfterRecovery) && no_mutation => {
            "this call wrote nothing: once the owner is back or another node is promoted, call it \
             again; a read reads again, and a write is sent again as a new write (`Db::put`, \
             `Db::txn` or `Db::write`), not with `Db::resend`"
        }
        ErrorKind::Unavailable if matches!(retry, RetryRule::RetryAfterRecovery) => {
            "once the owner is back, send the same request again with `Db::resend`, or ask \
             `Db::status`; never send it under a new request id"
        }
        // In plain words, not API names (W5, A-R256): the reader may be an app author or a
        // person running `scripts/local-rdb.sh`.
        ErrorKind::Unavailable => {
            "the database is not serving right now: wait a few seconds and try again; if it \
             keeps refusing, check that its nodes and its rEtcd are running (on a local run, \
             `scripts/local-rdb.sh status`)"
        }
        ErrorKind::UploadChunkConflict => "start the upload again under a new upload id",
        // `Db::wait_durable` ends every `Err` with [`BARRIER_STEP`], so this is not reached from
        // there; `step_for` is total. Same meaning.
        ErrorKind::DurabilityUnconfirmed => {
            "the barrier wrote nothing and says nothing about the write; there is no request to \
             send again; call `Db::wait_durable` again"
        }
        ErrorKind::CorruptBlob => "write `Change::Clear` to the blob, then upload it again",
        ErrorKind::TooLarge => {
            "make it smaller: `ApiError::too_large` names the limit, the size and the maximum; \
             the same request fails the same way"
        }
    }
}

/// `ProtectionPaused` → `PROTECTION_PAUSED`.
pub(super) fn wire_name(kind: ErrorKind) -> String {
    let camel = format!("{kind:?}");
    let mut out = String::with_capacity(camel.len() + 4);
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

// Texts for the open and the wait (ruling A-R230 Q3): W7 owns the words, W5 the call sites in
// `db.rs`. Each ends with the step embed.md section 6 gives for it, after `; next: `.
// W5 wired every one of them (host.rs for `corrupt_history`).

/// What `wait_ready` last saw of the owner, for [`ready_timed_out_text`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadySeen<'a> {
    /// The owner answered no status look in the time: it may be busy, for example syncing its
    /// copies to the barrier. Never "unknown": that read as a fault (tester-s2, #59).
    NoAnswer,
    /// The last look could not be sent: no link to the owner is up, as when its process is
    /// stopped. Never "may be busy": nothing reached it (tester tc2 P1, A-R394).
    NoLink,
    /// The owner answered and runs no recovery.
    NoRecovery,
    /// The owner's recovery phase, as it names it.
    Phase(&'a str),
}

/// `ReadyError::TimedOut`'s text. It names the node `wait_ready` asked, the owner, which is not
/// node 1 after a promote (A-R256), and its step is in plain words, not an API name.
pub(crate) fn ready_timed_out_text(within_ms: u128, owner: u32, seen: ReadySeen<'_>) -> String {
    let state = match seen {
        ReadySeen::NoAnswer => format!(
            "node {owner} answered no status look in that time (it may be busy, for example \
             syncing its copies)"
        ),
        ReadySeen::NoLink => format!(
            "node {owner} is not reachable from this process: no link to it is up (its process \
             may have stopped)"
        ),
        ReadySeen::NoRecovery => format!("node {owner} runs no recovery"),
        ReadySeen::Phase(phase) => format!("node {owner}'s recovery phase is {phase}"),
    };
    format!(
        "node {owner} did not admit writes within {within_ms} ms; {state}; next: wait again for \
         longer, or close the database and open its folder again, once; if that times out too, \
         stop and report it with this message"
    )
}

/// The step after the control store does not hold this folder's records.
const NOT_THIS_STORE_STEP: &str = "point `Db::open` at the rEtcd this folder was made with; for a \
                                   new database, start a separate rEtcd";

/// `OpenError::Mismatch`'s detail when the store does not hold this folder's records: its
/// `partitions/1` is absent, names no instance or another one, or its open counter is absent.
/// `what` says which, without a step.
pub(crate) fn not_this_store_text(what: &str) -> String {
    format!("{what}; next: {NOT_THIS_STORE_STEP}")
}

/// `OpenError::Mismatch`'s detail when the copies name different instances: they come from
/// different folders. `named` lists the instances.
pub(crate) fn copies_disagree_text(named: &str) -> String {
    format!(
        "the copies name different instances: {named}; next: the copies come from different \
         folders, so restore this folder's copies from one backup, then open it again"
    )
}

/// `CORRUPT_HISTORY` for a read of `object` that met `corrupt` (#59, A-R230): it names the object,
/// and an element's key when [`rdb_value::Corrupt::Element`] carries one, for the caller only.
/// A log line names neither; `host.rs` logs the damage code and `key_hex`.
///
/// Only a map's or set's entry keeps the kind's step (write over its key). Anything else is the
/// object's own value, a document's included, and ends with [`DAMAGED_OBJECT_STEP`] (A-R420).
pub(crate) fn corrupt_history(object: &[u8], corrupt: &rdb_value::Corrupt) -> ApiError {
    let object = object.escape_ascii();
    let detail = match corrupt {
        rdb_value::Corrupt::Element { .. } | rdb_value::Corrupt::Entry { .. } => {
            format!("object \"{object}\": {corrupt}")
        }
        _ => format!("object \"{object}\": {corrupt}; next: {DAMAGED_OBJECT_STEP}"),
    };
    ApiError::new(ErrorKind::CorruptHistory, detail)
}

/// The open counter's refusal for good, for `admin::CounterError::Exhausted`.
pub(crate) fn counter_exhausted_text(key: &str, max: u64) -> String {
    format!(
        "{key} has counted {max} opens, the most one directory may have; next: start a new \
         folder"
    )
}

/// The open counter's refusal for good, for `admin::CounterError::Unreadable`.
pub(crate) fn counter_unreadable_text(key: &str, revision: u64) -> String {
    format!(
        "{key} at revision {revision} is not an open counter; next: do not delete this \
         folder, its acknowledged writes are intact; stop and report it with this message"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every [`ErrorKind`]. The match fails to compile when a kind is added and not listed.
    fn every_kind() -> Vec<ErrorKind> {
        use ErrorKind::*;
        let all = vec![
            NotPrimary,
            RouteChanged,
            LeaseExpired,
            RecoveryReadOnly,
            ProtectionPaused,
            ConditionFailed,
            CrossAffinity,
            InvalidArgument,
            Overloaded,
            DeadlineBeforeAdmission,
            UnknownOutcome,
            GenerationChanged,
            RequestIdReuse,
            StaleContinuation,
            CorruptHistory,
            IncompatibleVersion,
            StatusExpired,
            DivergenceRequiresOperator,
            Unavailable,
            UploadChunkConflict,
            CorruptBlob,
            DurabilityUnconfirmed,
            TooLarge,
        ];
        for kind in &all {
            match kind {
                NotPrimary
                | RouteChanged
                | LeaseExpired
                | RecoveryReadOnly
                | ProtectionPaused
                | ConditionFailed
                | CrossAffinity
                | InvalidArgument
                | Overloaded
                | DeadlineBeforeAdmission
                | UnknownOutcome
                | GenerationChanged
                | RequestIdReuse
                | StaleContinuation
                | CorruptHistory
                | IncompatibleVersion
                | StatusExpired
                | DivergenceRequiresOperator
                | Unavailable
                | UploadChunkConflict
                | CorruptBlob
                | DurabilityUnconfirmed
                | TooLarge => {}
            }
        }
        all
    }

    /// Every [`RetryRule`], guarded the same way.
    fn every_retry() -> Vec<RetryRule> {
        use RetryRule::*;
        let all = vec![
            RefreshRoute,
            RetryAfterRecovery,
            Definitive,
            BoundedJitter,
            QueryStatus,
            Reconcile,
            Quarantine,
            NotWired,
        ];
        for retry in &all {
            match retry {
                RefreshRoute | RetryAfterRecovery | Definitive | BoundedJitter | QueryStatus
                | Reconcile | Quarantine | NotWired => {}
            }
        }
        all
    }

    /// W7 audit: every error text ends with a next step. Each kind with each retry rule shows
    /// `NAME (detail; next: step)`, the step is words, and `explain` ends with it.
    #[test]
    fn every_error_text_ends_with_a_next_step() {
        let mut missing = Vec::new();
        for kind in every_kind() {
            for retry in every_retry() {
                for no_mutation in [false, true] {
                    let error = ApiError {
                        retry,
                        no_mutation,
                        ..ApiError::new(kind, "d")
                    };
                    let step = error.next_step();
                    let ok = step.is_some_and(|step| {
                        step.split_whitespace().count() >= 3
                            && error.explain() == format!("d; next: {step}")
                            && error.to_string() == format!("{} (d; next: {step})", error.name())
                    });
                    if !ok {
                        missing.push(format!("{kind:?}/{retry:?}/{no_mutation}: {error}"));
                    }
                }
            }
        }
        assert!(missing.is_empty(), "no next step: {missing:#?}");
    }

    /// A-R200, then W5 on A-R256: a refusal with no recorded state says what to do in plain
    /// words. It said "call `wait_ready` or `node_status` for the reason", API names a person
    /// running `scripts/local-rdb.sh` cannot call; a read's unknown outcome said the same.
    /// Cut: put either old step back and this row goes red.
    #[test]
    fn an_unavailable_refusal_says_what_to_do_in_plain_words() {
        let error = ApiError::new(ErrorKind::Unavailable, "node 1 does not admit writes");
        assert_eq!(
            error.to_string(),
            "UNAVAILABLE (node 1 does not admit writes; next: the database is not serving right \
             now: wait a few seconds and try again; if it keeps refusing, check that its nodes \
             and its rEtcd are running (on a local run, `scripts/local-rdb.sh status`))"
        );
        let read = step_for(ErrorKind::UnknownOutcome, RetryRule::BoundedJitter, true);
        for step in [error.next_step().expect("a step"), read] {
            assert!(
                !step.contains("wait_ready") && !step.contains("node_status"),
                "{step}"
            );
        }
    }

    /// #59 (tester-s2, A-R230 Q3): every open and wait text W5's sites will carry ends with a
    /// step after `; next: `, and `TimedOut` never says "unknown", which read as a fault while
    /// node 1 was busy syncing.
    #[test]
    fn every_open_and_wait_text_ends_with_a_next_step() {
        let texts = [
            ready_timed_out_text(5_000, 2, ReadySeen::NoAnswer),
            ready_timed_out_text(5_000, 2, ReadySeen::NoRecovery),
            ready_timed_out_text(5_000, 2, ReadySeen::NoLink),
            ready_timed_out_text(5_000, 2, ReadySeen::Phase("Fencing")),
            not_this_store_text("the copies hold data, but the store has no partitions/1"),
            copies_disagree_text("aa, bb"),
            counter_exhausted_text("opens/x", 8),
            counter_unreadable_text("opens/x", 7),
        ];
        for text in &texts {
            let step = text.rsplit_once("; next: ").map(|(_, step)| step);
            assert!(
                step.is_some_and(|step| step.split_whitespace().count() >= 3),
                "no next step: {text}"
            );
            assert!(!text.contains("unknown"), "{text}");
            // One line, single spaces: a broken string continuation once left "\n" and a run
            // of indent inside these texts.
            assert!(!text.contains('\n') && !text.contains("  "), "{text:?}");
        }
        assert!(texts[0].contains("may be busy"), "{}", texts[0]);
        assert!(texts[2].contains("not reachable"), "{}", texts[2]);
        assert!(!texts[2].contains("may be busy"), "{}", texts[2]);
        assert!(texts[3].contains("phase is Fencing"), "{}", texts[3]);
    }

    /// #59, A-R230: `CORRUPT_HISTORY` names the object and the damaged element's key for the
    /// caller, then says how to keep the rest.
    #[test]
    fn a_damaged_element_names_its_object_and_key() {
        let corrupt = rdb_value::Corrupt::Element {
            key: rdb_value::value::Value::Text("b".into()),
            error: Box::new(rdb_value::Corrupt::SetMemberHasValue { len: 3 }),
        };
        let error = corrupt_history(b"cart\n", &corrupt);
        assert_eq!(error.kind, ErrorKind::CorruptHistory);
        let text = error.to_string();
        assert!(
            text.starts_with("CORRUPT_HISTORY (object \"cart\\n\": element "),
            "{text}"
        );
        assert!(text.contains("\"b\""), "{text}");
        assert!(text.ends_with("(embed.md, Known limits))"), "{text}");
        assert!(!text.contains("admin clear"), "{text}");
    }

    /// A-R420 (tester-q G1/G2): a damaged document has no key to write over, so its
    /// `CORRUPT_HISTORY` leads with the clear, naming the API and the `rdb_dev` command, and
    /// gives no map or set advice. Cut: `corrupt_history` without its document arm, red.
    #[test]
    fn a_damaged_document_says_to_clear_it() {
        let corrupt = rdb_value::Corrupt::VersionWithoutValue;
        let error = corrupt_history(b"d", &corrupt);
        assert_eq!(error.kind, ErrorKind::CorruptHistory);
        assert_eq!(
            error.next_step(),
            None,
            "the detail already ends with its step"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "CORRUPT_HISTORY (object \"d\": {corrupt}; next: the object's own stored value is \
                 damaged, so delete the whole object with `Change::Clear` (in rdb_dev: `admin \
                 clear <object>`), then write it again)"
            )
        );
        assert!(!error.explain().contains("write over its key"));
    }

    /// A-R230 (tester-s2): a refused read said `UNKNOWN_OUTCOME` and to query a status, but a read
    /// has no request. With any rule but `QueryStatus` the step is a read's. Green once W5's
    /// `refused_read` gives the read its own rule; this row pins the text for that rule.
    #[test]
    fn an_unknown_outcome_without_a_request_says_to_read_again() {
        for retry in every_retry() {
            let error = ApiError {
                retry,
                ..ApiError::new(ErrorKind::UnknownOutcome, "the read was refused")
            };
            let read = error
                .explain()
                .contains("a read changes nothing, so read again");
            let status = error.explain().contains("ask `Db::status`");
            match retry {
                RetryRule::QueryStatus => assert!(status && !read, "{error}"),
                RetryRule::NotWired => assert!(!status && !read, "{error}"),
                _ => assert!(read && !status, "{error}"),
            }
        }
    }

    /// A-R360 (critic-q3r): a `LEASE_EXPIRED` step never says a later `STATUS_EXPIRED` means
    /// nothing was written. The step shows on `Db::retried` errors too, whose `no_mutation` is
    /// false: an earlier send may have applied, and spec §8.1 says `STATUS_EXPIRED` does not
    /// prove a write never ran. Sending it as new could apply it twice.
    /// Cut: put the "send it as a new request" clause back and this row goes red.
    /// review-s2 F-002 keeps this for `no_mutation` false only: a first send that proves it
    /// wrote nothing is told to send it as a new write, which the row below pins.
    #[test]
    fn a_lease_expired_step_never_says_to_send_it_as_new() {
        for retry in every_retry() {
            let error = ApiError {
                retry,
                no_mutation: false,
                ..ApiError::new(ErrorKind::LeaseExpired, "lease lapsed")
            };
            let text = error.explain();
            assert!(
                !text.contains("STATUS_EXPIRED")
                    && !text.contains("as a new request")
                    && !text.contains("as a new write"),
                "{text}"
            );
        }
    }

    /// review-s2 F-002: a refused write's step follows `no_mutation`, for every kind and rule.
    /// True (a first send that wrote nothing, which the ack floor counts as settled, so a later
    /// write can expire its id and a resend then applies nothing): never "the same request again"
    /// and, wherever it says to send again, "as a new write". False (an earlier send may have
    /// applied): never "as a new write".
    /// Cut: the `no_mutation` arms of `step_for` removed, so true takes false's step, red.
    #[test]
    fn a_write_that_wrote_nothing_is_sent_as_new_and_one_that_may_have_applied_is_resent() {
        for kind in every_kind() {
            for retry in every_retry() {
                let with = |no_mutation| {
                    ApiError {
                        retry,
                        no_mutation,
                        ..ApiError::new(kind, "d")
                    }
                    .explain()
                };
                let (nothing, maybe) = (with(true), with(false));
                assert!(
                    !nothing.contains("same request again"),
                    "{kind:?}: {nothing}"
                );
                assert!(
                    !nothing.contains("Db::resend") || nothing.contains("as a new write"),
                    "{kind:?}/{retry:?}: {nothing}"
                );
                assert!(!maybe.contains("as a new write"), "{kind:?}: {maybe}");
            }
        }
    }

    /// review-s2 F-003: the owner went out of reach after the write was sent, so it may have
    /// applied. Its step is picked from the kind, the rule and `no_mutation`, not from how the
    /// detail ends, and it is said once: resend the same request or ask its status, never a new
    /// id. It was the plain "wait a few seconds and try again", appended after the detail's own
    /// resend step, and an app that "tries again" with `Db::put` mints a new id. The same owner
    /// lost before the write was sent says to send it as a new write.
    /// Cut: the `RetryAfterRecovery` arms of `step_for` removed, red.
    #[test]
    fn an_owner_lost_under_a_write_says_once_to_resend_or_ask_status() {
        let lost = ApiError::owner_lost(NodeId(2));
        let generic = step_for(ErrorKind::Unavailable, RetryRule::BoundedJitter, false);
        let step = lost.next_step().expect("a step");
        assert_ne!(step, generic, "{lost}");
        let text = lost.explain();
        assert_eq!(text.matches("; next: ").count(), 1, "{text}");
        assert_eq!(text.matches("Db::resend").count(), 1, "{text}");
        assert!(
            step.contains("`Db::resend`") && step.contains("`Db::status`"),
            "{text}"
        );
        assert!(
            step.contains("never send it under a new request id"),
            "{text}"
        );
        assert!(!text.contains("try again"), "{text}");

        for unsent in [
            ApiError::owner_lost_unsent(NodeId(2)),
            ApiError::owner_unreachable(NodeId(2)),
        ] {
            let text = unsent.explain();
            assert!(text.contains("as a new write"), "{text}");
            assert!(!text.contains("same request again"), "{text}");
        }
    }

    /// review-s2 F-003: an `UNAVAILABLE` that carries the quarantined text already says what to
    /// do, so it gets no second step; "wait a few seconds and try again" contradicted it.
    /// Cut: `next_step` does not stop on `QUARANTINED_STEP`, red.
    #[test]
    fn an_unavailable_that_carries_the_quarantined_text_gets_no_second_step() {
        let quarantined = super::super::ReadyError::Quarantined { owner: NodeId(2) }.to_string();
        assert!(quarantined.ends_with(QUARANTINED_STEP), "{quarantined}");
        let error = ApiError::new(ErrorKind::Unavailable, format!("d; {quarantined}"));
        assert_eq!(error.next_step(), None, "{error}");
        assert_eq!(error.explain(), format!("d; {quarantined}"));
        assert!(!error.to_string().contains("try again"), "{error}");
    }

    /// A host fault (`NotWired`) says retrying at this node will not help, whatever its kind.
    /// PR #104 review: a barrier's `Err` keeps the step that fits it. A retryable one says to call
    /// the barrier again and never to resend; a bad argument says to fix it, and a host fault to
    /// report it, and neither says to call again; a detail that already ends with its own step
    /// gets no second one. Cut: append [`BARRIER_STEP`] to every `Err`, and the argument and the
    /// host fault are told to call again.
    #[test]
    fn a_barrier_error_keeps_the_step_that_fits_it() {
        let retryable = [
            ApiError::new(ErrorKind::DurabilityUnconfirmed, "not proved"),
            ApiError::from_kernel(&RdbError::GenerationChanged {
                expected: rdb_core::contracts::ids::Generation(1),
                current: rdb_core::contracts::ids::Generation(2),
            }),
            ApiError::from_kernel(&RdbError::NotPrimary {
                partition: rdb_core::contracts::ids::PartitionId(1),
                hint: None,
            }),
        ];
        for error in retryable {
            let error = error.for_barrier();
            assert!(!error.no_mutation, "{error:?}");
            assert!(error.detail.ends_with(BARRIER_STEP), "{error:?}");
            assert_eq!(error.next_step(), None, "{error:?}");
            assert!(
                !error.explain().contains("send the same request"),
                "{error:?}"
            );
        }

        let argument =
            ApiError::invalid("seq 9 is above what partition 1 has applied").for_barrier();
        let explained = argument.explain();
        assert!(explained.contains(BARRIER_NOTE), "{explained}");
        assert!(explained.contains("fix the argument"), "{explained}");
        assert!(
            !explained.contains("call `Db::wait_durable` again"),
            "{explained}"
        );

        let host = ApiError::owner_fault("the owner's node thread has stopped").for_barrier();
        let explained = host.explain();
        assert!(explained.contains(BARRIER_NOTE), "{explained}");
        assert_eq!(
            host.next_step(),
            Some(owner_fault_step(false)),
            "{explained}"
        );
        assert!(
            explained.contains("retrying at this node will not help"),
            "{explained}"
        );
        assert!(
            !explained.contains("call `Db::wait_durable` again"),
            "{explained}"
        );
        assert!(
            !explained.contains("send the same request again"),
            "{explained}"
        );

        let mut reopen = ApiError::new(ErrorKind::Unavailable, "stuck");
        reopen.detail = format!("stuck; {OPEN_AGAIN}");
        let kept = reopen.clone().for_barrier();
        assert_eq!(kept.detail, reopen.detail, "its own step stays last");
    }

    /// PR #129 review, #134: a host fault says retrying at this node will not help, and, if the
    /// owner faulted, that a call can succeed once another node takes over, after
    /// `Db::wait_ready` returns `Ok` (this `Db` routes to the old owner until it reads the owner
    /// again). `no_mutation` true sends a write again as a new write; false asks its status
    /// first, and a read reads again. A `NotWired` refusal that is not a host fault keeps its
    /// plain step. Cuts: `next_step` without its host-fault arm: red on the takeover clause;
    /// `owner_fault_step` false reusing true's words: red on `Db::status`.
    #[test]
    fn a_host_fault_says_retrying_will_not_help() {
        for no_mutation in [false, true] {
            let mut error = ApiError::owner_fault("storage read fault");
            error.no_mutation = no_mutation;
            let text = error.explain();
            let case = format!("no_mutation={no_mutation}: {text}");
            assert!(
                text.starts_with(
                    "host fault: storage read fault; next: retrying at this node will not help: \
                     keep this Db's log and report this message; if the owner faulted, once \
                     another node takes over (an operator promote does that), call \
                     `Db::wait_ready` until it returns `Ok`; then"
                ),
                "{case}"
            );
            assert_eq!(text.contains("as a new write"), no_mutation, "{case}");
            assert_eq!(
                text.contains(
                    "for a write, ask `Db::status` with its request id before writing again, and \
                     never send it under a new request id; for a read, read again"
                ),
                !no_mutation,
                "{case}"
            );
        }

        // Not a host fault: an undecodable call or an admin refusal keeps its own step.
        let refusal = ApiError {
            retry: RetryRule::NotWired,
            no_mutation: true,
            ..ApiError::new(ErrorKind::Unavailable, "the call could not be decoded")
        };
        assert_eq!(
            refusal.next_step(),
            Some("retrying will not help: keep this Db's log and report this message"),
            "{refusal}"
        );
    }

    /// PR #129 review: a host fault proven to have written nothing tells a read, after
    /// `Db::wait_ready`, to read again. Cut: drop "a read reads again" from `owner_fault_step`
    /// true: red.
    #[test]
    fn an_unsent_host_fault_says_a_read_reads_again() {
        let mut error = ApiError::owner_fault("storage read fault");
        error.no_mutation = true;
        let step = error.next_step().expect("a host fault has a step");
        assert!(
            step.contains("call `Db::wait_ready` until it returns `Ok`; then a read reads again"),
            "{step}"
        );
    }

    /// PR #129 review: a host fault proven to have written nothing sends a write again as a new
    /// write, naming the calls that do that, and never with `Db::resend`. Cut: drop the call
    /// names or "not with `Db::resend`" from `owner_fault_step` true: red.
    #[test]
    fn an_unsent_host_fault_sends_a_write_as_new_never_resend() {
        let mut error = ApiError::owner_fault("storage read fault");
        error.no_mutation = true;
        let step = error.next_step().expect("a host fault has a step");
        assert!(
            step.contains(
                "a write is sent again as a new write (`Db::put`, `Db::txn` or `Db::write`), not \
                 with `Db::resend`: this send wrote nothing"
            ),
            "{step}"
        );
    }

    /// #134: only a true owner fault gets the takeover step. An owner fault from a real site
    /// (`unanswered`, a local owner that dropped the call) gets it on both `no_mutation`
    /// branches; a host error that is not an owner fault (`written` of nothing, an answer of the
    /// wrong shape) keeps "retrying will not help". Cut: `next_step` testing
    /// `detail.contains("host ")` instead of the marker, or `host` setting the marker: red on
    /// the non-fault half.
    #[test]
    fn only_an_owner_fault_gets_the_takeover_step() {
        const PLAIN: &str = "retrying will not help: keep this Db's log and report this message";
        let fault = super::super::unanswered(true, NodeId(1), super::super::Carries::Nothing);
        for no_mutation in [false, true] {
            let mut fault = fault.clone();
            fault.no_mutation = no_mutation;
            assert_eq!(
                fault.next_step(),
                Some(owner_fault_step(no_mutation)),
                "no_mutation={no_mutation}: {fault}"
            );
            assert!(
                fault
                    .explain()
                    .contains("once another node takes over (an operator promote does that)"),
                "{fault}"
            );
        }
        let nothing = super::super::written(None).expect_err("nothing was written");
        let shape = ApiError::host("a write was answered with Unchanged");
        for error in [nothing.error, shape] {
            assert!(!error.owner_fault, "{error}");
            assert!(error.detail.starts_with(HOST_ERROR), "{error}");
            assert_eq!(error.next_step(), Some(PLAIN), "{error}");
            assert!(!error.explain().contains("takes over"), "{error}");
        }
    }

    /// #134: the wire carries no marker, so an answer that crossed a link is an owner fault by
    /// its detail's prefix: an owner fault keeps the takeover step and a host error keeps its
    /// plain step after a round trip. Cut: `From<WireApiError>` setting `owner_fault: false`:
    /// red on the fault.
    #[test]
    fn the_owner_fault_marker_survives_the_wire() {
        use crate::net::wire::WireApiError;
        for error in [
            ApiError::owner_fault("the owner's node thread has stopped"),
            ApiError::host("a read was answered with Unchanged"),
        ] {
            let landed = ApiError::from(WireApiError::from(&error));
            assert_eq!(landed, error);
            assert_eq!(landed.next_step(), error.next_step(), "{error}");
        }
    }

    /// When `db.rs` already ended the detail with "open the folder again", no second step is
    /// added, except `UnknownOutcome`'s, which still holds after the new open.
    #[test]
    fn a_detail_that_already_says_open_again_gets_no_second_step() {
        assert!(super::super::REOPEN_STEP.ends_with(OPEN_AGAIN));
        let detail = format!("refused; {}", super::super::REOPEN_STEP);
        for kind in every_kind() {
            let error = ApiError::new(kind, detail.clone());
            if kind == ErrorKind::UnknownOutcome {
                assert!(error
                    .explain()
                    .ends_with("never send it under a new request id"));
            } else {
                assert_eq!(error.next_step(), None, "{kind:?}");
                assert_eq!(error.explain(), detail, "{kind:?}");
            }
        }
        // An admin store error that says to run it again keeps that as its only step.
        let admin = ApiError {
            retry: RetryRule::NotWired,
            ..ApiError::new(
                ErrorKind::Unavailable,
                "control store: x; the delete may have applied; run it again to read where it \
                 stands",
            )
        };
        assert_eq!(admin.next_step(), None, "{admin}");
        // A read's `UnknownOutcome` says "read again", which a reopen step contradicts.
        let read = ApiError {
            retry: RetryRule::BoundedJitter,
            ..ApiError::new(ErrorKind::UnknownOutcome, detail.clone())
        };
        assert_eq!(read.next_step(), None, "{read}");
    }
}
