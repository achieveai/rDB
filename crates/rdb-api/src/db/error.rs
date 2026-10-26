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
}

/// The tail every "open the folder again" step in `db.rs` ends with ([`super::REOPEN_STEP`] and
/// the stuck-recovery step). A detail that ends with it already says what to do next.
const OPEN_AGAIN: &str = "open the folder again";

/// The tail `admin.rs` ends a store error with when a delete may have applied: it already says
/// to run the call again, which a "retrying will not help" step would contradict (rdb_dev row
/// `an_admin_error_that_may_have_deleted_says_a_rerun_shows_whether_it_did` pins the text).
const RUN_AGAIN: &str = "run it again to read where it stands";

/// The tail every `admin rebuild` error ends with (#65): its detail already names the step
/// (stop it, send a write, promote first, restart it), which "retrying will not help" would
/// contradict (bb walk paper cut).
pub(crate) const REBUILD_STEP: &str = "the rebuild did nothing more than this says";

/// The tail of [`super::ReadyError::Quarantined`]'s text, which a refusal carries while the
/// owner's recovery is quarantined ([`super::Db::explained`]). It already says what to do, which
/// a "wait a few seconds and try again" step would contradict (review-s2 F-003).
pub(crate) const QUARANTINED_STEP: &str =
    "do not delete the folder; stop and report it with this message";

/// The step a damaged object's root (a document, or a map's or set's own root) ends its
/// `CORRUPT_HISTORY` with ([`corrupt_history`]), in place of the map or set entry's step: no
/// key can be written over, so only a clear helps (A-R420, tester-q G1/G2). It names the API
/// and the `rdb_dev` command, since the reader may be either.
pub(crate) const DAMAGED_OBJECT_STEP: &str =
    "the object's own stored value is damaged, so delete the whole object with `Change::Clear` \
     (in rdb_dev: `admin clear <object>`), then write it again";

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
    /// | `ConditionFailed`, `InvalidArgument` | `Definitive` | `true` |
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
            ErrorKind::ConditionFailed | ErrorKind::InvalidArgument => {
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
        }
    }

    /// A caller's argument the API refuses before the kernel sees it.
    #[must_use]
    pub fn invalid(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, detail)
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
        }
    }

    /// The node faulted (§4.4): the host could not serve an effect. Nothing is retried.
    #[must_use]
    pub fn host(detail: &str) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::NotWired,
            no_mutation: false,
            detail: format!("host fault: {detail}"),
            retry_after: None,
        }
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
            || self.detail.ends_with(QUARANTINED_STEP)
            || self.detail.ends_with(DAMAGED_OBJECT_STEP)
        {
            return None;
        }
        Some(step_for(self.kind, self.retry, self.no_mutation))
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
        ErrorKind::CorruptBlob => "write `Change::Clear` to the blob, then upload it again",
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
                | CorruptBlob => {}
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

    /// A host fault (`NotWired`) says no retry helps, whatever its kind.
    #[test]
    fn a_host_fault_says_retrying_will_not_help() {
        let error = ApiError::host("storage read fault");
        assert!(
            error
                .explain()
                .starts_with("host fault: storage read fault; next: retrying will not help"),
            "{error}"
        );
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
