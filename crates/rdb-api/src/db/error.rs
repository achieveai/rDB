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

    /// What the caller does next, in words, for this kind and retry rule. `None` when
    /// [`Self::detail`] already ends with the step: the host said to shut the `Db` down and open
    /// the folder again, and a second step would contradict it. A write's `UnknownOutcome`
    /// (`QueryStatus`) keeps its step even then, because asking [`super::Db::status`] with the same id is still right after the
    /// new open.
    #[must_use]
    pub fn next_step(&self) -> Option<&'static str> {
        let write_unknown =
            self.kind == ErrorKind::UnknownOutcome && self.retry == RetryRule::QueryStatus;
        if self.detail.ends_with(OPEN_AGAIN) && !write_unknown {
            return None;
        }
        Some(step_for(self.kind, self.retry))
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

/// The next step for an error of `kind` with `retry`. Total over [`ErrorKind`], so a new kind
/// does not compile until it has a step. `NotWired` wins over the kind: nothing in this open
/// serves it, so no kind's retry advice applies.
const fn step_for(kind: ErrorKind, retry: RetryRule) -> &'static str {
    if matches!(retry, RetryRule::NotWired) {
        return "retrying will not help: keep this Db's log and report this message; to try \
                again, shut this Db down and open the folder again";
    }
    match kind {
        ErrorKind::NotPrimary | ErrorKind::RouteChanged => {
            "send the same request again with `Db::resend`; if this repeats, `Db::node_status` \
             shows which node is primary"
        }
        ErrorKind::LeaseExpired => {
            "call `Db::wait_ready`, then send the same request again with `Db::resend`"
        }
        ErrorKind::RecoveryReadOnly => {
            "reads still work; writes return when the lost copies are back, and \
             `Db::node_status` shows the recovery"
        }
        ErrorKind::ProtectionPaused => {
            "wait, then send the same request again with `Db::resend`; `Db::node_status` shows \
             what writes are waiting on"
        }
        ErrorKind::ConditionFailed => "read the object again, then decide whether to write",
        ErrorKind::CrossAffinity => {
            "split the request: one request names objects of one affinity group only"
        }
        ErrorKind::InvalidArgument => {
            "fix the argument this message names; the same request fails the same way"
        }
        ErrorKind::Overloaded => {
            "wait for `retry_after`, then send the same request again with `Db::resend`; a \
             shorter `DbConfig::dedup_retention` raises the cap"
        }
        ErrorKind::DeadlineBeforeAdmission => {
            "send the same request again with `Db::resend_within` and a longer deadline"
        }
        // Only a write carries `QueryStatus`; a read has no request to ask about (tester-s2,
        // A-R230), so any other rule here is a read's.
        ErrorKind::UnknownOutcome if matches!(retry, RetryRule::QueryStatus) => {
            "ask `Db::status` with this request id; never send it under a new request id"
        }
        ErrorKind::UnknownOutcome => {
            "a read changes nothing, so read again; if this repeats, call `wait_ready` or \
             `node_status` for the reason"
        }
        ErrorKind::GenerationChanged => {
            "ask `Db::status` with this request id to learn whether it applied; never replay it \
             as it is"
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
            "the answer is no longer kept: read the object to see whether the write is there"
        }
        ErrorKind::DivergenceRequiresOperator => {
            "an operator must act: keep this Db's log and report this message; retrying will \
             not help"
        }
        ErrorKind::Unavailable => "call `wait_ready` or `node_status` for the reason",
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
// Unused outside tests until W5 swaps those sites, hence the `allow`s; W5 removes them.

/// What `wait_ready` last saw of node 1, for [`ready_timed_out_text`].
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadySeen<'a> {
    /// Node 1 answered no status look in the time: it may be busy, for example syncing its
    /// copies to the barrier. Never "unknown": that read as a fault (tester-s2, #59).
    NoAnswer,
    /// Node 1 answered and runs no recovery.
    NoRecovery,
    /// Node 1's recovery phase, as it names it.
    Phase(&'a str),
}

/// `ReadyError::TimedOut`'s text.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn ready_timed_out_text(within_ms: u128, seen: ReadySeen<'_>) -> String {
    let state = match seen {
        ReadySeen::NoAnswer => {
            "node 1 answered no status look in that time (it may be busy, for example syncing \n             its copies)"
                .to_owned()
        }
        ReadySeen::NoRecovery => "node 1 runs no recovery".to_owned(),
        ReadySeen::Phase(phase) => format!("node 1's recovery phase is {phase}"),
    };
    format!(
        "node 1 did not admit writes within {within_ms} ms; {state}; next: call `wait_ready` \n         again with a longer bound, or shut this Db down and open the folder again, once; if \n         that times out too, stop and report it with this message"
    )
}

/// The step after the control store does not hold this folder's records.
#[cfg_attr(not(test), allow(dead_code))]
const NOT_THIS_STORE_STEP: &str = "point `Db::open` at the rEtcd this folder was made with; for a \n                                   new database, start a separate rEtcd";

/// `OpenError::Mismatch`'s detail when the store does not hold this folder's records: its
/// `partitions/1` is absent, names no instance or another one, or its open counter is absent.
/// `what` says which, without a step.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn not_this_store_text(what: &str) -> String {
    format!("{what}; next: {NOT_THIS_STORE_STEP}")
}

/// `OpenError::Mismatch`'s detail when the copies name different instances: they come from
/// different folders. `named` lists the instances.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn copies_disagree_text(named: &str) -> String {
    format!(
        "the copies name different instances: {named}; next: the copies come from different \n         folders, so restore this folder's copies from one backup, then open it again"
    )
}

/// The open counter's refusal for good, for `admin::CounterError::Exhausted`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn counter_exhausted_text(key: &str, max: u64) -> String {
    format!(
        "{key} has counted {max} opens, the most one directory may have; next: start a new \n         folder"
    )
}

/// The open counter's refusal for good, for `admin::CounterError::Unreadable`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn counter_unreadable_text(key: &str, revision: u64) -> String {
    format!(
        "{key} at revision {revision} is not an open counter; next: do not delete this \n         folder, its acknowledged writes are intact; stop and report it with this message"
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
                NotPrimary | RouteChanged | LeaseExpired | RecoveryReadOnly | ProtectionPaused
                | ConditionFailed | CrossAffinity | InvalidArgument | Overloaded
                | DeadlineBeforeAdmission | UnknownOutcome | GenerationChanged
                | RequestIdReuse | StaleContinuation | CorruptHistory | IncompatibleVersion
                | StatusExpired | DivergenceRequiresOperator | Unavailable
                | UploadChunkConflict | CorruptBlob => {}
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
                let error = ApiError {
                    retry,
                    ..ApiError::new(kind, "d")
                };
                let step = error.next_step();
                let ok = step.is_some_and(|step| {
                    step.split_whitespace().count() >= 3
                        && error.explain() == format!("d; next: {step}")
                        && error.to_string() == format!("{} (d; next: {step})", error.name())
                });
                if !ok {
                    missing.push(format!("{kind:?}/{retry:?}: {error}"));
                }
            }
        }
        assert!(missing.is_empty(), "no next step: {missing:#?}");
    }

    /// A-R200: a refusal with no recorded state names where the reason is.
    #[test]
    fn an_unavailable_refusal_says_where_the_reason_is() {
        let error = ApiError::new(ErrorKind::Unavailable, "node 1 does not admit writes");
        assert_eq!(
            error.to_string(),
            "UNAVAILABLE (node 1 does not admit writes; next: call `wait_ready` or \
             `node_status` for the reason)"
        );
    }

    /// #59 (tester-s2, A-R230 Q3): every open and wait text W5's sites will carry ends with a
    /// step after `; next: `, and `TimedOut` never says "unknown", which read as a fault while
    /// node 1 was busy syncing.
    #[test]
    fn every_open_and_wait_text_ends_with_a_next_step() {
        let texts = [
            ready_timed_out_text(5_000, ReadySeen::NoAnswer),
            ready_timed_out_text(5_000, ReadySeen::NoRecovery),
            ready_timed_out_text(5_000, ReadySeen::Phase("Fencing")),
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
        }
        assert!(texts[0].contains("may be busy"), "{}", texts[0]);
        assert!(texts[2].contains("phase is Fencing"), "{}", texts[2]);
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
            let read = error.explain().contains("a read changes nothing, so read again");
            let status = error.explain().contains("ask `Db::status`");
            match retry {
                RetryRule::QueryStatus => assert!(status && !read, "{error}"),
                RetryRule::NotWired => assert!(!status && !read, "{error}"),
                _ => assert!(read && !status, "{error}"),
            }
        }
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
                assert!(error.explain().ends_with("never send it under a new request id"));
            } else {
                assert_eq!(error.next_step(), None, "{kind:?}");
                assert_eq!(error.explain(), detail, "{kind:?}");
            }
        }
        // A read's `UnknownOutcome` says "read again", which a reopen step contradicts.
        let read = ApiError {
            retry: RetryRule::BoundedJitter,
            ..ApiError::new(ErrorKind::UnknownOutcome, detail.clone())
        };
        assert_eq!(read.next_step(), None, "{read}");
    }
}
