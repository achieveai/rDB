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

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", wire_name(self.kind), self.detail)
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
