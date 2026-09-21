//! Replay.
//!
//! A seed is not a reproducer (spike §4). Replay takes the recorded event stream, re-runs it, and
//! proves the result is identical — which is the only evidence that the kernel is actually
//! deterministic rather than merely usually the same.
//!
//! The comparison is over the whole trace, not over a verdict. Two runs that both fail the same
//! invariant at different sequences are not a successful replay.
//!
//! # Seed state
//!
//! Signatures only; package I1 lands replay.

use rdb_core::contracts::trace::Trace;

use crate::error::SimError;

/// How a replay came out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// Every event matched, in order and in content.
    Identical,
    /// The traces diverged.
    Diverged {
        /// The `event_id` of the first event that differed, or of the first missing event.
        first_divergence: u64,
        /// What was recorded, rendered for a report.
        recorded: String,
        /// What the replay produced.
        replayed: String,
    },
    /// The trace could not be replayed at all: a schema or generator version mismatch.
    Unreplayable {
        /// Why.
        reason: &'static str,
    },
}

/// Re-run `trace` and compare.
///
/// # Errors
///
/// [`SimError::Unavailable`] until package I1 lands replay.
pub const fn replay(_trace: &Trace) -> Result<ReplayOutcome, SimError> {
    Err(SimError::unavailable("harness::replay::replay"))
}
