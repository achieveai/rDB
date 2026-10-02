//! The admission seam: what L1 publishes about exposure and liveness, and what T1 replies.
//!
//! Contract types only. When admission pauses, when it resumes and what the lag domain is are
//! package L1's rules (team kernel-b `design.md` §4.1, §4.2, §4.4); the shape is here because
//! three modules read it — T1 for the two decision fields, P1 and telemetry for the rest.
//!
//! Distinct from [`crate::protection`], which is the L1 kernel module itself.

use core::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::contracts::errors::ErrorKind;
use crate::contracts::ids::{ConfigVersion, Seq};
use crate::contracts::membership::CopyId;

/// How stale our freshest evidence is that every peer we depend on is keeping up (team kernel-b
/// `design.md` §4.2).
///
/// Liveness, not exposure: its input is one `PeerProgress` per accepted acknowledgement, and it
/// is the `max` over the lag domain — the pinned predicate's copies, minus self, minus `lost`.
/// The 250 ms resume threshold reads this value and nothing else.
///
/// # Why this is a newtype and not `Option<u64>` (lead ruling R-S3)
///
/// A copy never heard from has **infinite** lag and must block resume: fail-closed on absence,
/// exactly as `NotRetained` fails P1's digest conjunct. Both obvious spellings of that are
/// traps, and they fail in opposite directions:
///
/// * `u64::MAX` arithmetics silently — one subtraction and the infinity is gone.
/// * `Option<u64>` with `None` meaning infinite orders **backwards** under a derived `Ord`,
///   because `None < Some(_)`. The never-heard-from peer would sort as the *least* lagged, and
///   [`AdmissionState::stalest_copy`] is chosen by exactly that comparison — so the one copy
///   that must hold resume open would be the one copy never selected.
///
/// The representation is `Option<u64>` behind a private field, and the ordering is written by
/// hand below so [`Self::INFINITE`] is the greatest value there is. The derive is forbidden on
/// this ordering; a future edit that replaces `impl Ord` with `#[derive(Ord)]` silently inverts
/// the resume rule.
///
/// The assertion that catches that edit is the comparison the rule turns on, so it is written
/// here rather than left entirely to the kernel row that also owns it:
///
/// ```
/// use rdb_core::ReplicationLag;
///
/// let never_heard_from = ReplicationLag::INFINITE;
/// let nearly_forever = ReplicationLag::millis(u64::MAX - 1);
///
/// assert!(never_heard_from > nearly_forever);
/// assert_eq!(
///     [nearly_forever, never_heard_from, ReplicationLag::ZERO]
///         .iter()
///         .max(),
///     Some(&never_heard_from),
///     "stalest_copy is chosen by this max; the never-heard-from peer has to win it"
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplicationLag(Option<u64>);

impl ReplicationLag {
    /// No lag at all.
    pub const ZERO: Self = Self(Some(0));

    /// A copy we have never heard from. Greater than every finite lag, including
    /// `ReplicationLag::millis(u64::MAX)`.
    pub const INFINITE: Self = Self(None);

    /// A finite lag, in milliseconds.
    #[must_use]
    pub const fn millis(millis: u64) -> Self {
        Self(Some(millis))
    }

    /// The lag in milliseconds, or `None` when it is [`Self::INFINITE`].
    ///
    /// Returns an `Option` rather than a saturating number on purpose: a caller that wants to
    /// do arithmetic has to say what it means by infinity first.
    #[must_use]
    pub const fn as_millis(self) -> Option<u64> {
        self.0
    }

    /// Whether this is [`Self::INFINITE`].
    #[must_use]
    pub const fn is_infinite(self) -> bool {
        self.0.is_none()
    }
}

impl Ord for ReplicationLag {
    /// [`Self::INFINITE`] is the greatest value. Written by hand, never derived — see the type's
    /// documentation for what the derive would do.
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0, other.0) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(ours), Some(theirs)) => ours.cmp(&theirs),
        }
    }
}

impl PartialOrd for ReplicationLag {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// What L1 publishes about admission — the `L1 -> T1/P1` seam (team kernel-b `design.md` §4.5,
/// spike §4).
///
/// T1 consumes [`Self::allow`] and [`Self::reason`] and passes the rest through to telemetry; it
/// never computes any of it. An operator watching a pause needs three different questions
/// answered — is the exposure draining, are the copies responsive, and if not which one — so
/// exposure and liveness are exported separately and the stalest copy is named (team kernel-b
/// `design.md` §4.2, spec §6.2 "export age and bytes separately").
///
/// Not `Copy`: two of the eleven fields are `Vec`s.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AdmissionState {
    /// Whether T1 may admit a transaction.
    pub allow: bool,
    /// The answer T1 replies when it may not, and `None` while [`Self::allow`] is true.
    ///
    /// **T1 replies this value; it does not map it to a code of its own** (team kernel-a
    /// `design.md` §1.6, check 8 of team kernel-b `design.md` §3.2). That is the whole reason
    /// the field is an [`ErrorKind`] rather than a private admission-denial enum: a private
    /// enum would force T1 to choose a client code, which is the one thing the design says T1
    /// must not do.
    ///
    /// Two values reach it, and they are different client answers:
    /// [`ErrorKind::DivergenceRequiresOperator`] whenever L1's `blocked` is set — *nothing on
    /// the data path will change this* — and [`ErrorKind::ProtectionPaused`] otherwise while
    /// `!allow` — *retry later*. The first is filled from
    /// [`crate::contracts::authority::BlockReason::client_error_kind`], which is the one place
    /// the two layers of that fact are mapped onto each other (lead rulings R-S2, B-R31,
    /// finding K-B-46).
    pub reason: Option<ErrorKind>,
    /// Exposure: how long the oldest locally applied record has gone without being durable on
    /// every required copy, in milliseconds. Zero when the unsafe queue is empty — by
    /// definition, not by "now minus last activity", which is spec §6.2's rule that idle
    /// partitions do not become falsely unsafe. `warn_ms` and `pause_ms` read this one.
    pub oldest_unsafe_age: u64,
    /// The sequence of that oldest unsafe record.
    pub oldest_unsafe_seq: Seq,
    /// Liveness: the `max` lag over the lag domain. `resume_lag_ms` reads this one, and only
    /// this one — see [`ReplicationLag`] for why it is not an `Option<u64>`.
    pub replication_lag: ReplicationLag,
    /// The copy holding [`Self::replication_lag`] up, and `None` when the lag domain is empty.
    ///
    /// Chosen by the `max` over [`ReplicationLag`], so a copy never heard from wins it.
    pub stalest_copy: Option<CopyId>,
    /// L1's `lost` set, so a pause on two copies is visible rather than inferred.
    pub lost_copies: Vec<CopyId>,
    /// The prefix admission was paused after.
    ///
    /// Always meaningful: L1 is constructed `Paused` (team kernel-b `design.md` §4.1), so there
    /// is no state before the first pause for this to be absent in.
    pub paused_prefix: Seq,
    /// The durable barrier that must be met before `Paused` may move to `Reprotecting`.
    pub resume_barrier: Seq,
    /// Every active protection predicate's configuration version, for telemetry. More than one
    /// while a membership transition is in flight and the old predicate has not retired.
    pub required_config_versions: Vec<ConfigVersion>,
    /// The sum of `bytes` over the unsafe queue.
    ///
    /// Spec §6.2 wants age and bytes exported separately, because a small number of large
    /// records and a large number of small ones are the same age and very different exposure.
    pub outstanding_unsafe_bytes: u64,
}
