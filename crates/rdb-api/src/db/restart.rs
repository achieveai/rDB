//! #81 V10: when a node process should restart, the rule [`super::Db::restart_wanted`] polls
//! and `rdb_node` exits 75 on.

use std::time::{Duration, Instant};

use config_core::{ConfigStore, ListRequest};
use rdb_core::authority::partition::PartitionRecord;
use rdb_core::contracts::control::ControlPrefix;
use rdb_core::contracts::ids::NodeId;

/// Why [`super::Db::restart_wanted`] asks for a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RestartReason {
    /// A1's self-replace has stayed stopped for [`RESTART_HOLD`]: a promote froze this node's
    /// grant, and nothing at this boot lifts it.
    ReplaceStopped,
    /// The host faulted (V10b, lead ruling): a faulted host stays down until a restart.
    HostFaulted,
}

impl RestartReason {
    /// The reason as logs and `rdb_node`'s `restart-wanted` line spell it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReplaceStopped => "replace_stopped",
            Self::HostFaulted => "host_faulted",
        }
    }
}

impl std::fmt::Display for RestartReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How long `replace_stopped` must have stayed true before a restart is due.
pub(crate) const RESTART_HOLD: Duration = Duration::from_secs(5);

/// Whether a restart is due, and why. Due only when `owns_none` (asked last, and only then)
/// says no `partitions/` record names this process's nodes owner: a restart while one still
/// owns a partition would cut that partition's writes short of the planner's move. Then due
/// when the host faulted, or when the self-replace has stayed stopped since `stopped_since`
/// for [`RESTART_HOLD`].
pub(crate) fn restart_due(
    stopped_since: Option<Instant>,
    faulted: bool,
    now: Instant,
    owns_none: impl FnOnce() -> bool,
) -> Option<RestartReason> {
    let reason = if faulted {
        RestartReason::HostFaulted
    } else if stopped_since
        .is_some_and(|since| now.saturating_duration_since(since) >= RESTART_HOLD)
    {
        RestartReason::ReplaceStopped
    } else {
        return None;
    };
    owns_none().then_some(reason)
}

/// Whether no `partitions/` record names any of `nodes` owner, in any lifecycle (`Fencing`
/// too). Read from the control store, not from a `Db`'s route, which refreshes lazily. A failed
/// or truncated list, or a record that does not decode, is an error: the caller stays up.
pub(crate) async fn owns_no_partition(
    store: &dyn ConfigStore,
    nodes: &[NodeId],
) -> Result<bool, String> {
    let prefix = ControlPrefix::Partitions.encode();
    let listed = store
        .list(ListRequest {
            prefix: bytes::Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .map_err(|e| format!("list {prefix}: {e}"))?;
    if listed.truncated {
        return Err(format!(
            "list {prefix} came back truncated at revision {}",
            listed.read_revision
        ));
    }
    for found in &listed.records {
        let record = PartitionRecord::decode(&found.value).ok_or_else(|| {
            format!(
                "{} is not a partition record",
                String::from_utf8_lossy(&found.key)
            )
        })?;
        if nodes.contains(&record.owner) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{restart_due, RestartReason, RESTART_HOLD};
    use std::time::{Duration, Instant};

    /// V10 (#81) `restart_due`: a stopped self-replace is due only once held for
    /// `RESTART_HOLD`, and only when the node owns no partition; the ownership read is not
    /// made while nothing else is due. Unit.
    /// Cut: drop the hold (due as soon as `stopped_since` is set); the 4.9 s case is red.
    /// Cut: drop the owns-none guard (`Some(reason)` without asking); the owning cases are red.
    #[test]
    fn own81_v10_restart_due_holds_five_seconds_and_needs_no_partition_owned() {
        let at = Instant::now();
        let since = Some(at);
        let owns_none = || true;
        let owns_some = || false;
        assert_eq!(restart_due(None, false, at, || panic!("not asked")), None);
        assert_eq!(
            restart_due(
                since,
                false,
                at + RESTART_HOLD - Duration::from_millis(100),
                || { panic!("not asked while the hold runs") }
            ),
            None,
            "held 4.9 s"
        );
        assert_eq!(
            restart_due(since, false, at + RESTART_HOLD, owns_none),
            Some(RestartReason::ReplaceStopped),
            "held 5 s, owns none"
        );
        assert_eq!(
            restart_due(since, false, at + RESTART_HOLD * 4, owns_some),
            None,
            "held 20 s, still owns a partition"
        );
    }

    /// V10b (#81, lead ruling): a faulted host is due at once, with no hold, and only when it
    /// owns no partition. Unit.
    /// Cut: drop the owns-none guard; the owning case is red.
    #[test]
    fn own81_v10b_a_faulted_host_owning_nothing_is_due_at_once() {
        let at = Instant::now();
        assert_eq!(
            restart_due(None, true, at, || true),
            Some(RestartReason::HostFaulted)
        );
        assert_eq!(restart_due(None, true, at, || false), None);
    }
}
