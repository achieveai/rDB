//! #81 V10: when a node process should restart, the rule [`super::Db::restart_wanted`] polls
//! and `rdb_node` exits 75 on.

use std::sync::Arc;
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

/// [`owns_no_partition`] run on `rt` and waited for, from this blocking thread, for at most
/// `within`: `true` only when the read answered that no `partitions/` record names one of
/// `nodes` owner. Every other outcome is `false` and logged `db_restart_check_unread`: a list
/// that fails, comes back truncated, or holds a record that does not decode, and a read that
/// does not answer within `within`. A read that does not answer is aborted, so a store that
/// never answers leaves no task behind, poll after poll (PR #171 review).
pub(crate) fn owns_none_within(
    rt: &tokio::runtime::Handle,
    store: Arc<dyn ConfigStore>,
    nodes: Vec<NodeId>,
    within: Duration,
) -> bool {
    let (done, answer) = std::sync::mpsc::channel();
    let read = rt.spawn(async move {
        let _ = done.send(owns_no_partition(&*store, &nodes).await);
    });
    match answer.recv_timeout(within) {
        Ok(Ok(none)) => none,
        Ok(Err(error)) => {
            tracing::warn!(%error, "db_restart_check_unread");
            false
        }
        Err(_) => {
            read.abort();
            tracing::warn!(
                error = "partitions/ did not answer within the read timeout",
                within_ms = within.as_millis(),
                "db_restart_check_unread"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{owns_none_within, restart_due, RestartReason, RESTART_HOLD};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use config_core::ConfigStore;
    use rdb_core::contracts::ids::NodeId;

    /// How long the rows give a read that never answers.
    const WITHIN: Duration = Duration::from_millis(50);

    /// The `db_restart_check_unread` lines the current row logged.
    fn unread(method: &str) -> Vec<serde_json::Value> {
        config_testkit::logs::lines_for_current_test(module_path!(), method)
            .into_iter()
            .filter(|line| line["@m"] == "db_restart_check_unread")
            .collect()
    }

    /// PR #171 review (item 7): a control store whose `partitions/` list never answers. Each of
    /// three polls gives up at the read timeout, says `db_restart_check_unread`, answers "owns
    /// some", and aborts its read, so all three pending reads are dropped: no task is left per
    /// poll. Unit (in-memory store, no sockets).
    /// Cut: no `abort` after the timeout; no read is dropped, red at "every pending read is
    /// dropped".
    #[config_log::retcd_test]
    fn own81_v10_a_restart_check_that_never_answers_leaves_no_task() {
        const METHOD: &str = "own81_v10_a_restart_check_that_never_answers_leaves_no_task";
        let rt = crate::db::reopen_tests::runtime();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let store: Arc<dyn ConfigStore> = crate::test_store::ListFault::new(
            Arc::new(config_testkit::MemStore::new()),
            crate::test_store::ListFaultMode::Never(Arc::clone(&drops)),
        );
        for _ in 0..3 {
            assert!(!owns_none_within(
                rt.handle(),
                Arc::clone(&store),
                vec![NodeId(1)],
                WITHIN
            ));
        }
        assert_eq!(unread(METHOD).len(), 3);
        let deadline = Instant::now() + Duration::from_secs(5);
        while drops.load(std::sync::atomic::Ordering::SeqCst) < 3 {
            assert!(
                Instant::now() < deadline,
                "every pending read is dropped: {} of 3",
                drops.load(std::sync::atomic::Ordering::SeqCst)
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// `restart_wanted`'s last step for a faulted host, which is due at once whatever the hold:
    /// `restart_due` with [`owns_none_within`] over `store` for node 1, as `Db::restart_wanted`
    /// runs it with `Db::owns_no_partition`.
    fn faulted_host_restart(store: Arc<dyn ConfigStore>) -> Option<RestartReason> {
        let rt = crate::db::reopen_tests::runtime();
        restart_due(None, true, Instant::now(), || {
            owns_none_within(rt.handle(), store, vec![NodeId(1)], WITHIN)
        })
    }

    /// An in-memory store holding `partitions/{id}` as `body`.
    fn holding(id: u32, body: &[u8]) -> Arc<dyn ConfigStore> {
        let store = config_testkit::MemStore::new();
        crate::db::reopen_tests::runtime()
            .block_on(
                store.put(config_core::PutRequest {
                    key: bytes::Bytes::from(
                        rdb_core::contracts::control::ControlKey::Partition(
                            rdb_core::contracts::ids::PartitionId(id),
                        )
                        .encode(),
                    ),
                    value: bytes::Bytes::copy_from_slice(body),
                    expected_mod_revision: None,
                    dedup: None,
                }),
            )
            .expect("the put lands");
        Arc::new(store)
    }

    /// `partitions/{id}` naming `owner`.
    fn owned_by(id: u32, owner: u32) -> Arc<dyn ConfigStore> {
        let record = rdb_core::authority::partition::PartitionRecord {
            partition: rdb_core::contracts::ids::PartitionId(id),
            owner: NodeId(owner),
            ..rdb_core::authority::partition::PartitionRecord::default()
        };
        holding(id, &record.encode())
    }

    /// PR #171 review (item 4), fail closed: the restart check's `partitions/` list fails. The
    /// check reads "owns some", so a faulted host is not restarted (`None`), and the failure is
    /// said (`db_restart_check_unread`). Unit.
    /// Cut: the error arm of `owns_none_within` answers "owns none"; `Some`, red.
    #[config_log::retcd_test]
    fn own81_v10_a_failed_restart_check_keeps_the_node_up() {
        const METHOD: &str = "own81_v10_a_failed_restart_check_keeps_the_node_up";
        let store = crate::test_store::PrefixCut::new(
            Arc::new(config_testkit::MemStore::new()),
            rdb_core::contracts::control::ControlPrefix::Partitions.encode(),
            crate::test_store::CutCalls::Lists,
        );
        assert_eq!(faulted_host_restart(store), None);
        let said = unread(METHOD);
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0]["error"]
                .as_str()
                .is_some_and(|error| error.contains("list partitions/")),
            "{said:?}"
        );
    }

    /// PR #171 review (item 4), fail closed: the list does not answer within the read
    /// timeout. `None`, and said. Unit.
    /// Cut: the timeout arm answers "owns none"; `Some`, red.
    #[config_log::retcd_test]
    fn own81_v10_a_restart_check_that_times_out_keeps_the_node_up() {
        const METHOD: &str = "own81_v10_a_restart_check_that_times_out_keeps_the_node_up";
        let store = crate::test_store::ListFault::new(
            Arc::new(config_testkit::MemStore::new()),
            crate::test_store::ListFaultMode::Never(Arc::new(std::sync::atomic::AtomicUsize::new(
                0,
            ))),
        );
        assert_eq!(faulted_host_restart(store), None);
        assert_eq!(unread(METHOD).len(), 1);
    }

    /// PR #171 review (item 4), fail closed: the list comes back truncated, so it cannot say
    /// that nothing names the node. `None`, and said. Unit.
    /// Cut: the error arm answers "owns none"; `Some`, red.
    #[config_log::retcd_test]
    fn own81_v10_a_truncated_restart_check_keeps_the_node_up() {
        const METHOD: &str = "own81_v10_a_truncated_restart_check_keeps_the_node_up";
        let store = crate::test_store::ListFault::new(
            owned_by(3, 2),
            crate::test_store::ListFaultMode::Truncate,
        );
        assert_eq!(faulted_host_restart(store), None);
        let said = unread(METHOD);
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0]["error"]
                .as_str()
                .is_some_and(|error| error.contains("truncated")),
            "{said:?}"
        );
    }

    /// PR #171 review (item 4), fail closed: `partitions/5` holds bytes that are no partition
    /// record, so the read cannot say who owns it. `None`, and said. Unit.
    /// Cut: the error arm answers "owns none"; `Some`, red.
    #[config_log::retcd_test]
    fn own81_v10_an_undecodable_restart_check_keeps_the_node_up() {
        const METHOD: &str = "own81_v10_an_undecodable_restart_check_keeps_the_node_up";
        assert_eq!(faulted_host_restart(holding(5, b"not a record")), None);
        let said = unread(METHOD);
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0]["error"]
                .as_str()
                .is_some_and(|error| error.contains("is not a partition record")),
            "{said:?}"
        );
    }

    /// PR #171 review (item 4), owner match: `partitions/3` names node 1 owner, so node 1's
    /// faulted host is not restarted (`None`) and nothing is said: the read answered. The same
    /// record naming node 2 lets node 1 restart (`Some(HostFaulted)`), so the `None` is the
    /// owner match and not a failed read. Unit.
    /// Cut: `owns_no_partition` never matches an owner (always `Ok(true)`); the first is
    /// `Some`, red.
    #[config_log::retcd_test]
    fn own81_v10_a_partition_naming_the_node_keeps_restart_wanted_none() {
        const METHOD: &str = "own81_v10_a_partition_naming_the_node_keeps_restart_wanted_none";
        assert_eq!(faulted_host_restart(owned_by(3, 1)), None);
        assert_eq!(
            faulted_host_restart(owned_by(3, 2)),
            Some(RestartReason::HostFaulted)
        );
        assert!(unread(METHOD).is_empty(), "both reads answered");
    }

    /// V10 (#81) `restart_due`: a stopped self-replace is due only once held for
    /// `RESTART_HOLD`, and only when the node owns no partition; the ownership read is not
    /// made while nothing else is due. Unit.
    /// Cut: drop the hold (due as soon as `stopped_since` is set); the 4.9 s case is red.
    /// Cut: drop the owns-none guard (`Some(reason)` without asking); the owning cases are red.
    #[config_log::retcd_test]
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
    #[config_log::retcd_test]
    fn own81_v10b_a_faulted_host_owning_nothing_is_due_at_once() {
        let at = Instant::now();
        assert_eq!(
            restart_due(None, true, at, || true),
            Some(RestartReason::HostFaulted)
        );
        assert_eq!(restart_due(None, true, at, || false), None);
    }
}
