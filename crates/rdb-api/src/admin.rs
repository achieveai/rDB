//! Bootstrap of partition 1 through F1 (M9 architecture §3, `rdb_api::admin`).
//!
//! A new partition has no prior owner, so nothing in the kernel starts its first recovery. The
//! admin plays the planner's part, once, the way the scenarios lowering does in the sim:
//!
//! 1. Create `partitions/1` in rEtcd: generation 0, epoch 0, owner node 1, `Serving`. Create-only,
//!    so a partition that already has a history is refused rather than overwritten. When the
//!    create is not reported applied (it conflicts, or its reply is lost), the admin reads the
//!    record back and adopts it only if it is exactly this record and nothing has written it since
//!    it was created (`create_revision == mod_revision`). Such a record means no recovery has
//!    committed, so there is no history to lose; a bootstrap that lost its reply can then be run
//!    again. If two bootstraps adopt the same record, F1's generation-1 commit is a CAS on its
//!    revision, so only one of them wins. Anything else is refused (F-004).
//! 2. Hand node 1's F1 the `Plan`: rf3, node 1 primary, nodes 2 and 3 regular secondaries, the
//!    anchor at generation 0, sequence 0, `Digest::ROOT`.
//! 3. Hand it the `FenceProven` whose `DurableDrain` revision is the revision step 1 wrote.
//!
//! From there F1 queries the three empty copies, selects, commits generation 1 and the host
//! carries it to every member. The admin never injects `Recovered`, never writes a record into
//! the partition and never answers for a module (lead ruling: no workaround in `rdb_api`).
//!
//! [`clear_grant`] (M9 S2b) is the planner's other duty here: it removes the `grants/{node}`
//! record a restarted node's old process left behind, through rdb-core's three guards.

use std::sync::Arc;

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, DeleteRequest, GetRequest, ListRequest, MutationOutcome, PutRequest,
};
use rdb_core::authority::grant::{clear_verdict, ClearRefusal, ClearVerdict, GrantRecord};
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::contracts::authority::{
    AuthorityView, DenyReason, FencingProof, Lineage, Revocation,
};
use rdb_core::contracts::control::{ControlKey, ControlPrefix};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::ErrorKind;
use rdb_core::contracts::event::{EventKind, KernelEvent};
use rdb_core::contracts::ids::{
    AuthorityGeneration, ConfigVersion, CorrelationId, Generation, GrantId, NodeId, OwnerEpoch,
    PartitionId, ReplicaRole, Revision, Seq,
};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::recovery::{Candidate, LineageAnchor, RecoveryEvent, RecoveryPlan};
use rdb_core::contracts::time::Tick;

use crate::clock::HostClock;
use crate::db::{ApiError, Db};
use crate::host::{Msg, NodeStopped, BOOT};

/// The partition S0 serves.
pub const PARTITION: PartitionId = PartitionId(1);

/// The configuration version every M9 member set is pinned at.
pub const CONFIG_VERSION: ConfigVersion = ConfigVersion(1);

/// The node that leads the bootstrap recovery and owns partition 1.
pub const OWNER: NodeId = NodeId(1);

/// How long a returning stale owner's suffix is kept: the lowering's value.
const RETENTION_MILLIS: u64 = 1_000;

/// The correlation the bootstrap's two events carry.
const BOOTSTRAP: CorrelationId = CorrelationId(1);

/// Why the bootstrap did not start.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// `partitions/1` already exists in the control store and is not one this bootstrap can
    /// adopt: it differs from this bootstrap's record, or it has been written since it was
    /// created.
    #[error("partitions/{} already exists at revision {current}: refusing to bootstrap twice", PARTITION.0)]
    AlreadyExists {
        /// Its revision.
        current: u64,
    },
    /// The control store refused or failed the write.
    #[error("control store: {0}")]
    Control(String),
    /// Node 1's thread is gone.
    #[error("node {} stopped before the bootstrap reached it", OWNER.0)]
    NodeStopped,
}

/// The rf3 member set: node 1 primary as copy 0, nodes 2 and 3 regular secondaries.
#[must_use]
pub fn members() -> Vec<Member> {
    (1..=3u8)
        .map(|n| Member {
            copy: CopyId(n - 1),
            node: NodeId(u32::from(n)),
            boot: BOOT,
            role: if n == 1 {
                ReplicaRole::Primary
            } else {
                ReplicaRole::RegularSecondary
            },
        })
        .collect()
}

/// Write `partitions/1`, then start F1 on node 1 through `send`.
///
/// # Errors
///
/// [`BootstrapError`] when a record this bootstrap cannot adopt exists, the store fails, or
/// node 1 is gone.
pub async fn bootstrap(
    store: &Arc<dyn ConfigStore>,
    now: Tick,
    send: impl Fn(Msg) -> Result<(), NodeStopped>,
) -> Result<Revision, BootstrapError> {
    let record = PartitionRecord {
        partition: PARTITION,
        owner: OWNER,
        generation: Generation(0),
        owner_epoch: OwnerEpoch(0),
        config_version: CONFIG_VERSION,
        lifecycle: PartitionLifecycle::Serving,
    };
    let key = ControlKey::Partition(PARTITION).encode();
    let value = record.encode();
    let created = store
        .put(PutRequest {
            key: Bytes::from(key.clone()),
            value: value.clone(),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await;
    let revision = match created {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            let revision = Revision(response.revision);
            tracing::info!(key = %key, revision = revision.0, "bootstrap_partition_record");
            revision
        }
        Ok(_) | Err(ConfigError::DeadlineExceededUnknownOutcome) => {
            adopt(store, &key, &value).await?
        }
        Err(error) => return Err(BootstrapError::Control(error.to_string())),
    };

    let lineage = Lineage {
        partition: PARTITION,
        generation: Generation(0),
        owner_epoch: OwnerEpoch(0),
    };
    let anchor = LineageAnchor {
        lineage,
        base_seq: Seq::ZERO,
        base_digest: Digest::ROOT,
    };
    let config = PartitionConfig::new(PARTITION, CONFIG_VERSION, members());
    let plan = RecoveryPlan {
        anchor,
        candidates: config
            .members
            .iter()
            .map(|member| Candidate {
                copy: member.copy,
                primary_eligible: true,
                healthy: true,
                within_capacity: true,
                has_valid_grant: true,
            })
            .collect(),
        rebuild_required: config.members.iter().map(|member| member.copy).collect(),
        authority_view: AuthorityView {
            lineage,
            grant_id: GrantId(1),
            boot_id: BOOT,
            authority_generation: AuthorityGeneration(1),
            config_version: CONFIG_VERSION,
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: RETENTION_MILLIS,
        config,
    };
    let fence = FencingProof {
        partition: PARTITION,
        prior_generation: Generation(0),
        prior_owner_epoch: OwnerEpoch(0),
        prior_grant_id: GrantId(1),
        prior_boot_id: BOOT,
        revocation: Revocation::DurableDrain {
            ack_revision: revision,
        },
        control_revision: revision,
        decision_tick: now,
    };
    for event in [
        RecoveryEvent::Plan(Box::new(plan)),
        RecoveryEvent::FenceProven(Box::new(fence)),
    ] {
        send(Msg::Inject {
            partition: PARTITION,
            correlation: BOOTSTRAP,
            kind: EventKind::Kernel(KernelEvent::Recovery(event)),
        })
        .map_err(|_| BootstrapError::NodeStopped)?;
    }
    tracing::info!(
        partition = PARTITION.0,
        owner = OWNER.0,
        "bootstrap_recovery_started"
    );
    Ok(revision)
}

/// Read `key` back after a create that was not reported applied, and return its revision when
/// it is `value` and has not been written since it was created (F-004).
async fn adopt(
    store: &Arc<dyn ConfigStore>,
    key: &str,
    value: &Bytes,
) -> Result<Revision, BootstrapError> {
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.to_owned()),
        })
        .await
        .map_err(|error| BootstrapError::Control(error.to_string()))?;
    let Some(found) = read.record else {
        return Err(BootstrapError::Control(format!(
            "{key} is absent after a create that was not reported applied"
        )));
    };
    if found.value != *value || found.create_revision != found.mod_revision {
        return Err(BootstrapError::AlreadyExists {
            current: found.mod_revision,
        });
    }
    tracing::info!(key = %key, revision = found.mod_revision, "bootstrap_partition_record_adopted");
    Ok(Revision(found.mod_revision))
}

/// What [`clear_grant`] did with `grants/{node}`. The simulator's service answers the same way
/// (`rdb_sim`'s `grant_service::Clearance`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clearance {
    /// `grants/{node}` holds nothing: there is nothing to clear.
    Absent,
    /// The record names the node's current boot: it is the running process's grant, however
    /// expired it reads. Nothing was written.
    Current,
    /// The record was deleted at this revision.
    Cleared(Revision),
    /// A guard held the record in place. Nothing was written.
    Refused {
        /// Which guard.
        refusal: ClearRefusal,
        /// `E + epsilon + delta` in UTC milliseconds: once control time is past it, guard 2
        /// holds. It moves if the old process still renews.
        until_utc_ms: i64,
    },
    /// All three guards held, but the record moved between the read and the delete. Nothing was
    /// deleted; run it again.
    Conflict,
}

/// Remove `grants/{node}` if a restarted node's old process left it and it may safely go (M9
/// S2b). The operator runs it, again until it answers [`Clearance::Cleared`] or
/// [`Clearance::Absent`]; a polling service is M10.
///
/// Reads the record, then the whole `partitions/` family, then asks
/// [`rdb_core::authority::grant::clear_verdict`] with the boot `db` runs `node` at now, never
/// the boot the record names, and the `Db`'s own clock. Only a [`ClearVerdict::Clear`] writes:
/// a delete at the record's own `mod_revision`, so a record renewed or replaced since the read
/// is left alone ([`Clearance::Conflict`]).
///
/// The `partitions/` list is read before the verdict because the verdict's read is synchronous
/// and `store` is not (critic A2). So with lists failing this is an error even where it would
/// answer [`Clearance::Current`].
///
/// Guard 3 and the delete are two calls: rEtcd has no compare across keys. That is safe in M9
/// because nothing can make guard 3 false between them. The only writers of `partitions/` are
/// [`bootstrap`] and F1's commit, and both write `Serving` (`rdb_dev`'s `control lifecycle`
/// runs on the same thread as its `admin clear-grant`). **M10 tripwire:** the first writer of a
/// non-`Serving` lifecycle, the planner, must also be the only clearer, or rEtcd must gain a
/// multi-key compare.
///
/// Logged as `grant_clear{node, boot, record_boot, revision, expiry, now, until, verdict}`, and a
/// failure as `grant_clear_failed{node, error}`.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for a node `db` does not run, or a `Db` built without budgets (a crate
/// test's). `UNAVAILABLE` when a read fails, the `partitions/` list comes back truncated, or a
/// record does not decode: nothing is deleted then. `UNKNOWN_OUTCOME` when the delete's reply is
/// lost; running it again reads where it stands.
pub async fn clear_grant(
    db: &Db,
    store: &dyn ConfigStore,
    node: NodeId,
) -> Result<Clearance, ApiError> {
    let answer = judge_and_clear(db, store, node).await;
    if let Err(error) = &answer {
        tracing::warn!(node = node.0, %error, "grant_clear_failed");
    }
    answer
}

async fn judge_and_clear(
    db: &Db,
    store: &dyn ConfigStore,
    node: NodeId,
) -> Result<Clearance, ApiError> {
    let boot = db.boot_of(node).ok_or_else(|| {
        ApiError::invalid(format!(
            "node {}: no such node; this Db runs nodes 1, 2 and 3",
            node.0
        ))
    })?;
    let budgets = db.budgets().ok_or_else(|| {
        ApiError::invalid("this Db was built without budgets, so it cannot judge a grant")
    })?;
    let key = ControlKey::Grant(node).encode();
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| unavailable(format!("get {key}: {error}")))?;
    let Some(found) = read.record else {
        tracing::info!(
            node = node.0,
            boot = boot.0,
            verdict = ?Clearance::Absent,
            "grant_clear"
        );
        return Ok(Clearance::Absent);
    };
    let record = GrantRecord::decode(&found.value).ok_or_else(|| {
        unavailable(format!(
            "{key} at revision {} is not a grant record",
            found.mod_revision
        ))
    })?;
    let partitions = list_partitions(store).await?;
    let now = db.now();
    let sample = HostClock::control_time(now, &budgets);
    let verdict = clear_verdict(node, &record, boot, sample, now, &budgets, || {
        Ok::<_, std::convert::Infallible>(partitions)
    })
    .unwrap_or_else(|never| match never {});
    let margin = budgets
        .clock_error_millis
        .saturating_add(budgets.dispatch_margin_millis);
    let until_utc_ms = record
        .expiry_utc_ms
        .saturating_add(i64::try_from(margin).unwrap_or(i64::MAX));
    let clearance = match verdict {
        ClearVerdict::Current => Clearance::Current,
        ClearVerdict::Refused(refusal) => Clearance::Refused {
            refusal,
            until_utc_ms,
        },
        ClearVerdict::Clear => delete(store, &key, found.mod_revision).await?,
    };
    tracing::info!(
        node = node.0,
        boot = boot.0,
        record_boot = record.boot.0,
        revision = found.mod_revision,
        expiry = record.expiry_utc_ms,
        now = now.0,
        until = until_utc_ms,
        verdict = ?clearance,
        "grant_clear"
    );
    Ok(clearance)
}

/// The whole `partitions/` family, or an error: a failed or truncated list is never read as a
/// complete one, since a missing record could hide a partition in transfer.
async fn list_partitions(store: &dyn ConfigStore) -> Result<Vec<PartitionRecord>, ApiError> {
    let prefix = ControlPrefix::Partitions.encode();
    let listed = store
        .list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .map_err(|error| unavailable(format!("list {prefix}: {error}")))?;
    if listed.truncated {
        return Err(unavailable(format!(
            "list {prefix} came back truncated at revision {}",
            listed.read_revision
        )));
    }
    listed
        .records
        .iter()
        .map(|found| {
            PartitionRecord::decode(&found.value).ok_or_else(|| {
                unavailable(format!(
                    "{} at revision {} is not a partition record",
                    String::from_utf8_lossy(&found.key),
                    found.mod_revision
                ))
            })
        })
        .collect()
}

/// Delete `key` only while it is still at `revision`.
async fn delete(store: &dyn ConfigStore, key: &str, revision: u64) -> Result<Clearance, ApiError> {
    let answer = store
        .delete(DeleteRequest {
            key: Bytes::from(key.to_owned()),
            expected_mod_revision: Some(revision),
            dedup: None,
        })
        .await;
    match answer {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            Ok(Clearance::Cleared(Revision(response.revision)))
        }
        Ok(_) | Err(ConfigError::Conflict { .. } | ConfigError::NotFound) => {
            Ok(Clearance::Conflict)
        }
        Err(ConfigError::DeadlineExceededUnknownOutcome) => Err(ApiError::new(
            ErrorKind::UnknownOutcome,
            format!("delete {key} at revision {revision}: the reply was lost"),
        )),
        Err(error) => Err(unavailable(format!(
            "delete {key} at revision {revision}: {error}"
        ))),
    }
}

fn unavailable(detail: String) -> ApiError {
    ApiError::new(
        ErrorKind::Unavailable,
        format!("control store: {detail}; nothing deleted"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_store::{Script, Scripted};

    fn record(generation: u64) -> PartitionRecord {
        PartitionRecord {
            partition: PARTITION,
            owner: OWNER,
            generation: Generation(generation),
            owner_epoch: OwnerEpoch(0),
            config_version: CONFIG_VERSION,
            lifecycle: PartitionLifecycle::Serving,
        }
    }

    fn put(store: &Arc<dyn ConfigStore>, rt: &tokio::runtime::Runtime, value: Bytes, cas: u64) {
        let response = rt
            .block_on(store.put(PutRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                value,
                expected_mod_revision: Some(cas),
                dedup: None,
            }))
            .expect("put");
        assert_eq!(response.outcome, MutationOutcome::Applied);
    }

    /// F-004 (b): only an untouched record of this bootstrap's own is adopted. A record that
    /// has been written since it was created, or one at a later generation (what F1's commit
    /// leaves), is a partition with a history: the bootstrap is refused with the record's
    /// revision and sends node 1 nothing, so it never starts a second generation-0 recovery.
    #[test]
    fn a_second_bootstrap_over_a_changed_record_is_refused_and_sends_nothing() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let sent = std::sync::Mutex::new(0u32);
        let send = |_: Msg| -> Result<(), NodeStopped> {
            *sent.lock().expect("count") += 1;
            Ok(())
        };

        // Rewritten with the same bytes: `mod_revision` moved past `create_revision`.
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let first = rt
            .block_on(bootstrap(&store, Tick(0), send))
            .expect("first bootstrap");
        assert_eq!(*sent.lock().expect("count"), 2, "Plan and FenceProven");
        put(&store, &rt, record(0).encode(), first.0);
        match rt.block_on(bootstrap(&store, Tick(0), send)) {
            Err(BootstrapError::AlreadyExists { current }) => assert!(current > first.0),
            other => panic!("over a rewritten record: {other:?}"),
        }
        assert_eq!(*sent.lock().expect("count"), 2, "the refusal sent nothing");

        // Created once, untouched since, but at generation 1.
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        put(&store, &rt, record(1).encode(), 0);
        match rt.block_on(bootstrap(&store, Tick(0), send)) {
            Err(BootstrapError::AlreadyExists { .. }) => {}
            other => panic!("over a generation-1 record: {other:?}"),
        }
        assert_eq!(*sent.lock().expect("count"), 2, "the refusal sent nothing");
    }

    /// F-004: the create applies but its reply is lost. The bootstrap reads the record back,
    /// finds its own, untouched, and carries on at that record's revision. When the read-back
    /// fails as well, the bootstrap fails with `Control` and sends nothing; the next open
    /// adopts the record (the `Db` row covers that retry). (c) When the create never applied,
    /// the read-back finds nothing: `Control` again, nothing sent, and the next open creates it.
    #[test]
    fn a_bootstrap_whose_create_reply_was_lost_adopts_the_record_it_wrote() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let sent = std::sync::Mutex::new(0u32);
        let send = |_: Msg| -> Result<(), NodeStopped> {
            *sent.lock().expect("count") += 1;
            Ok(())
        };

        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            lose_put_replies: 1,
            ..Script::default()
        }));
        let adopted = rt
            .block_on(bootstrap(&store, Tick(0), send))
            .expect("the lost create is adopted");
        let key = Bytes::from(ControlKey::Partition(PARTITION).encode());
        let found = rt
            .block_on(store.get(GetRequest { key }))
            .expect("get")
            .record
            .expect("partitions/1");
        assert_eq!(
            (adopted.0, found.value),
            (found.mod_revision, record(0).encode())
        );
        assert_eq!(*sent.lock().expect("count"), 2, "Plan and FenceProven");

        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            lose_put_replies: 1,
            fail_gets: 1,
            ..Script::default()
        }));
        match rt.block_on(bootstrap(&store, Tick(0), send)) {
            Err(BootstrapError::Control(_)) => {}
            other => panic!("the read-back failed: {other:?}"),
        }
        assert_eq!(*sent.lock().expect("count"), 2, "the failure sent nothing");

        // (c) The reply is lost and the create never applied: the read-back finds no record.
        // That is not a history to refuse, and not a record to adopt (critic C-1).
        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            drop_puts: 1,
            ..Script::default()
        }));
        match rt.block_on(bootstrap(&store, Tick(0), send)) {
            Err(BootstrapError::Control(_)) => {}
            other => panic!("(c) the create never applied: {other:?}"),
        }
        assert_eq!(
            *sent.lock().expect("count"),
            2,
            "(c) the failure sent nothing"
        );
    }
}
