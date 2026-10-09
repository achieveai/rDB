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

use std::sync::Arc;

use bytes::Bytes;
use config_core::{ConfigError, ConfigStore, GetRequest, MutationOutcome, PutRequest};
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::contracts::authority::{
    AuthorityView, DenyReason, FencingProof, Lineage, Revocation,
};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{EventKind, KernelEvent};
use rdb_core::contracts::ids::{
    AuthorityGeneration, ConfigVersion, CorrelationId, Generation, GrantId, NodeId, OwnerEpoch,
    PartitionId, ReplicaRole, Revision, Seq,
};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::recovery::{Candidate, LineageAnchor, RecoveryEvent, RecoveryPlan};
use rdb_core::contracts::time::Tick;

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
