//! Bootstrap of partition 1 through F1 (M9 architecture §3, `rdb_api::admin`).
//!
//! A new partition has no prior owner, so nothing in the kernel starts its first recovery. The
//! admin plays the planner's part, once, the way the scenarios lowering does in the sim:
//!
//! 1. Create `partitions/1` in rEtcd: generation 0, epoch 0, owner node 1, `Serving`. Create-only,
//!    so a second bootstrap over the same control store is refused rather than overwritten.
//! 2. Hand node 1's F1 the `Plan`: rf3, node 1 primary, nodes 2 and 3 regular secondaries, the
//!    anchor at generation 0, sequence 0, `Digest::ROOT`.
//! 3. Hand it the `FenceProven` whose `DurableDrain` revision is the revision step 1 wrote.
//!
//! From there F1 queries the three empty copies, selects, commits generation 1 and the host
//! carries it to every member. The admin never injects `Recovered`, never writes a record into
//! the partition and never answers for a module (lead ruling: no workaround in `rdb_api`).

use std::sync::Arc;

use bytes::Bytes;
use config_core::{ConfigStore, MutationOutcome, PutRequest};
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
    /// `partitions/1` already exists in the control store.
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
/// [`BootstrapError`] when the record exists, the store fails, or node 1 is gone.
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
    let response = store
        .put(PutRequest {
            key: Bytes::from(key.clone()),
            value: record.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await
        .map_err(|error| BootstrapError::Control(error.to_string()))?;
    if response.outcome != MutationOutcome::Applied {
        return Err(BootstrapError::AlreadyExists {
            current: response.current_mod_revision,
        });
    }
    let revision = Revision(response.revision);
    tracing::info!(key = %key, revision = revision.0, "bootstrap_partition_record");

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
