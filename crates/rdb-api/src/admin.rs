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
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, DeleteRequest, GetRequest, ListRequest, MutationOutcome, PutRequest,
};
use rdb_core::authority::grant::{clear_verdict, ClearRefusal, ClearVerdict, GrantRecord};
use rdb_core::authority::partition::{
    PartitionLifecycle, PartitionRecord, PartitionRoot, INSTANCE_LEN,
};
use rdb_core::contracts::authority::{
    AuthorityView, DenyReason, FencingProof, Lineage, Revocation,
};
use rdb_core::contracts::control::{ControlKey, ControlPrefix};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::{ErrorKind, RetryRule};
use rdb_core::contracts::event::{Budgets, EventKind, KernelEvent};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, CorrelationId, Generation, GrantId, NodeId,
    OwnerEpoch, PartitionId, ReplicaRole, Revision, Seq,
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

/// The rf3 member set of [`members`], every node at `boot`: the boot one open starts all three
/// nodes at (M10, arch R3b).
#[must_use]
pub fn members_at(boot: BootId) -> Vec<Member> {
    members()
        .into_iter()
        .map(|member| Member { boot, ..member })
        .collect()
}

/// A recovery plan for partition 1 from `anchor`: rf3 at `version`, every node at `boot`, every
/// copy a candidate and in `rebuild_required`, node 1 primary with `grant` at `generation`.
fn recovery_plan(
    anchor: LineageAnchor,
    version: ConfigVersion,
    boot: BootId,
    grant: GrantId,
    generation: AuthorityGeneration,
) -> RecoveryPlan {
    let config = PartitionConfig::new(PARTITION, version, members_at(boot));
    RecoveryPlan {
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
            lineage: anchor.lineage,
            grant_id: grant,
            boot_id: boot,
            authority_generation: generation,
            config_version: version,
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: RETENTION_MILLIS,
        config,
    }
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
    bootstrap_at(store, now, BOOT, send).await
}

/// [`bootstrap`] with every node at `boot`, the boot this open started them at (M10, arch R3b).
///
/// # Errors
///
/// As [`bootstrap`].
pub async fn bootstrap_at(
    store: &Arc<dyn ConfigStore>,
    now: Tick,
    boot: BootId,
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
    let plan = recovery_plan(
        anchor,
        CONFIG_VERSION,
        boot,
        GrantId(1),
        AuthorityGeneration(1),
    );
    let fence = FencingProof {
        partition: PARTITION,
        prior_generation: Generation(0),
        prior_owner_epoch: OwnerEpoch(0),
        prior_grant_id: GrantId(1),
        prior_boot_id: boot,
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

/// The most opens one data directory may have (arch R3b): `N <= 2^24`, so a boot and a
/// request id derived from N never wrap.
pub const MAX_OPENS: u64 = 1 << 24;

/// The version byte of an open counter's value. Layout 2 (ruling A-R184): N, then the newest
/// generation of partition 1 a host marked activated, each a little-endian u64. A build from
/// before A-R184 reads layout 2 as `Unreadable`, never as a count.
const COUNTER_LAYOUT: u8 = 2;

/// Layout 1, written before A-R184: N alone. Read with the activated generation unknown.
const COUNTER_LAYOUT_N_ONLY: u8 = 1;

/// The activated generation a layout-1 counter leaves unknown: no generation is ever rolled
/// forward on it, and the next activation replaces it.
pub const ACTIVATED_UNKNOWN: Generation = Generation(u64::MAX);

/// An open counter's value (arch R3b, rulings A-R160 and A-R184).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count {
    /// How many opens were counted: N.
    pub n: u64,
    /// The newest generation of partition 1 a host marked activated before its first acquire
    /// under it ([`activate`]): `Generation(0)` before any, [`ACTIVATED_UNKNOWN`] on a layout-1
    /// value.
    pub activated: Generation,
}

impl Count {
    /// Whether `generation` may have admitted a write: always, unless the marker proves that no
    /// host ever activated it.
    #[must_use]
    pub fn may_have_admitted(self, generation: Generation) -> bool {
        self.activated == ACTIVATED_UNKNOWN || self.activated >= generation
    }
}

/// How long [`count_open`] keeps reading and CASing before it gives up.
pub(crate) const COUNT_WITHIN: Duration = Duration::from_secs(10);

/// An instance id as 32 lowercase hex digits: how logs and keys name it.
#[must_use]
pub fn instance_hex(instance: &[u8; INSTANCE_LEN]) -> String {
    instance.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The key of `instance`'s open counter (M10, arch R3b, ruling A-R160).
#[must_use]
pub fn instance_key(instance: &[u8; INSTANCE_LEN]) -> String {
    format!("rdb/instances/{}", instance_hex(instance))
}

fn encode_count(count: Count) -> Bytes {
    let mut out = Vec::with_capacity(17);
    out.push(COUNTER_LAYOUT);
    out.extend_from_slice(&count.n.to_le_bytes());
    out.extend_from_slice(&count.activated.0.to_le_bytes());
    Bytes::from(out)
}

fn decode_count(body: &[u8]) -> Option<Count> {
    match body {
        [COUNTER_LAYOUT, rest @ ..] if rest.len() == 16 => Some(Count {
            n: u64::from_le_bytes(rest[..8].try_into().ok()?),
            activated: Generation(u64::from_le_bytes(rest[8..].try_into().ok()?)),
        }),
        [COUNTER_LAYOUT_N_ONLY, rest @ ..] => Some(Count {
            n: u64::from_le_bytes(rest.try_into().ok()?),
            activated: ACTIVATED_UNKNOWN,
        }),
        _ => None,
    }
}

/// Why [`count_open`] did not count an open.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CounterError {
    /// The copies name an instance whose counter the store does not hold: the store is not the
    /// one this directory was opened against.
    #[error("{key} is absent: this directory was not opened against this control store")]
    Absent {
        /// The counter's key.
        key: String,
    },
    /// The directory has been opened [`MAX_OPENS`] times.
    #[error("{key} has counted {MAX_OPENS} opens, the most one directory may have")]
    Exhausted {
        /// The counter's key.
        key: String,
    },
    /// The counter's value is not one this build writes.
    #[error("{key} at revision {revision} is not an open counter")]
    Unreadable {
        /// The counter's key.
        key: String,
        /// Where it was read.
        revision: u64,
    },
    /// A roll-forward's count found the counter moved from the revision its decision read
    /// ([`count_open_at`]): another open counted in between, so the decision no longer holds.
    #[error("{key} moved from revision {read} that the roll-forward decision read")]
    Moved {
        /// The counter's key.
        key: String,
        /// The revision the decision read.
        read: u64,
    },
    /// The store refused, or did not settle the count within [`COUNT_WITHIN`].
    #[error("control store: {0}")]
    Control(String),
}

/// Count one more open of `instance` and return its number N (arch R3b): N = 1 by a create-only
/// write when `fresh` finds no counter, otherwise N + 1 by a CAS at the revision just read. A
/// conflict or a lost reply reads again and CASes again, so a lost reply can waste a number but
/// never hand one out twice. A transient store error waits 50 ms and tries again; all of it is
/// bounded by `within`. Logged as `open_counted{instance_hex, n, revision}` and each retry as
/// `open_count_retry{instance_hex, error}`.
///
/// # Errors
///
/// [`CounterError`].
pub async fn count_open(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    fresh: bool,
    within: Duration,
) -> Result<u64, CounterError> {
    let key = instance_key(instance);
    let hex = instance_hex(instance);
    let deadline = tokio::time::Instant::now() + within;
    let mut last = String::from("nothing answered");
    while tokio::time::Instant::now() < deadline {
        let read = match store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Ok(read) => read,
            Err(error) if crate::control::transient(&error) => {
                last = error.to_string();
                tracing::warn!(instance_hex = %hex, error = %error, "open_count_retry");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(error) => return Err(CounterError::Control(format!("get {key}: {error}"))),
        };
        let (next, expected) = match read.record {
            None if fresh => (
                Count {
                    n: 1,
                    activated: Generation(0),
                },
                0,
            ),
            None => return Err(CounterError::Absent { key }),
            Some(found) => {
                let count = decode_count(&found.value).ok_or_else(|| CounterError::Unreadable {
                    key: key.clone(),
                    revision: found.mod_revision,
                })?;
                if count.n >= MAX_OPENS {
                    return Err(CounterError::Exhausted { key });
                }
                (
                    Count {
                        n: count.n + 1,
                        ..count
                    },
                    found.mod_revision,
                )
            }
        };
        let written = store
            .put(PutRequest {
                key: Bytes::from(key.clone()),
                value: encode_count(next),
                expected_mod_revision: Some(expected),
                dedup: None,
            })
            .await;
        match written {
            Ok(response) if response.outcome == MutationOutcome::Applied => {
                tracing::info!(
                    instance_hex = %hex,
                    n = next.n,
                    activated = next.activated.0,
                    revision = response.revision,
                    "open_counted"
                );
                return Ok(next.n);
            }
            Ok(_) => last = format!("CAS at {expected} was not applied"),
            Err(error)
                if matches!(
                    error,
                    ConfigError::Conflict { .. } | ConfigError::DeadlineExceededUnknownOutcome
                ) || crate::control::transient(&error) =>
            {
                last = error.to_string();
            }
            Err(error) => return Err(CounterError::Control(format!("put {key}: {error}"))),
        }
        tracing::warn!(instance_hex = %hex, error = %last, "open_count_retry");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(CounterError::Control(format!(
        "{key} was not counted within {} ms; last: {last}",
        within.as_millis()
    )))
}

/// `instance`'s open counter and the revision it was read at, for a reopen's roll-forward
/// decision (ruling A-R184). One read, no retry, as the reopen's read of `partitions/1`.
///
/// # Errors
///
/// [`CounterError::Absent`], [`CounterError::Unreadable`], or [`CounterError::Control`] when the
/// read fails.
pub async fn read_count(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
) -> Result<(Count, u64), CounterError> {
    let key = instance_key(instance);
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| CounterError::Control(format!("get {key}: {error}")))?;
    let found = read
        .record
        .ok_or_else(|| CounterError::Absent { key: key.clone() })?;
    let count = decode_count(&found.value).ok_or(CounterError::Unreadable {
        key,
        revision: found.mod_revision,
    })?;
    Ok((count, found.mod_revision))
}

/// Count one more open of `instance` on a roll-forward (ruling A-R184, k199): a CAS from `read`
/// at exactly the `revision` the decision read it at, never from a newer value. A conflict means
/// another open counted since, so the decision no longer holds: [`CounterError::Moved`]. A lost
/// reply reads again: the counter still at `revision` sends the same CAS again, this CAS's own
/// bytes there count as applied, and anything else is [`CounterError::Moved`]. Bounded by
/// `within`. Logged as `open_counted{instance_hex, n, activated, revision, rolled_forward}`.
///
/// # Errors
///
/// [`CounterError::Moved`], [`CounterError::Exhausted`], or [`CounterError::Control`] when the
/// store refuses or does not settle within `within`.
pub async fn count_open_at(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    read: Count,
    revision: u64,
    within: Duration,
) -> Result<u64, CounterError> {
    let key = instance_key(instance);
    let hex = instance_hex(instance);
    if read.n >= MAX_OPENS {
        return Err(CounterError::Exhausted { key });
    }
    let next = Count {
        n: read.n + 1,
        ..read
    };
    let counted = |at: u64| {
        tracing::info!(
            instance_hex = %hex,
            n = next.n,
            activated = next.activated.0,
            revision = at,
            rolled_forward = true,
            "open_counted"
        );
        Ok(next.n)
    };
    let deadline = tokio::time::Instant::now() + within;
    let mut last = String::from("nothing answered");
    let mut send = true;
    while tokio::time::Instant::now() < deadline {
        if send {
            let written = store
                .put(PutRequest {
                    key: Bytes::from(key.clone()),
                    value: encode_count(next),
                    expected_mod_revision: Some(revision),
                    dedup: None,
                })
                .await;
            match written {
                Ok(response) if response.outcome == MutationOutcome::Applied => {
                    return counted(response.revision)
                }
                Ok(_) | Err(ConfigError::Conflict { .. }) => {
                    return Err(CounterError::Moved {
                        key,
                        read: revision,
                    })
                }
                Err(error)
                    if matches!(error, ConfigError::DeadlineExceededUnknownOutcome)
                        || crate::control::transient(&error) =>
                {
                    last = error.to_string();
                }
                Err(error) => return Err(CounterError::Control(format!("put {key}: {error}"))),
            }
        }
        tracing::warn!(instance_hex = %hex, error = %last, "open_count_retry");
        tokio::time::sleep(Duration::from_millis(50)).await;
        match store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Ok(read) => match read.record {
                Some(found) if found.mod_revision == revision => send = true,
                Some(found) if decode_count(&found.value) == Some(next) => {
                    return counted(found.mod_revision)
                }
                _ => {
                    return Err(CounterError::Moved {
                        key,
                        read: revision,
                    })
                }
            },
            Err(error) if crate::control::transient(&error) => {
                last = error.to_string();
                send = false;
            }
            Err(error) => return Err(CounterError::Control(format!("get {key}: {error}"))),
        }
    }
    Err(CounterError::Control(format!(
        "{key} was not counted within {} ms; last: {last}",
        within.as_millis()
    )))
}

/// Mark `generation` of partition 1 activated in `instance`'s open counter (ruling A-R184,
/// critic-rollfwd F2). A host calls it before its first acquire under `generation` and acquires
/// only once it returns, so a generation the counter does not name never admitted a write. It
/// reads the counter and CASes `activated = generation`, keeping N, at the revision read. A
/// conflict, a lost reply or a store error reads again, so it returns only once the counter names
/// `generation` or newer, and it never gives up on its own: the node's acquire waits on it, and
/// a node that stops abandons it. Logged as `open_activated{instance_hex, n, generation,
/// revision}`, each retry as `open_activate_retry{instance_hex, generation, error}`.
///
/// # Errors
///
/// [`CounterError::Absent`] or [`CounterError::Unreadable`]: no read again changes them.
pub async fn activate(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    generation: Generation,
) -> Result<Count, CounterError> {
    let key = instance_key(instance);
    let hex = instance_hex(instance);
    let activated = |count: Count, revision: u64| {
        tracing::info!(
            instance_hex = %hex,
            n = count.n,
            generation = generation.0,
            activated = count.activated.0,
            revision,
            "open_activated"
        );
        Ok(count)
    };
    let mut wait = Duration::from_millis(50);
    loop {
        let error = match store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Ok(read) => {
                let Some(found) = read.record else {
                    return Err(CounterError::Absent { key });
                };
                let count = decode_count(&found.value).ok_or_else(|| CounterError::Unreadable {
                    key: key.clone(),
                    revision: found.mod_revision,
                })?;
                if count.activated != ACTIVATED_UNKNOWN && count.activated >= generation {
                    return activated(count, found.mod_revision);
                }
                let next = Count {
                    activated: generation,
                    ..count
                };
                let written = store
                    .put(PutRequest {
                        key: Bytes::from(key.clone()),
                        value: encode_count(next),
                        expected_mod_revision: Some(found.mod_revision),
                        dedup: None,
                    })
                    .await;
                match written {
                    Ok(response) if response.outcome == MutationOutcome::Applied => {
                        return activated(next, response.revision)
                    }
                    Ok(_) => format!("CAS at {} was not applied", found.mod_revision),
                    Err(error) => error.to_string(),
                }
            }
            Err(error) => error.to_string(),
        };
        tracing::warn!(
            instance_hex = %hex,
            generation = generation.0,
            error = %error,
            "open_activate_retry"
        );
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(1));
    }
}

/// What a reopen's heal proved and handed node 1 (M10, arch R3a, ruling A-R163).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Healed {
    /// How node 1's old grant was found gone: deleted by this heal (case A), or absent (case B).
    pub cleared: GrantCleared,
    /// The store revision `partitions/1` was then read at: never below [`Self::cleared`]'s.
    pub read_revision: Revision,
    /// The fence's `control_revision`: `partitions/1`'s `mod_revision` from that read.
    pub control_revision: Revision,
}

/// How a reopen's heal found node 1's old grant gone (M10, arch R3a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantCleared {
    /// Case A: the heal deleted it, at this revision ([`Revocation::ExpiryProven`]).
    Deleted(Revision),
    /// Case B: a read at this store revision found it absent ([`Revocation::NoGrantHeld`]).
    Absent(Revision),
}

impl GrantCleared {
    /// The revision `partitions/1` must be read at or after.
    #[must_use]
    pub const fn revision(self) -> Revision {
        match self {
            Self::Deleted(revision) | Self::Absent(revision) => revision,
        }
    }
}

/// Why a reopen's heal did not hand node 1 a fence.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HealError {
    /// A grant was still not clearable when the heal's time ran out.
    #[error(
        "the last run's grant for node {node} (grants/{node}) was still held after {secs} \
         seconds of trying: {last}",
        secs = seconds_in_words(*.within_ms)
    )]
    NotCleared {
        /// The node.
        node: u32,
        /// The heal's bound.
        within_ms: u128,
        /// Why the last answer kept the grant, and the next step, in words (A-R191b).
        last: String,
    },
    /// The `partitions/1` read was served before the grant delete it must follow.
    #[error("partitions/1 was read at revision {read}, before the grant delete at {deleted}")]
    ReadBeforeDelete {
        /// The read's store revision.
        read: u64,
        /// The delete's revision.
        deleted: u64,
    },
    /// The `partitions/1` read was served before the read that found `grants/1` absent.
    #[error(
        "partitions/1 was read at revision {read}, before grants/1 was read absent at {absent}"
    )]
    ReadBeforeAbsence {
        /// The read's store revision.
        read: u64,
        /// The absent read's store revision.
        absent: u64,
    },
    /// `partitions/1` is not a layout-3 record.
    #[error("partitions/1: {0}")]
    Record(String),
    /// The store refused.
    #[error("control store: {0}")]
    Control(String),
    /// Node 1's thread is gone.
    #[error("node {} stopped before the heal reached it", OWNER.0)]
    NodeStopped,
}

/// What a reopen's heal runs with: it runs as a task, after `Db::open` has returned.
pub(crate) struct HealCtx {
    pub(crate) store: Arc<dyn ConfigStore>,
    /// The boot this open started every node at.
    pub(crate) boot: BootId,
    pub(crate) budgets: Budgets,
    pub(crate) clock: HostClock,
    /// Node 1's mailbox.
    pub(crate) owner: std::sync::mpsc::Sender<Msg>,
    /// How long clearing the old grants may take.
    pub(crate) within: Duration,
}

/// The correlation a heal's two events carry.
const HEAL: CorrelationId = CorrelationId(2);

/// Heal partition 1 after a reopen (M10, arch R3a, rulings A-R159, A-R163): clear the three old
/// grants, node 1's first; only after node 1's grant is known gone, read `partitions/1`; then
/// hand node 1's F1 a `Plan` from the record's root at config version + 1 with this open's
/// boots, and a takeover authorization whose `control_revision` is the record's `mod_revision`.
/// The same proof `bootstrap` and the simulator's `reinspect_now` already use: a CAS at that
/// revision lands only if nothing wrote `partitions/1` after the read.
///
/// Node 1's grant is gone one of two ways:
/// - **Case A**, deleted by this heal under `clear_verdict`'s guards:
///   [`Revocation::ExpiryProven`] from the record it judged, `prior_grant_id` that record's
///   grant. A record holding `GrantId(0)` is refused before the delete: A1 never issues 0.
/// - **Case B**, read absent (an earlier heal deleted it and its process died before an
///   acquire): [`Revocation::NoGrantHeld`] at that read's revision, `prior_grant_id`
///   `GrantId(0)` ("none held").
///
/// Either way `prior_boot_id` is the record's `owner_boot`, the owner the F1 commit pinned,
/// never 0 (R3a; R5 row 8).
///
/// Logged as `heal_takeover_authorized{variant, control_revision, prior_boot, prior_grant,
/// grant_delete_revision | absent_revision, read_revision}`.
///
/// # Errors
///
/// [`HealError`]. Nothing is sent to node 1 unless every step held.
pub(crate) async fn heal(ctx: HealCtx) -> Result<Healed, HealError> {
    let deadline = tokio::time::Instant::now() + ctx.within;
    let gone = clear_until(&ctx, OWNER, deadline).await?;
    for node in [NodeId(2), NodeId(3)] {
        clear_until(&ctx, node, deadline).await?;
    }
    // Only now, after node 1's delete was acknowledged or its absence read (A-R163).
    let read = ctx
        .store
        .get(GetRequest {
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
        })
        .await
        .map_err(|error| HealError::Control(format!("get partitions/1: {error}")))?;
    let cleared = match &gone {
        Gone::Deleted(revision, _) => GrantCleared::Deleted(*revision),
        Gone::Absent(revision) => GrantCleared::Absent(*revision),
    };
    if Revision(read.read_revision) < cleared.revision() {
        return Err(match cleared {
            GrantCleared::Deleted(deleted) => HealError::ReadBeforeDelete {
                read: read.read_revision,
                deleted: deleted.0,
            },
            GrantCleared::Absent(absent) => HealError::ReadBeforeAbsence {
                read: read.read_revision,
                absent: absent.0,
            },
        });
    }
    let found = read
        .record
        .ok_or_else(|| HealError::Record("absent".to_owned()))?;
    let (Some(record), Some(root)) = (
        PartitionRecord::decode(&found.value),
        PartitionRoot::decode(&found.value),
    ) else {
        return Err(HealError::Record(format!(
            "the record at revision {} carries no root (layout 3)",
            found.mod_revision
        )));
    };
    let healed = Healed {
        cleared,
        read_revision: Revision(read.read_revision),
        control_revision: Revision(found.mod_revision),
    };
    // R3a, both cases: the prior boot is the owner the F1 commit pinned (R5 row 8: never 0).
    if root.owner_boot == BootId(0) {
        return Err(HealError::Record(format!(
            "the record at revision {} names owner boot 0",
            found.mod_revision
        )));
    }
    let prior_boot = root.owner_boot;
    let now = ctx.clock.now();
    // The plan's view: case A carries the old grant, as before; case B has none, so it takes
    // bootstrap's. A1 replaces the view at its first acquire either way.
    let (prior_grant, view_grant, view_generation, revocation) = match &gone {
        Gone::Deleted(_, held) => {
            let sample = HostClock::control_time(now, &ctx.budgets);
            let revocation = Revocation::ExpiryProven {
                frozen_expiry_utc_ms: held.expiry_utc_ms,
                authority_utc_ms: i64::try_from(sample.estimate.0).unwrap_or(i64::MAX),
                authority_tick: now,
                epsilon_ms: u32::try_from(ctx.budgets.clock_error_millis).unwrap_or(u32::MAX),
                delta_ms: u32::try_from(ctx.budgets.dispatch_margin_millis).unwrap_or(u32::MAX),
            };
            let held = *held;
            (
                held.grant,
                held.grant,
                held.authority_generation,
                revocation,
            )
        }
        Gone::Absent(absent_revision) => {
            let revocation = Revocation::NoGrantHeld {
                absent_revision: *absent_revision,
            };
            (GrantId(0), GrantId(1), AuthorityGeneration(1), revocation)
        }
    };
    let lineage = Lineage {
        partition: PARTITION,
        generation: record.generation,
        owner_epoch: record.owner_epoch,
    };
    let anchor = LineageAnchor {
        lineage,
        base_seq: root.base_seq,
        base_digest: root.base_digest,
    };
    let version = ConfigVersion(record.config_version.0 + 1);
    let plan = recovery_plan(anchor, version, ctx.boot, view_grant, view_generation);
    let fence = FencingProof {
        partition: PARTITION,
        prior_generation: record.generation,
        prior_owner_epoch: record.owner_epoch,
        prior_grant_id: prior_grant,
        prior_boot_id: prior_boot,
        revocation,
        control_revision: healed.control_revision,
        decision_tick: now,
    };
    match cleared {
        GrantCleared::Deleted(deleted) => tracing::info!(
            variant = "ExpiryProven",
            control_revision = healed.control_revision.0,
            prior_boot = prior_boot.0,
            prior_grant = prior_grant.0,
            grant_delete_revision = deleted.0,
            read_revision = read.read_revision,
            "heal_takeover_authorized"
        ),
        GrantCleared::Absent(absent) => tracing::info!(
            variant = "NoGrantHeld",
            control_revision = healed.control_revision.0,
            prior_boot = prior_boot.0,
            prior_grant = prior_grant.0,
            absent_revision = absent.0,
            read_revision = read.read_revision,
            "heal_takeover_authorized"
        ),
    }
    for event in [
        RecoveryEvent::Plan(Box::new(plan)),
        RecoveryEvent::FenceProven(Box::new(fence)),
    ] {
        ctx.owner
            .send(Msg::Inject {
                partition: PARTITION,
                correlation: HEAL,
                kind: EventKind::Kernel(KernelEvent::Recovery(event)),
            })
            .map_err(|_| HealError::NodeStopped)?;
    }
    Ok(healed)
}

/// How [`clear_until`] found a node's grant gone.
enum Gone {
    /// Deleted at this revision; the record it judged.
    Deleted(Revision, GrantRecord),
    /// Read absent at this store revision.
    Absent(Revision),
}

/// Run [`judge`] for `node` until it answers `Cleared` or `Absent`, or `deadline` passes. A
/// guard that still holds is waited out until about when it says it will stop holding.
async fn clear_until(
    ctx: &HealCtx,
    node: NodeId,
    deadline: tokio::time::Instant,
) -> Result<Gone, HealError> {
    let mut last = format!("the control store did not answer; {STORE_STEP}");
    while tokio::time::Instant::now() < deadline {
        match judge(&*ctx.store, node, ctx.boot, &ctx.budgets, ctx.clock).await {
            Ok((Clearance::Cleared(revision), Some(record), _)) => {
                return Ok(Gone::Deleted(revision, record));
            }
            Ok((Clearance::Absent, None, read)) => return Ok(Gone::Absent(read)),
            Ok((clearance, ..)) => last = kept_in_words(clearance, node, ctx.clock.now()),
            // A lost delete reply is read again next time round, as `clear_grant` advises.
            Err(error)
                if error.retry == RetryRule::BoundedJitter
                    || error.kind == ErrorKind::UnknownOutcome =>
            {
                last = format!("the control store answered: {}; {STORE_STEP}", error.detail);
            }
            Err(error) => return Err(HealError::Control(error.detail)),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(HealError::NotCleared {
        node: node.0,
        within_ms: ctx.within.as_millis(),
        last,
    })
}

/// The next step when another process may still hold the grant (A-R191b, tester w3a03).
const OTHER_OPEN_STEP: &str = "another open may hold this folder's data, for example a live \
                               copy of this folder: shut it down, then open again";

/// The next step when the control store did not settle an answer.
const STORE_STEP: &str = "open again once the control store answers";

/// Why `clearance` kept `grants/{node}` in place, with the next step, in words for
/// [`HealError::NotCleared`] (A-R191b). `now` is the heal's clock, so a guard's until-time reads
/// as a time of day and a distance from now.
fn kept_in_words(clearance: Clearance, node: NodeId, now: Tick) -> String {
    match clearance {
        Clearance::Refused {
            refusal: ClearRefusal::NotProvenExpired,
            until_utc_ms,
        } => {
            let left = i128::from(until_utc_ms) - i128::from(now.0);
            let when = if left > 0 {
                format!(
                    "about {} seconds from now",
                    seconds_in_words(left.unsigned_abs())
                )
            } else {
                "which has just passed".to_owned()
            };
            format!(
                "it may still be in use until {} UTC ({when}), and later if its holder keeps \
                 renewing it; {OTHER_OPEN_STEP}",
                clock_in_words(until_utc_ms)
            )
        }
        Clearance::Refused {
            refusal: ClearRefusal::Frozen,
            ..
        } => "it is frozen for a takeover that has not finished, so another open may be taking \
              over this folder's data: let it finish or shut it down, then open again"
            .to_owned(),
        Clearance::Refused {
            refusal: ClearRefusal::PartitionInTransfer(partition),
            ..
        } => format!(
            "partition {} names node {} as its owner and is not serving yet: let that move \
             finish, then open again",
            partition.0, node.0
        ),
        Clearance::Current => format!(
            "it names the boot this open runs node {} at, so another open of this folder's data \
             may be running now, for example a live copy of this folder: shut it down, then open \
             again",
            node.0
        ),
        Clearance::Conflict => format!(
            "it changed while it was being cleared, so something still renews it; \
             {OTHER_OPEN_STEP}"
        ),
        Clearance::Absent | Clearance::Cleared(_) => {
            format!(
                "the control store gave an answer that does not fit ({clearance:?}); {STORE_STEP}"
            )
        }
    }
}

/// Milliseconds as seconds with at most one decimal: 13200 is "13.2", 30000 is "30".
fn seconds_in_words(ms: u128) -> String {
    let tenths = ms / 100;
    if tenths.is_multiple_of(10) {
        (tenths / 10).to_string()
    } else {
        format!("{}.{}", tenths / 10, tenths % 10)
    }
}

/// UTC milliseconds as a time of day, `HH:MM:SS`.
fn clock_in_words(utc_ms: i64) -> String {
    let secs = utc_ms.div_euclid(1000).rem_euclid(86_400);
    format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
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
/// record does not decode: this call deletes nothing then, and `no_mutation` says so. Its retry rule is
/// `BoundedJitter` only for a store that was unavailable, timed out or not the leader, and
/// `NotWired` otherwise: repeating a refused or unreadable call changes nothing (F-012).
/// `UNKNOWN_OUTCOME` when the delete's reply is lost; running it again reads where it stands.
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
    judge(store, node, boot, &budgets, db.clock())
        .await
        .map(|(clearance, ..)| clearance)
}

/// [`clear_grant`] for `node` running at `boot`, judged with `budgets` on `clock`, without a
/// `Db`: the reopen's heal runs it from a task (M10, arch R3c). Also returns the record it
/// judged, when there was one, and the store revision of the read that found it or its absence.
async fn judge(
    store: &dyn ConfigStore,
    node: NodeId,
    boot: BootId,
    budgets: &Budgets,
    clock: HostClock,
) -> Result<(Clearance, Option<GrantRecord>, Revision), ApiError> {
    let budgets = *budgets;
    let key = ControlKey::Grant(node).encode();
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| store_failed(&format!("get {key}"), &error, true))?;
    let read_revision = Revision(read.read_revision);
    let Some(found) = read.record else {
        tracing::info!(
            node = node.0,
            boot = boot.0,
            read_revision = read_revision.0,
            verdict = ?Clearance::Absent,
            "grant_clear"
        );
        return Ok((Clearance::Absent, None, read_revision));
    };
    let record = GrantRecord::decode(&found.value).ok_or_else(|| {
        unreadable(format!(
            "{key} at revision {} is not a grant record",
            found.mod_revision
        ))
    })?;
    // R3a; M10 arch R5 row 9: A1's one minting site counts from 1, so a record holding 0 was
    // not written by A1. Refused before anything is deleted; a heal's proof carrying it would
    // read as `NoGrantHeld`'s "none held".
    if record.grant == GrantId(0) {
        tracing::warn!(
            node = node.0,
            revision = found.mod_revision,
            "grant_clear_grant_0_refused"
        );
        return Err(unreadable(format!(
            "{key} at revision {} holds grant 0, which no grant issuer writes",
            found.mod_revision
        )));
    }
    let partitions = list_partitions(store).await?;
    let now = clock.now();
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
    Ok((clearance, Some(record), read_revision))
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
        .map_err(|error| store_failed(&format!("list {prefix}"), &error, true))?;
    if listed.truncated {
        return Err(unreadable(format!(
            "list {prefix} came back truncated at revision {}",
            listed.read_revision
        )));
    }
    listed
        .records
        .iter()
        .map(|found| {
            PartitionRecord::decode(&found.value).ok_or_else(|| {
                unreadable(format!(
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
        // Refused before it was proposed: the record is as it was. Any other failure may have
        // reached the store.
        Err(error) => Err(store_failed(
            &format!("delete {key} at revision {revision}"),
            &error,
            matches!(
                error,
                ConfigError::Unavailable { .. } | ConfigError::NotLeader { .. }
            ),
        )),
    }
}

/// `call` failed in the control store, told briefly: the store's own words go to the log
/// only (`grant_clear_store_error{call, error}`), since an `Unavailable` names the whole
/// membership with its ports. A read that timed out says so: the store's wording, "mutation
/// outcome is unknown", is about a write. Never a lost delete reply, which
/// [`delete`] answers itself. Only a transient error ([`crate::control::transient`]) is worth
/// a jittered retry; any other says the call itself is refused, and repeating it changes
/// nothing (review F-012). `no_mutation` is the caller's: true for a read, false for a delete
/// that may have reached the store.
fn store_failed(call: &str, error: &ConfigError, no_mutation: bool) -> ApiError {
    tracing::warn!(call, %error, "grant_clear_store_error");
    let short = match error {
        ConfigError::DeadlineExceededUnknownOutcome => "timed out".to_owned(),
        ConfigError::Unavailable { .. } => {
            "unavailable now (the store's reason is in the log)".to_owned()
        }
        other => other.to_string(),
    };
    let retry = if crate::control::transient(error) {
        RetryRule::BoundedJitter
    } else {
        RetryRule::NotWired
    };
    unavailable(format!("{call}: {short}"), retry, no_mutation)
}

/// A record this build cannot read, or a list cut short: before anything is written, and the
/// same on every retry, so `NotWired` (review F-012).
fn unreadable(detail: String) -> ApiError {
    unavailable(detail, RetryRule::NotWired, true)
}

fn unavailable(detail: String, retry: RetryRule, no_mutation: bool) -> ApiError {
    let tail = if no_mutation {
        "nothing deleted"
    } else {
        "the delete may have applied; run it again to read where it stands"
    };
    ApiError {
        kind: ErrorKind::Unavailable,
        retry,
        no_mutation,
        detail: format!("control store: {detail}; {tail}"),
        retry_after: None,
    }
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

    /// A `Db` whose owner restarted (boot 2, no thread) with `grants/1` left by its boot-1
    /// process, long expired and not frozen: every guard but the store's would clear it.
    /// Returns the revision the record was written at.
    fn restarted_owner(
        store: &dyn ConfigStore,
        rt: &tokio::runtime::Runtime,
    ) -> (Db, std::sync::mpsc::Receiver<Msg>, u64) {
        let (mut owner, mailbox) = crate::host::NodeHandle::unanswered(OWNER);
        owner.boot = rdb_core::contracts::ids::BootId(BOOT.0 + 1);
        let held: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = rdb_core::contracts::event::Budgets::SPEC_DEFAULTS;
        let db = Db::by_hand(
            vec![owner],
            held,
            rt.handle(),
            crate::db::Timeouts::default(),
            Some((std::path::PathBuf::new(), budgets)),
        );
        let old = GrantRecord {
            grant: GrantId(1),
            node: OWNER,
            boot: BOOT,
            authority_generation: AuthorityGeneration(0),
            expiry_utc_ms: 0,
            frozen: false,
        };
        let written = rt
            .block_on(store.put(PutRequest {
                key: Bytes::from(ControlKey::Grant(OWNER).encode()),
                value: old.encode(),
                expected_mod_revision: Some(0),
                dedup: None,
            }))
            .expect("put grants/1");
        (db, mailbox, written.revision)
    }

    fn grant_revision(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime) -> Option<u64> {
        rt.block_on(store.get(GetRequest {
            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
        }))
        .expect("get grants/1")
        .record
        .map(|found| found.mod_revision)
    }

    /// Message 6, round-3 mutant M2: the delete names the revision `clear_grant` judged. A write
    /// to `grants/1` between that read and the delete (here, while the `partitions/` list is
    /// held) makes the delete lose: `Conflict`, and the moved record stays. The control row:
    /// the same record with nothing in between clears.
    /// Unit (~20 ms): `MemStore` behind the scripted list.
    #[test]
    fn a_grant_that_moves_after_it_was_judged_is_not_deleted() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store = Scripted::new(Script {
            hold_lists: 1,
            ..Script::default()
        });
        let (db, _mailbox, written) = restarted_owner(&store, &rt);
        let moved = rt.block_on(async {
            let move_it = async {
                store.list_held.notified().await;
                let key = Bytes::from(ControlKey::Grant(OWNER).encode());
                let current = store.mem().get(GetRequest { key: key.clone() }).await;
                let value = current.expect("get").record.expect("grants/1").value;
                let moved = store
                    .mem()
                    .put(PutRequest {
                        key,
                        value,
                        expected_mod_revision: Some(written),
                        dedup: None,
                    })
                    .await
                    .expect("move grants/1");
                store.release_lists();
                moved.revision
            };
            let (clearance, moved) = tokio::join!(clear_grant(&db, &store, OWNER), move_it);
            assert_eq!(clearance, Ok(Clearance::Conflict));
            moved
        });
        assert_eq!(
            grant_revision(&store, &rt),
            Some(moved),
            "the moved record stays"
        );

        let store = config_testkit::MemStore::new();
        let (db, _mailbox, written) = restarted_owner(&store, &rt);
        assert!(
            matches!(rt.block_on(clear_grant(&db, &store, OWNER)), Ok(Clearance::Cleared(at)) if at.0 > written)
        );
        assert_eq!(grant_revision(&store, &rt), None);
    }

    /// Message 6, round-3 mutant M3: a `partitions/` list that fails or comes back truncated is
    /// never read as complete, since a missing record could hide a partition in transfer. Each
    /// is `UNAVAILABLE` ending "nothing deleted", and `grants/1` stays.
    /// Unit (~20 ms).
    #[test]
    fn a_failed_or_truncated_partition_list_deletes_nothing() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let failing = Scripted::new(Script {
            fail_lists: 1,
            ..Script::default()
        });
        let short = config_testkit::MemStore::with_limits(config_core::Limits {
            max_list_items: 1,
            ..config_core::Limits::DEFAULT
        });
        for id in [1, 2] {
            let mut serving = record(1);
            serving.partition = PartitionId(id);
            put_at(
                &short,
                &rt,
                ControlKey::Partition(PartitionId(id)),
                serving.encode(),
            );
        }
        let stores: [(&str, &dyn ConfigStore); 2] = [("failed", &failing), ("truncated", &short)];
        for (case, store) in stores {
            let (db, _mailbox, written) = restarted_owner(store, &rt);
            let error = rt.block_on(clear_grant(&db, store, OWNER)).expect_err(case);
            assert_eq!(error.kind, ErrorKind::Unavailable, "{case}: {error:?}");
            assert!(
                error.detail.ends_with("; nothing deleted"),
                "{case}: {error:?}"
            );
            assert_eq!(grant_revision(store, &rt), Some(written), "{case}");
        }
    }

    /// Msg 8, F-012: a refusal before the delete says nothing was mutated, and only a transient
    /// store error says a jittered retry helps. A permission error and a `grants/1` that does
    /// not decode stay the same however often they are retried, so they answer `NotWired`; an
    /// unavailable store answers `BoundedJitter`. Each leaves `grants/1` as it was.
    /// Unit (~20 ms).
    #[test]
    fn a_clear_refused_before_the_delete_says_no_mutation_and_retries_only_transient_errors() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        for (case, error, retry) in [
            (
                "permission",
                ConfigError::PermissionDenied {
                    detail: "no grant on grants/".to_owned(),
                },
                RetryRule::NotWired,
            ),
            (
                "unavailable",
                ConfigError::Unavailable {
                    reason: "down".to_owned(),
                },
                RetryRule::BoundedJitter,
            ),
        ] {
            let store = config_testkit::MemStore::new();
            let (db, _mailbox, written) = restarted_owner(&store, &rt);
            store.failing_with(error);
            let refused = rt
                .block_on(clear_grant(&db, &store, OWNER))
                .expect_err(case);
            store.stop_failing();
            assert_eq!(
                (refused.kind, refused.retry, refused.no_mutation),
                (ErrorKind::Unavailable, retry, true),
                "{case}: {refused:?}"
            );
            assert!(
                refused.detail.ends_with("nothing deleted"),
                "{case}: {refused:?}"
            );
            assert_eq!(grant_revision(&store, &rt), Some(written), "{case}");
        }

        let store = config_testkit::MemStore::new();
        let (db, _mailbox, _) = restarted_owner(&store, &rt);
        let key = Bytes::from(ControlKey::Grant(OWNER).encode());
        let found = rt
            .block_on(store.get(GetRequest { key: key.clone() }))
            .expect("get")
            .record
            .expect("grants/1");
        let garbled = rt
            .block_on(store.put(PutRequest {
                key,
                value: Bytes::from_static(b"not a grant record"),
                expected_mod_revision: Some(found.mod_revision),
                dedup: None,
            }))
            .expect("put")
            .revision;
        let refused = rt
            .block_on(clear_grant(&db, &store, OWNER))
            .expect_err("does not decode");
        assert_eq!(
            (refused.kind, refused.retry, refused.no_mutation),
            (ErrorKind::Unavailable, RetryRule::NotWired, true),
            "{refused:?}"
        );
        assert_eq!(grant_revision(&store, &rt), Some(garbled));
    }

    /// Review F-024 (mutant U4): a delete the store refused before proposing it, `Unavailable`
    /// or `NotLeader`, deleted nothing, and says so; any other delete error may have reached
    /// the store, so it says `no_mutation=false` and to run it again to see. Only the transient
    /// ones say a jittered retry helps. Unit (~20 ms): the scripted store fails the delete only.
    #[test]
    fn a_delete_the_store_refused_before_proposing_says_nothing_deleted_and_no_other_does() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        for (case, error, retry, no_mutation) in [
            (
                "unavailable",
                ConfigError::Unavailable {
                    reason: "down".to_owned(),
                },
                RetryRule::BoundedJitter,
                true,
            ),
            (
                "not leader",
                ConfigError::NotLeader { hint: None },
                RetryRule::BoundedJitter,
                true,
            ),
            (
                "permission",
                ConfigError::PermissionDenied {
                    detail: "no delete on grants/".to_owned(),
                },
                RetryRule::NotWired,
                false,
            ),
        ] {
            let store = Scripted::new(Script {
                fail_delete_with: Some(error),
                ..Script::default()
            });
            let (db, _mailbox, written) = restarted_owner(&store, &rt);
            let refused = rt
                .block_on(clear_grant(&db, &store, OWNER))
                .expect_err(case);
            assert_eq!(
                (refused.kind, refused.retry, refused.no_mutation),
                (ErrorKind::Unavailable, retry, no_mutation),
                "{case}: {refused:?}"
            );
            assert!(
                refused.detail.contains("delete grants/1"),
                "{case}: {refused:?}"
            );
            let tail = if no_mutation {
                "nothing deleted"
            } else {
                "the delete may have applied; run it again to read where it stands"
            };
            assert!(refused.detail.ends_with(tail), "{case}: {refused:?}");
            assert_eq!(grant_revision(&store, &rt), Some(written), "{case}");
        }
    }

    fn put_at(
        store: &dyn ConfigStore,
        rt: &tokio::runtime::Runtime,
        key: ControlKey,
        value: Bytes,
    ) {
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(key.encode()),
            value,
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put");
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

    // ---- case B: the heal with no grant held (M10 arch R3a, rulings A-R159, A-R163, A-R171) ----

    /// The boot the reopen in these rows runs at: open 2's first (arch R3b).
    const OPEN_2: BootId = BootId((1 << 16) + 1);

    /// `partitions/1` as a layout-3 record at generation 3, owner epoch 3, whose owner boot is
    /// `owner_boot`. Returns the revision it was written at.
    fn rooted(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime, owner_boot: BootId) -> u64 {
        let record = PartitionRecord {
            generation: Generation(3),
            owner_epoch: OwnerEpoch(3),
            ..record(3)
        };
        let root = PartitionRoot {
            mode: rdb_core::authority::partition::RecordMode::Active,
            owner_boot,
            base_seq: Seq(7),
            base_digest: Digest::ROOT,
            predecessor_generation: Generation(2),
            predecessor_cutoff: Seq(7),
            instance: [7; INSTANCE_LEN],
            run: Revision(1),
        };
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            value: record.encode_with(&root),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put partitions/1")
        .revision
    }

    /// Run one heal over `store` and return its answer and every message node 1 received.
    fn heal_once(
        store: Arc<dyn ConfigStore>,
        rt: &tokio::runtime::Runtime,
    ) -> (Result<Healed, HealError>, Vec<Msg>) {
        let (owner, mailbox) = std::sync::mpsc::channel();
        let ctx = HealCtx {
            store,
            boot: OPEN_2,
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            owner,
            within: Duration::from_millis(500),
        };
        let healed = rt.block_on(heal(ctx));
        (healed, mailbox.try_iter().collect())
    }

    /// The `FencingProof` a heal sent node 1, if it sent one.
    fn fence_sent(sent: &[Msg]) -> Option<FencingProof> {
        sent.iter().find_map(|msg| match msg {
            Msg::Inject {
                kind: EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(fence))),
                ..
            } => Some((**fence).clone()),
            _ => None,
        })
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime")
    }

    /// Case B (R3a; R5 rows 6 and 8): every `grants/{n}` absent and a layout-3 `partitions/1`.
    /// The heal hands node 1 a `Plan`, then a `NoGrantHeld` takeover authorization whose
    /// `absent_revision` is the read that found `grants/1` absent, whose `control_revision` is
    /// `partitions/1`'s `mod_revision` read at or after it (A-R163), with `prior_grant_id` 0
    /// ("none held") and `prior_boot_id` the record's `owner_boot`. Unit (~10 ms): `MemStore`.
    #[test]
    fn a_reopen_with_no_grant_held_hands_node_1_a_no_grant_held_takeover() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let written = rooted(&*store, &rt, BootId(5));
        // A later write elsewhere, so the absent read's revision is past the record's.
        rt.block_on(store.put(PutRequest {
            key: Bytes::from_static(b"unrelated"),
            value: Bytes::from_static(b"x"),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("put an unrelated key");
        let (healed, sent) = heal_once(Arc::clone(&store), &rt);
        let healed = healed.expect("case B heals");
        assert_eq!(sent.len(), 2, "a Plan, then the proof");
        assert!(matches!(
            &sent[0],
            Msg::Inject {
                kind: EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(_))),
                ..
            }
        ));
        let fence = fence_sent(&sent).expect("a takeover authorization");
        let Revocation::NoGrantHeld { absent_revision } = fence.revocation else {
            panic!("expected NoGrantHeld: {fence:?}");
        };
        assert!(absent_revision.0 > written, "{fence:?}");
        assert_eq!(
            fence.control_revision,
            Revision(written),
            "partitions/1's mod_revision"
        );
        assert_eq!(fence.prior_grant_id, GrantId(0));
        assert_eq!(fence.prior_boot_id, BootId(5));
        assert_eq!(fence.prior_generation, Generation(3));
        assert_eq!(fence.prior_owner_epoch, OwnerEpoch(3));
        assert_eq!(healed.control_revision, fence.control_revision);
        assert!(healed.read_revision >= absent_revision, "{healed:?}");
    }

    /// R5 row 8 for case B: a record whose `owner_boot` is 0 cannot name the prior process, so
    /// the heal refuses it and sends nothing. Unit (~10 ms).
    #[test]
    fn a_no_grant_heal_over_owner_boot_0_is_refused_and_sends_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(0));
        let (healed, sent) = heal_once(store, &rt);
        assert!(matches!(healed, Err(HealError::Record(_))), "{healed:?}");
        assert!(sent.is_empty(), "{} messages sent", sent.len());
    }

    /// Case B over a layout-2 `partitions/1`: no root, so no anchor and no owner boot. Refused,
    /// and nothing sent. Unit (~10 ms).
    #[test]
    fn a_no_grant_heal_over_a_record_with_no_root_is_refused_and_sends_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        put(&store, &rt, record(3).encode(), 0);
        let (healed, sent) = heal_once(store, &rt);
        assert!(matches!(healed, Err(HealError::Record(_))), "{healed:?}");
        assert!(sent.is_empty(), "{} messages sent", sent.len());
    }

    /// `grants/1` as a dead open's boot-1 process left it holding `grant`: long expired and not
    /// frozen, so case A's guards clear it. Returns the revision it was written at.
    fn old_grant(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime, grant: GrantId) -> u64 {
        let old = GrantRecord {
            grant,
            node: OWNER,
            boot: BOOT,
            authority_generation: AuthorityGeneration(0),
            expiry_utc_ms: 0,
            frozen: false,
        };
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
            value: old.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put grants/1")
        .revision
    }

    /// Case A (R3a, A-R175): the heal deletes a dead open's expired grant and hands node 1 an
    /// `ExpiryProven` takeover. `prior_grant_id` is the record's grant; `prior_boot_id` is
    /// `partitions/1`'s `owner_boot`, which the F1 commit pinned, not the grant's own boot.
    /// Unit (~10 ms): `MemStore`.
    #[test]
    fn a_reopen_over_an_expired_grant_hands_node_1_an_expiry_proven_takeover_naming_the_owner_boot()
    {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let written = rooted(&*store, &rt, BootId(5));
        let granted = old_grant(&*store, &rt, GrantId(4));
        let (healed, sent) = heal_once(Arc::clone(&store), &rt);
        let healed = healed.expect("case A heals");
        assert!(
            matches!(healed.cleared, GrantCleared::Deleted(deleted) if deleted.0 > granted),
            "{healed:?}"
        );
        let fence = fence_sent(&sent).expect("a FenceProven was sent");
        assert!(
            matches!(
                fence.revocation,
                Revocation::ExpiryProven {
                    frozen_expiry_utc_ms: 0,
                    ..
                }
            ),
            "{fence:?}"
        );
        assert_eq!(fence.prior_grant_id, GrantId(4));
        assert_eq!(
            fence.prior_boot_id,
            BootId(5),
            "partitions/1's owner_boot, not the grant's boot {}",
            BOOT.0
        );
        assert_eq!(fence.control_revision, Revision(written));
        assert_eq!(grant_revision(&*store, &rt), None, "the old grant is gone");
    }

    /// Case A (R3a; R5 row 9; A-R175): A1 never issues `GrantId(0)`, and a proof carrying it
    /// would read as case B's "none held". A record holding it is refused before anything is
    /// deleted: the heal fails naming it, `grants/1` stays at its revision, and node 1 is sent
    /// nothing. Unit (~10 ms): `MemStore`.
    #[test]
    fn a_heal_over_a_grant_record_holding_grant_0_is_refused_and_deletes_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let written = old_grant(&*store, &rt, GrantId(0));
        let (healed, sent) = heal_once(Arc::clone(&store), &rt);
        match &healed {
            Err(HealError::Control(detail)) => assert!(detail.contains("grant 0"), "{detail}"),
            other => panic!("expected a refusal naming grant 0: {other:?}"),
        }
        assert_eq!(
            grant_revision(&*store, &rt),
            Some(written),
            "nothing deleted"
        );
        assert!(sent.is_empty(), "{} messages sent", sent.len());
    }

    /// A control store whose `partitions/1` reads answer from the snapshot [`Self::freeze`]
    /// took: a lagging read, older than anything written after it. Everything else is the
    /// `MemStore` underneath. Written out by hand because `async-trait` is not a dependency of
    /// this crate.
    struct StalePartition {
        inner: config_testkit::MemStore,
        stale: std::sync::Mutex<Option<config_core::GetResponse>>,
    }

    type Reply<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, ConfigError>> + Send + 'a>>;

    impl StalePartition {
        fn new() -> Self {
            Self {
                inner: config_testkit::MemStore::new(),
                stale: std::sync::Mutex::new(None),
            }
        }

        fn freeze(&self, rt: &tokio::runtime::Runtime) {
            let read = rt
                .block_on(self.inner.get(GetRequest {
                    key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                }))
                .expect("get partitions/1");
            *self.stale.lock().expect("stale") = Some(read);
        }
    }

    impl ConfigStore for StalePartition {
        fn get<'a, 'b>(&'a self, request: GetRequest) -> Reply<'b, config_core::GetResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            let stale = self.stale.lock().expect("stale").clone();
            match stale {
                Some(read)
                    if request.key == ControlKey::Partition(PARTITION).encode().as_bytes() =>
                {
                    Box::pin(std::future::ready(Ok(read)))
                }
                _ => self.inner.get(request),
            }
        }

        fn list<'a, 'b>(&'a self, request: ListRequest) -> Reply<'b, config_core::ListResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.list(request)
        }

        fn put<'a, 'b>(&'a self, request: PutRequest) -> Reply<'b, config_core::MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.put(request)
        }

        fn delete<'a, 'b>(
            &'a self,
            request: DeleteRequest,
        ) -> Reply<'b, config_core::MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.delete(request)
        }

        fn capabilities(&self) -> config_core::Capabilities {
            self.inner.capabilities()
        }

        fn watch<'a, 'b>(
            &'a self,
            request: config_core::WatchRequest,
        ) -> Reply<'b, config_core::WatchStream>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.watch(request)
        }
    }

    /// A-R163's read-before guards (A-R175): a `partitions/1` read served below the revision of
    /// the grant delete (case A) or of the read that found the grant absent (case B) is a
    /// lagging answer, so the heal refuses it and sends node 1 nothing. Unit (~20 ms):
    /// `MemStore` behind [`StalePartition`].
    #[test]
    fn a_partitions_read_older_than_the_grant_is_gone_is_refused_and_sends_nothing() {
        let rt = runtime();

        let store = Arc::new(StalePartition::new());
        rooted(&store.inner, &rt, BootId(5));
        store.freeze(&rt);
        old_grant(&store.inner, &rt, GrantId(4));
        let (healed, sent) = heal_once(Arc::clone(&store) as Arc<dyn ConfigStore>, &rt);
        assert!(
            matches!(healed, Err(HealError::ReadBeforeDelete { read, deleted }) if read < deleted),
            "case A: {healed:?}"
        );
        assert!(sent.is_empty(), "case A: {} messages sent", sent.len());

        let store = Arc::new(StalePartition::new());
        rooted(&store.inner, &rt, BootId(5));
        store.freeze(&rt);
        rt.block_on(store.inner.put(PutRequest {
            key: Bytes::from_static(b"unrelated"),
            value: Bytes::from_static(b"x"),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("put unrelated");
        let (healed, sent) = heal_once(Arc::clone(&store) as Arc<dyn ConfigStore>, &rt);
        assert!(
            matches!(healed, Err(HealError::ReadBeforeAbsence { read, absent }) if read < absent),
            "case B: {healed:?}"
        );
        assert!(sent.is_empty(), "case B: {} messages sent", sent.len());
    }
}
