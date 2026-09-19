//! Startup order, the ready line, and graceful shutdown (ADR-0018 §3, §4, m3-architecture §4).
//!
//! Everything the daemon does is library behaviour composed here: an embedder that wires
//! `ConfigNode` and `config-grpc` the same way gets the same semantics, which is why this
//! module creates no types of its own beyond the ready line.
//!
//! # Order, and why each step is where it is
//!
//! 1. **Open the store.** `RocksStore::open` is where an identity mismatch, a locked
//!    directory or a missing column family is detected. It happens before any socket exists,
//!    so a directory bound to another identity exits 2 and an unopenable one (locked, missing
//!    column family) exits 3, without either ever having been reachable (ADR-0011, ADR-0018).
//! 2. **Verify the manifest** (with `--form`), except the endpoint check. A forged or expired
//!    manifest must not reach a listener either, and neither must one that gives this node
//!    `role = "learner"`: a learner is added by a leader, never formed (ADR-0023).
//! 3. **Load the policy.** Missing or invalid without `--dev-allow-all` starts the node
//!    *unready* — it is not a startup failure, because a node that cannot authorize still
//!    has to replicate (ADR-0018 §6).
//! 4. **Bind** peer, client, gossip and health. Port `0` is resolved here, and the resolved
//!    addresses are what the node advertises and what the ready line reports. Binding is not
//!    serving: the listener exists, nothing answers on it yet.
//! 5. **Check the manifest's endpoints** (with `--form`) against the addresses just bound.
//! 6. **Start** gossip, then the node, and — with `--form` — **form**, which is also where the
//!    already-formed refusal happens.
//! 7. **Serve** both gRPC planes and health. Nothing before this point can accept a client or
//!    peer RPC, so every refusal above exits 2 or 3 without a single connection having been
//!    answered (ADR-0018 §5).
//! 8. **Announce** — exactly one JSON line on stdout, and nothing else on stdout ever.
//! 9. **Wait** for Ctrl-C or the shutdown file, then drain in the ADR-0018 §4 order.
//!
//! Steps 5 and 6 sit *between* binding and serving on purpose. An earlier revision served both
//! planes before the endpoint and already-formed checks ran, so a node destined to exit 2 spent
//! the interval accepting connections it was about to abandon (E2E-18).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use config_core::{
    AllowAll, Authorizer, Capabilities, ClusterIdentity, Dedup, Durability,
    GossipObservationSource, Liveness, NoGossip, ObservedPeerHint, Pagination, Principal,
    StaticAllowlist, TransportSecurity, WatchResumption,
};
use config_engine::{
    AuthzKind, ConfigNode, FormationError, FormationPlan, NodeConfig, StorageHandle,
};
use config_gossip::{GossipConfig, GossipNode};
use config_grpc::{
    serve_client_plane, serve_peer_plane, ClientBackend, GrpcPeerTransport, MtlsConfig,
    PeerIdentity, ServerHandle, TlsMode,
};
use config_storage::{NoFaults, RocksOptions, RocksStore, StorageOpenError};
use serde::Serialize;
use tokio::net::TcpListener;

use crate::backup;
use crate::cli::Cli;
use crate::config::{ServerConfig, TlsModeName};
use crate::{health, manifest};

/// How often the shutdown file is looked for (ADR-0018 §4).
///
/// The one periodic timer in the daemon. Windows has no `SIGTERM` and no portable way to
/// watch a single path cheaply, so this is a poll; 100 ms is far below any test's deadline
/// and far above any cost worth measuring.
const SHUTDOWN_POLL: Duration = Duration::from_millis(100);

/// What the process exits with (ADR-0018 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// Clean shutdown.
    Ok = 0,
    /// Configuration, identity, TLS-gate or manifest rejection.
    Rejected = 2,
    /// Fatal storage failure.
    Storage = 3,
}

/// Anything that stops the daemon before or during startup, already carrying its exit code.
#[derive(Debug)]
pub struct Fatal {
    /// The exit code to use.
    pub code: ExitCode,
    /// A stable `msg=` value for the log line, so a test can assert on the reason without
    /// matching prose (`identity_mismatch`, `manifest_rejected`, …).
    pub reason: &'static str,
    /// The operator-facing detail. Never contains a key, a value, or key material.
    pub detail: String,
}

impl Fatal {
    pub(crate) fn rejected(reason: &'static str, detail: impl std::fmt::Display) -> Self {
        Self {
            code: ExitCode::Rejected,
            reason,
            detail: detail.to_string(),
        }
    }

    pub(crate) fn storage(reason: &'static str, detail: impl std::fmt::Display) -> Self {
        Self {
            code: ExitCode::Storage,
            reason,
            detail: detail.to_string(),
        }
    }
}

/// The one line the daemon writes to stdout (ADR-0018 §3, TA-20.2).
///
/// Field order is the serialization order and is part of the contract only in the sense that
/// a parser must not depend on it; `wait_ready` parses JSON, not a prefix.
#[derive(Debug, Clone, Serialize)]
pub struct ReadyLine {
    /// Always `true`. Present so a reader can tell this line from any future stdout protocol.
    pub ready: bool,
    /// This node's id.
    pub node_id: u64,
    /// Bound peer-plane address.
    pub peer: String,
    /// Bound client-plane address.
    pub client: String,
    /// Bound gossip address, when gossip is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gossip: Option<String>,
    /// Bound health address, when `--health-listen` was given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
}

/// What `--capabilities` prints: the [`Capabilities`] contract plus this run's schema triple.
///
/// Flattened rather than nested so every key `Capabilities` has keeps the exact name and place
/// it had before M6 — the report is a machine-readable contract that operator tooling already
/// parses, and moving its fields under a wrapper to add one would be a breaking change for the
/// benefit of a field nothing had asked for yet (ADR-0030 M6-87).
#[derive(Debug, Clone, serde::Serialize)]
pub struct CapabilitiesReport {
    /// The unchanged M0–M5 capability contract.
    #[serde(flatten)]
    pub capabilities: Capabilities,
    /// What this run can read and write (ADR-0030). Operator-facing only: nothing in the
    /// cluster reads it back.
    pub schema: config_core::SchemaTriple,
}

/// Compute the capability report from configuration alone, opening nothing (`--capabilities`).
///
/// Deliberately duplicated from `ConfigNode::capabilities` rather than obtained from a started
/// node: the whole point of the flag is to answer without a store, a listener, or a lock on
/// the data directory. E2E-02 asserts the two agree, which is what keeps the duplication
/// honest.
pub fn capabilities_without_opening(cfg: &ServerConfig, cli: &Cli) -> CapabilitiesReport {
    let capabilities = Capabilities {
        durability: if cli.unsafe_no_sync {
            Durability::PersistentUnverified
        } else {
            Durability::Persistent
        },
        // M4: this daemon retains an event journal and exposes its compaction floor, so a
        // resuming watcher can tell "you are behind" from "your cursor is gone" (ADR-0020).
        watch_resumption: WatchResumption::Retained {
            compact_revision_visible: true,
        },
        authz: authz_kind(cfg, cli).into(),
        transport_security: match cfg.tls_mode {
            TlsModeName::Mutual => TransportSecurity::MutualTls,
            TlsModeName::Insecure => TransportSecurity::Insecure,
        },
        pagination: Pagination::Unsupported,
        // M5: read from `[dedup]` rather than pinned off, because this function's contract is
        // that it agrees with the started node's own report (E2E-02) — and the started node
        // reports `Bounded` as soon as the section enables it (ADR-0025, ADR-0016).
        dedup: if cfg.dedup.enabled {
            Dedup::Bounded {
                window_requests: cfg.dedup.window_requests,
            }
        } else {
            Dedup::Unsupported
        },
    };
    CapabilitiesReport {
        capabilities,
        schema: cli.schema(),
    }
}

/// Which authorization model this run will enforce, without reading the policy file.
///
/// `--dev-allow-all` wins outright; otherwise a configured policy path means
/// `StaticAllowlist`. Whether that file actually loads is decided at startup and can still
/// downgrade the *running* node to `Missing`/`Invalid` — `--capabilities` cannot promise that
/// a file it has not read will parse, and says the strictest thing instead.
fn authz_kind(cfg: &ServerConfig, cli: &Cli) -> AuthzKind {
    if cli.dev_allow_all {
        AuthzKind::Development
    } else if cfg.signed_policy.is_some() {
        // The strictest honest answer before the files are read: configuration validation has
        // already proved the paths and the trust keys exist, but not that the document on disk
        // verifies (M6-37, M6-38).
        AuthzKind::SignedPolicy
    } else if cfg.policy_path.is_some() {
        AuthzKind::StaticAllowlist
    } else {
        AuthzKind::Missing
    }
}

/// Serves each authenticated principal a `DirectClient` over the local node.
///
/// The client plane holds no state of its own; it binds the transport-derived principal to
/// this node and nothing else (ADR-0012: a request field can never influence identity).
struct NodeBackend {
    node: ConfigNode,
    /// The store the admin plane's `Backup` RPC exports from (M5, ADR-0024).
    storage: StorageHandle,
    /// Key material for that export. Never logged; only whether a key is present is reported.
    backup: crate::config::BackupKeys,
    /// The node's pinned-pagination path (M6, ADR-0029).
    ///
    /// One per daemon, shared by every handle this backend hands out: the pin table is the
    /// node's bounded resource, so two principals walking the same revision must share one
    /// snapshot rather than hold two against `list.max_pinned_snapshots`.
    paginator: Arc<config_engine::Paginator>,
    /// The signed-policy reload seam (M6, ADR-0027). `None` under every other `authz.mode`.
    policy: Option<Arc<crate::policy::PolicyLoader>>,
    /// The TLS reload seam (M6, ADR-0028). `None` under `tls.mode = "insecure"`.
    tls: Option<Arc<config_grpc::TlsRotator>>,
    /// The gossip keyring seam (M6, ADR-0028). `None` when gossip is off or unencrypted.
    gossip: Option<Arc<GossipNode>>,
}

impl ClientBackend for NodeBackend {
    fn store_for(&self, principal: Principal) -> Arc<dyn config_core::ConfigStore> {
        Arc::new(
            self.node
                .direct_client(principal)
                .with_pagination(Arc::clone(&self.paginator)),
        )
    }

    /// Forward the plane's authentication refusals to the node's counter (M3-81).
    ///
    /// The client plane is the only place that sees a certificate and the node is the only
    /// place that keeps counters, so without this the health endpoint reports zero rejections
    /// no matter how many certificates the listener turned away.
    fn record_authn_rejection(&self, reason: config_engine::AuthnRejectReason) {
        self.node.record_authn_rejection(reason);
    }
}

/// How long the admin-plane `Backup` RPC waits for the snapshot it triggered to be published.
///
/// A backup is not a request the caller can usefully retry into a tighter loop, so the bound is
/// generous; what matters is that it *is* bounded, because a build that never publishes would
/// otherwise hold the RPC open until the client gave up and left the operator with no answer.
const BACKUP_BUILD_DEADLINE: Duration = Duration::from_secs(300);

/// How often the `Backup` RPC re-reads the store's published snapshot while waiting.
///
/// There is no publication notification to await — the store publishes from OpenRaft's own
/// build task — so this polls. The interval is small enough to be invisible next to the build
/// it is waiting on and large enough to cost nothing.
const BACKUP_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[async_trait::async_trait]
impl config_grpc::AdminBackend for NodeBackend {
    /// Forward the admin plane's allowlist refusals to the node's counter (C5B-15), so
    /// `retcd_authz_denied_total{plane="admin"}` is a fact rather than a declared label.
    fn record_authz_denial(&self) {
        self.node.record_admin_authz_denial();
    }

    fn cluster_id(&self) -> config_core::ClusterId {
        self.node.identity().cluster_id
    }

    fn membership_report(&self) -> config_engine::MembershipReport {
        self.node.membership_report()
    }

    async fn add_learner(
        &self,
        node_id: config_core::NodeId,
        peer_endpoint: String,
        client_endpoint: String,
    ) -> Result<Option<config_engine::LogIdView>, config_engine::AdminError> {
        self.node
            .add_learner(node_id, peer_endpoint, client_endpoint)
            .await
    }

    async fn promote_voter(
        &self,
        node_id: config_core::NodeId,
    ) -> Result<Option<config_engine::LogIdView>, config_engine::AdminError> {
        self.node.promote_voter(node_id).await
    }

    async fn remove_member(
        &self,
        node_id: config_core::NodeId,
    ) -> Result<Option<config_engine::LogIdView>, config_engine::AdminError> {
        self.node.remove_member(node_id).await
    }

    async fn trigger_snapshot(
        &self,
    ) -> Result<config_engine::SnapshotTriggered, config_engine::AdminError> {
        self.node.trigger_snapshot().await
    }

    /// Write a signed backup triple into `dest_dir` **on this node** (ADR-0024, M5-76).
    ///
    /// A snapshot is built fresh rather than reusing whatever `current_snapshot` happens to be
    /// on disk: a backup is a point-in-time export on its own schedule, independent of the Raft
    /// snapshot policy's cadence. That fresh snapshot is then *copied* into `dest_dir` as a
    /// scratch file, because the node still owns the published one. Everything after — hash,
    /// manifest,
    /// signature, optional encryption — is the same [`crate::backup::finish_artifact`] the
    /// offline CLI runs, which is what makes the two paths differ only in `node_id` and
    /// `created_unix_ms`.
    async fn backup(
        &self,
        dest_dir: PathBuf,
        name: Option<String>,
    ) -> Result<config_grpc::BackupArtifact, config_engine::AdminError> {
        let store = match &self.storage {
            StorageHandle::Rocks(store) => store.clone(),
            // Every other handle, present and future: a store that cannot build a snapshot
            // cannot be backed up, and saying so is better than exporting something weaker
            // than the artifact ADR-0024 describes.
            _ => {
                return Err(config_engine::AdminError::Unavailable {
                    reason: "this node's store cannot build a snapshot, so there is nothing to \
                             back up"
                        .to_string(),
                })
            }
        };
        // Both inputs come off the network, so both are checked before anything is built.
        // `dest_dir` must already be a directory — a server that creates directories wherever a
        // caller names one is a filesystem write primitive with an allowlist in front of it —
        // and `name` is a file *stem*, so it must not be able to escape `dest_dir` (C5-03).
        config_grpc::check_backup_dir(&dest_dir)?;
        let name = match &name {
            Some(n) => backup::validate_name(n)
                .map_err(|e| config_engine::AdminError::InvalidArgument {
                    detail: format!("{}: {e}", e.reason()),
                })?
                .to_string(),
            None => backup::default_name(),
        };
        let keys = backup::KeyFiles {
            signing_key: self.backup.signing_key.as_deref(),
            encryption_key: self.backup.encryption_key.as_deref(),
            trust_key: None,
        };

        // Trigger, then wait for a *newer* snapshot than the one that was current when the
        // call arrived. Comparing ids rather than indexes is what makes an idle cluster work:
        // a build at an unchanged last_log_id still gets a fresh id (ADR-0022), so an operator
        // taking two backups of a quiet cluster gets two fresh exports rather than a hang.
        let before = store.snapshot_meta().map(|m| m.snapshot_id);
        self.node.trigger_snapshot().await?;
        let deadline = tokio::time::Instant::now() + BACKUP_BUILD_DEADLINE;
        let meta = loop {
            match store.snapshot_meta() {
                Some(meta) if Some(&meta.snapshot_id) != before.as_ref() => break meta,
                _ if tokio::time::Instant::now() >= deadline => {
                    return Err(config_engine::AdminError::Unavailable {
                        reason: format!(
                            "no snapshot was published within {}s of the trigger",
                            BACKUP_BUILD_DEADLINE.as_secs()
                        ),
                    })
                }
                _ => tokio::time::sleep(BACKUP_POLL_INTERVAL).await,
            }
        };

        let snap_path = config_storage::snapshot::snap_path(store.path(), &meta.snapshot_id);
        // Blocking file work — hashing and, for an encrypted backup, sealing a whole snapshot
        // — off the reactor. Leaving it inline would stall every other RPC this worker thread
        // is driving for as long as the artifact takes to write.
        let signing_key = keys.signing_key.map(Path::to_path_buf);
        let encryption_key = keys.encryption_key.map(Path::to_path_buf);
        let joined = tokio::task::spawn_blocking(move || {
            // Copied to a scratch file inside `dest_dir` first, exactly as the offline path
            // exports to one. Handing the node's *live* `<id>.snap` to `finish_artifact`
            // coupled the artifact to a file the node still owns: the published snapshot can be
            // replaced or purged mid-read, and anything that consumed the path would unlink the
            // file `state_meta/current_snapshot` names, after which openraft's next
            // `InstallSnapshot` fails with "snapshot not found". The copy costs one pass over
            // the snapshot and removes both hazards (C5-01, M5-76).
            let scratch = dest_dir.join(format!("{name}.snap.tmp"));
            let outcome = (|| -> Result<backup::BackupOutcome, backup::BackupError> {
                std::fs::copy(&snap_path, &scratch).map_err(|e| backup::BackupError::Store {
                    what: "source",
                    path: snap_path.display().to_string(),
                    detail: format!("cannot stage a copy at {}: {e}", scratch.display()),
                })?;
                let header = config_storage::snapshot::SnapshotReader::open(&scratch)
                    .map_err(|e| backup::BackupError::Store {
                        what: "source",
                        path: scratch.display().to_string(),
                        detail: e.to_string(),
                    })?
                    .header()
                    .clone();
                let keys = backup::KeyFiles {
                    signing_key: signing_key.as_deref(),
                    trust_key: None,
                    encryption_key: encryption_key.as_deref(),
                };
                backup::finish_artifact(&header, &scratch, &dest_dir, &name, &keys)
            })();
            // This task created the scratch file, so this task removes it — on both paths.
            let _ = std::fs::remove_file(&scratch);
            outcome
        })
        .await;
        let outcome = match joined {
            Ok(Ok(outcome)) => outcome,
            // A refusal keeps its stable `reason` on the wire, so an operator scripting the
            // RPC branches on the same string the CLI prints.
            Ok(Err(e)) => {
                return Err(config_engine::AdminError::InvalidArgument {
                    detail: format!("{}: {e}", e.reason()),
                })
            }
            Err(e) => {
                return Err(config_engine::AdminError::Unavailable {
                    reason: format!("the backup task did not complete: {e}"),
                })
            }
        };

        tracing::info!(
            cluster_id = %outcome.cluster_id,
            revision = outcome.revision,
            sha256 = %outcome.sha256,
            dest = %outcome.snapshot_file.display(),
            encrypted = outcome.encrypted,
            "backup_created"
        );
        let (snapshot_file, manifest_file, signature_file) =
            backup::BackupManifest::file_names(&outcome.name);
        Ok(config_grpc::BackupArtifact {
            name: outcome.name,
            snapshot_file,
            manifest_file,
            signature_file,
            sha256: outcome.sha256,
            revision: outcome.revision,
            size_bytes: outcome.size_bytes,
            encrypted: outcome.encrypted,
        })
    }

    /// M6-12: re-read the signed policy now, without waiting for a poll tick (ADR-0027).
    ///
    /// The admin plane has already checked the caller against the **currently active**
    /// document's `admins`; this method only does the work.
    async fn reload_policy(&self) -> Result<config_grpc::PolicyReload, config_engine::AdminError> {
        let Some(loader) = self.policy.clone() else {
            return Err(config_engine::AdminError::Unavailable {
                reason: format!(
                    "{}: this node is not running authz.mode = \"signed\"",
                    config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED
                ),
            });
        };
        // File reads and the journal gate, so not on a runtime worker.
        tokio::task::spawn_blocking(move || loader.reload("rpc"))
            .await
            .map_err(|e| config_engine::AdminError::Unavailable {
                reason: format!("the policy reload task did not complete: {e}"),
            })?
            .map_err(|rejected| config_engine::AdminError::InvalidArgument {
                detail: format!("{}: {rejected}", rejected.reason()),
            })
    }

    /// M6-42: re-read the configured TLS files now, without waiting for a poll tick
    /// (ADR-0028).
    async fn reload_tls(
        &self,
    ) -> Result<Vec<config_grpc::TlsPlaneReload>, config_engine::AdminError> {
        let Some(rotator) = self.tls.clone() else {
            return Err(config_engine::AdminError::Unavailable {
                reason: format!(
                    "{}: this node runs tls.mode = \"insecure\" and has no credentials to rotate",
                    config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED
                ),
            });
        };
        // Three file reads, so not on a runtime worker — the same reasoning as `reload_policy`.
        tokio::task::spawn_blocking(move || rotator.reload("rpc"))
            .await
            .map_err(|e| config_engine::AdminError::Unavailable {
                reason: format!("the tls reload task did not complete: {e}"),
            })?
            .map_err(|refused| config_engine::AdminError::InvalidArgument {
                detail: format!("{}: {refused}", refused.reason()),
            })
    }

    /// M6-57..M6-59: one step of a gossip key rotation on this node (ADR-0028).
    ///
    /// The key is parsed here rather than at the transport, against the same
    /// [`crate::config::parse_gossip_key`] that validates `gossip.secret_key_hex` — one
    /// definition of what a gossip key is, so an operator cannot install over the wire
    /// something their configuration file would have refused.
    async fn rotate_gossip_key(
        &self,
        op: config_grpc::GossipKeyOp,
        key_hex: &str,
        force: bool,
    ) -> Result<config_grpc::GossipKeyringView, config_engine::AdminError> {
        let Some(gossip) = self.gossip.clone() else {
            return Err(config_engine::AdminError::Unavailable {
                reason: format!(
                    "{}: this node runs no encrypted gossip",
                    config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED
                ),
            });
        };
        // The refusal names the key only by shape, never by value: an error string is the one
        // place a mistyped key would otherwise end up in a log.
        let key = crate::config::parse_gossip_key(key_hex).map_err(|_| {
            config_engine::AdminError::InvalidArgument {
                detail: "invalid_gossip_key: key_hex must be 64 hex characters (an AES-256 key)"
                    .to_string(),
            }
        })?;

        let keyring = match op {
            config_grpc::GossipKeyOp::Add => gossip.add_gossip_key(&key).await,
            config_grpc::GossipKeyOp::Use => gossip.use_gossip_key(&key).await,
            config_grpc::GossipKeyOp::Remove => gossip.remove_gossip_key(&key, force).await,
        }
        .map_err(gossip_rotation_error)?;

        Ok(config_grpc::GossipKeyringView {
            primary_fingerprint: config_gossip::fingerprint_hex(keyring.primary),
            accepted_fingerprints: keyring
                .accepted
                .into_iter()
                .map(config_gossip::fingerprint_hex)
                .collect(),
        })
    }
}

/// Map a keyring refusal onto the admin plane's error vocabulary.
///
/// Both refusals become `InvalidArgument` with a greppable reason prefix rather than new
/// [`config_engine::AdminError`] variants: the admin error type is the membership plane's
/// vocabulary, and a gossip keyring is not membership. What a caller branches on is the
/// prefix, which is stable.
fn gossip_rotation_error(e: config_gossip::GossipError) -> config_engine::AdminError {
    match e {
        // The one refusal an operator may legitimately overrule, so it is worth telling apart.
        still_needed @ config_gossip::GossipError::GossipKeyStillNeeded { .. } => {
            config_engine::AdminError::InvalidArgument {
                detail: format!("gossip_key_still_needed: {still_needed}"),
            }
        }
        refused @ config_gossip::GossipError::Keyring(_) => {
            config_engine::AdminError::InvalidArgument {
                detail: format!("gossip_keyring_refused: {refused}"),
            }
        }
        // Advertising failed after the keyring already changed. Reported as unavailable, not
        // as invalid: the step *was* taken here, and what failed is telling the cluster — which
        // the next `update_hint` will do anyway.
        other => config_engine::AdminError::Unavailable {
            reason: format!("gossip_advertise_failed: {other}"),
        },
    }
}

/// Rotate `handle`'s credentials with the rest of this node's, from now on.
///
/// A no-op under `tls.mode = "insecure"`, where there is no rotator and the handle carries no
/// credentials — written as one helper because both planes register identically and a second
/// copy of it is a second place for the two to drift apart.
fn register_plane(rotator: &Option<Arc<config_grpc::TlsRotator>>, handle: Option<&ServerHandle>) {
    if let (Some(rotator), Some(source)) = (rotator, handle.and_then(ServerHandle::credentials)) {
        rotator.register(source);
    }
}

/// Everything a running daemon holds, in the order it must be torn down.
///
/// The two plane handles are `Option` because the value exists before either plane is served:
/// formation runs between binding and serving (see the module docs), and a formation refusal
/// has to tear down the node, the store and gossip without ever having had a server to drain.
struct Running {
    node: ConfigNode,
    storage: StorageHandle,
    peer_server: Option<ServerHandle>,
    client_server: Option<ServerHandle>,
    gossip: Option<Arc<GossipNode>>,
    health_shutdown: Option<Arc<tokio::sync::Notify>>,
    health_task: Option<tokio::task::JoinHandle<()>>,
    /// The signed-policy poller and the notify that stops it (M6, ADR-0027).
    policy_shutdown: Option<Arc<tokio::sync::Notify>>,
    policy_task: Option<tokio::task::JoinHandle<()>>,
    /// The TLS file poller and the notify that stops it (M6, ADR-0028).
    ///
    /// Held separately from the policy pair rather than merged into one "pollers" list: they
    /// stop at different points of the teardown, because a plane still draining requests is a
    /// plane that must not have its credentials replaced underneath it.
    tls_shutdown: Option<Arc<tokio::sync::Notify>>,
    tls_task: Option<tokio::task::JoinHandle<()>>,
}

/// Start, serve, and shut down. Returns the process exit code.
pub async fn run(cfg: ServerConfig, cli: Cli) -> Result<ExitCode, Fatal> {
    let identity = cfg.identity;

    // ---- 1. store -------------------------------------------------------------------
    // The hub is built *before* the store because the store publishes applied batches into
    // it: it is the store's `AppliedBatchSink`, so it cannot be created from a node that does
    // not exist yet. It learns the reader and the authorizer later, in `ConfigNode::start`
    // (ADR-0020).
    let watch = config_engine::WatchHub::with_defaults(cfg.watch_limits);
    let storage = open_store(
        &cfg,
        &cli,
        Arc::clone(&watch) as Arc<dyn config_storage::AppliedBatchSink>,
    )?;

    // ---- 2. manifest (everything that does not need a bound port) ---------------------
    let verified = if cli.form {
        let files = cfg.manifest.as_ref().ok_or_else(|| {
            Fatal::rejected(
                "manifest_rejected",
                "--form requires a [manifest] section naming path, sig and signing_key_pub",
            )
        })?;
        let verified = manifest::verify_document(files, &identity)
            .map_err(|e| Fatal::rejected("manifest_rejected", e))?;
        // A learner-role manifest says what this node may become, not what it is (ADR-0023).
        // Only a committed Raft entry adds a member, so a node holding one waits idle until an
        // operator calls `AddLearner` against the leader — and `--form` against it is a mistake
        // worth refusing loudly, because the alternative is a second cluster with one voter in
        // it. Refused here, before the planes bind, so such a node never serves anyone.
        if verified.self_is_learner {
            return Err(Fatal::rejected(
                "learner_cannot_form",
                format!(
                    "the manifest gives node {} role \"learner\"; a learner is added by the \
                     leader through AddLearner, never by forming. Start this node without \
                     --form.",
                    identity.node_id
                ),
            ));
        }
        Some(verified)
    } else {
        None
    };

    // ---- 3. policy -------------------------------------------------------------------
    let policy = load_authorizer(&cfg, &cli, &watch);

    // ---- 4. bind ---------------------------------------------------------------------
    let tls = tls_mode(&cfg)?;
    let peer_listener = bind("peer", cfg.peer_listen).await?;
    let client_listener = bind("client", cfg.client_listen).await?;
    let peer_endpoint = local_addr(&peer_listener)?;
    let client_endpoint = local_addr(&client_listener)?;
    let health_listener = match cfg.health_listen {
        Some(addr) => Some(bind("health", addr).await?),
        None => None,
    };
    let health_endpoint = health_listener.as_ref().map(local_addr).transpose()?;

    // ---- 5. endpoint check ------------------------------------------------------------
    // The one manifest check that needs a bound port. It runs here, before a single byte is
    // served, so a manifest that publishes an address this node does not serve is refused
    // without the node ever having answered anyone (ADR-0018 §5).
    if let Some(verified) = &verified {
        manifest::check_endpoints(verified, &identity, &peer_endpoint, &client_endpoint)
            .map_err(|e| Fatal::rejected("manifest_rejected", e))?;
    }

    // ---- 6. start and form ------------------------------------------------------------
    let (gossip_node, gossip_endpoint, gossip_source) =
        start_gossip(&cfg, &peer_endpoint, &client_endpoint, cli.schema()).await?;

    let mut node_cfg = NodeConfig::new(identity, peer_endpoint.clone());
    node_cfg.client_endpoint = Some(client_endpoint.clone());
    node_cfg.raft = cfg.raft;
    // `--compat-schema` reaches the engine here and nowhere else: the gate, the health payload
    // and the peer-plane header all read `NodeConfig::schema` (ADR-0030).
    node_cfg.schema = cli.schema();
    node_cfg.authz_kind = policy.kind;
    node_cfg.transport_security = tls.transport_security();
    node_cfg.limits.watch = cfg.watch_limits;
    // Replicated policy (ADR-0025): the engine enforces it inside apply, so it has to come
    // from the same document the store was sized from.
    node_cfg.limits.dedup = cfg.dedup;
    node_cfg.watch_retention = cfg.retention;
    node_cfg.watch_progress_interval = cfg.watch_progress_interval;
    node_cfg.promote_max_lag = cfg.promote_max_lag;
    node_cfg = node_cfg
        .with_snapshots(cfg.snapshot)
        .map_err(|e| Fatal::rejected("invalid_config", format!("[snapshot]: {e}")))?;
    // The engine cannot count grants through `Arc<dyn Authorizer>` and never sees the file, so
    // both facts the health endpoint publishes have to be handed to it here (M3-42).
    node_cfg = node_cfg.with_policy_grants(policy.grants);
    if let Some(document) = &policy.document {
        node_cfg = node_cfg.with_policy_document(document);
    }

    // Read out before `node_cfg` is moved into the node: both planes and the peer transport
    // size their codecs from the same caps this node enforces (ADR-0010 fix-round note).
    let limits = node_cfg.limits;

    let transport = GrpcPeerTransport::new(tls.clone(), config_engine::NetFault::new(), limits);
    // The same transport, kept concretely. The node only ever wants a `dyn PeerTransport`, but
    // a rotation has to reach `reload`, which is not on that trait — nothing in the engine has
    // any business replacing credentials.
    let peer_dial = Arc::clone(&transport);
    // Present exactly when this node serves mutual TLS: `config::validate` produces
    // `tls_material` and `tls_reload` from the same match arm, and `insecure` produces neither.
    // Built from the profile the planes are about to serve, not from a second translation of
    // `[tls]`: `tls_mode` above is the one place a `TlsMaterial` becomes an `MtlsConfig`, so a
    // reload cannot end up compiling the same files into a different profile than boot did.
    let tls_rotator = match (&cfg.tls_reload, &tls) {
        (Some(files), TlsMode::MutualTls(serving)) => Some(config_grpc::TlsRotator::new(
            config_grpc::TlsFiles {
                ca: files.ca.clone(),
                cert: files.cert.clone(),
                key: files.key.clone(),
            },
            serving.clone(),
            peer_dial,
        )),
        _ => None,
    };
    let node = ConfigNode::start(
        node_cfg,
        storage.clone(),
        transport as Arc<dyn config_engine::PeerTransport>,
        gossip_source,
        policy.authorizer,
        watch,
    )
    .await
    .map_err(node_start_fatal)?;

    let mut running = Running {
        node,
        storage,
        peer_server: None,
        client_server: None,
        gossip: gossip_node,
        health_shutdown: None,
        health_task: None,
        policy_shutdown: None,
        policy_task: None,
        tls_shutdown: None,
        tls_task: None,
    };

    if let Some(verified) = verified {
        if let Err(fatal) = form(&running.node, &verified, &identity).await {
            shutdown(running).await;
            return Err(fatal);
        }
    }

    // ---- 7. serve ---------------------------------------------------------------------
    let peer_server = match serve_peer_plane(
        running.node.peer_handler(),
        peer_listener,
        tls.clone(),
        PeerIdentity {
            cluster_id: identity.cluster_id,
            recovery_epoch: identity.recovery_epoch,
            node_id: identity.node_id,
        },
        limits,
    ) {
        Ok(handle) => handle,
        Err(e) => {
            shutdown(running).await;
            return Err(Fatal::rejected("peer_plane_failed", e));
        }
    };
    running.peer_server = Some(peer_server);
    register_plane(&tls_rotator, running.peer_server.as_ref());

    // One backend value behind two traits, so the admin plane and the client plane can never
    // disagree about which node they are talking to (OQ-43: the admin service is co-located on
    // the client-plane mTLS listener, not given a port of its own).
    // Built here rather than inside the node: the pin table's bounds are the operator's
    // (`[list]`), and the node has no business reading the daemon's configuration document.
    // The clock is the real one — the deterministic `ManualClock` exists for the tests, which
    // construct their own `Paginator` (anti-flake rule 33).
    let paginator = Arc::new(config_engine::Paginator::new(
        identity.node_id,
        running.storage.reader(),
        Arc::new(config_engine::SystemClock),
        limits,
        cfg.list.clone(),
    ));
    let backend = Arc::new(NodeBackend {
        node: running.node.clone(),
        storage: running.storage.clone(),
        backup: cfg.backup.clone(),
        paginator: Arc::clone(&paginator),
        policy: policy.loader.clone(),
        tls: tls_rotator.clone(),
        // Only when the keyring exists: a node gossiping in plaintext has nothing to rotate,
        // and answering `RotateGossipKey` with a success would say otherwise.
        gossip: running
            .gossip
            .as_ref()
            .filter(|gossip| gossip.keyring().is_some())
            .map(Arc::clone),
    });
    // M6-40: under signed mode the admin set is the active document's, re-read on every call;
    // `[authz] admins` is not consulted at all, and startup said so.
    let admins = match &policy.loader {
        Some(loader) => config_grpc::AdminAllowlist::from_signed_policy(Arc::clone(
            loader.authorizer(),
        )
            as Arc<dyn Authorizer>),
        None => config_grpc::AdminAllowlist::new(cfg.admins.clone()),
    };
    let admin = Some(config_grpc::admin_service(
        Arc::clone(&backend) as Arc<dyn config_grpc::AdminBackend>,
        tls.clone(),
        identity.cluster_id,
        admins,
    ));
    let client_server = match serve_client_plane(
        backend as Arc<dyn ClientBackend>,
        client_listener,
        tls,
        identity.cluster_id,
        limits,
        admin,
    ) {
        Ok(handle) => handle,
        Err(e) => {
            shutdown(running).await;
            return Err(Fatal::rejected("client_plane_failed", e));
        }
    };
    running.client_server = Some(client_server);
    register_plane(&tls_rotator, running.client_server.as_ref());

    if let Some(listener) = health_listener {
        let notify = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(config_log::testing::in_current_span(health::serve(
            listener,
            health::Sources {
                node: running.node.clone(),
                pagination: Some(Arc::clone(&paginator)),
                policy: policy.loader.clone(),
                tls: tls_rotator.clone(),
            },
            Arc::clone(&notify),
            cfg.metrics_enabled,
        )));
        running.health_shutdown = Some(notify);
        running.health_task = Some(task);
    }

    // The policy poller starts only once the planes are up: a node that never finished
    // starting has nothing to rotate, and a reload that raced formation would revoke watches
    // on a node with none (M6-11, ADR-0027).
    if let Some(loader) = &policy.loader {
        let notify = Arc::new(tokio::sync::Notify::new());
        // Convergence needs both halves, so it is wired only when gossip is running: with it
        // off there is no way to learn what the other voters hold, and claiming convergence
        // from silence is the one answer that is never safe (ADR-0027 §15.3).
        let convergence = running.gossip.as_ref().map(|gossip| {
            Arc::new(crate::policy::GossipPolicyVersions::new(
                Arc::clone(gossip),
                running.node.clone(),
            )) as Arc<dyn crate::policy::ClusterPolicyVersions>
        });
        running.policy_task = Some(loader.spawn_poller(Arc::clone(&notify), convergence));
        running.policy_shutdown = Some(notify);
    }

    // The TLS poller starts here for the same reason, and one step later than the planes it
    // rotates: both are registered by now, so the first tick cannot replace one plane's
    // credentials on a node whose other plane is still binding.
    if let (Some(rotator), Some(files)) = (&tls_rotator, &cfg.tls_reload) {
        let notify = Arc::new(tokio::sync::Notify::new());
        running.tls_task = Some(crate::rotation::spawn_tls_poller(
            rotator,
            files.watch_files,
            Arc::clone(&notify),
        ));
        running.tls_shutdown = Some(notify);
    }

    // ---- 8. announce -----------------------------------------------------------------
    let ready = ReadyLine {
        ready: true,
        node_id: identity.node_id.0,
        peer: peer_endpoint.clone(),
        client: client_endpoint.clone(),
        gossip: gossip_endpoint.clone(),
        health: health_endpoint.clone(),
    };
    announce(&ready);
    tracing::info!(
        peer = %peer_endpoint,
        client = %client_endpoint,
        gossip = gossip_endpoint.as_deref().unwrap_or("-"),
        health = health_endpoint.as_deref().unwrap_or("-"),
        authz_kind = policy.kind.as_str(),
        ready = running.node.is_ready(),
        "serving"
    );

    // ---- 9. wait and drain -----------------------------------------------------------
    let trigger = wait_for_shutdown(cli.shutdown_file.as_deref()).await;
    tracing::info!(trigger, "shutdown requested");
    // `stop()` before the planes would leave a listener accepting calls into a stopped node,
    // so the servers drain first (ADR-0018 §4).
    shutdown(running).await;
    Ok(ExitCode::Ok)
}

/// Write the one stdout line the process is allowed to write.
fn announce(ready: &ReadyLine) {
    let line = serde_json::to_string(ready).expect("the ready line always serializes");
    println!("{line}");
    use std::io::Write;
    // Flushed explicitly: a parent process blocks on this line, and Rust's stdout is line
    // buffered only when it is a terminal — under a pipe it is block buffered, which would
    // make `wait_ready` hang until the child exits.
    let _ = std::io::stdout().flush();
}

fn open_store(
    cfg: &ServerConfig,
    cli: &Cli,
    sink: Arc<dyn config_storage::AppliedBatchSink>,
) -> Result<StorageHandle, Fatal> {
    let options = RocksOptions {
        sync_writes: !cli.unsafe_no_sync,
        create_if_missing: true,
        // A `--compat-schema 1` node must refuse a newer directory rather than serve it while
        // advertising the older schema (ADR-0030, OQ-65).
        max_format_version: cli.schema().format_version,
    };
    if cli.unsafe_no_sync {
        tracing::warn!(
            reason = "sync_disabled",
            detail = "--unsafe-no-sync disables fsync; this node reports PersistentUnverified",
            "durability_unverified"
        );
    }
    let span = tracing::info_span!("store", node_id = cfg.identity.node_id.0);
    match RocksStore::open_with(
        &cfg.data_dir,
        cfg.identity,
        limits(cfg),
        Arc::new(NoFaults),
        span,
        options,
        sink,
    ) {
        Ok(store) => Ok(store.into()),
        Err(e @ StorageOpenError::IdentityMismatch { .. }) => {
            Err(Fatal::rejected("identity_mismatch", e))
        }
        Err(e) => Err(Fatal::storage("storage_open_failed", e)),
    }
}

/// The caps this node enforces, as the store must also see them.
///
/// The store is opened before `NodeConfig` exists, so the watch caps have to be folded in
/// here too: a store sized from `Limits::DEFAULT` and an engine sized from the document would
/// disagree about the same node (ADR-0010).
fn limits(cfg: &ServerConfig) -> config_core::Limits {
    let mut limits = config_core::Limits::DEFAULT;
    limits.watch = cfg.watch_limits;
    limits.dedup = cfg.dedup;
    limits
}

/// Load the allowlist policy, or record honestly why there is none.
///
/// Never fatal: `Missing` and `Invalid` are *failed* authorization, and a node with failed
/// authorization still replicates while denying every client call (OQ-19).
fn load_authorizer(
    cfg: &ServerConfig,
    cli: &Cli,
    watch: &Arc<config_engine::WatchHub>,
) -> LoadedPolicy {
    if cli.dev_allow_all {
        tracing::warn!(
            "authorization is --dev-allow-all: every request is permitted (development only)"
        );
        return LoadedPolicy::without_document(Arc::new(AllowAll), AuthzKind::Development);
    }
    if cfg.signed_policy.is_some() {
        return load_signed_policy(cfg, cli, watch);
    }
    let Some(path) = cfg.policy_path.as_deref() else {
        tracing::warn!(
            reason = "no_policy_configured",
            detail = "no [authz] policy and no --dev-allow-all; this node starts unready and \
                      denies every client call",
            "authz_unavailable"
        );
        return LoadedPolicy::without_document(deny_everything(), AuthzKind::Missing);
    };
    let text = match crate::config::read_policy(path) {
        Ok(text) => text,
        Err(e) => {
            tracing::error!(error = %e, reason = "unreadable", "authz_unavailable");
            return LoadedPolicy::without_document(deny_everything(), AuthzKind::Missing);
        }
    };
    match crate::config::parse_policy(path, &text) {
        Ok(policy) => {
            let summary = crate::config::grant_summary(&policy);
            let grants = policy.grants.len();
            tracing::info!(
                grants,
                principals = summary.len(),
                "allowlist policy loaded"
            );
            LoadedPolicy {
                authorizer: Arc::new(StaticAllowlist::new(policy)),
                kind: AuthzKind::StaticAllowlist,
                grants: grants as u64,
                document: Some(text.into_bytes()),
                loader: None,
            }
        }
        Err(e) => {
            tracing::error!(error = %e, reason = "invalid", "authz_unavailable");
            LoadedPolicy {
                authorizer: deny_everything(),
                kind: AuthzKind::Invalid,
                grants: 0,
                // The bytes still exist and are still worth publishing a digest of: a fleet
                // holding one identical broken file is a different incident from a fleet
                // holding several different ones.
                document: Some(text.into_bytes()),
                loader: None,
            }
        }
    }
}

/// The `authz.mode = "signed"` branch of [`load_authorizer`] (M6, ADR-0027).
///
/// The first load happens **here**, before any listener is bound, so a node is either serving a
/// verified document or visibly unready — never briefly open under no policy at all. A refusal
/// is not fatal, for the same reason a missing static allowlist is not: the peer plane and
/// consensus are authorized separately (§15.3 bullet 5), and a policy outage must not be a
/// consensus outage.
fn load_signed_policy(
    cfg: &ServerConfig,
    cli: &Cli,
    watch: &Arc<config_engine::WatchHub>,
) -> LoadedPolicy {
    let signed = cfg
        .signed_policy
        .clone()
        .expect("callers check this branch first");
    if !cfg.admins.is_empty() {
        // Once, at startup, and loudly: under signed mode the admin set comes only from the
        // document's own `admins` list (M6-40). An operator who believes this key is granting
        // admin has a security expectation the node does not meet.
        tracing::warn!(
            configured = cfg.admins.len(),
            "authz.admins is ignored under authz.mode = \"signed\"; the admin set comes only \
             from the signed document"
        );
    }
    let authorizer = Arc::new(config_core::SignedPolicyAuthorizer::new(
        cli.break_glass_policy_rollback,
    ));
    if cli.break_glass_policy_rollback {
        tracing::warn!(
            "--break-glass-policy-rollback is set: a signed document at or below the active \
             version will be accepted for the lifetime of this process"
        );
    }
    let loader =
        crate::policy::PolicyLoader::new(signed, Arc::clone(&authorizer), Arc::clone(watch));
    // Synchronous on purpose: nothing else is running yet, so nothing can hold the journal
    // gate, and readiness must be decided before step 4 binds anything.
    let kind = match loader.reload("startup") {
        Ok(_) => AuthzKind::SignedPolicy,
        Err(_) => AuthzKind::NoValidPolicy,
    };
    LoadedPolicy {
        authorizer: authorizer as Arc<dyn Authorizer>,
        kind,
        // Grant *rules* are a static-allowlist concept; the signed document publishes its
        // version and hash instead, which is what an operator correlates across a fleet.
        grants: 0,
        document: None,
        loader: Some(loader),
    }
}

/// What [`load_authorizer`] produced: the authorizer itself, plus the facts the node's health
/// payload reports about the policy in force (M3-42).
struct LoadedPolicy {
    /// Consulted on every client request.
    authorizer: Arc<dyn Authorizer>,
    /// The model, as `capabilities().authz` reports it.
    kind: AuthzKind,
    /// Grant rules held; `0` for allow-all and for a policy that did not load.
    grants: u64,
    /// The exact document bytes, when a document was read. `None` means there was nothing to
    /// hash, not that the hash was dropped.
    document: Option<Vec<u8>>,
    /// The reload seam, under `authz.mode = "signed"` only (M6).
    loader: Option<Arc<crate::policy::PolicyLoader>>,
}

impl LoadedPolicy {
    /// The cases with no document behind them: allow-all, no policy configured, and a policy
    /// file that could not be read at all.
    fn without_document(authorizer: Arc<dyn Authorizer>, kind: AuthzKind) -> Self {
        Self {
            authorizer,
            kind,
            grants: 0,
            document: None,
            loader: None,
        }
    }
}

/// An allowlist with no grants: the deny-everything end of the scale, which is what
/// "authorization failed to load" must behave like.
fn deny_everything() -> Arc<dyn Authorizer> {
    Arc::new(StaticAllowlist::new(config_core::AllowlistPolicy::default()))
}

/// Which transport this run serves, from the validated document.
///
/// Deliberately exhaustive over [`TlsModeName`], with no catch-all arm: a wildcard here would
/// silently map a future mode the daemon does not understand onto plaintext, which is the one
/// mistake this function must never be able to make. A new variant is a compile error instead
/// (critic A2). `(Mutual, None)` is likewise a refusal rather than a downgrade — configuration
/// validation guarantees mutual mode carries material, so reaching it means the two have
/// drifted apart, and answering "then serve plaintext" would be the worst possible reading.
fn tls_mode(cfg: &ServerConfig) -> Result<TlsMode, Fatal> {
    match (cfg.tls_mode, &cfg.tls_material) {
        (TlsModeName::Mutual, Some(material)) => Ok(TlsMode::MutualTls(
            MtlsConfig::new(
                material.ca_pem.clone(),
                material.cert_pem.clone(),
                material.key_pem.clone(),
            )
            // `tls.allow_common_name_principals`: shut unless the document opened it;
            // `config::validate` has already logged `common_name_principals_enabled` if so.
            .with_common_name_principals(material.allow_common_name_principals),
        )),
        (TlsModeName::Mutual, None) => Err(Fatal::rejected(
            "invalid_config",
            "tls.mode = \"mutual\" without ca/cert/key material; refusing to start rather than \
             falling back to plaintext",
        )),
        (TlsModeName::Insecure, _) => {
            // Reachable only behind `--allow-insecure-dev` (config::validate refuses the
            // document otherwise), so this line is exactly "the insecure gate was opened"
            // (M3-44, ADR-0018 §2).
            tracing::warn!(
                detail = "tls.mode = \"insecure\" was accepted via --allow-insecure-dev; both \
                          planes serve plaintext and no peer or client identity is \
                          authenticated (ADR-0010)",
                "insecure_transport_enabled"
            );
            Ok(TlsMode::Insecure)
        }
    }
}

/// Map a [`config_engine::EngineError`] from `ConfigNode::start` onto an exit code.
///
/// Before this existed every failure became exit 3 "storage", so a node that refused to start
/// for a reason the disk had nothing to do with told an operator to go and look at the disk
/// (critic A6). The split is by variant, and ADR-0018 §5 records the one residual imprecision:
/// `EngineError::Raft` covers both "OpenRaft rejected the `[raft]` timers" and "OpenRaft could
/// not replay the log", and the engine does not distinguish them, so both exit 2.
fn node_start_fatal(error: config_engine::EngineError) -> Fatal {
    use config_engine::EngineError;
    match &error {
        EngineError::Storage(_) => Fatal::storage("storage_open_failed", error),
        EngineError::Raft(_) => Fatal::rejected("node_start_failed", error),
        EngineError::NoRuntime => Fatal::rejected("runtime_unavailable", error),
    }
}

async fn bind(what: &'static str, addr: std::net::SocketAddr) -> Result<TcpListener, Fatal> {
    TcpListener::bind(addr)
        .await
        .map_err(|e| Fatal::rejected("bind_failed", format!("{what} listener on {addr}: {e}")))
}

fn local_addr(listener: &TcpListener) -> Result<String, Fatal> {
    listener
        .local_addr()
        .map(|a| a.to_string())
        .map_err(|e| Fatal::rejected("bind_failed", format!("bound listener has no address: {e}")))
}

/// Start advisory gossip, if the configuration asks for it.
///
/// A gossip failure is an alert, not an outage (ADR-0003): if the node cannot start gossip it
/// keeps running with no observation source at all rather than refusing to serve.
async fn start_gossip(
    cfg: &ServerConfig,
    peer_endpoint: &str,
    client_endpoint: &str,
    schema: config_core::SchemaTriple,
) -> Result<
    (
        Option<Arc<GossipNode>>,
        Option<String>,
        Arc<dyn GossipObservationSource>,
    ),
    Fatal,
> {
    let Some(bind_addr) = cfg.gossip_listen else {
        return Ok((None, None, Arc::new(NoGossip)));
    };
    let mut gcfg = GossipConfig::new(cfg.identity.cluster_id, cfg.identity.node_id, bind_addr);
    gcfg.seeds = cfg.gossip_seeds.clone();
    gcfg.secret_key = cfg.gossip_secret_key;
    gcfg.accepted_keys = cfg.gossip_accepted_keys.clone();
    // Advisory only (ADR-0003 §19.9): an operator watching a rolling upgrade can see which
    // nodes are still old without querying each one, but no decision is ever taken from it.
    gcfg.extras = Some(config_gossip::HintExtras {
        schema: Some(schema),
        // Left unset deliberately: `GossipNode::start` fills this slot from the keyring it
        // builds, so the advertised fingerprints and the keys actually installed cannot
        // disagree (ADR-0028). Setting it here would be a second, staler answer.
        accepted_gossip_keys: None,
        // Not filled here: gossip starts before the policy loader does, so there is no
        // version to advertise yet. `None` reads as "lagging" everywhere, which is the
        // fail-closed answer for the moments before the poller's first convergence pass
        // re-advertises the real one through `update_extras` (C6R-02, lead ruling M6-R18).
        policy_version: None,
    });
    let hint = ObservedPeerHint {
        cluster_id: cfg.identity.cluster_id,
        recovery_epoch: cfg.identity.recovery_epoch,
        node_id: cfg.identity.node_id,
        peer_endpoint: peer_endpoint.to_string(),
        client_endpoint: Some(client_endpoint.to_string()),
        software_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: 1,
        zone: None,
        liveness: Liveness::Alive,
    };
    match GossipNode::start(gcfg, hint).await {
        Ok(node) => {
            let addr = node.advertise_addr().to_string();
            let node = Arc::new(node);
            let source = Arc::clone(&node) as Arc<dyn GossipObservationSource>;
            Ok((Some(node), Some(addr), source))
        }
        Err(e) => {
            tracing::warn!(error = %e, "gossip failed to start; continuing without it");
            Ok((None, None, Arc::new(NoGossip)))
        }
    }
}

/// Create the cluster from the verified manifest.
///
/// The endpoint check has already run (step 5); what is left here is `form_cluster` itself,
/// whose refusals — above all "already formed" — are the last thing that can turn a start into
/// an exit 2, and therefore the last thing that must happen before either plane serves.
async fn form(
    node: &ConfigNode,
    verified: &manifest::VerifiedManifest,
    identity: &ClusterIdentity,
) -> Result<(), Fatal> {
    // The learner-role refusal is not here: it runs in `run` as soon as the manifest is
    // verified, before either plane binds, so a node holding such a manifest never serves an
    // RPC on its way to exiting 2.
    let plan = FormationPlan::with_client_endpoints(
        identity,
        verified
            .voters
            .iter()
            .map(|(id, peer, client)| (*id, peer.clone(), client.clone())),
    );
    tracing::info!(voters = verified.voters.len(), "formation_started");
    match node.form_cluster(plan).await {
        Ok(()) => {
            tracing::info!("formation_succeeded");
            Ok(())
        }
        Err(e @ (FormationError::AlreadyFormed | FormationError::StoreNotFresh)) => {
            Err(Fatal::rejected(
                "already_formed",
                format!("{e}; --form is a one-time action"),
            ))
        }
        Err(e) => Err(Fatal::rejected("formation_failed", e)),
    }
}

/// Block until Ctrl-C or the shutdown file appears, and say which it was (OQ-17).
async fn wait_for_shutdown(shutdown_file: Option<&Path>) -> &'static str {
    let file = shutdown_file.map(Path::to_path_buf);
    tokio::select! {
        _ = tokio::signal::ctrl_c() => "ctrl_c",
        () = wait_for_file(file) => "shutdown_file",
    }
}

/// Resolve when `path` exists. With no path, never resolves.
async fn wait_for_file(path: Option<PathBuf>) {
    let Some(path) = path else {
        return std::future::pending().await;
    };
    let mut ticker = tokio::time::interval(SHUTDOWN_POLL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if path.exists() {
            return;
        }
    }
}

/// Drain in the ADR-0018 §4 order and log the final line.
async fn shutdown(running: Running) {
    let Running {
        node,
        storage,
        peer_server,
        client_server,
        gossip,
        health_shutdown,
        health_task,
        policy_shutdown,
        policy_task,
        tls_shutdown,
        tls_task,
    } = running;

    // The poller first: it takes the journal gate, and a reload landing mid-drain would revoke
    // streams the planes are already shutting down.
    if let Some(notify) = policy_shutdown {
        notify.notify_waiters();
    }
    if let Some(task) = policy_task {
        task.abort();
        let _ = task.await;
    }
    // And the TLS poller, before the planes drain: replacing a listener's credentials while it
    // is finishing its last handshakes would be a rotation nobody asked for.
    if let Some(notify) = tls_shutdown {
        notify.notify_waiters();
    }
    if let Some(task) = tls_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(notify) = health_shutdown {
        notify.notify_waiters();
    }
    if let Some(task) = health_task {
        task.abort();
        let _ = task.await;
    }
    // A plane that was never served has nothing to drain — the refusal happened between
    // binding and serving, and the listener is dropped with its handle.
    if let Some(client_server) = client_server {
        if let Err(e) = client_server.shutdown().await {
            tracing::warn!(plane = "client", error = %e, "plane did not drain cleanly");
        }
    }
    if let Some(peer_server) = peer_server {
        if let Err(e) = peer_server.shutdown().await {
            tracing::warn!(plane = "peer", error = %e, "plane did not drain cleanly");
        }
    }
    if let Err(e) = node.stop().await {
        tracing::warn!(error = %e, "node did not stop cleanly");
    }
    if let Some(gossip) = gossip {
        gossip.shutdown().await;
    }
    // Dropping the handle closes RocksDB; nothing may reopen the directory until it has.
    drop(storage);
    tracing::info!("drained");
    // `shutdown_complete` is logged by `main` after the runtime itself has stopped: OpenRaft's
    // tick loop logs as it is cancelled, and a "final line" that another task can still write
    // after is not a final line (ADR-0018 §4, E2E-09).
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M6-87, the `--capabilities` half: the schema triple is an added *top-level* key.
    ///
    /// Asserted on the serialized shape rather than the struct because `#[serde(flatten)]` is
    /// the whole claim: a wrapper field would compile, pass every type check, and silently
    /// break the operator tooling that reads `durability` and friends at the root.
    #[test]
    fn capabilities_report_adds_schema_without_moving_anything() {
        let report = CapabilitiesReport {
            capabilities: Capabilities::EPHEMERAL_DEVELOPMENT,
            schema: config_core::CURRENT_SCHEMA,
        };
        let json: serde_json::Value = serde_json::to_value(&report).expect("the report serializes");
        let flat: serde_json::Value = serde_json::to_value(Capabilities::EPHEMERAL_DEVELOPMENT)
            .expect("the contract serializes");

        let object = json.as_object().expect("a JSON object");
        for (key, value) in flat.as_object().expect("a JSON object") {
            assert_eq!(
                object.get(key),
                Some(value),
                "`{key}` must keep the name and place it had before M6"
            );
        }
        assert_eq!(
            object.len(),
            flat.as_object().expect("a JSON object").len() + 1,
            "exactly one key is added: {json}"
        );
        assert_eq!(
            object.get("schema"),
            Some(&serde_json::to_value(config_core::CURRENT_SCHEMA).expect("a triple")),
            "and it is the schema triple this run advertises"
        );
    }
}
