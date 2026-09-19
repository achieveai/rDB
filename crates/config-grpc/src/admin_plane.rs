//! The admin plane: `AdminService` over tonic (spec §13.2, §19.8; ADR-0023, ADR-0024).
//!
//! Served on the **client-plane listener**, not on a listener of its own (OQ-43). A separate
//! privileged port would mean a second certificate profile, a second port to fence and a
//! second surface to test, for no isolation the `[authz] admins` allowlist does not already
//! provide over mutual TLS.
//!
//! Three things happen here and nowhere else in the admin path:
//!
//! 1. **Identity**, from the certificate exactly as on the client plane — a request field can
//!    never influence it (ADR-0012).
//! 2. **The admin allowlist.** The principal must appear verbatim in `[authz] admins`.
//!    Matching is exact: no prefixes, no wildcards, no case folding. A principal that is
//!    allowed to write keys is *not* thereby allowed to change membership.
//! 3. **The `admin_op` audit line**, emitted for every attempt, refusals included — a
//!    membership change that left no trail is the one an operator cannot explain afterwards.
//!
//! The service performs no consensus itself; it calls an [`AdminBackend`], which
//! `config-server` implements over a `ConfigNode`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use config_core::{
    Authorizer, ClusterId, ConfigError, NodeId, Principal, UNAVAILABLE_FEATURE_NOT_ACTIVATED,
};
use config_engine::admin::{AdminError, MembershipReport, SnapshotTriggered};
use config_engine::metrics::LogIdView;
use config_log::TraceContext;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::error::{mark_rejected, status_from_error};
use crate::pb;
use crate::pb::admin_service_server::{AdminService, AdminServiceServer};
use crate::tls::{principal_from_certs, TlsMode};

/// What a `Backup` produced on the server's filesystem (ADR-0024).
///
/// File **names**, not paths: the caller asked for a directory and knows what it asked for,
/// and echoing absolute server paths back over the wire leaks the node's layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupArtifact {
    /// Stem shared by the three files.
    pub name: String,
    /// `<name>.snap`.
    pub snapshot_file: String,
    /// `<name>.manifest.json`.
    pub manifest_file: String,
    /// `<name>.manifest.sig`.
    pub signature_file: String,
    /// Lowercase hex SHA-256 of the **plaintext** snapshot, exactly as the manifest records
    /// it — so a verifier that decrypts first can still check one number.
    pub sha256: String,
    /// Cluster revision the snapshot covers.
    pub revision: u64,
    /// Size of the snapshot file as written (encrypted size when encryption is on).
    pub size_bytes: u64,
    /// Whether the `.snap` on disk is AES-256-GCM ciphertext.
    pub encrypted: bool,
}

/// The node operations the admin plane needs.
///
/// A trait rather than a bare `ConfigNode` for the same reason [`crate::ClientBackend`] is
/// one: the transport's own behaviour — the allowlist, the status mapping, the audit line —
/// must be testable without standing up consensus.
#[async_trait]
pub trait AdminBackend: Send + Sync {
    /// The cluster this node is bound to, for `AddLearnerRequest.cluster_id`.
    fn cluster_id(&self) -> ClusterId;

    /// Everything this node knows about membership.
    fn membership_report(&self) -> MembershipReport;

    /// Add a learner at both of its endpoints.
    async fn add_learner(
        &self,
        node_id: NodeId,
        peer_endpoint: String,
        client_endpoint: String,
    ) -> Result<Option<LogIdView>, AdminError>;

    /// Promote a caught-up learner.
    async fn promote_voter(&self, node_id: NodeId) -> Result<Option<LogIdView>, AdminError>;

    /// Remove a member and fence its identity.
    async fn remove_member(&self, node_id: NodeId) -> Result<Option<LogIdView>, AdminError>;

    /// Build a snapshot now.
    async fn trigger_snapshot(&self) -> Result<SnapshotTriggered, AdminError>;

    /// Record one allowlist refusal on the node's counter (C5B-15).
    ///
    /// Defaulted to a no-op so a test backend need not care, exactly as
    /// `ClientBackend::record_authn_rejection` is. A real backend forwards it to
    /// `ConfigNode::record_admin_authz_denial`.
    fn record_authz_denial(&self) {}

    /// Write a signed backup triple into `dest_dir` **on this node**.
    async fn backup(
        &self,
        dest_dir: PathBuf,
        name: Option<String>,
    ) -> Result<BackupArtifact, AdminError>;

    /// Re-read and adopt the configured signed policy document now (M6, ADR-0027).
    ///
    /// Defaulted rather than required: a node in `authz.mode = "static"` has no policy files to
    /// re-read, and so does an embedder that never wired one. Both answer the caller honestly
    /// with [`UNAVAILABLE_FEATURE_NOT_ACTIVATED`] instead of forcing every implementor to write
    /// the same stub.
    async fn reload_policy(&self) -> Result<PolicyReload, AdminError> {
        Err(AdminError::Unavailable {
            reason: format!(
                "{UNAVAILABLE_FEATURE_NOT_ACTIVATED}: this node does not serve a signed policy \
                 document"
            ),
        })
    }

    /// Re-read the configured TLS PEM files now and serve what they hold (M6, ADR-0028).
    ///
    /// Defaulted for the same reason [`AdminBackend::reload_policy`] is: a node running
    /// `tls.mode = "insecure"` has no credentials to rotate, and neither has an embedder that
    /// never wired any.
    async fn reload_tls(&self) -> Result<Vec<TlsPlaneReload>, AdminError> {
        Err(AdminError::Unavailable {
            reason: format!(
                "{UNAVAILABLE_FEATURE_NOT_ACTIVATED}: this node does not serve TLS credentials"
            ),
        })
    }

    /// Take one step of a gossip key rotation on this node (M6, ADR-0028).
    ///
    /// `key_hex` arrives unparsed so that one definition of "a gossip key" — the daemon's, the
    /// same one its configuration file is validated against — decides what is accepted, rather
    /// than this crate growing a second.
    ///
    /// Defaulted: a node with gossip disabled, or with gossip unencrypted, has no keyring.
    async fn rotate_gossip_key(
        &self,
        _op: GossipKeyOp,
        _key_hex: &str,
        _force: bool,
    ) -> Result<GossipKeyringView, AdminError> {
        Err(AdminError::Unavailable {
            reason: format!(
                "{UNAVAILABLE_FEATURE_NOT_ACTIVATED}: this node runs no encrypted gossip"
            ),
        })
    }
}

/// What one plane's credentials are after a [`AdminBackend::reload_tls`] attempt.
///
/// One per plane rather than one per node: the planes hold separate credentials and can
/// legitimately end a reload in different states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsPlaneReload {
    /// `"client"`, `"peer"` or `"peer_dial"`.
    pub plane: &'static str,
    /// `"reloaded"` or `"unchanged"`.
    pub outcome: &'static str,
    /// How many times this plane's credentials have been replaced since the node started.
    pub generation: u64,
    /// Lowercase hex fingerprint of the served leaf certificate. Never the certificate.
    pub cert_fingerprint: String,
    /// The served leaf's `notAfter`, in seconds since the Unix epoch.
    pub cert_expiry_unix: i64,
}

/// Which step of a gossip key rotation to take (M6, ADR-0028).
///
/// The wire enum's `UNSPECIFIED` has no counterpart here on purpose: it is refused at the
/// handler, so a backend is never handed a step nobody chose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GossipKeyOp {
    /// Accept the key on receive.
    Add,
    /// Sign outgoing gossip with it.
    Use,
    /// Stop accepting it.
    Remove,
}

impl GossipKeyOp {
    /// The audit `op` this step is recorded under.
    ///
    /// One token per step rather than a shared `gossip_key_rotate`: the three differ in what
    /// they risk, and an audit trail that cannot tell "started accepting a key" from "stopped
    /// accepting one" is not an audit trail of a rotation.
    pub fn audit_op(self) -> &'static str {
        match self {
            Self::Add => "gossip_key_add",
            Self::Use => "gossip_key_use",
            Self::Remove => "gossip_key_remove",
        }
    }
}

/// A node's gossip keyring, in fingerprints (M6, ADR-0028).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipKeyringView {
    /// Fingerprint of the key outgoing gossip is signed with.
    pub primary_fingerprint: String,
    /// Fingerprints of every key accepted on receive, primary first.
    pub accepted_fingerprints: Vec<String>,
}

/// What one [`AdminBackend::reload_policy`] attempt left active.
///
/// A *refused* document is an `Err`, never one of these: "the reload failed and the old policy
/// is still serving" is not an outcome an operator should have to read out of a success field
/// (M6-13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyReload {
    /// The version active before the call, if any.
    pub from: Option<u64>,
    /// The version active now.
    pub to: u64,
    /// Lowercase hex SHA-256 of the active document's bytes.
    pub hash_hex: String,
    /// Whether the call changed anything: `"reloaded"` or `"unchanged"`.
    pub outcome: &'static str,
    /// Why nothing changed, on `"unchanged"`. Empty otherwise.
    pub reason: &'static str,
}

/// The admin allowlist (`[authz] admins`, ADR-0012 extended by ADR-0023 and ADR-0027).
///
/// An empty set means *no principal* may call the admin plane, which is the safe reading of
/// an absent configuration key: an admin surface that defaults to open is a surface nobody
/// remembered to close.
#[derive(Clone)]
pub struct AdminAllowlist(AdminSource);

/// Where the admin set is read from.
///
/// Two sources, never both. Under `authz.mode = "signed"` the configuration file's `[authz]
/// admins` is ignored entirely (M6-40): if a TOML key could add an admin, an attacker who can
/// write that file gets admin without touching the signed artifact, and the signature buys
/// nothing.
#[derive(Clone)]
enum AdminSource {
    /// `[authz] admins`, verbatim from the configuration file.
    Static(BTreeSet<String>),
    /// The `admins` list of the **currently active** signed document (OQ-58).
    ///
    /// Read through the authorizer on every call rather than captured once, so a rotation that
    /// removes an admin takes effect on the next request instead of at the next restart. With
    /// no valid document there is no admin set and the plane is closed — the same answer an
    /// empty allowlist gives, and the right one for a node that is already unready.
    Signed(Arc<dyn Authorizer>),
}

impl AdminAllowlist {
    /// Build from configured names.
    pub fn new(names: impl IntoIterator<Item = String>) -> Self {
        Self(AdminSource::Static(names.into_iter().collect()))
    }

    /// Take the admin set from the active signed policy document instead (M6, ADR-0027).
    pub fn from_signed_policy(authorizer: Arc<dyn Authorizer>) -> Self {
        Self(AdminSource::Signed(authorizer))
    }

    /// The names in force right now, or `None` when there is no admin set at all.
    fn names(&self) -> Option<Vec<String>> {
        match &self.0 {
            AdminSource::Static(names) => Some(names.iter().cloned().collect()),
            AdminSource::Signed(authorizer) => authorizer.admin_set(),
        }
    }

    /// Whether `principal` may call the admin plane. Exact match, by contract.
    pub fn permits(&self, principal: &Principal) -> bool {
        match &self.0 {
            AdminSource::Static(names) => names.contains(principal.name.as_str()),
            AdminSource::Signed(authorizer) => authorizer
                .admin_set()
                .is_some_and(|admins| admins.iter().any(|n| n == &principal.name)),
        }
    }

    /// How many principals are listed (for the startup line; never the names).
    pub fn len(&self) -> usize {
        self.names().map_or(0, |names| names.len())
    }

    /// Whether the admin plane is closed to everyone.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for AdminAllowlist {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// Counts and the source, never the names: the startup line and any `{:?}` an operator adds
/// later must not be a way to read the admin set out of a log (§15.2 redaction).
impl std::fmt::Debug for AdminAllowlist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let source = match &self.0 {
            AdminSource::Static(_) => "static",
            AdminSource::Signed(_) => "signed_policy",
        };
        f.debug_struct("AdminAllowlist")
            .field("source", &source)
            .field("len", &self.len())
            .finish()
    }
}

/// The `AdminService` implementation.
///
/// Public only so it can be named in [`crate::serve_client_plane`]'s signature, which is what
/// puts the admin surface on the client plane's listener (OQ-43). Construct it with
/// [`admin_service`].
pub struct AdminSvc {
    backend: Arc<dyn AdminBackend>,
    tls: TlsMode,
    cluster_id: ClusterId,
    admins: AdminAllowlist,
    /// See the client plane's field of the same name: hyper spawns each connection task, and
    /// `tokio::spawn` does not carry the caller's span.
    server_span: tracing::Span,
}

/// One audited admin attempt.
struct Audit {
    op: &'static str,
    target_node: u64,
}

impl AdminSvc {
    fn principal<T>(&self, request: &Request<T>) -> Result<Principal, Status> {
        match &self.tls {
            TlsMode::Insecure => Ok(Principal::development()),
            TlsMode::MutualTls(cfg) => match request.peer_certs() {
                Some(certs) if !certs.is_empty() => {
                    let der: Vec<&[u8]> = certs.iter().map(|c| c.as_ref()).collect();
                    principal_from_certs(&der, self.cluster_id, cfg.allow_common_name_principals)
                }
                _ => Err(Status::unauthenticated(
                    "mutual TLS is required on this listener but no client certificate was \
                     presented",
                )),
            },
        }
    }

    /// Identity → allowlist → call → one `admin_op` line, for every method.
    async fn dispatch<Res, Call, Fut>(
        &self,
        audit: Audit,
        request: Request<impl Sized>,
        call: Call,
    ) -> Result<Response<Res>, Status>
    where
        Call: FnOnce(Arc<dyn AdminBackend>, Principal) -> Fut,
        Fut: std::future::Future<Output = Result<Res, AdminError>>,
    {
        let started = Instant::now();
        let meta = request.metadata();
        let header = |k: &str| meta.get(k).and_then(|v| v.to_str().ok());
        let ctx = TraceContext::from_headers(
            header(config_log::HEADER_TRACE_ID),
            header(config_log::HEADER_PARENT_SPAN),
            header(config_log::HEADER_REQUEST_ID),
        );
        let span = self.server_span.in_scope(|| ctx.span(audit.op));
        let trace_id = ctx.trace_id.clone();

        let refuse = |principal: &str, reason: &'static str, status: Status| -> Status {
            span.in_scope(|| {
                tracing::warn!(
                    op = audit.op,
                    principal,
                    target_node = audit.target_node,
                    outcome = "rejected",
                    reason,
                    trace_id = %trace_id,
                    latency_ms = started.elapsed().as_millis() as u64,
                    detail = status.message(),
                    "admin_op"
                )
            });
            mark_rejected(status)
        };

        let principal = match self.principal(&request) {
            Ok(p) => p,
            Err(status) => return Err(refuse("", "unauthenticated", status)),
        };
        // The allowlist is checked before the handler runs, so a non-admin principal never
        // reaches consensus and never learns whether the id it named exists.
        if !self.admins.permits(&principal) {
            self.backend.record_authz_denial();
            return Err(refuse(
                principal.name.as_str(),
                "not_an_admin",
                Status::permission_denied(
                    "this principal is not listed in [authz] admins; the admin plane is \
                     authorized separately from the keyspace",
                ),
            ));
        }

        let result = call(Arc::clone(&self.backend), principal.clone())
            .instrument(span.clone())
            .await;
        let latency_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok(res) => {
                span.in_scope(|| {
                    tracing::info!(
                        op = audit.op,
                        principal = %principal.name,
                        target_node = audit.target_node,
                        outcome = "ok",
                        trace_id = %trace_id,
                        latency_ms,
                        "admin_op"
                    )
                });
                Ok(Response::new(res))
            }
            Err(e) => {
                let reason = e.reason();
                let status = status_from_error(&ConfigError::from(e));
                Err(refuse(principal.name.as_str(), reason, status))
            }
        }
    }
}

fn log_id_pb(id: Option<LogIdView>) -> Option<pb::AdminLogId> {
    id.map(|l| pb::AdminLogId {
        term: l.term,
        index: l.index,
    })
}

fn ack(node_id: NodeId, log_id: Option<LogIdView>) -> pb::AdminAck {
    pb::AdminAck {
        node_id: node_id.0,
        membership_log_id: log_id_pb(log_id),
    }
}

/// Flatten a [`MembershipReport`] onto the wire.
///
/// Endpoints come from the *effective* membership when it knows more than the committed one,
/// because a learner's address is exactly the thing an operator wants to read back while its
/// membership entry is still in flight; `voters` stays committed-only, because "who is in
/// this cluster" has one authoritative answer (ADR-0009).
fn report_pb(report: &MembershipReport) -> pb::MembershipReport {
    let endpoints = report
        .effective_endpoints
        .iter()
        .map(|(id, (peer, client))| pb::MemberEndpoints {
            node_id: id.0,
            peer: peer.clone(),
            client: client.clone(),
        })
        .collect();
    pb::MembershipReport {
        voters: report.membership.voters.iter().map(|n| n.0).collect(),
        learners: report.learners.iter().map(|n| n.0).collect(),
        joint_config_len: report.joint_config_len as u32,
        membership_log_id: report
            .membership
            .membership_log_id
            .map(|(term, index)| pb::AdminLogId { term, index }),
        retired: report.retired.iter().map(|n| n.0).collect(),
        replication: report
            .replication
            .iter()
            .map(|(id, p)| pb::ReplicationEntry {
                node_id: id.0,
                matched_index: p.matched_index,
                lag: p.lag,
            })
            .collect(),
        leader_last_log_index: report.leader_last_log_index,
        current_leader: report.current_leader.map(|n| n.0),
        authoritative: report.authoritative,
        endpoints,
        promote_max_lag: report.promote_max_lag,
    }
}

#[tonic::async_trait]
impl AdminService for AdminSvc {
    async fn get_membership(
        &self,
        request: Request<pb::GetMembershipRequest>,
    ) -> Result<Response<pb::MembershipReport>, Status> {
        self.dispatch(
            Audit {
                op: "get_membership",
                target_node: 0,
            },
            request,
            |backend, _| async move { Ok(report_pb(&backend.membership_report())) },
        )
        .await
    }

    async fn add_learner(
        &self,
        request: Request<pb::AddLearnerRequest>,
    ) -> Result<Response<pb::AdminAck>, Status> {
        let req = request.get_ref().clone();
        let node_id = NodeId(req.node_id);
        let expected = self.cluster_id;
        self.dispatch(
            Audit {
                op: "add_learner",
                target_node: req.node_id,
            },
            request,
            move |backend, _| async move {
                // Checked before anything is proposed: an operator pointed at the wrong
                // cluster gets a refusal, not a learner in the wrong place (ADR-0023).
                if !req.cluster_id.is_empty() {
                    match req.cluster_id.parse::<ClusterId>() {
                        Ok(id) if id == expected => {}
                        Ok(id) => {
                            return Err(AdminError::InvalidArgument {
                                detail: format!(
                                    "cluster_mismatch: this node serves {expected}, request \
                                     names {id}"
                                ),
                            })
                        }
                        Err(e) => {
                            return Err(AdminError::InvalidArgument {
                                detail: format!("cluster_mismatch: unparseable cluster id: {e}"),
                            })
                        }
                    }
                }
                let log_id = backend
                    .add_learner(node_id, req.peer_endpoint, req.client_endpoint)
                    .await?;
                Ok(ack(node_id, log_id))
            },
        )
        .await
    }

    async fn promote_voter(
        &self,
        request: Request<pb::NodeRef>,
    ) -> Result<Response<pb::AdminAck>, Status> {
        let node_id = NodeId(request.get_ref().node_id);
        self.dispatch(
            Audit {
                op: "promote_voter",
                target_node: node_id.0,
            },
            request,
            move |backend, _| async move {
                let log_id = backend.promote_voter(node_id).await?;
                Ok(ack(node_id, log_id))
            },
        )
        .await
    }

    async fn remove_member(
        &self,
        request: Request<pb::NodeRef>,
    ) -> Result<Response<pb::AdminAck>, Status> {
        let node_id = NodeId(request.get_ref().node_id);
        self.dispatch(
            Audit {
                op: "remove_member",
                target_node: node_id.0,
            },
            request,
            move |backend, _| async move {
                let log_id = backend.remove_member(node_id).await?;
                Ok(ack(node_id, log_id))
            },
        )
        .await
    }

    async fn trigger_snapshot(
        &self,
        request: Request<pb::TriggerSnapshotRequest>,
    ) -> Result<Response<pb::SnapshotInfo>, Status> {
        self.dispatch(
            Audit {
                op: "trigger_snapshot",
                target_node: 0,
            },
            request,
            |backend, _| async move {
                Ok(match backend.trigger_snapshot().await? {
                    SnapshotTriggered::Started {
                        snapshot_id,
                        last_log_id,
                    } => pb::SnapshotInfo {
                        snapshot_id: snapshot_id.unwrap_or_default(),
                        last_log_id: log_id_pb(last_log_id),
                        already_in_progress: false,
                    },
                    SnapshotTriggered::AlreadyInProgress => pb::SnapshotInfo {
                        snapshot_id: String::new(),
                        last_log_id: None,
                        already_in_progress: true,
                    },
                })
            },
        )
        .await
    }

    async fn backup(
        &self,
        request: Request<pb::BackupRequest>,
    ) -> Result<Response<pb::BackupInfo>, Status> {
        let req = request.get_ref().clone();
        self.dispatch(
            Audit {
                op: "backup",
                target_node: 0,
            },
            request,
            move |backend, _| async move {
                if req.dest_dir.trim().is_empty() {
                    return Err(AdminError::InvalidArgument {
                        detail: "dest_dir_required: Backup writes on the server's filesystem \
                                 and needs a directory there"
                            .to_string(),
                    });
                }
                let name = Some(req.name).filter(|n| !n.trim().is_empty());
                let a = backend.backup(PathBuf::from(req.dest_dir), name).await?;
                Ok(pb::BackupInfo {
                    name: a.name,
                    snapshot_file: a.snapshot_file,
                    manifest_file: a.manifest_file,
                    signature_file: a.signature_file,
                    sha256: a.sha256,
                    revision: a.revision,
                    size_bytes: a.size_bytes,
                    encrypted: a.encrypted,
                })
            },
        )
        .await
    }

    /// M6-12: admin-only, immediate, and audited on both outcomes.
    ///
    /// The allowlist check in [`AdminSvc::dispatch`] has already run against the **currently
    /// active** document, never the incoming one (OQ-58). That ordering is the whole security
    /// property: a document that adds its own author to `admins` cannot be the document that
    /// authorizes its own adoption.
    async fn reload_policy(
        &self,
        request: Request<pb::ReloadPolicyRequest>,
    ) -> Result<Response<pb::PolicyInfo>, Status> {
        self.dispatch(
            Audit {
                op: "reload_policy",
                target_node: 0,
            },
            request,
            |backend, _| async move {
                let reload = backend.reload_policy().await?;
                Ok(pb::PolicyInfo {
                    from_version: reload.from,
                    version: reload.to,
                    hash_hex: reload.hash_hex,
                    outcome: reload.outcome.to_string(),
                    reason: reload.reason.to_string(),
                })
            },
        )
        .await
    }

    /// M6-42: admin-only, immediate, and reported per plane.
    ///
    /// Carries no payload, so there is nothing here to validate: the node reloads the paths its
    /// own configuration names. A reload that no plane could serve leaves every plane on the
    /// material it already had and answers with an error, never with a success naming what did
    /// not happen.
    async fn reload_tls(
        &self,
        request: Request<pb::ReloadTlsRequest>,
    ) -> Result<Response<pb::TlsInfo>, Status> {
        self.dispatch(
            Audit {
                op: "reload_tls",
                target_node: 0,
            },
            request,
            |backend, _| async move {
                let planes = backend.reload_tls().await?;
                Ok(pb::TlsInfo {
                    planes: planes
                        .into_iter()
                        .map(|p| pb::TlsPlaneInfo {
                            plane: p.plane.to_string(),
                            outcome: p.outcome.to_string(),
                            generation: p.generation,
                            cert_fingerprint: p.cert_fingerprint,
                            cert_expiry_unix: p.cert_expiry_unix,
                        })
                        .collect(),
                })
            },
        )
        .await
    }

    /// M6-57..M6-59: admin-only, node-local, and audited per step.
    ///
    /// The step is read before [`AdminSvc::dispatch`] runs so the audit line names which step
    /// was attempted even when the caller is refused. An unset step is refused here rather than
    /// defaulted, because every default would be someone's wrong guess: adding a key is
    /// harmless, promoting one can partition the cluster.
    ///
    /// `key_hex` is never logged, never echoed in an error and never audited — the fingerprints
    /// in the reply are what an operator follows a rotation by (ADR-0028).
    async fn rotate_gossip_key(
        &self,
        request: Request<pb::RotateGossipKeyRequest>,
    ) -> Result<Response<pb::GossipKeyringInfo>, Status> {
        let op = match pb::GossipKeyOp::try_from(request.get_ref().op) {
            Ok(pb::GossipKeyOp::Add) => GossipKeyOp::Add,
            Ok(pb::GossipKeyOp::Use) => GossipKeyOp::Use,
            Ok(pb::GossipKeyOp::Remove) => GossipKeyOp::Remove,
            Ok(pb::GossipKeyOp::Unspecified) | Err(_) => {
                return Err(mark_rejected(Status::invalid_argument(
                    "unknown_gossip_key_op: op must be one of add, use or remove",
                )))
            }
        };
        let key_hex = request.get_ref().key_hex.clone();
        let force = request.get_ref().force;
        self.dispatch(
            Audit {
                op: op.audit_op(),
                target_node: 0,
            },
            request,
            move |backend, _| async move {
                let keyring = backend.rotate_gossip_key(op, &key_hex, force).await?;
                Ok(pb::GossipKeyringInfo {
                    primary_fingerprint: keyring.primary_fingerprint,
                    accepted_fingerprints: keyring.accepted_fingerprints,
                })
            },
        )
        .await
    }
}

/// Build the tonic service so it can be added to the **client plane's** router (OQ-43).
///
/// Returned rather than served, because co-location is the whole point: the admin surface
/// shares the client plane's listener, its certificate profile and its cluster binding, and
/// differs from it only by the allowlist checked above.
pub fn admin_service(
    backend: Arc<dyn AdminBackend>,
    tls: TlsMode,
    cluster_id: ClusterId,
    admins: AdminAllowlist,
) -> AdminServiceServer<AdminSvc> {
    AdminServiceServer::new(AdminSvc {
        backend,
        tls,
        cluster_id,
        admins,
        server_span: tracing::Span::current(),
    })
}

/// Serve `AdminService` alone on an already-bound listener.
///
/// Only for tests and for an embedder that deliberately wants a separate port; production
/// co-locates it on the client plane ([`admin_service`], OQ-43).
pub fn serve_admin_plane(
    backend: Arc<dyn AdminBackend>,
    listener: tokio::net::TcpListener,
    tls: TlsMode,
    cluster_id: ClusterId,
    admins: AdminAllowlist,
) -> Result<crate::server::ServerHandle, crate::error::GrpcError> {
    let svc = admin_service(backend, tls.clone(), cluster_id, admins);
    let router = tonic::transport::Server::builder().add_service(svc);
    crate::server::spawn("admin", router, listener, &tls)
}

/// A backup path check shared by the plane and the CLI: refuse anything that is not a
/// directory that already exists.
///
/// Deliberately not "create it if missing": `Backup` takes a path from the network, and a
/// server that happily creates directories anywhere a caller names is a file-system write
/// primitive with an admin allowlist in front of it.
pub fn check_backup_dir(dir: &Path) -> Result<(), AdminError> {
    if dir.is_dir() {
        Ok(())
    } else {
        Err(AdminError::InvalidArgument {
            detail: format!(
                "dest_dir_not_a_directory: {} does not exist on this node",
                dir.display()
            ),
        })
    }
}
