//! The node configuration file (m3-architecture §4, ADR-0018 §1).
//!
//! One TOML document describes the node completely. Nothing is read from the environment
//! (TA-11), and every path in it is resolved relative to the *configuration file's own
//! directory* so a test can write a self-contained node directory and move it.
//!
//! Validation happens once, before anything is opened: an invalid document, a non-loopback
//! health address, or an insecure TLS mode without `--allow-insecure-dev` is exit code 2 and
//! no listener is ever bound (ADR-0018 §5).

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use config_core::{
    ClusterId, ClusterIdentity, DedupLimits, NodeId, RecoveryEpoch, WatchLimits, WatchRetention,
};
use serde::Deserialize;

/// Why a configuration document was refused. Every variant is exit code 2.
#[derive(Debug, thiserror::Error)]
pub enum ConfigFileError {
    /// The file could not be read.
    #[error("cannot read config file {path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// The document is not valid TOML, or does not match the schema.
    #[error("invalid config file {path}: {detail}")]
    Parse {
        /// The file.
        path: PathBuf,
        /// The parser's description.
        detail: String,
    },
    /// The document parsed but says something impossible.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

/// `[node]` — the identity this data directory is permanently bound to (ADR-0011).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSection {
    /// This node's id.
    pub node_id: u64,
    /// The cluster, as 32 lowercase hex characters.
    pub cluster_id: String,
    /// The recovery epoch. `0` for a fresh cluster.
    #[serde(default)]
    pub recovery_epoch: u32,
    /// RocksDB data directory.
    pub data_dir: PathBuf,
}

/// `[listen]` — the three listeners. Port `0` binds an ephemeral port, which the ready line
/// then reports.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenSection {
    /// Raft peer plane.
    pub peer: String,
    /// Client plane.
    pub client: String,
    /// Advisory gossip. Absent means this node runs no gossip at all.
    #[serde(default)]
    pub gossip: Option<String>,
}

/// How a plane is protected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsModeName {
    /// Mutual TLS: the production profile.
    Mutual,
    /// Plain TCP. Accepted only behind `--allow-insecure-dev`.
    Insecure,
}

/// `[tls]` — transport security for both planes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    /// `"mutual"` or `"insecure"`.
    pub mode: TlsModeName,
    /// Trust anchor PEM. Required for `mutual`.
    #[serde(default)]
    pub ca: Option<PathBuf>,
    /// This node's certificate chain PEM. Required for `mutual`.
    #[serde(default)]
    pub cert: Option<PathBuf>,
    /// This node's private key PEM. Required for `mutual`.
    #[serde(default)]
    pub key: Option<PathBuf>,
    /// Let a client certificate that asserts no `retcd://` SAN authenticate under its Common
    /// Name (client plane only, ADR-0012). Off unless written, and it is written only for a CA
    /// that cannot mint URI SANs: a Common Name carries no cluster id, so with this on a
    /// CN-only certificate minted by the shared CA for *another* cluster authenticates here
    /// under that name.
    #[serde(default)]
    pub allow_common_name_principals: bool,
}

/// `[authz]` — the static allowlist policy file (ADR-0012).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthzSection {
    /// Path to the allowlist TOML. Absent, unreadable or unparsable means the node starts
    /// unready unless `--dev-allow-all` was given (ADR-0018 §6).
    #[serde(default)]
    pub policy: Option<PathBuf>,
    /// Principals allowed to call the admin plane (M5, ADR-0023, OQ-43).
    ///
    /// A separate list from the data-plane policy because the two answer different questions:
    /// the policy says who may read and write keys, this says who may change the shape of the
    /// cluster. Absent means *nobody*, and `--dev-allow-all` does **not** open it — an
    /// allow-all development gate for keys is a different risk from an allow-all gate for
    /// `RemoveMember`.
    #[serde(default)]
    pub admins: Vec<String>,
    /// Which authorization model this node serves (M6, ADR-0027). Default: `static`.
    #[serde(default)]
    pub mode: AuthzModeName,
    /// The signed policy document. Required by `mode = "signed"`.
    #[serde(default)]
    pub policy_file: Option<PathBuf>,
    /// The detached signature envelope. Defaults to `policy_file` with `.sig` appended.
    #[serde(default)]
    pub signature_file: Option<PathBuf>,
    /// Public keys a document may be signed by. Required, and non-empty, by `mode = "signed"`.
    #[serde(default)]
    pub trust_keys: Vec<TrustKeyEntry>,
    /// How often the files are re-read. Default 10 s (D6.1).
    #[serde(default)]
    pub poll_interval_secs: Option<u64>,
}

/// `[authz] mode` (M6, ADR-0027).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthzModeName {
    /// M3's `StaticAllowlist` over `[authz] policy`. The default, so the M3 release stays
    /// reproducible byte for byte (M6-36).
    #[default]
    Static,
    /// The signed document of ADR-0027.
    Signed,
}

/// One entry of `[authz] trust_keys` (M6, ADR-0027).
///
/// A *set*, not a single key: a rotation needs the old and the new signer to both verify for
/// as long as documents signed by either may still be deployed (M6-06).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustKeyEntry {
    /// The name the signature envelope selects this key by. Unique within the list.
    pub name: String,
    /// The ed25519 public key as 64 lowercase hex characters.
    ///
    /// Inline rather than a path: a public key is not a secret, and a key the operator can read
    /// in the same file as the mode it enables is a key they are likelier to review.
    pub public_key: String,
}

/// Everything `authz.mode = "signed"` needs, validated (M6, ADR-0027).
///
/// Its mere existence is the mode: a node that reached `run` with `Some(_)` here has a policy
/// file, a signature path and at least one parsed trust key, because the alternative was exit
/// code 2 (M6-37). A node that starts in signed mode with no trust keys would fail closed on
/// every request — an outage disguised as a configuration nicety.
#[derive(Debug, Clone)]
pub struct SignedPolicyConfig {
    /// The document.
    pub policy_file: PathBuf,
    /// The detached signature envelope.
    pub signature_file: PathBuf,
    /// Trusted signers, by envelope key name. Non-empty, and free of duplicate names.
    pub trust_keys: Vec<(String, config_core::VerifyingKey)>,
    /// How often the files are re-read (D6.1's bounded polling).
    pub poll_interval: Duration,
}

/// `[membership]` — the learner lifecycle knobs (M5, ADR-0023).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipSection {
    /// How far behind the leader's last log index a learner may still be and be promoted.
    ///
    /// Evaluated live on the leader at `PromoteVoter` time (A5/OQ-50); it is not a timer and
    /// not a stored value, so changing it changes the next promotion and nothing else.
    #[serde(default)]
    pub promote_max_lag: Option<u64>,
}

/// `[snapshot]` — build and purge policy (M5, ADR-0022).
///
/// The three OpenRaft knobs move together or not at all; `config_storage::SnapshotConfig`
/// refuses a half-applied change rather than silently doing nothing, which is why this section
/// is validated as a unit instead of field by field.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSection {
    /// Build a snapshot once committed has advanced this far past the current one. `0`
    /// disables snapshotting, which also requires `logs_to_keep` to be absent.
    #[serde(default)]
    pub logs_since_last: Option<u64>,
    /// How many snapshot-covered log entries to retain rather than purge.
    #[serde(default)]
    pub logs_to_keep: Option<u64>,
    /// Minimum number of entries a purge must be able to remove before one is scheduled.
    #[serde(default)]
    pub purge_batch_size: Option<u64>,
    /// How many published `.snap` files to keep, including the current one.
    #[serde(default)]
    pub retain_snapshots: Option<usize>,
}

/// `[metrics]` — the Prometheus endpoint on the health listener (M5, ADR-0026).
///
/// There is no separate address: `/metrics` is served by the same loopback listener as
/// `/health`, under the same posture (ADR-0018 §2). A deployment that wants it scraped from
/// off-box fronts it with its own proxy rather than having the daemon open a second port.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsSection {
    /// Serve `GET /metrics`. Absent is `true`: the endpoint carries no key material and its
    /// labels are the ADR-0026 allowlist, so an operator who configured a health listener has
    /// already accepted the surface it is served on.
    #[serde(default)]
    pub enabled: Option<bool>,
}

/// `[dedup]` — bounded request deduplication (M5, ADR-0025).
///
/// **Replicated policy, not a node-local knob.** The window and the cap decide whether a
/// resubmission is a hit, a fresh application, or an `InvalidArgument`, and that decision is
/// made inside the state machine. Every voter must therefore carry the same three values;
/// two voters configured differently would answer the same duplicate differently and diverge.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DedupSection {
    /// Retain deduplication records at all. Absent is `false` — the conservative default
    /// (M5-108): the `dedup` column family stays empty and the node reports
    /// `Dedup::Unsupported`, which is exactly how an M4 build behaves.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Retained request ids per `(principal, client_id)`.
    #[serde(default)]
    pub window_requests: Option<u32>,
    /// Retained records across every principal and client id combined.
    #[serde(default)]
    pub max_records: Option<u64>,
}

/// `[backup]` — key material for the backup artifact triple (M5, ADR-0024).
///
/// Every file is raw bytes: a 32-byte Ed25519 signing seed, a 32-byte Ed25519 verifying key,
/// a 32-byte AES-256 key. Nothing here is ever logged; the daemon reports only whether a key
/// is configured.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupSection {
    /// Signs `<name>.manifest.json`. Required for the admin plane's `Backup` RPC.
    #[serde(default)]
    pub signing_key_file: Option<PathBuf>,
    /// Verifies a manifest signature. Only the CLI uses it; the daemon never verifies its own.
    #[serde(default)]
    pub trust_key_file: Option<PathBuf>,
    /// Encrypts the `.snap` with AES-256-GCM. Absent means the snapshot is written in
    /// plaintext and the Ed25519 signature protects integrity only, not secrecy.
    #[serde(default)]
    pub encryption_key_file: Option<PathBuf>,
}

/// The backup key material, with every path resolved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackupKeys {
    /// Ed25519 signing seed.
    pub signing_key: Option<PathBuf>,
    /// Ed25519 verifying key.
    pub trust_key: Option<PathBuf>,
    /// AES-256 key.
    pub encryption_key: Option<PathBuf>,
}

/// `[manifest]` — the signed bootstrap manifest, required for `--form`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSection {
    /// The signed document.
    pub path: PathBuf,
    /// The detached Ed25519 signature (64 raw bytes).
    pub sig: PathBuf,
    /// The signing public key (32 raw bytes).
    pub signing_key_pub: PathBuf,
}

/// `[raft]` — timers. Omitted fields keep the Windows-safe engine defaults.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftSection {
    /// Leader heartbeat interval, milliseconds.
    #[serde(default)]
    pub heartbeat_ms: Option<u64>,
    /// Minimum election timeout, milliseconds.
    #[serde(default)]
    pub election_min_ms: Option<u64>,
    /// Maximum election timeout, milliseconds.
    #[serde(default)]
    pub election_max_ms: Option<u64>,
}

/// `[gossip]` — advisory gossip (ADR-0003). Nothing here can change membership.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GossipSection {
    /// Seed gossip addresses contacted at startup. Unreachable seeds are warnings.
    #[serde(default)]
    pub seeds: Vec<String>,
    /// AES-256 gossip key as 64 hex characters. Absent disables encryption, which is
    /// single-host development only (spec §15.1).
    #[serde(default)]
    pub secret_key_hex: Option<String>,
}

/// `[watch]` — watch delivery caps (M4, ADR-0020). Omitted fields keep the engine defaults.
///
/// These are *caps*, not sizing hints: a stream that exceeds one is terminated rather than
/// allowed to grow, because the memory a watcher can pin on this node has to be bounded by
/// something the operator chose (spec §11.4).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchSection {
    /// Concurrent watch streams this node will serve at once.
    #[serde(default)]
    pub max_streams_per_node: Option<u32>,
    /// Concurrent watch streams one principal will be served at once.
    #[serde(default)]
    pub max_streams_per_principal: Option<u32>,
    /// Undelivered events one stream may hold before it is terminated.
    #[serde(default)]
    pub queue_events: Option<u32>,
    /// Undelivered event bytes one stream may hold before it is terminated.
    #[serde(default)]
    pub queue_bytes: Option<u64>,
    /// Applied batches the node fans out to watchers before a slow one is declared lagged.
    #[serde(default)]
    pub live_buffer_batches: Option<u32>,
    /// Default progress-frame interval for streams that do not ask for one, milliseconds.
    #[serde(default)]
    pub progress_interval_ms: Option<u64>,
}

/// `[list]` — revision-pinned pagination (M6, ADR-0029). Omitted fields keep the defaults.
///
/// The two numbers bound what a walk can cost this node: how many snapshots may be held open
/// at once, and how long one may sit idle before it is released. They are caps rather than
/// guarantees — a client whose walk is evicted or expires is told so and restarts it — because
/// the alternative is letting an abandoned walk pin state until the process dies (§19.12).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSection {
    /// Pinned snapshots held open on this node at once, across all clients.
    #[serde(default)]
    pub max_pinned_snapshots: Option<u32>,
    /// How long a pinned snapshot survives without being read, seconds.
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
    /// File holding the 32-byte page-token HMAC key, as 64 hex characters.
    ///
    /// Absent means a fresh random key per process, which is the honest default: a page token
    /// is already bound to the node that minted it and to that process's start time, so it
    /// could not outlive a restart even with a stable key. Naming a file is for an operator
    /// who wants the key under their own rotation policy, not for continuity across restarts.
    #[serde(default)]
    pub token_key_file: Option<PathBuf>,
}

/// `[retention]` — when the leader proposes a compaction (M4, ADR-0019).
///
/// Every field is a ceiling on the *retained* event journal, and the leader compacts to the
/// oldest revision that satisfies all of them. Leaving the section out keeps the engine
/// defaults; setting a field to `0` disables that particular ceiling, which is the only way to
/// say "never compact for this reason" without also saying it for the others.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionSection {
    /// Oldest retained event age, seconds.
    #[serde(default)]
    pub max_age_secs: Option<u64>,
    /// Retained event count.
    #[serde(default)]
    pub max_revisions: Option<u64>,
    /// Retained event bytes.
    #[serde(default)]
    pub max_bytes: Option<u64>,
    /// How often the leader evaluates the ceilings, seconds.
    #[serde(default)]
    pub check_interval_secs: Option<u64>,
}

/// The whole configuration document, as written.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfigFile {
    /// `[node]`.
    pub node: NodeSection,
    /// `[listen]`.
    pub listen: ListenSection,
    /// `[tls]`.
    pub tls: TlsSection,
    /// `[authz]`.
    #[serde(default)]
    pub authz: AuthzSection,
    /// `[manifest]`.
    #[serde(default)]
    pub manifest: Option<ManifestSection>,
    /// `[raft]`.
    #[serde(default)]
    pub raft: RaftSection,
    /// `[gossip]`.
    #[serde(default)]
    pub gossip: GossipSection,
    /// `[watch]`.
    #[serde(default)]
    pub watch: WatchSection,
    /// `[retention]`.
    #[serde(default)]
    pub retention: RetentionSection,
    /// `[list]`.
    #[serde(default)]
    pub list: ListSection,
    /// `[membership]`.
    #[serde(default)]
    pub membership: MembershipSection,
    /// `[snapshot]`.
    #[serde(default)]
    pub snapshot: SnapshotSection,
    /// `[backup]`.
    #[serde(default)]
    pub backup: BackupSection,
    /// `[metrics]`.
    #[serde(default)]
    pub metrics: MetricsSection,
    /// `[dedup]`.
    #[serde(default)]
    pub dedup: DedupSection,
}

/// The validated configuration the daemon actually runs on.
///
/// Every string that had to parse has parsed, every relative path has been resolved against
/// the configuration file's directory, and every gate has been checked. Nothing downstream
/// re-validates.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// This node's identity.
    pub identity: ClusterIdentity,
    /// RocksDB data directory.
    pub data_dir: PathBuf,
    /// Peer-plane bind address.
    pub peer_listen: SocketAddr,
    /// Client-plane bind address.
    pub client_listen: SocketAddr,
    /// Gossip bind address, when gossip is configured.
    pub gossip_listen: Option<SocketAddr>,
    /// Health bind address, when `--health-listen` was given. Always loopback.
    pub health_listen: Option<SocketAddr>,
    /// Transport security profile.
    pub tls_mode: TlsModeName,
    /// PEM material for `mutual`.
    pub tls_material: Option<TlsMaterial>,
    /// The allowlist policy file, if the document names one.
    pub policy_path: Option<PathBuf>,
    /// The signed-policy configuration, present only under `authz.mode = "signed"` (M6).
    pub signed_policy: Option<SignedPolicyConfig>,
    /// The bootstrap manifest files, if the document names them.
    pub manifest: Option<ManifestFiles>,
    /// Raft timers.
    pub raft: config_engine::RaftTimers,
    /// Gossip seeds.
    pub gossip_seeds: Vec<SocketAddr>,
    /// Gossip encryption key.
    pub gossip_secret_key: Option<[u8; 32]>,
    /// Watch delivery caps, folded into the limits this node enforces.
    pub watch_limits: WatchLimits,
    /// Default progress-frame interval for streams that do not ask for one.
    pub watch_progress_interval: Duration,
    /// Journal retention ceilings the leader compacts against.
    pub retention: WatchRetention,
    /// Principals permitted on the admin plane (M5, OQ-43). Empty means the plane is closed.
    pub admins: Vec<String>,
    /// Promotion catch-up bound (M5, A5/OQ-50).
    pub promote_max_lag: u64,
    /// Snapshot build and purge policy (M5, ADR-0022).
    pub snapshot: config_storage::SnapshotConfig,
    /// Backup key material (M5, ADR-0024).
    pub backup: BackupKeys,
    /// Whether the health listener also serves `GET /metrics` (M5, ADR-0026).
    pub metrics_enabled: bool,
    /// Bounded deduplication policy, folded into the limits this node enforces (M5,
    /// ADR-0025).
    pub dedup: DedupLimits,
    /// Revision-pinned pagination policy (M6, ADR-0029).
    ///
    /// Parsed and validated here; `run.rs` builds the node's one `Paginator` from it.
    pub list: config_engine::PaginationConfig,
}

/// PEM bytes for the mutual-TLS profile, read once at validation time.
#[derive(Clone, PartialEq, Eq)]
pub struct TlsMaterial {
    /// Trust anchor.
    pub ca_pem: Vec<u8>,
    /// This node's chain.
    pub cert_pem: Vec<u8>,
    /// This node's private key.
    pub key_pem: Vec<u8>,
    /// `tls.allow_common_name_principals`, carried here because it describes the same mutual
    /// profile the PEM does and is meaningless without one: it becomes
    /// [`config_grpc::MtlsConfig::allow_common_name_principals`] on the client plane.
    pub allow_common_name_principals: bool,
}

/// `Debug` prints sizes, never key bytes.
impl std::fmt::Debug for TlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsMaterial")
            .field("ca_pem_bytes", &self.ca_pem.len())
            .field("cert_pem_bytes", &self.cert_pem.len())
            .field("key_pem_bytes", &self.key_pem.len())
            .field(
                "allow_common_name_principals",
                &self.allow_common_name_principals,
            )
            .finish()
    }
}

/// Resolved paths of the three bootstrap-manifest files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestFiles {
    /// The signed document.
    pub manifest: PathBuf,
    /// The detached signature.
    pub signature: PathBuf,
    /// The signing public key.
    pub public_key: PathBuf,
}

/// Read and validate a configuration file.
///
/// `allow_insecure_dev` is passed in rather than read here so that the gate and the document
/// are refused by the same code path, and the refusal names the flag that would have allowed
/// it (E2E-12).
pub fn load(
    path: &Path,
    health_listen: Option<&str>,
    allow_insecure_dev: bool,
) -> Result<ServerConfig, ConfigFileError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigFileError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let file: ServerConfigFile = toml::from_str(&text).map_err(|e| ConfigFileError::Parse {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    validate(file, base, health_listen, allow_insecure_dev)
}

fn validate(
    file: ServerConfigFile,
    base: &Path,
    health_listen: Option<&str>,
    allow_insecure_dev: bool,
) -> Result<ServerConfig, ConfigFileError> {
    let cluster_id = ClusterId::from_str(&file.node.cluster_id)
        .map_err(|e| ConfigFileError::Invalid(e.to_string()))?;
    if file.node.node_id == 0 {
        return Err(ConfigFileError::Invalid(
            "node.node_id must be non-zero; 0 is OpenRaft's \"no node\" sentinel".into(),
        ));
    }
    let identity = ClusterIdentity {
        cluster_id,
        recovery_epoch: RecoveryEpoch(file.node.recovery_epoch),
        node_id: NodeId(file.node.node_id),
    };

    let peer_listen = socket_addr("listen.peer", &file.listen.peer)?;
    let client_listen = socket_addr("listen.client", &file.listen.client)?;
    let gossip_listen = file
        .listen
        .gossip
        .as_deref()
        .map(|a| socket_addr("listen.gossip", a))
        .transpose()?;

    let health_listen = health_listen
        .map(|a| socket_addr("--health-listen", a))
        .transpose()?;
    if let Some(addr) = health_listen {
        if !is_loopback(&addr.ip()) {
            return Err(ConfigFileError::Invalid(format!(
                "--health-listen {addr} is not a loopback address; the health endpoint is \
                 plaintext and is a local oracle, not a remote surface (ADR-0018 §2)"
            )));
        }
    }

    if file.tls.mode == TlsModeName::Insecure && !allow_insecure_dev {
        return Err(ConfigFileError::Invalid(
            "tls.mode = \"insecure\" requires --allow-insecure-dev; refusing to serve an \
             unauthenticated cluster by accident (ADR-0010)"
                .into(),
        ));
    }

    let tls_material = match file.tls.mode {
        TlsModeName::Insecure => None,
        TlsModeName::Mutual => {
            let ca = required_path("tls.ca", base, file.tls.ca.as_deref())?;
            let cert = required_path("tls.cert", base, file.tls.cert.as_deref())?;
            let key = required_path("tls.key", base, file.tls.key.as_deref())?;
            if file.tls.allow_common_name_principals {
                // Not a refusal — it is a supported profile for a CA that cannot mint URI SANs
                // — but it narrows the cluster binding the rest of ADR-0011/ADR-0012 rests on,
                // so it must be visible in the log rather than only in the file, exactly like
                // `insecure_transport_enabled` (ADR-0010).
                tracing::warn!(
                    detail = "tls.allow_common_name_principals = true; a client certificate \
                              asserting no retcd:// SAN is served under its Common Name, which \
                              carries no cluster id — a CN-only certificate the shared CA \
                              minted for another cluster authenticates here (ADR-0012)",
                    "common_name_principals_enabled"
                );
            }
            Some(TlsMaterial {
                ca_pem: read_bytes("tls.ca", &ca)?,
                cert_pem: read_bytes("tls.cert", &cert)?,
                key_pem: read_bytes("tls.key", &key)?,
                allow_common_name_principals: file.tls.allow_common_name_principals,
            })
        }
    };

    let manifest = file.manifest.as_ref().map(|m| ManifestFiles {
        manifest: resolve(base, &m.path),
        signature: resolve(base, &m.sig),
        public_key: resolve(base, &m.signing_key_pub),
    });

    let mut raft = config_engine::RaftTimers::default();
    if let Some(v) = file.raft.heartbeat_ms {
        raft.heartbeat_ms = v;
    }
    if let Some(v) = file.raft.election_min_ms {
        raft.election_min_ms = v;
    }
    if let Some(v) = file.raft.election_max_ms {
        raft.election_max_ms = v;
    }
    if !(raft.heartbeat_ms < raft.election_min_ms && raft.election_min_ms < raft.election_max_ms) {
        return Err(ConfigFileError::Invalid(format!(
            "raft timers must satisfy heartbeat_ms < election_min_ms < election_max_ms, got \
             {}/{}/{}",
            raft.heartbeat_ms, raft.election_min_ms, raft.election_max_ms
        )));
    }

    let mut gossip_seeds = Vec::with_capacity(file.gossip.seeds.len());
    for seed in &file.gossip.seeds {
        gossip_seeds.push(socket_addr("gossip.seeds", seed)?);
    }
    let gossip_secret_key = file
        .gossip
        .secret_key_hex
        .as_deref()
        .map(parse_gossip_key)
        .transpose()?;

    let mut watch_limits = WatchLimits::default();
    if let Some(v) = file.watch.max_streams_per_node {
        watch_limits.max_streams_per_node = v;
    }
    if let Some(v) = file.watch.max_streams_per_principal {
        watch_limits.max_streams_per_principal = v;
    }
    if let Some(v) = file.watch.queue_events {
        watch_limits.queue_events = v;
    }
    if let Some(v) = file.watch.queue_bytes {
        watch_limits.queue_bytes = v;
    }
    if let Some(v) = file.watch.live_buffer_batches {
        watch_limits.live_buffer_batches = v;
    }
    // A zero anywhere here is a configuration that cannot serve a single watcher, and it is
    // far better to refuse at startup than to have every `Watch` fail at runtime with a limit
    // nobody meant to set.
    for (key, value) in [
        (
            "max_streams_per_node",
            u64::from(watch_limits.max_streams_per_node),
        ),
        (
            "max_streams_per_principal",
            u64::from(watch_limits.max_streams_per_principal),
        ),
        ("queue_events", u64::from(watch_limits.queue_events)),
        ("queue_bytes", watch_limits.queue_bytes),
        (
            "live_buffer_batches",
            u64::from(watch_limits.live_buffer_batches),
        ),
    ] {
        if value == 0 {
            return Err(ConfigFileError::Invalid(format!(
                "watch.{key} must be greater than zero"
            )));
        }
    }
    if watch_limits.max_streams_per_principal > watch_limits.max_streams_per_node {
        return Err(ConfigFileError::Invalid(format!(
            "watch.max_streams_per_principal ({}) cannot exceed watch.max_streams_per_node ({})",
            watch_limits.max_streams_per_principal, watch_limits.max_streams_per_node
        )));
    }

    let watch_progress_interval = match file.watch.progress_interval_ms {
        None => config_engine::DEFAULT_PROGRESS_INTERVAL,
        Some(ms) => {
            let interval = Duration::from_millis(ms);
            if interval < config_engine::MIN_PROGRESS_INTERVAL
                || interval > config_engine::MAX_PROGRESS_INTERVAL
            {
                return Err(ConfigFileError::Invalid(format!(
                    "watch.progress_interval_ms must be between {} and {}, got {ms}",
                    config_engine::MIN_PROGRESS_INTERVAL.as_millis(),
                    config_engine::MAX_PROGRESS_INTERVAL.as_millis()
                )));
            }
            interval
        }
    };

    let mut retention = WatchRetention::default();
    if let Some(v) = file.retention.max_age_secs {
        retention.max_age = Duration::from_secs(v);
    }
    if let Some(v) = file.retention.max_revisions {
        retention.max_revisions = v;
    }
    if let Some(v) = file.retention.max_bytes {
        retention.max_bytes = v;
    }
    if let Some(v) = file.retention.check_interval_secs {
        if v == 0 {
            return Err(ConfigFileError::Invalid(
                "retention.check_interval_secs must be greater than zero".to_string(),
            ));
        }
        retention.check_interval = Duration::from_secs(v);
    }

    // An admin principal named twice, or named empty, is a configuration mistake worth
    // surfacing: the allowlist is an exact-match set, so a duplicate is silently absorbed and
    // an empty name can never match a certificate subject.
    let mut admins = file.authz.admins.clone();
    admins.sort();
    if admins.iter().any(|a| a.trim().is_empty()) {
        return Err(ConfigFileError::Invalid(
            "authz.admins contains an empty principal name".to_string(),
        ));
    }
    if admins.windows(2).any(|w| w[0] == w[1]) {
        return Err(ConfigFileError::Invalid(
            "authz.admins lists the same principal twice".to_string(),
        ));
    }
    let signed_policy = signed_policy(base, &file.authz)?;

    let promote_max_lag = file
        .membership
        .promote_max_lag
        .unwrap_or(config_engine::DEFAULT_PROMOTE_MAX_LAG);

    let mut snapshot = config_storage::SnapshotConfig::default();
    if let Some(v) = file.snapshot.logs_since_last {
        snapshot.logs_since_last = v;
    }
    if let Some(v) = file.snapshot.logs_to_keep {
        snapshot.logs_to_keep = v;
    }
    if let Some(v) = file.snapshot.purge_batch_size {
        snapshot.purge_batch_size = v;
    }
    if let Some(v) = file.snapshot.retain_snapshots {
        snapshot.retain_snapshots = v;
    }
    // `logs_since_last = 0` is how an operator disables snapshotting, and the latch requires
    // `logs_to_keep = u64::MAX` to go with it. Writing the one without the other is the silent
    // unbounded-log trap `SnapshotConfig::validate` exists to catch, so the sentinel is
    // supplied here rather than demanded of the operator.
    if file.snapshot.logs_since_last == Some(0) && file.snapshot.logs_to_keep.is_none() {
        snapshot.logs_to_keep = u64::MAX;
    }
    snapshot
        .validate()
        .map_err(|e| ConfigFileError::Invalid(format!("[snapshot]: {e}")))?;

    let backup = BackupKeys {
        signing_key: file
            .backup
            .signing_key_file
            .as_deref()
            .map(|p| resolve(base, p)),
        trust_key: file
            .backup
            .trust_key_file
            .as_deref()
            .map(|p| resolve(base, p)),
        encryption_key: file
            .backup
            .encryption_key_file
            .as_deref()
            .map(|p| resolve(base, p)),
    };

    let metrics_enabled = file.metrics.enabled.unwrap_or(true);

    let mut dedup = DedupLimits::DISABLED;
    dedup.enabled = file.dedup.enabled.unwrap_or(false);
    if let Some(v) = file.dedup.window_requests {
        dedup.window_requests = v;
    }
    if let Some(v) = file.dedup.max_records {
        dedup.max_records = v;
    }
    // Refused at startup rather than at apply time: a zero window retains nothing, so every
    // resubmission would apply a second time on a node that advertises `Dedup::Bounded` — the
    // one promise the section exists to make. The cap is refused for the same reason.
    if dedup.enabled {
        if dedup.window_requests == 0 {
            return Err(ConfigFileError::Invalid(
                "dedup.window_requests must be greater than zero when dedup is enabled".into(),
            ));
        }
        if dedup.max_records == 0 {
            return Err(ConfigFileError::Invalid(
                "dedup.max_records must be greater than zero when dedup is enabled".into(),
            ));
        }
        if u64::from(dedup.window_requests) > dedup.max_records {
            return Err(ConfigFileError::Invalid(format!(
                "dedup.window_requests ({}) cannot exceed dedup.max_records ({}): one client \
                 could not fill its own window",
                dedup.window_requests, dedup.max_records
            )));
        }
    }

    let mut list = config_engine::PaginationConfig::new(random_token_key());
    if let Some(path) = file.list.token_key_file.as_deref() {
        list.token_key = read_token_key(&resolve(base, path))?;
    }
    if let Some(v) = file.list.max_pinned_snapshots {
        list.max_pinned = v;
    }
    if let Some(v) = file.list.ttl_seconds {
        list.ttl = Duration::from_secs(v);
    }
    // Both refused at startup: zero pins means the first page of every walk is also its last
    // with no way to continue, and a zero TTL expires a token before the client can present
    // it. Either would advertise `Pagination::RevisionPinned` for a surface that cannot work.
    if list.max_pinned == 0 {
        return Err(ConfigFileError::Invalid(
            "list.max_pinned_snapshots must be greater than zero".into(),
        ));
    }
    if list.ttl.is_zero() {
        return Err(ConfigFileError::Invalid(
            "list.ttl_seconds must be greater than zero".into(),
        ));
    }

    Ok(ServerConfig {
        identity,
        data_dir: resolve(base, &file.node.data_dir),
        peer_listen,
        client_listen,
        gossip_listen,
        health_listen,
        tls_mode: file.tls.mode,
        tls_material,
        policy_path: file.authz.policy.as_deref().map(|p| resolve(base, p)),
        signed_policy,
        manifest,
        raft,
        gossip_seeds,
        gossip_secret_key,
        watch_limits,
        watch_progress_interval,
        retention,
        admins,
        promote_max_lag,
        metrics_enabled,
        dedup,
        list,
        snapshot,
        backup,
    })
}

/// Default poll interval for the signed policy files (D6.1).
const DEFAULT_POLICY_POLL_SECS: u64 = 10;

/// Validate `[authz]` under `mode = "signed"` (M6-37, ADR-0027).
///
/// Every missing field is named in **one** error rather than one per run: an operator fixing a
/// configuration by restarting until the message changes is an operator who will get the last
/// field wrong at 3am.
fn signed_policy(
    base: &Path,
    authz: &AuthzSection,
) -> Result<Option<SignedPolicyConfig>, ConfigFileError> {
    if authz.mode != AuthzModeName::Signed {
        // Named in static mode, the signed fields are a configuration that does nothing — and a
        // node whose operator believes it is verifying signatures when it is not is exactly the
        // failure ADR-0027 exists to prevent.
        if authz.policy_file.is_some() || !authz.trust_keys.is_empty() {
            return Err(ConfigFileError::Invalid(
                "authz.policy_file and authz.trust_keys require authz.mode = \"signed\"; under \
                 the default static mode they would be read by nothing"
                    .to_string(),
            ));
        }
        return Ok(None);
    }

    let mut missing = Vec::new();
    if authz.policy_file.is_none() {
        missing.push("authz.policy_file");
    }
    if authz.trust_keys.is_empty() {
        missing.push("authz.trust_keys");
    }
    if !missing.is_empty() {
        return Err(ConfigFileError::Invalid(format!(
            "authz.mode = \"signed\" requires {}; a node in signed mode without them fails \
             closed on every request",
            missing.join(" and ")
        )));
    }

    let policy_file = resolve(base, authz.policy_file.as_deref().expect("checked above"));
    let signature_file = match authz.signature_file.as_deref() {
        Some(path) => resolve(base, path),
        // `<policy>.sig`, not `<policy stem>.sig`: the two files travel together and a stem
        // rule would collide the moment a deployment has `policy.json` and `policy.yaml`.
        None => {
            let mut name = policy_file.clone().into_os_string();
            name.push(".sig");
            PathBuf::from(name)
        }
    };

    let mut trust_keys = Vec::with_capacity(authz.trust_keys.len());
    let mut names = std::collections::BTreeSet::new();
    for entry in &authz.trust_keys {
        if entry.name.trim().is_empty() {
            return Err(ConfigFileError::Invalid(
                "authz.trust_keys contains an entry with an empty name".to_string(),
            ));
        }
        if !names.insert(entry.name.as_str()) {
            return Err(ConfigFileError::Invalid(format!(
                "authz.trust_keys lists the key name {:?} twice; the signature envelope selects \
                 a key by name, so a duplicate makes the choice ambiguous",
                entry.name
            )));
        }
        trust_keys.push((entry.name.clone(), verifying_key(entry)?));
    }

    let poll_interval = Duration::from_secs(match authz.poll_interval_secs {
        Some(0) => {
            return Err(ConfigFileError::Invalid(
                "authz.poll_interval_secs must be greater than zero".to_string(),
            ))
        }
        Some(v) => v,
        None => DEFAULT_POLICY_POLL_SECS,
    });

    Ok(Some(SignedPolicyConfig {
        policy_file,
        signature_file,
        trust_keys,
        poll_interval,
    }))
}

/// Parse one `[authz] trust_keys` entry's hex into a verifying key.
fn verifying_key(entry: &TrustKeyEntry) -> Result<config_core::VerifyingKey, ConfigFileError> {
    let invalid = |detail: &str| {
        ConfigFileError::Invalid(format!(
            "authz.trust_keys entry {:?}: {detail}; expected 64 hex characters of an ed25519 \
             public key",
            entry.name
        ))
    };
    let raw = hex_32(entry.public_key.trim()).ok_or_else(|| invalid("not 32 bytes of hex"))?;
    // A 32-byte string is not automatically a point on the curve, and a key that cannot verify
    // anything must be refused here rather than at the first policy load — which happens after
    // the listeners are bound.
    config_core::VerifyingKey::from_bytes(&raw).map_err(|e| invalid(&e.to_string()))
}

/// Decode exactly 32 bytes of hex, or nothing.
fn hex_32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (slot, pair) in out.iter_mut().zip(s.as_bytes().chunks_exact(2)) {
        let text = std::str::from_utf8(pair).ok()?;
        *slot = u8::from_str_radix(text, 16).ok()?;
    }
    Some(out)
}

/// Loopback, including an IPv4-mapped IPv6 loopback — `::ffff:127.0.0.1` is the same machine
/// and a check that missed it would reject a legitimate address.
fn is_loopback(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

fn socket_addr(what: &str, raw: &str) -> Result<SocketAddr, ConfigFileError> {
    SocketAddr::from_str(raw).map_err(|e| {
        ConfigFileError::Invalid(format!(
            "{what} = {raw:?} is not a `host:port` address: {e}"
        ))
    })
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn required_path(what: &str, base: &Path, path: Option<&Path>) -> Result<PathBuf, ConfigFileError> {
    match path {
        Some(p) => Ok(resolve(base, p)),
        None => Err(ConfigFileError::Invalid(format!(
            "{what} is required when tls.mode = \"mutual\""
        ))),
    }
}

fn read_bytes(what: &str, path: &Path) -> Result<Vec<u8>, ConfigFileError> {
    std::fs::read(path).map_err(|source| ConfigFileError::Io {
        path: PathBuf::from(format!("{} ({what})", path.display())),
        source,
    })
}

/// A page-token signing key for a process that was not given one.
///
/// Random rather than derived from the node identity: two nodes deriving the same key would
/// let a token minted on one be *opened* on the other, and the node check would then be the
/// only thing between a client and a walk over a snapshot that does not exist there.
fn random_token_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    rand::Rng::fill(&mut rand::thread_rng(), &mut key);
    key
}

/// Read `list.token_key_file`: 64 hex characters, whitespace around them ignored.
///
/// Hex rather than raw bytes for the same reason `gossip.secret_key_hex` is: a key an operator
/// can paste, diff and rotate without a binary editor. The error never echoes the file's
/// contents.
fn read_token_key(path: &Path) -> Result<[u8; 32], ConfigFileError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigFileError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let hex = text.trim();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ConfigFileError::Invalid(format!(
            "list.token_key_file ({}) must hold 64 hex characters (a 256-bit HMAC key)",
            path.display()
        )));
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16).expect("checked hex") as u8;
        let lo = (chunk[1] as char).to_digit(16).expect("checked hex") as u8;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn parse_gossip_key(hex: &str) -> Result<[u8; 32], ConfigFileError> {
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ConfigFileError::Invalid(
            "gossip.secret_key_hex must be 64 hex characters (an AES-256 key)".into(),
        ));
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16).expect("checked hex") as u8;
        let lo = (chunk[1] as char).to_digit(16).expect("checked hex") as u8;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

/// Read the static allowlist policy document, without interpreting it.
///
/// Reading and parsing are separate steps because the *bytes* are a fact in their own right:
/// the node publishes their SHA-256 on its health endpoint (M3-42), and it publishes it for a
/// document that failed to parse too — "all three nodes are holding the same broken file" is
/// exactly what an operator needs to know then. A combined load would have thrown the bytes
/// away on the path where they matter most.
pub fn read_policy(path: &Path) -> Result<String, PolicyError> {
    std::fs::read_to_string(path).map_err(|source| PolicyError::Unreadable {
        path: path.to_path_buf(),
        source,
    })
}

/// Parse an allowlist policy document (ADR-0012 grammar).
///
/// `config-core` reads no formats, so the TOML round trip lives here. The two failure modes
/// are kept apart because they mean different things to an operator: `Missing` is "you did
/// not configure one", `Invalid` is "you did, and it is broken".
pub fn parse_policy(path: &Path, text: &str) -> Result<config_core::AllowlistPolicy, PolicyError> {
    toml::from_str(text).map_err(|e| PolicyError::Invalid {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })
}

/// Why an allowlist policy could not be loaded. Never fatal: it makes the node unready
/// (ADR-0018 §6), it does not stop the process.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    /// The file is absent or unreadable.
    #[error("cannot read allowlist policy {path}: {source}")]
    Unreadable {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// The file exists but is not a valid policy document.
    #[error("invalid allowlist policy {path}: {detail}")]
    Invalid {
        /// The file.
        path: PathBuf,
        /// The parser's description.
        detail: String,
    },
}

/// Grants per principal, for the startup summary log line. Never logs a key, only counts.
pub fn grant_summary(policy: &config_core::AllowlistPolicy) -> BTreeMap<String, usize> {
    let mut summary = BTreeMap::new();
    for grant in &policy.grants {
        *summary.entry(grant.principal.clone()).or_insert(0) += 1;
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLUSTER: &str = "0123456789abcdef0123456789abcdef";

    fn minimal(extra: &str) -> String {
        format!(
            r#"
[node]
node_id = 1
cluster_id = "{CLUSTER}"
data_dir = "data"

[listen]
peer = "127.0.0.1:0"
client = "127.0.0.1:0"

[tls]
mode = "insecure"
{extra}
"#
        )
    }

    fn parse(
        text: &str,
        health: Option<&str>,
        insecure_ok: bool,
    ) -> Result<ServerConfig, ConfigFileError> {
        let file: ServerConfigFile = toml::from_str(text).map_err(|e| ConfigFileError::Parse {
            path: PathBuf::from("<test>"),
            detail: e.to_string(),
        })?;
        validate(file, Path::new("."), health, insecure_ok)
    }

    #[test]
    fn a_minimal_insecure_document_needs_the_gate() {
        let error = parse(&minimal(""), None, false).expect_err("the gate is closed by default");
        assert!(
            error.to_string().contains("--allow-insecure-dev"),
            "the refusal must name the flag: {error}"
        );
        let cfg = parse(&minimal(""), None, true).expect("gate open");
        assert_eq!(cfg.identity.node_id, NodeId(1));
        assert_eq!(cfg.peer_listen.port(), 0);
        assert!(cfg.gossip_listen.is_none());
    }

    #[test]
    fn a_non_loopback_health_address_is_refused() {
        let error =
            parse(&minimal(""), Some("0.0.0.0:0"), true).expect_err("health must be loopback only");
        assert!(error.to_string().contains("loopback"), "{error}");
        parse(&minimal(""), Some("127.0.0.1:0"), true).expect("loopback health is accepted");
    }

    #[test]
    fn unknown_keys_are_refused_rather_than_ignored() {
        let text = minimal("") + "\n[nonsense]\nx = 1\n";
        assert!(
            parse(&text, None, true).is_err(),
            "a typo in a config file must not be silently ignored"
        );
    }

    #[test]
    fn raft_timers_must_be_ordered() {
        let text = minimal("") + "\n[raft]\nheartbeat_ms = 900\nelection_min_ms = 800\n";
        let error = parse(&text, None, true).expect_err("unordered timers");
        assert!(error.to_string().contains("election_min_ms"), "{error}");
    }

    #[test]
    fn mutual_tls_requires_all_three_paths() {
        let text = minimal("").replace("mode = \"insecure\"", "mode = \"mutual\"");
        let error = parse(&text, None, false).expect_err("no ca/cert/key");
        assert!(error.to_string().contains("tls.ca"), "{error}");
    }

    #[test]
    fn a_gossip_key_must_be_thirty_two_bytes_of_hex() {
        let text = minimal("") + "\n[gossip]\nsecret_key_hex = \"abcd\"\n";
        assert!(parse(&text, None, true).is_err());
        let text = minimal("") + &format!("\n[gossip]\nsecret_key_hex = \"{}\"\n", "ab".repeat(32));
        let cfg = parse(&text, None, true).expect("64 hex characters");
        assert_eq!(cfg.gossip_secret_key, Some([0xab; 32]));
    }

    /// F-015: the Common Name fallback is a written-down decision, not a default. An operator
    /// who never heard of the key must get the bound-to-a-cluster behaviour.
    #[test]
    fn the_common_name_principal_gate_is_shut_unless_the_document_opens_it() {
        let file: ServerConfigFile = toml::from_str(&minimal("")).expect("minimal document");
        assert!(
            !file.tls.allow_common_name_principals,
            "a document that does not mention the key must not enable it"
        );

        // It reaches the validated material, which is what the client plane is built from.
        let dir = tempfile::tempdir().expect("temp dir");
        for name in ["ca.pem", "node.cert.pem", "node.key.pem"] {
            std::fs::write(dir.path().join(name), b"-----BEGIN-----\n").expect("write pem");
        }
        let text = minimal("allow_common_name_principals = true")
            .replace("mode = \"insecure\"", "mode = \"mutual\"")
            + "ca = \"ca.pem\"\ncert = \"node.cert.pem\"\nkey = \"node.key.pem\"\n";
        let file: ServerConfigFile = toml::from_str(&text).expect("the key belongs to [tls]");
        let cfg = validate(file, dir.path(), None, false).expect("a complete mutual document");
        assert!(
            cfg.tls_material
                .expect("mutual mode carries material")
                .allow_common_name_principals,
            "the gate the document opened must reach the profile the planes are served with"
        );
    }

    #[test]
    fn node_id_zero_is_refused() {
        let text = minimal("").replace("node_id = 1", "node_id = 0");
        assert!(parse(&text, None, true).is_err());
    }

    /// M6: `[list]` defaults, overrides, and the two values that would advertise a pagination
    /// surface that cannot work.
    #[test]
    fn the_list_section_sizes_pagination_and_refuses_useless_values() {
        let default = parse(&minimal(""), None, true).expect("gate open");
        assert_eq!(default.list.max_pinned, 64);
        assert_eq!(default.list.ttl, Duration::from_secs(60));

        let tuned = parse(
            &minimal(
                "
[list]
max_pinned_snapshots = 8
ttl_seconds = 15
",
            ),
            None,
            true,
        )
        .expect("gate open");
        assert_eq!(tuned.list.max_pinned, 8);
        assert_eq!(tuned.list.ttl, Duration::from_secs(15));

        for bad in ["max_pinned_snapshots = 0", "ttl_seconds = 0"] {
            let error = parse(
                &minimal(&format!(
                    "
[list]
{bad}
"
                )),
                None,
                true,
            )
            .expect_err("a walk that cannot continue is refused at startup");
            assert!(error.to_string().contains("greater than zero"), "{error}");
        }

        // Two processes must not be able to open each other's tokens by accident.
        let other = parse(&minimal(""), None, true).expect("gate open");
        assert_ne!(default.list.token_key, other.list.token_key);
    }

    // -----------------------------------------------------------------------------------
    // M6-37 — `[authz] mode = "signed"` is validated before anything binds
    // -----------------------------------------------------------------------------------

    /// RFC 8032 §7.1 test vector 1's public key: a real point on the curve, so a refusal of a
    /// document carrying it is never "the key was malformed".
    const GOOD_KEY: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";

    fn signed(extra: &str) -> String {
        minimal(&format!("\n[authz]\nmode = \"signed\"\n{extra}\n"))
    }

    fn key_entry(name: &str) -> String {
        format!("[[authz.trust_keys]]\nname = \"{name}\"\npublic_key = \"{GOOD_KEY}\"\n")
    }

    /// The happy path, so every refusal below is attributable to the one field it removes.
    #[test]
    fn signed_mode_accepts_a_policy_file_and_one_trust_key() {
        let cfg = parse(
            &signed(&format!("policy_file = \"p.json\"\n{}", key_entry("ops"))),
            None,
            true,
        )
        .expect("a complete signed section");
        let signed = cfg
            .signed_policy
            .expect("signed mode builds a policy config");
        assert_eq!(signed.policy_file, Path::new("./p.json"));
        // `<policy>.sig`, not `<stem>.sig`: the default has to be derivable by an operator
        // holding only the policy path.
        assert_eq!(signed.signature_file, Path::new("./p.json.sig"));
        assert_eq!(signed.trust_keys.len(), 1);
        assert_eq!(signed.trust_keys[0].0, "ops");
        assert_eq!(signed.poll_interval, Duration::from_secs(10));
    }

    /// M6-37: a signed section missing *both* required fields names *both* of them.
    ///
    /// One error per restart is the failure mode this row exists to prevent: an operator who
    /// fixes `policy_file`, restarts, and only then learns about `trust_keys` has taken two
    /// outages to read one message.
    #[test]
    fn signed_mode_names_every_missing_field_in_one_error() {
        let error = parse(&signed(""), None, true).expect_err("signed mode needs its inputs");
        let text = error.to_string();
        assert!(text.contains("authz.policy_file"), "{text}");
        assert!(text.contains("authz.trust_keys"), "{text}");

        // And each one alone names only itself, so the message tracks the document.
        let only_key = parse(&signed(&key_entry("ops")), None, true)
            .expect_err("a trust key without a document is not a policy");
        assert!(
            only_key.to_string().contains("authz.policy_file"),
            "{only_key}"
        );
        assert!(
            !only_key.to_string().contains("authz.trust_keys"),
            "the key that *is* present must not be reported missing: {only_key}"
        );

        let only_doc = parse(&signed("policy_file = \"p.json\"\n"), None, true)
            .expect_err("a document nobody can verify is not a signed policy");
        assert!(
            only_doc.to_string().contains("authz.trust_keys"),
            "{only_doc}"
        );
    }

    /// The signed fields under the default static mode are a refusal, not a no-op.
    ///
    /// An operator who writes `policy_file` and forgets `mode = "signed"` believes signatures
    /// are being checked. Ignoring the key would leave them believing it.
    #[test]
    fn the_signed_fields_are_refused_under_static_mode() {
        let error = parse(
            &minimal("\n[authz]\npolicy_file = \"p.json\"\n"),
            None,
            true,
        )
        .expect_err("signed fields under static mode");
        assert!(error.to_string().contains("signed"), "{error}");

        // Static mode without them is unchanged from M3: no policy config at all (M6-36).
        let cfg = parse(&minimal(""), None, true).expect("gate open");
        assert!(cfg.signed_policy.is_none());
    }

    /// A trust key that cannot verify anything is refused here, not at the first load.
    ///
    /// The first load happens after the listeners bind, so accepting a malformed key would
    /// turn a typo into a node that starts, serves nothing, and looks healthy while doing it.
    #[test]
    fn a_trust_key_must_be_a_usable_ed25519_public_key() {
        for bad in [
            String::new(),
            "not-hex".to_string(),
            "zz".repeat(32),
            "ab".repeat(31),
        ] {
            let text = signed(&format!(
                "policy_file = \"p.json\"
[[authz.trust_keys]]
name = \"ops\"
\n                 public_key = \"{bad}\"
"
            ));
            let error = parse(&text, None, true)
                .err()
                .unwrap_or_else(|| panic!("{bad:?} must be refused as a trust key"));
            assert!(
                error.to_string().contains("ed25519"),
                "the refusal must say what a trust key is: {error}"
            );
        }
    }

    /// Two keys under the same name make the envelope's key selection ambiguous.
    #[test]
    fn duplicate_and_empty_trust_key_names_are_refused() {
        let dup = parse(
            &signed(&format!(
                "policy_file = \"p.json\"\n{}{}",
                key_entry("ops"),
                key_entry("ops")
            )),
            None,
            true,
        )
        .expect_err("a duplicate key name");
        assert!(dup.to_string().contains("twice"), "{dup}");

        let empty = parse(
            &signed(&format!("policy_file = \"p.json\"\n{}", key_entry(" "))),
            None,
            true,
        )
        .expect_err("an unnamed key");
        assert!(empty.to_string().contains("empty name"), "{empty}");
    }

    /// A zero poll interval is a busy loop over two files, not "poll as fast as possible".
    #[test]
    fn a_zero_poll_interval_is_refused_and_a_set_one_is_honoured() {
        let error = parse(
            &signed(&format!(
                "policy_file = \"p.json\"\npoll_interval_secs = 0\n{}",
                key_entry("ops")
            )),
            None,
            true,
        )
        .expect_err("zero is not an interval");
        assert!(error.to_string().contains("greater than zero"), "{error}");

        let cfg = parse(
            &signed(&format!(
                "policy_file = \"p.json\"\npoll_interval_secs = 3\nsignature_file = \"s.bin\"\n{}",
                key_entry("ops")
            )),
            None,
            true,
        )
        .expect("a set interval and an explicit signature path");
        let signed = cfg.signed_policy.expect("signed mode");
        assert_eq!(signed.poll_interval, Duration::from_secs(3));
        assert_eq!(signed.signature_file, Path::new("./s.bin"));
    }
}
