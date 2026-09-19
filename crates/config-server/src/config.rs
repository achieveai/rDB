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

use config_core::{ClusterId, ClusterIdentity, NodeId, RecoveryEpoch};
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
    /// The bootstrap manifest files, if the document names them.
    pub manifest: Option<ManifestFiles>,
    /// Raft timers.
    pub raft: config_engine::RaftTimers,
    /// Gossip seeds.
    pub gossip_seeds: Vec<SocketAddr>,
    /// Gossip encryption key.
    pub gossip_secret_key: Option<[u8; 32]>,
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
        manifest,
        raft,
        gossip_seeds,
        gossip_secret_key,
    })
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
}
