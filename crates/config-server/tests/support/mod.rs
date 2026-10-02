//! The E2E harness: a temp directory holding a whole three-process cluster.
//!
//! One [`Harness`] owns everything on disk for one test — the CA, every node's PEM material,
//! the signed bootstrap manifest, the allowlist policy, and one directory per node containing
//! its TOML, its RocksDB data and its JSONL log. The `TempDir` is dropped with the harness, so
//! a finished test leaves nothing behind (E2E-17).
//!
//! # Ports
//!
//! ADR-0018 lets a node bind port `0` and report what it got, but a bootstrap manifest has to
//! name every voter's endpoints *before* any of them starts. The two cannot both be satisfied,
//! so the harness binds an ephemeral listener for the peer and client planes, reads the port
//! the OS assigned, writes it into the TOML and the manifest, and keeps the listener open until
//! the instant before that node is spawned. The health endpoint is not in the manifest, so it
//! listens on port `0` and the ready line reports it.
//!
//! That leaves a window in which another process on the machine could take the port before the
//! daemon rebinds it. It is small (milliseconds, and the OS does not immediately reuse a just
//! released ephemeral port), it is the same trade-off every "pre-allocate then exec" harness
//! makes, and a collision fails loudly as a bind error rather than silently — but it is real,
//! and it is why a bind failure in this suite should be read as "port raced", not "daemon
//! broken".

#![allow(dead_code)]

pub mod daemon;

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use config_core::{ClusterId, NodeId, SchemaTriple};
use config_testkit::manifest::{Manifest, ManifestFixture, ManifestPaths, Voter};
use config_testkit::poll::TestTimers;
use config_testkit::tls::{CertProfile, TlsFixture};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub use daemon::{daemon_logs_glob, DaemonProcess, DaemonSpec};

/// The cluster every E2E row forms, unless it is testing a mismatch.
pub const CLUSTER_HEX: &str = "e2ee2ee2e00000000000000000000001";

/// The three voters.
pub const NODE_IDS: [u64; 3] = [1, 2, 3];

/// The principal the suite's client certificate names, and the one the policy grants.
pub const PRINCIPAL: &str = "svc-a";

/// A principal the policy does not mention (E2E-13).
pub const UNLISTED_PRINCIPAL: &str = "svc-z";

/// Raft timers written into every node's TOML.
///
/// Faster than the engine default so that "10 × election timeout" — the deadline every wait in
/// this suite derives from (anti-flake rule 3) — stays inside the 15 s per-row budget even on a
/// slow Windows VM.
pub const HEARTBEAT_MS: u64 = 150;
/// Minimum election timeout for the E2E cluster.
pub const ELECTION_MIN_MS: u64 = 450;
/// Maximum election timeout for the E2E cluster; every deadline is a multiple of this.
pub const ELECTION_MAX_MS: u64 = 900;

/// The timers a test derives its deadlines from. Identical to what the daemons run.
pub fn timers() -> TestTimers {
    TestTimers {
        heartbeat: Duration::from_millis(HEARTBEAT_MS),
        election_timeout_min: Duration::from_millis(ELECTION_MIN_MS),
        election_timeout_max: Duration::from_millis(ELECTION_MAX_MS),
    }
}

/// `n` × the worst-case election timeout.
pub fn deadline(n: u32) -> Duration {
    timers().multiple(n)
}

/// How long a daemon gets to bind, form and print its ready line.
///
/// Larger than a consensus deadline because it also covers process spawn, RocksDB open and
/// certificate parsing — on Windows the first of those alone can take a noticeable fraction of
/// a second.
pub fn startup_deadline() -> Duration {
    deadline(10)
}

/// The cluster id every row uses.
pub fn cluster_id() -> ClusterId {
    CLUSTER_HEX.parse().expect("the suite cluster id is valid")
}

/// Everything one node needs on disk.
#[derive(Debug, Clone)]
pub struct NodeLayout {
    /// Stable node id.
    pub node_id: u64,
    /// The node's own directory.
    pub dir: PathBuf,
    /// Its TOML configuration.
    pub config: PathBuf,
    /// Its RocksDB directory.
    pub data_dir: PathBuf,
    /// Its JSONL log directory.
    pub log_dir: PathBuf,
    /// The file whose creation asks it to stop.
    pub shutdown_file: PathBuf,
    /// Pre-allocated peer-plane address.
    pub peer: SocketAddr,
    /// Pre-allocated client-plane address.
    pub client: SocketAddr,
    /// Health address as configured: always port `0`; read the real one from the ready line.
    pub health: SocketAddr,
    /// The peer and client listeners, held from reservation until the first spawn so the
    /// window in which another process can take the ports is microseconds, not the whole
    /// harness construction. `None` after the first start (and always on a restart).
    pub reserved: Arc<Mutex<Option<(TcpListener, TcpListener)>>>,
}

impl NodeLayout {
    /// The voter entry this node contributes to the bootstrap manifest.
    pub fn voter(&self) -> Voter {
        Voter::new(
            NodeId(self.node_id),
            self.peer.to_string(),
            self.client.to_string(),
        )
    }
}

/// One test's whole on-disk world.
pub struct Harness {
    /// Root temp directory; everything else is under it.
    pub dir: tempfile::TempDir,
    /// The cluster being formed.
    pub cluster_id: ClusterId,
    /// The certificate authority and the identities it issues.
    pub tls: TlsFixture,
    /// The bootstrap manifest signer.
    pub manifest_fixture: ManifestFixture,
    /// Where the signed manifest was written.
    pub manifest: ManifestPaths,
    /// The allowlist policy file.
    pub policy: PathBuf,
    /// Per-node layout, in `NODE_IDS` order.
    pub nodes: Vec<NodeLayout>,
    /// The test method name, propagated into every daemon's log lines.
    pub method: &'static str,
}

impl Harness {
    /// Lay out a three-node cluster for `method`.
    ///
    /// `method` becomes the `testMethod` log field of every daemon, which is what makes the
    /// cross-process DuckDB joins (E2E-10, E2E-11) selective.
    pub async fn new(method: &'static str) -> Self {
        Self::with_nodes(method, &NODE_IDS).await
    }

    /// Lay out a cluster with an explicit voter set.
    pub async fn with_nodes(method: &'static str, node_ids: &[u64]) -> Self {
        Self::with_nodes_seeded(method, node_ids, cluster_id(), 0xE2E).await
    }

    /// Lay out a cluster under a distinct cluster identity, for a row that needs two
    /// independent clusters alive at once (E2E-45's restore target: a fresh identity, never
    /// the suite's own [`cluster_id()`]).
    ///
    /// `seed` must differ from whatever seed the row's other harness (if any) is using — it
    /// feeds both the CA and the manifest signer, so a shared seed would mint byte-identical
    /// certificates and a byte-identical manifest key for two supposedly-unrelated clusters.
    /// [`config_testkit::tls::TlsFixture::other_ca`]'s own derivation (`seed ^ 0x5ca1_ab1e_0000_0001`)
    /// is a convenient way to pick one deterministically.
    pub async fn with_cluster(
        method: &'static str,
        node_ids: &[u64],
        cluster_id: ClusterId,
        seed: u64,
    ) -> Self {
        Self::with_nodes_seeded(method, node_ids, cluster_id, seed).await
    }

    /// Shared body behind [`Harness::with_nodes`] and [`Harness::with_cluster`].
    async fn with_nodes_seeded(
        method: &'static str,
        node_ids: &[u64],
        cluster_id: ClusterId,
        seed: u64,
    ) -> Self {
        let dir = config_testkit::fs::temp_dir();
        let root = dir.path().to_path_buf();
        // Seeded from the cluster id so a rerun issues byte-identical certificates; nothing in
        // the suite depends on a fresh CA.
        let tls = TlsFixture::new(cluster_id, seed);
        let manifest_fixture = ManifestFixture::new(seed);

        let mut nodes = Vec::with_capacity(node_ids.len());
        for &node_id in node_ids {
            let node_dir = root.join(format!("node-{node_id}"));
            std::fs::create_dir_all(&node_dir).expect("create node directory");
            let (reserved, peer, client) = reserve_two().await;
            let health: SocketAddr = "127.0.0.1:0".parse().expect("a literal address");
            nodes.push(NodeLayout {
                node_id,
                dir: node_dir.clone(),
                config: node_dir.join("config.toml"),
                data_dir: node_dir.join("data"),
                log_dir: node_dir.join("logs"),
                shutdown_file: node_dir.join("stop"),
                peer,
                client,
                health,
                reserved,
            });
        }

        let policy = root.join("policy.toml");
        std::fs::write(&policy, default_policy()).expect("write the allowlist policy");

        let manifest_dir = root.join("manifest");
        let mut manifest = Manifest::new(cluster_id);
        for node in &nodes {
            manifest = manifest.with_voter(node.voter());
        }
        let manifest = manifest_fixture.write(&manifest_dir, &manifest);

        let harness = Self {
            dir,
            cluster_id,
            tls,
            manifest_fixture,
            manifest,
            policy,
            nodes,
            method,
        };
        let options = harness.node_options();
        for node in &harness.nodes {
            harness.write_node_files(node, &options);
        }
        harness
    }

    /// The root temp directory.
    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// Write one node's certificate material and TOML, with `options` applied.
    ///
    /// Called once per node at construction, and again by the rows that need a *different*
    /// document for the same node (E2E-12's insecure config, E2E-14's swapped data dirs).
    pub fn write_node_files(&self, node: &NodeLayout, options: &NodeOptions) {
        let profile = CertProfile::node(NodeId(node.node_id));
        let pair = self.tls.issue(profile);
        pair.write_to(&node.dir, "node");

        let data_dir = options
            .data_dir
            .clone()
            .unwrap_or_else(|| node.data_dir.clone());
        // Absent writes no key at all, which keeps the daemon's own default (30s) and keeps
        // every pre-rotation row's document byte-identical.
        let watch_files_secs_line = match options.tls_watch_files_secs {
            Some(secs) => format!("watch_files_secs = {secs}\n"),
            None => String::new(),
        };
        let handshake_timeout_line = match options.tls_handshake_timeout_ms {
            Some(ms) => format!("handshake_timeout_ms = {ms}\n"),
            None => String::new(),
        };
        let tls_keys = format!("{watch_files_secs_line}{handshake_timeout_line}");
        let tls_block = if options.insecure {
            format!("[tls]\nmode = \"insecure\"\n{tls_keys}")
        } else {
            format!(
                "[tls]\nmode = \"mutual\"\nca = \"ca.pem\"\ncert = \"node.cert.pem\"\n\
                 key = \"node.key.pem\"\n{tls_keys}"
            )
        };
        // `admins` is emitted only when a row asked for it, so every existing row's document is
        // byte-identical to what it was before the admin plane existed.
        let admins_key = if options.admins.is_empty() {
            String::new()
        } else {
            let names = options
                .admins
                .iter()
                .map(|n| format!("\"{n}\""))
                .collect::<Vec<_>>()
                .join(", ");
            format!("admins = [{names}]\n")
        };
        // The array-of-tables has to come last inside `[authz]`: a scalar key written after
        // `[[authz.trust_keys]]` would belong to the trust key, not to the section.
        let authz_block = match (&options.signed_policy, &options.policy) {
            (Some(signed), _) => format!(
                "\n[authz]\nmode = \"signed\"\npolicy_file = {}\nsignature_file = {}\n\
                 poll_interval_secs = {}\n{admins_key}\n[[authz.trust_keys]]\n\
                 name = \"{TRUST_KEY_NAME}\"\npublic_key = \"{}\"\n",
                toml_path(&signed.policy_file),
                toml_path(&signed.signature_file),
                signed.poll_interval_secs,
                signed.trust_key_hex,
            ),
            (None, Some(path)) => {
                format!("\n[authz]\npolicy = {}\n{admins_key}", toml_path(path))
            }
            (None, None) if !admins_key.is_empty() => format!("\n[authz]\n{admins_key}"),
            (None, None) => String::new(),
        };
        let backup_block = match &options.backup_signing_key {
            Some(path) => format!("\n[backup]\nsigning_key_file = {}\n", toml_path(path)),
            None => String::new(),
        };
        // Like `[snapshot]`: absent writes no section at all, so every pre-M6 row's document
        // is byte-identical to what it was before pagination existed.
        let list_block = match &options.list {
            Some(l) => {
                let token_key_line = match &l.token_key_file {
                    Some(path) => format!("token_key_file = {}\n", toml_path(path)),
                    None => String::new(),
                };
                format!(
                    "
[list]
max_pinned_snapshots = {}
ttl_seconds = {}
{token_key_line}",
                    l.max_pinned_snapshots, l.ttl_seconds
                )
            }
            None => String::new(),
        };
        let snapshot_block = match &options.snapshot {
            Some(s) => format!(
                "\n[snapshot]\nlogs_since_last = {}\nlogs_to_keep = {}\npurge_batch_size = {}\n",
                s.logs_since_last, s.logs_to_keep, s.purge_batch_size
            ),
            None => String::new(),
        };
        let retention_block = match &options.retention {
            Some(r) => {
                let mut lines = String::new();
                if let Some(v) = r.max_age_secs {
                    lines.push_str(&format!("max_age_secs = {v}\n"));
                }
                if let Some(v) = r.max_revisions {
                    lines.push_str(&format!("max_revisions = {v}\n"));
                }
                if let Some(v) = r.max_bytes {
                    lines.push_str(&format!("max_bytes = {v}\n"));
                }
                if let Some(v) = r.check_interval_secs {
                    lines.push_str(&format!("check_interval_secs = {v}\n"));
                }
                format!("\n[retention]\n{lines}")
            }
            None => String::new(),
        };
        // Two keys in two sections, so they are built together and can never disagree about
        // whether this node gossips at all.
        let (gossip_listen_key, gossip_block) = match &options.gossip {
            Some(seeds) => {
                let list = seeds
                    .iter()
                    .map(|s| format!("\"{s}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                // Absent writes neither key, so every row that predates gossip key rotation
                // keeps a byte-identical `[gossip]` section.
                let key_lines = match &options.gossip_keys {
                    Some(keys) => {
                        let mut lines = String::new();
                        if let Some(secret) = &keys.secret_key_hex {
                            lines.push_str(&format!("secret_key_hex = \"{secret}\"\n"));
                        }
                        if !keys.accepted_key_hex.is_empty() {
                            let accepted = keys
                                .accepted_key_hex
                                .iter()
                                .map(|k| format!("\"{k}\""))
                                .collect::<Vec<_>>()
                                .join(", ");
                            lines.push_str(&format!("accepted_key_hex = [{accepted}]\n"));
                        }
                        lines
                    }
                    None => String::new(),
                };
                (
                    "gossip = \"127.0.0.1:0\"\n".to_string(),
                    format!("\n[gossip]\nseeds = [{list}]\n{key_lines}"),
                )
            }
            None => (String::new(), String::new()),
        };
        let manifest_block = match &options.manifest {
            Some(paths) => format!(
                "\n[manifest]\npath = {}\nsig = {}\nsigning_key_pub = {}\n",
                toml_path(&paths.manifest),
                toml_path(&paths.signature),
                toml_path(&paths.public_key),
            ),
            None => String::new(),
        };

        let document = format!(
            "[node]\n\
             node_id = {node_id}\n\
             cluster_id = \"{cluster}\"\n\
             recovery_epoch = {epoch}\n\
             data_dir = {data_dir}\n\
             \n\
             [listen]\n\
             peer = \"{peer}\"\n\
             client = \"{client}\"\n\
             {gossip_listen_key}\
             \n\
             {tls_block}\
             {gossip_block}\
             {authz_block}\
             {manifest_block}\
             {backup_block}\
             {snapshot_block}\
             {retention_block}\
             {list_block}\
             \n[raft]\n\
             heartbeat_ms = {HEARTBEAT_MS}\n\
             election_min_ms = {ELECTION_MIN_MS}\n\
             election_max_ms = {ELECTION_MAX_MS}\n",
            node_id = node.node_id,
            cluster = self.cluster_id,
            epoch = options.recovery_epoch,
            data_dir = toml_path(&data_dir),
            peer = node.peer,
            client = node.client,
        );
        std::fs::write(&node.config, document).expect("write the node configuration");
    }

    /// The default options for a node of this harness: this harness's policy and manifest.
    pub fn node_options(&self) -> NodeOptions {
        NodeOptions {
            policy: Some(self.policy.clone()),
            manifest: Some(self.manifest.clone()),
            ..NodeOptions::default()
        }
    }

    /// The spawn spec for node index `index`.
    ///
    /// Asking for the spec is the signal that the node is about to be spawned, so the reserved
    /// peer/client ports are released here: the daemon binds them within milliseconds,
    /// whichever way the row spawns it. A run that will never bind them —
    /// `--capabilities` — must use [`Harness::spec_no_listen`] instead.
    pub fn spec(&self, index: usize) -> DaemonSpec {
        drop(
            self.nodes[index]
                .reserved
                .lock()
                .expect("reservation lock")
                .take(),
        );
        self.spec_no_listen(index)
    }

    /// The spawn spec for node index `index`, **keeping** its reserved ports.
    ///
    /// For a run that opens no listener at all (`--capabilities`, ADR-0018 §2). `spec` releases
    /// the reservation because it means "this node is about to bind"; a `--capabilities` run
    /// does not bind, so releasing there hands the ports back to the OS for however long the
    /// rest of the test takes before `start_all()` finally spawns the real daemon — which is
    /// exactly the window the reservation exists to close (critic A8).
    pub fn spec_no_listen(&self, index: usize) -> DaemonSpec {
        let node = &self.nodes[index];
        let mut spec = DaemonSpec::new(&node.config, &node.log_dir, &node.shutdown_file);
        spec.health_listen = Some(node.health);
        spec.log_fields = vec![
            ("testModule".to_string(), "e2e_daemon".to_string()),
            ("testMethod".to_string(), self.method.to_string()),
        ];
        spec
    }

    /// Spawn one node and wait for its ready line.
    pub fn start(&self, index: usize, form: bool) -> DaemonProcess {
        let mut spec = self.spec(index);
        spec.form = form;
        let mut process = DaemonProcess::spawn(spec);
        process.wait_ready(startup_deadline()).unwrap_or_else(|e| {
            panic!("node {} never became ready: {e}", self.nodes[index].node_id)
        });
        process
    }

    /// Start every node, forming the cluster from the signed manifest on node index 0.
    ///
    /// The followers start first on purpose: `--form` writes the initial membership and
    /// immediately begins replicating it, so a node that is not listening yet would just make
    /// the first append attempt fail and wait out a retry.
    pub fn start_all(&self) -> Vec<DaemonProcess> {
        let followers: Vec<DaemonProcess> = (1..self.nodes.len())
            .map(|index| self.start(index, false))
            .collect();
        let mut processes = vec![self.start(0, true)];
        processes.extend(followers);
        processes
    }

    /// Every node's log directory, for a DuckDB glob over the whole cluster.
    pub fn logs_glob(&self) -> String {
        daemon_logs_glob(self.root())
    }
}

/// What makes one node's configuration document differ from the harness default.
#[derive(Debug, Clone, Default)]
pub struct NodeOptions {
    /// `tls.mode = "insecure"` instead of mutual (E2E-12).
    pub insecure: bool,
    /// A data directory other than the node's own (E2E-14).
    pub data_dir: Option<PathBuf>,
    /// The allowlist policy file, if any.
    pub policy: Option<PathBuf>,
    /// The bootstrap manifest, if any.
    pub manifest: Option<ManifestPaths>,
    /// `node.recovery_epoch`.
    pub recovery_epoch: u32,
    /// `[authz] admins` — principals allowed on the admin plane (M5, ADR-0023).
    ///
    /// Empty by default, which writes no key at all: an empty allowlist denies every admin
    /// RPC, which is the posture every pre-M5 row was already running under.
    pub admins: Vec<String>,
    /// `[backup] signing_key_file` — required before the admin plane will serve `Backup`.
    pub backup_signing_key: Option<PathBuf>,
    /// `[snapshot]` — build and purge aggressively enough that a row can observe both.
    ///
    /// Absent writes no section at all, so every pre-M5 row keeps the daemon's own defaults:
    /// a snapshot every few thousand entries, which no E2E row is long enough to reach.
    pub snapshot: Option<SnapshotTuning>,
    /// `[retention]` — how soon the leader proposes a compaction (M4, ADR-0019).
    ///
    /// Absent writes no section at all, so every pre-E2E-42 row keeps the engine defaults
    /// (compaction only once the journal is large or old). E2E-42 has no client-facing way to
    /// force a `Compact` — `propose_compact` is reachable only from in-process code
    /// (`config_testkit::cluster::Cluster::compact_now`), never from a client or admin RPC
    /// against a real daemon — so the row tightens this instead and lets ordinary write load
    /// cross the ceiling within its own lifetime.
    pub retention: Option<RetentionTuning>,
    /// `[authz] mode = "signed"` — the ADR-0027 document, its signature and its trust key.
    ///
    /// Mutually exclusive with `policy`: the two describe different authorization models, and a
    /// document naming both is refused at startup (M6-37). A row that sets this leaves `policy`
    /// at `None`.
    pub signed_policy: Option<SignedAuthz>,
    /// `[list]` — revision-pinned pagination bounds (M6, ADR-0029).
    ///
    /// Absent keeps the daemon defaults (64 pins, 60 s), which is what every pre-M6 row ran
    /// under. A row sets it only when the *bounds themselves* are what it is exercising.
    pub list: Option<ListTuning>,
    /// `[listen] gossip` and `[gossip] seeds` — advisory gossip (ADR-0003 §19.9).
    ///
    /// `None` writes neither key, so every row that predates gossip keeps a byte-identical
    /// document. `Some(seeds)` binds `127.0.0.1:0` and joins those addresses at startup.
    ///
    /// The bind is port `0` rather than a reservation because memberlist wants UDP as well as
    /// TCP on its port, and this harness can only reserve TCP: a reserved port would be a
    /// half-guarantee that reads as one. The daemon reports what it bound on its ready line
    /// (`Ready::gossip`), so a row seeds the next node from the previous node's ready line and
    /// never needs to know a port in advance.
    pub gossip: Option<Vec<String>>,
    /// `[tls] watch_files_secs` — how often the TLS poller checks the CA/cert/key files for
    /// changes (ADR-0028).
    ///
    /// Absent writes no key at all, which keeps the daemon's own default (30s) and keeps every
    /// pre-rotation row's document byte-identical.
    pub tls_watch_files_secs: Option<u64>,
    /// `[tls] handshake_timeout_ms` — how long a TLS handshake may take before the listener
    /// drops it (gap G-01).
    ///
    /// Absent writes no key at all, for the same reason as `tls_watch_files_secs`: the daemon
    /// keeps its own default and every row that predates the setting keeps a byte-identical
    /// document, so exposing the constant changed no existing row's meaning.
    pub tls_handshake_timeout_ms: Option<u64>,
    /// `[gossip] secret_key_hex` / `accepted_key_hex` — the gossip encryption keyring
    /// (ADR-0028).
    ///
    /// `None` writes neither key, so every row that predates gossip key rotation keeps a
    /// byte-identical document. Only meaningful alongside `Some(_)` `gossip` — the `[gossip]`
    /// section has to exist for these keys to land inside it.
    pub gossip_keys: Option<GossipKeyOptions>,
}

/// `[gossip] secret_key_hex` / `accepted_key_hex` for one node's document (ADR-0028).
#[derive(Debug, Clone, Default)]
pub struct GossipKeyOptions {
    /// The primary signing key this node advertises, 64 lowercase hex characters.
    pub secret_key_hex: Option<String>,
    /// Further keys this node still accepts mid-rotation, each 64 lowercase hex characters.
    pub accepted_key_hex: Vec<String>,
}

/// What one node's `[authz] mode = "signed"` section names (M6, ADR-0027).
#[derive(Debug, Clone)]
pub struct SignedAuthz {
    /// `authz.policy_file`.
    pub policy_file: PathBuf,
    /// `authz.signature_file`.
    pub signature_file: PathBuf,
    /// The one trusted signer's public key, 64 lowercase hex characters.
    pub trust_key_hex: String,
    /// `authz.poll_interval_secs`. Short on purpose: a row that waits for a rotation waits for
    /// this, and the wait is bounded by a deadline rather than slept through.
    pub poll_interval_secs: u64,
}

/// A signing key and the two files `authz.mode = "signed"` reads.
///
/// The suite signs its own documents for the same reason `config-core`'s rows do: a fixture that
/// could not produce a *valid* signature could not prove that the daemon refuses an invalid one.
/// Nothing here re-implements verification — it builds exactly what `config_core::verify_policy`
/// accepts, from the same envelope type the daemon decodes.
pub struct PolicyFixture {
    key: ed25519_dalek::SigningKey,
    /// Where the document is written. Deleting it is how a row starts a node with no policy.
    pub policy_file: PathBuf,
    /// Where the detached envelope is written.
    pub signature_file: PathBuf,
    /// Which cluster the written document claims (gap G-06). `None` is the legacy-unscoped
    /// document every row wrote before the field existed, which still verifies and still
    /// adopts — so leaving this alone keeps an existing row testing exactly what it tested.
    /// A row that wants the scoped path calls [`Self::for_cluster`].
    cluster_id: Option<config_core::ClusterId>,
}

impl PolicyFixture {
    /// Lay out `policy.json` / `policy.json.sig` under `root`, with a deterministic key.
    pub fn new(root: &Path) -> Self {
        Self {
            key: ed25519_dalek::SigningKey::from_bytes(&[0xA7; 32]),
            policy_file: root.join("policy.json"),
            signature_file: root.join("policy.json.sig"),
            cluster_id: None,
        }
    }

    /// Write documents scoped to `cluster` instead of legacy-unscoped ones (gap G-06).
    ///
    /// Separate from [`Self::new`] so that adding the field changed no existing row's meaning:
    /// a row that never calls this still writes the document it always wrote.
    pub fn for_cluster(mut self, cluster: config_core::ClusterId) -> Self {
        self.cluster_id = Some(cluster);
        self
    }

    /// The `[[authz.trust_keys]]` entry for this fixture's signer.
    pub fn trust_key_hex(&self) -> String {
        hex::encode(self.key.verifying_key().to_bytes())
    }

    /// The `[authz]` section a node configured from this fixture gets.
    pub fn authz(&self, poll_interval_secs: u64) -> SignedAuthz {
        SignedAuthz {
            policy_file: self.policy_file.clone(),
            signature_file: self.signature_file.clone(),
            trust_key_hex: self.trust_key_hex(),
            poll_interval_secs,
        }
    }

    /// Write a document at `version` granting [`PRINCIPAL`] read and write on each prefix.
    ///
    /// The signature is written **first** so the pair is never observed as "new document, old
    /// signature": the poller reads the document then the signature, and the reverse order would
    /// hand it a `hash_mismatch` on every rotation for one tick.
    pub fn write(&self, version: u64, prefixes: &[&str], admins: &[&str]) {
        use config_core::policy::{document_hash, grant, signature_payload, PolicySignature};
        use ed25519_dalek::Signer;

        let document = config_core::PolicyDocument {
            version,
            issued_unix_ms: 1_700_000_000_000 + version,
            grants: prefixes
                .iter()
                .map(|prefix| {
                    grant(
                        PRINCIPAL,
                        prefix,
                        &[config_core::Action::Read, config_core::Action::Write],
                    )
                })
                .collect(),
            admins: admins.iter().map(|a| (*a).to_string()).collect(),
            cluster_id: self.cluster_id,
        };
        let bytes = serde_json::to_vec(&document).expect("a policy document serializes");
        let hash = document_hash(&bytes);
        let envelope = PolicySignature {
            envelope_version: config_core::policy::POLICY_SIGNATURE_VERSION,
            key_name: TRUST_KEY_NAME.to_string(),
            version,
            hash,
            signature: self
                .key
                .sign(&signature_payload(&hash, version))
                .to_bytes()
                .to_vec(),
        }
        .encode()
        .expect("an envelope encodes");
        write_atomically(&self.signature_file, &envelope);
        write_atomically(&self.policy_file, &bytes);
    }

    /// Remove both files, which is how a node starts with no valid policy at all.
    pub fn remove(&self) {
        let _ = std::fs::remove_file(&self.policy_file);
        let _ = std::fs::remove_file(&self.signature_file);
    }
}

/// The name every fixture envelope selects its key by.
pub const TRUST_KEY_NAME: &str = "rotate";

/// Write `bytes` to `path` through a temporary file in the same directory.
///
/// A poller that reads a half-written document sees a `parse_error` and keeps the old policy,
/// which is correct but makes a rotation row flap; renaming into place removes the window.
fn write_atomically(path: &Path, bytes: &[u8]) {
    let staging = path.with_extension("staging");
    std::fs::write(&staging, bytes).expect("write the staged policy artifact");
    std::fs::rename(&staging, path).expect("rename the staged policy artifact into place");
}

/// The `[list]` knobs an E2E row needs to observe a pin being held, evicted or expired.
#[derive(Debug, Clone)]
pub struct ListTuning {
    /// Snapshots this node holds open at once.
    pub max_pinned_snapshots: u32,
    /// How long one survives unread.
    pub ttl_seconds: u64,
    /// `list.token_key_file` (M6, ADR-0029) — 64 hex characters, the HMAC key that seals and
    /// opens page tokens.
    ///
    /// `None` (every pre-E2E-44 row) writes no key at all, so each node falls back to its own
    /// `random_token_key()` (`config-server/src/config.rs`) and no two nodes' tokens ever
    /// cross-validate — fine for a row that only ever continues a walk on the node that opened
    /// it. E2E-44 needs a token minted on one node to reach `PageTokenExpiredReason::Node`
    /// (not `Mac`) when presented to a different one after a leader failover, which requires
    /// every node in the row's cluster to share one key file.
    pub token_key_file: Option<PathBuf>,
}

/// The `[snapshot]` knobs a row needs to make a leader snapshot and purge within its lifetime.
///
/// The three move together because `config_storage::SnapshotConfig` refuses a half-applied
/// change (ADR-0022); writing them as a unit is the only shape the daemon accepts.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotTuning {
    /// Build once committed is this far past the current snapshot.
    pub logs_since_last: u64,
    /// Snapshot-covered entries to retain rather than purge.
    pub logs_to_keep: u64,
    /// Minimum entries a purge must be able to remove before one is scheduled.
    pub purge_batch_size: u64,
}

/// The `[retention]` knobs a row needs the leader to propose a `Compact` within its lifetime
/// (M4, ADR-0019), used in place of a client-facing "force a compaction" call — none exists.
///
/// `None` on any field writes no key for it, matching `RetentionSection`'s own semantics: the
/// engine default applies to that ceiling alone.
#[derive(Debug, Clone, Copy, Default)]
pub struct RetentionTuning {
    /// `retention.max_age_secs`.
    pub max_age_secs: Option<u64>,
    /// `retention.max_revisions`.
    pub max_revisions: Option<u64>,
    /// `retention.max_bytes`.
    pub max_bytes: Option<u64>,
    /// `retention.check_interval_secs`.
    pub check_interval_secs: Option<u64>,
}

/// The suite's allowlist: `svc-a` may read and write everything, and nobody else is named.
///
/// The prefix is empty rather than `"/"` because the conformance suite namespaces its keys
/// under `__conformance/...`; a grant covering only `/` would fail E2E-03 for a reason that has
/// nothing to do with the daemon.
pub fn default_policy() -> String {
    format!(
        "[[grant]]\nprincipal = \"{PRINCIPAL}\"\nprefix = \"\"\naccess = [\"read\", \"write\"]\n"
    )
}

/// Reserve the peer and client ports and keep both listeners open.
///
/// The listeners are returned (as blocking `std` sockets, so dropping them never needs a
/// runtime) and released by [`Harness::start`] just before the daemon is spawned.
async fn reserve_two() -> (
    Arc<Mutex<Option<(TcpListener, TcpListener)>>>,
    SocketAddr,
    SocketAddr,
) {
    let (peer_listener, peer) = config_testkit::ports::ephemeral_listener().await;
    let (client_listener, client) = config_testkit::ports::ephemeral_listener().await;
    let peer_listener = peer_listener.into_std().expect("std peer listener");
    let client_listener = client_listener.into_std().expect("std client listener");
    (
        Arc::new(Mutex::new(Some((peer_listener, client_listener)))),
        peer,
        client,
    )
}

/// A TOML string literal for a path, with Windows separators escaped.
fn toml_path(path: &Path) -> String {
    format!("\"{}\"", path.display().to_string().replace('\\', "\\\\"))
}

/// The subset of [`config_engine::HealthPayload`] the E2E rows assert on.
///
/// Declared here rather than deserialized into the engine's own type because `HealthPayload`
/// is `Serialize` only — it is something a node *publishes*, and giving it a `Deserialize`
/// impl in the engine would suggest the daemon parses one back, which it never does.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct Health {
    /// This node's id.
    pub node_id: u64,
    /// The cluster, as hex.
    pub cluster_id: String,
    /// The node it currently believes is leader.
    pub current_leader: Option<u64>,
    /// Last applied log index.
    pub last_applied: Option<u64>,
    /// Committed voter ids, ascending.
    pub membership_voter_ids: Vec<u64>,
    /// Log id of the committed membership entry, as raw JSON (shape is the engine's).
    pub membership_log_id: Option<serde_json::Value>,
    /// The public cluster revision applied here.
    pub cluster_revision: u64,
    /// The deterministic applied-state digest.
    pub state_hash_hex: String,
    /// Command-carrying entries applied so far.
    pub applied_commands: u64,
    /// What this node's store guarantees survives a restart.
    pub durability: String,
    /// Whether this node will serve client traffic.
    pub ready: bool,
    /// Which authorization model is in force.
    pub authz_kind: String,
    /// How the client plane is protected.
    pub transport_security: String,
    /// What policy this node holds: kind, grant count, and the digest of the document bytes.
    pub policy: Policy,
    /// What this node itself advertises on all three planes (M6, ADR-0030).
    pub schema: SchemaTriple,
    /// The lowest schema any committed voter is known to have — `None` on a node that is not
    /// the leader (M6-R12): only a leader calls every voter on the peer plane.
    pub cluster_min_schema: Option<SchemaTriple>,
    /// Authorization decisions refused since start.
    pub authz_denied: u64,
    /// Client connections whose identity could not be established since start.
    pub authn_rejected: u64,
    /// The replicated compaction watermark (TA-39).
    pub compact_revision: u64,
    /// Oldest retained journal revision, or `None` when the journal is empty.
    pub journal_oldest_revision: Option<u64>,
    /// Newest retained journal revision, or `None` when the journal is empty.
    pub journal_newest_revision: Option<u64>,
    /// Digest over the retained journal above the watermark, as 64 lowercase hex characters.
    pub journal_hash: String,
    /// Watch streams open on this node right now.
    pub watch_streams_open: usize,
    /// Active signed policy version, `None` under the static allowlist (M6-16).
    pub policy_version: Option<u64>,
    /// Why there is no active policy, or which versions this node is converging between.
    ///
    /// Raw JSON for the same reason `membership_log_id` is: the shape belongs to
    /// `config_core::policy::PolicyState`, and mirroring its variants here would make this
    /// file a second, drifting definition of the same enum.
    pub policy_state: Option<serde_json::Value>,
}

/// The `policy` object inside [`Health`], mirroring `config_engine::PolicySummary`.
///
/// It is what answers "are these processes enforcing the same policy?" across a fleet, which
/// is only checkable from outside the process — hence its presence here and not merely in the
/// in-process rows.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct Policy {
    /// The model in force, as `config_core::Authz` serializes it.
    ///
    /// Raw JSON, not a string: `Authz` is an externally tagged enum, so the static models are
    /// bare strings (`"StaticAllowlist"`) while `SignedPolicy` is an object carrying the live
    /// version. A `String` here would parse the M3 rows and fail every M6 one.
    pub kind: serde_json::Value,
    /// How many grant rules the allowlist holds.
    pub grants: u64,
    /// Lowercase hex SHA-256 of the policy document bytes, when there were any.
    pub policy_hash_hex: Option<String>,
}

/// `GET /health` over the daemon's loopback endpoint.
pub async fn health(endpoint: &str) -> Health {
    let body = http_get(endpoint, "/health").await;
    serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("health payload from {endpoint} did not parse ({e}): {body}"))
}

/// A minimal HTTP/1.1 `GET`, returning the response body.
///
/// The health endpoint answers one request per connection and closes, so "read to EOF" is the
/// whole protocol here.
pub async fn http_get(endpoint: &str, path: &str) -> String {
    let (status, body) = http_get_status(endpoint, path).await;
    assert_eq!(
        status, 200,
        "GET {path} on {endpoint} answered {status}: {body}"
    );
    body
}

/// A minimal HTTP/1.1 `GET`, returning `(status code, body)`.
pub async fn http_get_status(endpoint: &str, path: &str) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(endpoint)
        .await
        .unwrap_or_else(|e| panic!("connect to {endpoint}: {e}"));
    let request = format!("GET {path} HTTP/1.1\r\nHost: {endpoint}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write the request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read the response");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("no header/body separator in response: {response:?}"));
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status code in response head: {head:?}"));
    (status, body.to_string())
}

/// Every JSON line of a daemon log file.
pub fn log_lines(path: &Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read daemon log {}: {e}", path.display()));
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("malformed JSONL in {}: {e}\n{line}", path.display()))
        })
        .collect()
}

/// This node's JSONL log file (`<log-dir>/<node_id>.jsonl`), whether or not it exists yet.
pub fn log_file(node: &NodeLayout) -> PathBuf {
    node.log_dir.join(format!("{}.jsonl", node.node_id))
}

/// Every `@m == "startup_failed"` line `node` wrote for test method `method`.
///
/// The refusal is asserted from the daemon's own structured log rather than by scraping
/// stderr: `msg="startup_failed"` plus the stable `reason` field is the contract (ADR-0018 §5),
/// and stderr carries the same information only as prose for a human. Filtering on `testMethod`
/// keeps a node directory that was reused across sub-cases (or across a successful start and a
/// later refusal) from answering for the wrong attempt.
pub fn startup_failed_lines(node: &NodeLayout, method: &str) -> Vec<serde_json::Value> {
    let path = log_file(node);
    if !path.exists() {
        return Vec::new();
    }
    log_lines(&path)
        .into_iter()
        .filter(|line| {
            line.get("@m").and_then(serde_json::Value::as_str) == Some("startup_failed")
                && line.get("testMethod").and_then(serde_json::Value::as_str) == Some(method)
        })
        .collect()
}

/// A string-valued field of a JSONL log line, or `None` if absent or not a string.
pub fn log_field<'a>(row: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    row.get(name).and_then(serde_json::Value::as_str)
}

/// How many lines of `path` carry `@m == message`.
pub fn count_messages(path: &Path, message: &str) -> usize {
    log_lines(path)
        .iter()
        .filter(|line| line.get("@m").and_then(serde_json::Value::as_str) == Some(message))
        .count()
}
