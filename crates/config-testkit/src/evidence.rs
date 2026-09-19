//! Evidence artifacts: one JSON file per evidence row (ADR-0031, test plan TA-61).
//!
//! An *evidence* row is not a gate. It records what a run on **this** host actually did —
//! capacity, recovery timings, fault-matrix coverage — and asserts only the structural
//! invariants that hold on any host (no starvation, no split-brain, no lost mutation). The
//! numbers themselves are recorded for a human to read and are never turned into a pass/fail
//! threshold, because a threshold that passes on one runner and fails on a slightly slower one
//! is a flaky gate rather than evidence (ADR-0031 "What the remaining evidence rows assert").
//!
//! Everything here exists so that fact cannot be lost in transit:
//!
//! * [`write_evidence`] is the **only** writer. It stamps [`DISCLAIMER`] itself, so no row can
//!   forget to say that its numbers are dev-host numbers.
//! * [`RunInfo`] carries `achieved / requested`, so [`RunRecord::scale_factor`] records what the
//!   run *reached*, not what it asked for. A row that could not reach its configured scale says
//!   so with `full_scale: false` (M6-115).
//! * [`read_evidence`] parses with `deny_unknown_fields` and [`validate`] rejects an artifact
//!   with an empty `values`, a missing build stamp or an edited disclaimer (M6-113) — a
//!   malformed evidence file is worse than no evidence file, because it looks like evidence.
//! * [`partition_arrangements`], [`crash_cases`] and [`security_cases`] enumerate their matrices
//!   (TA-63) so that adding a [`Boundary`] or a partition shape cannot silently skip a case.
//!
//! `RETCD_EVIDENCE=1` asks for full scale; unset or `0` runs the reduced scale whose constants
//! live in the test source, never derived from the host (ADR-0031 "Fixed reduced-scale
//! constants"). Evidence rows are not `#[ignore]`d: they run in the ordinary gate at reduced
//! scale, which is what keeps the code path exercised between the rare full-scale runs.
//!
//! # Example
//!
//! ```no_run
//! use config_testkit::evidence::{write_evidence, RunInfo};
//!
//! let run = RunInfo::start(7);
//! // … the row does its work, reaching 100 of the 1000 streams it would run at full scale …
//! let path = write_evidence(
//!     "watch-capacity",
//!     serde_json::json!({ "streams": 100 }),
//!     run.scaled(1000.0, 100.0),
//! );
//! # let _ = path;
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use config_core::NodeId;
use config_storage::Boundary;
use serde::{Deserialize, Serialize};

use crate::cluster::Cluster;

/// The one disclaimer every artifact carries, emitted by [`write_evidence`] and never typed per
/// row (ADR-0031: "a hand-typed disclaimer is a disclaimer someone will eventually forget to
/// type").
pub const DISCLAIMER: &str = "Dev-host evidence. Not a production claim; production designation \
                              requires re-running on target hardware (spec §20, §12.2).";

/// Schema version of the artifact envelope (TA-61).
pub const SCHEMA: u64 = 1;

/// Environment variable that asks for a full-scale run.
pub const FULL_SCALE_ENV: &str = "RETCD_EVIDENCE";

/// Whether this process was asked to run the evidence rows at full scale.
///
/// `RETCD_EVIDENCE=1` means yes; unset, empty or `0` means the reduced scale. Only the value
/// `1` counts, so a stray `RETCD_EVIDENCE=true` does not silently promise a full-scale run the
/// gate script would then fail on.
pub fn full_scale_requested() -> bool {
    std::env::var(FULL_SCALE_ENV)
        .map(|v| v == "1")
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------------------
// The artifact
// ---------------------------------------------------------------------------------------

/// One evidence file, exactly as it is written to `docs/evidence/<name>.json`.
///
/// `deny_unknown_fields` is the point of round-tripping through this type: M6-113 rejects an
/// artifact carrying a top-level key the schema does not define, which is how a hand-edited or
/// half-migrated file is caught before someone quotes it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    /// Envelope version; always [`SCHEMA`].
    pub schema: u64,
    /// Row name, which is also the file stem (`watch-capacity` → `watch-capacity.json`).
    pub name: String,
    /// The host the numbers were measured on.
    pub host: HostInfo,
    /// The build that produced them.
    pub build: BuildInfo,
    /// When, how long, at what scale.
    pub run: RunRecord,
    /// Row-specific measurements. Never empty.
    pub values: serde_json::Value,
    /// Always [`DISCLAIMER`].
    pub disclaimer: String,
}

/// Generic host facts, captured at run time (TA-61).
///
/// Generic on purpose: enough for a reader to judge whether two artifacts are comparable, and
/// nothing that identifies a machine beyond its hostname.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostInfo {
    /// Hostname as the OS reports it.
    pub hostname: String,
    /// Operating system and architecture, e.g. `windows/x86_64`.
    pub os: String,
    /// CPU model, when a cheap source for it exists on this platform.
    pub cpu_model: Option<String>,
    /// Logical cores available to this process.
    pub cpu_cores: u32,
    /// Physical memory, when a cheap source for it exists on this platform.
    pub ram_bytes: Option<u64>,
    /// `nvme`, `ssd`, `hdd` or `unknown`. Always `unknown` here: nothing in the workspace's
    /// dependency set can classify a volume, and guessing would be worse than saying so.
    pub disk_class: String,
}

/// The build under measurement (TA-61).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildInfo {
    /// `git rev-parse HEAD`, or `unknown` outside a git checkout.
    pub git_sha: String,
    /// Whether tracked files differed from `HEAD` when the row ran. A dirty run is still
    /// evidence; it is just evidence of something not committed anywhere.
    pub dirty: bool,
    /// `debug` or `release`.
    pub profile: String,
    /// `rustc --version`, or `unknown`.
    pub rustc: String,
}

/// What the run itself did (TA-61).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRecord {
    /// ISO 8601 UTC instant the row started.
    pub utc: String,
    /// Wall-clock duration from [`RunInfo::start`] to [`write_evidence`].
    pub duration_ms: u64,
    /// The row's seed, so a replay is possible.
    pub seed: u64,
    /// `achieved / requested`: what the run reached, never what it asked for.
    pub scale_factor: f64,
    /// `achieved >= requested`. Derived, and written out, so a reader cannot miss it.
    pub full_scale: bool,
}

/// A row's run, from its first instruction to the moment its artifact is written.
///
/// Built at the top of a row, handed to [`write_evidence`] at the bottom. The scale is set with
/// [`RunInfo::scaled`] **after** the work, from the count the row actually reached — which is
/// what makes `scale_factor` a measurement rather than a restatement of the configuration.
#[derive(Debug, Clone)]
pub struct RunInfo {
    started: Instant,
    utc: String,
    seed: u64,
    requested: f64,
    achieved: f64,
}

impl RunInfo {
    /// Start timing a row seeded with `seed`.
    ///
    /// The scale defaults to `1.0 / 1.0`; a row that scales calls [`RunInfo::scaled`] before
    /// writing.
    pub fn start(seed: u64) -> Self {
        Self {
            started: Instant::now(),
            utc: iso8601_utc_now(),
            seed,
            requested: 1.0,
            achieved: 1.0,
        }
    }

    /// Record the full-scale target and what this run actually reached.
    ///
    /// `requested` is the row's full-scale configuration (1,000 streams, ~1 GiB of state);
    /// `achieved` is the number the run observed. Panics on a non-positive `requested`, because
    /// a scale factor over zero is not a number anyone should read as evidence.
    pub fn scaled(mut self, requested: f64, achieved: f64) -> Self {
        assert!(
            requested > 0.0,
            "a requested scale of {requested} cannot be divided into"
        );
        assert!(
            achieved >= 0.0,
            "an achieved scale of {achieved} is not a measurement"
        );
        self.requested = requested;
        self.achieved = achieved;
        self
    }

    /// `achieved / requested`.
    pub fn scale_factor(&self) -> f64 {
        self.achieved / self.requested
    }

    /// Whether the run reached its full-scale target.
    pub fn full_scale(&self) -> bool {
        self.achieved >= self.requested
    }

    /// How long the row has been running.
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// The seed this row was started with.
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

// ---------------------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------------------

/// `docs/evidence/`, created if it does not exist.
///
/// Resolved from this crate's manifest directory rather than from the working directory, so the
/// artifacts land in the repository whichever directory `cargo test` was invoked from.
pub fn evidence_dir() -> PathBuf {
    let dir = workspace_root().join("docs").join("evidence");
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
    dir
}

/// Write one row's artifact to `docs/evidence/<name>.json` and return the path.
///
/// The envelope — schema, host, build, run, disclaimer — is stamped here and nowhere else.
/// `values` is the row's own measurements and must be a non-empty JSON object: an artifact with
/// nothing measured in it is a file that looks like evidence and is not.
///
/// Rewrites in place (write to `<name>.json.tmp`, then rename), so a re-run overwrites rather
/// than appending and an interrupted run never leaves a half-parsed artifact behind (E2E-47).
pub fn write_evidence(name: &str, values: serde_json::Value, run: RunInfo) -> PathBuf {
    assert!(
        values.as_object().is_some_and(|o| !o.is_empty()),
        "evidence row {name} measured nothing: `values` must be a non-empty object"
    );
    let artifact = Artifact {
        schema: SCHEMA,
        name: name.to_string(),
        host: host_info(),
        build: build_info(),
        run: RunRecord {
            utc: run.utc.clone(),
            duration_ms: run.elapsed().as_millis() as u64,
            seed: run.seed,
            scale_factor: run.scale_factor(),
            full_scale: run.full_scale(),
        },
        values,
        disclaimer: DISCLAIMER.to_string(),
    };

    let dir = evidence_dir();
    let path = dir.join(format!("{name}.json"));
    let tmp = dir.join(format!("{name}.json.tmp"));
    let mut json = serde_json::to_string_pretty(&artifact)
        .unwrap_or_else(|e| panic!("serialize evidence {name}: {e}"));
    json.push('\n');
    std::fs::write(&tmp, json).unwrap_or_else(|e| panic!("write {}: {e}", tmp.display()));
    std::fs::rename(&tmp, &path).unwrap_or_else(|e| panic!("publish {}: {e}", path.display()));
    path
}

// ---------------------------------------------------------------------------------------
// Reading back
// ---------------------------------------------------------------------------------------

/// Why an artifact is not evidence.
#[derive(Debug, thiserror::Error)]
pub enum EvidenceError {
    /// The file could not be read.
    #[error("cannot read {file}: {detail}")]
    Io {
        /// The file that could not be read.
        file: String,
        /// The OS error.
        detail: String,
    },
    /// The file is not the schema — unparsable, or carrying an undefined top-level key.
    #[error("{file} does not parse as an evidence artifact: {detail}")]
    Malformed {
        /// The offending file.
        file: String,
        /// What serde said.
        detail: String,
    },
    /// The file parses but a field does not hold.
    #[error("{name}: {detail}")]
    Invalid {
        /// The artifact's row name.
        name: String,
        /// Which rule it broke.
        detail: String,
    },
}

/// Parse one artifact, rejecting unknown top-level keys (M6-113).
pub fn read_evidence(path: &Path) -> Result<Artifact, EvidenceError> {
    let bytes = std::fs::read(path).map_err(|e| EvidenceError::Io {
        file: path.display().to_string(),
        detail: e.to_string(),
    })?;
    serde_json::from_slice(&bytes).map_err(|e| EvidenceError::Malformed {
        file: path.display().to_string(),
        detail: e.to_string(),
    })
}

/// Check every TA-61 rule that a parse alone does not (M6-113).
///
/// The disclaimer must be the exact constant, the schema must be [`SCHEMA`], the build and run
/// stamps must be populated, and `values` must hold something.
pub fn validate(artifact: &Artifact) -> Result<(), EvidenceError> {
    let fail = |detail: String| {
        Err(EvidenceError::Invalid {
            name: artifact.name.clone(),
            detail,
        })
    };
    if artifact.schema != SCHEMA {
        return fail(format!("schema is {}, not {SCHEMA}", artifact.schema));
    }
    if artifact.name.trim().is_empty() {
        return fail("name is empty".to_string());
    }
    if artifact.disclaimer != DISCLAIMER {
        return fail("disclaimer is not the fixed constant".to_string());
    }
    if artifact.host.hostname.trim().is_empty() || artifact.host.os.trim().is_empty() {
        return fail("host is not identified".to_string());
    }
    if artifact.build.git_sha.trim().is_empty() {
        return fail("build.git_sha is empty".to_string());
    }
    if artifact.run.utc.trim().is_empty() {
        return fail("run.utc is empty".to_string());
    }
    if !artifact.run.scale_factor.is_finite() || artifact.run.scale_factor <= 0.0 {
        return fail(format!(
            "run.scale_factor {} is not a measurement",
            artifact.run.scale_factor
        ));
    }
    if artifact.run.full_scale != (artifact.run.scale_factor >= 1.0) {
        return fail(format!(
            "run.full_scale {} disagrees with scale_factor {}",
            artifact.run.full_scale, artifact.run.scale_factor
        ));
    }
    match artifact.values.as_object() {
        Some(o) if !o.is_empty() => Ok(()),
        _ => fail("values is empty".to_string()),
    }
}

/// Every artifact currently in `docs/evidence/`, by row name.
///
/// `.json.tmp` files are ignored: an interrupted write is not an artifact.
pub fn read_all() -> BTreeMap<String, (PathBuf, Artifact)> {
    let dir = evidence_dir();
    let mut out = BTreeMap::new();
    let entries = std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("list {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("read a docs/evidence entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let artifact = read_evidence(&path).unwrap_or_else(|e| {
            panic!(
                "{} is in docs/evidence but is not evidence: {e}",
                path.display()
            )
        });
        out.insert(artifact.name.clone(), (path, artifact));
    }
    out
}

// ---------------------------------------------------------------------------------------
// Matrix enumerators (TA-63)
// ---------------------------------------------------------------------------------------

/// One arrangement of a cluster's nodes for the partition matrix (§20, TA-63).
///
/// The three shapes are distinct *harness actions*, not just distinct sets: a two-way split and
/// an isolation coincide for three nodes but reach the cluster through different `NetFault`
/// calls, and a one-way loss is not a cut at all. Enumerating the actions is what makes the
/// matrix a matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partition {
    /// A two-way cut between two disjoint, non-empty sets covering every node.
    Split {
        /// One side.
        a: Vec<NodeId>,
        /// The other.
        b: Vec<NodeId>,
    },
    /// Traffic from one node to another is dropped; the reverse direction still flows.
    OneWay {
        /// Sender whose packets are dropped.
        from: NodeId,
        /// Intended receiver.
        to: NodeId,
    },
    /// One node is cut off from every other.
    Isolate(NodeId),
}

impl Partition {
    /// Stable identifier recorded in the artifact, e.g. `split_1|2_3`, `oneway_1_2`, `isolate_3`.
    pub fn id(&self) -> String {
        fn join(ids: &[NodeId]) -> String {
            ids.iter()
                .map(|n| n.0.to_string())
                .collect::<Vec<_>>()
                .join("_")
        }
        match self {
            Partition::Split { a, b } => format!("split_{}|{}", join(a), join(b)),
            Partition::OneWay { from, to } => format!("oneway_{}_{}", from.0, to.0),
            Partition::Isolate(id) => format!("isolate_{}", id.0),
        }
    }

    /// Which nodes this arrangement can leave without quorum, i.e. the smaller side of a cut.
    ///
    /// Empty for [`Partition::OneWay`], which never partitions the cluster by itself.
    pub fn minority(&self, total: usize) -> Vec<NodeId> {
        match self {
            Partition::Split { a, b } => {
                let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
                if small.len() * 2 < total && large.len() * 2 > total {
                    small.clone()
                } else {
                    Vec::new()
                }
            }
            Partition::OneWay { .. } => Vec::new(),
            Partition::Isolate(id) => vec![*id],
        }
    }

    /// Apply this arrangement to a running cluster. [`Cluster::heal`] undoes it.
    pub fn apply(&self, cluster: &Cluster) {
        match self {
            Partition::Split { a, b } => cluster.partition_sets(a, b),
            Partition::OneWay { from, to } => cluster.partition_one_way(*from, *to),
            Partition::Isolate(id) => cluster.isolate(*id),
        }
    }
}

/// Every partition arrangement of `ids` (§20 "every three-node partition arrangement").
///
/// For `n` nodes that is `2^(n-1) - 1` two-way splits, `n * (n - 1)` one-way losses and `n`
/// isolations — the closed form M6-107 asserts the length against, so a shape that stops being
/// generated fails the row instead of quietly shrinking the matrix.
pub fn partition_arrangements(ids: &[NodeId]) -> Vec<Partition> {
    let n = ids.len();
    let mut out = Vec::new();
    // Each non-empty proper subset containing the first node, so that a split and its mirror
    // are enumerated once rather than twice.
    for mask in 1u32..(1 << n) {
        if mask & 1 == 0 || mask == (1 << n) - 1 {
            continue;
        }
        let a: Vec<NodeId> = ids
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, id)| *id)
            .collect();
        let b: Vec<NodeId> = ids
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) == 0)
            .map(|(_, id)| *id)
            .collect();
        out.push(Partition::Split { a, b });
    }
    for from in ids {
        for to in ids {
            if from != to {
                out.push(Partition::OneWay {
                    from: *from,
                    to: *to,
                });
            }
        }
    }
    for id in ids {
        out.push(Partition::Isolate(*id));
    }
    out
}

/// How many arrangements [`partition_arrangements`] produces for `n` nodes.
///
/// Written as a closed form so M6-107 asserts the enumerator against arithmetic rather than
/// against a literal that would have to be edited whenever a shape is added.
pub fn partition_arrangement_count(n: usize) -> usize {
    ((1usize << (n - 1)) - 1) + n * (n - 1) + n
}

/// Every durability boundary, in crossing order (TA-63; `== Boundary::ALL`).
pub fn crash_cases() -> Vec<Boundary> {
    Boundary::ALL.to_vec()
}

/// One case of the §20 "Gossip and identity" security matrix (TA-63).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SecurityCase {
    /// A peer presenting a node id that is not the one it is dialling as.
    WrongNodeId,
    /// A peer from a different cluster.
    WrongClusterId,
    /// A certificate whose identity does not match the node it claims to be.
    WrongCertIdentity,
    /// An envelope addressed to a different destination than the node receiving it.
    WrongDestinationBinding,
    /// Gossip packets replayed after they stopped being current.
    StalePackets,
    /// A gossip hint naming an endpoint the node does not serve.
    PoisonedEndpoint,
    /// Two nodes claiming the same node id.
    DuplicateNodeId,
    /// A gossip key rotation across the cluster (ADR-0028).
    GossipKeyRotation,
    /// Every configured seed unreachable after the cluster has already formed.
    AllSeedsUnavailable,
    /// A voter advertising an older, or an unknown newer, schema (ADR-0030).
    VersionSkew,
    /// A peer falsely suspected of failure by gossip.
    FalseSuspicion,
    /// Traffic lost in one direction only.
    OneWayLoss,
}

impl SecurityCase {
    /// The artifact spelling of this case.
    pub const fn as_str(self) -> &'static str {
        match self {
            SecurityCase::WrongNodeId => "wrong_node_id",
            SecurityCase::WrongClusterId => "wrong_cluster_id",
            SecurityCase::WrongCertIdentity => "wrong_cert_identity",
            SecurityCase::WrongDestinationBinding => "wrong_destination_binding",
            SecurityCase::StalePackets => "stale_packets",
            SecurityCase::PoisonedEndpoint => "poisoned_endpoint",
            SecurityCase::DuplicateNodeId => "duplicate_node_id",
            SecurityCase::GossipKeyRotation => "gossip_key_rotation",
            SecurityCase::AllSeedsUnavailable => "all_seeds_unavailable",
            SecurityCase::VersionSkew => "version_skew",
            SecurityCase::FalseSuspicion => "false_suspicion",
            SecurityCase::OneWayLoss => "one_way_loss",
        }
    }
}

impl std::fmt::Display for SecurityCase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The twelve security cases (TA-63; M6-109 asserts the count).
pub fn security_cases() -> Vec<SecurityCase> {
    vec![
        SecurityCase::WrongNodeId,
        SecurityCase::WrongClusterId,
        SecurityCase::WrongCertIdentity,
        SecurityCase::WrongDestinationBinding,
        SecurityCase::StalePackets,
        SecurityCase::PoisonedEndpoint,
        SecurityCase::DuplicateNodeId,
        SecurityCase::GossipKeyRotation,
        SecurityCase::AllSeedsUnavailable,
        SecurityCase::VersionSkew,
        SecurityCase::FalseSuspicion,
        SecurityCase::OneWayLoss,
    ]
}

// ---------------------------------------------------------------------------------------
// Host, build and memory facts
// ---------------------------------------------------------------------------------------

/// Resident set size of this process, when the platform offers one for free.
///
/// Linux reads `/proc/self/statm`. Everything else — Windows included, which is where this
/// harness runs — returns `None`, because the only Windows answer is `GetProcessMemoryInfo` and
/// pulling a Win32 binding into the test toolkit to fill in one evidence field would be a
/// dependency bought for a number. A row that gets `None` records `rss_bytes: null` with a
/// `not_measured` reason and asserts its bounded-memory invariant from the server's own queue
/// accounting instead (TA-62: the server's counters are the oracle).
pub fn rss_bytes() -> Option<u64> {
    if cfg!(target_os = "linux") {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        Some(resident_pages * 4096)
    } else {
        None
    }
}

/// Why [`rss_bytes`] returned nothing on this platform, for the artifact's `values`.
pub fn rss_not_measured_reason() -> &'static str {
    "not_measured: no process-memory API in std or in the workspace dependency set on this platform"
}

/// The repository root, derived from this crate's manifest directory.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("config-testkit lives two directories below the workspace root")
        .to_path_buf()
}

/// Run a command and return its trimmed stdout, or `None` if it is unavailable or failed.
fn capture(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program)
        .args(args)
        .current_dir(workspace_root())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// Host facts, captured once per process.
pub fn host_info() -> HostInfo {
    static HOST: OnceLock<HostInfo> = OnceLock::new();
    HOST.get_or_init(|| HostInfo {
        hostname: std::env::var("COMPUTERNAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .ok()
            .or_else(|| capture("hostname", &[]))
            .unwrap_or_else(|| "unknown".to_string()),
        os: format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
        cpu_model: std::env::var("PROCESSOR_IDENTIFIER").ok(),
        cpu_cores: std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(0),
        ram_bytes: None,
        disk_class: "unknown".to_string(),
    })
    .clone()
}

/// Build facts, captured once per process.
pub fn build_info() -> BuildInfo {
    static BUILD: OnceLock<BuildInfo> = OnceLock::new();
    BUILD
        .get_or_init(|| BuildInfo {
            git_sha: capture("git", &["rev-parse", "HEAD"])
                .unwrap_or_else(|| "unknown".to_string()),
            dirty: capture("git", &["status", "--porcelain", "--untracked-files=no"]).is_some(),
            profile: if cfg!(debug_assertions) {
                "debug".to_string()
            } else {
                "release".to_string()
            },
            rustc: capture("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string()),
        })
        .clone()
}

/// `1970-01-01T00:00:00Z`-style stamp for the current instant.
///
/// Hand-rolled rather than pulled from a date crate: the workspace has no date dependency, and
/// an evidence stamp needs to be readable, not locale-aware.
fn iso8601_utc_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (days, rem) = ((secs / 86_400) as i64, secs % 86_400);
    let (hour, minute, second) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Howard Hinnant's `civil_from_days`: days since the Unix epoch to a proleptic Gregorian date.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
