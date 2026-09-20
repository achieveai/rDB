//! The on-disk snapshot format, its policy knobs, and the metrics a store exposes about it
//! (M5, ADR-0022; spec §12.1, §17, §19.7).
//!
//! This module owns the *file*: how a snapshot is framed, hashed, validated and named. It owns
//! no database. [`crate::rocks`] drives it — capture a view, stream records out, publish, or
//! validate an incoming file and stream it back in — so the format can be read by a tool that
//! has no RocksDB handle at all (which is what `config-server backup` / `verify-backup` /
//! `restore` are, ADR-0024).
//!
//! # Layout
//!
//! ```text
//! [u32 LE header_len][postcard(SnapshotHeader)]
//! repeat header.total_records() times:
//!     [u32 LE frame_len][postcard(SnapshotRecord)]
//! [32 bytes sha256 over every byte above]
//! ```
//!
//! There is no end-of-records sentinel and none is needed: `header.counts` says exactly how
//! many frames follow, so a truncated file is caught by the record loop *and* by the trailer.
//!
//! # Why the body is column-family generic
//!
//! [`SnapshotHeader::cfs`] lists the exported column families in export order and
//! [`SnapshotHeader::counts`] is keyed by name; a [`SnapshotRecord`] carries an index into
//! `cfs`, not a hard-coded tag. The exported set is discovered at build time as "every column
//! family that holds replicated state machine data" — see [`is_snapshot_data_cf`] — so when a
//! later milestone adds one (M5's `dedup`, ADR-0025), it is exported, validated, transferred
//! and installed with **no change to this format and no change to any reader**.
//!
//! # Why `state_meta` is not in the body
//!
//! Deviation from ADR-0022's original "cf ∈ {kv, state_meta, events, dedup}", noted in that ADR
//! on 2026-09-18 and accepted by the lead. Every value an install or a restore needs out of
//! `state_meta` is in the header — `last_applied`, `membership`, `cluster_revision`,
//! `compact_revision` — and those are exactly the values install writes in its final synced
//! batch. Streaming `state_meta` raw would additionally carry the **source node's**
//! `state_meta/identity` into the file, which an install must never write and a restore must
//! always replace (ADR-0011 identity binding, ADR-0024 fenced restore). Excluding it removes
//! the possibility rather than relying on every future reader to remember to skip it.
//!
//! The one `state_meta` value that *must* cross is the retired-node set, and it crosses in the
//! header as [`SnapshotHeader::retired_nodes`] rather than as raw bytes (ruling M5-R21, finding
//! C5B-18). It is replicated state, so a node that learned its membership from a snapshot has
//! to learn the retirements with it or the ADR-0023 fence silently lapses on exactly the node
//! least able to notice.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use config_core::{ClusterId, NodeId, RecoveryEpoch};
use openraft::{LogId, SnapshotMeta, StoredMembership};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::types::{RaftNode, RaftNodeId};

/// Subdirectory of the data directory that holds every snapshot artefact.
pub const SNAPSHOT_DIR: &str = "snapshots";

/// Extension of a published, immutable snapshot.
pub const SNAP_EXT: &str = "snap";
/// Extension of a snapshot this node is still building.
pub const TMP_EXT: &str = "tmp";
/// Extension of a snapshot this node is receiving from a leader.
pub const RECV_EXT: &str = "recv.tmp";
/// Prefix of the checkpoint directory a build reads its consistent view through.
pub const BUILD_VIEW_PREFIX: &str = "build-";

/// Column families that are **not** state-machine data and are therefore never exported.
///
/// `raft_log` and `raft_meta` belong to the log store, which a snapshot deliberately does not
/// carry (openraft rebuilds the log relationship from `meta.last_log_id`); `state_meta` is
/// excluded for the identity reason in the module docs; `default` is RocksDB's own and is
/// unused by rEtcd but cannot be dropped.
const NON_DATA_CFS: [&str; 4] = ["default", "raft_log", "raft_meta", "state_meta"];

/// Whether a column family's contents belong in a snapshot body.
///
/// The rule is stated as an exclusion rather than an inclusion list on purpose: a milestone
/// that adds a replicated state-machine column family must get it into snapshots by default,
/// and a milestone that adds a *node-local* one has to say so here, in one place, next to the
/// reason.
pub fn is_snapshot_data_cf(name: &str) -> bool {
    !NON_DATA_CFS.contains(&name)
}

/// Snapshot and log-purge policy (ADR-0022 "Policy values changed together", research §4.5).
///
/// The three OpenRaft knobs move together or not at all — see [`SnapshotConfig::validate`] for
/// why a half-applied change is a silent no-op rather than an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotConfig {
    /// `SnapshotPolicy::LogsSinceLast(n)`: build once committed has advanced this far past the
    /// current snapshot's last log id. `0` means "never build" — see
    /// [`SnapshotConfig::DISABLED`].
    pub logs_since_last: u64,
    /// OpenRaft's `max_in_snapshot_log_to_keep`: how many *snapshot-covered* entries to retain
    /// rather than purge. `u64::MAX` makes `calc_purge_upto` saturate to zero and purge
    /// nothing, which is the M0–M4 latch.
    pub logs_to_keep: u64,
    /// OpenRaft's `purge_batch_size`: a **minimum** batch, not a chunk size. Purge is skipped
    /// until at least this many entries would go.
    pub purge_batch_size: u64,
    /// How many published `.snap` files to keep, including the current one.
    pub retain_snapshots: usize,
}

impl SnapshotConfig {
    /// The M5 production profile (ADR-0022, D5.1).
    pub const DEFAULT: Self = Self {
        logs_since_last: 5_000,
        logs_to_keep: 1_000,
        purge_batch_size: 1,
        retain_snapshots: 2,
    };

    /// The M0–M4 latch: never build, never purge.
    ///
    /// Both halves are load-bearing and are why this is a named constant rather than two
    /// numbers a caller could set independently: `logs_since_last == 0` maps to
    /// `SnapshotPolicy::Never`, and `logs_to_keep == u64::MAX` makes `calc_purge_upto`'s
    /// `saturating_sub` produce `purge_end == 0` so no purge is ever scheduled even if a
    /// snapshot somehow appeared (research §4.5).
    pub const DISABLED: Self = Self {
        logs_since_last: 0,
        logs_to_keep: u64::MAX,
        purge_batch_size: 1,
        retain_snapshots: 2,
    };

    /// Whether this configuration builds snapshots at all.
    pub const fn enabled(&self) -> bool {
        self.logs_since_last > 0
    }

    /// Reject a configuration whose three knobs disagree (test plan M5-21).
    ///
    /// The failure this exists to prevent is not a crash, it is *silence*: `LogsSinceLast(n)`
    /// with `logs_to_keep == u64::MAX` builds snapshots forever and purges nothing, with no
    /// error anywhere and an unbounded log (research §4.5, test plan M5-22 documents the trap
    /// deliberately at the store level). The inverse — a finite `logs_to_keep` with
    /// `SnapshotPolicy::Never` — would let a purge be scheduled against a snapshot the store
    /// cannot build, which trap T10 turns into a dead leader.
    pub fn validate(&self) -> Result<(), SnapshotConfigError> {
        if self.enabled() != (self.logs_to_keep != u64::MAX) {
            return Err(SnapshotConfigError::PolicyLatchMismatch {
                logs_since_last: self.logs_since_last,
                logs_to_keep: self.logs_to_keep,
                purge_batch_size: self.purge_batch_size,
            });
        }
        if self.purge_batch_size == 0 {
            return Err(SnapshotConfigError::ZeroPurgeBatchSize);
        }
        if self.enabled() && self.retain_snapshots == 0 {
            return Err(SnapshotConfigError::ZeroRetainSnapshots);
        }
        Ok(())
    }
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Why a [`SnapshotConfig`] was refused (test plan M5-21).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotConfigError {
    /// The build policy and the purge retention disagree, which is a silent no-op in one
    /// direction and an unservable purge in the other. Names all three fields, because the fix
    /// is always to move them together.
    #[error(
        "snapshot policy and purge retention disagree: logs_since_last={logs_since_last}, \
         logs_to_keep={logs_to_keep}, purge_batch_size={purge_batch_size}; \
         snapshots must be enabled (logs_since_last > 0) if and only if \
         logs_to_keep != u64::MAX"
    )]
    PolicyLatchMismatch {
        /// `SnapshotPolicy::LogsSinceLast` threshold; `0` means `Never`.
        logs_since_last: u64,
        /// `max_in_snapshot_log_to_keep`; `u64::MAX` means "never purge".
        logs_to_keep: u64,
        /// `purge_batch_size`, reported so the whole triple is visible at the point of failure.
        purge_batch_size: u64,
    },

    /// `purge_batch_size` of `0` is not OpenRaft's "no minimum", it is an arithmetic trap:
    /// `last_purged.next_index() + 0 > purge_end` changes the comparison the whole purge
    /// schedule rests on.
    #[error("purge_batch_size must be at least 1")]
    ZeroPurgeBatchSize,

    /// Retaining zero snapshots would delete the file that was just published, which is the
    /// one `get_current_snapshot` has to be able to open (trap T10).
    #[error("retain_snapshots must be at least 1 when snapshots are enabled")]
    ZeroRetainSnapshots,
}

/// The header of a snapshot file (ADR-0022 "File format").
///
/// Everything needed to decide whether this file may be installed here, and everything install
/// writes into `state_meta` afterwards. It is read before a single record is decoded, so a
/// foreign or unreadable snapshot is refused before it can touch any column family.
///
/// # Field order is the format
///
/// `postcard` is positional and not self-describing, so this declaration *is* the on-disk
/// layout: a field may be appended at the end, never inserted or reordered.
/// [`SnapshotHeader::format_version`] cannot gate its own decode — it lives inside the struct
/// being decoded — so a header written before a field was appended does not report an
/// unsupported format, it runs out of bytes and is refused as
/// [`SnapshotFileError::Malformed`] by [`SnapshotReader::open`]. That is a typed refusal
/// before anything is touched, which is the property that matters; it is not a compatibility
/// story, and there is none to keep, because snapshots and `FORMAT_VERSION = 3` both first
/// exist in M5 (ADR-0022 note 5) — no earlier build ever wrote one of these files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotHeader {
    /// [`crate::FORMAT_VERSION`] at export time (ADR-0021).
    pub format_version: u32,
    /// `config_core::COMMAND_ENVELOPE_VERSION` at export time (ADR-0007/0019).
    pub command_schema: u16,
    /// The cluster this snapshot belongs to. A mismatch is refused (ADR-0011).
    pub cluster_id: ClusterId,
    /// The recovery epoch this snapshot belongs to. A mismatch is refused: a restored cluster's
    /// snapshot must never install into the cluster it was restored from (spec §14.4).
    pub recovery_epoch: RecoveryEpoch,
    /// Which node built it. Diagnostics and the backup manifest only — never a gate.
    pub built_by: RaftNodeId,
    /// Unique per build, even for an identical `last_log_id` (research §1.2).
    pub snapshot_id: String,
    /// Logs up to and including this id are covered.
    pub last_log_id: Option<LogId<RaftNodeId>>,
    /// The state machine's applied pointer at capture. Equal to `last_log_id` for a snapshot
    /// built by this store; carried separately because OpenRaft's contract distinguishes them.
    pub last_applied: Option<LogId<RaftNodeId>>,
    /// The last applied membership at capture.
    pub membership: StoredMembership<RaftNodeId, RaftNode>,
    /// The public revision at capture.
    pub cluster_revision: u64,
    /// The retained-history watermark at capture (M4, ADR-0019).
    pub compact_revision: u64,
    /// Exported column families, in export order. A [`SnapshotRecord::cf`] indexes this.
    pub cfs: Vec<String>,
    /// Record count per column family, keyed by name. Keys are exactly [`SnapshotHeader::cfs`].
    pub counts: BTreeMap<String, u64>,
    /// Total key + value payload bytes across every record.
    ///
    /// Deviation from ADR-0022's "bytes", noted there on 2026-09-18: the framed file size
    /// cannot be known before the header is written, whereas the payload total can be taken in
    /// the same cheap counting pre-pass that fills `counts`. Framing errors and truncation are
    /// caught by the trailer and by the record count, so nothing is lost by measuring the
    /// payload instead of the frame.
    pub bytes: u64,
    /// Wall-clock build time, for snapshot-age metrics and the backup manifest.
    pub created_unix_ms: u64,
    /// The retired-node set at capture (M5, ADR-0023; ruling M5-R21, finding C5B-18).
    ///
    /// The only `state_meta` value carried by a snapshot, and it is carried because it is the
    /// only one whose absence is *silent*: a node caught up by an install has the membership
    /// the snapshot names but, without this, none of the retirements that produced it, so
    /// `is_retired` answers `false` for an identity the cluster has permanently fenced and the
    /// node re-admits it at the peer plane and through `AddLearner`.
    ///
    /// Appended last, and consumed by unioning it into the receiver's set — never by replacing
    /// it - so an install can only ever widen the fence (see `rocks::apply_snapshot_records`).
    pub retired_nodes: BTreeSet<NodeId>,
    /// The highest `command_schema` the captured state had ever applied (M6, ADR-0030 M6-R15).
    ///
    /// Carried for the same reason as `retired_nodes`, and consumed the same way - by `max`
    /// rather than by assignment. A node caught up by an install has the *state* a schema-2
    /// command produced; without this it would not have the *proof* that the cluster is past
    /// activation, and the gate would refuse the next such command whenever a voter happened
    /// to be unreachable.
    ///
    /// Appended last. `command_schema` above is the builder's envelope generation, which is a
    /// property of the binary; this is a property of the applied state.
    pub max_applied_command_schema: u16,
}

impl SnapshotHeader {
    /// Total records the body must contain.
    pub fn total_records(&self) -> u64 {
        self.counts.values().sum()
    }

    /// The OpenRaft `SnapshotMeta` this file represents.
    pub fn meta(&self) -> SnapshotMeta<RaftNodeId, RaftNode> {
        SnapshotMeta {
            last_log_id: self.last_log_id,
            last_membership: self.membership.clone(),
            snapshot_id: self.snapshot_id.clone(),
        }
    }
}

/// One exported key/value pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    /// Index into [`SnapshotHeader::cfs`].
    pub cf: u16,
    /// The raw column-family key.
    pub key: Vec<u8>,
    /// The raw stored value, exactly as the column family holds it — **not** re-encoded.
    /// Re-encoding would make the snapshot depend on this build's ability to decode every
    /// stored type, which is precisely what `format_version` exists to avoid having to assume.
    pub value: Vec<u8>,
}

/// What `state_meta/current_snapshot` holds.
///
/// The header is authoritative about the file's contents; this is authoritative about *which*
/// file is current, which is the thing a purge depends on and the thing that must survive a
/// restart (ADR-0022 publish ordering step 5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSnapshot {
    /// The snapshot's id; also the stem of its file name.
    pub snapshot_id: String,
    /// Logs up to and including this id are covered. The purge bound.
    pub last_log_id: Option<LogId<RaftNodeId>>,
    /// The membership the snapshot carries.
    pub membership: StoredMembership<RaftNodeId, RaftNode>,
    /// File name within `<data_dir>/snapshots/`, not a full path: a data directory that is
    /// moved or restored under a different path must still find its own snapshot.
    pub file_name: String,
    /// Size of the published file in bytes, for the snapshot-size metric.
    pub size_bytes: u64,
    /// When it was built, for the snapshot-age metric.
    pub created_unix_ms: u64,
}

impl StoredSnapshot {
    /// The OpenRaft `SnapshotMeta` for this snapshot.
    pub fn meta(&self) -> SnapshotMeta<RaftNodeId, RaftNode> {
        SnapshotMeta {
            last_log_id: self.last_log_id,
            last_membership: self.membership.clone(),
            snapshot_id: self.snapshot_id.clone(),
        }
    }

    /// The index this snapshot covers up to, or `0` when it covers nothing.
    pub fn covered_index(&self) -> u64 {
        self.last_log_id.map_or(0, |l| l.index)
    }
}

/// Why a snapshot file could not be written, read, or accepted.
///
/// Every variant names what specifically was wrong, because all of them end up in front of an
/// operator as the reason a node refused a snapshot, and "corrupt snapshot" is not an
/// actionable sentence (ADR-0022 Validation matrix).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotFileError {
    /// The file was written by a build with a different on-disk format (ADR-0021, §17).
    #[error("snapshot format version {found}; this build supports only version {supported}")]
    UnsupportedFormat {
        /// The version stamped in the header.
        found: u32,
        /// The only version this build reads and writes.
        supported: u32,
    },

    /// The file carries commands encoded with a newer envelope schema than this build decodes
    /// (ADR-0007/0019, §17).
    #[error("snapshot command schema {found}; this build supports at most {supported}")]
    UnsupportedCommandSchema {
        /// The schema stamped in the header.
        found: u16,
        /// The newest schema this build understands.
        supported: u16,
    },

    /// The snapshot belongs to a different cluster or a different recovery epoch (ADR-0011,
    /// spec §14.4, §19.10, §19.11).
    #[error(
        "snapshot identity mismatch: snapshot [{found_cluster}/{found_epoch}] \
         node [{expected_cluster}/{expected_epoch}]"
    )]
    IdentityMismatch {
        /// The cluster id in the header.
        found_cluster: ClusterId,
        /// The recovery epoch in the header.
        found_epoch: RecoveryEpoch,
        /// This node's bound cluster id.
        expected_cluster: ClusterId,
        /// This node's bound recovery epoch.
        expected_epoch: RecoveryEpoch,
    },

    /// The trailer's sha256 does not match the bytes that preceded it.
    #[error("snapshot checksum mismatch in {file}: computed {computed}, trailer {stored}")]
    ChecksumMismatch {
        /// The file, so an operator can find it.
        file: String,
        /// Lowercase hex of what the bytes actually hash to.
        computed: String,
        /// Lowercase hex of what the trailer claims.
        stored: String,
    },

    /// The decoded record count or payload size disagrees with the header.
    ///
    /// The checksum would also catch this; a separate error exists because a count mismatch is
    /// diagnosable (a truncated transfer, a column family that changed under the builder) and a
    /// checksum failure is not.
    #[error("snapshot {what} mismatch in {file}: header says {expected}, body has {found}")]
    CountMismatch {
        /// The file.
        file: String,
        /// Which quantity disagreed: `"record count"`, `"payload bytes"`, or a CF name.
        what: String,
        /// What the header claimed.
        expected: u64,
        /// What the body actually held.
        found: u64,
    },

    /// A column family named in the header does not exist in this store.
    #[error("snapshot names column family {name:?} which this build does not have")]
    UnknownColumnFamily {
        /// The unknown name.
        name: String,
    },

    /// The framing, the header, or a record did not decode.
    #[error("malformed snapshot {file}: {detail}")]
    Malformed {
        /// The file.
        file: String,
        /// What went wrong, as specifically as the decoder could say.
        detail: String,
    },

    /// The filesystem refused.
    #[error("snapshot io error on {file}: {detail}")]
    Io {
        /// The file or directory.
        file: String,
        /// The operating system's description.
        detail: String,
    },
}

impl SnapshotFileError {
    /// A short, stable snake_case reason for the `reason=` log field (test plan M5-34/M5-35).
    pub const fn reason(&self) -> &'static str {
        match self {
            SnapshotFileError::UnsupportedFormat { .. } => "snapshot_format_mismatch",
            SnapshotFileError::UnsupportedCommandSchema { .. } => "snapshot_schema_mismatch",
            SnapshotFileError::IdentityMismatch { .. } => "snapshot_identity_mismatch",
            SnapshotFileError::ChecksumMismatch { .. } => "snapshot_checksum_mismatch",
            SnapshotFileError::CountMismatch { .. } => "snapshot_count_mismatch",
            SnapshotFileError::UnknownColumnFamily { .. } => "snapshot_unknown_column_family",
            SnapshotFileError::Malformed { .. } => "snapshot_malformed",
            SnapshotFileError::Io { .. } => "snapshot_io",
        }
    }
}

/// Helper: render a path for an error message without panicking on non-UTF-8.
fn show(path: &Path) -> String {
    path.display().to_string()
}

fn io_err(path: &Path, e: std::io::Error) -> SnapshotFileError {
    SnapshotFileError::Io {
        file: show(path),
        detail: e.to_string(),
    }
}

/// `<data_dir>/snapshots`.
pub fn snapshot_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(SNAPSHOT_DIR)
}

/// `<data_dir>/snapshots/<id>.snap`.
pub fn snap_path(data_dir: &Path, snapshot_id: &str) -> PathBuf {
    snapshot_dir(data_dir).join(format!("{snapshot_id}.{SNAP_EXT}"))
}

/// `<data_dir>/snapshots/<id>.tmp`.
pub fn tmp_path(data_dir: &Path, snapshot_id: &str) -> PathBuf {
    snapshot_dir(data_dir).join(format!("{snapshot_id}.{TMP_EXT}"))
}

/// `<data_dir>/snapshots/<id>.recv.tmp`.
pub fn recv_path(data_dir: &Path, snapshot_id: &str) -> PathBuf {
    snapshot_dir(data_dir).join(format!("{snapshot_id}.{RECV_EXT}"))
}

/// `<data_dir>/snapshots/build-<id>` — the checkpoint a build reads its consistent view from.
pub fn build_view_path(data_dir: &Path, snapshot_id: &str) -> PathBuf {
    snapshot_dir(data_dir).join(format!("{BUILD_VIEW_PREFIX}{snapshot_id}"))
}

/// Milliseconds since the Unix epoch, saturating rather than panicking on a clock before 1970.
pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// `"<last_log_index>-<term>-<unix_ms>"` (ADR-0022).
///
/// The timestamp is what makes it unique per build. Research §1.2: two snapshots with the same
/// `last_log_id` may still differ in bytes, and OpenRaft uses `snapshot_id` for in-flight
/// snapshot identity, so deriving it from `last_log_id` alone would make two different files
/// claim to be the same snapshot mid-transfer.
pub fn snapshot_id(last_log_id: Option<LogId<RaftNodeId>>, built_at_ms: u64) -> String {
    let (index, term) = last_log_id.map_or((0, 0), |l| (l.index, l.leader_id.term));
    format!("{index}-{term}-{built_at_ms}")
}

/// Every published snapshot file in the directory, newest first.
///
/// "Newest" is by the `created_unix_ms` suffix of the id, falling back to the file name, so the
/// order is a property of the names and needs no filesystem timestamps (which a restore or a
/// backup tool would not preserve).
pub fn list_snapshots(data_dir: &Path) -> Result<Vec<(String, PathBuf)>, SnapshotFileError> {
    let dir = snapshot_dir(data_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err(&dir, e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io_err(&dir, e))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(&format!(".{SNAP_EXT}")) else {
            continue;
        };
        // `<id>.recv.tmp` also ends in a suffix we do not want to mistake for a publication;
        // it cannot reach here because it ends in `.tmp`, but the stem check keeps the
        // intent obvious to the next reader.
        if stem.is_empty() {
            continue;
        }
        out.push((stem.to_string(), path));
    }
    out.sort_by(|a, b| id_sort_key(&b.0).cmp(&id_sort_key(&a.0)));
    Ok(out)
}

/// Sort key for a snapshot id: `(created_ms, index, term, raw)`.
fn id_sort_key(id: &str) -> (u64, u64, u64, String) {
    let parts: Vec<&str> = id.split('-').collect();
    let num = |i: usize| {
        parts
            .get(i)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    };
    (num(2), num(0), num(1), id.to_string())
}

/// Streaming writer for one snapshot file: frames, hashes, and counts as it goes.
///
/// It never buffers the body — that is the whole point of `SnapshotData = tokio::fs::File`
/// (research trap T4) and would be undone by a writer that materialised the records first.
pub struct SnapshotWriter {
    path: PathBuf,
    out: BufWriter<std::fs::File>,
    hasher: Sha256,
    records_written: u64,
    payload_bytes: u64,
}

impl fmt::Debug for SnapshotWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotWriter")
            .field("path", &self.path)
            .field("records_written", &self.records_written)
            .finish()
    }
}

impl SnapshotWriter {
    /// Create `path`, truncating any leftover from an earlier interrupted build, and write the
    /// header.
    pub fn create(path: &Path, header: &SnapshotHeader) -> Result<Self, SnapshotFileError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
        }
        let file = std::fs::File::create(path).map_err(|e| io_err(path, e))?;
        let mut writer = Self {
            path: path.to_path_buf(),
            out: BufWriter::new(file),
            hasher: Sha256::new(),
            records_written: 0,
            payload_bytes: 0,
        };
        let encoded = postcard::to_stdvec(header).map_err(|e| SnapshotFileError::Malformed {
            file: show(path),
            detail: format!("cannot encode header: {e}"),
        })?;
        writer.write_framed(&encoded)?;
        Ok(writer)
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), SnapshotFileError> {
        self.hasher.update(bytes);
        self.out
            .write_all(bytes)
            .map_err(|e| io_err(&self.path, e))?;
        Ok(())
    }

    fn write_framed(&mut self, bytes: &[u8]) -> Result<(), SnapshotFileError> {
        let len = u32::try_from(bytes.len()).map_err(|_| SnapshotFileError::Malformed {
            file: show(&self.path),
            detail: format!("frame of {} bytes exceeds u32", bytes.len()),
        })?;
        self.write_all(&len.to_le_bytes())?;
        self.write_all(bytes)
    }

    /// Append one record.
    pub fn write_record(
        &mut self,
        cf: u16,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), SnapshotFileError> {
        let record = SnapshotRecord {
            cf,
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let encoded = postcard::to_stdvec(&record).map_err(|e| SnapshotFileError::Malformed {
            file: show(&self.path),
            detail: format!("cannot encode record: {e}"),
        })?;
        self.write_framed(&encoded)?;
        self.records_written += 1;
        self.payload_bytes += (key.len() + value.len()) as u64;
        Ok(())
    }

    /// Records appended so far.
    pub fn records_written(&self) -> u64 {
        self.records_written
    }

    /// Key + value payload bytes appended so far.
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Write the trailer and flush to the OS. Returns the open file so the caller can decide
    /// *when* to fsync — which is a durability boundary the caller must be able to crash at
    /// ([`crate::Boundary::BeforeSnapshotTmpSync`]), not something this writer may do on its
    /// own.
    pub fn finish(mut self) -> Result<(std::fs::File, [u8; 32]), SnapshotFileError> {
        let digest: [u8; 32] = self.hasher.clone().finalize().into();
        self.write_all(&digest)?;
        let mut file = self
            .out
            .into_inner()
            .map_err(|e| io_err(&self.path, e.into_error()))?;
        file.flush().map_err(|e| io_err(&self.path, e))?;
        Ok((file, digest))
    }
}

/// Streaming reader: header first, then records, then the trailer check.
pub struct SnapshotReader {
    path: PathBuf,
    file: std::io::BufReader<std::fs::File>,
    hasher: Sha256,
    header: SnapshotHeader,
    records_read: u64,
    payload_bytes: u64,
}

impl fmt::Debug for SnapshotReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotReader")
            .field("path", &self.path)
            .field("snapshot_id", &self.header.snapshot_id)
            .field("records_read", &self.records_read)
            .finish()
    }
}

impl SnapshotReader {
    /// Open `path` and decode its header.
    ///
    /// The header alone is enough to refuse a foreign, too-new, or unreadable snapshot, which
    /// is why it is a separate step from [`SnapshotReader::verify_to_end`]: ADR-0022 requires
    /// identity and version refusals to happen before anything is written, and a caller that
    /// had to read the whole body first could not offer that.
    pub fn open(path: &Path) -> Result<Self, SnapshotFileError> {
        let file = std::fs::File::open(path).map_err(|e| io_err(path, e))?;
        let mut file = std::io::BufReader::new(file);
        let mut hasher = Sha256::new();
        let bytes = read_framed(path, &mut file, &mut hasher)?;
        let header: SnapshotHeader =
            postcard::from_bytes(&bytes).map_err(|e| SnapshotFileError::Malformed {
                file: show(path),
                detail: format!("cannot decode header: {e}"),
            })?;
        if header.counts.len() != header.cfs.len()
            || header.cfs.iter().any(|cf| !header.counts.contains_key(cf))
        {
            return Err(SnapshotFileError::Malformed {
                file: show(path),
                detail: "header cfs and counts disagree".to_string(),
            });
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
            hasher,
            header,
            records_read: 0,
            payload_bytes: 0,
        })
    }

    /// The decoded header.
    pub fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    /// The file this reader is reading.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_framed(&mut self) -> Result<Vec<u8>, SnapshotFileError> {
        let path = self.path.clone();
        read_framed(&path, &mut self.file, &mut self.hasher)
    }

    /// Decode the next record, or `None` once `header.total_records()` have been read.
    pub fn next_record(&mut self) -> Result<Option<SnapshotRecord>, SnapshotFileError> {
        if self.records_read >= self.header.total_records() {
            return Ok(None);
        }
        let bytes = self.read_framed()?;
        let record: SnapshotRecord =
            postcard::from_bytes(&bytes).map_err(|e| SnapshotFileError::Malformed {
                file: show(&self.path),
                detail: format!("cannot decode record {}: {e}", self.records_read),
            })?;
        if usize::from(record.cf) >= self.header.cfs.len() {
            return Err(SnapshotFileError::Malformed {
                file: show(&self.path),
                detail: format!(
                    "record {} names column family index {} but the header lists {}",
                    self.records_read,
                    record.cf,
                    self.header.cfs.len()
                ),
            });
        }
        self.records_read += 1;
        self.payload_bytes += (record.key.len() + record.value.len()) as u64;
        Ok(Some(record))
    }

    /// Consume the trailer and verify the digest, the record count and the payload size.
    ///
    /// Must be called after the last [`SnapshotReader::next_record`]; a caller that installs
    /// records without reaching this has installed unverified bytes, which is why install
    /// verifies the **whole file** before it writes anything (ADR-0022 Validation matrix).
    pub fn verify_to_end(mut self) -> Result<(), SnapshotFileError> {
        while self.next_record()?.is_some() {}
        if self.records_read != self.header.total_records() {
            return Err(SnapshotFileError::CountMismatch {
                file: show(&self.path),
                what: "record count".to_string(),
                expected: self.header.total_records(),
                found: self.records_read,
            });
        }
        if self.payload_bytes != self.header.bytes {
            return Err(SnapshotFileError::CountMismatch {
                file: show(&self.path),
                what: "payload bytes".to_string(),
                expected: self.header.bytes,
                found: self.payload_bytes,
            });
        }
        // The trailer is the only part of the file that is *not* folded into the digest.
        let computed: [u8; 32] = self.hasher.clone().finalize().into();
        let mut stored = [0u8; 32];
        self.file
            .read_exact(&mut stored)
            .map_err(|e| SnapshotFileError::Malformed {
                file: show(&self.path),
                detail: format!("cannot read trailer: {e}"),
            })?;
        if computed != stored {
            return Err(SnapshotFileError::ChecksumMismatch {
                file: show(&self.path),
                computed: hex32(&computed),
                stored: hex32(&stored),
            });
        }
        let mut extra = [0u8; 1];
        match self.file.read(&mut extra) {
            Ok(0) => Ok(()),
            Ok(_) => Err(SnapshotFileError::Malformed {
                file: show(&self.path),
                detail: "trailing bytes after the snapshot trailer".to_string(),
            }),
            Err(e) => Err(io_err(&self.path, e)),
        }
    }
}

/// Read one `[u32 LE len][payload]` frame, folding every byte into `hasher`.
fn read_framed(
    path: &Path,
    file: &mut std::io::BufReader<std::fs::File>,
    hasher: &mut Sha256,
) -> Result<Vec<u8>, SnapshotFileError> {
    let mut read_exact = |len: usize, hasher: &mut Sha256| -> Result<Vec<u8>, SnapshotFileError> {
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .map_err(|e| SnapshotFileError::Malformed {
                file: show(path),
                detail: format!("expected {len} more bytes: {e}"),
            })?;
        hasher.update(&buf);
        Ok(buf)
    };
    let len_bytes = read_exact(4, hasher)?;
    let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
    read_exact(len as usize, hasher)
}

/// Lowercase hex of a 32-byte digest.
pub fn hex32(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 0x0f)] as char);
    }
    out
}

/// State-meta keys this module reads directly when it opens a store **offline** (ADR-0024).
///
/// A deliberate, documented mirror of the private constants in [`crate::rocks`]. The offline
/// exporter runs without a `RocksStore` — that is the whole point, because opening one takes
/// the directory lock and replays an interrupted install — so it cannot borrow them. They are
/// part of the on-disk format, which ADR-0021 already fixes, so mirroring them here is no
/// weaker a promise than the file format itself. A change to either side is caught by
/// `offline_export_matches_the_online_build` in `tests/rocks.rs`.
mod offline_keys {
    pub(super) const IDENTITY: &[u8] = b"identity";
    pub(super) const LAST_APPLIED: &[u8] = b"last_applied";
    pub(super) const MEMBERSHIP: &[u8] = b"membership";
    pub(super) const CLUSTER_REVISION: &[u8] = b"cluster_revision";
    pub(super) const COMPACT_REVISION: &[u8] = b"compact_revision";
    pub(super) const RETIRED_NODES: &[u8] = b"retired_nodes";
    pub(super) const MAX_COMMAND_SCHEMA: &[u8] = b"max_command_schema";
}

/// Read one postcard-encoded `state_meta` value out of an offline store.
fn offline_meta<T: serde::de::DeserializeOwned>(
    db: &rocksdb::DB,
    path: &Path,
    key: &[u8],
) -> Result<Option<T>, SnapshotFileError> {
    let malformed = |detail: String| SnapshotFileError::Malformed {
        file: show(path),
        detail,
    };
    let handle = db
        .cf_handle(crate::CF_STATE_META)
        .ok_or_else(|| malformed(format!("no {} column family", crate::CF_STATE_META)))?;
    match db
        .get_cf(handle, key)
        .map_err(|e| malformed(format!("cannot read state_meta: {e}")))?
    {
        None => Ok(None),
        Some(bytes) => postcard::from_bytes(&bytes).map(Some).map_err(|e| {
            malformed(format!(
                "state_meta/{} did not decode: {e}",
                String::from_utf8_lossy(key)
            ))
        }),
    }
}

/// Export a **closed** store's applied state into a snapshot file at `out_path` (ADR-0024).
///
/// This is the offline half of the backup path: `config-server backup` runs it against a data
/// directory with no node attached, so a backup can be taken from a stopped node, from a
/// restored copy of one, or from a directory that was simply moved off the host.
///
/// It deliberately does **not** open a [`crate::RocksStore`]. Opening one would take the
/// directory lock, rewrite `state_meta` on a version bump and redo an interrupted snapshot
/// install — three side effects a backup has no business having. RocksDB is opened read-only
/// instead, which also means this can run against a directory another process has open, at the
/// cost of seeing that process's last flushed state rather than its memtables.
///
/// The exported set is discovered, not hard-coded ([`is_snapshot_data_cf`]), so this produces
/// byte-identical output to the online builder for the same applied state, including any
/// column family a later milestone adds. What it cannot reproduce is a *checkpoint*: an online
/// build snapshots the database first, this one reads it live. With no writer attached there
/// is nothing to be inconsistent with.
///
/// Returns the header it wrote, which carries everything the backup manifest needs — cluster
/// id, recovery epoch, revision, record counts — and whose [`SnapshotHeader::meta`] is the
/// OpenRaft `SnapshotMeta`.
pub fn export_snapshot(
    data_dir: &Path,
    out_path: &Path,
) -> Result<SnapshotHeader, SnapshotFileError> {
    let opts = rocksdb::Options::default();
    let cf_names = rocksdb::DB::list_cf(&opts, data_dir).map_err(|e| SnapshotFileError::Io {
        file: show(data_dir),
        detail: format!("cannot list column families: {e}"),
    })?;
    let db =
        rocksdb::DB::open_cf_for_read_only(&opts, data_dir, &cf_names, false).map_err(|e| {
            SnapshotFileError::Io {
                file: show(data_dir),
                detail: format!("cannot open the store read-only: {e}"),
            }
        })?;

    let identity: config_core::ClusterIdentity =
        offline_meta(&db, data_dir, offline_keys::IDENTITY)?.ok_or_else(|| {
            SnapshotFileError::Malformed {
                file: show(data_dir),
                detail: "state_meta/identity is absent: this directory has never been bound to \
                         a cluster, so there is nothing to back up"
                    .to_string(),
            }
        })?;
    let last_applied: Option<LogId<RaftNodeId>> =
        offline_meta(&db, data_dir, offline_keys::LAST_APPLIED)?;
    let membership: StoredMembership<RaftNodeId, RaftNode> =
        offline_meta(&db, data_dir, offline_keys::MEMBERSHIP)?.unwrap_or_default();
    let cluster_revision: u64 =
        offline_meta(&db, data_dir, offline_keys::CLUSTER_REVISION)?.unwrap_or(0);
    let compact_revision: u64 =
        offline_meta(&db, data_dir, offline_keys::COMPACT_REVISION)?.unwrap_or(0);
    // Absent on a directory that has never retired anyone, which is the common case and not an
    // error: an empty set widens nothing when the snapshot is installed (M5-R21).
    let retired_nodes: BTreeSet<NodeId> =
        offline_meta(&db, data_dir, offline_keys::RETIRED_NODES)?.unwrap_or_default();
    // Absent on a store that has only ever applied schema-1 commands, which is what
    // `COMMAND_SCHEMA_V1` means, so the default is the fact rather than a guess.
    let max_applied_command_schema: u16 =
        offline_meta(&db, data_dir, offline_keys::MAX_COMMAND_SCHEMA)?
            .unwrap_or(config_core::COMMAND_SCHEMA_V1);

    let mut cfs: Vec<String> = cf_names
        .iter()
        .filter(|name| is_snapshot_data_cf(name))
        .cloned()
        .collect();
    cfs.sort();

    // The same counting pre-pass as the online builder, for the same reason: the header is
    // written first, and `counts` is only an independent check on the body if it was measured
    // separately from writing it.
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut payload_bytes = 0u64;
    for name in &cfs {
        let cf = db
            .cf_handle(name)
            .ok_or_else(|| SnapshotFileError::UnknownColumnFamily { name: name.clone() })?;
        let mut count = 0u64;
        for item in db.iterator_cf(cf, rocksdb::IteratorMode::Start) {
            let (key, value) = item.map_err(|e| SnapshotFileError::Io {
                file: show(data_dir),
                detail: format!("cannot scan {name}: {e}"),
            })?;
            count += 1;
            payload_bytes += (key.len() + value.len()) as u64;
        }
        counts.insert(name.clone(), count);
    }

    let created_unix_ms = unix_ms();
    let header = SnapshotHeader {
        format_version: crate::FORMAT_VERSION,
        command_schema: config_core::COMMAND_ENVELOPE_VERSION,
        cluster_id: identity.cluster_id,
        recovery_epoch: identity.recovery_epoch,
        built_by: identity.node_id.0,
        snapshot_id: snapshot_id(last_applied, created_unix_ms),
        last_log_id: last_applied,
        last_applied,
        membership,
        cluster_revision,
        compact_revision,
        cfs: cfs.clone(),
        counts,
        bytes: payload_bytes,
        created_unix_ms,
        retired_nodes,
        max_applied_command_schema,
    };

    // Written through a `.tmp` beside the destination and renamed, so a reader never observes
    // a half-written artifact even if this process is killed mid-export.
    let tmp = out_path.with_extension(TMP_EXT);
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
    }
    let mut writer = SnapshotWriter::create(&tmp, &header)?;
    for (index, name) in cfs.iter().enumerate() {
        let cf = db
            .cf_handle(name)
            .ok_or_else(|| SnapshotFileError::UnknownColumnFamily { name: name.clone() })?;
        for item in db.iterator_cf(cf, rocksdb::IteratorMode::Start) {
            let (key, value) = item.map_err(|e| SnapshotFileError::Io {
                file: show(data_dir),
                detail: format!("cannot scan {name}: {e}"),
            })?;
            writer.write_record(index as u16, &key, &value)?;
        }
    }
    let (file, _digest) = writer.finish()?;
    file.sync_all().map_err(|e| io_err(&tmp, e))?;
    drop(file);
    std::fs::rename(&tmp, out_path).map_err(|e| io_err(out_path, e))?;
    Ok(header)
}

/// Snapshot, purge and backend counters a store exposes for `/metrics` (D5.5, ADR-0026).
///
/// A plain snapshot struct rather than a live handle: the engine polls it, and a metrics
/// scrape must never be able to take a lock the apply path also wants.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageMetrics {
    /// `build_snapshot` calls that returned `Ok`.
    pub snapshot_builds: u64,
    /// Builds started and not yet finished. A gauge: a value stuck above zero is a wedged
    /// build, which a rising `snapshot_builds` alone cannot show (ADR-0026).
    pub snapshot_builds_in_flight: u64,
    /// `build_snapshot` calls that returned `Err` — each one is a node this store killed
    /// (research §1.5, trap T5).
    pub snapshot_build_failures: u64,
    /// Transient build failures absorbed and retried internally, which is what keeps
    /// `snapshot_build_failures` at zero (test plan M5-03).
    pub snapshot_build_retries: u64,
    /// `state_meta/current_snapshot` batches committed. The §19.7 ordering oracle: this must
    /// increment before `purges` ever does.
    pub snapshot_publications: u64,
    /// Duration of the most recent successful build.
    pub snapshot_build_duration_ms: u64,
    /// Size of the current snapshot file.
    pub snapshot_size_bytes: u64,
    /// Age of the current snapshot at the time this struct was taken.
    pub snapshot_age_ms: u64,
    /// Last log index the current snapshot covers.
    pub snapshot_last_log_index: u64,
    /// Snapshots received and installed from a leader.
    pub snapshot_installs: u64,
    /// Installs refused by the validation matrix.
    pub snapshot_install_failures: u64,
    /// Installs redone at open because an `install_in_progress` marker was found.
    pub snapshot_install_redos: u64,
    /// `RaftLogStorage::purge` calls that actually deleted entries.
    pub purges: u64,
    /// `purge` calls deferred because durability could not yet be proven (ADR-0022 note of
    /// 2026-09-18, lead ruling M5-R11).
    pub purge_deferrals: u64,
    /// `purge` calls refused outright.
    pub purge_refusals: u64,
    /// Highest durably purged log index.
    pub purged_index: u64,
    /// Published `.snap` files currently on disk.
    pub snapshot_files: u64,
    /// RocksDB's estimate of memory held by table readers, bytes.
    pub rocks_table_readers_bytes: u64,
    /// RocksDB's estimate of memtable memory, bytes.
    pub rocks_memtable_bytes: u64,
    /// Files at level 0 — the number that precedes a write stall.
    pub rocks_level0_files: u64,
    /// Whether RocksDB is currently stopping writes (`1`) or not (`0`).
    pub rocks_write_stopped: u64,
}

/// Restore-time state-meta keys this module writes directly (M5, ADR-0024).
///
/// The same deliberate mirror as [`offline_keys`], for the same reason and with the same
/// guarantee: restore creates a store without opening a [`crate::RocksStore`], because the
/// store it is creating does not exist yet and the identity it will be bound to is not the one
/// the snapshot carries.
mod restore_keys {
    pub(super) const FORMAT_VERSION: &[u8] = b"format_version";
    pub(super) const IDENTITY: &[u8] = b"identity";
    pub(super) const CLUSTER_REVISION: &[u8] = b"cluster_revision";
    pub(super) const COMPACT_REVISION: &[u8] = b"compact_revision";
    pub(super) const RESTORED_FROM: &[u8] = b"restored_from";
}

/// Column families a restore populates from the snapshot body.
///
/// `events` is deliberately absent even though it is in the snapshot: ADR-0024 sets
/// `compact_revision = cluster_revision` on a restored store, which makes every retained event
/// unresumable by definition. Writing them and then declaring them compacted would leave an
/// events column family whose contents no watcher may ever read — bytes that exist only to be
/// refused. Spec §14 step 9 already tells clients to relist after a restore; this is that
/// instruction made true at the storage layer instead of only in a runbook.
const RESTORED_CFS: [&str; 2] = [crate::CF_KV, crate::CF_DEDUP];

/// What a completed restore wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    /// Records written, per column family.
    pub written: BTreeMap<String, u64>,
    /// Records the snapshot held but this restore deliberately dropped, per column family.
    pub skipped: BTreeMap<String, u64>,
    /// The revision the restored store continues from.
    pub revision: u64,
}

/// Records written per batch while streaming a snapshot into a store.
///
/// Not one batch for the whole snapshot: a `WriteBatch` is held entirely in memory, so a single
/// batch would put the whole state machine there — the exact cost `SnapshotData =
/// tokio::fs::File` exists to avoid (trap T4). Used by both the live install and the offline
/// restore (C5-07).
pub(crate) const INSTALL_BATCH_RECORDS: usize = 4_096;

/// Create a fresh store at `data_dir` from a plaintext snapshot, bound to a **new** identity
/// (M5, ADR-0024 "What restore writes", OQ-45).
///
/// This is the one place in rEtcd where a snapshot enters a store whose identity is *not* the
/// snapshot's, and it is deliberately unreachable from a running node: the normal
/// `install_snapshot` path keeps refusing an identity mismatch, because a peer must never be
/// able to do what an operator is doing here. The refusals that make that safe — a new cluster
/// id, an advanced epoch, a verified signature — are enforced by the CLI before this is
/// called; what this function enforces is the part the CLI cannot, namely that the destination
/// is genuinely empty at the moment it is written.
///
/// What it writes, and what it does not:
///
/// * `kv` and `dedup` from the snapshot body; `events` is dropped.
/// * `cluster_revision` **preserved**, so a client that read a revision before the disaster
///   sees the number continue rather than restart.
/// * `compact_revision = cluster_revision`, so a watch resuming at or below it is told
///   `RevisionCompacted` instead of being handed a partial replay (ADR-0020, M5-91).
/// * `identity` = the new one. No `last_applied`, no `membership`, no `current_snapshot`: the
///   restored store has data but no Raft position, which is exactly what lets `--form` treat
///   it as the genesis member of the new cluster (OQ-45) without ever having to treat a
///   half-wiped directory as one.
/// * `restored_from` = the **source** identity, for audit, for the lifetime of this directory
///   (spec §14 step 4). Nothing compares it against a live peer; doing so would reintroduce
///   the coupling the new identity exists to break.
pub fn restore_into_fresh_store(
    data_dir: &Path,
    new_identity: &config_core::ClusterIdentity,
    snapshot: &Path,
    restored_from: &config_core::RestoredFrom,
) -> Result<RestoreReport, SnapshotFileError> {
    if !dir_is_absent_or_empty(data_dir)? {
        return Err(SnapshotFileError::Io {
            file: show(data_dir),
            detail: "destination is not empty; restore never writes into a directory that \
                     might be live or might belong to another node"
                .to_string(),
        });
    }

    // The header is validated before the destination is created, so a refusal leaves no
    // directory behind for an operator to clean up before retrying (M5-82, M5-85).
    let mut reader = SnapshotReader::open(snapshot)?;
    let header = reader.header().clone();
    if header.format_version != crate::FORMAT_VERSION {
        return Err(SnapshotFileError::UnsupportedFormat {
            found: header.format_version,
            supported: crate::FORMAT_VERSION,
        });
    }
    if header.command_schema > config_core::COMMAND_ENVELOPE_VERSION {
        return Err(SnapshotFileError::UnsupportedCommandSchema {
            found: header.command_schema,
            supported: config_core::COMMAND_ENVELOPE_VERSION,
        });
    }

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs: Vec<rocksdb::ColumnFamilyDescriptor> = crate::COLUMN_FAMILIES
        .iter()
        .map(|name| rocksdb::ColumnFamilyDescriptor::new(*name, rocksdb::Options::default()))
        .collect();
    let db = rocksdb::DB::open_cf_descriptors(&opts, data_dir, cfs).map_err(|e| {
        SnapshotFileError::Io {
            file: show(data_dir),
            detail: format!("cannot create the destination store: {e}"),
        }
    })?;

    // The data goes in bounded batches; only the last one, carrying `state_meta`, is synced.
    // A restore of a large snapshot must not build one `WriteBatch` the size of the whole
    // state machine (C5-07), and it does not have to: RocksDB's WAL is ordered, so the synced
    // final batch makes every earlier batch durable with it. What the single-batch version
    // really bought was the *acceptance* property below, and that is preserved — the identity
    // key lands in the final batch, and a directory without `state_meta/identity` is one no
    // `RocksStore::open` will accept.
    let mut batch = rocksdb::WriteBatch::default();
    let mut in_batch = 0usize;
    let mut write_data = rocksdb::WriteOptions::default();
    write_data.set_sync(false);
    let mut written: BTreeMap<String, u64> = BTreeMap::new();
    let mut skipped: BTreeMap<String, u64> = BTreeMap::new();
    while let Some(record) = reader.next_record()? {
        if in_batch >= INSTALL_BATCH_RECORDS {
            db.write_opt(std::mem::take(&mut batch), &write_data)
                .map_err(|e| SnapshotFileError::Io {
                    file: show(data_dir),
                    detail: format!("cannot write the restored state: {e}"),
                })?;
            in_batch = 0;
        }
        let name =
            header
                .cfs
                .get(record.cf as usize)
                .ok_or_else(|| SnapshotFileError::Malformed {
                    file: show(snapshot),
                    detail: format!(
                        "record names column family index {}, out of range",
                        record.cf
                    ),
                })?;
        if RESTORED_CFS.contains(&name.as_str()) {
            let handle =
                db.cf_handle(name)
                    .ok_or_else(|| SnapshotFileError::UnknownColumnFamily {
                        name: name.to_string(),
                    })?;
            batch.put_cf(handle, &record.key, &record.value);
            in_batch += 1;
            *written.entry(name.clone()).or_default() += 1;
        } else {
            *skipped.entry(name.clone()).or_default() += 1;
        }
    }
    // Reads to the end and checks the trailer. A restore that wrote a truncated snapshot and
    // *then* discovered the checksum was wrong would already have created the very thing the
    // refusal matrix promises not to leave behind, so this happens before the batch commits.
    reader.verify_to_end()?;

    let meta = db.cf_handle(crate::CF_STATE_META).ok_or_else(|| {
        SnapshotFileError::UnknownColumnFamily {
            name: crate::CF_STATE_META.to_string(),
        }
    })?;
    let encode = |what: &'static str,
                  value: Result<Vec<u8>, postcard::Error>|
     -> Result<Vec<u8>, SnapshotFileError> {
        value.map_err(|e| SnapshotFileError::Malformed {
            file: show(data_dir),
            detail: format!("cannot encode state_meta/{what}: {e}"),
        })
    };
    batch.put_cf(
        meta,
        restore_keys::FORMAT_VERSION,
        crate::FORMAT_VERSION.to_le_bytes(),
    );
    batch.put_cf(
        meta,
        restore_keys::IDENTITY,
        encode("identity", postcard::to_stdvec(new_identity))?,
    );
    batch.put_cf(
        meta,
        restore_keys::CLUSTER_REVISION,
        encode(
            "cluster_revision",
            postcard::to_stdvec(&header.cluster_revision),
        )?,
    );
    batch.put_cf(
        meta,
        restore_keys::COMPACT_REVISION,
        encode(
            "compact_revision",
            postcard::to_stdvec(&header.cluster_revision),
        )?,
    );
    batch.put_cf(
        meta,
        restore_keys::RESTORED_FROM,
        encode("restored_from", postcard::to_stdvec(restored_from))?,
    );

    // The final synced batch: a restored directory either holds the whole restore or holds
    // nothing an open would accept, because `state_meta/identity` arrives here, after the last
    // data record. A directory left behind by a crash mid-restore has data and no identity,
    // which `RocksStore::open` refuses and `dir_is_absent_or_empty` will not restore into
    // again — the operator deletes it and retries, which is the documented story.
    let mut write = rocksdb::WriteOptions::default();
    write.set_sync(true);
    db.write_opt(batch, &write)
        .map_err(|e| SnapshotFileError::Io {
            file: show(data_dir),
            detail: format!("cannot commit the restored state: {e}"),
        })?;
    db.flush().map_err(|e| SnapshotFileError::Io {
        file: show(data_dir),
        detail: format!("cannot flush the restored store: {e}"),
    })?;

    Ok(RestoreReport {
        written,
        skipped,
        revision: header.cluster_revision,
    })
}

/// Whether a destination may be restored into: absent, or present and holding nothing.
fn dir_is_absent_or_empty(dir: &Path) -> Result<bool, SnapshotFileError> {
    match std::fs::read_dir(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(io_err(dir, e)),
        Ok(mut entries) => Ok(entries.next().is_none()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_validates_and_disabled_config_validates() {
        SnapshotConfig::DEFAULT
            .validate()
            .expect("default is valid");
        SnapshotConfig::DISABLED
            .validate()
            .expect("the M0-M4 latch is valid");
    }

    #[test]
    fn half_applied_policy_change_is_refused() {
        // The silent-no-op direction: builds snapshots, purges nothing (research §4.5).
        let silent = SnapshotConfig {
            logs_since_last: 16,
            logs_to_keep: u64::MAX,
            ..SnapshotConfig::DEFAULT
        };
        assert!(matches!(
            silent.validate(),
            Err(SnapshotConfigError::PolicyLatchMismatch { .. })
        ));
        // The unservable-purge direction: never builds, but would purge if one appeared.
        let unservable = SnapshotConfig {
            logs_since_last: 0,
            logs_to_keep: 4,
            ..SnapshotConfig::DEFAULT
        };
        assert!(matches!(
            unservable.validate(),
            Err(SnapshotConfigError::PolicyLatchMismatch { .. })
        ));
    }

    #[test]
    fn zero_purge_batch_and_zero_retain_are_refused() {
        assert_eq!(
            SnapshotConfig {
                purge_batch_size: 0,
                ..SnapshotConfig::DEFAULT
            }
            .validate(),
            Err(SnapshotConfigError::ZeroPurgeBatchSize)
        );
        assert_eq!(
            SnapshotConfig {
                retain_snapshots: 0,
                ..SnapshotConfig::DEFAULT
            }
            .validate(),
            Err(SnapshotConfigError::ZeroRetainSnapshots)
        );
    }

    #[test]
    fn snapshot_ids_from_the_same_log_id_differ_by_time() {
        let log_id = LogId::new(openraft::CommittedLeaderId::new(7, 0), 42);
        let a = snapshot_id(Some(log_id), 1_000);
        let b = snapshot_id(Some(log_id), 1_001);
        assert_eq!(a, "42-7-1000");
        assert_ne!(a, b, "research §1.2: ids must be unique per build");
    }

    #[test]
    fn data_cfs_exclude_the_log_and_state_meta_but_include_new_ones() {
        assert!(is_snapshot_data_cf("kv"));
        assert!(is_snapshot_data_cf("events"));
        // The property dev-dedup depends on: a column family this build has never heard of is
        // snapshot data by default, so adding one needs no format change.
        assert!(is_snapshot_data_cf("dedup"));
        for excluded in NON_DATA_CFS {
            assert!(
                !is_snapshot_data_cf(excluded),
                "{excluded} must be excluded"
            );
        }
    }

    #[test]
    fn newest_snapshot_sorts_first_by_creation_time() {
        let mut ids = ["10-1-500", "10-1-900", "4-1-700"];
        ids.sort_by_key(|id| std::cmp::Reverse(id_sort_key(id)));
        assert_eq!(ids, ["10-1-900", "4-1-700", "10-1-500"]);
    }
}
