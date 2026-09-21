//! The command line (ADR-0018 §2, test plan TA-21).
//!
//! Every setting that describes the *node* lives in the TOML file named by `--config`
//! (TA-11: no environment-variable-only settings). The flags here are the ones that describe
//! this *run* of the daemon — the two safety gates, the two test-facing lifecycle controls,
//! and where the logs go.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// rEtcd node daemon.
#[derive(Debug, Clone, Parser)]
#[command(name = "config-server", version, about, long_about = None)]
pub struct Cli {
    /// The offline subcommand to run instead of the daemon (M5, TA-47).
    ///
    /// Absent means "be a node". Every subcommand is offline by construction: it opens no
    /// listener, joins no cluster, and never touches a running node's data directory — which
    /// is how `restore` satisfies spec §14 step 1 ("block ordinary client traffic") without
    /// any coordination at all, because the process that would serve traffic does not exist.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Path to the node's TOML configuration file.
    ///
    /// Required to run as a node. The `backup` subcommand accepts it as a source of
    /// `[backup]` key paths; the other two take every input as a flag, so a recovery never
    /// depends on a configuration file that may itself have been lost.
    #[arg(long, value_name = "FILE", global = true)]
    pub config: Option<PathBuf>,

    /// Form the cluster from the signed bootstrap manifest, then serve.
    ///
    /// A second run against a store that is already formed exits with code 2.
    #[arg(long)]
    pub form: bool,

    /// Print the capability report as JSON and exit 0, without opening any listener or store.
    #[arg(long)]
    pub capabilities: bool,

    /// The only way `tls.mode = "insecure"` is accepted (ADR-0010).
    #[arg(long)]
    pub allow_insecure_dev: bool,

    /// The only way allow-all authorization is accepted (ADR-0012).
    ///
    /// Accepted only alongside `--allow-insecure-dev`; see [`Cli::check_dev_gates`].
    #[arg(long)]
    pub dev_allow_all: bool,

    /// Run RocksDB without fsync. Capabilities then report `PersistentUnverified` (OQ-15).
    #[arg(long)]
    pub unsafe_no_sync: bool,

    /// Behave as a node of an older command schema (ADR-0030, M6-94).
    ///
    /// `1` makes this node advertise, gate and refuse exactly as a pre-M4 build does: it
    /// proposes no command that needs schema 2, refuses to decode one that does, and refuses
    /// to open a data directory written by a newer build. It exists so that a mixed-version
    /// cluster — the normal state of a rolling upgrade — is runnable in CI against one binary
    /// rather than only against two releases that cannot both be built from this tree.
    ///
    /// Only `1` is accepted: the flag pins a node *below* current, so naming the current
    /// schema would be a no-op and naming a future one would be a claim this build cannot
    /// honour.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u16).range(1..=1))]
    pub compat_schema: Option<u16>,

    /// Accept a signed policy document whose version is at or below the active one (ADR-0027).
    ///
    /// The emergency exit for "the good document is the old one". Version monotonicity is the
    /// defence against replaying a previously valid, previously signed document, so it cannot
    /// be lifted from the network, from the policy files, or from the configuration file —
    /// only by an operator restarting the process with this flag.
    ///
    /// **Process-scoped, not one-shot** (OQ-57): while it is set every rollback is permitted
    /// and each one emits its own audit line. A one-shot flag would stop working halfway
    /// through a multi-step recovery, which is the worst moment to discover a semantic.
    #[arg(long)]
    pub break_glass_policy_rollback: bool,

    /// Shut down gracefully as soon as this file exists.
    ///
    /// Windows has no `SIGTERM`, and sending Ctrl-C to one child without hitting the whole
    /// console group is unreliable, so this is what the E2E suite uses (OQ-17).
    #[arg(long, value_name = "FILE")]
    pub shutdown_file: Option<PathBuf>,

    /// Serve `GET /health` as plaintext HTTP on this loopback address (OQ-16).
    ///
    /// A non-loopback address is rejected during configuration validation: the health surface
    /// carries no keys and no values, but it is still not a remote surface.
    #[arg(long, value_name = "ADDR")]
    pub health_listen: Option<String>,

    /// Constant `key=value` field added to every log line (repeatable, OQ-18).
    ///
    /// The E2E suite passes `testModule` and `testMethod` so cross-process log joins work.
    #[arg(long = "log-field", value_name = "K=V")]
    pub log_fields: Vec<String>,

    /// Directory for this node's JSONL log file.
    #[arg(long, value_name = "DIR", default_value = "logs")]
    pub log_dir: PathBuf,
}

/// The offline subcommands (M5, TA-47, ADR-0024).
///
/// Their exit codes extend ADR-0018 §5 rather than replacing it: `0` success, `2` any
/// refusal, `3` a store that could not be opened, `4` an incomplete artifact. `4` exists
/// because "you pointed me at the wrong directory" and "this artifact is not trustworthy"
/// call for different operator actions, and an unattended verification script has to tell
/// them apart without parsing prose.
#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Export a signed backup triple from a **stopped** data directory.
    Backup {
        /// The data directory to back up. Must not be in use by a running node.
        #[arg(long, value_name = "DIR")]
        data_dir: PathBuf,
        /// Where the three files are written. Created if it does not exist.
        #[arg(long, value_name = "DIR")]
        out: PathBuf,
        /// Stem the three files share. Defaults to `backup-<unix-millis>`.
        #[arg(long, value_name = "NAME")]
        name: Option<String>,
        /// Ed25519 signing seed, 32 raw bytes. Overrides `[backup] signing_key_file`.
        #[arg(long, value_name = "FILE")]
        signing_key: Option<PathBuf>,
        /// AES-256 key, 32 raw bytes. Overrides `[backup] encryption_key_file`.
        #[arg(long, value_name = "FILE")]
        encryption_key: Option<PathBuf>,
    },

    /// Check a backup triple's signature, format and checksum.
    VerifyBackup {
        /// Directory holding the triple.
        #[arg(long, value_name = "DIR")]
        from: PathBuf,
        /// Which triple, when the directory holds more than one.
        #[arg(long, value_name = "NAME")]
        name: Option<String>,
        /// Ed25519 verifying key, 32 raw bytes. Required: there is no implicit trust store.
        #[arg(long, value_name = "FILE")]
        trust_key: Option<PathBuf>,
        /// AES-256 key, 32 raw bytes. Without it an encrypted artifact's signature is still
        /// checked, but its checksum cannot be.
        #[arg(long, value_name = "FILE")]
        encryption_key: Option<PathBuf>,
    },

    /// Restore a verified backup into a fresh data directory under a **new** identity.
    ///
    /// The new cluster id and the advanced recovery epoch are not conveniences: they are what
    /// makes spec §19.11 hold structurally. The peer plane already refuses any RPC whose
    /// cluster id does not match, so once the identity is guaranteed to differ, the old
    /// cluster and the restored one cannot exchange a single log entry — with no
    /// restore-specific code path anywhere in the engine.
    Restore {
        /// Directory holding the verified backup triple.
        #[arg(long, value_name = "DIR")]
        from: PathBuf,
        /// Which triple, when the directory holds more than one.
        #[arg(long, value_name = "NAME")]
        name: Option<String>,
        /// The fresh data directory to create. Must not exist, or must be empty.
        #[arg(long, value_name = "DIR")]
        data_dir: PathBuf,
        /// The **new** cluster id, 32 lowercase hex characters. Must differ from the source's.
        #[arg(long, value_name = "HEX")]
        cluster_id: String,
        /// The **new** recovery epoch. Must be strictly greater than the source's.
        #[arg(long, value_name = "N")]
        recovery_epoch: u32,
        /// This node's id in the new cluster.
        #[arg(long, value_name = "ID")]
        node_id: u64,
        /// A fresh ADR-0011 bootstrap manifest for the **new** cluster.
        #[arg(long, value_name = "FILE")]
        manifest: PathBuf,
        /// The bootstrap manifest's detached signature. Defaults to `<manifest>.sig`.
        ///
        /// Deviation from TA-47's flag list, which names only `--manifest`: the bootstrap
        /// manifest is a signed triple (ADR-0011) and restore performs the same verification
        /// `--form` does, so the other two members have to be nameable. They default to the
        /// conventional neighbours, so the common case is still one flag.
        #[arg(long, value_name = "FILE")]
        manifest_sig: Option<PathBuf>,
        /// The bootstrap manifest's signing public key. Defaults to `<manifest>.pub`.
        #[arg(long, value_name = "FILE")]
        manifest_key: Option<PathBuf>,
        /// Ed25519 verifying key for the **backup** manifest, 32 raw bytes. Required.
        #[arg(long, value_name = "FILE")]
        trust_key: Option<PathBuf>,
        /// AES-256 key, 32 raw bytes. Required when the backup is encrypted.
        #[arg(long, value_name = "FILE")]
        encryption_key: Option<PathBuf>,
        /// The signed policy version the restored node will run under (M6-35, ADR-0027).
        ///
        /// A number rather than a policy file, because the manifest's reference is a breadcrumb
        /// for a human and not a validation input: verifying a signed document here would buy
        /// nothing the daemon does not already do at startup, and would pull policy
        /// verification into a recovery path that deliberately depends on as little as
        /// possible — restore reads no configuration file at all.
        ///
        /// Optional. Absent means no comparison is made and nothing is reported, which is what
        /// a restore did before this flag existed. Silence here is the absence of a check, not
        /// a statement that the versions agree.
        #[arg(long, value_name = "N")]
        active_policy_version: Option<u64>,
    },
}

impl Cli {
    /// The schema triple this run advertises and enforces (ADR-0030).
    ///
    /// One accessor rather than a `compat_schema.is_some()` test at each use, so the store
    /// ceiling, the node's advertisement and the capability report cannot drift apart.
    #[must_use]
    pub fn schema(&self) -> config_core::SchemaTriple {
        match self.compat_schema {
            Some(_) => config_core::COMPAT_SCHEMA_1,
            None => config_core::CURRENT_SCHEMA,
        }
    }

    /// Refuse the one safety-gate combination that describes an impossible posture.
    ///
    /// The two dev gates used to be independent, so a node could serve real mutual TLS on both
    /// planes while authorizing every request that arrived over it: production-shaped in its
    /// certificates, in `transport_security`, and in every log line, yet open to anyone the CA
    /// has ever issued to. Requiring the pair makes "this is a development build" one fact an
    /// operator can check rather than two they have to correlate (lead ruling, 2026-09-19;
    /// ADR-0012, ADR-0018 §2). It costs nothing that worked before: both local-cluster scripts
    /// already pass the pair, and so does ADR-0027's own description of them.
    ///
    /// Checked here rather than in [`crate::config`] because it reads no part of the document;
    /// it is reported through the same pre-bind refusal path, so it still exits 2 before any
    /// listener, store or log file exists.
    pub fn check_dev_gates(&self) -> Result<(), String> {
        if self.dev_allow_all && !self.allow_insecure_dev {
            return Err(
                "--dev-allow-all requires --allow-insecure-dev; refusing to authorize \
                        every request on a node that otherwise presents itself as a secured one \
                        (ADR-0012)"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Split `--log-field k=v` arguments into pairs.
    ///
    /// Only the first `=` splits, so a value may contain one.
    pub fn log_fields(&self) -> Result<Vec<(String, String)>, String> {
        self.log_fields
            .iter()
            .map(|raw| match raw.split_once('=') {
                Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
                _ => Err(format!(
                    "--log-field expects `key=value` with a non-empty key, got {raw:?}"
                )),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        let mut all = vec!["config-server"];
        all.extend_from_slice(args);
        Cli::parse_from(all)
    }

    #[test]
    fn log_fields_split_on_the_first_equals_only() {
        let cli = parse(&[
            "--config",
            "n.toml",
            "--log-field",
            "testMethod=a",
            "--log-field",
            "note=x=y",
        ]);
        assert_eq!(
            cli.log_fields().expect("well-formed log fields"),
            vec![
                ("testMethod".to_string(), "a".to_string()),
                ("note".to_string(), "x=y".to_string()),
            ]
        );
    }

    #[test]
    fn a_log_field_without_a_key_is_rejected() {
        let cli = parse(&["--config", "n.toml", "--log-field", "=v"]);
        assert!(cli.log_fields().is_err());
    }

    #[test]
    fn the_gates_default_to_closed() {
        let cli = parse(&["--config", "n.toml"]);
        assert!(!cli.allow_insecure_dev);
        assert!(!cli.dev_allow_all);
        assert!(!cli.unsafe_no_sync);
        assert!(!cli.break_glass_policy_rollback);
        assert!(!cli.form);
        assert!(!cli.capabilities);
        assert!(cli.check_dev_gates().is_ok());
    }

    #[test]
    fn dev_allow_all_is_only_accepted_alongside_allow_insecure_dev() {
        let alone = parse(&["--config", "n.toml", "--dev-allow-all"]);
        let refusal = alone
            .check_dev_gates()
            .expect_err("allow-all authorization on a TLS-secured node is refused");
        assert!(
            refusal.contains("--allow-insecure-dev"),
            "the refusal names the flag that would have allowed it: {refusal}"
        );

        let paired = parse(&[
            "--config",
            "n.toml",
            "--dev-allow-all",
            "--allow-insecure-dev",
        ]);
        assert!(
            paired.check_dev_gates().is_ok(),
            "the pair is the supported development posture, and both local-cluster scripts use it"
        );

        // The other gate keeps standing on its own: `tls.mode = \"insecure\"` with a real policy
        // is a legitimate configuration, and coupling must not have made it a refusal.
        let insecure_only = parse(&["--config", "n.toml", "--allow-insecure-dev"]);
        assert!(insecure_only.check_dev_gates().is_ok());
    }
}
