//! The command line (ADR-0018 §2, test plan TA-21).
//!
//! Every setting that describes the *node* lives in the TOML file named by `--config`
//! (TA-11: no environment-variable-only settings). The flags here are the ones that describe
//! this *run* of the daemon — the two safety gates, the two test-facing lifecycle controls,
//! and where the logs go.

use std::path::PathBuf;

use clap::Parser;

/// rEtcd node daemon.
#[derive(Debug, Clone, Parser)]
#[command(name = "config-server", version, about, long_about = None)]
pub struct Cli {
    /// Path to the node's TOML configuration file.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,

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
    #[arg(long)]
    pub dev_allow_all: bool,

    /// Run RocksDB without fsync. Capabilities then report `PersistentUnverified` (OQ-15).
    #[arg(long)]
    pub unsafe_no_sync: bool,

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

impl Cli {
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
        assert!(!cli.form);
        assert!(!cli.capabilities);
    }
}
