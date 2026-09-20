//! The rEtcd node daemon (ADR-0018).
//!
//! `config-server` is a thin composition of the libraries: it parses one TOML document and a
//! handful of run-scoped flags, starts a [`config_engine::ConfigNode`] behind the two
//! `config-grpc` planes, prints one JSON ready line, and drains in a defined order when asked
//! to stop. It adds no distributed-systems behaviour of its own — anything an operator can
//! observe here, an embedder wiring the same crates also gets.
//!
//! # Contract
//!
//! * **stdout** carries exactly one line, the ready line, and only after every listener is
//!   bound and (with `--form`) the cluster is formed. Everything else goes to the JSONL log.
//! * **Exit codes** are `0` clean shutdown, `2` a refusal (configuration, identity, TLS gate,
//!   manifest, formation), `3` a fatal storage failure. `2` and `3` are distinct because they
//!   mean different things to an operator: `2` is "you asked for something I will not do",
//!   `3` is "the disk let us down".
//! * **`--capabilities`** opens no store, binds no listener, and takes no lock. It reports
//!   what *this configuration* would produce, which is why it can be asked of a node that is
//!   already running from the same data directory.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod backup;
mod cli;
mod config;
mod health;
mod logging;
mod manifest;
mod policy;
mod rotation;
mod run;

use clap::Parser;

use crate::cli::{Cli, Command};
use crate::run::{ExitCode, Fatal};

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    if let Some(command) = cli.command.clone() {
        return offline(command, &cli);
    }
    match start(cli) {
        Ok(code) => std::process::ExitCode::from(code as u8),
        Err(fatal) => {
            // The structured `startup_failed` line is emitted inside `start`, where the
            // process span is still entered; here we only mirror the refusal to stderr, for
            // the refusals that happen before there is a log at all. stdout stays reserved
            // for the ready line.
            eprintln!("config-server: {}: {}", fatal.reason, fatal.detail);
            std::process::ExitCode::from(fatal.code as u8)
        }
    }
}

/// The offline subcommands (M5, TA-47, ADR-0024).
///
/// None of them installs logging, opens a listener or starts a runtime: they are file
/// operations with cryptography in them, and an operator runs them on a host that may have no
/// cluster at all. The one diagnostic line goes to stderr and the exit code carries the
/// machine-readable answer, so an unattended verification script needs neither a log directory
/// nor a JSON parser.
fn offline(command: Command, cli: &Cli) -> std::process::ExitCode {
    let result = run_offline(command, cli);
    match result {
        Ok(line) => {
            println!("{line}");
            std::process::ExitCode::from(0)
        }
        Err(e) => {
            eprintln!("config-server: {}: {e}", e.reason());
            std::process::ExitCode::from(e.code())
        }
    }
}

/// One JSONL audit record for an offline subcommand, on **stderr**.
///
/// Not `tracing`: [`offline`] installs no subscriber, so a `tracing::info!` here would compile,
/// look like an audit trail, and emit nothing. Not stdout either — that is reserved for the one
/// result line an unattended script parses. stderr is already this command's diagnostic
/// channel, it needs no log directory, and it therefore works on the bare recovery host where
/// `restore` is actually run. Emitted only on success, so a refusal still prints exactly one
/// line (TA-47).
fn audit_line(record: serde_json::Value) {
    eprintln!("{record}");
}

/// The body of [`offline`], split out so every path has one exit-code mapping.
///
/// Returns the one stdout line on success. It is JSON for the same reason the ready line is:
/// the harness and an operator's script read the same bytes, so there is nothing to keep in
/// sync between them (TA-20.2).
fn run_offline(command: Command, cli: &Cli) -> Result<String, backup::BackupError> {
    // `[backup]` supplies the key paths a `backup` run does not name on the command line. The
    // other two subcommands deliberately do not read it: a recovery must not depend on a
    // configuration file that may itself have been lost with the cluster.
    let from_config = match (&command, &cli.config) {
        (Command::Backup { .. }, Some(path)) => {
            Some(config::load(path, None, true).map_err(|e| {
                backup::BackupError::refused("invalid_config", format!("{}: {e}", path.display()))
            })?)
        }
        _ => None,
    };
    let configured = from_config.as_ref().map(|c| &c.backup);
    let pick = |flag: &Option<std::path::PathBuf>,
                from_cfg: fn(&config::BackupKeys) -> &Option<std::path::PathBuf>|
     -> Option<std::path::PathBuf> {
        flag.clone()
            .or_else(|| configured.and_then(|k| from_cfg(k).clone()))
    };

    match &command {
        Command::Backup {
            data_dir,
            out,
            name,
            signing_key,
            encryption_key,
        } => {
            let signing = pick(signing_key, |k| &k.signing_key);
            let encryption = pick(encryption_key, |k| &k.encryption_key);
            let keys = backup::KeyFiles {
                signing_key: signing.as_deref(),
                trust_key: None,
                encryption_key: encryption.as_deref(),
            };
            let outcome = backup::backup_offline(data_dir, out, name.as_deref(), &keys)?;
            // The same `backup_created` record the admin-plane path emits (ADR-0024). An
            // artifact produced by the CLI is exactly as much of an event as one produced by
            // the RPC, and an operator correlating a recovery afterwards cannot be expected to
            // know which route made it.
            audit_line(serde_json::json!({
                "msg": "backup_created",
                "source": "cli",
                "name": outcome.name,
                "cluster_id": outcome.cluster_id,
                "revision": outcome.revision,
                "sha256": outcome.sha256,
                "dest": outcome.snapshot_file,
                "encrypted": outcome.encrypted,
            }));
            Ok(serde_json::json!({
                "name": outcome.name,
                "cluster_id": outcome.cluster_id,
                "revision": outcome.revision,
                "sha256": outcome.sha256,
                "size_bytes": outcome.size_bytes,
                "encrypted": outcome.encrypted,
                "snapshot": outcome.snapshot_file,
                "manifest": outcome.manifest_file,
                "signature": outcome.signature_file,
            })
            .to_string())
        }

        Command::VerifyBackup {
            from,
            name,
            trust_key,
            encryption_key,
        } => {
            let keys = backup::KeyFiles {
                signing_key: None,
                trust_key: trust_key.as_deref(),
                encryption_key: encryption_key.as_deref(),
            };
            let verified = backup::verify_backup(from, name.as_deref(), &keys)?;
            Ok(serde_json::json!({
                "verified": true,
                "name": verified.name,
                "cluster_id": verified.manifest.cluster_id,
                "recovery_epoch": verified.manifest.recovery_epoch,
                "revision": verified.manifest.revision,
                "counts": verified.manifest.counts,
                "sha256": verified.manifest.sha256,
                "encrypted": verified.manifest.encrypted,
                // False means the signature and the manifest verified but the snapshot's own
                // bytes were not checked, because it is encrypted and no key was supplied.
                // Reporting that plainly is the difference between "verified" and "verified
                // as far as I was allowed to look".
                "checksum_checked": verified.checksum_checked,
            })
            .to_string())
        }

        Command::Restore {
            from,
            name,
            data_dir,
            cluster_id,
            recovery_epoch,
            node_id,
            manifest,
            manifest_sig,
            manifest_key,
            trust_key,
            encryption_key,
        } => {
            let keys = backup::KeyFiles {
                signing_key: None,
                trust_key: trust_key.as_deref(),
                encryption_key: encryption_key.as_deref(),
            };
            let request = backup::RestoreRequest {
                from,
                name: name.as_deref(),
                data_dir,
                cluster_id,
                recovery_epoch: *recovery_epoch,
                node_id: *node_id,
                manifest,
                manifest_sig: manifest_sig.as_deref(),
                manifest_key: manifest_key.as_deref(),
            };
            let outcome = backup::restore(&request, &keys)?;
            // M5-81. The one record that says a new authority was minted, and the only place
            // both identities appear together: afterwards the store knows only the new one and
            // the artifact knows only the old one, so nothing else can ever be correlated back.
            audit_line(serde_json::json!({
                "msg": "restore_completed",
                "source": "cli",
                "source_cluster_id": outcome.source_cluster_id,
                "source_epoch": outcome.source_epoch,
                "new_cluster_id": outcome.new_cluster_id,
                "new_epoch": outcome.new_epoch,
                "node_id": node_id,
                "revision": outcome.revision,
                "data_dir": data_dir,
            }));
            Ok(serde_json::json!({
                "restored": true,
                "source_cluster_id": outcome.source_cluster_id,
                "source_epoch": outcome.source_epoch,
                "new_cluster_id": outcome.new_cluster_id,
                "new_epoch": outcome.new_epoch,
                "revision": outcome.revision,
                "written": outcome.written,
            })
            .to_string())
        }
    }
}

/// Everything after argument parsing, so that each failure has one exit-code mapping.
fn start(cli: Cli) -> Result<ExitCode, Fatal> {
    let log_fields = cli
        .log_fields()
        .map_err(|e| Fatal::rejected("invalid_log_field", e))?;
    // Before the document is even read: a refused safety gate must not depend on a config file
    // being present or parseable (ADR-0018 §5's pre-bind class).
    cli.check_dev_gates()
        .map_err(|e| Fatal::rejected("invalid_dev_gate", e))?;

    let config_path = cli.config.clone().ok_or_else(|| {
        Fatal::rejected(
            "missing_config",
            "--config is required to run as a node; the offline subcommands (backup, \
             verify-backup, restore) take their inputs as flags",
        )
    })?;
    let cfg = config::load(
        &config_path,
        cli.health_listen.as_deref(),
        cli.allow_insecure_dev,
    )
    .map_err(|e| Fatal::rejected("invalid_config", e))?;

    // `--capabilities` answers from the document alone and exits. Logging is not installed:
    // the flag must not create a log file, and it must not contend with a daemon that is
    // already running from this node directory.
    if cli.capabilities {
        let report = run::capabilities_without_opening(&cfg, &cli);
        let json = serde_json::to_string(&report).expect("the capability report serializes");
        println!("{json}");
        return Ok(ExitCode::Ok);
    }

    logging::init(&cli.log_dir, &cfg.identity, &log_fields)
        .map_err(|e| Fatal::rejected("logging_unavailable", e))?;
    let span = logging::process_span(&cfg.identity);
    let _entered = span.enter();

    // One current-thread-per-core runtime, built here rather than by `#[tokio::main]` so that
    // the `--capabilities` path above never starts one.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            let fatal = Fatal::rejected("runtime_unavailable", e);
            log_startup_failure(&fatal);
            return Err(fatal);
        }
    };
    let result = runtime.block_on(config_log::testing::in_current_span(run::run(cfg, cli)));
    // Shut the runtime down explicitly: a background task still holding the store open when
    // `main` returns would make the data directory unopenable for a moment after exit, which
    // a restart test would see as a lock error.
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    match &result {
        // Last, and provably last: every task that could still have logged is gone.
        Ok(_) => tracing::info!("shutdown_complete"),
        Err(fatal) => log_startup_failure(fatal),
    }
    result
}

/// Emit the one structured refusal line (ADR-0018 §5).
///
/// This is called from inside `start`, with the process span still entered, so the line
/// carries `node_id`, `cluster_id` and every `--log-field` — which is what the E2E rows join
/// on. Emitting it from `main` instead would produce a line with none of those fields,
/// because the span guard is dropped when `start` returns.
fn log_startup_failure(fatal: &Fatal) {
    tracing::error!(reason = fatal.reason, detail = %fatal.detail, "startup_failed");
}
