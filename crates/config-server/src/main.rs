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

mod cli;
mod config;
mod health;
mod logging;
mod manifest;
mod run;

use clap::Parser;

use crate::cli::Cli;
use crate::run::{ExitCode, Fatal};

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
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

/// Everything after argument parsing, so that each failure has one exit-code mapping.
fn start(cli: Cli) -> Result<ExitCode, Fatal> {
    let log_fields = cli
        .log_fields()
        .map_err(|e| Fatal::rejected("invalid_log_field", e))?;

    let cfg = config::load(
        &cli.config,
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
