//! Test context (ADR-0013).
//!
//! `#[retcd_test]` (from `config-log-macros`, re-exported here) wraps a test in a root span
//! with `testModule`, `testMethod`, `testRun`. The JSONL layer routes every line inside that
//! span to `<target>/test-logs/<testModule>/<testMethod>.jsonl`.
//!
//! Environment:
//! * `RETCD_TEST_LOG_DIR` — override the test log directory;
//! * `RETCD_TEST_LOG` — filter directive (default `trace` for `config_*` crates, `info` else).
//!
//! Query with DuckDB:
//! ```sql
//! SELECT "@t","@l","@logger","@m",trace_id,node_id
//! FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
//! WHERE testMethod = 'puts_then_gets' ORDER BY "@t";
//! ```

use std::future::Future;
use std::path::PathBuf;
use std::sync::OnceLock;

use tracing::{Instrument, Span};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub use config_log_macros::retcd_test;

use crate::init::{build, LogConfig};

static TEST_RUN: OnceLock<String> = OnceLock::new();
static INIT: OnceLock<Option<Vec<tracing_appender::non_blocking::WorkerGuard>>> = OnceLock::new();

/// Unique id for this test process run (one per `cargo test` binary execution).
pub fn test_run_id() -> &'static str {
    TEST_RUN.get_or_init(|| uuid::Uuid::new_v4().simple().to_string())
}

/// Locate `<workspace>/target/test-logs` from the running test binary, honoring
/// `RETCD_TEST_LOG_DIR`.
pub fn test_log_dir() -> PathBuf {
    if let Ok(d) = std::env::var("RETCD_TEST_LOG_DIR") {
        return PathBuf::from(d);
    }
    if let Ok(exe) = std::env::current_exe() {
        for anc in exe.ancestors() {
            if anc.file_name().is_some_and(|n| n == "target") {
                return anc.join("test-logs");
            }
        }
    }
    PathBuf::from("target").join("test-logs")
}

/// Install the test subscriber once per process (idempotent, safe under parallel tests).
pub fn init_test_logging() {
    INIT.get_or_init(|| {
        let dir = test_log_dir();
        let filter = std::env::var("RETCD_TEST_LOG").unwrap_or_else(|_| {
            "info,config_log=trace,config_core=trace,config_storage=trace,config_engine=trace,\
             config_gossip=trace,config_grpc=trace,config_client=trace,config_server=trace,\
             config_testkit=trace,openraft=debug"
                .to_string()
        });
        let cfg = LogConfig {
            application: "retcd-tests".into(),
            dir: dir.clone(),
            file_name: format!("_untagged-{}.jsonl", std::process::id()),
            filter,
            include_location: true,
            also_stderr: false,
            test_log_dir: Some(dir),
        };
        let (layer, filter, guards) = build(&cfg).expect("test logging init");
        let installed = tracing_subscriber::registry()
            .with(filter)
            .with(layer)
            .try_init()
            .is_ok();
        installed.then_some(guards)
    });
}

/// Create the root span for a test. Prefer `#[retcd_test]`; use this directly only when the
/// attribute cannot be applied.
pub fn test_span(module: &'static str, method: &'static str) -> Span {
    init_test_logging();
    let span = tracing::info_span!(
        "test",
        testModule = module,
        testMethod = method,
        testRun = test_run_id(),
    );
    span.in_scope(|| tracing::info!(target: "config_log::testing", "test started"));
    span
}

/// Run an async test body inside `span` and log the completion.
pub async fn run_instrumented<F: Future<Output = T>, T>(span: Span, fut: F) -> T {
    async move {
        let out = fut.await;
        tracing::info!(target: "config_log::testing", "test finished");
        out
    }
    .instrument(span)
    .await
}

/// Log completion of a sync test (called by the macro before the guard drops).
pub fn finish_sync(span: &Span) {
    span.in_scope(|| tracing::info!(target: "config_log::testing", "test finished"));
}

/// Spawn a Tokio-free helper: instrument any future with the current span so work moved to
/// another task keeps `testMethod`/`trace_id`. (Tokio's `spawn` does not propagate spans.)
pub fn in_current_span<F: Future>(fut: F) -> tracing::instrument::Instrumented<F> {
    fut.instrument(Span::current())
}
