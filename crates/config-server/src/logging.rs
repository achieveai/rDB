//! Process logging (ADR-0013, ADR-0018 §2).
//!
//! The daemon writes JSONL through [`config_log::layer::JsonlLayer`] into
//! `<--log-dir>/<node_id>.jsonl`, and every line carries `node_id`, `cluster_id` and each
//! `--log-field k=v`.
//!
//! # Why this is not `config_log::init`
//!
//! `tracing` field names are fixed at the macro callsite, so `info_span!` cannot express an
//! arbitrary runtime key — and `--log-field` is explicitly arbitrary (OQ-18). The fields are
//! therefore injected by a layer of our own, [`ConstFieldsLayer`], which merges them into the
//! root span's `JsonFields` map exactly where a real span field would land.
//! `config_log::init` installs a complete registry and accepts no extra layer, so the daemon
//! assembles filter + `JsonlLayer` + [`ConstFieldsLayer`] itself.
//!
//! Layer order is load-bearing: the `JsonlLayer` comes first so that `Layered` calls its
//! `on_new_span` before [`ConstFieldsLayer`]'s, which then merges into the map that is already
//! there instead of being overwritten by it.
//!
//! # Why not the non-blocking appender
//!
//! `tracing_appender::non_blocking` buffers in a worker thread and needs its guard dropped to
//! flush. The daemon's last line is `msg="shutdown_complete"` and E2E-09 reads it from the
//! file after the process has exited, so a plain `File` — one `write_all` per line, straight
//! to the OS — is the behaviour that assertion needs.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use config_core::ClusterIdentity;
use config_log::layer::{JsonFields, JsonlLayer};
use serde_json::Value;
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Default log filter when `RUST_LOG` is unset.
const DEFAULT_FILTER: &str = "info,config_core=debug,config_storage=debug,config_engine=debug,\
                              config_gossip=info,config_grpc=debug,config_server=debug,\
                              openraft=info";

/// Why logging could not be started. Exit code 2: a daemon that cannot log must not run.
#[derive(Debug, thiserror::Error)]
pub enum LogSetupError {
    /// The log directory or file could not be created.
    #[error("cannot open log file {path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// The filter directive is malformed.
    #[error("invalid log filter {0:?}: {1}")]
    Filter(String, String),
    /// A global subscriber was already installed in this process.
    #[error("a global tracing subscriber is already installed")]
    AlreadyInitialized,
}

/// Adds constant fields to the process root span.
///
/// "Root span" means a span created with no parent. The daemon opens exactly one, so the
/// fields land once and every descendant inherits them through `JsonlLayer`'s root-to-leaf
/// merge.
struct ConstFieldsLayer {
    fields: BTreeMap<String, Value>,
}

impl<S> Layer<S> for ConstFieldsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.parent().is_some() || ctx.current_span().id().is_some() {
            return;
        }
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        match ext.get_mut::<JsonFields>() {
            Some(existing) => {
                for (k, v) in &self.fields {
                    existing.0.insert(k.clone(), v.clone());
                }
            }
            None => {
                let mut map = serde_json::Map::new();
                for (k, v) in &self.fields {
                    map.insert(k.clone(), v.clone());
                }
                ext.insert(JsonFields(map));
            }
        }
    }
}

/// Install the daemon's global subscriber.
///
/// `log_dir` is both the directory of the process file and the per-test routing root: when the
/// harness passes `--log-field testModule=… --log-field testMethod=…`, `JsonlLayer` writes
/// those lines to `<log_dir>/<module>/<method>.jsonl` as well, which is what the cross-process
/// DuckDB joins read (test plan §7 Q10/Q12) — with no environment variable involved (TA-11).
pub fn init(
    log_dir: &Path,
    identity: &ClusterIdentity,
    extra_fields: &[(String, String)],
) -> Result<(), LogSetupError> {
    std::fs::create_dir_all(log_dir).map_err(|source| LogSetupError::Io {
        path: log_dir.to_path_buf(),
        source,
    })?;
    let file_path = log_dir.join(format!("{}.jsonl", identity.node_id));
    let file = File::options()
        .create(true)
        .append(true)
        .open(&file_path)
        .map_err(|source| LogSetupError::Io {
            path: file_path.clone(),
            source,
        })?;

    let directive = std::env::var("RUST_LOG").unwrap_or_else(|_| DEFAULT_FILTER.to_string());
    let filter = EnvFilter::try_new(&directive)
        .map_err(|e| LogSetupError::Filter(directive.clone(), e.to_string()))?;

    let mirror = open_mirror(log_dir, extra_fields)?;
    let jsonl = JsonlLayer::new("config-server", Box::new(Tee { file, mirror }), None, true);

    let mut fields = BTreeMap::new();
    for (k, v) in extra_fields {
        fields.insert(k.clone(), Value::from(v.as_str()));
    }
    let const_fields = ConstFieldsLayer { fields };

    tracing_subscriber::registry()
        .with(filter)
        .with(jsonl)
        .with(const_fields)
        .try_init()
        .map_err(|_| LogSetupError::AlreadyInitialized)
}

/// Open `<log_dir>/<testModule>/<testMethod>.jsonl` when the harness tagged this process.
///
/// `JsonlLayer`'s own `test_log_dir` routing is not used, for two reasons: it sends a tagged
/// line to the per-test file *instead of* the process file, and a second layer cannot be added
/// to produce the other copy because both would insert `JsonFields` into the same span
/// extensions, which `tracing-subscriber` refuses. Since `--log-field` is constant for the
/// whole process, the destination is known once at startup and needs no per-line inspection.
fn open_mirror(
    log_dir: &Path,
    extra_fields: &[(String, String)],
) -> Result<Option<File>, LogSetupError> {
    let field = |name: &str| {
        extra_fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    };
    let Some(method) = field("testMethod") else {
        return Ok(None);
    };
    let module = field("testModule").unwrap_or("unknown_module");
    let path = config_log::layer::test_file_path(log_dir, module, method);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| LogSetupError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    File::options()
        .create(true)
        .append(true)
        .open(&path)
        .map(Some)
        .map_err(|source| LogSetupError::Io { path, source })
}

/// Writes every line to the process file, and to the per-test file when there is one.
struct Tee {
    file: File,
    mirror: Option<File>,
}

impl std::io::Write for Tee {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(mirror) = self.mirror.as_mut() {
            mirror.write_all(buf)?;
        }
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(mirror) = self.mirror.as_mut() {
            mirror.flush()?;
        }
        self.file.flush()
    }
}

/// Report a background poller that stopped without being asked to.
///
/// `spawn_blocking(...).await` hands back a `JoinError` in two quite different situations: the
/// blocking pool is gone, which only happens once the runtime is shutting down, and **the
/// closure panicked**. The second is the dangerous one — the node keeps serving traffic and
/// keeps reporting its policy and TLS material as current, while nothing is re-reading either
/// file ever again — so it gets an `error` line naming the dead poller. A clean shutdown stays
/// silent, because `shutdown_complete` already reports that.
///
/// The panic's own payload is deliberately not echoed into the line: ADR-0013 keeps arbitrary
/// runtime strings out of the log, and the default panic hook has already written the message
/// and location to stderr for whoever is reading it.
pub fn poller_stopped(poller: &'static str, error: &tokio::task::JoinError) {
    if error.is_panic() {
        tracing::error!(
            poller,
            detail = "the poller's closure panicked; this node keeps serving traffic but will \
                      never re-read these files again until it is restarted",
            "poller_stopped"
        );
    }
}

/// The process root span: the one span every daemon line descends from.
pub fn process_span(identity: &ClusterIdentity) -> tracing::Span {
    tracing::info_span!(
        parent: None,
        "config_server",
        node_id = identity.node_id.0,
        cluster_id = %identity.cluster_id,
        recovery_epoch = identity.recovery_epoch.0,
    )
}

#[cfg(test)]
mod tests {
    //! F-003: a poller that stopped because its own closure panicked has to be distinguishable,
    //! in the log, from one that stopped because the runtime is going away. Both arrive as a
    //! `JoinError` from the same `await`, and treating the pair as one silent case is what left
    //! a node serving traffic with a dead policy or TLS poller and nothing in the log.

    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A `MakeWriter` that keeps everything written to it, so a test can read back the line a
    /// `tracing` macro produced.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Captured {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().expect("the capture buffer").clone())
                .expect("tracing writes UTF-8")
        }
    }

    impl Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("the capture buffer")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl tracing_subscriber::fmt::MakeWriter<'_> for Captured {
        type Writer = Self;

        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }

    /// Everything `body` logged, against a subscriber of this test's own.
    ///
    /// Thread-local rather than global: the test binary already installed one subscriber, and
    /// `with_default` takes precedence over it for the duration of the call.
    fn captured(body: impl FnOnce()) -> String {
        let sink = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(sink.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, body);
        sink.text()
    }

    /// A string no line of this module contains, so "the payload did not reach the log" is a
    /// claim about the payload rather than about a phrase that happens to appear twice.
    const PAYLOAD: &str = "PANIC-PAYLOAD-4b19c7";

    /// A real `JoinError` from a blocking closure that panicked, which is what a panicking
    /// poller hands its `await`. The default panic hook prints the payload to stderr while this
    /// runs; that is the behaviour under test, not noise to be suppressed.
    #[tokio::test]
    async fn a_panicked_poller_is_named_in_the_log() {
        let error = tokio::task::spawn_blocking(|| panic!("{PAYLOAD}"))
            .await
            .expect_err("a panicking closure joins as an error");
        assert!(error.is_panic(), "the fixture produced the wrong JoinError");

        let logged = captured(|| poller_stopped("policy", &error));
        assert!(
            logged.contains("poller_stopped") && logged.contains("policy"),
            "a poller that died of a panic must say so, by name: {logged:?}"
        );
        assert!(
            !logged.contains(PAYLOAD),
            "the panic payload stays out of the log (ADR-0013): {logged:?}"
        );
    }

    /// The other half of the same `JoinError`: nothing to report, because the process is on its
    /// way out and `shutdown_complete` already says so.
    ///
    /// Produced by aborting a task rather than by dropping a runtime: a `spawn_blocking` closure
    /// cannot be aborted once it is running, so there is no way to manufacture the real
    /// pool-is-gone error in-process. What both share — and all this branch reads — is that
    /// `is_panic()` is false.
    #[tokio::test]
    async fn a_poller_that_stopped_without_panicking_logs_nothing() {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        let error = handle.await.expect_err("an aborted task joins as an error");
        assert!(
            !error.is_panic(),
            "the fixture produced the wrong JoinError"
        );

        let logged = captured(|| poller_stopped("tls", &error));
        assert!(
            logged.is_empty(),
            "an ordinary shutdown must not look like a fault: {logged:?}"
        );
    }
}
