//! The JSONL `tracing` layer.
//!
//! Design (ADR-0013):
//! * span attributes are captured as `serde_json::Value`s at span creation (and on
//!   `Span::record`) and stored in the span's extensions;
//! * on every event we merge fields root → leaf → event, add canonical render fields, and
//!   write exactly one line;
//! * if the merged fields contain `testMethod`, the line is routed to
//!   `<test_log_dir>/<testModule>/<testMethod>.jsonl`; otherwise to the process file.
//!
//! Redaction is the caller's responsibility (log `key_hex`, never values), but as a safety
//! net any field literally named `value`, `password`, `secret`, `token`, or `private_key`
//! is replaced with `"<redacted>"`.
//!
//! Per-test lines are written by one background thread, never by the logging thread. On
//! 2026-10-12 a single append to one per-test file blocked for 2.2–2.8 s on the Windows test
//! host. The layer then held a process-wide lock across that write, so every thread of every
//! test in the binary waited behind it, and whole cluster suites failed with deadline and
//! leader errors. Now a slow write delays only the writer thread. The cost is that a line is
//! on disk only once the writer reaches it: [`test_file_path`] and [`flush_test_logs`] wait
//! for that, and `#[retcd_test]` flushes again when the test ends
//! ([`crate::testing::FlushTestLogsOnDrop`]).

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Mutex, OnceLock};

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// Span fields captured at creation / record time. Public so
/// [`crate::context::TraceContext::current`] can read them back.
#[derive(Debug, Default, Clone)]
pub struct JsonFields(pub Map<String, Value>);

const REDACTED_FIELDS: &[&str] = &[
    "value",
    "password",
    "secret",
    "token",
    "private_key",
    "key_bytes",
];

/// Field name that triggers per-test file routing.
pub const TEST_METHOD_FIELD: &str = "testMethod";
/// Field name used for the per-test directory.
pub const TEST_MODULE_FIELD: &str = "testModule";

struct JsonVisitor<'a>(&'a mut Map<String, Value>);

impl Visit for JsonVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
    fn record_i128(&mut self, field: &Field, value: i128) {
        self.0
            .insert(field.name().to_string(), Value::from(value.to_string()));
    }
    fn record_u128(&mut self, field: &Field, value: u128) {
        self.0
            .insert(field.name().to_string(), Value::from(value.to_string()));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.0
            .insert(field.name().to_string(), Value::from(value.to_string()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), Value::from(format!("{value:?}")));
    }
}

fn level_name(level: &Level) -> &'static str {
    match *level {
        Level::TRACE => "Trace",
        Level::DEBUG => "Debug",
        Level::INFO => "Information",
        Level::WARN => "Warning",
        Level::ERROR => "Error",
    }
}

fn sanitize(component: &str) -> String {
    component
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Where lines go.
pub enum Sink {
    /// Any `Write` (the non-blocking appender in production, a `Vec<u8>` in tests).
    Writer(Box<dyn Write + Send>),
}

/// The layer. Construct through [`JsonlLayer::new`].
pub struct JsonlLayer {
    application: String,
    default_sink: Mutex<Sink>,
    test_log_dir: Option<PathBuf>,
    include_location: bool,
}

/// Work for the per-test writer thread, handled in the order it was sent.
enum WriterMsg {
    /// Append `line` to the file at the path.
    Line(PathBuf, Vec<u8>),
    /// Every line sent before this one is handled; answer with every open that has failed.
    Flush(mpsc::SyncSender<Vec<(PathBuf, String)>>),
}

/// The per-test writer, started by the first per-test line. One per process, shared by every
/// [`JsonlLayer`], so [`flush_test_logs`] covers them all.
static TEST_WRITER: OnceLock<mpsc::Sender<WriterMsg>> = OnceLock::new();

fn test_writer() -> &'static mpsc::Sender<WriterMsg> {
    TEST_WRITER.get_or_init(|| {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("retcd-test-log-writer".into())
            .spawn(move || run_test_writer(rx))
            .expect("spawn the per-test log writer thread");
        tx
    })
}

fn run_test_writer(rx: mpsc::Receiver<WriterMsg>) {
    let mut files: HashMap<PathBuf, File> = HashMap::new();
    // A file that would not open loses its own lines and nothing else. The writer is shared by
    // every test in the binary, so it records the failure for the file's reader and carries on.
    let mut failed: Vec<(PathBuf, String)> = Vec::new();
    for msg in rx {
        match msg {
            WriterMsg::Line(path, line) => {
                if failed.iter().any(|(p, _)| *p == path) {
                    continue;
                }
                let file = match files.entry(path) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => {
                        let path = e.key();
                        if let Some(parent) = path.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        match OpenOptions::new().create(true).append(true).open(path) {
                            Ok(file) => e.insert(file),
                            Err(err) => {
                                failed.push((e.into_key(), err.to_string()));
                                continue;
                            }
                        }
                    }
                };
                let _ = file.write_all(&line);
            }
            WriterMsg::Flush(done) => {
                let _ = done.send(failed.clone());
            }
        }
    }
}

/// Per-test log lines were lost, so a reader would see a file with lines missing.
#[derive(Debug, thiserror::Error)]
pub enum TestLogError {
    /// The per-test writer thread has stopped, so every line sent to it since was dropped.
    #[error("the per-test log writer thread has stopped; per-test log lines were lost")]
    WriterStopped,
    /// These per-test files could not be opened, so their lines were dropped.
    #[error("per-test log lines were lost: {}", describe_failed(.0))]
    OpenFailed(Vec<(PathBuf, String)>),
}

fn describe_failed(failed: &[(PathBuf, String)]) -> String {
    failed
        .iter()
        .map(|(path, err)| format!("cannot open test log {}: {err}", path.display()))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Wait until every per-test line logged before this call has been handled, and return
/// every per-test file that has failed to open so far.
fn flush_and_collect_failures() -> Result<Vec<(PathBuf, String)>, TestLogError> {
    let Some(tx) = TEST_WRITER.get() else {
        return Ok(Vec::new());
    };
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    tx.send(WriterMsg::Flush(done_tx))
        .map_err(|_| TestLogError::WriterStopped)?;
    done_rx.recv().map_err(|_| TestLogError::WriterStopped)
}

/// Wait until every per-test line logged before this call is in its file.
///
/// Fails if any per-test file in this process could not be opened, naming each one. Returns
/// at once in a process that never routed a line to a per-test file.
pub fn try_flush_test_logs() -> Result<(), TestLogError> {
    let failed = flush_and_collect_failures()?;
    if failed.is_empty() {
        Ok(())
    } else {
        Err(TestLogError::OpenFailed(failed))
    }
}

/// [`try_flush_test_logs`], panicking on failure: a reader must not see a file with lines
/// silently missing.
pub fn flush_test_logs() {
    if let Err(e) = try_flush_test_logs() {
        panic!("{e}");
    }
}

impl JsonlLayer {
    /// Create a layer writing to `default_writer`. If `test_log_dir` is set, events inside a
    /// test span are routed to per-test files under it.
    pub fn new(
        application: impl Into<String>,
        default_writer: Box<dyn Write + Send>,
        test_log_dir: Option<PathBuf>,
        include_location: bool,
    ) -> Self {
        Self {
            application: application.into(),
            default_sink: Mutex::new(Sink::Writer(default_writer)),
            test_log_dir,
            include_location,
        }
    }

    fn write_line(&self, fields: &Map<String, Value>, line: &[u8]) {
        if let Some(dir) = &self.test_log_dir {
            if let Some(Value::String(method)) = fields.get(TEST_METHOD_FIELD) {
                let module = fields
                    .get(TEST_MODULE_FIELD)
                    .and_then(Value::as_str)
                    .unwrap_or("unknown_module");
                let path = test_file_path_of(dir, module, method);
                // A send fails only once the writer has stopped; `flush_test_logs` reports
                // that to whoever reads the file next.
                let _ = test_writer().send(WriterMsg::Line(path, line.to_vec()));
                return;
            }
        }
        let mut sink = self.default_sink.lock().unwrap_or_else(|e| e.into_inner());
        match &mut *sink {
            Sink::Writer(w) => {
                let _ = w.write_all(line);
            }
        }
    }
}

/// Path of the per-test JSONL file for `(module, method)` under `dir`.
///
/// Flushes first ([`flush_test_logs`]), so the file holds every line logged before the call.
/// Every reader of its own test log gets the path here, which is why the flush lives here and
/// not at each call site. A reader that finds a log file some other way calls
/// [`flush_test_logs`] itself. In a process with no per-test routing, such as a daemon, the
/// flush returns at once.
///
/// Panics if the writer has stopped, or if this file could not be opened, with the path and the
/// cause. Another test's failed file is not this caller's problem, so it does not panic here.
pub fn test_file_path(dir: &Path, module: &str, method: &str) -> PathBuf {
    let path = test_file_path_of(dir, module, method);
    let failed = flush_and_collect_failures().unwrap_or_else(|e| panic!("{e}"));
    if let Some((_, err)) = failed.iter().find(|(p, _)| *p == path) {
        panic!(
            "per-test log lines were lost: cannot open test log {}: {err}",
            path.display()
        );
    }
    path
}

fn test_file_path_of(dir: &Path, module: &str, method: &str) -> PathBuf {
    dir.join(sanitize(&module.replace("::", ".")))
        .join(format!("{}.jsonl", sanitize(method)))
}

impl<S> Layer<S> for JsonlLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut fields = JsonFields::default();
        attrs.record(&mut JsonVisitor(&mut fields.0));
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        if let Some(fields) = ext.get_mut::<JsonFields>() {
            values.record(&mut JsonVisitor(&mut fields.0));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut merged: Map<String, Value> = Map::new();

        // root -> leaf so leaf overrides
        let mut span_name = None;
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(f) = span.extensions().get::<JsonFields>() {
                    for (k, v) in &f.0 {
                        merged.insert(k.clone(), v.clone());
                    }
                }
                span_name = Some(span.name());
            }
        }

        let mut ev: Map<String, Value> = Map::new();
        event.record(&mut JsonVisitor(&mut ev));
        let message = ev.remove("message").unwrap_or(Value::String(String::new()));
        for (k, v) in ev {
            merged.insert(k, v);
        }
        for k in REDACTED_FIELDS {
            if merged.contains_key(*k) {
                merged.insert((*k).to_string(), Value::from("<redacted>"));
            }
        }

        let mut out: Map<String, Value> = Map::with_capacity(merged.len() + 8);
        out.insert(
            "@t".into(),
            Value::from(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)),
        );
        out.insert("@l".into(), Value::from(level_name(meta.level())));
        out.insert("@m".into(), message);
        out.insert("@logger".into(), Value::from(meta.target()));
        out.insert("application".into(), Value::from(self.application.as_str()));
        if let Some(n) = span_name {
            out.insert("span".into(), Value::from(n));
        }
        if self.include_location {
            if let Some(f) = meta.file() {
                out.insert("file".into(), Value::from(f));
            }
            if let Some(l) = meta.line() {
                out.insert("line".into(), Value::from(l));
            }
        }
        out.insert(
            "thread".into(),
            Value::from(std::thread::current().name().unwrap_or("unnamed")),
        );
        for (k, v) in &merged {
            out.insert(k.clone(), v.clone());
        }

        let mut line = serde_json::to_vec(&out).unwrap_or_default();
        line.push(b'\n');
        self.write_line(&merged, &line);
    }
}
