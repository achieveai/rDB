//! Recording.
//!
//! The recorder assigns [`TraceEvent::event_id`] and nothing else does. That single assignment
//! is what gives the oracle one total order and lets it be a left-to-right fold that never sorts
//! and never searches.
//!
//! JSONL, one event per line, is the on-disk form (team rules): the header on line 1, then one
//! [`TraceEvent`] per line. DuckDB reads it directly, which is how a failing campaign gets
//! queried instead of grepped.

use std::io::{BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use rdb_core::contracts::ids::{BootId, CorrelationId, EventId, NodeId, PartitionId};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{EventRef, Trace, TraceEvent, TraceHeader, TraceKind};
use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;

use crate::error::SimError;

/// Where a declaration was made: everything on a [`TraceEvent`] except the id and the kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Site {
    /// Simulated time.
    pub at: Tick,
    /// The node.
    pub node: NodeId,
    /// Its process lifetime.
    pub boot: BootId,
    /// The partition.
    pub partition: PartitionId,
    /// The request this ties back to.
    pub correlation: CorrelationId,
}

/// Collects trace events in the order they happen.
#[derive(Debug, Default)]
pub struct Recorder {
    header: Option<TraceHeader>,
    events: Vec<TraceEvent>,
    next: EventId,
}

impl Recorder {
    /// A recorder that has not yet seen its header.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open the trace with its header. Must be called once, before any event.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `header` when a header was already given.
    pub fn begin(&mut self, header: TraceHeader) -> Result<(), SimError> {
        if self.header.is_some() {
            return Err(SimError::Config { field: "header" });
        }
        self.header = Some(header);
        Ok(())
    }

    /// Record one declaration and return the `event_id` it was given, for back-references.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `header` when [`Recorder::begin`] has not been called: an
    /// event with no header is a trace that cannot be replayed, and recording it would only
    /// defer the failure to a reader.
    pub fn record(&mut self, site: Site, kind: TraceKind) -> Result<EventRef, SimError> {
        if self.header.is_none() {
            return Err(SimError::Config { field: "header" });
        }
        let event_id = self.next;
        self.next = EventId(event_id.0 + 1);
        self.events.push(TraceEvent {
            event_id,
            logical_tick: site.at.0,
            partition: site.partition,
            node: site.node,
            boot: site.boot,
            correlation: site.correlation,
            kind,
        });
        Ok(event_id)
    }

    /// The events recorded so far, in `event_id` order.
    #[must_use]
    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }

    /// Close the trace.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `header` when [`Recorder::begin`] was never called.
    pub fn finish(self) -> Result<Trace, SimError> {
        let header = self.header.ok_or(SimError::Config { field: "header" })?;
        Ok(Trace {
            header,
            events: self.events,
        })
    }
}

/// Write a trace as JSONL: the header on the first line, then one event per line.
///
/// # Errors
///
/// [`SimError::Io`] naming the operation that failed. Serialising a contract type cannot fail;
/// if it ever does, that is reported as `Io` on `serialize` rather than swallowed.
pub fn write_jsonl(trace: &Trace, path: &Path) -> Result<(), SimError> {
    let file = std::fs::File::create(path).map_err(|error| SimError::Io {
        op: "create",
        kind: error.kind(),
    })?;
    let mut out = BufWriter::new(file);
    write_line(&mut out, &trace.header)?;
    for event in &trace.events {
        write_line(&mut out, event)?;
    }
    out.flush().map_err(|error| SimError::Io {
        op: "flush",
        kind: error.kind(),
    })
}

fn write_line<T: serde::Serialize>(out: &mut impl Write, value: &T) -> Result<(), SimError> {
    let line = serde_json::to_string(value).map_err(|_| SimError::Io {
        op: "serialize",
        kind: std::io::ErrorKind::InvalidData,
    })?;
    out.write_all(line.as_bytes())
        .and_then(|()| out.write_all(b"\n"))
        .map_err(|error| SimError::Io {
            op: "write",
            kind: error.kind(),
        })
}

// ---------------------------------------------------------------------------------------------
// Tier 1: the query shape
// ---------------------------------------------------------------------------------------------

/// `@l` on every tier-1 line — the level name `config-log` writes for `tracing::info!`.
const LOG_LEVEL: &str = "Information";
/// `@logger` on every tier-1 line.
const LOG_TARGET: &str = "rdb_sim::harness::trace";

/// The test identity a tier-1 line carries, so a query's `WHERE testMethod = ?` reaches it.
///
/// The same three names `config-log`'s root span records (`config_log::testing::test_span`),
/// spelled the same way, because a Q-row filters both kinds of line with one predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogTags<'a> {
    /// `testModule` — the emitting test's `module_path!()`.
    pub test_module: &'a str,
    /// `testMethod` — the emitting test's function name.
    pub test_method: &'a str,
    /// `testRun` — `config_log::testing::test_run_id()`.
    pub test_run: &'a str,
    /// `application` — `retcd-tests` under the test subscriber.
    pub application: &'a str,
}

impl<'a> LogTags<'a> {
    /// What `config-log` sets `application` to in a test binary.
    pub const TEST_APPLICATION: &'static str = "retcd-tests";

    /// Tags for a test row, with `application` set to [`LogTags::TEST_APPLICATION`].
    #[must_use]
    pub const fn new(test_module: &'a str, test_method: &'a str, test_run: &'a str) -> Self {
        Self {
            test_module,
            test_method,
            test_run,
            application: Self::TEST_APPLICATION,
        }
    }
}

/// One [`TraceEvent`] as one JSONL line in the shape the DuckDB query rows read
/// (`docs/testing/m7-log-fields.md`, tier 1).
///
/// `@m` is the [`TraceKind`] variant in snake_case, the envelope sits under its landed names,
/// and the variant's own fields are flattened beside them under their serde names. A tuple field
/// stays a list of two-element lists and a struct field stays a list of structs; verification's
/// Q-35 indexes both, so neither is flattened or renamed.
///
/// # Why this is not a `tracing::info!`
///
/// The contract asks for one `tracing` event per recorded [`TraceEvent`]. That cannot carry this
/// shape. `tracing` field names are `&'static str` fixed at the call site, so 24 variants with
/// different field sets cannot come from one call; and `config-log`'s visitor has only the
/// scalar `record_*` methods, so anything composite arrives through `Debug` and is stored as a
/// **string** — `nodes` would land as `"[(NodeId(1), Primary)]"` rather than as a list DuckDB
/// can index. Emitting the line here keeps the shape the queries were written against.
///
/// # Why not [`write_jsonl`]
///
/// That is the replay round trip: a header line and externally tagged events that
/// [`read_jsonl`] parses straight back. It is byte-shaped for a reader, not column-shaped for a
/// query, and changing it would break replay. This is a separate path over the same events.
///
/// # Errors
///
/// [`SimError::Io`] on `serialize` if a [`TraceKind`] ever stops being an externally tagged
/// struct variant — a unit or tuple variant has no fields to flatten, and silently emitting a
/// line with no columns would make every query over it read as a clean run.
pub fn log_line(event: &TraceEvent) -> Result<Map<String, Value>, SimError> {
    let Value::Object(tagged) = json(&event.kind)? else {
        return Err(malformed_kind());
    };
    let mut tagged = tagged.into_iter();
    let (Some((variant, fields)), None) = (tagged.next(), tagged.next()) else {
        return Err(malformed_kind());
    };
    let Value::Object(fields) = fields else {
        return Err(malformed_kind());
    };

    let mut line = Map::new();
    line.insert("@m".to_owned(), Value::from(snake_case(&variant)));
    line.insert("@l".to_owned(), Value::from(LOG_LEVEL));
    line.insert("@logger".to_owned(), Value::from(LOG_TARGET));
    for (name, value) in fields {
        line.insert(name, value);
    }
    // The envelope goes in last on purpose: it is the identity of the line, and a variant field
    // that one day shares one of these six names must not be able to take it over.
    line.insert("event_id".to_owned(), json(&event.event_id)?);
    line.insert("logical_tick".to_owned(), Value::from(event.logical_tick));
    line.insert("partition".to_owned(), json(&event.partition)?);
    line.insert("node".to_owned(), json(&event.node)?);
    line.insert("boot".to_owned(), json(&event.boot)?);
    line.insert("correlation".to_owned(), json(&event.correlation)?);
    Ok(line)
}

/// Where the tier-1 lines for one test go: the test's own directory under the log root, in a
/// file beside `config-log`'s.
///
/// A separate file, not `config-log`'s own. Two writers holding one appending handle is the
/// same class of hazard as two cargo runs sharing a target directory, and it would surface as a
/// torn line in somebody else's query rather than as a failure here. The name still ends
/// `.jsonl` and still sits at `<run>/<module>/`, which is what
/// `config_testkit::logs::test_logs_relation` walks and what the `**/*.jsonl` glob in every
/// Q-row matches.
///
/// Note that `config_testkit::logs::relation_for_current_test` names `<method>.jsonl` exactly,
/// so it does **not** see these lines; a query for them wants the run-wide relation with a
/// `WHERE testMethod = ?`.
#[must_use]
pub fn log_jsonl_path(test_log_dir: &Path, test_module: &str, test_method: &str) -> PathBuf {
    test_log_dir
        .join(sanitize(&test_module.replace("::", ".")))
        .join(format!("{}.trace.jsonl", sanitize(test_method)))
}

/// Append one tier-1 line per event to `path`, each tagged with `tags`.
///
/// Appends rather than truncates so a row may record in stages, and so this never silently
/// discards lines a previous call wrote.
///
/// # Errors
///
/// [`SimError::Io`] naming the operation that failed, or propagated from [`log_line`].
pub fn write_log_jsonl(
    events: &[TraceEvent],
    tags: &LogTags<'_>,
    path: &Path,
) -> Result<(), SimError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| SimError::Io {
            op: "create_dir_all",
            kind: error.kind(),
        })?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| SimError::Io {
            op: "open",
            kind: error.kind(),
        })?;
    let mut out = BufWriter::new(file);
    for event in events {
        let mut line = log_line(event)?;
        line.insert("application".to_owned(), Value::from(tags.application));
        line.insert("testModule".to_owned(), Value::from(tags.test_module));
        line.insert("testMethod".to_owned(), Value::from(tags.test_method));
        line.insert("testRun".to_owned(), Value::from(tags.test_run));
        write_line(&mut out, &Value::Object(line))?;
    }
    out.flush().map_err(|error| SimError::Io {
        op: "flush",
        kind: error.kind(),
    })
}

fn json<T: serde::Serialize>(value: &T) -> Result<Value, SimError> {
    serde_json::to_value(value).map_err(|_| SimError::Io {
        op: "serialize",
        kind: std::io::ErrorKind::InvalidData,
    })
}

fn malformed_kind() -> SimError {
    SimError::Io {
        op: "serialize",
        kind: std::io::ErrorKind::InvalidData,
    }
}

/// `ClientOutcomeReported` -> `client_outcome_reported`: the `@m` vocabulary the query rows name.
fn snake_case(variant: &str) -> String {
    let mut out = String::with_capacity(variant.len() + 4);
    for (index, ch) in variant.char_indices() {
        if ch.is_ascii_uppercase() {
            if index != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// The same path-component rule `config-log`'s layer applies, so the tier-1 file lands in the
/// directory `config-log` routed that test's own lines to and one `WHERE` reaches both.
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

/// Read a trace back from JSONL.
///
/// Refuses a file whose `schema_version` is not [`TRACE_SCHEMA_VERSION`]. A schema bump
/// invalidates checked-in fixtures on purpose, and silently reading an old one would be worse
/// than failing. Refuses a header with a field this build does not know, for the same reason
/// (`deny_unknown_fields` on [`TraceHeader`]).
///
/// # Errors
///
/// [`SimError::Io`] when the file cannot be read; [`SimError::Malformed`] naming the line that
/// could not be parsed, the header being line 1 and an empty file counting as a missing line 1;
/// [`SimError::Config`] naming `schema_version` on a schema mismatch.
pub fn read_jsonl(path: &Path) -> Result<Trace, SimError> {
    let file = std::fs::File::open(path).map_err(|error| SimError::Io {
        op: "open",
        kind: error.kind(),
    })?;
    let mut lines = std::io::BufReader::new(file).lines();
    let first = lines
        .next()
        .ok_or(SimError::Malformed { line: 1 })?
        .map_err(|error| SimError::Io {
            op: "read",
            kind: error.kind(),
        })?;
    let header: TraceHeader =
        serde_json::from_str(&first).map_err(|_| SimError::Malformed { line: 1 })?;
    if header.schema_version != TRACE_SCHEMA_VERSION {
        return Err(SimError::Config {
            field: "schema_version",
        });
    }
    let mut events = Vec::new();
    for (index, line) in lines.enumerate() {
        let number = index as u64 + 2;
        let line = line.map_err(|error| SimError::Io {
            op: "read",
            kind: error.kind(),
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let event: TraceEvent =
            serde_json::from_str(&line).map_err(|_| SimError::Malformed { line: number })?;
        events.push(event);
    }
    Ok(Trace { header, events })
}
