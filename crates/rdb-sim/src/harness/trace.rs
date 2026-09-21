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
use std::path::Path;

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
