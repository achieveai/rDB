//! Recording.
//!
//! The recorder assigns [`rdb_core::contracts::trace::TraceEvent::event_id`] and nothing else
//! does. That single assignment is what gives the oracle one total order and lets it be a
//! left-to-right fold that never sorts and never searches.
//!
//! JSONL, one event per line, is the on-disk form (team rules). DuckDB reads it directly, which
//! is how a failing campaign gets queried instead of grepped.
//!
//! # Seed state
//!
//! Signatures only; package I1 lands the recorder.

use std::path::Path;

use rdb_core::contracts::trace::{Trace, TraceEvent, TraceHeader, TraceKind};

use crate::error::SimError;

/// Collects trace events in the order they happen.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Recorder;

impl Recorder {
    /// A recorder that has not yet seen its header.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Open the trace with its header. Must be called before any event.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package I1 lands the recorder.
    pub fn begin(&mut self, _header: TraceHeader) -> Result<(), SimError> {
        Err(SimError::unavailable("harness::trace::Recorder::begin"))
    }

    /// Record one event and return the `event_id` it was given.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package I1 lands the recorder.
    pub fn record(&mut self, _kind: TraceKind) -> Result<TraceEvent, SimError> {
        Err(SimError::unavailable("harness::trace::Recorder::record"))
    }

    /// Close the trace.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package I1 lands the recorder.
    pub const fn finish(self) -> Result<Trace, SimError> {
        Err(SimError::unavailable("harness::trace::Recorder::finish"))
    }
}

/// Write a trace as JSONL: the header on the first line, then one event per line.
///
/// # Errors
///
/// [`SimError::Unavailable`] until package I1 lands the recorder.
pub const fn write_jsonl(_trace: &Trace, _path: &Path) -> Result<(), SimError> {
    Err(SimError::unavailable("harness::trace::write_jsonl"))
}

/// Read a trace back from JSONL.
///
/// Refuses a file whose `schema_version` is not
/// [`rdb_core::contracts::version::TRACE_SCHEMA_VERSION`]. A schema bump invalidates checked-in
/// fixtures on purpose, and silently reading an old one would be worse than failing.
///
/// # Errors
///
/// [`SimError::Unavailable`] until package I1 lands the recorder.
pub const fn read_jsonl(_path: &Path) -> Result<Trace, SimError> {
    Err(SimError::unavailable("harness::trace::read_jsonl"))
}
