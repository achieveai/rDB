//! rDB documents (M8 S2): the deterministic CBOR profile, the object envelope and path ops,
//! compiled to one whole-document `Put` (ADR-rdb-0012).
//!
//! - [`cbor`]: canonical [`cbor::encode`] and one strict [`cbor::decode`]. Non-canonical input is
//!   refused with a named error, never repaired.
//! - [`envelope`]: the 40-byte header (format, kind, codec, digest algorithm, length, SHA-256).
//! - [`path`]: JSON Pointer paths. [`delta`]: ops and [`delta::materialize`].
//! - [`compile()`]: snapshot → after-image `Put` + its precondition. [`read`]: snapshot → document.
//! - [`testing::MapSnapshot`]: an in-memory `SnapshotRead`.
//!
//! Pure (ADR-rdb-0011): no clock, no I/O, no logging. Every outcome is a returned value.
//! Manual entry point: `cargo run -p rdb-value --example doc_scenario -- --help`.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod cbor;
mod compile;
pub mod delta;
pub mod envelope;
pub mod keys;
pub mod path;
pub mod testing;
pub mod value;

pub use compile::{compile, read, Compiled, Corrupt, Document, Expected, ValueError};
