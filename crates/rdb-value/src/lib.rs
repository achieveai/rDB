//! rDB values (M8 S2, S3): the deterministic CBOR profile, the object envelope and path ops,
//! compiled to one whole-document `Put` (ADR-rdb-0012); maps and sets, a root record plus one
//! record per element (ADR-rdb-0013).
//!
//! - [`cbor`]: canonical [`cbor::encode`] and one strict [`cbor::decode`]. Non-canonical input is
//!   refused with a named error, never repaired.
//! - [`envelope`]: the 40-byte header (format, kind, codec, digest algorithm, length, SHA-256).
//! - [`path`]: JSON Pointer paths. [`delta`]: ops and [`delta::materialize`].
//! - [`compile()`]: snapshot → after-image `Put` + its precondition. [`read`]: snapshot → document.
//! - [`keys`]: the object-key layout and [`keys::RootKey`] (ADR-rdb-0013 §1–§6).
//! - [`collection`]: maps and sets, a root record plus one record per element (ADR-rdb-0013 §7–§13).
//! - [`blob`]: large blobs, chunk records and one manifest root (ADR-rdb-0014).
//! - [`list`]: ordered lists, a root, one record per item, and pages (ADR-rdb-0016).
//! - [`testing::MapSnapshot`]: an in-memory `SnapshotRead`.
//!
//! Pure (ADR-rdb-0011): no clock, no I/O, no logging. Every outcome is a returned value.
//! Manual entry point: `cargo run -p rdb-value --example doc_scenario -- --help`.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod blob;
pub mod cbor;
pub mod collection;
mod compile;
pub mod delta;
pub mod envelope;
pub mod keys;
pub mod list;
pub mod path;
pub mod testing;
pub mod value;

pub use compile::{
    compile, read, Compiled, Corrupt, Document, Expected, ManifestError, ValueError,
};
