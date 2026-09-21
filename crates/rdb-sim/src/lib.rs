//! The rDB deterministic simulation environment (M7): everything the kernel is *not*.
//!
//! One kernel, replaceable environment (spike §6). `rdb-core` holds every protocol decision;
//! this crate holds the scheduler, the clock, the network, the fake control store, the memory
//! storage engine, the effect dispatcher, the canonical trace and the replayer. It contains no
//! second implementation of the protocol, and a fix that belongs in the kernel must never be
//! made here.
//!
//! # The determinism rules this crate must keep
//!
//! * No `Instant::now`, no `SystemTime`, no `rand`. Logical time comes from [`sim::clock`] and
//!   every generated choice is recorded in the trace.
//! * No `HashMap` or `HashSet` anywhere on a trace path. Ordered maps only — iteration order is
//!   part of the output.
//! * No thread, no async runtime, no socket, no file on a core test path.
//! * Jump to the next deadline; never tick through idle milliseconds (spike §6).
//!
//! # Layout
//!
//! | Module | Package | Owns |
//! |---|---|---|
//! | [`sim`] | H1 | scheduler, clock and timers, network, fake control store, cluster |
//! | [`storage`] | M1 | ordered memory engine, crash images, snapshots |
//! | [`harness`] | I1 | effect dispatch, canonical trace, replay |
//! | [`error`] | H1 | the environment's own error type |
//!
//! # Seed state (2026-09-20)
//!
//! The types and signatures are the contract. Behaviour that is not built yet returns an
//! explicit [`error::SimError::Unavailable`] rather than pretending (spike §8), and nothing here
//! calls `todo!()` — a panic would abort the campaign runner instead of letting it report which
//! package is missing. Two things *are* built, because three other teams need them to compile
//! against: [`harness::dispatch::Dispatcher`]'s registry and capability report, and
//! [`storage::snapshot::EmptySnapshot`].
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod error;
pub mod harness;
pub mod sim;
pub mod storage;

pub use error::SimError;
