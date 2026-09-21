//! The harness: run a scenario, record a trace, replay it.
//!
//! Package I1. The seam every other team writes tests against, and the only place the simulator
//! and the kernel meet.
//!
//! | Module | What it owns |
//! |---|---|
//! | [`self::dispatch`] | the module registry and the capability report |
//! | [`self::trace`] | recording [`rdb_core::contracts::trace::Trace`] and writing JSONL |
//! | [`self::replay`] | re-running a recorded trace and proving the result is identical |
//!
//! # Seed state
//!
//! [`self::dispatch::Dispatcher`] is real: it registers the six kernel modules and reports each
//! one's capability. That is what lets every team run a row today and watch it flip from
//! `Unavailable` to `Wired` as their package lands. Recording and replay are I1.

pub mod dispatch;
pub mod replay;
pub mod trace;
