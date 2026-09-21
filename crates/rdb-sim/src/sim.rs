//! The deterministic environment: time, delivery, control metadata and topology (package H1).
//!
//! Everything a real deployment would get from the operating system, the network or rEtcd, the
//! scenario gets to decide instead — and every decision is recorded, so the same event log
//! replays to the same trace.

pub mod clock;
pub mod cluster;
pub mod control;
pub mod network;
pub mod scheduler;
