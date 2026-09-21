//! Invariant checkers. **Owned by team verification, package O1.**
//!
//! Registered here by team foundation so the seam exists on day one and O1 can land without
//! touching [`super`]. Empty on purpose: a checker written by anyone other than the team that
//! owns the oracle would be the second implementation of the protocol that spike §6 forbids.
//!
//! What goes here reads [`rdb_core::contracts::trace::Trace`] and nothing else — never simulator
//! state, never a kernel type's internals. The oracle judges what the system declared, not what
//! it can be asked.
