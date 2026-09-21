//! The environment's own error type.
//!
//! Separate from [`RdbError`] on purpose. An `RdbError` is a *protocol decision* the oracle
//! checks; a [`SimError`] is the harness saying it cannot run the scenario. Collapsing them
//! would let a broken simulator look like a rejected request.

use rdb_core::contracts::errors::RdbError;

/// Something the simulation environment could not do.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SimError {
    /// A kernel module returned an error. Carried through unchanged; the trace records it and
    /// the oracle judges it.
    #[error(transparent)]
    Kernel(#[from] RdbError),

    /// An environment seam is not built yet. Explicit by design (spike §8).
    #[error("simulation seam unavailable: {seam}")]
    Unavailable {
        /// Which seam. A static name, never scenario data.
        seam: &'static str,
    },

    /// The scenario configuration is malformed or names something unknown.
    ///
    /// Spike §7 requires unknown environment and configuration fields to be errors, not
    /// defaults: a typo that silently disables a fault is a test that passes for the wrong
    /// reason.
    #[error("invalid scenario configuration: {field}")]
    Config {
        /// The offending field.
        field: &'static str,
    },

    /// The run hit its bound before it finished. Not a failure — spike §6 forbids calling an
    /// unhealed partition a liveness failure — but never silently a pass either.
    #[error("run exhausted its {bound} budget")]
    BudgetExhausted {
        /// Which bound: events, ticks or histories.
        bound: &'static str,
    },
}

impl SimError {
    /// Shorthand for the stub bodies of unbuilt seams.
    #[must_use]
    pub const fn unavailable(seam: &'static str) -> Self {
        Self::Unavailable { seam }
    }
}
