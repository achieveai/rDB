//! Row M7F-01: the seed is honest about what it has not built.
//!
//! The only assertion team foundation can make today, and the one that matters most: every kernel
//! package reports [`CapabilityState::Unavailable`], and the campaign survives asking. A seed that
//! answered anything else — a panic from `todo!()`, or a fake success — would make the first
//! genuinely green campaign indistinguishable from this one.
//!
//! This row is expected to **change** as packages land. When A1 wires authority, the first slot
//! becomes `Wired` and this file's expectation moves with it. That is the point: the flip is a
//! test edit, not an unobserved change in behaviour.

mod support;

use rdb_core::contracts::errors::{Capability, ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::event::ModuleName;
use rdb_core::contracts::trace::CapabilityState;
use rdb_sim::harness::dispatch::Dispatcher;

#[test]
fn m7f_01_every_kernel_package_reports_unavailable() {
    let ctx = support::ctx();
    let probe = support::probe_event();
    let mut dispatcher = Dispatcher::new();

    let report = dispatcher.capability_report(&ctx, &probe);

    assert_eq!(report, [CapabilityState::Unavailable; 6]);
    assert_eq!(ModuleName::ALL.len(), report.len());
}

#[test]
fn m7f_01_unavailable_names_the_capability_that_is_missing() {
    let ctx = support::ctx();
    let probe = support::probe_event();
    let mut dispatcher = Dispatcher::new();

    for module in ModuleName::ALL {
        let error = dispatcher
            .step(module, &ctx, &probe)
            .expect_err("no kernel package is wired yet");

        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert_eq!(
            error.capability(),
            Some(module.capability()),
            "{module:?} must report its own capability, not a neighbour's"
        );
    }
}

/// An unwired seam is definitive about the one thing it can be definitive about.
///
/// Not-wired code cannot have mutated anything, so [`RetryRule::NotWired`] proves no mutation.
/// This is deliberately *not* the same claim as the control store's
/// [`rdb_core::contracts::control::CasOutcome::Unavailable`], which proves nothing at all — two
/// different types, on purpose, because conflating them is how a retry loop duplicates a write.
#[test]
fn m7f_01_unwired_is_definitive_and_not_retryable() {
    let error = RdbError::unavailable(Capability::Authority, "package A1 is not wired yet");

    assert_eq!(error.retry_rule(), RetryRule::NotWired);
    assert!(error.proves_no_mutation());
    assert_eq!(error.capability(), Some(Capability::Authority));
}
