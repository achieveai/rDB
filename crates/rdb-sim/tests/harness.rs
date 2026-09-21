//! Rows M7F-01 and M7F-22: the seed is honest about what it has not built, and says so in a
//! file DuckDB can read.
//!
//! M7F-01 is the assertion that matters most today: every kernel package reports
//! [`CapabilityState::Unavailable`] from [`Module::capability`] without being stepped, and
//! stepping one through the dispatcher returns `Unavailable` with **no effect** and never
//! panics. A seed that answered anything else — a panic from `todo!()`, or a fake success —
//! would make the first genuinely green campaign indistinguishable from this one.
//!
//! This row is expected to **change** as packages land. When A1 wires authority, the first slot
//! becomes `Wired` and this file's expectation moves with it. That is the point: the flip is a
//! test edit, not an unobserved change in behaviour.
//!
//! M7F-22 (finding K-F-30) is the row-level proof that every row here is a `#[retcd_test]`: it
//! reads its own JSONL file back and finds the three `Capability` lines the preamble wrote.

mod support;

use config_log::layer::test_file_path;
use config_log::retcd_test;
use config_log::testing::test_log_dir;
use rdb_core::contracts::errors::{Capability, ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::event::{Module, ModuleName};
use rdb_core::contracts::trace::{CapabilityState, PackageId};
use rdb_sim::harness::dispatch::Dispatcher;
use rdb_sim::harness::environment_capabilities;

#[retcd_test]
fn m7f_01_every_kernel_package_reports_unavailable_without_being_stepped() {
    support::preamble();
    let dispatcher = Dispatcher::new();

    let report = dispatcher.capability_report();

    assert_eq!(report, [CapabilityState::Unavailable; 6]);
    assert_eq!(ModuleName::ALL.len(), report.len());
    // The default answer, straight from the trait, for a module nobody has stepped.
    assert_eq!(
        rdb_core::authority::Authority.capability(),
        CapabilityState::Unavailable
    );
}

#[retcd_test]
fn m7f_01_stepping_an_unwired_module_returns_unavailable_and_no_effect() {
    support::preamble();
    let ctx = support::ctx();
    let probe = support::probe_event();
    let mut dispatcher = Dispatcher::new();

    for module in ModuleName::ALL {
        let error = dispatcher
            .step(module, &ctx, &probe)
            .expect_err("no kernel package is wired yet: no effect may come back");

        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert_eq!(
            error.capability(),
            Some(module.capability()),
            "{module:?} must report its own capability, not a neighbour's"
        );
    }
    assert!(
        dispatcher.take_replies().is_empty(),
        "an unwired module handed nothing to the environment"
    );
}

/// An unwired seam proves nothing about mutation (finding K-F-26).
///
/// `NotWired` used to claim `proves_no_mutation`. It cannot: a partially wired module may have
/// emitted effects before an unwired neighbour refused, and a retry loop that trusted the claim
/// would duplicate a write. The claim is now the same as the control store's
/// [`rdb_core::contracts::control::CasOutcome::Unavailable`] — nothing is proved — while the
/// retry rule stays `NotWired`, so the two are still told apart.
#[retcd_test]
fn m7f_01_unwired_is_definitive_and_proves_no_mutation_claim() {
    support::preamble();
    let error = RdbError::unavailable(Capability::Authority, "package A1 is not wired yet");

    assert_eq!(error.retry_rule(), RetryRule::NotWired);
    assert!(
        !error.proves_no_mutation(),
        "not-wired proves nothing about what a neighbour did (K-F-26)"
    );
    assert_eq!(error.capability(), Some(Capability::Authority));
}

/// The environment is as honest as the kernel: H1 and I1 still owe seams and say so.
#[retcd_test]
fn m7f_22_environment_capabilities_name_what_is_owed() {
    support::preamble();
    let report = environment_capabilities();

    assert_eq!(report[0], (PackageId::H1, CapabilityState::Unavailable));
    assert_eq!(report[1], (PackageId::M1, CapabilityState::Wired));
    assert_eq!(report[2], (PackageId::I1, CapabilityState::Unavailable));
}

/// Every row here writes one JSONL file under the test log root, and its first three lines are
/// the `Capability` lines. Read back synchronously: `config-log` appends per-test files with a
/// blocking `write_all`, so the row's own lines are on disk before this assertion runs.
#[retcd_test]
fn m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root() {
    support::preamble();
    let path = test_file_path(
        &test_log_dir(),
        module_path!(),
        "m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root",
    );

    let text = std::fs::read_to_string(&path).expect("the row's own JSONL file exists");
    let lines: Vec<serde_json::Value> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("every line is one JSON object"))
        .collect();

    let capability_lines = lines
        .iter()
        .filter(|line| line["@m"] == "capability")
        .count();
    let packages: Vec<&str> = lines
        .iter()
        .filter(|line| line["@m"] == "capability")
        .filter_map(|line| line["package"].as_str())
        .collect();
    tracing::info!(lines = lines.len(), capability_lines, "m7f_22 self-read");

    assert_eq!(capability_lines, 3, "one line per environment package");
    assert_eq!(packages, ["H1", "M1", "I1"], "in package order");
    assert!(
        lines.iter().all(|line| line["testMethod"]
            == "m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root"),
        "every line carries this row's testMethod"
    );
}
