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
//!
//! M7F-50 … M7F-52 are the tier-1 trace serialiser (`docs/testing/m7-log-fields.md`): one
//! `TraceEvent`, one JSONL line, `@m` the variant in snake_case, the envelope under its landed
//! names and the variant's own fields flattened under their serde names. Until it landed, 19 of
//! the 37 cross-team query rows returned zero rows — and a query that returns zero rows looks
//! exactly like a clean run.

mod support;

use config_log::layer::test_file_path;
use config_log::retcd_test;
use config_log::testing::{test_log_dir, test_run_id};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::{Capability, ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::event::{Module, ModuleName};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, EventId, Generation, NodeId, PartitionId, ReplicaRole,
    Seq,
};
use rdb_core::contracts::trace::{
    AckEvidence, CapabilityState, DurabilityClass, PackageId, TraceEvent, TraceKind,
};
use rdb_sim::harness::dispatch::Dispatcher;
use rdb_sim::harness::environment_capabilities;
use rdb_sim::harness::trace::{log_jsonl_path, log_line, write_log_jsonl, LogTags};

#[retcd_test]
fn m7f_01_every_kernel_package_reports_unavailable_without_being_stepped() {
    support::preamble();
    let dispatcher = Dispatcher::new();

    let report = dispatcher.capability_report();

    assert_eq!(report, [CapabilityState::Unavailable; 6]);
    assert_eq!(ModuleName::ALL.len(), report.len());
    // The default answer, straight from the trait, for a module nobody has stepped.
    assert_eq!(
        rdb_core::authority::Authority::new().capability(),
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

// ---------------------------------------------------------------------------------------------
// The tier-1 trace serialiser (M7F-50 … M7F-52)
// ---------------------------------------------------------------------------------------------

/// A `TopologyChange` on node 2, partition 1, at tick 7 — the variant verification's Q-35 reads
/// for the role in force, and the one whose `nodes` field pins the tuple shape.
fn topology_event() -> TraceEvent {
    TraceEvent {
        event_id: EventId(3),
        logical_tick: 7,
        partition: PartitionId(1),
        node: NodeId(2),
        boot: BootId(5),
        correlation: CorrelationId(11),
        kind: TraceKind::TopologyChange {
            config_version: ConfigVersion(4),
            nodes: vec![
                (NodeId(1), ReplicaRole::Primary),
                (NodeId(2), ReplicaRole::RegularSecondary),
            ],
        },
    }
}

/// A `Publish` carrying two `AckEvidence` entries: the struct-list shape Q-35 indexes into.
fn publish_event() -> TraceEvent {
    TraceEvent {
        event_id: EventId(4),
        logical_tick: 9,
        partition: PartitionId(1),
        node: NodeId(1),
        boot: BootId(5),
        correlation: CorrelationId(11),
        kind: TraceKind::Publish {
            generation: Generation(2),
            seq: Seq(12),
            published_digest: Digest::ROOT,
            ack_evidence: vec![
                AckEvidence {
                    node: NodeId(1),
                    boot: BootId(5),
                    role: ReplicaRole::Primary,
                    durability: DurabilityClass::Durable,
                },
                AckEvidence {
                    node: NodeId(2),
                    boot: BootId(5),
                    role: ReplicaRole::RegularSecondary,
                    durability: DurabilityClass::Buffered,
                },
            ],
            authority_recheck: EventId(3),
        },
    }
}

/// M7F-50: one event becomes one line — `@m` is the variant in snake_case, the envelope sits
/// under its landed names, and the variant's own fields are flattened beside them.
///
/// The flattening is the whole point. A line that nested the variant's fields under a
/// `kind` object would still be valid JSON and would still round-trip, and every Q-row naming a
/// bare column would still bind to nothing.
#[retcd_test]
fn m7f_50_one_trace_event_becomes_one_flattened_jsonl_line() {
    support::preamble();
    let line = log_line(&topology_event()).expect("a landed variant serialises");

    assert_eq!(line["@m"], "topology_change", "the variant, in snake_case");
    assert_eq!(line["@l"], "Information");

    assert_eq!(line["event_id"], 3);
    assert_eq!(line["logical_tick"], 7);
    assert_eq!(line["partition"], 1);
    assert_eq!(line["node"], 2);
    assert_eq!(line["boot"], 5);
    assert_eq!(line["correlation"], 11);

    // Flattened, not nested: the variant's own field is a top-level column.
    assert_eq!(line["config_version"], 4);
    assert!(
        line.get("kind").is_none() && line.get("TopologyChange").is_none(),
        "the variant name is the message, never a wrapping object: {line:?}"
    );
}

/// M7F-51: the composite shapes survive untouched.
///
/// `m7-log-fields.md` states that a tuple field serialises as a list of two-element lists and a
/// struct field as a list of structs, and that the serialiser "must not flatten or rename
/// either shape" because verification's Q-35 indexes them. `tracing` cannot carry either —
/// `config-log`'s visitor renders anything composite through `Debug` into a string — so this row
/// is what proves the serialiser did not take that path.
#[retcd_test]
fn m7f_51_tuple_and_struct_fields_keep_their_json_shape() {
    support::preamble();
    let topology = log_line(&topology_event()).expect("serialises");
    let publish = log_line(&publish_event()).expect("serialises");

    let nodes = topology["nodes"]
        .as_array()
        .expect("a list, not a debug string");
    assert_eq!(nodes.len(), 2);
    let first = nodes[0].as_array().expect("a two-element list");
    assert_eq!(first.len(), 2, "DuckDB indexes these as [1] and [2]");
    assert_eq!(first[0], 1);
    assert_eq!(first[1], "Primary");

    let evidence = publish["ack_evidence"]
        .as_array()
        .expect("a list of structs, not a debug string");
    assert_eq!(evidence.len(), 2);
    assert_eq!(evidence[0]["node"], 1);
    assert_eq!(evidence[0]["role"], "Primary");
    assert_eq!(evidence[1]["durability"], "Buffered");

    // The back-reference is a bare id, so a query can join a publish to its recheck.
    assert_eq!(publish["authority_recheck"], 3);
}

/// M7F-52: the lines land in a file under the test log root, tagged so every Q-row's
/// `WHERE testMethod = ?` and `@m` filters reach them.
///
/// Written beside `config-log`'s own file for this row rather than into it. Two writers holding
/// one appending handle is the same hazard as two cargo runs sharing a target directory, and it
/// would show up as a torn line in somebody else's query rather than as a failure here.
#[retcd_test]
fn m7f_52_serialised_lines_land_in_a_tagged_file_under_the_test_log_root() {
    support::preamble();
    let method = "m7f_52_serialised_lines_land_in_a_tagged_file_under_the_test_log_root";
    let tags = LogTags::new(module_path!(), method, test_run_id());
    let path = log_jsonl_path(&test_log_dir(), module_path!(), method);

    let events = [topology_event(), publish_event()];
    write_log_jsonl(&events, &tags, &path).expect("the tier-1 lines are written");

    let text = std::fs::read_to_string(&path).expect("the tier-1 file exists");
    let lines: Vec<serde_json::Value> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("every line is one JSON object"))
        .collect();

    assert_eq!(lines.len(), 2, "one line per event, and no header line");
    assert_eq!(lines[0]["@m"], "topology_change");
    assert_eq!(lines[1]["@m"], "publish");
    for line in &lines {
        assert_eq!(line["testModule"], module_path!());
        assert_eq!(line["testMethod"], method);
        assert_eq!(line["testRun"], test_run_id());
        assert_eq!(line["application"], "retcd-tests");
    }

    // Field discipline (Q-45, Q-48, Q-60): no line carries a key byte, a value byte or a
    // payload. Asserted here as well as in the crate-wide query, because a new variant reaches
    // this row before it reaches a gate.
    for line in &lines {
        let object = line.as_object().expect("one object per line");
        for name in ["key", "value", "payload", "key_bytes"] {
            assert!(
                !object.contains_key(name),
                "a tier-1 line may never carry `{name}`: {line}"
            );
        }
    }
}
