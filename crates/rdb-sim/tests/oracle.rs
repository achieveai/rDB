//! Oracle rows M7V-01..M7V-41, plus M7V-66..68, M7V-71, M7V-79, M7V-81 and M7V-85.
//!
//! Every row here is a hand-built trace judged by the oracle, so every one of them runs against
//! the **landed C0 contract** and needs no runner. Each checker gets a bad trace that trips it and
//! a valid trace that does not (charter O1): the near-miss is not decoration, it is what stops a
//! checker that fires on everything from looking correct.
//!
//! Rows on hold are not written here, and the handoff says which and why. A stub that asserts
//! nothing would be worse than an absence, because it would count as a row.

mod support;

use std::collections::BTreeSet;

use config_log::retcd_test;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::ErrorKind;
use rdb_core::contracts::ids::{
    BootId, ClientId, ConfigVersion, CorrelationId, EventId, Generation, NodeId, OwnerEpoch,
    PartitionId, ReplicaRole, RequestId, Seq, TenantId,
};
use rdb_core::contracts::trace::{
    AdmissionOutcome, ApplyOutcome, AuthorityGate, AuthorityOutcome, ClientOutcome, DedupAction,
    DurabilityClass, KeyId, LineageSource, PackageId, ProtectionPhase, QuarantineReason,
    QueriedSource, ReadRequestKind, ReadServiceOutcome, RecoveryMode, SchedulePhase, SyncOutcome,
    Trace, TraceKind, VersionOutcome, VersionSurface,
};

use support::oracle::model::TraceEventKind;
use support::oracle::{Invariant, Oracle, Report, Unavailable, Verdict};
use support::scenarios::builder::{digest_at, evidence, TraceBuilder, CONFIG_V1, GEN_1};
use support::scenarios::mutate::{self, MutationId};

// ------------------------------------------------------------------------------------------
// Row helpers. Every fixture below states only what differs from this shape.
// ------------------------------------------------------------------------------------------

const P0: PartitionId = PartitionId(0);
const P1: PartitionId = PartitionId(1);
const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);
const B1: BootId = BootId(1);
const K1: KeyId = KeyId(1);
const K2: KeyId = KeyId(2);
const CORR1: CorrelationId = CorrelationId(1);
const CORR2: CorrelationId = CorrelationId(2);
const CORR3: CorrelationId = CorrelationId(3);
const TENANT: TenantId = TenantId(1);
const CLIENT: ClientId = ClientId(1);
const REQ1: RequestId = RequestId(1);
const REQ2: RequestId = RequestId(2);

/// The judge. It holds no configuration; everything it knows comes from the trace.
fn judge(trace: &Trace) -> Report {
    Oracle::new().judge(trace)
}

/// An RF3 single-partition builder with every package wired.
fn base(case: &str) -> TraceBuilder {
    TraceBuilder::new().case(case).capabilities(&[])
}

/// `client_submit` for one identity.
fn submit(request: RequestId, keys: &[KeyId]) -> TraceKind {
    TraceKind::ClientSubmit {
        request,
        tenant: TENANT,
        client: CLIENT,
        affinity: 1,
        expected_generation: None,
        request_digest: Digest::ROOT,
        deadline_remaining_ms: 10_000,
        mutation_keys: keys.to_vec(),
        condition_keys: Vec::new(),
    }
}

/// `admission_decision{Admitted}` pinning a required-copy set.
fn admit(seq: Seq, required: &[NodeId], config_version: ConfigVersion) -> TraceKind {
    TraceKind::AdmissionDecision {
        outcome: AdmissionOutcome::Admitted,
        reason: None,
        admitted_seq: Some(seq),
        paused: false,
        oldest_unsafe_age_ms: 0,
        required_copies: required.to_vec(),
        config_version,
    }
}

/// `admission_decision{Rejected}` with a reason.
fn reject(reason: ErrorKind, paused: bool) -> TraceKind {
    TraceKind::AdmissionDecision {
        outcome: AdmissionOutcome::Rejected,
        reason: Some(reason),
        admitted_seq: None,
        paused,
        oldest_unsafe_age_ms: 0,
        required_copies: vec![N1, N2, N3],
        config_version: CONFIG_V1,
    }
}

/// A `protection_state` in one phase.
#[allow(clippy::too_many_arguments)]
fn protection(
    phase: ProtectionPhase,
    age_ms: u64,
    required: &[NodeId],
    config_version: ConfigVersion,
    paused_prefix: Seq,
    barrier: Seq,
    healthy_since: Option<u64>,
) -> TraceKind {
    TraceKind::ProtectionState {
        phase,
        oldest_unsafe_age_ms: age_ms,
        required_copy_set: required.to_vec(),
        config_version,
        paused_prefix_seq: paused_prefix,
        resume_barrier_seq: barrier,
        healthy_since_tick: healthy_since,
    }
}

/// A healthy `protection_state` pinning the RF3 set.
fn healthy() -> TraceKind {
    protection(
        ProtectionPhase::Healthy,
        0,
        &[N1, N2, N3],
        CONFIG_V1,
        Seq::ZERO,
        Seq::ZERO,
        Some(0),
    )
}

/// An `authority_decision` at one gate.
fn authority(
    gate: AuthorityGate,
    generation: Generation,
    window: (u64, u64),
    decision_tick: u64,
    outcome: AuthorityOutcome,
) -> TraceKind {
    TraceKind::AuthorityDecision {
        gate,
        owner_node: N1,
        owner_epoch: OwnerEpoch(1),
        grant: rdb_core::contracts::ids::GrantId(1),
        grant_boot: B1,
        generation,
        valid_from_tick: window.0,
        expiry_tick: window.1,
        decision_tick,
        authority_seq: decision_tick,
        outcome,
    }
}

/// A valid publication-gate decision over `[0, 3000)`.
fn valid_authority() -> TraceKind {
    authority(
        AuthorityGate::Publication,
        GEN_1,
        (0, 3_000),
        0,
        AuthorityOutcome::Valid,
    )
}

/// The lineage root every fixture starts from. Its `base_digest` is the digest the builder's
/// first apply cites, so the near-miss in row M7V-16 is clean by construction.
fn initial_root(generation: Generation) -> TraceKind {
    TraceKind::LineageRoot {
        generation,
        owner_epoch: OwnerEpoch(1),
        base_seq: Seq::ZERO,
        base_digest: digest_at(generation, Seq::ZERO),
        predecessor_generation: None,
        predecessor_cutoff: None,
        source: LineageSource::Initial,
    }
}

/// A recovery root cutting the predecessor off at `cutoff`.
fn recovery_root(generation: Generation, predecessor: Generation, cutoff: Seq) -> TraceKind {
    TraceKind::LineageRoot {
        generation,
        owner_epoch: OwnerEpoch(2),
        base_seq: Seq::ZERO,
        base_digest: digest_at(generation, Seq::ZERO),
        predecessor_generation: Some(predecessor),
        predecessor_cutoff: Some(cutoff),
        source: LineageSource::Recovery,
    }
}

/// A `read` observing a prefix and a set of key versions.
fn read(
    kind: ReadRequestKind,
    generation: Generation,
    observed_seq: Seq,
    observed: &[(KeyId, u64)],
) -> TraceKind {
    TraceKind::Read {
        request_kind: kind,
        barrier: EventId(1),
        generation,
        observed_seq,
        observed_key_versions: observed.to_vec(),
        recovery_mode: false,
        outcome: ReadServiceOutcome::Served,
    }
}

/// A terminal `client_outcome`.
fn outcome(request: RequestId, outcome: ClientOutcome, seq: Option<Seq>) -> TraceKind {
    TraceKind::ClientOutcomeReported {
        request,
        outcome,
        generation: GEN_1,
        seq,
        result_digest: Digest::ROOT,
        delivered: true,
    }
}

/// One queried source, as a recovery records it.
fn source(
    node: NodeId,
    boot: BootId,
    reachable: bool,
    reported: Option<(Generation, Seq)>,
) -> QueriedSource {
    QueriedSource {
        node,
        boot,
        role: ReplicaRole::RegularSecondary,
        reachable,
        reported_generation: reported.map(|(generation, _)| generation),
        reported_seq: reported.map(|(_, seq)| seq),
        reported_digest: reported.map(|(generation, seq)| digest_at(generation, seq)),
    }
}

/// A `recovery_decision` over a set of sources.
fn recovery(
    sources: &[QueriedSource],
    selected: Option<NodeId>,
    cutoff: Seq,
    mode: RecoveryMode,
) -> TraceKind {
    TraceKind::RecoveryDecision {
        fenced_epoch: OwnerEpoch(1),
        discovery_window_ticks: 2_000,
        queried_sources: sources.to_vec(),
        selected_source: selected,
        selected_cutoff_seq: cutoff,
        selected_digest: digest_at(GEN_1, cutoff),
        mode,
        loss_uncertainty: false,
        new_generation: Generation(2),
    }
}

/// `schedule_phase{Healed, fair_delivery=true}`, the only thing that arms INV-LIVE and INV-ISO.
fn healed(budget: u32) -> TraceKind {
    TraceKind::SchedulePhaseChanged {
        phase: SchedulePhase::Healed,
        fair_delivery: true,
        remaining_event_budget: budget,
    }
}

/// A clean `version_check`.
fn version_check(unknown: &[u16], declared_schema: u16, outcome: VersionOutcome) -> TraceKind {
    TraceKind::VersionCheck {
        surface: VersionSurface::Message,
        declared_protocol_version: 1,
        declared_config_version: CONFIG_V1,
        declared_schema_version: declared_schema,
        known_max: 1,
        mandatory_unknown_fields: unknown.to_vec(),
        outcome,
    }
}

/// Assert one invariant came out `Violated` with this rule, and return the signature.
#[track_caller]
fn violated(report: &Report, invariant: Invariant, rule: &str) -> support::oracle::Signature {
    match report.verdict(invariant) {
        Verdict::Violated(signature) => {
            assert_eq!(
                signature.core.rule,
                rule,
                "{} fired {} — detail: {}",
                invariant.id(),
                signature.core.rule,
                signature.detail
            );
            signature.clone()
        }
        other => panic!(
            "{} expected Violated{{{rule}}}, got {other:?}",
            invariant.id()
        ),
    }
}

/// Assert one invariant came out `Proven`.
#[track_caller]
fn proven(report: &Report, invariant: Invariant) {
    assert_eq!(
        report.verdict(invariant),
        &Verdict::Proven,
        "{} expected Proven, got {:?}",
        invariant.id(),
        report.verdict(invariant)
    );
}

/// Assert one invariant came out `Unavailable{NotArmed}` — never a pass.
#[track_caller]
fn not_armed(report: &Report, invariant: Invariant) {
    assert_eq!(
        report.verdict(invariant),
        &Verdict::Unavailable(Unavailable::NotArmed),
        "{} expected Unavailable{{NotArmed}}, got {:?}",
        invariant.id(),
        report.verdict(invariant)
    );
}

// ------------------------------------------------------------------------------------------
// M7V-01 — independence
// ------------------------------------------------------------------------------------------

/// Every `rdb_core` path token under `tests/support/oracle/` must be on the allowlist.
///
/// **Allowlist, not blocklist.** A blocklist passes a module nobody thought to name; an
/// allowlist fails it. Tokens are extracted from `use` lines including brace groups, and from
/// inline paths, and anything the extractor cannot classify is a failure.
///
/// Deviation from the plan, reported in the handoff: the allowlist is
/// `rdb_core::contracts::{trace, ids, digest}` rather than `{trace, ids}`, because `TraceKind`'s
/// own fields are typed with `contracts::digest::Digest` and the judge cannot name a field's type
/// without naming its module. The six kernel modules the charter excludes are additionally
/// blocked by name, so widening the allowlist cannot quietly admit one.
#[retcd_test]
fn m7v_01_oracle_imports_no_kernel_algorithm() {
    support::preamble();
    const ALLOWED: [&str; 3] = [
        "rdb_core::contracts::trace",
        "rdb_core::contracts::ids",
        "rdb_core::contracts::digest",
    ];
    const FORBIDDEN: [&str; 6] = [
        "authority",
        "transaction",
        "replication",
        "publication",
        "protection",
        "recovery",
    ];

    let files = oracle_sources();
    assert!(
        !files.is_empty(),
        "an empty file list must fail this row, not pass it"
    );

    let mut tokens: BTreeSet<String> = BTreeSet::new();
    for (path, text) in &files {
        for token in rdb_core_tokens(text) {
            assert!(
                ALLOWED.iter().any(|allowed| token.starts_with(allowed)),
                "{path} names {token}, which is not on the oracle's allowlist {ALLOWED:?}"
            );
            for module in FORBIDDEN {
                assert!(
                    !token.contains(&format!("rdb_core::{module}")),
                    "{path} names the kernel module {module}: the oracle is an independent \
                     judge, not a second implementation"
                );
            }
            tokens.insert(token);
        }
    }
    assert!(
        tokens
            .iter()
            .any(|t| t.starts_with("rdb_core::contracts::trace")),
        "the oracle must read the trace vocabulary; an empty token set would pass vacuously"
    );
}

/// Every `.rs` file of the judge, as `(path, contents)`.
fn oracle_sources() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support");
    let mut files = vec![read_source(&root.join("oracle.rs"))];
    let mut stack = vec![root.join("oracle")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(read_source(&path));
            }
        }
    }
    files
}

fn read_source(path: &std::path::Path) -> (String, String) {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("{} is unreadable: {error}", path.display()));
    (path.display().to_string(), text)
}

/// Every `rdb_core` path token in `text`, with brace groups expanded to their leaves.
///
/// Whole-line comments are skipped, because this module's own docs **name** the forbidden modules
/// in order to forbid them, and prose imports nothing. A trailing comment after code is still
/// scanned: over-strict in that direction is the safe way to be wrong.
///
/// Fails closed: a `use rdb_core::*` or a token this cannot classify comes back as the literal
/// `rdb_core::*`, which no allowlist entry matches.
fn rdb_core_tokens(text: &str) -> Vec<String> {
    let code: String = text
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut tokens = Vec::new();
    let mut rest = code.as_str();
    while let Some(at) = rest.find("rdb_core::") {
        rest = &rest[at..];
        let end = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':'))
            .unwrap_or(rest.len());
        let (head, tail) = rest.split_at(end);
        // `use rdb_core::{a, b}` has no leaf of its own: pushing the bare prefix here would fail
        // the row on a shape that names nothing.
        if !tail.starts_with('{') {
            tokens.push(head.trim_end_matches(':').to_owned());
        }
        // A brace group expands to one token per leaf, each prefixed by the head.
        if tail.starts_with('{') {
            let close = tail.find('}').unwrap_or(tail.len());
            for leaf in tail[1..close].split(',') {
                let leaf = leaf.trim();
                if !leaf.is_empty() && !leaf.starts_with("self") {
                    tokens.push(format!("{head}{leaf}"));
                }
            }
        }
        rest = tail;
    }
    tokens
}

// ------------------------------------------------------------------------------------------
// M7V-02, M7V-03 — the positive control and the two Unavailable arms
// ------------------------------------------------------------------------------------------

/// The golden trace: RF3 healthy on two partitions, arming **all ten** checkers.
///
/// Every arming condition in the system is pinned here and nowhere else, so a checker whose
/// arming event changes turns this row red rather than quietly disarming in the campaign.
fn golden(unavailable: &[PackageId]) -> Trace {
    let mut b = TraceBuilder::new()
        .case("m7v-02-golden")
        .rf3(P1, CONFIG_V1)
        .partitions(2)
        .capabilities(unavailable);

    // The heal comes first so INV-LIVE and INV-ISO are armed over the whole run.
    b = b.push(healed(500));

    // ---- partition 0: apply, publish, dedup, version, and the lag cycle ------------------
    b = b
        .on(P0)
        .by(N1, B1)
        .about(CORR1)
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(1), &[N1, N2, N3], CONFIG_V1));
    b = b.push(valid_authority());
    let recheck_p0 = b.last_event();
    b = b
        .at(1)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .flush(N2, Seq(1))
        .ack_from(N2, Seq(1), DurabilityClass::Durable)
        .flush(N3, Seq(1))
        .ack_from(N3, Seq(1), DurabilityClass::Durable)
        .publish(
            Seq(1),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck_p0,
        )
        .push(outcome(REQ1, ClientOutcome::Success, Some(Seq(1))));

    // A retained identity, resubmitted and answered from the record: INV-DEDUP arms.
    b = b
        .at(2)
        .push(TraceKind::DedupRecord {
            tenant: TENANT,
            client: CLIENT,
            request: REQ1,
            request_digest: Digest::ROOT,
            result_digest: Digest::ROOT,
            generation: GEN_1,
            retained_until_tick: 100_000,
            action: DedupAction::Store,
        })
        .about(CORR2)
        .push(submit(REQ1, &[K1]))
        .push(TraceKind::DedupRecord {
            tenant: TENANT,
            client: CLIENT,
            request: REQ1,
            request_digest: Digest::ROOT,
            result_digest: Digest::ROOT,
            generation: GEN_1,
            retained_until_tick: 100_000,
            action: DedupAction::Hit,
        })
        .push(outcome(REQ1, ClientOutcome::Success, Some(Seq(1))))
        .push(version_check(&[], 1, VersionOutcome::Accept));

    // ---- partition 1: a recovery with a genuine restricted loss above the cutoff ---------
    b = b
        .at(20)
        .on(P1)
        .by(N1, B1)
        .about(CORR3)
        .push(initial_root(GEN_1))
        .push(submit(REQ2, &[K2]))
        .push(admit(Seq(1), &[N1, N2, N3], CONFIG_V1));
    b = b.push(valid_authority());
    let recheck_p1 = b.last_event();
    b = b
        .at(21)
        .apply(Seq(1), &[(K2, 5)], ApplyOutcome::Applied)
        .flush(N2, Seq(1))
        .ack_from(N2, Seq(1), DurabilityClass::Durable)
        .flush(N3, Seq(1))
        .ack_from(N3, Seq(1), DurabilityClass::Durable)
        .publish(
            Seq(1),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck_p1,
        )
        .push(outcome(REQ2, ClientOutcome::Success, Some(Seq(1))));

    // The survivors are gone: one unreachable, one back at a different boot. The suffix above
    // the declared cutoff may be lost, and INV-LOSS says so by staying clean.
    b = b
        .at(30)
        .push(recovery(
            &[
                source(N2, B1, false, None),
                source(N3, BootId(2), true, Some((GEN_1, Seq::ZERO))),
            ],
            Some(N3),
            Seq::ZERO,
            RecoveryMode::TwoSurvivor,
        ))
        .push(recovery_root(Generation(2), GEN_1, Seq::ZERO))
        .push(read(
            ReadRequestKind::Read,
            Generation(2),
            Seq(1),
            &[(K2, 4)],
        ));

    // ---- the complete legal lag cycle, in M7V-41's exact shape ---------------------------
    b = b
        .at(2_000)
        .on(P0)
        .by(N1, B1)
        .push(protection(
            ProtectionPhase::Paused,
            1_800,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(1),
            Seq(1),
            None,
        ))
        .flush(N1, Seq(1))
        .at(3_000)
        .push(protection(
            ProtectionPhase::Resuming,
            240,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(1),
            Seq(1),
            Some(3_000),
        ))
        .at(8_000)
        .push(protection(
            ProtectionPhase::Healthy,
            0,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(1),
            Seq(1),
            Some(3_000),
        ));

    b.build()
}

#[retcd_test]
fn m7v_02_golden_valid_trace_trips_no_checker() {
    support::preamble();
    let report = judge(&golden(&[]));

    for (invariant, verdict) in report.verdicts() {
        assert_eq!(
            verdict,
            &Verdict::Proven,
            "{} must be Proven on the golden trace, got {verdict:?}",
            invariant.id()
        );
    }
    assert!(report.is_clean());
    assert_eq!(report.violations(), Vec::new());
}

#[retcd_test]
fn m7v_03_unwired_package_reports_capability_never_proven() {
    support::preamble();
    let report = judge(&golden(&[PackageId::P1]));

    // Every checker that reads an event only P1 emits points at P1 by name.
    for invariant in [Invariant::Pub, Invariant::Live, Invariant::Iso] {
        assert_eq!(
            report.verdict(invariant),
            &Verdict::Unavailable(Unavailable::Capability(PackageId::P1)),
            "{} must report the package that did not land",
            invariant.id()
        );
    }
    for invariant in [
        Invariant::Atom,
        Invariant::Auth,
        Invariant::Lin,
        Invariant::Dedup,
        Invariant::Loss,
        Invariant::Ver,
        Invariant::Lag,
    ] {
        proven(&report, invariant);
    }
}

#[retcd_test]
fn m7v_03b_zero_event_control_reports_not_armed_for_all_ten() {
    support::preamble();
    // The header plus exactly ten `capability{Wired}` events and nothing else. Every package is
    // wired, so the `Capability` arm has nothing to point at, and the only honest answer is
    // `NotArmed`.
    let trace = base("m7v-03b-zero-event").build();
    assert_eq!(trace.events.len(), 10);

    let report = judge(&trace);
    for (invariant, verdict) in report.verdicts() {
        assert_eq!(
            verdict,
            &Verdict::Unavailable(Unavailable::NotArmed),
            "{} must report NotArmed, never Proven and never Capability",
            invariant.id()
        );
    }
}

// ------------------------------------------------------------------------------------------
// INV-ATOM — M7V-04, M7V-05
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_04_atom_partial_batch_in_published_prefix_violates() {
    support::preamble();
    let mut b = base("m7v-04").push(healthy()).push(initial_root(GEN_1));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    let trace = b
        .push(submit(REQ1, &[K1, K2]))
        .push(admit(Seq(3), &[N1, N2, N3], CONFIG_V1))
        .at(1)
        .apply(Seq(3), &[(K1, 3), (K2, 3)], ApplyOutcome::Applied)
        .flush(N2, Seq(3))
        .ack_from(N2, Seq(3), DurabilityClass::Durable)
        .flush(N3, Seq(3))
        .ack_from(N3, Seq(3), DurabilityClass::Durable)
        .publish(
            Seq(3),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck,
        )
        // k1 moved, k2 did not: half a transaction is visible.
        .push(read(
            ReadRequestKind::Read,
            GEN_1,
            Seq(3),
            &[(K1, 3), (K2, 2)],
        ))
        .build();

    let signature = violated(&judge(&trace), Invariant::Atom, "partial_batch_visible");
    assert_eq!(signature.core.partition, P0);
    assert_eq!(signature.core.event_kind, TraceEventKind::Read);
    assert_eq!(signature.core.role, ReplicaRole::Primary);
}

#[retcd_test]
fn m7v_05_atom_crashed_before_commit_batch_is_absent_and_clean() {
    support::preamble();
    let trace = base("m7v-05")
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1, K2]))
        .at(1)
        .apply(
            Seq(3),
            &[(K1, 3), (K2, 3)],
            ApplyOutcome::CrashedBeforeCommit,
        )
        .push(read(
            ReadRequestKind::Read,
            GEN_1,
            Seq::ZERO,
            &[(K1, 2), (K2, 2)],
        ))
        .build();

    proven(&judge(&trace), Invariant::Atom);
}

#[retcd_test]
fn m7v_04b_atom_failed_batch_key_version_published_violates() {
    support::preamble();
    let trace = base("m7v-04b")
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .at(1)
        .apply(Seq(3), &[(K1, 3)], ApplyOutcome::Failed)
        .push(read(ReadRequestKind::Read, GEN_1, Seq::ZERO, &[(K1, 3)]))
        .build();

    violated(&judge(&trace), Invariant::Atom, "failed_batch_published");
}

// ------------------------------------------------------------------------------------------
// INV-PUB — M7V-06..M7V-12, M7V-79
// ------------------------------------------------------------------------------------------

/// The shared publication fixture: an admission pinning `required` at `config_version`, an apply
/// at `seq`, and a publication resting on `evidence`.
fn publication_fixture(
    case: &str,
    required: &[NodeId],
    config_version: ConfigVersion,
    seq: Seq,
    ack_nodes: &[(NodeId, DurabilityClass, bool)],
    ack_evidence: &[rdb_core::contracts::trace::AckEvidence],
) -> Trace {
    let mut b = base(case)
        .pinned_to(config_version)
        .push(protection(
            ProtectionPhase::Healthy,
            0,
            required,
            config_version,
            Seq::ZERO,
            Seq::ZERO,
            Some(0),
        ))
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(seq, required, config_version));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    b = b.at(1).apply(seq, &[(K1, 1)], ApplyOutcome::Applied);
    for (node, durability, grounded) in ack_nodes {
        if *grounded {
            b = b.flush(*node, seq);
        }
        b = b.ack_from(*node, seq, *durability);
    }
    b.publish(seq, ack_evidence, recheck).build()
}

#[retcd_test]
fn m7v_06_pub_observation_above_the_published_prefix_violates() {
    support::preamble();
    // `request_kind` is the discriminator: a checker that only handles `Read` fails here.
    for kind in [
        ReadRequestKind::Read,
        ReadRequestKind::Status,
        ReadRequestKind::Export,
        ReadRequestKind::ActorRead,
    ] {
        let mut b = base("m7v-06").push(healthy()).push(initial_root(GEN_1));
        b = b.push(valid_authority());
        let recheck = b.last_event();
        let trace = b
            .push(submit(REQ1, &[K1]))
            .push(admit(Seq(7), &[N1, N2, N3], CONFIG_V1))
            .at(1)
            .apply(Seq(7), &[(K1, 7)], ApplyOutcome::Applied)
            .flush(N2, Seq(7))
            .ack_from(N2, Seq(7), DurabilityClass::Durable)
            .flush(N3, Seq(7))
            .ack_from(N3, Seq(7), DurabilityClass::Durable)
            .publish(
                Seq(7),
                &[
                    evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                    evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                ],
                recheck,
            )
            .at(2)
            .apply(Seq(8), &[(K2, 8)], ApplyOutcome::Applied)
            .push(read(kind, GEN_1, Seq(7), &[(K2, 8)]))
            .build();

        let signature = violated(
            &judge(&trace),
            Invariant::Pub,
            "observation_above_published_prefix",
        );
        assert!(
            signature.detail.contains(&format!("{kind:?}")),
            "the signature must name the surface: {}",
            signature.detail
        );
    }
}

#[retcd_test]
fn m7v_07_pub_publish_without_the_pinned_required_copy_set_violates() {
    support::preamble();
    // n4 is a Shadow in the header, holds the record durably, and is not in the pinned set.
    let trace = base("m7v-07")
        .place(P0, CONFIG_V1, &[(N4, ReplicaRole::Shadow)])
        .push(protection(
            ProtectionPhase::Healthy,
            0,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq::ZERO,
            Seq::ZERO,
            Some(0),
        ))
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(5), &[N1, N2, N3], CONFIG_V1))
        .push(valid_authority());
    let recheck = trace.last_event();
    let trace = trace
        .at(1)
        .apply(Seq(5), &[(K1, 5)], ApplyOutcome::Applied)
        .flush(N4, Seq(5))
        .ack_from(N4, Seq(5), DurabilityClass::Durable)
        .publish(
            Seq(5),
            &[evidence(N4, ReplicaRole::Shadow, DurabilityClass::Durable)],
            recheck,
        )
        .build();

    let signature = violated(
        &judge(&trace),
        Invariant::Pub,
        "required_copy_set_unsatisfied",
    );
    assert!(
        signature.detail.contains("config_version 1"),
        "{}",
        signature.detail
    );
}

#[retcd_test]
fn m7v_07b_pub_required_copy_set_of_an_uncovered_length_violates_at_the_protection_state() {
    support::preamble();
    // Sizes 1 and 4 are the violation; 2 and 3 are the near-misses (M7V-07 and M7V-09).
    for required in [vec![N1], vec![N1, N2, N3, N4]] {
        let length = required.len();
        let trace = base("m7v-07b")
            .place(P0, CONFIG_V1, &[(N4, ReplicaRole::RegularSecondary)])
            .push(protection(
                ProtectionPhase::Healthy,
                0,
                &required,
                CONFIG_V1,
                Seq::ZERO,
                Seq::ZERO,
                Some(0),
            ))
            .build();

        let signature = violated(&judge(&trace), Invariant::Pub, "required_copy_set_shape");
        assert_eq!(
            signature.core.event_kind,
            TraceEventKind::ProtectionState,
            "the shape clause fires before any quorum arithmetic"
        );
        assert!(
            signature.detail.contains(&format!("length {length}")),
            "the signature carries the observed length: {}",
            signature.detail
        );
        assert!(
            signature.detail.contains("config_version 1"),
            "{}",
            signature.detail
        );
    }
}

#[retcd_test]
fn m7v_08_pub_degraded_rf2_one_ack_publish_violates() {
    support::preamble();
    // (a) membership: n3 is a legitimately named regular secondary that is not in the pinned set.
    let trace = publication_fixture(
        "m7v-08a",
        &[N1, N2],
        ConfigVersion(4),
        Seq(9),
        &[(N3, DurabilityClass::Durable, true)],
        &[evidence(
            N3,
            ReplicaRole::RegularSecondary,
            DurabilityClass::Durable,
        )],
    );

    let signature = violated(
        &judge(&trace),
        Invariant::Pub,
        "required_copy_set_unsatisfied",
    );
    assert!(
        signature.detail.contains("regular_acks_counted=0")
            && signature.detail.contains("min_regular_acks=1")
            && signature.detail.contains("DegradedRf2"),
        "the signature reports the derived rule and both counts: {}",
        signature.detail
    );
}

#[retcd_test]
fn m7v_08b_pub_pin_is_the_admissions_config_version_not_the_latest_protection_state() {
    support::preamble();
    // The admission pins [n1,n2] at config_version 4. A later protection_state pins [n1,n3] at 5.
    // The publication's grounded ack from n3 satisfies the NEW set and not the pinned one.
    let mut b = base("m7v-08b")
        .pinned_to(ConfigVersion(4))
        .place(
            P0,
            ConfigVersion(4),
            &[
                (N1, ReplicaRole::Primary),
                (N2, ReplicaRole::RegularSecondary),
                (N3, ReplicaRole::RegularSecondary),
            ],
        )
        .push(protection(
            ProtectionPhase::Healthy,
            0,
            &[N1, N2],
            ConfigVersion(4),
            Seq::ZERO,
            Seq::ZERO,
            Some(0),
        ))
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(9), &[N1, N2], ConfigVersion(4)));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    let trace = b
        .at(1)
        .push(TraceKind::TopologyChange {
            config_version: ConfigVersion(5),
            nodes: vec![
                (N1, ReplicaRole::Primary),
                (N2, ReplicaRole::RegularSecondary),
                (N3, ReplicaRole::RegularSecondary),
            ],
        })
        .push(protection(
            ProtectionPhase::Healthy,
            0,
            &[N1, N3],
            ConfigVersion(5),
            Seq::ZERO,
            Seq::ZERO,
            Some(0),
        ))
        .apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied)
        .flush(N3, Seq(9))
        .ack_from(N3, Seq(9), DurabilityClass::Durable)
        .publish(
            Seq(9),
            &[evidence(
                N3,
                ReplicaRole::RegularSecondary,
                DurabilityClass::Durable,
            )],
            recheck,
        )
        .build();

    let signature = violated(
        &judge(&trace),
        Invariant::Pub,
        "required_copy_set_unsatisfied",
    );
    assert!(
        signature.detail.contains("config_version 4"),
        "the pin is the admission's, not the last protection_state's: {}",
        signature.detail
    );
}

#[retcd_test]
fn m7v_09_pub_degraded_rf2_publish_with_the_pinned_single_regular_ack_is_clean() {
    support::preamble();
    // M7V-08(a)'s trace, differing by exactly one fact: the ack comes from n2, which is pinned.
    let trace = publication_fixture(
        "m7v-09",
        &[N1, N2],
        ConfigVersion(4),
        Seq(9),
        &[(N2, DurabilityClass::Durable, true)],
        &[evidence(
            N2,
            ReplicaRole::RegularSecondary,
            DurabilityClass::Durable,
        )],
    );

    let report = judge(&trace);
    proven(&report, Invariant::Pub);
    assert!(report.is_clean(), "{:?}", report.violations());
}

#[retcd_test]
fn m7v_79_pub_degraded_rf2_publish_on_the_primarys_own_durability_alone_violates() {
    support::preamble();
    // No `replication_ack` from n2 at all: the primary's own durability is the only evidence.
    let mut b = base("m7v-79")
        .pinned_to(ConfigVersion(4))
        .push(protection(
            ProtectionPhase::Healthy,
            0,
            &[N1, N2],
            ConfigVersion(4),
            Seq::ZERO,
            Seq::ZERO,
            Some(0),
        ))
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(9), &[N1, N2], ConfigVersion(4)));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    let trace = b
        .at(1)
        .apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied)
        .flush(N1, Seq(9))
        .publish(
            Seq(9),
            &[evidence(N1, ReplicaRole::Primary, DurabilityClass::Durable)],
            recheck,
        )
        .build();

    let signature = violated(
        &judge(&trace),
        Invariant::Pub,
        "required_copy_set_unsatisfied",
    );
    assert!(
        signature.detail.contains("regular_acks_counted=0")
            && signature.detail.contains("min_regular_acks=1"),
        "1-of-1 means one, not zero: {}",
        signature.detail
    );
}

#[retcd_test]
fn m7v_10_pub_ack_role_claim_mismatching_topology_violates() {
    support::preamble();
    // n4 is a Shadow in the header. The acknowledgement claims RegularSecondary.
    let trace = base("m7v-10")
        .place(P0, CONFIG_V1, &[(N4, ReplicaRole::Shadow)])
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(5), &[N1, N2, N3], CONFIG_V1))
        .at(1)
        .flush(N4, Seq(5))
        .by(N4, B1)
        .push(TraceKind::BatchApply {
            role: ReplicaRole::Shadow,
            generation: GEN_1,
            seq: Seq(5),
            predecessor_seq: Seq(4),
            predecessor_digest: digest_at(GEN_1, Seq(4)),
            entry_digest: digest_at(GEN_1, Seq(5)),
            batch: 5,
            key_versions: Vec::new(),
            outcome: ApplyOutcome::Applied,
        })
        .push(TraceKind::ReplicationAck {
            from_node: N4,
            to_node: N1,
            peer_role: ReplicaRole::RegularSecondary,
            peer_boot: B1,
            config_version: CONFIG_V1,
            generation: GEN_1,
            owner_epoch: OwnerEpoch(1),
            contiguous_seq: Seq(5),
            contiguous_digest: digest_at(GEN_1, Seq(5)),
            durability_class: DurabilityClass::Durable,
            accepted: true,
            reject_reason: None,
        })
        .build();

    let signature = violated(&judge(&trace), Invariant::Pub, "ack_role_claim_mismatch");
    assert_eq!(signature.core.event_kind, TraceEventKind::ReplicationAck);
    assert_eq!(signature.core.role, ReplicaRole::RegularSecondary);
}

#[retcd_test]
fn m7v_10b_pub_the_same_claim_is_clean_after_the_topology_change_that_grants_it() {
    support::preamble();
    // The role is resolved from the topology **in force at that ack**, never from the header.
    let trace = base("m7v-10b")
        .place(P0, CONFIG_V1, &[(N4, ReplicaRole::Shadow)])
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(TraceKind::TopologyChange {
            config_version: ConfigVersion(2),
            nodes: vec![(N4, ReplicaRole::RegularSecondary)],
        })
        .place(P0, ConfigVersion(2), &[(N4, ReplicaRole::RegularSecondary)])
        .pinned_to(ConfigVersion(2))
        .at(1)
        .flush(N4, Seq(5))
        .ack_from(N4, Seq(5), DurabilityClass::Durable)
        .build();

    let report = judge(&trace);
    assert!(report.is_clean(), "{:?}", report.violations());
}

#[retcd_test]
fn m7v_11_pub_durable_ack_without_a_preceding_flush_violates() {
    support::preamble();
    // The apply is present; it is the flush that is missing.
    let trace = base("m7v-11")
        .push(healthy())
        .push(initial_root(GEN_1))
        .at(1)
        .ack_from(N2, Seq(5), DurabilityClass::Durable)
        .build();

    let signature = violated(&judge(&trace), Invariant::Pub, "durable_ack_ungrounded");
    assert_eq!(signature.core.event_kind, TraceEventKind::ReplicationAck);
}

#[retcd_test]
fn m7v_11b_pub_a_short_flush_does_not_ground_a_durable_ack_and_a_long_one_does() {
    support::preamble();
    // Watermarks, not point equality (plan §4 convention 2).
    let short = base("m7v-11b-short")
        .push(healthy())
        .push(initial_root(GEN_1))
        .at(1)
        .flush(N2, Seq(4))
        .ack_from(N2, Seq(5), DurabilityClass::Durable)
        .build();
    violated(&judge(&short), Invariant::Pub, "durable_ack_ungrounded");

    let long = base("m7v-11b-long")
        .push(healthy())
        .push(initial_root(GEN_1))
        .at(1)
        .flush(N2, Seq(7))
        .ack_from(N2, Seq(5), DurabilityClass::Durable)
        .build();
    assert!(judge(&long).is_clean());

    // A failed or partial sync grounds nothing.
    for outcome in [SyncOutcome::Failed, SyncOutcome::Partial] {
        let trace = base("m7v-11b-failed")
            .push(healthy())
            .push(initial_root(GEN_1))
            .at(1)
            .by(N2, B1)
            .push(TraceKind::DurabilityAdvance {
                generation: GEN_1,
                durable_seq: Seq(5),
                durable_digest: digest_at(GEN_1, Seq(5)),
                flush_ticket: 5,
                captured: vec![(P0, Seq(5))],
                outcome,
            })
            .by(N1, B1)
            .ack_from(N2, Seq(5), DurabilityClass::Durable)
            .build();
        violated(&judge(&trace), Invariant::Pub, "durable_ack_ungrounded");
    }
}

#[retcd_test]
fn m7v_12_pub_lost_reply_does_not_retract_the_publish() {
    support::preamble();
    let mut b = base("m7v-12")
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(6), &[N1, N2, N3], CONFIG_V1));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    let trace = b
        .at(1)
        .apply(Seq(6), &[(K1, 6)], ApplyOutcome::Applied)
        .flush(N2, Seq(6))
        .ack_from(N2, Seq(6), DurabilityClass::Durable)
        .flush(N3, Seq(6))
        .ack_from(N3, Seq(6), DurabilityClass::Durable)
        .publish(
            Seq(6),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck,
        )
        .push(TraceKind::ClientOutcomeReported {
            request: REQ1,
            outcome: ClientOutcome::Error(ErrorKind::UnknownOutcome),
            generation: GEN_1,
            seq: None,
            result_digest: Digest::ROOT,
            delivered: false,
        })
        .push(read(ReadRequestKind::Read, GEN_1, Seq(6), &[(K1, 6)]))
        .build();

    proven(&judge(&trace), Invariant::Pub);
}

// ------------------------------------------------------------------------------------------
// INV-AUTH — M7V-13, M7V-14, M7V-15
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_13_auth_overlapping_valid_generations_violate() {
    support::preamble();
    for gate in [AuthorityGate::Admission, AuthorityGate::Publication] {
        let trace = base("m7v-13")
            .at(100)
            .push(authority(
                AuthorityGate::Publication,
                Generation(7),
                (100, 200),
                100,
                AuthorityOutcome::Valid,
            ))
            .at(180)
            .push(authority(
                gate,
                Generation(8),
                (180, 260),
                180,
                AuthorityOutcome::Valid,
            ))
            .build();

        let signature = violated(&judge(&trace), Invariant::Auth, "overlapping_generations");
        assert!(
            signature.detail.contains("generation 8") && signature.detail.contains("generation 7"),
            "the signature names both generations: {}",
            signature.detail
        );
    }
}

#[retcd_test]
fn m7v_14_auth_apply_or_publish_under_an_expired_or_fenced_grant_violates() {
    support::preamble();

    // (a) an apply past the grant's expiry.
    let trace = base("m7v-14a")
        .push(initial_root(Generation(7)))
        .in_generation(Generation(7))
        .at(100)
        .push(authority(
            AuthorityGate::Dispatch,
            Generation(7),
            (100, 200),
            100,
            AuthorityOutcome::Valid,
        ))
        .at(250)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .build();
    violated(&judge(&trace), Invariant::Auth, "apply_after_expiry");

    // (b) and (c): the publication gate's recheck came out Fenced, then Uncertain. Uncertainty
    // denies (spec §7.2) and must not be collapsed into the fenced arm.
    for (outcome, rule) in [
        (AuthorityOutcome::Fenced, "publish_under_fenced_authority"),
        (
            AuthorityOutcome::Uncertain,
            "publish_under_uncertain_authority",
        ),
        (AuthorityOutcome::Expired, "publish_under_expired_authority"),
    ] {
        let b = base("m7v-14bc")
            .push(healthy())
            .push(initial_root(GEN_1))
            .push(submit(REQ1, &[K1]))
            .push(admit(Seq(1), &[N1, N2, N3], CONFIG_V1))
            .at(100)
            .push(authority(
                AuthorityGate::Publication,
                GEN_1,
                (100, 200),
                150,
                outcome,
            ));
        let recheck = b.last_event();
        let trace = b
            .at(150)
            .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
            .flush(N2, Seq(1))
            .ack_from(N2, Seq(1), DurabilityClass::Durable)
            .flush(N3, Seq(1))
            .ack_from(N3, Seq(1), DurabilityClass::Durable)
            .publish(
                Seq(1),
                &[
                    evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                    evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                ],
                recheck,
            )
            .build();
        violated(&judge(&trace), Invariant::Auth, rule);
    }
}

#[retcd_test]
fn m7v_15_auth_adjacent_non_overlapping_generations_are_clean() {
    support::preamble();
    // Half-open windows: one grant's expiry equalling the next's start is a clean handover.
    let trace = base("m7v-15")
        .at(100)
        .push(authority(
            AuthorityGate::Publication,
            Generation(7),
            (100, 200),
            100,
            AuthorityOutcome::Valid,
        ))
        .at(200)
        .push(authority(
            AuthorityGate::Publication,
            Generation(8),
            (200, 300),
            200,
            AuthorityOutcome::Valid,
        ))
        .build();

    proven(&judge(&trace), Invariant::Auth);
}

// ------------------------------------------------------------------------------------------
// INV-LIN — M7V-16..M7V-19
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_16_lin_predecessor_digest_mismatch_violates() {
    support::preamble();
    let trace = base("m7v-16")
        .push(initial_root(GEN_1))
        .at(1)
        .apply(Seq(4), &[(K1, 4)], ApplyOutcome::Applied)
        .push(TraceKind::BatchApply {
            role: ReplicaRole::Primary,
            generation: GEN_1,
            seq: Seq(5),
            predecessor_seq: Seq(4),
            // The digest recorded at seq 4 is `digest_at(GEN_1, Seq(4))`; this is not it.
            predecessor_digest: digest_at(GEN_1, Seq(3)),
            entry_digest: digest_at(GEN_1, Seq(5)),
            batch: 5,
            key_versions: vec![(K2, 5)],
            outcome: ApplyOutcome::Applied,
        })
        .build();

    violated(
        &judge(&trace),
        Invariant::Lin,
        "predecessor_digest_mismatch",
    );
}

#[retcd_test]
fn m7v_16b_lin_the_first_apply_citing_the_roots_base_digest_is_clean() {
    support::preamble();
    let trace = base("m7v-16b")
        .push(initial_root(GEN_1))
        .at(1)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .build();

    proven(&judge(&trace), Invariant::Lin);
}

#[retcd_test]
fn m7v_17_lin_two_entry_digests_at_one_generation_seq_demand_quarantine() {
    support::preamble();
    let conflicting = |case: &str| {
        base(case)
            .in_generation(Generation(7))
            .push(initial_root(Generation(7)))
            .at(1)
            .apply(Seq(5), &[(K1, 5)], ApplyOutcome::Applied)
            .by(N2, B1)
            .push(TraceKind::BatchApply {
                role: ReplicaRole::RegularSecondary,
                generation: Generation(7),
                seq: Seq(5),
                predecessor_seq: Seq(4),
                predecessor_digest: digest_at(Generation(7), Seq(4)),
                entry_digest: support::scenarios::builder::forked_digest_at(Generation(7), Seq(5)),
                batch: 5,
                key_versions: vec![(K1, 5)],
                outcome: ApplyOutcome::Applied,
            })
            .by(N1, B1)
    };

    let trace = conflicting("m7v-17").build();
    violated(
        &judge(&trace),
        Invariant::Lin,
        "digest_conflict_without_quarantine",
    );

    // Near-miss: both the quarantine and the recovery decision, because divergence never
    // auto-merges.
    let quarantined = conflicting("m7v-17b")
        .at(2)
        .push(TraceKind::Quarantine {
            reason: QuarantineReason::DigestConflict,
            generation: Generation(7),
            seq: Seq(5),
            sources: vec![N1, N2],
        })
        .push(recovery(
            &[source(N2, B1, true, None)],
            None,
            Seq::ZERO,
            RecoveryMode::Quarantine,
        ))
        .build();
    proven(&judge(&quarantined), Invariant::Lin);
}

#[retcd_test]
fn m7v_18_lin_cutoff_above_a_recorded_matching_prefix_violates() {
    support::preamble();
    let trace = base("m7v-18")
        .in_generation(Generation(7))
        .push(initial_root(Generation(7)))
        .at(1)
        .apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied)
        .at(2)
        .push(recovery(
            &[source(N2, B1, true, Some((Generation(7), Seq(9))))],
            Some(N2),
            Seq(6),
            RecoveryMode::TwoSurvivor,
        ))
        .build();

    violated(
        &judge(&trace),
        Invariant::Lin,
        "cutoff_below_an_available_recorded_prefix",
    );
}

#[retcd_test]
fn m7v_19_lin_cutoff_is_clean_when_the_longer_source_is_unreachable_or_mismatched() {
    support::preamble();

    // (a) the longer source did not answer.
    let unreachable = base("m7v-19a")
        .in_generation(Generation(7))
        .push(initial_root(Generation(7)))
        .at(1)
        .apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied)
        .at(2)
        .push(recovery(
            &[source(N2, B1, false, Some((Generation(7), Seq(9))))],
            None,
            Seq(6),
            RecoveryMode::TwoSurvivor,
        ))
        .build();
    proven(&judge(&unreachable), Invariant::Lin);

    // (b) it answered, and its digest is not the one recorded. The oracle looks up what it
    // already recorded; it never derives the pairwise-compatibility relation. A correct kernel
    // answers a recovery-path disagreement with `recovery_decision{mode=Quarantine}`, and that
    // is F1's decision, not the oracle's.
    let mut sources = vec![source(N2, B1, true, Some((Generation(7), Seq(9))))];
    sources[0].reported_digest = Some(support::scenarios::builder::forked_digest_at(
        Generation(7),
        Seq(9),
    ));
    let mismatched = base("m7v-19b")
        .in_generation(Generation(7))
        .push(initial_root(Generation(7)))
        .at(1)
        .apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied)
        .at(2)
        .push(recovery(&sources, None, Seq(6), RecoveryMode::TwoSurvivor))
        .build();
    let report = judge(&mismatched);
    proven(&report, Invariant::Lin);
    assert!(report.is_clean(), "{:?}", report.violations());
}

// ------------------------------------------------------------------------------------------
// INV-LIN, the two clauses beyond the plan
//
// `recovery_root_without_predecessor` and `cutoff_above_selected_source` are checker code the
// plan's M7V-16..M7V-19 never asked for. Review F2 found both unexercised. They are kept rather
// than deleted — each states a real §8.1 obligation, and deleting a correct clause to close a
// coverage finding trades a weaker oracle for a tidier table — so each gets the trip and the
// near-miss every other clause has. No row id, because the plan does not own them.
// ------------------------------------------------------------------------------------------

/// A lineage root that names itself a recovery, with whatever predecessor fields are passed.
fn orphan_recovery_root(
    predecessor_generation: Option<Generation>,
    predecessor_cutoff: Option<Seq>,
) -> TraceKind {
    TraceKind::LineageRoot {
        generation: Generation(2),
        owner_epoch: OwnerEpoch(2),
        base_seq: Seq::ZERO,
        base_digest: digest_at(Generation(2), Seq::ZERO),
        predecessor_generation,
        predecessor_cutoff,
        source: LineageSource::Recovery,
    }
}

#[retcd_test]
fn lin_a_recovery_root_that_cites_no_predecessor_violates() {
    support::preamble();

    // `source=Recovery` *is* the claim that a predecessor was cut off. A root that makes the
    // claim without the fields leaves a cutoff nothing can check — including
    // `cutoff_below_an_available_recorded_prefix`, which is why this clause exists.
    for missing in [
        orphan_recovery_root(None, None),
        // The `||` disjunct: half a citation is still not one.
        orphan_recovery_root(Some(GEN_1), None),
        orphan_recovery_root(None, Some(Seq(6))),
    ] {
        let trace = base("lin-recovery-root-orphan")
            .in_generation(Generation(2))
            .push(missing)
            .build();
        violated(
            &judge(&trace),
            Invariant::Lin,
            "recovery_root_without_predecessor",
        );
    }

    // Near-miss (a): the same root, both fields present.
    let cited = base("lin-recovery-root-cited")
        .in_generation(Generation(2))
        .push(recovery_root(Generation(2), GEN_1, Seq(6)))
        .build();
    proven(&judge(&cited), Invariant::Lin);

    // Near-miss (b): an *initial* root legitimately has neither field. The clause is scoped to
    // `LineageSource::Recovery`; a version that dropped the scope would fire on every fixture in
    // this file, and this is the row that says so.
    let initial = base("lin-initial-root").push(initial_root(GEN_1)).build();
    proven(&judge(&initial), Invariant::Lin);
}

#[retcd_test]
fn lin_a_cutoff_above_the_selected_sources_prefix_violates() {
    support::preamble();

    let decision = |case: &str, reported: Option<Seq>| {
        base(case)
            .in_generation(Generation(7))
            .push(initial_root(Generation(7)))
            .at(1)
            .push(recovery(
                &[source(
                    N2,
                    B1,
                    true,
                    reported.map(|seq| (Generation(7), seq)),
                )],
                Some(N2),
                Seq(6),
                RecoveryMode::TwoSurvivor,
            ))
            .build()
    };

    // Trip: the selected source reported seq 3 and the decision cut at seq 6. The cutoff names a
    // prefix the source it was selected from does not have.
    violated(
        &judge(&decision("lin-cutoff-above", Some(Seq(3)))),
        Invariant::Lin,
        "cutoff_above_selected_source",
    );

    // Near-miss, the boundary: `reported_seq == selected_cutoff_seq` is legal — the cutoff is the
    // source's whole prefix, not above it. A `<=` in place of the `<` fails here.
    proven(
        &judge(&decision("lin-cutoff-boundary", Some(Seq(6)))),
        Invariant::Lin,
    );

    // Near-miss: a source that reported no prefix is neither above nor below the cutoff.
    proven(&judge(&decision("lin-cutoff-silent", None)), Invariant::Lin);
}

#[retcd_test]
fn lin_the_two_cutoff_clauses_do_not_shadow_each_other() {
    support::preamble();

    // Both clauses live in the `RecoveryDecision` arm and `cutoff_above_selected_source` returns
    // before the loop that carries M7V-18's clause. `Oracle::judge` keeps the **first** violation
    // per invariant, so which one a seed's signature names is a fact about ordering, not taste.
    // Pinned here so a reorder in `lineage.rs` is a red row rather than a changed signature.
    let contested = |case: &str, selected_reported: Seq| {
        base(case)
            .in_generation(Generation(7))
            .push(initial_root(Generation(7)))
            .at(1)
            .apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied)
            .at(2)
            .push(recovery(
                &[
                    source(N2, B1, true, Some((Generation(7), selected_reported))),
                    source(N3, B1, true, Some((Generation(7), Seq(9)))),
                ],
                Some(N2),
                Seq(6),
                RecoveryMode::TwoSurvivor,
            ))
            .build()
    };

    // Both conditions hold: N2 is below the cutoff, N3 is above it with the recorded digest.
    // The first clause wins.
    violated(
        &judge(&contested("lin-cutoff-both", Seq(3))),
        Invariant::Lin,
        "cutoff_above_selected_source",
    );

    // The converse, and the one M7V-18 depends on: with the selected source *not* below the
    // cutoff, the first clause stands aside and the loop reaches N3. If it did not, M7V-18 would
    // be passing against the wrong clause.
    violated(
        &judge(&contested("lin-cutoff-only-below", Seq(6))),
        Invariant::Lin,
        "cutoff_below_an_available_recorded_prefix",
    );
}

// ------------------------------------------------------------------------------------------
// INV-DEDUP — M7V-24, M7V-25, M7V-26
// ------------------------------------------------------------------------------------------

/// A trace that has already armed INV-DEDUP: one identity, stored and resubmitted.
fn dedup_armed(case: &str) -> TraceBuilder {
    base(case)
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(TraceKind::DedupRecord {
            tenant: TENANT,
            client: CLIENT,
            request: REQ1,
            request_digest: Digest::ROOT,
            result_digest: Digest::ROOT,
            generation: GEN_1,
            retained_until_tick: 100_000,
            action: DedupAction::Store,
        })
        .about(CORR2)
        .push(submit(REQ1, &[K1]))
}

#[retcd_test]
fn m7v_24_dedup_two_applies_for_one_identity_violate() {
    support::preamble();
    let trace = dedup_armed("m7v-24")
        .about(CORR1)
        .at(1)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .at(2)
        .apply(Seq(2), &[(K1, 2)], ApplyOutcome::Applied)
        .build();

    let signature = violated(&judge(&trace), Invariant::Dedup, "duplicate_effect");
    assert_eq!(signature.core.event_kind, TraceEventKind::BatchApply);
}

#[retcd_test]
fn m7v_24b_dedup_a_hit_with_no_second_apply_is_clean() {
    support::preamble();
    let trace = dedup_armed("m7v-24b")
        .push(TraceKind::DedupRecord {
            tenant: TENANT,
            client: CLIENT,
            request: REQ1,
            request_digest: Digest::ROOT,
            result_digest: Digest::ROOT,
            generation: GEN_1,
            retained_until_tick: 100_000,
            action: DedupAction::Hit,
        })
        .about(CORR1)
        .at(1)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .build();

    proven(&judge(&trace), Invariant::Dedup);
}

#[retcd_test]
fn m7v_25_dedup_the_three_pre_mutation_rejections_are_clean_and_distinct() {
    support::preamble();
    for reason in [
        ErrorKind::RequestIdReuse,
        ErrorKind::CrossAffinity,
        ErrorKind::GenerationChanged,
    ] {
        let trace = dedup_armed("m7v-25")
            .push(reject(reason, false))
            .push(outcome(REQ1, ClientOutcome::Error(reason), None))
            .build();

        let report = judge(&trace);
        proven(&report, Invariant::Dedup);

        // The half that makes these rejections rather than errors after the fact: no
        // `batch_apply` carries the correlation.
        assert!(
            !trace
                .events
                .iter()
                .any(|event| matches!(event.kind, TraceKind::BatchApply { .. })),
            "{reason:?} must be rejected before any mutation"
        );
    }
}

#[retcd_test]
fn m7v_26_dedup_absence_is_never_reported_as_proof_of_nonexecution() {
    support::preamble();
    let trace = dedup_armed("m7v-26")
        .push(read(ReadRequestKind::Status, GEN_1, Seq::ZERO, &[]))
        .push(outcome(REQ2, ClientOutcome::Success, None))
        .build();

    violated(
        &judge(&trace),
        Invariant::Dedup,
        "absence_reported_as_nonexecution",
    );
}

#[retcd_test]
fn m7v_26b_dedup_honest_answers_to_an_absent_record_are_clean() {
    support::preamble();
    for answer in [
        ClientOutcome::Error(ErrorKind::UnknownOutcome),
        ClientOutcome::Error(ErrorKind::StatusExpired),
        ClientOutcome::RecoveredApplied,
    ] {
        let trace = dedup_armed("m7v-26b")
            .push(read(ReadRequestKind::Status, GEN_1, Seq::ZERO, &[]))
            .push(outcome(REQ2, answer, None))
            .build();
        proven(&judge(&trace), Invariant::Dedup);
    }
}

// ------------------------------------------------------------------------------------------
// INV-LOSS — M7V-27, M7V-28, M7V-29
// ------------------------------------------------------------------------------------------

/// A published key at `seq=9`, held by `holders`, that then disappears from a later read.
fn loss_fixture(
    case: &str,
    holders: &[(NodeId, DurabilityClass, bool)],
    recovery_sources: Option<&[QueriedSource]>,
    cutoff: Option<Seq>,
) -> Trace {
    let mut b = base(case)
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(9), &[N1, N2, N3], CONFIG_V1));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    b = b.at(1).apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied);
    let mut ack_evidence = Vec::new();
    for (node, durability, grounded) in holders {
        if *grounded {
            b = b.flush(*node, Seq(9));
        }
        b = b.ack_from(*node, Seq(9), *durability);
        ack_evidence.push(evidence(*node, ReplicaRole::RegularSecondary, *durability));
    }
    b = b.publish(Seq(9), &ack_evidence, recheck);

    if let Some(sources) = recovery_sources {
        b = b.at(2).push(recovery(
            sources,
            None,
            cutoff.unwrap_or(Seq::ZERO),
            RecoveryMode::TwoSurvivor,
        ));
    }
    if let Some(cutoff) = cutoff {
        b = b.push(recovery_root(Generation(2), GEN_1, cutoff));
    }
    b.at(3)
        .push(read(
            ReadRequestKind::Read,
            Generation(2),
            Seq(9),
            &[(K1, 8)],
        ))
        .build()
}

#[retcd_test]
fn m7v_27_loss_under_an_unchanged_generation_violates() {
    support::preamble();
    // A published key regresses with no recovery in between and the generation never changed.
    // Recovery is the only thing that may narrow a published prefix, so its absence is the whole
    // violation: there is no cutoff to argue about.
    let mut b = base("m7v-27")
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(9), &[N1, N2, N3], CONFIG_V1));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    let trace = b
        .at(1)
        .apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied)
        .flush(N2, Seq(9))
        .ack_from(N2, Seq(9), DurabilityClass::Durable)
        .flush(N3, Seq(9))
        .ack_from(N3, Seq(9), DurabilityClass::Durable)
        .publish(
            Seq(9),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck,
        )
        .at(3)
        .push(read(ReadRequestKind::Read, GEN_1, Seq(9), &[(K1, 8)]))
        .build();

    let signature = violated(
        &judge(&trace),
        Invariant::Loss,
        "loss_without_recovery_root",
    );
    assert_eq!(signature.core.event_kind, TraceEventKind::Read);
}

#[retcd_test]
fn m7v_28_loss_with_a_reachable_durable_holder_at_its_boot_violates() {
    support::preamble();
    let trace = loss_fixture(
        "m7v-28",
        &[(N2, DurabilityClass::Durable, true)],
        Some(&[source(N2, B1, true, Some((GEN_1, Seq(9))))]),
        Some(Seq(6)),
    );

    let signature = violated(
        &judge(&trace),
        Invariant::Loss,
        "loss_with_a_surviving_durable_holder",
    );
    assert!(signature.detail.contains("boot 1"), "{}", signature.detail);
}

#[retcd_test]
fn m7v_28b_loss_a_higher_watermark_still_makes_the_node_a_holder_at_nine() {
    support::preamble();
    // No acknowledgement literally "at 9": `contiguous_seq = 11` covers it (convention 2).
    let mut b = base("m7v-28b")
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(9), &[N1, N2, N3], CONFIG_V1));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    let trace = b
        .at(1)
        .apply(Seq(9), &[(K1, 9)], ApplyOutcome::Applied)
        .flush(N2, Seq(11))
        .ack_from(N2, Seq(11), DurabilityClass::Durable)
        .flush(N3, Seq(11))
        .ack_from(N3, Seq(11), DurabilityClass::Durable)
        .publish(
            Seq(9),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck,
        )
        .at(2)
        .push(recovery(
            &[source(N2, B1, true, Some((GEN_1, Seq(9))))],
            None,
            Seq(6),
            RecoveryMode::TwoSurvivor,
        ))
        .push(recovery_root(Generation(2), GEN_1, Seq(6)))
        .at(3)
        .push(read(
            ReadRequestKind::Read,
            Generation(2),
            Seq(9),
            &[(K1, 8)],
        ))
        .build();

    violated(
        &judge(&trace),
        Invariant::Loss,
        "loss_with_a_surviving_durable_holder",
    );
}

#[retcd_test]
fn m7v_29_loss_with_buffered_only_holders_returning_at_a_new_boot_is_clean() {
    support::preamble();
    // The holder is buffered-only and came back at a **different** boot after a host crash, so
    // its buffer proves nothing.
    let clean = loss_fixture(
        "m7v-29",
        &[(N2, DurabilityClass::Buffered, false)],
        Some(&[source(N2, BootId(2), true, Some((GEN_1, Seq(9))))]),
        Some(Seq(6)),
    );
    let report = judge(&clean);
    proven(&report, Invariant::Loss);

    // The sub-case that must still violate: reachable at the **same** boot, so nothing could
    // have discarded the buffer.
    let same_boot = loss_fixture(
        "m7v-29b",
        &[(N2, DurabilityClass::Buffered, false)],
        Some(&[source(N2, B1, true, Some((GEN_1, Seq(9))))]),
        Some(Seq(6)),
    );
    violated(
        &judge(&same_boot),
        Invariant::Loss,
        "loss_with_a_surviving_buffered_holder",
    );
}

#[retcd_test]
fn m7v_29c_loss_at_or_below_the_declared_cutoff_violates() {
    support::preamble();
    // The cutoff is the promise about what survives: loss at or below it is never permitted.
    let trace = loss_fixture(
        "m7v-29c",
        &[(N2, DurabilityClass::Buffered, false)],
        Some(&[source(N2, BootId(2), true, None)]),
        Some(Seq(9)),
    );
    violated(
        &judge(&trace),
        Invariant::Loss,
        "loss_below_declared_cutoff",
    );
}

// ------------------------------------------------------------------------------------------
// INV-LIVE and INV-ISO — M7V-30..M7V-33
// ------------------------------------------------------------------------------------------

/// A run with one admitted request that never reaches a terminal outcome.
fn stuck(
    case: &str,
    heal: Option<u32>,
    partitions: &[PartitionId],
    terminal: &[PartitionId],
) -> Trace {
    let mut b = TraceBuilder::new()
        .case(case)
        .rf3(P1, CONFIG_V1)
        .partitions(2)
        .capabilities(&[]);
    if let Some(budget) = heal {
        b = b.push(healed(budget));
    }
    b = b.push(valid_authority());
    for partition in partitions {
        b = b
            .on(*partition)
            .about(CorrelationId(u64::from(partition.0) + 1))
            .push(submit(REQ1, &[K1]))
            .push(admit(Seq(1), &[N1, N2, N3], CONFIG_V1));
    }
    for partition in terminal {
        b = b
            .on(*partition)
            .about(CorrelationId(u64::from(partition.0) + 1))
            .push(outcome(REQ1, ClientOutcome::Success, Some(Seq(1))));
    }
    b.on(P0)
        .at(2_000)
        .push(protection(
            ProtectionPhase::Paused,
            2_500,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(1),
            Seq(1),
            None,
        ))
        .build()
}

#[retcd_test]
fn m7v_30_live_healed_schedule_with_a_stuck_request_violates() {
    support::preamble();
    let trace = stuck("m7v-30", Some(200), &[P0], &[]);

    violated(
        &judge(&trace),
        Invariant::Live,
        "no_terminal_outcome_under_healed_schedule",
    );

    // The second sub-assertion: protection never left `Paused`.
    let last_phase = trace
        .events
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            TraceKind::ProtectionState { phase, .. } => Some(*phase),
            _ => None,
        });
    assert_eq!(last_phase, Some(ProtectionPhase::Paused));
}

#[retcd_test]
fn m7v_31_live_unhealed_or_exhausted_budget_disarms_the_checker() {
    support::preamble();

    // (a) never healed.
    let unhealed = stuck("m7v-31a", None, &[P0], &[]);
    not_armed(&judge(&unhealed), Invariant::Live);

    // (b) healed, then the event budget ran out first. The checker armed and then disarmed, and
    // the end-of-fold state is what `seeds_armed` reads.
    let exhausted = stuck("m7v-31b", Some(2), &[P0], &[]);
    not_armed(&judge(&exhausted), Invariant::Live);
}

#[retcd_test]
fn m7v_32_iso_partition_b_stalls_while_only_partition_a_is_blocked_violates() {
    support::preamble();
    // Both partitions have admitted work; only A is blocked, and B produced nothing.
    let trace = stuck("m7v-32", Some(500), &[P0, P1], &[]);

    violated(&judge(&trace), Invariant::Iso, "sibling_partition_starved");
}

#[retcd_test]
fn m7v_33_iso_disarms_when_the_sibling_partition_has_no_admitted_work() {
    support::preamble();
    // The same trace with no admission for partition B: there is nothing to starve.
    let trace = stuck("m7v-33", Some(500), &[P0], &[]);
    not_armed(&judge(&trace), Invariant::Iso);
}

// ------------------------------------------------------------------------------------------
// INV-VER — M7V-34, M7V-35
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_34_ver_unknown_mandatory_field_applied_violates() {
    support::preamble();

    // Half one: the wrong outcome.
    let accepted = base("m7v-34a")
        .push(initial_root(GEN_1))
        .push(version_check(&[17], 1, VersionOutcome::Accept))
        .at(1)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .build();
    let signature = violated(
        &judge(&accepted),
        Invariant::Ver,
        "unknown_mandatory_field_applied",
    );
    assert_eq!(signature.core.event_kind, TraceEventKind::VersionCheck);
    assert!(signature.detail.contains("[17]"), "{}", signature.detail);

    // Half two: the refusal was recorded, and the batch was applied anyway. A refusal after the
    // storage batch is not a refusal.
    let applied_anyway = base("m7v-34b")
        .push(initial_root(GEN_1))
        .push(version_check(&[17], 1, VersionOutcome::RefuseBeforeApply))
        .at(1)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .build();
    let signature = violated(
        &judge(&applied_anyway),
        Invariant::Ver,
        "unknown_mandatory_field_applied",
    );
    assert_eq!(signature.core.event_kind, TraceEventKind::BatchApply);
}

#[retcd_test]
fn m7v_35_ver_additive_unknown_optional_fields_are_accepted_and_clean() {
    support::preamble();
    // A newer peer carrying no unknown **mandatory** field is the required behaviour. A checker
    // that refuses anything newer fails the other half of V12.
    let trace = base("m7v-35")
        .push(initial_root(GEN_1))
        .push(version_check(&[], 9, VersionOutcome::Accept))
        .at(1)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .build();

    proven(&judge(&trace), Invariant::Ver);
}

// ------------------------------------------------------------------------------------------
// INV-LAG — M7V-36..M7V-41
// ------------------------------------------------------------------------------------------

/// A pause-to-healthy cycle, with the durability each pinned node reached.
fn lag_cycle(case: &str, durable: &[(NodeId, Seq)], resuming_age: u64, healthy_tick: u64) -> Trace {
    let mut b = base(case).at(2_000).push(protection(
        ProtectionPhase::Paused,
        1_800,
        &[N1, N2, N3],
        CONFIG_V1,
        Seq(40),
        Seq(40),
        None,
    ));
    for (node, seq) in durable {
        b = b.flush(*node, *seq);
    }
    b.at(3_000)
        .push(protection(
            ProtectionPhase::Resuming,
            resuming_age,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(40),
            Seq(40),
            Some(3_000),
        ))
        .at(healthy_tick)
        .push(protection(
            ProtectionPhase::Healthy,
            0,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(40),
            Seq(40),
            Some(3_000),
        ))
        .build()
}

#[retcd_test]
fn m7v_36_lag_resume_before_every_pinned_copy_reaches_the_barrier_violates() {
    support::preamble();
    // Only n2 reached the barrier. A checker written with a singular `durability_advance` passes
    // this and is the weaker of the two implementations.
    let trace = lag_cycle("m7v-36", &[(N2, Seq(40))], 240, 8_000);

    let signature = violated(
        &judge(&trace),
        Invariant::Lag,
        "resume_without_every_pinned_copy",
    );
    assert!(
        signature.detail.contains('1') && signature.detail.contains('3'),
        "the signature records which pinned nodes were short: {}",
        signature.detail
    );
}

#[retcd_test]
fn m7v_37_lag_resume_with_lag_above_250ms_inside_the_hold_violates() {
    support::preamble();

    // (a) every pinned copy at the barrier, but lag reached 400 ms inside the 5 s hold.
    let broken_hold = lag_cycle(
        "m7v-37a",
        &[(N1, Seq(40)), (N2, Seq(40)), (N3, Seq(40))],
        400,
        8_000,
    );
    violated(&judge(&broken_hold), Invariant::Lag, "resume_hold_broken");

    // (b) a flush that overshoots the declared barrier: the barrier never gated the resume.
    let overshoot = lag_cycle(
        "m7v-37b",
        &[(N1, Seq(40)), (N2, Seq(40)), (N3, Seq(41))],
        240,
        8_000,
    );
    violated(
        &judge(&overshoot),
        Invariant::Lag,
        "resume_barrier_not_exact",
    );
}

#[retcd_test]
fn m7v_38_lag_unsafe_age_reset_by_a_config_version_change_violates() {
    support::preamble();
    // A membership edit renames the required-copy set and the timer falls to zero, with the
    // paused prefix still not durable anywhere.
    let trace = base("m7v-38")
        .at(2_000)
        .push(protection(
            ProtectionPhase::Paused,
            1_800,
            &[N1, N2, N3],
            ConfigVersion(3),
            Seq(40),
            Seq(40),
            None,
        ))
        .at(2_100)
        .push(protection(
            ProtectionPhase::Paused,
            0,
            &[N1, N2, N3],
            ConfigVersion(4),
            Seq(40),
            Seq(40),
            None,
        ))
        .build();

    let signature = violated(
        &judge(&trace),
        Invariant::Lag,
        "unsafe_age_reset_across_config_version",
    );
    assert!(signature.detail.contains("1800"), "{}", signature.detail);

    // Near-miss: the same drop **with** a retirement barrier — the paused prefix really did
    // become durable on every pinned copy.
    let retired = base("m7v-38b")
        .at(2_000)
        .push(protection(
            ProtectionPhase::Paused,
            1_800,
            &[N1, N2, N3],
            ConfigVersion(3),
            Seq(40),
            Seq(40),
            None,
        ))
        .flush(N1, Seq(40))
        .flush(N2, Seq(40))
        .flush(N3, Seq(40))
        .at(2_100)
        .push(protection(
            ProtectionPhase::Paused,
            0,
            &[N1, N2, N3],
            ConfigVersion(4),
            Seq(40),
            Seq(40),
            None,
        ))
        .build();
    proven(&judge(&retired), Invariant::Lag);
}

#[retcd_test]
fn m7v_39_lag_admission_admitted_while_paused_violates() {
    support::preamble();
    let trace = base("m7v-39")
        .at(2_000)
        .push(protection(
            ProtectionPhase::Paused,
            2_500,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(40),
            Seq(40),
            None,
        ))
        .at(2_100)
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(41), &[N1, N2, N3], CONFIG_V1))
        .build();

    violated(&judge(&trace), Invariant::Lag, "admitted_while_paused");

    // Near-miss: a rejection carrying `ProtectionPaused` in the same window is exactly right.
    let rejected = base("m7v-39b")
        .at(2_000)
        .push(protection(
            ProtectionPhase::Paused,
            2_500,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(40),
            Seq(40),
            None,
        ))
        .at(2_100)
        .push(submit(REQ1, &[K1]))
        .push(reject(ErrorKind::ProtectionPaused, true))
        .build();
    proven(&judge(&rejected), Invariant::Lag);
}

#[retcd_test]
fn m7v_40_lag_publish_of_an_already_admitted_transaction_while_paused_is_clean() {
    support::preamble();
    // **This row exists to fail if anyone re-adds "no publish while paused".** An admitted,
    // applied transaction must be resolved by ACK or recovery, not abandoned (spec §5.3), and
    // publication is P1's independent decision.
    let mut b = base("m7v-40")
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(1), &[N1, N2, N3], CONFIG_V1));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    let trace = b
        .at(1)
        .apply(Seq(1), &[(K1, 1)], ApplyOutcome::Applied)
        .flush(N2, Seq(1))
        .ack_from(N2, Seq(1), DurabilityClass::Durable)
        .flush(N3, Seq(1))
        .ack_from(N3, Seq(1), DurabilityClass::Durable)
        .at(2_000)
        .push(protection(
            ProtectionPhase::Paused,
            2_500,
            &[N1, N2, N3],
            CONFIG_V1,
            Seq(1),
            Seq(1),
            None,
        ))
        .at(2_500)
        .publish(
            Seq(1),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck,
        )
        .build();

    let report = judge(&trace);
    proven(&report, Invariant::Lag);
    assert!(report.is_clean(), "{:?}", report.violations());
}

#[retcd_test]
fn m7v_41_lag_complete_exact_resume_is_clean() {
    support::preamble();
    // Every pinned copy at the barrier exactly, lag under 250 ms for the whole hold.
    let trace = lag_cycle(
        "m7v-41",
        &[(N1, Seq(40)), (N2, Seq(40)), (N3, Seq(40))],
        240,
        8_000,
    );

    let report = judge(&trace);
    proven(&report, Invariant::Lag);
    assert!(report.is_clean(), "{:?}", report.violations());
}

// ------------------------------------------------------------------------------------------
// Mutations — M7V-66..M7V-68, M7V-71, M7V-81
// ------------------------------------------------------------------------------------------

/// A clean run whose publication gate additionally recorded an `Expired` decision that nothing
/// rests on. Flipping that decision to `Valid` is MUT-1, and it fires because the decision's own
/// tick is outside the window it claims.
fn mutation_base() -> Trace {
    let mut b = base("mutation-base")
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(3), &[N1, N2, N3], CONFIG_V1))
        .at(100)
        .push(authority(
            AuthorityGate::Publication,
            GEN_1,
            (100, 3_000),
            100,
            AuthorityOutcome::Valid,
        ));
    let recheck = b.last_event();
    // Three applies, so MUT-4 has a `seq - 2` that was actually recorded to skip back to. The
    // first two are earlier traffic under their own correlation and carry no client identity:
    // three applies under one request would be a duplicate effect, which is a different bug.
    b = b
        .at(101)
        .about(CORR2)
        .apply(Seq(1), &[(K2, 1)], ApplyOutcome::Applied)
        .apply(Seq(2), &[(K1, 2)], ApplyOutcome::Applied)
        .about(CORR1)
        .apply(Seq(3), &[(K2, 3)], ApplyOutcome::Applied)
        .flush(N2, Seq(3))
        .ack_from(N2, Seq(3), DurabilityClass::Durable)
        .flush(N3, Seq(3))
        .ack_from(N3, Seq(3), DurabilityClass::Durable)
        .publish(
            Seq(3),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck,
        );
    // An expired publication-gate decision that no publication rests on. Clean as it stands.
    b.at(4_000)
        .push(authority(
            AuthorityGate::Publication,
            GEN_1,
            (100, 3_000),
            4_000,
            AuthorityOutcome::Expired,
        ))
        .build()
}

#[retcd_test]
fn m7v_66_mut1_accept_stale_authority_trips_inv_auth() {
    support::preamble();
    let good = mutation_base();
    assert!(judge(&good).is_clean(), "{:?}", judge(&good).violations());

    let mutated = mutate::mut1_accept_stale_authority(&good);
    assert_eq!(
        mutated.touched, 1,
        "the mutation must change exactly one event"
    );

    violated(
        &judge(&mutated.trace),
        Invariant::Auth,
        "valid_decision_outside_grant_window",
    );
}

#[retcd_test]
fn m7v_67_mut3_publish_before_ack_trips_inv_pub() {
    support::preamble();
    let good = mutation_base();
    let mutated = mutate::mut3_publish_before_ack(&good);
    assert_eq!(mutated.touched, 1);

    violated(
        &judge(&mutated.trace),
        Invariant::Pub,
        "required_copy_set_unsatisfied",
    );
}

#[retcd_test]
fn m7v_68_mut4_skip_ancestry_trips_inv_lin() {
    support::preamble();
    let good = mutation_base();
    let mutated = mutate::mut4_skip_ancestry(&good);
    assert_eq!(mutated.touched, 1);

    violated(
        &judge(&mutated.trace),
        Invariant::Lin,
        "predecessor_digest_mismatch",
    );
}

#[retcd_test]
fn m7v_81_mut2_counted_forged_ack_trips_inv_pub() {
    support::preamble();
    // Until H1's `ForgeAck` lands, the oracle half is reachable by rewrite. The unmutated run has
    // to be genuinely clean or the mutation proves nothing, so n4 claims the role the topology
    // really grants it — Shadow — and is refused. The mutation is the **elevation** of that claim.
    let mut b = base("m7v-81")
        .place(P0, CONFIG_V1, &[(N4, ReplicaRole::Shadow)])
        .push(healthy())
        .push(initial_root(GEN_1))
        .push(submit(REQ1, &[K1]))
        .push(admit(Seq(5), &[N1, N2, N3], CONFIG_V1));
    b = b.push(valid_authority());
    let recheck = b.last_event();
    let good = b
        .at(1)
        .apply(Seq(5), &[(K1, 5)], ApplyOutcome::Applied)
        .flush(N2, Seq(5))
        .ack_from(N2, Seq(5), DurabilityClass::Durable)
        .flush(N3, Seq(5))
        .ack_from(N3, Seq(5), DurabilityClass::Durable)
        .by(N4, B1)
        .push(TraceKind::ReplicationAck {
            from_node: N4,
            to_node: N1,
            peer_role: ReplicaRole::Shadow,
            peer_boot: B1,
            config_version: CONFIG_V1,
            generation: GEN_1,
            owner_epoch: OwnerEpoch(1),
            contiguous_seq: Seq(5),
            contiguous_digest: digest_at(GEN_1, Seq(5)),
            durability_class: DurabilityClass::Buffered,
            accepted: false,
            reject_reason: Some(rdb_core::contracts::trace::AckRejectReason::StaleConfig),
        })
        .by(N1, B1)
        .publish(
            Seq(5),
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck,
        )
        .build();

    let report = judge(&good);
    assert!(
        report.is_clean(),
        "the unmutated run must be clean or the mutation proves nothing: {:?}",
        report.violations()
    );

    let mutated = mutate::mut2_count_forged_ack(&good);
    assert_eq!(mutated.touched, 1);
    violated(
        &judge(&mutated.trace),
        Invariant::Pub,
        "ack_role_claim_mismatch",
    );
}

#[retcd_test]
fn m7v_71_every_named_mutation_has_a_catching_row() {
    support::preamble();
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/oracle.rs"),
    )
    .expect("this test binary's own source is readable");

    for mutation in MutationId::ALL {
        let rows = mutation.catching_rows();
        assert!(
            !rows.is_empty(),
            "{} has no catching row: a mutation nothing catches is a checker that guards nothing",
            mutation.name()
        );
        if mutation == MutationId::Mut2CountForgedAck {
            assert_eq!(rows.len(), 2, "MUT-2 has a kernel half and an oracle half");
        }
        for row in rows {
            // Kernel-half rows live in kernel-b's binaries; only the oracle halves are asserted
            // to exist here, and the handoff says which are which.
            if mutation.is_trace_rewrite() || *row == "m7v_81" {
                assert!(
                    source.contains(&format!("fn {row}")),
                    "{} names row {row}, which this binary does not define",
                    mutation.name()
                );
            }
        }
    }
}

// ------------------------------------------------------------------------------------------
// M7V-85 — the one assertion-lowering surface
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_85_without_rule_has_no_call_site_until_m7v_23() {
    support::preamble();
    // Built at run time. Spelling the call syntax out as a literal would make this row's own
    // source a call site and fail it on itself.
    let needle = format!(".{}(", "without_rule");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut occurrences: Vec<(String, usize)> = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().is_none_or(|ext| ext != "rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            let count = text.matches(needle.as_str()).count();
            if count > 0 {
                occurrences.push((path.display().to_string(), count));
            }
        }
    }

    let total: usize = occurrences.iter().map(|(_, count)| count).sum();
    assert_eq!(
        total, 0,
        "`{needle}` is the single assertion-lowering surface in this crate and M7V-23 is its \
         only permitted caller; found {total} call site(s) in {occurrences:?}"
    );
    // M7V-23 is held (it needs the I1 runner), so today there is no permitted caller at all and
    // the definition on its own is not a call. When M7V-23 lands, this becomes exactly one.
}
