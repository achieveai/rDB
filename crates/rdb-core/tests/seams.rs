//! Rows M7F-14, M7F-16 and M7F-17, and the B-R30 threshold vector: package C0's contract
//! seams that a kernel module leans on and must not have to re-derive.
//!
//! | Row | Claim |
//! |---|---|
//! | M7F-14 | `ControlTime::compare` with a sample older than the caller's maximum age is `Uncertain` even when the bound alone would say `DefinitelyBefore` (K-F-15) |
//! | M7F-16 | `copy_of` is `None` for an authenticated peer whose node is a member but whose boot differs (K-F-21) |
//! | M7F-17 | `required_regular` counts two on RF3 and zero on a lone survivor; `primary` is the one primary (K-F-23) |
//! | B-R30 | `PartitionConfig.min_regular_acks` defaults to 1; zero is refused at construction; two is accepted |
//! | K-F-39 | Deserialising a `PartitionConfig` refuses a zero `min_regular_acks` and accepts a valid one |
//! | M7F-53 | CB-1: `EventKind::Kernel` / `EffectKind::Kernel` are one carrier pair, and an exhaustive match over either names it |
//! | M7F-54 | CB-4: `AppendOutcome` is one enum over the accepted, the three non-reject outcomes and the reject ladder |
//! | M7F-55 | CB-2: `AppendReject::NeedPrefix` carries `head_digest` beside `have`, so the cursor can prove divergence |
//! | M7F-56 | CB-3: `AckRejectReason` carries the landed seven plus kernel-b's seven |
//! | M7F-36 | A-R23 1-3: `authority_seq` is on the decision, the view and the trace event, and nothing converts between `Generation`, `OwnerEpoch` and `AuthorityGeneration` |
//! | M7F-37 | A-R23 5: `EventKind::ExternalFenceVerified` is a direct `EventKind` arm carrying its six binding fields, and an exhaustive match names all eight variants |
//! | M7F-40 | B-R30 Q2 / K-F-34: `AppendReject` names each ladder row exactly once — sixteen, asserted against a literal list |
//! | M7F-41 | V-R20 (2): `TraceHeader` has no bare `seed`, no `config_digest` and no top-level `budgets`, and an unknown field is refused
//! | M7F-38 | A-R23 6 / B-R33 Q-B-3, **arm 1**: every `PartitionMode` survives the serde form, and a `Blocked` whose `reason` key is missing is refused. Arm 2 is held on CB-5 |

use config_log::retcd_test;
use rdb_core::contracts::authority::{
    AuthorityDecision, AuthorityView, BlockReason, Checkpoint, DenyReason, EvidenceRef, Lineage,
    PartitionMode, Verdict,
};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::{AppendOutcome, AppendReject};
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{Budgets, EffectKind, EventKind, KernelEffect, KernelEvent};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, CorrelationId, Generation, GrantId, NodeId,
    OwnerEpoch, PartitionId, ReplicaRole, Revision, Seq, TimerId, TimerVersion,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::time::{ClockVerdict, ControlTime, Tick, TimerFired};
use rdb_core::contracts::trace::{
    AckRejectReason, AuthorityGate, AuthorityOutcome, Provenance, RunManifest, TraceHeader,
    TraceKind,
};
use rdb_core::contracts::transport::PeerLabel;
use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;

const fn member(copy: u8, node: u32, boot: u64, role: ReplicaRole) -> Member {
    Member {
        copy: CopyId(copy),
        node: NodeId(node),
        boot: BootId(boot),
        role,
    }
}

/// Node 1 primary, 2 and 3 regular, 4 shadow, every member at boot 1.
fn rf3() -> PartitionConfig {
    PartitionConfig::new(
        PartitionId(1),
        ConfigVersion(1),
        vec![
            member(0, 1, 1, ReplicaRole::Primary),
            member(1, 2, 1, ReplicaRole::RegularSecondary),
            member(2, 3, 1, ReplicaRole::RegularSecondary),
            member(3, 4, 1, ReplicaRole::Shadow),
        ],
    )
}

#[retcd_test]
fn m7f_14_a_stale_sample_is_uncertain_even_when_the_bound_is_confident() {
    let sample = ControlTime {
        estimate: Tick(1_000),
        error_millis: 10,
        bound_established: true,
        sampled_at: Tick(1_000),
    };
    let instant = Tick(5_000);
    let margin = 10;

    // Fresh: the bound alone says the estimate is provably before the instant.
    assert_eq!(
        sample.compare(Tick(1_100), 500, instant, margin),
        ClockVerdict::DefinitelyBefore
    );

    // The same sample, judged later than the caller's maximum age: uncertain.
    let now = Tick(1_000 + 500 + 1);
    assert!(sample.is_stale(now, 500));
    assert_eq!(
        sample.compare(now, 500, instant, margin),
        ClockVerdict::Uncertain,
        "a stale sample denies, whatever the bound says (A-R12, K-F-15)"
    );

    // A sample from the future of `now` is a caller error, and stale.
    assert!(sample.is_stale(Tick(999), 500));
    assert_eq!(
        sample.compare(Tick(999), 500, instant, margin),
        ClockVerdict::Uncertain
    );

    // And no established bound is uncertain before any arithmetic.
    let unbounded = ControlTime {
        bound_established: false,
        ..sample
    };
    assert_eq!(
        unbounded.compare(Tick(1_100), 500, instant, margin),
        ClockVerdict::Uncertain
    );
    tracing::info!(sampled_at = sample.sampled_at.0, now = now.0, "m7f_14");
}

#[retcd_test]
fn m7f_16_a_member_node_at_another_boot_is_not_a_copy() {
    let config = rf3();

    let current = PeerLabel {
        node: NodeId(2),
        boot: BootId(1),
        authenticated: true,
    };
    assert_eq!(
        config.copy_of(&current).map(|member| member.copy),
        Some(CopyId(1))
    );

    let reincarnated = PeerLabel {
        node: NodeId(2),
        boot: BootId(2),
        authenticated: true,
    };
    assert!(
        config.copy_of(&reincarnated).is_none(),
        "same node, new boot: not the copy it used to be (K-F-21)"
    );

    let forged = PeerLabel {
        node: NodeId(2),
        boot: BootId(1),
        authenticated: false,
    };
    assert!(config.copy_of(&forged).is_none());

    let stranger = PeerLabel {
        node: NodeId(9),
        boot: BootId(1),
        authenticated: true,
    };
    assert!(config.copy_of(&stranger).is_none());
}

#[retcd_test]
fn m7f_17_required_regular_excludes_the_primary_and_the_shadow() {
    let config = rf3();

    let required: Vec<NodeId> = config
        .required_regular()
        .map(|member| member.node)
        .collect();
    assert_eq!(
        required,
        vec![NodeId(2), NodeId(3)],
        "two regular secondaries"
    );
    assert_eq!(config.required_regular().count(), 2);
    assert_eq!(
        config.primary().map(|member| member.node),
        Some(NodeId(1)),
        "the one primary"
    );

    let lone = PartitionConfig::new(
        PartitionId(1),
        ConfigVersion(2),
        vec![member(0, 1, 1, ReplicaRole::Primary)],
    );
    assert_eq!(
        lone.required_regular().count(),
        0,
        "a lone survivor waits on nobody"
    );
    assert_eq!(lone.primary().map(|member| member.node), Some(NodeId(1)));

    let fenced = PartitionConfig::new(
        PartitionId(1),
        ConfigVersion(3),
        vec![
            member(1, 2, 1, ReplicaRole::RegularSecondary),
            member(2, 3, 1, ReplicaRole::RegularSecondary),
        ],
    );
    assert!(
        fenced.primary().is_none(),
        "between a fence and the next grant"
    );
    assert_eq!(fenced.required_regular().count(), 2);
    tracing::info!(required = required.len(), "m7f_17");
}

/// Ruling B-R30: the acknowledgement threshold defaults to one and can never be zero.
#[retcd_test]
fn b_r30_min_regular_acks_defaults_to_one_and_refuses_zero() {
    let config = rf3();
    assert_eq!(
        config.min_regular_acks,
        PartitionConfig::DEFAULT_MIN_REGULAR_ACKS
    );
    assert_eq!(config.min_regular_acks, 1);
    assert_eq!(config.validate(), Ok(()));

    let zero = rf3().with_min_regular_acks(0);
    assert_eq!(
        zero,
        Err(RdbError::InvalidArgument {
            field: "min_regular_acks"
        }),
        "zero acknowledgements is not a threshold"
    );

    let two = rf3().with_min_regular_acks(2).expect("two is a threshold");
    assert_eq!(two.min_regular_acks, 2);
    assert_eq!(two.validate(), Ok(()));

    // A literal that bypassed the constructor is caught by validate.
    let mut literal = rf3();
    literal.min_regular_acks = 0;
    assert_eq!(
        literal.validate(),
        Err(RdbError::InvalidArgument {
            field: "min_regular_acks"
        })
    );
    tracing::info!(default = PartitionConfig::DEFAULT_MIN_REGULAR_ACKS, "b_r30");
}

/// Ruling B-R43: one peer is one member. A copy id listed twice, or two copies on one node,
/// is refused — otherwise one node's acknowledgement counts as two copies.
#[retcd_test]
fn b_r43_a_config_refuses_a_repeated_copy_or_a_shared_node() {
    assert_eq!(rf3().validate(), Ok(()));

    let mut repeated_copy = rf3();
    repeated_copy.members[2].copy = CopyId(1);
    assert_eq!(
        repeated_copy.validate(),
        Err(RdbError::InvalidArgument { field: "members" }),
        "copy 1 on nodes 2 and 3 would let node 3's ACK stand for copy 1"
    );

    let mut shared_node = rf3();
    shared_node.members[1].node = NodeId(1);
    assert_eq!(
        shared_node.validate(),
        Err(RdbError::InvalidArgument { field: "members" }),
        "a regular copy on the primary's node is not a second machine"
    );

    let mut shadow_on_regular = rf3();
    shadow_on_regular.members[3].node = NodeId(3);
    assert_eq!(
        shadow_on_regular.validate(),
        Err(RdbError::InvalidArgument { field: "members" }),
        "a shadow sharing a node makes the peer-to-copy mapping ambiguous"
    );

    // The decode path goes through the same rule.
    let wire = serde_json::to_string(&repeated_copy).expect("a contract type serialises");
    assert!(serde_json::from_str::<PartitionConfig>(&wire).is_err());
    tracing::info!("b_r43");
}

/// Finding K-F-39: the threshold invariant is structural on the decode path, not a convention
/// a caller has to remember. A control record carrying zero is refused by the type.
#[retcd_test]
fn k_f_39_a_zero_threshold_is_refused_on_deserialisation() {
    let mut wire = serde_json::to_value(rf3()).expect("a configuration serialises");
    wire["min_regular_acks"] = serde_json::json!(0);

    let refusal = serde_json::from_value::<PartitionConfig>(wire)
        .expect_err("zero acknowledgements is not a threshold, whatever the wire says")
        .to_string();
    assert!(
        refusal.contains("min_regular_acks"),
        "the refusal names the field it refused, got {refusal:?}"
    );
    tracing::info!(refusal = %refusal, "k_f_39");
}

/// Finding K-F-39: the same decode path still accepts a configuration that holds the invariant.
#[retcd_test]
fn k_f_39_a_valid_threshold_deserialises() {
    let two = rf3().with_min_regular_acks(2).expect("two is a threshold");
    let wire = serde_json::to_value(&two).expect("a configuration serialises");

    let decoded =
        serde_json::from_value::<PartitionConfig>(wire).expect("two is a threshold on decode too");
    assert_eq!(decoded, two, "a valid configuration round-trips unchanged");
    assert_eq!(decoded.validate(), Ok(()));
    tracing::info!(min_regular_acks = decoded.min_regular_acks, "k_f_39");
}

// ---------------------------------------------------------------------------------------------
// The round-2 contract asks: CB-1 … CB-4 (kernel-b `architect-handoff.md` §15)
// ---------------------------------------------------------------------------------------------

/// M7F-53 (CB-1): the kernel carrier pair, on both enums, matched exhaustively.
///
/// Kernel-b's §15 calls CB-1 and CB-4 "one shape decision". The shape chosen for both is the one
/// `AppendReject` already uses: **one carrier variant holding an enum the consuming team owns**,
/// rather than a flat variant per kernel fact. A flat spelling would put ~130 kernel-b rows'
/// worth of variants in foundation's file and make every addition a foundation edit.
///
/// Both matches are exhaustive with **no `_` arm**. A wildcard would compile forever and stop
/// catching the next variant silently, which is the whole failure this row exists for.
#[retcd_test]
fn m7f_53_the_kernel_carrier_pair_is_one_variant_on_each_enum() {
    let event = EventKind::Kernel(KernelEvent::PeerProgress {
        peer: NodeId(2),
        contiguous_seq: Seq(9),
    });
    let effect = EffectKind::Kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Error(ErrorKind::Unavailable),
    });

    let event_named = match &event {
        EventKind::Client(_) => "client",
        EventKind::Node(_) => "node",
        EventKind::Transport(_) => "transport",
        EventKind::Storage(_) => "storage",
        EventKind::Control(_) => "control",
        EventKind::Timer(_) => "timer",
        EventKind::ExternalFenceVerified { .. } => "external_fence_verified",
        EventKind::Kernel(_) => "kernel",
    };
    let effect_named = match &effect {
        EffectKind::Send(_) => "send",
        EffectKind::Store(_) => "store",
        EffectKind::Control(_) => "control",
        EffectKind::Timer(_) => "timer",
        EffectKind::Reply(_) => "reply",
        EffectKind::AdoptAuthority { .. } => "adopt_authority",
        EffectKind::Kernel(_) => "kernel",
    };

    assert_eq!(event_named, "kernel");
    assert_eq!(effect_named, "kernel");

    // The carried enums are kernel-b's to grow, so a consumer's match must stay open. This is
    // the machine-readable half of "variants owned by kernel-b".
    let reason = match &effect {
        EffectKind::Kernel(KernelEffect::Ignored { reason }) => Some(reason.clone()),
        _ => None,
    };
    assert_eq!(
        reason,
        Some(KernelIgnoredReason::Error(ErrorKind::Unavailable))
    );
    tracing::info!(event_named, effect_named, "m7f_53 carrier pair");
}

/// M7F-54 (CB-4): `AppendOutcome` is **one enum**, not `Result<Accepted, AppendReject>`.
///
/// The ask asked foundation to state which. One enum, because the three non-reject outcomes are
/// neither an acceptance nor a refusal: a `Result` would have to nest a second enum inside `Ok`
/// to carry them, and the cursor would then match twice to answer one question. One enum keeps
/// the cursor's match total over the whole ladder, which is the discipline `AppendReject`'s
/// sixteen variants already have.
#[retcd_test]
fn m7f_54_append_outcome_is_one_enum_over_the_whole_ladder() {
    let busy = AppendOutcome::Busy {
        accepted_through: Seq(7),
    };
    let already = AppendOutcome::AlreadyHave;
    let probe = AppendOutcome::ProbeDigestAt { seq: Seq(9) };
    let rejected = AppendOutcome::Rejected(AppendReject::Quarantined);

    // Exhaustive, no `_` arm: a sixth outcome has to be read here before it can be ignored.
    let name = |outcome: &AppendOutcome| match outcome {
        AppendOutcome::Accepted(_) => "accepted",
        AppendOutcome::Busy { .. } => "busy",
        AppendOutcome::AlreadyHave => "already_have",
        AppendOutcome::ProbeDigestAt { .. } => "probe_digest_at",
        AppendOutcome::Rejected(_) => "rejected",
    };

    assert_eq!(name(&busy), "busy");
    assert_eq!(name(&already), "already_have");
    assert_eq!(name(&probe), "probe_digest_at");
    assert_eq!(name(&rejected), "rejected");

    // The three drive the cursor: re-send from `accepted_through`, advance, answer a probe.
    assert!(matches!(
        busy,
        AppendOutcome::Busy {
            accepted_through: Seq(7)
        }
    ));
    assert!(matches!(
        probe,
        AppendOutcome::ProbeDigestAt { seq: Seq(9) }
    ));
    tracing::info!(outcomes = 5, "m7f_54 append outcome");
}

/// M7F-55 (CB-2): `NeedPrefix` carries the head digest beside the sequence.
///
/// With `have` alone the cursor knows where to resume and cannot tell "you are behind" from
/// "your history and mine disagree" — the one-writer path (K-B-52) loses one of its two inputs.
/// So two rejects at the same `have` with different digests must not compare equal; that
/// inequality is the whole content of the field.
#[retcd_test]
fn m7f_55_need_prefix_carries_the_head_digest_beside_have() {
    let behind = AppendReject::NeedPrefix {
        have: Seq(4),
        head_digest: Digest::ROOT,
    };
    let diverged = AppendReject::NeedPrefix {
        have: Seq(4),
        head_digest: Digest([7; 32]),
    };

    assert_ne!(
        behind, diverged,
        "same position, different history: the digest is what tells them apart"
    );
    let AppendReject::NeedPrefix { have, head_digest } = behind else {
        panic!("NeedPrefix carries both fields");
    };
    assert_eq!(have, Seq(4));
    assert_eq!(head_digest, Digest::ROOT);
    tracing::info!(have = have.0, "m7f_55 need prefix");
}

/// M7F-56 (CB-3): `AckRejectReason` carries the landed seven plus kernel-b's seven.
///
/// §3.4's ladder has eleven drop reasons and the landed enum carried seven of them, none of
/// which was one of the seven asked for. Without the widening a row cannot tell "dropped
/// because diverged" from "dropped because stale", which is the entire content of kernel-b's
/// rows 1d and 9.
///
/// Asserted against a literal list rather than a count. A count passes when a variant is
/// renamed, and renaming one is exactly how an oracle stops seeing a reason it used to fold.
#[retcd_test]
fn m7f_56_ack_reject_reason_carries_fourteen_named_reasons() {
    let all = [
        AckRejectReason::Gap,
        AckRejectReason::DigestMismatch,
        AckRejectReason::StaleEpoch,
        AckRejectReason::StaleBoot,
        AckRejectReason::StaleConfig,
        AckRejectReason::ForgedIdentity,
        AckRejectReason::IncompatibleVersion,
        AckRejectReason::StaleGeneration,
        AckRejectReason::RoleMismatch,
        AckRejectReason::InconsistentProgress,
        AckRejectReason::RegressedProgress,
        AckRejectReason::Unverifiable,
        AckRejectReason::Diverged,
        AckRejectReason::NotAMember,
    ];

    let mut names: Vec<String> = all
        .iter()
        .map(|reason| {
            serde_json::to_value(reason)
                .expect("serialises")
                .to_string()
        })
        .collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 14, "fourteen distinct reasons, no alias");

    // `StaleGeneration` and `NotAMember` are also `AppendReject` variants. Same words, different
    // enum, different meaning: one is why a replica refused an append, the other why the
    // primary would not count an acknowledgement. Kept deliberately, not by accident.
    assert_ne!(
        serde_json::to_value(AckRejectReason::NotAMember).expect("serialises"),
        serde_json::to_value(AckRejectReason::Diverged).expect("serialises"),
    );
    tracing::info!(reasons = names.len(), "m7f_56 ack reject reason");
}

// ---------------------------------------------------------------------------------------------
// M7F-36, M7F-37, M7F-40, M7F-41 — the cross-team contract shapes foundation owns (plan §10)
// ---------------------------------------------------------------------------------------------

/// The `EventKind` variant's own name, from an exhaustive match with **no `_` arm**.
///
/// The exhaustiveness is the assertion: a ninth `EventKind` variant makes this file fail to
/// compile, which is what a wildcard arm would silently absorb. `design.md` §4.1 still lists six
/// variants, so a match written from the design would not compile either — deliberately.
const fn event_kind_name(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::Client(_) => "Client",
        EventKind::Node(_) => "Node",
        EventKind::Transport(_) => "Transport",
        EventKind::Storage(_) => "Storage",
        EventKind::Control(_) => "Control",
        EventKind::Timer(_) => "Timer",
        EventKind::ExternalFenceVerified { .. } => "ExternalFenceVerified",
        EventKind::Kernel(_) => "Kernel",
    }
}

/// M7F-37: `ExternalFenceVerified` is a direct `EventKind` arm and carries its six binding
/// fields.
///
/// **What turns this red:** dropping or renaming any of the six fields; moving the variant out
/// of `EventKind` (say, into a `ControlEvent`), which would let a module manufacture one instead
/// of receiving it through the scheduler like every other event; adding a ninth `EventKind`
/// variant without re-reading this row, because `event_kind_name` has no wildcard arm.
///
/// The six fields together are what makes a takeover auditable from history alone. In
/// particular `control_revision` is the revision at which a **linearizable read** found the
/// grant frozen — evidence, not a belief — and the A1 guard compares all six against its own
/// takeover state rather than against values filled in from that state (finding K-A-37).
#[retcd_test]
fn m7f_37_external_fence_verified_carries_its_six_binding_fields() {
    let event = EventKind::ExternalFenceVerified {
        partition: PartitionId(7),
        prior_generation: Generation(3),
        prior_owner_epoch: OwnerEpoch(4),
        prior_boot_id: BootId(5),
        control_revision: Revision(6),
        evidence: EvidenceRef([9_u8; 32]),
    };

    let EventKind::ExternalFenceVerified {
        partition,
        prior_generation,
        prior_owner_epoch,
        prior_boot_id,
        control_revision,
        evidence,
    } = &event
    else {
        panic!("the value just built is that variant");
    };
    assert_eq!(*partition, PartitionId(7), "the partition taken over");
    assert_eq!(*prior_generation, Generation(3), "the lineage served");
    assert_eq!(*prior_owner_epoch, OwnerEpoch(4), "the tenure held");
    assert_eq!(*prior_boot_id, BootId(5), "the prior process lifetime");
    assert_eq!(
        *control_revision,
        Revision(6),
        "the revision a linearizable read found the grant frozen at"
    );
    assert_eq!(evidence.0, [9_u8; 32], "the opaque external handle");

    assert_eq!(
        event_kind_name(&event),
        "ExternalFenceVerified",
        "a direct EventKind arm, so it arrives through the scheduler like every other event"
    );
    // A second arm, so the name function is not a constant dressed as a match.
    assert_eq!(
        event_kind_name(&EventKind::Timer(TimerFired {
            id: TimerId(1),
            version: TimerVersion(1),
            scheduled_at: Tick::ZERO,
        })),
        "Timer"
    );
    tracing::info!(variant = event_kind_name(&event), "m7f_37 external fence");
}

/// M7F-36: `authority_seq` is on the decision, on the view and on the trace event — and the
/// three generation-shaped newtypes do not convert into one another.
///
/// **What turns this red:** dropping `authority_seq` from any of the three (each read-back is a
/// field access, so the field going away is a compile failure and the value going wrong is an
/// assertion failure); retyping `AuthorityView.past_horizon` to something other than
/// `DenyReason`; or adding an `impl From<..>` in `ids.rs` between `Generation`, `OwnerEpoch` and
/// `AuthorityGeneration`.
///
/// The last clause is a source read rather than a value assertion because that is the only way
/// to catch it: a `From` is an addition, and an addition breaks no existing assertion. A1's
/// `GenerationChanged` (a partition's history incarnation) and `AuthorityGenerationChanged` (the
/// cluster's) are different facts, and a conversion between them would let one be reported as
/// the other by a caller who only had to write `.into()`.
#[retcd_test]
fn m7f_36_authority_seq_is_on_the_decision_the_view_and_the_trace() {
    let lineage = Lineage {
        partition: PartitionId(1),
        generation: Generation(2),
        owner_epoch: OwnerEpoch(3),
    };

    let decision = AuthorityDecision {
        owner: NodeId(1),
        boot: BootId(1),
        grant: GrantId(4),
        authority_generation: AuthorityGeneration(6),
        lineage,
        expiry_utc_ms: 0,
        decided_at: Tick(11),
        authority_seq: 5,
        checkpoint: Checkpoint::Admission,
        correlation: CorrelationId(1),
        verdict: Verdict::Admit,
    };
    assert_eq!(decision.authority_seq, 5, "on the decision");

    let view = AuthorityView {
        lineage,
        grant_id: GrantId(4),
        boot_id: BootId(1),
        authority_generation: AuthorityGeneration(6),
        config_version: ConfigVersion(1),
        authority_seq: 5,
        valid_through_tick: Tick(100),
        past_horizon: DenyReason::AuthorityGenerationChanged,
    };
    assert_eq!(view.authority_seq, 5, "on the view");
    assert_eq!(
        view.past_horizon,
        DenyReason::AuthorityGenerationChanged,
        "past_horizon is a DenyReason, so the reason whose horizon bound first is named rather \
         than inferred"
    );

    let earlier = AuthorityView {
        authority_seq: 4,
        ..view
    };
    assert!(
        earlier.authority_seq < view.authority_seq,
        "a consumer keeps the highest authority_seq it has seen and rejects an answer below it; \
         that comparison is what makes a stale pushed view refusable without comparing ticks"
    );
    assert!(
        decision.same_lineage_as_view(&view),
        "the decision and the view a consumer admitted under describe one lineage"
    );

    let recorded = TraceKind::AuthorityDecision {
        gate: AuthorityGate::Admission,
        owner_node: NodeId(1),
        owner_epoch: OwnerEpoch(3),
        grant: GrantId(4),
        grant_boot: BootId(1),
        generation: Generation(2),
        valid_from_tick: 0,
        expiry_tick: 100,
        decision_tick: 11,
        authority_seq: 5,
        outcome: AuthorityOutcome::Valid,
    };
    let TraceKind::AuthorityDecision { authority_seq, .. } = recorded else {
        panic!("the value just built is that variant");
    };
    assert_eq!(authority_seq, 5, "and on the trace event");

    // Three newtypes, no conversion between them. `ids.rs` is read rather than grepped from a
    // shell so the row runs the same way on every host the repository is worked on.
    let ids = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/contracts/ids.rs"))
        .expect("the identity module is part of this crate");
    assert!(
        !ids.contains("impl From<"),
        "no identity newtype converts into another: Generation is a partition history \
         incarnation, OwnerEpoch is a tenure inside one, AuthorityGeneration is the cluster's — \
         three facts, and a From between any two would let one be reported as another"
    );
    tracing::info!(authority_seq, "m7f_36 authority seq");
}

/// The `AppendReject` variant's own name, from an exhaustive match with **no `_` arm**.
const fn append_reject_name(reject: &AppendReject) -> &'static str {
    match reject {
        AppendReject::Quarantined => "Quarantined",
        AppendReject::IncompatibleVersion => "IncompatibleVersion",
        AppendReject::TooLarge => "TooLarge",
        AppendReject::WrongPartition => "WrongPartition",
        AppendReject::StaleGeneration { .. } => "StaleGeneration",
        AppendReject::NeedLineage { .. } => "NeedLineage",
        AppendReject::StaleEpoch { .. } => "StaleEpoch",
        AppendReject::UnknownEpoch { .. } => "UnknownEpoch",
        AppendReject::StaleConfig { .. } => "StaleConfig",
        AppendReject::NeedConfig { .. } => "NeedConfig",
        AppendReject::NotAMember => "NotAMember",
        AppendReject::CorruptHistory { .. } => "CorruptHistory",
        AppendReject::DivergentHistory { .. } => "DivergentHistory",
        AppendReject::NeedPrefix { .. } => "NeedPrefix",
        AppendReject::StaleFence => "StaleFence",
        AppendReject::Unauthenticated => "Unauthenticated",
    }
}

/// M7F-40: `AppendReject` names every ladder row once — sixteen names, no synonym.
///
/// **What turns this red:** adding a seventeenth variant (`append_reject_name` has no wildcard
/// arm, so the file stops compiling); removing or renaming one of the sixteen (the literal list
/// below stops matching); or introducing a second spelling of an existing ladder row, which
/// shows up as a seventeenth name.
///
/// Sixteen is the **code's** count, not the design's: `design.md` §4.8 shows five. A row written
/// from the design would assert a five-name set, pass, and never notice the other eleven. This
/// row does not assert a ladder *order* — that is kernel-b's `M7B-15`. It asserts that
/// foundation shipped one name per ladder row.
#[retcd_test]
fn m7f_40_append_reject_names_every_ladder_row_once() {
    let ladder = [
        AppendReject::Quarantined,
        AppendReject::IncompatibleVersion,
        AppendReject::TooLarge,
        AppendReject::WrongPartition,
        AppendReject::StaleGeneration {
            current: Generation(1),
        },
        AppendReject::NeedLineage {
            current: Generation(1),
        },
        AppendReject::StaleEpoch {
            current: OwnerEpoch(1),
        },
        AppendReject::UnknownEpoch {
            current: OwnerEpoch(1),
        },
        AppendReject::StaleConfig {
            current: ConfigVersion(1),
        },
        AppendReject::NeedConfig {
            current: ConfigVersion(1),
        },
        AppendReject::NotAMember,
        AppendReject::CorruptHistory { at: Seq(1) },
        AppendReject::DivergentHistory { at: Seq(1) },
        AppendReject::NeedPrefix {
            have: Seq(1),
            head_digest: Digest::ROOT,
        },
        AppendReject::StaleFence,
        AppendReject::Unauthenticated,
    ];

    let mut names: Vec<&'static str> = ladder.iter().map(append_reject_name).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(
        names,
        [
            "CorruptHistory",
            "DivergentHistory",
            "IncompatibleVersion",
            "NeedConfig",
            "NeedLineage",
            "NeedPrefix",
            "NotAMember",
            "Quarantined",
            "StaleConfig",
            "StaleEpoch",
            "StaleFence",
            "StaleGeneration",
            "TooLarge",
            "Unauthenticated",
            "UnknownEpoch",
            "WrongPartition",
        ],
        "the sixteen ladder rows, sorted — one name each, and no two rows sharing a name"
    );

    // Distinct on the wire too: two ladder rows that serialised to one tag would be told apart
    // in the kernel and conflated in the record a later reader sees.
    let mut wire: Vec<String> = ladder
        .iter()
        .map(|reject| {
            serde_json::to_value(reject)
                .expect("serialises")
                .to_string()
        })
        .collect();
    wire.sort_unstable();
    wire.dedup();
    assert_eq!(wire.len(), 16, "sixteen distinct wire forms");

    // `NotAMember` is also spelled on `AckRejectReason`. Same word, different enum, different
    // fact — one is why a replica refused an append, the other why a primary would not count an
    // acknowledgement. Their **bare** wire tags are the same string, so the ladder's sixteen
    // names are unique only inside this enum:
    assert_eq!(
        serde_json::to_value(AppendReject::NotAMember).expect("serialises"),
        serde_json::to_value(AckRejectReason::NotAMember).expect("serialises"),
        "the bare tags collide across the two enums — uniqueness here is per enum, not global"
    );
    // …and the thing that tells the two facts apart on the wire is CB-7's carrier arm, not the
    // word. An untagged or flattened carrier would lose the distinction silently, which the
    // frozen design rated CB-7's most likely real defect.
    assert_ne!(
        serde_json::to_value(KernelIgnoredReason::AppendRejected(
            AppendReject::NotAMember
        ))
        .expect("serialises"),
        serde_json::to_value(KernelIgnoredReason::AckRejected(
            AckRejectReason::NotAMember
        ))
        .expect("serialises"),
        "the arm is the namespace: {{\"AppendRejected\":\"NotAMember\"}} is not \
         {{\"AckRejected\":\"NotAMember\"}}"
    );
    tracing::info!(rows = names.len(), "m7f_40 append reject ladder");
}

/// M7F-41: the trace header carries no bare seed, no `config_digest` and no top-level
/// `budgets`, and an unknown field is refused.
///
/// **What turns this red:** adding a convenience `seed` (or `config_digest`, or a top-level
/// `budgets`) to `TraceHeader`, which the key-set assertion catches; or dropping
/// `#[serde(deny_unknown_fields)]`, which the refusal catches.
///
/// A seed is not a reproducer (ADR-rdb-0003 decision 6): the recorded event stream is. A second
/// copy of the seed on the header is a value a writer can disagree with, and the only seed the
/// header holds is inside `Provenance::Generated`, where it is one of three mutually exclusive
/// provenances rather than a field every header has. The budgets live in `config: RunManifest`
/// with their `overridden` list, so a top-level copy would be a second answer to "what did this
/// run use".
#[retcd_test]
fn m7f_41_the_header_carries_no_bare_seed() {
    let header = TraceHeader {
        schema_version: TRACE_SCHEMA_VERSION,
        generator_version: 1,
        provenance: Provenance::Generated { seed: 7 },
        config: RunManifest {
            budgets: Budgets::SPEC_DEFAULTS,
            overridden: Vec::new(),
            nodes: 3,
            event_cap: 1_000,
        },
        partitions: 1,
        topology: Vec::new(),
        oracle_checkpoint_digest: Digest::ROOT,
    };

    let value = serde_json::to_value(&header).expect("the header serialises");
    let object = value
        .as_object()
        .expect("a header is a JSON object")
        .clone();
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let field_count = keys.len();
    assert_eq!(
        keys,
        [
            "config",
            "generator_version",
            "oracle_checkpoint_digest",
            "partitions",
            "provenance",
            "schema_version",
            "topology",
        ],
        "seven fields, and none of them is `seed`, `config_digest` or a top-level `budgets`"
    );

    // The positive control: the header this row built is readable, so the refusal below is
    // about the extra field and not about the fixture.
    assert_eq!(
        serde_json::from_value::<TraceHeader>(value.clone()).expect("round-trips"),
        header
    );

    let mut smuggled = object;
    smuggled.insert("seed".to_owned(), serde_json::Value::from(7));
    let refused = serde_json::from_value::<TraceHeader>(serde_json::Value::Object(smuggled))
        .expect_err("deny_unknown_fields refuses a header field this build does not know");
    assert!(
        refused.to_string().contains("seed"),
        "the refusal names the field that was smuggled in, got {refused}"
    );

    // The seed that does exist is inside the provenance, where it is one of three alternatives.
    let Provenance::Generated { seed } = header.provenance else {
        panic!("this header was generated");
    };
    assert_eq!(seed, 7, "one seed, in the provenance, with no second copy");
    tracing::info!(fields = field_count, "m7f_41 trace header");
}

/// M7F-38 **arm 1**: every `PartitionMode` survives the wire, and a `Blocked` with no reason
/// does not.
///
/// **What turns this red:** anything that lets a `Blocked` decode without the reason that was
/// written — `#[serde(default)]` on `BlockReason`, changing the field to `Option<BlockReason>`,
/// a `#[serde(skip)]`, or a hand-written `Deserialize` that fills a missing reason in. All four
/// pass the round trip above and fail the refusal below. Adding a fifth `PartitionMode` variant
/// fails the exhaustive `match`, which has no `_` arm.
///
/// The round trip on its own is the weak half and is here as the positive control: it proves the
/// fixture is decodable, so the refusal is about the missing key and not about the value. The
/// refusal is the half a wrong implementation fails, and it is the case that matters, because
/// the control record is where `Blocked` is read by a node that was not there when it was
/// decided — and a `Blocked` with no reason is a partition an operator cannot act on.
///
/// **Arm 2 is `Unavailable` on ask CB-5** and is deliberately not written here. It asserts that
/// each of ruling B-R34's four reasons decodes to the reason that was written; `BlockReason` has
/// **one** variant at this commit (`contracts/authority.rs:198-206`), so three of the four names
/// do not compile. Under §2 rule 4 the arm lands *inside this function* when they arrive, never
/// as a second row. `ControlUnavailable` exists today only as a `DenyReason` variant, which is a
/// different enum; `ControlUnknown` and `NoEligibleRegular` exist nowhere in `crates/`.
#[retcd_test]
fn m7f_38_partition_mode_blocked_carries_a_block_reason() {
    let diverged = vec![CopyId(1), CopyId(2)];
    let blocked = PartitionMode::Blocked {
        reason: BlockReason::DivergenceRequiresOperator {
            diverged: diverged.clone(),
        },
    };

    // Four modes, named by an exhaustive match with no `_`: a fifth stops this compiling.
    let modes = [
        PartitionMode::Active,
        PartitionMode::DegradedRf2,
        PartitionMode::ReadOnly,
        blocked.clone(),
    ];
    let names: Vec<&str> = modes
        .iter()
        .map(|mode| match mode {
            PartitionMode::Active => "Active",
            PartitionMode::DegradedRf2 => "DegradedRf2",
            PartitionMode::ReadOnly => "ReadOnly",
            PartitionMode::Blocked { .. } => "Blocked",
        })
        .collect();
    assert_eq!(
        names,
        ["Active", "DegradedRf2", "ReadOnly", "Blocked"],
        "A-R23 item 6: four landed modes, and round 1 of this plan listed three"
    );

    for mode in &modes {
        let wire = serde_json::to_value(mode).expect("a mode serialises");
        assert_eq!(
            &serde_json::from_value::<PartitionMode>(wire).expect("a mode round-trips"),
            mode,
            "{mode:?} must come back as itself"
        );
    }

    // The reason survives by value, and the diverged copies keep their order: an alert that
    // renamed the copies would name the wrong ones.
    let decoded = serde_json::from_value::<PartitionMode>(
        serde_json::to_value(&blocked).expect("blocked serialises"),
    )
    .expect("blocked round-trips");
    let PartitionMode::Blocked { reason } = decoded else {
        panic!("a Blocked must decode as Blocked");
    };
    // `BlockReason` widened from one variant to six (seam freeze R-S1), so this binding is no
    // longer irrefutable. The assertion below is unchanged: this row is about `diverged` keeping
    // its order across a round-trip, not about the enum's arity.
    let BlockReason::DivergenceRequiresOperator { diverged: read } = reason else {
        panic!("a DivergenceRequiresOperator must decode as DivergenceRequiresOperator");
    };
    assert_eq!(
        read, diverged,
        "the copies come back in the order they were written"
    );

    // The arm a wrong implementation fails: the same wire value with `reason` taken out.
    let mut wire = serde_json::to_value(&blocked).expect("blocked serialises");
    let body = wire
        .get_mut("Blocked")
        .and_then(serde_json::Value::as_object_mut)
        .expect("Blocked is externally tagged, so its fields sit under its name");
    body.remove("reason")
        .expect("the fixture had a reason to remove");
    let refused = serde_json::from_value::<PartitionMode>(wire)
        .expect_err("a Blocked with no reason is not a PartitionMode");
    assert!(
        refused.to_string().contains("reason"),
        "the refusal names the field that is missing, got {refused}"
    );

    tracing::info!(
        modes = names.len(),
        diverged = read.len(),
        "m7f_38 partition mode"
    );
}
