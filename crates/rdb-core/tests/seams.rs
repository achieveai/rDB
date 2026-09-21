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

use config_log::retcd_test;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::{AppendOutcome, AppendReject};
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{EffectKind, EventKind, KernelEffect, KernelEvent};
use rdb_core::contracts::ids::{BootId, ConfigVersion, NodeId, PartitionId, ReplicaRole, Seq};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::time::{ClockVerdict, ControlTime, Tick};
use rdb_core::contracts::trace::AckRejectReason;
use rdb_core::contracts::transport::PeerLabel;

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
        reason: ErrorKind::Unavailable,
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
        EffectKind::Kernel(KernelEffect::Ignored { reason }) => Some(*reason),
        _ => None,
    };
    assert_eq!(reason, Some(ErrorKind::Unavailable));
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
