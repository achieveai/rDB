//! `admin rebuild N` (bug bash #65, critic-bb §3): take node N's copy of partition 1 out, then
//! bring it back, and the way out of `Quarantined`.
//!
//! Drop first, wipe after (B2): a copy is wiped only once a commit that leaves it out has
//! landed, so every acked write is already on the owner and another regular copy. The modes:
//!
//! - **A** N is not pinned (a drop left it out): restart N at a new boot; A-R407's re-add takes
//!   it back, first as a shadow. Wiped only when asked.
//! - **B** N is pinned: the owner re-plans without N, as `drop_copy` does, waits for that commit
//!   ([`drop_landed`], G5), then A.
//! - **C** the owner's F1 is quarantined: the owner replaces it with a fresh run planned without
//!   N, so F1 checks the kept copies again, then A with the wipe.
//!
//! The guards ([`decide`]) refuse with nothing written. The wipe is fenced ([`fence_refusal`],
//! M5) and moves N's folder aside, never deleting it (G7, `Db::restart_node_with`).

use bytes::Bytes;
use config_core::{ConfigStore, GetRequest};
use rdb_core::authority::partition::{PartitionRecord, RecordMember};
use rdb_core::contracts::authority::PartitionMode;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::ids::{BootId, Generation, NodeId, ReplicaRole};
use rdb_core::contracts::membership::Member;
use rdb_core::contracts::recovery::DivergenceEvidence;

/// Which way N comes back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RebuildMode {
    /// A: no commit pins N.
    NotPinned,
    /// B: the commit pins N; a re-plan drops it first.
    Pinned,
    /// C: the owner's F1 is quarantined; a fresh run plans without N.
    Quarantined,
}

impl RebuildMode {
    /// The mode's letter, as critic-bb §3 names it.
    pub(crate) const fn letter(self) -> &'static str {
        match self {
            Self::NotPinned => "A",
            Self::Pinned => "B",
            Self::Quarantined => "C",
        }
    }
}

/// The owner's answer once N may be restarted: a commit leaving N out has landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ready {
    /// How N comes back.
    pub(crate) mode: RebuildMode,
    /// The generation of the commit that leaves N out: the wipe is fenced at or above it.
    pub(crate) generation: Generation,
    /// The regular copies the commit keeps, the owner's included: where the data stays.
    pub(crate) kept: Vec<NodeId>,
}

/// What a rebuild did, for its accept line (G7: the kept copies and where N's folder went).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rebuilt {
    /// N.
    pub(crate) node: NodeId,
    /// How it came back.
    pub(crate) mode: RebuildMode,
    /// The commit that left N out.
    pub(crate) generation: Generation,
    /// The regular copies that commit kept.
    pub(crate) kept: Vec<NodeId>,
    /// The boot N runs now.
    pub(crate) boot: BootId,
    /// Where N's old folder went, when it was wiped.
    pub(crate) aside: Option<std::path::PathBuf>,
}

impl Rebuilt {
    /// The accept line `rdb_dev` prints.
    pub(crate) fn line(&self) -> String {
        let kept: Vec<String> = self.kept.iter().map(|node| node.0.to_string()).collect();
        format!(
            "rebuilt: node {} (mode {}) is pinned regular again at boot {}; generation {} left \
             it out and kept node{} {}; {}",
            self.node.0,
            self.mode.letter(),
            self.boot.0,
            self.generation.0,
            if kept.len() == 1 { "" } else { "s" },
            kept.join(" and "),
            self.aside.as_ref().map_or_else(
                || "its folder was kept, not wiped".to_owned(),
                |to| format!("its old folder was moved aside to {}", to.display())
            )
        )
    }
}

/// Why a rebuild was refused, with nothing written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refused {
    /// The guard that refused: `G1`..`G6`, `M5`, or `owner`, `timeout` and `quarantined`.
    pub(crate) guard: &'static str,
    /// What the operator reads, with the next step.
    pub(crate) detail: String,
}

impl Refused {
    fn new(guard: &'static str, detail: String) -> Self {
        Self { guard, detail }
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.detail, self.guard)
    }
}

/// What the owner knows when it is asked to rebuild `node`.
pub(crate) struct Asked<'a> {
    /// N.
    pub(crate) node: NodeId,
    /// The owner: the node asked.
    pub(crate) owner: NodeId,
    /// The members the owner's newest commit pins.
    pub(crate) members: &'a [Member],
    /// Every node of the fixed membership (`admin::members`).
    pub(crate) known: &'a [NodeId],
    /// Whether a re-plan runs for the partition.
    pub(crate) rejoin: bool,
    /// `Some` when the owner's F1 is quarantined, with the divergence evidence it reported,
    /// when the owner holds it.
    pub(crate) quarantine: Option<Option<&'a DivergenceEvidence>>,
}

/// The mode for `asked`, or the guard that refuses (critic-bb §3). `current` says whether a
/// pinned member is linked to the owner at the boot it is pinned at: G4 counts only those.
///
/// # Errors
///
/// [`Refused`] naming G1 (N is no member), G2 (N owns the partition), G3 (a re-plan runs), G6
/// (quarantined, and the evidence does not name N) or G4 (no other regular copy would stay).
pub(crate) fn decide(
    asked: &Asked<'_>,
    current: impl Fn(&Member) -> bool,
) -> Result<RebuildMode, Refused> {
    let node = asked.node.0;
    let pinned = asked.members.iter().any(|m| m.node == asked.node);
    if !pinned && !asked.known.contains(&asked.node) {
        return Err(Refused::new(
            "G1",
            format!("node {node} holds no copy of partition 1: there is nothing to rebuild"),
        ));
    }
    if asked.node == asked.owner {
        return Err(Refused::new(
            "G2",
            format!(
                "node {node} owns partition 1, and the owner's copy is never rebuilt: move the \
                 partition first with `admin promote <other node> --drop {node}`, then rebuild \
                 node {node}"
            ),
        ));
    }
    // A re-plan F1 quarantined never ends by itself, so it does not count as in flight.
    if asked.rejoin && asked.quarantine.is_none() {
        return Err(Refused::new(
            "G3",
            format!(
                "a re-plan of partition 1 is running; rebuild node {node} once it has committed \
                 (`nodes` shows the recovery phase)"
            ),
        ));
    }
    let mode = match asked.quarantine {
        Some(evidence) => {
            let named = evidence.is_some_and(|e| names(e, asked.members, asked.node));
            if !named {
                return Err(Refused::new(
                    "G6",
                    format!(
                        "partition 1 is quarantined, but the divergence evidence ({}) does not \
                         name node {node}'s copy: rebuild a node it names",
                        evidence.map_or_else(|| "none held".to_owned(), |e| format!("{e:?}"))
                    ),
                ));
            }
            RebuildMode::Quarantined
        }
        None if pinned => RebuildMode::Pinned,
        None => return Ok(RebuildMode::NotPinned),
    };
    let others = asked
        .members
        .iter()
        .filter(|m| m.role != ReplicaRole::Shadow)
        .filter(|m| m.node != asked.node && m.node != asked.owner)
        .filter(|m| current(m))
        .count();
    if others == 0 {
        return Err(Refused::new(
            "G4",
            format!(
                "dropping node {node} would leave node {} the only regular copy of partition 1 \
                 (degraded to two copies, or the other copy is down or unlinked): bring the \
                 other copy back first",
                asked.owner.0
            ),
        ));
    }
    Ok(mode)
}

/// Whether `evidence` names `node`'s copy, as `members` place the copies.
fn names(evidence: &DivergenceEvidence, members: &[Member], node: NodeId) -> bool {
    let copies = match evidence {
        DivergenceEvidence::RootMismatch { copy, .. } => vec![*copy],
        DivergenceEvidence::Pairwise { a, b, .. } => vec![a.0, b.0],
    };
    members
        .iter()
        .any(|m| m.node == node && copies.contains(&m.copy))
}

/// G5: the drop commit landed `Active` or `DegradedRf2` and pins no `node`.
///
/// # Errors
///
/// [`Refused`] naming G5, saying node N stays as it is and how to take it back.
pub(crate) fn drop_landed(
    node: NodeId,
    mode: &PartitionMode,
    members: &[Member],
) -> Result<(), Refused> {
    let serving = matches!(mode, PartitionMode::Active | PartitionMode::DegradedRf2);
    let left_out = !members.iter().any(|m| m.node == node);
    if serving && left_out {
        return Ok(());
    }
    Err(Refused::new(
        "G5",
        format!(
            "the re-plan without node {} committed {mode:?}{}, so node {} was not wiped: restart \
             node {} to re-add it",
            node.0,
            if left_out { "" } else { " and still pins it" },
            node.0,
            node.0
        ),
    ))
}

/// The regular copies `members` pins, other than `node`.
pub(crate) fn kept(members: &[Member], node: NodeId) -> Vec<NodeId> {
    members
        .iter()
        .filter(|m| m.role != ReplicaRole::Shadow && m.node != node)
        .map(|m| m.node)
        .collect()
}

/// M5: why `node`'s wipe is refused, or `None` when it may go ahead. The wipe was asked at
/// generation `fenced`. `record` is `partitions/1`'s generation and its members (`None`:
/// layout 3, which cannot show N left out), read after N stopped. `boots` is N's boot when the
/// wipe was asked and its boot as of the stop, which must be equal. It is `None` only for
/// `lr`, which has no boot of N's drop and reads the owner's live pin after the stop instead
/// (A-R441 F2).
pub(crate) fn fence_refusal(
    node: NodeId,
    fenced: Generation,
    record: (Generation, Option<&[RecordMember]>),
    boots: Option<(BootId, BootId)>,
) -> Option<Refused> {
    let n = node.0;
    let (generation, members) = record;
    let why = if let Some((boot, last)) = boots.filter(|(boot, last)| boot != last) {
        format!(
            "node {n} ran boot {} when the wipe was asked and boot {} since: it restarted, \
             and may have been taken back",
            boot.0, last.0
        )
    } else if generation < fenced {
        format!(
            "partitions/1 is at generation {} below the drop's {}",
            generation.0, fenced.0
        )
    } else if let Some(members) = members {
        if !members.iter().any(|m| m.node == node) {
            return None;
        }
        format!(
            "partitions/1 at generation {} pins node {n} again",
            generation.0
        )
    } else {
        format!(
            "partitions/1 at generation {} carries no members (layout 3), so it cannot show \
             node {n} left out",
            generation.0
        )
    };
    Some(Refused::new(
        "M5",
        format!("node {n} not wiped: {why}; its folder is left in place"),
    ))
}

/// `partitions/1`'s generation and the members it pins (`None`: layout 3), read now, for
/// [`fence_refusal`].
///
/// # Errors
///
/// The store's error, or a record that is absent or does not decode, as text.
pub(crate) async fn record(
    store: &dyn ConfigStore,
) -> Result<(Generation, Option<Vec<RecordMember>>), String> {
    let read = store
        .get(GetRequest {
            key: Bytes::from(ControlKey::Partition(super::PARTITION).encode()),
        })
        .await
        .map_err(|error| format!("get partitions/1: {error}"))?;
    let found = read
        .record
        .ok_or_else(|| "partitions/1 is absent".to_owned())?;
    let record = PartitionRecord::decode(&found.value).ok_or_else(|| {
        format!(
            "partitions/1 at revision {} does not decode",
            found.mod_revision
        )
    })?;
    Ok((record.generation, RecordMember::decode_all(&found.value)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::ids::Seq;
    use rdb_core::contracts::membership::CopyId;

    fn member(node: u32, role: ReplicaRole) -> Member {
        Member {
            // The real map, as `admin::members` builds it: copy id is node - 1 (critic-65 F3).
            copy: CopyId(u8::try_from(node - 1).expect("small node")),
            node: NodeId(node),
            boot: BootId(10),
            role,
        }
    }

    fn rf3() -> Vec<Member> {
        vec![
            member(1, ReplicaRole::Primary),
            member(2, ReplicaRole::RegularSecondary),
            member(3, ReplicaRole::RegularSecondary),
        ]
    }

    const KNOWN: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

    fn asked<'a>(
        node: u32,
        members: &'a [Member],
        rejoin: bool,
        quarantine: Option<Option<&'a DivergenceEvidence>>,
    ) -> Asked<'a> {
        Asked {
            node: NodeId(node),
            owner: NodeId(1),
            members,
            known: &KNOWN,
            rejoin,
            quarantine,
        }
    }

    fn guard(result: Result<RebuildMode, Refused>) -> &'static str {
        result.expect_err("refused").guard
    }

    #[test]
    fn b3_modes_follow_what_the_commit_pins() {
        let members = rf3();
        let all = |_: &Member| true;
        assert_eq!(
            decide(&asked(3, &members, false, None), all),
            Ok(RebuildMode::Pinned)
        );
        let two = &members[..2];
        assert_eq!(
            decide(&asked(3, two, false, None), all),
            Ok(RebuildMode::NotPinned)
        );
    }

    #[test]
    fn b3_g1_refuses_a_node_outside_the_membership() {
        let members = rf3();
        let refused = decide(&asked(4, &members, false, None), |_| true);
        assert_eq!(guard(refused), "G1");
    }

    #[test]
    fn b3_g2_refuses_the_owner_and_names_promote() {
        let members = rf3();
        let refused = decide(&asked(1, &members, false, None), |_| true).expect_err("owner");
        assert_eq!(refused.guard, "G2");
        assert!(
            refused.detail.contains("admin promote"),
            "{}",
            refused.detail
        );
    }

    #[test]
    fn b3_g3_refuses_while_a_replan_runs() {
        let members = rf3();
        assert_eq!(
            guard(decide(&asked(3, &members, true, None), |_| true)),
            "G3"
        );
    }

    #[test]
    fn b3_g4_refuses_the_sole_other_regular_copy() {
        // DegradedRf2: owner and node 3 only, node 2 back as a shadow L1 never counts.
        let members = vec![
            member(1, ReplicaRole::Primary),
            member(2, ReplicaRole::Shadow),
            member(3, ReplicaRole::RegularSecondary),
        ];
        assert_eq!(
            guard(decide(&asked(3, &members, false, None), |_| true)),
            "G4"
        );
        let members = vec![
            member(1, ReplicaRole::Primary),
            member(3, ReplicaRole::RegularSecondary),
        ];
        assert_eq!(
            guard(decide(&asked(3, &members, false, None), |_| true)),
            "G4"
        );
        // RF3, but node 2 is not current: dropping 3 would leave the owner alone.
        let members = rf3();
        let current = |m: &Member| m.node != NodeId(2);
        assert_eq!(
            guard(decide(&asked(3, &members, false, None), current)),
            "G4"
        );
    }

    /// G6 maps the evidence's copy ids to nodes through the pinned members, on the real map
    /// (`admin::members`: copy = node - 1). Mixed cases (critic-65 F3): evidence naming copy 2
    /// is node 3, never node 2, and evidence naming copy 1 is node 2. Cut: compare
    /// `CopyId(node)` and the mixed asserts go red.
    #[test]
    fn b3_g6_quarantine_rebuilds_only_a_named_copy() {
        let members = rf3();
        let real: Vec<(CopyId, NodeId)> = crate::admin::members()
            .iter()
            .map(|m| (m.copy, m.node))
            .collect();
        let fixture: Vec<(CopyId, NodeId)> = members.iter().map(|m| (m.copy, m.node)).collect();
        assert_eq!(fixture, real, "the fixture uses the real copy ids");
        let pairwise = DivergenceEvidence::Pairwise {
            seq: Seq(5),
            a: (CopyId(0), Digest::ROOT),
            b: (CopyId(2), Digest::ROOT),
        };
        let root = |copy| DivergenceEvidence::RootMismatch {
            copy: CopyId(copy),
            base_seq: Seq(1),
            expected: Digest::ROOT,
            found: None,
        };
        let (copy2, copy1) = (root(2), root(1));
        assert_eq!(
            decide(&asked(3, &members, false, Some(Some(&copy2))), |_| true),
            Ok(RebuildMode::Quarantined),
            "copy 2 is node 3"
        );
        assert_eq!(
            guard(decide(
                &asked(2, &members, false, Some(Some(&copy2))),
                |_| true
            )),
            "G6",
            "copy 2 is not node 2"
        );
        assert_eq!(
            decide(&asked(2, &members, false, Some(Some(&copy1))), |_| true),
            Ok(RebuildMode::Quarantined),
            "copy 1 is node 2"
        );
        assert_eq!(
            guard(decide(
                &asked(3, &members, false, Some(Some(&copy1))),
                |_| true
            )),
            "G6",
            "copy 1 is not node 3"
        );
        assert_eq!(
            decide(&asked(3, &members, true, Some(Some(&pairwise))), |_| true),
            Ok(RebuildMode::Quarantined)
        );
        assert_eq!(
            guard(decide(
                &asked(2, &members, true, Some(Some(&pairwise))),
                |_| true
            )),
            "G6"
        );
        assert_eq!(
            guard(decide(&asked(3, &members, false, Some(None)), |_| true)),
            "G6"
        );
    }

    #[test]
    fn b3_g5_needs_a_serving_commit_without_n() {
        let members = rf3();
        let without = &members[..2];
        assert!(drop_landed(NodeId(3), &PartitionMode::DegradedRf2, without).is_ok());
        assert!(drop_landed(NodeId(3), &PartitionMode::Active, without).is_ok());
        let read_only = drop_landed(NodeId(3), &PartitionMode::ReadOnly, without);
        let refused = read_only.expect_err("read-only");
        assert_eq!(refused.guard, "G5");
        assert!(
            refused.detail.contains("restart node 3 to re-add"),
            "{}",
            refused.detail
        );
        assert_eq!(
            drop_landed(NodeId(3), &PartitionMode::Active, &members)
                .expect_err("pinned")
                .guard,
            "G5"
        );
    }

    #[test]
    fn b3_m5_fence_refuses_a_stale_wipe() {
        let members: Vec<RecordMember> = rf3()[..2].iter().map(RecordMember::of).collect();
        let named: Vec<RecordMember> = rf3().iter().map(RecordMember::of).collect();
        let n = NodeId(3);
        let same = Some((BootId(7), BootId(7)));
        let fence = |generation, members: Option<&[RecordMember]>, boots| {
            fence_refusal(n, Generation(4), (Generation(generation), members), boots)
        };
        assert_eq!(fence(4, Some(&members), same), None);
        assert_eq!(fence(4, Some(&members), None), None, "lr: no boot pair");
        let restarted = fence(4, Some(&members), Some((BootId(7), BootId(8))));
        assert!(restarted.expect("boot").detail.contains("restarted"));
        let behind = fence(3, Some(&members), same);
        assert!(behind.expect("generation").detail.contains("below"));
        let readded = fence(5, Some(&named), same);
        assert!(readded
            .expect("members")
            .detail
            .contains("pins node 3 again"));
        assert!(
            fence(5, Some(&named), None).is_some(),
            "lr: the record still names 3"
        );
        assert_eq!(fence(4, None, same).expect("layout 3").guard, "M5");
    }
}
