//! The placement planner's V9 Form line claims (M11 WS E), checked by an oracle written here,
//! not by the planner's own helpers:
//!
//! * no step breaks a hard rule (machine, failure domain when feasible, target state, primary
//!   eligibility);
//! * copy and primary skew end at most 1 on homogeneous feasible inventories;
//! * every claim left unmet is named by a residual;
//! * at most K steps.
//!
//! Seeded sweeps cover 3–50 nodes; the named rows each pin one guard or branch.

use std::collections::{BTreeMap, BTreeSet};

use config_log::retcd_test;
use rdb_core::placement::{
    plan, Cause, Inventory, InventoryError, Node, NodeState, Partition, Plan, Residual, Rule, Why,
};

// ---- building inventories ----

fn node(id: u32) -> Node {
    Node {
        id,
        machine: format!("m{id}"),
        domain: format!("d{id}"),
        state: NodeState::Ok,
        primary_eligible: true,
    }
}

fn part(id: u32, copies: &[u32], primary: u32) -> Partition {
    Partition {
        id,
        copies: copies.to_vec(),
        primary,
    }
}

fn inv(nodes: Vec<Node>, partitions: Vec<Partition>) -> Inventory {
    Inventory { nodes, partitions }
}

/// SplitMix64: the test's seeded generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u32) -> u32 {
        u32::try_from(self.next() % u64::from(n)).expect("below n")
    }
}

/// `count` partitions of three copies on distinct nodes, crowded onto the first few nodes.
fn crowded_partitions(rng: &mut Rng, nodes: u32, count: u32) -> Vec<Partition> {
    let crowd = 3.max(nodes / 4);
    (1..=count)
        .map(|id| {
            let mut copies = Vec::new();
            while copies.len() < 3 {
                let pool = if rng.below(4) == 0 { nodes } else { crowd };
                let n = 1 + rng.below(pool);
                if !copies.contains(&n) {
                    copies.push(n);
                }
            }
            let primary = copies[rng.below(3) as usize];
            part(id, &copies, primary)
        })
        .collect()
}

/// Homogeneous and feasible: distinct machines, every node ok and primary-eligible, and the
/// nodes split evenly over at least three domains.
fn homogeneous(seed: u64) -> Inventory {
    let mut rng = Rng(seed);
    let nodes = 3 + rng.below(48);
    let divisors: Vec<u32> = (3..=nodes).filter(|d| nodes.is_multiple_of(*d)).collect();
    let domains = divisors[rng.below(u32::try_from(divisors.len()).expect("few")) as usize];
    let list = (1..=nodes)
        .map(|id| Node {
            domain: format!("d{}", id % domains),
            ..node(id)
        })
        .collect();
    let count = 1 + rng.below(3 * nodes);
    inv(list, crowded_partitions(&mut rng, nodes, count))
}

/// Anything legal: mixed states, eligibility, shared machines, few domains.
fn messy(seed: u64) -> Inventory {
    let mut rng = Rng(seed);
    let nodes = 3 + rng.below(48);
    let machines = 1 + rng.below(nodes);
    let domains = 1 + rng.below(nodes);
    let list = (1..=nodes)
        .map(|id| Node {
            id,
            machine: format!("m{}", rng.below(machines.max(nodes / 2).max(3))),
            domain: format!("d{}", rng.below(domains)),
            state: match rng.below(10) {
                0 => NodeState::Draining,
                1 => NodeState::Full,
                2 => NodeState::Unhealthy,
                _ => NodeState::Ok,
            },
            primary_eligible: rng.below(5) != 0,
        })
        .collect();
    let count = 1 + rng.below(3 * nodes);
    inv(list, crowded_partitions(&mut rng, nodes, count))
}

// ---- the oracle ----

fn nodes_of(inventory: &Inventory) -> BTreeMap<u32, &Node> {
    inventory.nodes.iter().map(|n| (n.id, n)).collect()
}

const fn keeps(state: NodeState) -> bool {
    matches!(state, NodeState::Ok | NodeState::Full)
}

/// Usable domains for these copies: those of ok nodes and of full nodes holding one.
fn usable_domains(nodes: &BTreeMap<u32, &Node>, copies: &[u32]) -> usize {
    nodes
        .values()
        .filter(|n| {
            n.state == NodeState::Ok || (n.state == NodeState::Full && copies.contains(&n.id))
        })
        .map(|n| n.domain.clone())
        .collect::<BTreeSet<_>>()
        .len()
}

/// Every partition whose copies break a hard rule in `inventory`, with what is broken.
fn broken(inventory: &Inventory) -> BTreeMap<u32, BTreeSet<&'static str>> {
    let nodes = nodes_of(inventory);
    let mut out: BTreeMap<u32, BTreeSet<&'static str>> = BTreeMap::new();
    for p in &inventory.partitions {
        let mut add = |what| {
            out.entry(p.id).or_default().insert(what);
        };
        let primary = nodes[&p.primary];
        if !primary.primary_eligible || !keeps(primary.state) {
            add("primary");
        }
        for c in &p.copies {
            if !keeps(nodes[c].state) {
                add("state");
            }
        }
        let machines: BTreeSet<_> = p.copies.iter().map(|c| &nodes[c].machine).collect();
        if machines.len() < p.copies.len() {
            add("machine");
        }
        let domains: BTreeSet<_> = p.copies.iter().map(|c| &nodes[c].domain).collect();
        if domains.len() < p.copies.len() && usable_domains(&nodes, &p.copies) >= p.copies.len() {
            add("domain");
        }
    }
    out
}

fn skew(counts: &BTreeMap<u32, usize>) -> usize {
    counts.values().max().unwrap_or(&0) - counts.values().min().unwrap_or(&0)
}

fn copy_counts(inventory: &Inventory) -> BTreeMap<u32, usize> {
    let mut counts: BTreeMap<u32, usize> = inventory
        .nodes
        .iter()
        .filter(|n| keeps(n.state))
        .map(|n| (n.id, 0))
        .collect();
    for p in &inventory.partitions {
        for c in &p.copies {
            if let Some(n) = counts.get_mut(c) {
                *n += 1;
            }
        }
    }
    counts
}

fn primary_counts(inventory: &Inventory) -> BTreeMap<u32, usize> {
    let mut counts: BTreeMap<u32, usize> = inventory
        .nodes
        .iter()
        .filter(|n| n.primary_eligible && keeps(n.state))
        .map(|n| (n.id, 0))
        .collect();
    for p in &inventory.partitions {
        if let Some(n) = counts.get_mut(&p.primary) {
            *n += 1;
        }
    }
    counts
}

/// Replays `plan` on `input`, failing on the first step that breaks a hard rule, and returns the
/// inventory after it. Checks the replay equals `plan.after` and the step count is within `k`.
fn replay(input: &Inventory, plan: &Plan, k: usize, label: &str) -> Inventory {
    assert!(
        plan.steps.len() <= k,
        "{label}: {} steps over K={k}",
        plan.steps.len()
    );
    let mut now = input.clone();
    let nodes = nodes_of(input);
    for (i, step) in plan.steps.iter().enumerate() {
        let p = now
            .partitions
            .iter_mut()
            .find(|p| p.id == step.partition)
            .expect("planned partition exists");
        let to = nodes[&step.to];
        let at = format!("{label}: step {i} {step:?}");
        assert!(p.copies.contains(&step.from), "{at}: source holds no copy");
        if step.primary {
            assert_eq!(p.primary, step.from, "{at}: source is not the primary");
            assert!(
                p.copies.contains(&step.to),
                "{at}: primary target holds no copy"
            );
            assert!(
                to.primary_eligible,
                "{at}: primary target not primary-eligible"
            );
            assert!(keeps(to.state), "{at}: primary target is {:?}", to.state);
            p.primary = step.to;
        } else {
            assert_ne!(
                p.primary, step.from,
                "{at}: a copy move takes the primary's copy"
            );
            assert_eq!(
                to.state,
                NodeState::Ok,
                "{at}: copy target is {:?}",
                to.state
            );
            let others: Vec<u32> = p
                .copies
                .iter()
                .copied()
                .filter(|&c| c != step.from)
                .collect();
            assert!(
                !others.contains(&step.to),
                "{at}: target already holds a copy"
            );
            assert!(
                others.iter().all(|c| nodes[c].machine != to.machine),
                "{at}: target shares a machine with another copy"
            );
            let mut after = others.clone();
            after.push(step.to);
            if usable_domains(&nodes, &after) >= after.len() {
                assert!(
                    others.iter().all(|c| nodes[c].domain != to.domain),
                    "{at}: target shares a failure domain while spread is feasible"
                );
            }
            for c in &mut p.copies {
                if *c == step.from {
                    *c = step.to;
                }
            }
        }
    }
    assert_eq!(
        now, plan.after,
        "{label}: plan.after is not the replayed inventory"
    );
    now
}

/// Every claim the final inventory leaves unmet is named by a residual.
fn unmet_claims_are_named(after: &Inventory, plan: &Plan, label: &str) {
    let named_partitions: BTreeSet<u32> = plan
        .residuals
        .iter()
        .filter_map(|r| match r {
            Residual::Unrepaired { partition, .. } => Some(*partition),
            _ => None,
        })
        .collect();
    for (partition, what) in broken(after) {
        assert!(
            named_partitions.contains(&partition),
            "{label}: partition {partition} breaks {what:?} and no residual names it: {:?}",
            plan.residuals
        );
    }
    let nodes = nodes_of(after);
    for p in &after.partitions {
        if usable_domains(&nodes, &p.copies) < p.copies.len() {
            assert!(
                plan.residuals.iter().any(
                    |r| matches!(r, Residual::DomainSpread { partition, .. } if *partition == p.id)
                ),
                "{label}: partition {} cannot spread and no residual says so",
                p.id
            );
        }
    }
    if skew(&copy_counts(after)) > 1 {
        assert!(
            plan.residuals
                .iter()
                .any(|r| matches!(r, Residual::CopySkew { .. })),
            "{label}: copy skew {} unnamed",
            skew(&copy_counts(after))
        );
    }
    if skew(&primary_counts(after)) > 1 {
        assert!(
            plan.residuals
                .iter()
                .any(|r| matches!(r, Residual::PrimarySkew { .. })),
            "{label}: primary skew {} unnamed",
            skew(&primary_counts(after))
        );
    }
}

fn checked(input: &Inventory, k: usize, label: &str) -> (Plan, Inventory) {
    let plan = plan(input, k).unwrap_or_else(|e| panic!("{label}: refused: {e}"));
    let after = replay(input, &plan, k, label);
    unmet_claims_are_named(&after, &plan, label);
    (plan, after)
}

// ---- seeded sweeps ----

#[retcd_test]
fn homogeneous_feasible_inventories_end_with_no_violation_and_skew_at_most_one() {
    let mut sizes = BTreeSet::new();
    for seed in 0..30 {
        let input = homogeneous(seed);
        sizes.insert(input.nodes.len());
        let label = format!("seed {seed} ({} nodes)", input.nodes.len());
        let (plan, after) = checked(&input, usize::MAX, &label);
        assert!(broken(&after).is_empty(), "{label}: {:?}", broken(&after));
        assert!(skew(&copy_counts(&after)) <= 1, "{label}: copy skew");
        assert!(skew(&primary_counts(&after)) <= 1, "{label}: primary skew");
        assert!(plan.residuals.is_empty(), "{label}: {:?}", plan.residuals);
    }
    let (small, large) = (sizes.first().copied(), sizes.last().copied());
    assert!(
        small <= Some(6) && large >= Some(45),
        "sweep sizes {sizes:?}"
    );
}

#[retcd_test]
fn any_inventory_any_budget_gets_legal_steps_and_names_every_unmet_claim() {
    for seed in 0..60 {
        let input = messy(seed);
        let k = [0, 1, 3, 10, usize::MAX][usize::try_from(seed % 5).expect("small")];
        checked(&input, k, &format!("seed {seed} K={k}"));
    }
}

#[retcd_test]
fn a_plan_is_the_same_for_the_same_input_whatever_the_node_order() {
    for seed in 0..10 {
        let input = messy(seed);
        let mut reversed = input.clone();
        reversed.nodes.reverse();
        let a = plan(&input, usize::MAX).expect("plans");
        let b = plan(&reversed, usize::MAX).expect("plans");
        assert_eq!(a.steps, b.steps, "seed {seed}");
        assert_eq!(a.residuals, b.residuals, "seed {seed}");
    }
}

// ---- named rows: one guard or branch each ----

/// Empty nodes 4 (draining), 5 (full) and 6 (unhealthy) are the least loaded, and node 7 is ok.
#[retcd_test]
fn a_copy_moves_only_to_an_ok_node() {
    let mut nodes: Vec<Node> = (1..=7).map(node).collect();
    nodes[3].state = NodeState::Draining;
    nodes[4].state = NodeState::Full;
    nodes[5].state = NodeState::Unhealthy;
    let input = inv(
        nodes,
        (1..=4).map(|p| part(p, &[1, 2, 3], 1 + p % 3)).collect(),
    );
    let (plan, _) = checked(&input, usize::MAX, "states");
    assert!(plan.steps.iter().any(|s| !s.primary), "{:?}", plan.steps);
    assert!(
        plan.steps.iter().all(|s| s.primary || s.to == 7),
        "{:?}",
        plan.steps
    );
}

/// Node 4 is empty but on node 1's machine; node 5 is empty on its own machine.
#[retcd_test]
fn a_copy_never_lands_on_a_machine_another_copy_uses() {
    let mut nodes: Vec<Node> = (1..=5).map(node).collect();
    nodes[3].machine = "m1".to_owned();
    let input = inv(
        nodes,
        (1..=4).map(|p| part(p, &[1, 2, 3], 1 + p % 3)).collect(),
    );
    let (plan, _) = checked(&input, usize::MAX, "machine");
    let into_4: Vec<_> = plan
        .steps
        .iter()
        .filter(|s| !s.primary && s.to == 4)
        .collect();
    assert!(into_4.iter().all(|s| s.from == 1), "{into_4:?}");
}

/// Nodes 1 and 2 share machine m1 but sit in different domains; node 4 is free.
#[retcd_test]
fn a_shared_machine_is_repaired_by_moving_the_non_primary_copy() {
    let mut nodes: Vec<Node> = (1..=4).map(node).collect();
    nodes[1].machine = "m1".to_owned();
    let input = inv(nodes, vec![part(1, &[1, 2, 3], 1)]);
    let (plan, after) = checked(&input, usize::MAX, "shared machine");
    assert_eq!(plan.steps[0].why, Why::Repair(Rule::SameMachine));
    assert_eq!((plan.steps[0].from, plan.steps[0].to), (2, 4));
    assert!(broken(&after).is_empty());
}

/// Nodes 1 and 2 are both in domain "a"; node 4 is in its own domain and is the only fix.
#[retcd_test]
fn a_shared_domain_is_repaired_when_enough_domains_exist() {
    let mut nodes: Vec<Node> = (1..=5).map(node).collect();
    nodes[1].domain = "d1".to_owned();
    nodes[4].domain = "d3".to_owned();
    let input = inv(nodes, vec![part(1, &[1, 2, 3], 1)]);
    let (plan, after) = checked(&input, usize::MAX, "shared domain");
    assert_eq!(plan.steps[0].why, Why::Repair(Rule::SameDomain));
    assert_eq!((plan.steps[0].from, plan.steps[0].to), (2, 4));
    assert!(broken(&after).is_empty());
}

#[retcd_test]
fn a_draining_or_unhealthy_node_is_emptied_primary_first() {
    let mut nodes: Vec<Node> = (1..=6).map(node).collect();
    nodes[0].state = NodeState::Draining;
    nodes[1].state = NodeState::Unhealthy;
    let input = inv(nodes, (1..=3).map(|p| part(p, &[1, 2, 3], 1)).collect());
    let (plan, after) = checked(&input, usize::MAX, "drain");
    assert!(after
        .partitions
        .iter()
        .all(|p| !p.copies.contains(&1) && !p.copies.contains(&2)));
    let first = plan.steps[0];
    assert!(first.primary && first.from == 1, "{first:?}");
    assert_eq!(first.why, Why::Repair(Rule::NodeState(NodeState::Draining)));
    assert!(plan.residuals.is_empty(), "{:?}", plan.residuals);
}

/// Only node 4 may be primary, and partition 1 holds no copy there: a copy moves first.
#[retcd_test]
fn a_primary_moves_only_to_a_primary_eligible_node() {
    let nodes: Vec<Node> = (1..=6)
        .map(|id| Node {
            primary_eligible: id == 4,
            ..node(id)
        })
        .collect();
    let input = inv(nodes, vec![part(1, &[1, 2, 3], 1), part(2, &[4, 5, 6], 5)]);
    let (plan, after) = checked(&input, usize::MAX, "eligible");
    assert!(after.partitions.iter().all(|p| p.primary == 4), "{after:?}");
    assert!(
        plan.steps.iter().any(|s| s.why == Why::PrimaryRoom),
        "{:?}",
        plan.steps
    );
}

/// Node 2 is primary-eligible but draining: the primary goes to node 3, never node 2.
#[retcd_test]
fn a_primary_never_moves_to_a_draining_holder() {
    let mut nodes: Vec<Node> = (1..=4).map(node).collect();
    nodes[0].primary_eligible = false;
    nodes[1].state = NodeState::Draining;
    let input = inv(nodes, vec![part(1, &[1, 2, 3], 1)]);
    let (plan, after) = checked(&input, usize::MAX, "draining holder");
    assert!(
        plan.steps.iter().all(|s| !s.primary || s.to != 2),
        "{:?}",
        plan.steps
    );
    assert_eq!(after.partitions[0].primary, 3);
}

#[retcd_test]
fn three_nodes_with_every_primary_on_one_node_spread_the_primaries() {
    let input = inv(
        (1..=3).map(node).collect(),
        (1..=6).map(|p| part(p, &[1, 2, 3], 1)).collect(),
    );
    let (plan, after) = checked(&input, usize::MAX, "three");
    assert!(plan
        .steps
        .iter()
        .all(|s| s.primary && s.why == Why::PrimarySkew));
    assert_eq!(skew(&primary_counts(&after)), 0);
}

/// Every node is full, so no copy moves. Node 1 leads two partitions whose only other holder is
/// node 2, which already leads one; only a chain (node 2 hands its partition to node 3, then node 1
/// hands one to node 2) narrows primary skew.
#[retcd_test]
fn primaries_move_along_a_chain_when_no_direct_promote_narrows_skew() {
    let nodes: Vec<Node> = (1..=3)
        .map(|id| Node {
            state: NodeState::Full,
            ..node(id)
        })
        .collect();
    let input = inv(
        nodes,
        vec![
            part(1, &[1, 2], 1),
            part(2, &[1, 2], 1),
            part(3, &[2, 3], 2),
        ],
    );
    let (plan, after) = checked(&input, usize::MAX, "chain");
    assert_eq!(skew(&primary_counts(&after)), 0, "{:?}", plan.steps);
    assert_eq!(plan.steps.len(), 2, "{:?}", plan.steps);
}

/// One domain for five nodes: spread is waived and named, but copy and primary skew still close,
/// which needs the copy and primary phases to alternate (node 1 holds every primary at first).
#[retcd_test]
fn one_failure_domain_names_the_spread_and_still_balances() {
    let nodes: Vec<Node> = (1..=5)
        .map(|id| Node {
            domain: "rack".to_owned(),
            ..node(id)
        })
        .collect();
    let input = inv(nodes, (1..=4).map(|p| part(p, &[1, 2, 3], 1)).collect());
    let (plan, after) = checked(&input, usize::MAX, "one domain");
    let spread = plan
        .residuals
        .iter()
        .filter(|r| {
            matches!(
                r,
                Residual::DomainSpread {
                    domains: 1,
                    copies: 3,
                    ..
                }
            )
        })
        .count();
    assert_eq!(spread, 4, "{:?}", plan.residuals);
    assert!(skew(&copy_counts(&after)) <= 1, "{:?}", copy_counts(&after));
    assert!(
        skew(&primary_counts(&after)) <= 1,
        "{:?}",
        primary_counts(&after)
    );
}

#[retcd_test]
fn an_unrepairable_shared_machine_is_named_with_no_legal_move() {
    let mut nodes: Vec<Node> = (1..=3).map(node).collect();
    nodes[1].machine = "m1".to_owned();
    let input = inv(nodes, vec![part(1, &[1, 2, 3], 1)]);
    let (plan, _) = checked(&input, usize::MAX, "stuck machine");
    assert!(plan.steps.is_empty());
    assert_eq!(
        plan.residuals,
        vec![Residual::Unrepaired {
            partition: 1,
            node: 2,
            rule: Rule::SameMachine,
            cause: Cause::NoLegalMove,
        }]
    );
}

#[retcd_test]
fn the_move_budget_caps_the_plan_and_is_named() {
    let mut nodes: Vec<Node> = (1..=6).map(node).collect();
    nodes[0].state = NodeState::Draining;
    let input = inv(nodes, (1..=4).map(|p| part(p, &[1, 2, 3], 2)).collect());
    let (plan, _) = checked(&input, 1, "K=1");
    assert_eq!(plan.steps.len(), 1);
    assert!(
        plan.residuals.iter().any(|r| matches!(
            r,
            Residual::Unrepaired {
                cause: Cause::Budget,
                ..
            }
        )),
        "{:?}",
        plan.residuals
    );
    let (none, _) = checked(&input, 0, "K=0");
    assert!(none.steps.is_empty());
}

/// Node 3 is full and empty, so it can take neither copies nor primaries: both skews stay.
#[retcd_test]
fn skew_that_no_legal_move_narrows_is_named() {
    let mut nodes: Vec<Node> = (1..=4).map(node).collect();
    nodes[2].state = NodeState::Full;
    let input = inv(nodes, (1..=4).map(|p| part(p, &[1, 2, 4], 1)).collect());
    let (plan, _) = checked(&input, usize::MAX, "full and empty");
    assert!(plan.residuals.contains(&Residual::CopySkew {
        skew: 4,
        cause: Cause::NoLegalMove
    }));
    assert!(plan.residuals.iter().any(|r| matches!(
        r,
        Residual::PrimarySkew {
            cause: Cause::NoLegalMove,
            ..
        }
    )));
}

// ---- input ----

#[retcd_test]
fn an_inventory_that_does_not_add_up_is_refused() {
    let one = || vec![node(1), node(2)];
    let cases = [
        (inv(vec![], vec![]), InventoryError::NoNodes),
        (
            inv(vec![node(1), node(1)], vec![]),
            InventoryError::DuplicateNode(1),
        ),
        (
            inv(one(), vec![part(1, &[1], 1), part(1, &[2], 2)]),
            InventoryError::DuplicatePartition(1),
        ),
        (
            inv(one(), vec![part(1, &[], 1)]),
            InventoryError::NoCopies(1),
        ),
        (
            inv(one(), vec![part(1, &[1, 9], 1)]),
            InventoryError::UnknownNode {
                partition: 1,
                node: 9,
            },
        ),
        (
            inv(one(), vec![part(1, &[1, 1], 1)]),
            InventoryError::RepeatedCopy {
                partition: 1,
                node: 1,
            },
        ),
        (
            inv(one(), vec![part(1, &[1], 2)]),
            InventoryError::PrimaryNotACopy {
                partition: 1,
                primary: 2,
            },
        ),
    ];
    for (input, want) in cases {
        assert_eq!(plan(&input, usize::MAX), Err(want));
    }
}

/// The file format `rdb_dev admin plan` reads: every field required, no unknown field.
#[retcd_test]
fn the_inventory_file_format_reads_and_refuses_unknown_fields() {
    let text = r#"{"nodes": [{"id": 1, "machine": "m1", "domain": "rack-a", "state": "draining", "primary_eligible": false}],
                   "partitions": [{"id": 7, "copies": [1], "primary": 1}]}"#;
    let read: Inventory = serde_json::from_str(text).expect("reads");
    assert_eq!(read.nodes[0].state, NodeState::Draining);
    assert!(!read.nodes[0].primary_eligible);
    assert_eq!(read.partitions[0], part(7, &[1], 1));
    let extra = text.replace("\"id\": 7", "\"id\": 7, \"bytes\": 10");
    assert!(serde_json::from_str::<Inventory>(&extra).is_err());
    let missing = text.replace(", \"primary_eligible\": false", "");
    assert!(serde_json::from_str::<Inventory>(&missing).is_err());
    assert!(
        serde_json::from_str::<Inventory>(r#"{"nodes": [], "partitions": [], "x": 1}"#).is_err()
    );
}

/// No node may be primary, so the primary rule has no legal fix; node 4's draining copy does.
/// With K=0 only the second is the budget's fault.
#[retcd_test]
fn a_budget_residual_is_named_only_where_a_legal_move_exists() {
    let mut nodes: Vec<Node> = (1..=5)
        .map(|id| Node {
            primary_eligible: false,
            ..node(id)
        })
        .collect();
    nodes[3].state = NodeState::Draining;
    let input = inv(nodes, vec![part(1, &[1, 2, 3], 1), part(2, &[1, 2, 4], 1)]);
    let (plan, _) = checked(&input, 0, "K=0 mixed");
    let unrepaired = |partition, node, rule, cause| Residual::Unrepaired {
        partition,
        node,
        rule,
        cause,
    };
    for want in [
        unrepaired(1, 1, Rule::PrimaryNotEligible, Cause::NoLegalMove),
        unrepaired(2, 1, Rule::PrimaryNotEligible, Cause::NoLegalMove),
        unrepaired(2, 4, Rule::NodeState(NodeState::Draining), Cause::Budget),
    ] {
        assert!(
            plan.residuals.contains(&want),
            "{want:?} not in {:?}",
            plan.residuals
        );
    }
}

#[retcd_test]
fn skew_cut_short_by_the_budget_names_the_budget() {
    let crowded = inv(
        (1..=6).map(node).collect(),
        (1..=6).map(|p| part(p, &[1, 2, 3], 1)).collect(),
    );
    let (plan, _) = checked(&crowded, 1, "copy K=1");
    assert!(
        plan.residuals.iter().any(|r| matches!(
            r,
            Residual::CopySkew {
                cause: Cause::Budget,
                ..
            }
        )),
        "{:?}",
        plan.residuals
    );
    let primaries = inv(
        (1..=3).map(node).collect(),
        (1..=6).map(|p| part(p, &[1, 2, 3], 1)).collect(),
    );
    let (plan, _) = checked(&primaries, 1, "primary K=1");
    assert!(
        plan.residuals.iter().any(|r| matches!(
            r,
            Residual::PrimarySkew {
                cause: Cause::Budget,
                ..
            }
        )),
        "{:?}",
        plan.residuals
    );
}

/// Node 5 is empty and shares domain d1 with node 1, which leads every partition, so no copy may
/// move there from nodes 2 or 3 while five domains make spread feasible.
#[retcd_test]
fn a_copy_never_lands_in_a_failure_domain_another_copy_uses_when_spread_is_feasible() {
    let mut nodes: Vec<Node> = (1..=5).map(node).collect();
    nodes[4].domain = "d1".to_owned();
    let input = inv(nodes, (1..=4).map(|p| part(p, &[1, 2, 3], 1)).collect());
    let (plan, _) = checked(&input, usize::MAX, "domain target");
    let into_5: Vec<_> = plan
        .steps
        .iter()
        .filter(|s| !s.primary && s.to == 5)
        .collect();
    assert!(into_5.iter().all(|s| s.from == 1), "{into_5:?}");
}
