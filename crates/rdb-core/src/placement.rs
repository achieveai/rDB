//! The placement planner (M11, V9 Form line): **model only**.
//!
//! A pure function from an [`Inventory`] (nodes and where each partition's copies and primary
//! sit) and a move budget to an ordered list of [`Step`]s and the [`Residual`]s it could not
//! clear. It reads no clock, opens no file and talks to no cluster; the same input gives the same
//! plan, byte for byte. `rdb_dev admin plan <file>` reads the inventory from a JSON file and
//! prints the plan. Nothing applies it: the operator runs each line by hand.
//!
//! What it claims (V9 Form line), and nothing more:
//!
//! * no step breaks a hard rule: a copy moves only to a node in state [`NodeState::Ok`] that
//!   holds no copy of that partition, on a machine no other copy uses, and in a failure domain no
//!   other copy uses whenever enough domains exist; a primary moves only to a primary-eligible
//!   node that holds a copy and is not draining or unhealthy;
//! * hard rules the input already breaks are repaired when a legal step exists;
//! * copy-count skew and primary-count skew end at most 1 when that is reachable;
//! * every claim left unmet ends as a named [`Residual`];
//! * at most `max_moves` steps.
//!
//! Not claimed: shadows, bytes or CPU overrides (there is no data source for them), automatic
//! rebalancing.
//!
//! The order follows spec §9.2: hard rules first, then copy counts, then primary counts. A tie
//! between equally good targets is broken by a rendezvous score of (partition, node), so a plan
//! does not depend on the order the file lists nodes in.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

/// A node's service state, as the inventory says it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    /// In service: may take new copies.
    Ok,
    /// Being emptied: its copies must leave and it takes none.
    Draining,
    /// Out of space: keeps its copies, takes none.
    Full,
    /// Failing: its copies must leave and it takes none.
    Unhealthy,
}

impl NodeState {
    /// Whether a copy may stay on a node in this state.
    #[must_use]
    pub const fn keeps_copies(self) -> bool {
        matches!(self, Self::Ok | Self::Full)
    }

    /// The state as the inventory file spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Draining => "draining",
            Self::Full => "full",
            Self::Unhealthy => "unhealthy",
        }
    }
}

/// One node of the inventory.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    /// The node's number.
    pub id: u32,
    /// The physical machine. Two copies of one partition never share one.
    pub machine: String,
    /// The failure domain (rack, zone). Copies of one partition spread across these when enough
    /// usable domains exist.
    pub domain: String,
    /// Service state.
    pub state: NodeState,
    /// Whether this node may hold a partition's primary.
    pub primary_eligible: bool,
}

/// One partition of the inventory: where its copies are and which one is primary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Partition {
    /// The partition's number.
    pub id: u32,
    /// The nodes holding a copy.
    pub copies: Vec<u32>,
    /// The node holding the primary; one of `copies`.
    pub primary: u32,
}

/// The planner's whole input.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    /// Every node.
    pub nodes: Vec<Node>,
    /// Every partition.
    pub partitions: Vec<Partition>,
}

/// An inventory the planner refuses before planning anything.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InventoryError {
    /// No nodes at all.
    #[error("the inventory has no nodes")]
    NoNodes,
    /// Two nodes share one number.
    #[error("node {0} is listed twice")]
    DuplicateNode(u32),
    /// Two partitions share one number.
    #[error("partition {0} is listed twice")]
    DuplicatePartition(u32),
    /// A partition with no copy.
    #[error("partition {0} has no copies")]
    NoCopies(u32),
    /// A copy on a node the inventory does not list.
    #[error("partition {partition} has a copy on node {node}, which is not in the node list")]
    UnknownNode {
        /// The partition.
        partition: u32,
        /// The unlisted node.
        node: u32,
    },
    /// One node listed twice among a partition's copies.
    #[error("partition {partition} lists node {node} twice among its copies")]
    RepeatedCopy {
        /// The partition.
        partition: u32,
        /// The repeated node.
        node: u32,
    },
    /// The primary is not one of the copies.
    #[error("partition {partition}'s primary {primary} is not one of its copies")]
    PrimaryNotACopy {
        /// The partition.
        partition: u32,
        /// The named primary.
        primary: u32,
    },
}

/// A hard rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Rule {
    /// A copy sits on a node in this state (draining or unhealthy) and must leave.
    NodeState(NodeState),
    /// Two copies sit on one machine.
    SameMachine,
    /// Two copies sit in one failure domain while enough usable domains exist to spread them.
    SameDomain,
    /// The primary sits on a node that may not hold it: not primary-eligible, or draining or
    /// unhealthy.
    PrimaryNotEligible,
}

impl Rule {
    /// The rule in plain words.
    #[must_use]
    pub fn describe(self) -> String {
        match self {
            Self::NodeState(state) => format!("the node is {}", state.name()),
            Self::SameMachine => "another copy is on the same machine".to_owned(),
            Self::SameDomain => "another copy is in the same failure domain".to_owned(),
            Self::PrimaryNotEligible => "the primary is on a node that may not hold it".to_owned(),
        }
    }
}

/// Why a step is in the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Why {
    /// It repairs a hard rule on its source node.
    Repair(Rule),
    /// It makes room for a primary: no copy holder may take the primary, so a copy moves to a
    /// node that may, which the next step promotes.
    PrimaryRoom,
    /// It narrows copy-count skew.
    CopySkew,
    /// It narrows primary-count skew.
    PrimarySkew,
}

impl Why {
    /// The reason in plain words.
    #[must_use]
    pub fn describe(self) -> String {
        match self {
            Self::Repair(rule) => format!("repair: {}", rule.describe()),
            Self::PrimaryRoom => "make room: no copy holder may take the primary".to_owned(),
            Self::CopySkew => "balance copy counts".to_owned(),
            Self::PrimarySkew => "balance primary counts".to_owned(),
        }
    }
}

/// One step of a plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    /// The partition.
    pub partition: u32,
    /// The source node.
    pub from: u32,
    /// The target node.
    pub to: u32,
    /// `false`: the copy on `from` moves to `to`; `from` is never the primary. `true`: the
    /// primary role moves from `from` to `to`, which already holds a copy; no data moves.
    pub primary: bool,
    /// Why.
    pub why: Why,
}

impl Step {
    /// The step as `rdb_dev admin plan` prints it, ending `# <why>`. Only a line that is a live
    /// command starts `rdb_dev`: a copy move of partition 1 prints as
    /// `rdb_dev admin move <from> <to>`. Two steps have no live command and print as `step:`: a
    /// copy move of another partition (today's CLI moves partition 1 only, critic A2), and a
    /// primary handoff, because an owner move is refused (D3).
    #[must_use]
    pub fn line(&self) -> String {
        let action = if self.primary {
            format!(
                "step: hand partition {}'s primary from node {} to node {} \
                 (no live command: an owner move is refused today)",
                self.partition, self.from, self.to
            )
        } else if self.partition == 1 {
            format!("rdb_dev admin move {} {}", self.from, self.to)
        } else {
            format!(
                "step: move partition {}'s copy from node {} to node {} \
                 (no live command: today's CLI moves partition 1 only)",
                self.partition, self.from, self.to
            )
        };
        format!("{action:<40} # {}", self.why.describe())
    }
}

/// Why a skew claim was left unmet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// The move budget ran out first.
    Budget,
    /// No legal step narrows it.
    NoLegalMove,
}

/// A claim the plan leaves unmet, named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Residual {
    /// The partition's copies cannot sit in distinct failure domains: fewer usable domains than
    /// copies. The domain rule is waived for it; the machine rule is not.
    DomainSpread {
        /// The partition.
        partition: u32,
        /// Usable domains: those of `ok` nodes and of `full` nodes already holding a copy.
        domains: usize,
        /// Its copies.
        copies: usize,
    },
    /// A hard rule the input broke is still broken after the plan.
    Unrepaired {
        /// The partition.
        partition: u32,
        /// The node the rule is broken on.
        node: u32,
        /// The rule.
        rule: Rule,
        /// Why it was not repaired.
        cause: Cause,
    },
    /// Copy-count skew above 1.
    CopySkew {
        /// max − min copies over nodes in service (`ok` or `full`).
        skew: usize,
        /// Why it stayed.
        cause: Cause,
    },
    /// Primary-count skew above 1.
    PrimarySkew {
        /// max − min primaries over primary-eligible nodes in service.
        skew: usize,
        /// Why it stayed.
        cause: Cause,
    },
}

impl Residual {
    /// The residual in plain words.
    #[must_use]
    pub fn describe(&self) -> String {
        let cause = |cause: &Cause| match cause {
            Cause::Budget => "the move budget ran out",
            Cause::NoLegalMove => "no legal move narrows it",
        };
        match self {
            Self::DomainSpread {
                partition,
                domains,
                copies,
            } => format!(
                "partition {partition}: {copies} copies cannot sit in distinct failure domains; \
                 only {domains} usable domain(s)"
            ),
            Self::Unrepaired {
                partition,
                node,
                rule,
                cause: why,
            } => match why {
                Cause::Budget => format!(
                    "partition {partition} on node {node}: {}; not repaired, {}",
                    rule.describe(),
                    cause(why)
                ),
                Cause::NoLegalMove => format!(
                    "partition {partition} on node {node}: {}; not repaired, no legal target",
                    rule.describe()
                ),
            },
            Self::CopySkew { skew, cause: why } => {
                format!("copy skew {skew} (target 1): {}", cause(why))
            }
            Self::PrimarySkew { skew, cause: why } => {
                format!("primary skew {skew} (target 1): {}", cause(why))
            }
        }
    }
}

/// A plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The steps, in the order to run them.
    pub steps: Vec<Step>,
    /// Claims left unmet.
    pub residuals: Vec<Residual>,
    /// The inventory as it stands after every step.
    pub after: Inventory,
}

/// Copy-count skew of `inventory`: max − min copies over nodes in service (`ok` or `full`).
/// 0 with no node in service.
#[must_use]
pub fn copy_skew(inventory: &Inventory) -> usize {
    skew(&copy_counts(inventory))
}

/// Primary-count skew of `inventory`: max − min primaries over primary-eligible nodes in service.
/// 0 with no such node.
#[must_use]
pub fn primary_skew(inventory: &Inventory) -> usize {
    skew(&primary_counts(inventory))
}

/// Plans at most `max_moves` steps over `inventory`.
///
/// # Errors
///
/// [`InventoryError`] when the inventory is not self-consistent; nothing is planned then.
pub fn plan(inventory: &Inventory, max_moves: usize) -> Result<Plan, InventoryError> {
    validate(inventory)?;
    let mut planner = Planner {
        nodes: inventory.nodes.iter().map(|n| (n.id, n.clone())).collect(),
        parts: inventory
            .partitions
            .iter()
            .map(|p| (p.id, (p.copies.clone(), p.primary)))
            .collect(),
        steps: Vec::new(),
        max_moves,
        out_of_budget: false,
        ok_domains: inventory
            .nodes
            .iter()
            .filter(|n| n.state == NodeState::Ok)
            .map(|n| n.domain.clone())
            .collect(),
    };
    planner.repair();
    // Copy moves change which nodes can take a primary, and spreading primaries frees copies to
    // move, so the two phases alternate until a round adds nothing. Neither undoes the other's
    // progress (each step lowers one sum of squared counts and leaves the other alone), so it ends.
    let (mut copy_cause, mut primary_cause);
    loop {
        let before = planner.steps.len();
        copy_cause = planner.balance_copies();
        primary_cause = planner.balance_primaries();
        if planner.steps.len() == before || planner.out_of_budget {
            break;
        }
    }
    let after = planner.inventory();
    let mut residuals = Vec::new();
    for (id, (copies, _)) in &planner.parts {
        let domains = planner.usable_domains(copies);
        if domains < copies.len() {
            residuals.push(Residual::DomainSpread {
                partition: *id,
                domains,
                copies: copies.len(),
            });
        }
    }
    for (id, (copies, primary)) in &planner.parts {
        for (node, rule) in planner.violations(copies, *primary) {
            let cause = if planner.out_of_budget && planner.repairable(*id, node) {
                Cause::Budget
            } else {
                Cause::NoLegalMove
            };
            residuals.push(Residual::Unrepaired {
                partition: *id,
                node,
                rule,
                cause,
            });
        }
    }
    let skew_now = copy_skew(&after);
    if skew_now > 1 {
        residuals.push(Residual::CopySkew {
            skew: skew_now,
            cause: copy_cause,
        });
    }
    let skew_now = primary_skew(&after);
    if skew_now > 1 {
        residuals.push(Residual::PrimarySkew {
            skew: skew_now,
            cause: primary_cause,
        });
    }
    Ok(Plan {
        steps: planner.steps,
        residuals,
        after,
    })
}

fn validate(inventory: &Inventory) -> Result<(), InventoryError> {
    if inventory.nodes.is_empty() {
        return Err(InventoryError::NoNodes);
    }
    let mut nodes = BTreeSet::new();
    for node in &inventory.nodes {
        if !nodes.insert(node.id) {
            return Err(InventoryError::DuplicateNode(node.id));
        }
    }
    let mut parts = BTreeSet::new();
    for part in &inventory.partitions {
        if !parts.insert(part.id) {
            return Err(InventoryError::DuplicatePartition(part.id));
        }
        if part.copies.is_empty() {
            return Err(InventoryError::NoCopies(part.id));
        }
        let mut seen = BTreeSet::new();
        for &node in &part.copies {
            if !nodes.contains(&node) {
                return Err(InventoryError::UnknownNode {
                    partition: part.id,
                    node,
                });
            }
            if !seen.insert(node) {
                return Err(InventoryError::RepeatedCopy {
                    partition: part.id,
                    node,
                });
            }
        }
        if !seen.contains(&part.primary) {
            return Err(InventoryError::PrimaryNotACopy {
                partition: part.id,
                primary: part.primary,
            });
        }
    }
    Ok(())
}

/// Copies per node in service (`ok` or `full`), by node.
#[must_use]
pub fn copy_counts(inventory: &Inventory) -> BTreeMap<u32, usize> {
    count_copies(
        &inventory.nodes,
        inventory.partitions.iter().flat_map(|p| &p.copies),
    )
}

/// Primaries per primary-eligible node in service, by node.
#[must_use]
pub fn primary_counts(inventory: &Inventory) -> BTreeMap<u32, usize> {
    count_primaries(
        &inventory.nodes,
        inventory.partitions.iter().map(|p| &p.primary),
    )
}

fn count_copies<'a>(
    nodes: impl IntoIterator<Item = &'a Node>,
    copies: impl IntoIterator<Item = &'a u32>,
) -> BTreeMap<u32, usize> {
    let mut counts: BTreeMap<u32, usize> = nodes
        .into_iter()
        .filter(|n| n.state.keeps_copies())
        .map(|n| (n.id, 0))
        .collect();
    for node in copies {
        if let Some(count) = counts.get_mut(node) {
            *count += 1;
        }
    }
    counts
}

fn count_primaries<'a>(
    nodes: impl IntoIterator<Item = &'a Node>,
    primaries: impl IntoIterator<Item = &'a u32>,
) -> BTreeMap<u32, usize> {
    let mut counts: BTreeMap<u32, usize> = nodes
        .into_iter()
        .filter(|n| n.primary_eligible && n.state.keeps_copies())
        .map(|n| (n.id, 0))
        .collect();
    for primary in primaries {
        if let Some(count) = counts.get_mut(primary) {
            *count += 1;
        }
    }
    counts
}

fn skew(counts: &BTreeMap<u32, usize>) -> usize {
    match (counts.values().max(), counts.values().min()) {
        (Some(max), Some(min)) => max - min,
        _ => 0,
    }
}

/// A stable pseudo-random score for (partition, node): the rendezvous tie-break. SplitMix64.
fn rendezvous(partition: u32, node: u32) -> u64 {
    let mut z =
        ((u64::from(partition) << 32) | u64::from(node)).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

struct Planner {
    nodes: BTreeMap<u32, Node>,
    /// Partition -> (copies, primary).
    parts: BTreeMap<u32, (Vec<u32>, u32)>,
    steps: Vec<Step>,
    max_moves: usize,
    /// Set when a step was wanted and the budget was spent.
    out_of_budget: bool,
    /// Domains of `ok` nodes: usable by every partition. States never change while planning.
    ok_domains: BTreeSet<String>,
}

impl Planner {
    fn inventory(&self) -> Inventory {
        Inventory {
            nodes: self.nodes.values().cloned().collect(),
            partitions: self
                .parts
                .iter()
                .map(|(id, (copies, primary))| Partition {
                    id: *id,
                    copies: copies.clone(),
                    primary: *primary,
                })
                .collect(),
        }
    }

    fn node(&self, id: u32) -> &Node {
        &self.nodes[&id]
    }

    /// Domains a partition with these copies may use: those of `ok` nodes, and of `full` nodes
    /// already holding one of them.
    fn usable_domains(&self, copies: &[u32]) -> usize {
        let full_only: BTreeSet<&str> = copies
            .iter()
            .map(|&c| self.node(c))
            .filter(|n| n.state == NodeState::Full && !self.ok_domains.contains(&n.domain))
            .map(|n| n.domain.as_str())
            .collect();
        self.ok_domains.len() + full_only.len()
    }

    /// Every hard rule these copies break, one entry per offending node. The primary is read
    /// first, so a machine or domain clash is charged to a non-primary copy.
    fn violations(&self, copies: &[u32], primary: u32) -> Vec<(u32, Rule)> {
        let spread = self.usable_domains(copies) >= copies.len();
        let mut out = Vec::new();
        let primary_node = self.node(primary);
        if !primary_node.primary_eligible || !primary_node.state.keeps_copies() {
            out.push((primary, Rule::PrimaryNotEligible));
        }
        let mut machines = BTreeSet::new();
        let mut domains = BTreeSet::new();
        let ordered =
            std::iter::once(primary).chain(copies.iter().copied().filter(|&c| c != primary));
        for id in ordered {
            let node = self.node(id);
            if !node.state.keeps_copies() {
                out.push((id, Rule::NodeState(node.state)));
            }
            let new_machine = machines.insert(node.machine.as_str());
            let new_domain = domains.insert(node.domain.as_str());
            if !new_machine {
                out.push((id, Rule::SameMachine));
            } else if spread && !new_domain {
                out.push((id, Rule::SameDomain));
            }
        }
        out
    }

    /// Whether the copy of `partition` on `from` may move to `to`.
    fn copy_target_ok(&self, partition: u32, from: u32, to: u32) -> bool {
        let (copies, _) = &self.parts[&partition];
        let target = self.node(to);
        // A node already holding a copy fails the machine rule below, so no separate check.
        if target.state != NodeState::Ok {
            return false;
        }
        let others: Vec<u32> = copies.iter().copied().filter(|&c| c != from).collect();
        if others
            .iter()
            .any(|&c| self.node(c).machine == target.machine)
        {
            return false;
        }
        let mut after = others.clone();
        after.push(to);
        if self.usable_domains(&after) >= after.len()
            && others.iter().any(|&c| self.node(c).domain == target.domain)
        {
            return false;
        }
        true
    }

    /// Whether `node` may hold a primary. Callers only offer nodes that hold a copy of the
    /// partition, and never its current primary.
    fn may_hold_primary(&self, node: u32) -> bool {
        let target = self.node(node);
        target.primary_eligible && target.state.keeps_copies()
    }

    fn copy_counts(&self) -> BTreeMap<u32, usize> {
        count_copies(
            self.nodes.values(),
            self.parts.values().flat_map(|(copies, _)| copies),
        )
    }

    fn primary_counts(&self) -> BTreeMap<u32, usize> {
        count_primaries(
            self.nodes.values(),
            self.parts.values().map(|(_, primary)| primary),
        )
    }

    /// Records a step if the budget allows; `false` when it does not.
    fn take(&mut self, step: Step) -> bool {
        if self.steps.len() >= self.max_moves {
            self.out_of_budget = true;
            return false;
        }
        let (copies, primary) = self
            .parts
            .get_mut(&step.partition)
            .expect("planned partition");
        if step.primary {
            *primary = step.to;
        } else {
            for copy in copies.iter_mut() {
                if *copy == step.from {
                    *copy = step.to;
                }
            }
        }
        self.steps.push(step);
        true
    }

    /// The best copy target for `partition`'s copy on `from`: fewest copies, then rendezvous.
    fn best_copy_target(&self, partition: u32, from: u32, need_primary: bool) -> Option<u32> {
        let counts = self.copy_counts();
        self.nodes
            .values()
            .filter(|n| !need_primary || n.primary_eligible)
            .filter(|n| self.copy_target_ok(partition, from, n.id))
            .min_by_key(|n| {
                (
                    counts.get(&n.id).copied().unwrap_or(0),
                    u64::MAX - rendezvous(partition, n.id),
                )
            })
            .map(|n| n.id)
    }

    /// Phase 1: repair hard rules the input breaks, partition by partition.
    fn repair(&mut self) {
        let ids: Vec<u32> = self.parts.keys().copied().collect();
        for id in ids {
            if !self.repair_primary(id) {
                return;
            }
            loop {
                let (copies, primary) = self.parts[&id].clone();
                // PR #74 F-002: a copy with no legal target does not end the repair; a later
                // broken copy that has one still moves.
                let next = self
                    .violations(&copies, primary)
                    .into_iter()
                    .filter(|(node, rule)| *node != primary && *rule != Rule::PrimaryNotEligible)
                    .find_map(|(from, rule)| {
                        self.best_copy_target(id, from, false)
                            .map(|to| (from, rule, to))
                    });
                let Some((from, rule, to)) = next else { break };
                if !self.take(Step {
                    partition: id,
                    from,
                    to,
                    primary: false,
                    why: Why::Repair(rule),
                }) {
                    return;
                }
            }
        }
    }

    /// Whether a legal step exists now toward repairing a rule broken on `node`: a target for its
    /// copy, or, on the primary's node, somewhere for the primary to go first.
    fn repairable(&self, id: u32, node: u32) -> bool {
        let (copies, primary) = &self.parts[&id];
        if node != *primary {
            return self.best_copy_target(id, node, false).is_some();
        }
        copies.iter().any(|&c| self.may_hold_primary(c))
            || copies
                .iter()
                .any(|&c| c != *primary && self.best_copy_target(id, c, true).is_some())
    }

    /// Moves `id`'s primary off a node that may not hold it. `false` when the budget ran out.
    fn repair_primary(&mut self, id: u32) -> bool {
        let (copies, primary) = self.parts[&id].clone();
        let node = self.node(primary);
        if node.primary_eligible && node.state.keeps_copies() {
            return true;
        }
        let rule = if node.state.keeps_copies() {
            Rule::PrimaryNotEligible
        } else {
            Rule::NodeState(node.state)
        };
        let counts = self.primary_counts();
        let holder = copies
            .iter()
            .copied()
            .filter(|&c| self.may_hold_primary(c))
            .min_by_key(|&c| {
                (
                    counts.get(&c).copied().unwrap_or(0),
                    u64::MAX - rendezvous(id, c),
                )
            });
        if let Some(to) = holder {
            return self.take(Step {
                partition: id,
                from: primary,
                to,
                primary: true,
                why: Why::Repair(rule),
            });
        }
        // No holder may take it: move a non-primary copy to a node that may, then promote it.
        let room = copies
            .iter()
            .copied()
            .filter(|&c| c != primary)
            .find_map(|from| self.best_copy_target(id, from, true).map(|to| (from, to)));
        let Some((from, to)) = room else { return true };
        self.take(Step {
            partition: id,
            from,
            to,
            primary: false,
            why: Why::PrimaryRoom,
        }) && self.take(Step {
            partition: id,
            from: primary,
            to,
            primary: true,
            why: Why::Repair(rule),
        })
    }

    /// Phase 2: move non-primary copies from a node with at least two more copies than the
    /// target until skew is at most 1. Each move lowers the sum of squared counts, so it ends.
    fn balance_copies(&mut self) -> Cause {
        loop {
            let counts = self.copy_counts();
            if skew(&counts) <= 1 {
                return Cause::NoLegalMove;
            }
            let mut sources: Vec<(u32, usize)> = counts.iter().map(|(n, c)| (*n, *c)).collect();
            sources.sort_by_key(|(n, c)| (std::cmp::Reverse(*c), *n));
            let mut targets = sources.clone();
            targets.sort_by_key(|(n, c)| (*c, *n));
            let mut movable: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for (id, (copies, primary)) in &self.parts {
                for &c in copies.iter().filter(|&&c| c != *primary) {
                    movable.entry(c).or_default().push(*id);
                }
            }
            let found = sources.iter().find_map(|&(from, high)| {
                targets
                    .iter()
                    .take_while(|(_, low)| low + 2 <= high)
                    .find_map(|&(to, _)| {
                        movable
                            .get(&from)?
                            .iter()
                            .copied()
                            .filter(|&id| self.copy_target_ok(id, from, to))
                            .max_by_key(|&id| rendezvous(id, to))
                            .map(|id| (id, from, to))
                    })
            });
            let Some((partition, from, to)) = found else {
                return Cause::NoLegalMove;
            };
            if !self.take(Step {
                partition,
                from,
                to,
                primary: false,
                why: Why::CopySkew,
            }) {
                return Cause::Budget;
            }
        }
    }

    /// Phase 3: move primaries along a chain of copy holders, from a node with the most to one
    /// with at least two fewer, until skew is at most 1. Each chain lowers the sum of squared
    /// counts, so it ends.
    fn balance_primaries(&mut self) -> Cause {
        loop {
            let counts = self.primary_counts();
            if skew(&counts) <= 1 {
                return Cause::NoLegalMove;
            }
            let mut led: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for (id, (_, primary)) in &self.parts {
                led.entry(*primary).or_default().push(*id);
            }
            let mut sources: Vec<(u32, usize)> = counts.iter().map(|(n, c)| (*n, *c)).collect();
            sources.sort_by_key(|(n, c)| (std::cmp::Reverse(*c), *n));
            let Some(chain) = sources
                .iter()
                .find_map(|&(from, high)| self.primary_chain(from, high, &counts, &led))
            else {
                return Cause::NoLegalMove;
            };
            // Run the chain from its far end, so every node keeps its count until the last step.
            for &(partition, from, to) in chain.iter().rev() {
                if !self.take(Step {
                    partition,
                    from,
                    to,
                    primary: true,
                    why: Why::PrimarySkew,
                }) {
                    return Cause::Budget;
                }
            }
        }
    }

    /// The shortest chain of primary moves from `start` to a node with at most `high − 2`
    /// primaries: breadth first over "u holds p's primary, v holds a copy of p".
    fn primary_chain(
        &self,
        start: u32,
        high: usize,
        counts: &BTreeMap<u32, usize>,
        led: &BTreeMap<u32, Vec<u32>>,
    ) -> Option<Vec<(u32, u32, u32)>> {
        let mut came_from: BTreeMap<u32, (u32, u32)> = BTreeMap::new();
        let mut queue = std::collections::VecDeque::from([start]);
        let mut seen = BTreeSet::from([start]);
        while let Some(u) = queue.pop_front() {
            for id in led.get(&u).into_iter().flatten() {
                let (copies, _) = &self.parts[id];
                for &v in copies {
                    if seen.contains(&v) || !self.may_hold_primary(v) {
                        continue;
                    }
                    seen.insert(v);
                    came_from.insert(v, (u, *id));
                    if counts.get(&v).copied().unwrap_or(0) + 2 <= high {
                        let mut chain = Vec::new();
                        let mut at = v;
                        while let Some(&(prev, part)) = came_from.get(&at) {
                            chain.push((part, prev, at));
                            at = prev;
                        }
                        chain.reverse();
                        return Some(chain);
                    }
                    queue.push_back(v);
                }
            }
        }
        None
    }
}
