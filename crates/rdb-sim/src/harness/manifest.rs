//! The run manifest: what a scenario actually ran under (finding K-F-27).
//!
//! Spike §7 says the manifest records resolved budgets. The part that matters is *which* of
//! them were overridden from [`Budgets::SPEC_DEFAULTS`]: a campaign row that fails under an
//! override must not be mistaken for one that fails under defaults — the
//! `RETCD_TEST_DEADLINE_SCALE` lesson from the rEtcd gate. [`resolve`] builds
//! [`rdb_core::contracts::trace::TraceHeader::config`], and the campaign's report embeds the
//! same value, so a report and its trace agree on what was run.

use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::trace::{BudgetName, RunManifest};

use crate::error::SimError;
use crate::sim::cluster::ClusterConfig;

/// One budget a scenario sets away from the spec default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BudgetOverride {
    /// Which budget.
    pub name: BudgetName,
    /// Its value for this run, in milliseconds.
    pub millis: u64,
}

/// Resolve the budgets for a run and record which ones differ from the defaults.
///
/// `overridden` lists, in [`BudgetName::ALL`] order, every budget whose resolved value differs
/// from [`Budgets::SPEC_DEFAULTS`]. An override that sets a budget to its default is not an
/// override: the manifest records what the run *was*, not what the scenario file said.
///
/// # Errors
///
/// [`SimError::Config`] naming `overrides` when one budget is overridden twice — two values
/// for one budget is a scenario that means two different runs — and naming `nodes` when the
/// topology has more nodes than the header's `u8` can count.
pub fn resolve(
    cluster: &ClusterConfig,
    event_cap: u32,
    overrides: &[BudgetOverride],
) -> Result<RunManifest, SimError> {
    let mut budgets = Budgets::SPEC_DEFAULTS;
    for (index, first) in overrides.iter().enumerate() {
        if overrides[..index]
            .iter()
            .any(|earlier| earlier.name == first.name)
        {
            return Err(SimError::Config { field: "overrides" });
        }
        first.name.set(&mut budgets, first.millis);
    }
    let overridden = BudgetName::ALL
        .into_iter()
        .filter(|name| name.get(&budgets) != name.get(&Budgets::SPEC_DEFAULTS))
        .collect();
    let nodes =
        u8::try_from(cluster.nodes.len()).map_err(|_| SimError::Config { field: "nodes" })?;
    Ok(RunManifest {
        budgets,
        overridden,
        nodes,
        event_cap,
    })
}
