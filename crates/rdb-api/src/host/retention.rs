//! Issue #61 S3 (design section 3.1, PR-B): the History retention duty. Once a
//! [`RETENTION_DUTY_MILLIS`], each partition this node holds gets one round: the host gathers
//! what [`retention_floor`] reads from its engine and kernel, and when the floor moves it trims
//! there through `Host::trim_history_through` (R1's ladders, then the engine, then the kept
//! ladders). The simulator's twin returns under #239.
//!
//! * `prev`, `applied` and `own_durable` are the engine's, of the partition's newest lineage.
//!   `keep` comes from this node's [`RetentionSamples`], the cap from
//!   [`RocksEngine::history_bytes`] through [`cap_seq`].
//! * On a primary the copies are every other copy its tracker names, at `known` durable; a
//!   secondary passes none.
//! * Each snapshot transfer this node serves pins its `S`. A pin the cap overran is logged
//!   [`HISTORY_BUDGET_OVERRAN_PIN`] and its transfer ends `budget_overran`, so R1's next ask
//!   opens a new one.
//! * A copy behind the floor is logged [`HISTORY_COPY_BEHIND_FLOOR`] once, until it is counted
//!   again.
//! * A hold ([`Host::retention_hold`]) trims nothing (`history_trim_held{reason}`). An inventory
//!   pin and a grace are released first when they no longer hold. Under grace the cap still
//!   trims (lead ruling R23).
//! * Grace begins when a `Recovered` leaves this node a primary (`history_grace_begun`) and ends
//!   `history_grace_ended{why}`: `acked`, `expired`, `no_primary`, or `superseded` when a newer
//!   generation's `Recovered` begins a fresh one (lead ruling R24: one grace per generation).
//! * The samples are kept per generation: a newer one starts them afresh
//!   (`retention_samples_reset`), since its head may sit below the old one's.

use super::*;
use rdb_core::replication::retention::{
    cap_seq, needs_history_budget, overrun_action, retention_floor, OverrunAction,
    RetentionDecision, RetentionFloor, RetentionHold, RetentionInput, RetentionSamples,
    HISTORY_BUDGET_GIVE_UP_CLEARED, HISTORY_BUDGET_OVERRAN_PIN, HISTORY_COPY_BEHIND_FLOOR,
};

/// How often the retention duty runs.
pub(crate) const RETENTION_DUTY_MILLIS: u64 = 1_000;

/// The spec's `history_keep`: 30 minutes of History is kept at least (design 3.2).
pub(crate) const HISTORY_KEEP_MILLIS: u64 = 30 * 60 * 1_000;

/// The spec's `history_budget_bytes`: 20 GiB, a hard cap (design 3.2, owner ruling Q1(a)).
pub(crate) const HISTORY_BUDGET_BYTES: u64 = 20 << 30;

/// A node's History retention settings ([`crate::DbConfig::history_keep`],
/// [`crate::DbConfig::history_budget_bytes`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryRetention {
    /// History applied within this long is kept at least.
    pub(crate) keep_millis: u64,
    /// The History each copy of a partition keeps at most, in bytes.
    pub(crate) budget_bytes: u64,
}

impl HistoryRetention {
    /// The spec's defaults.
    pub(crate) const DEFAULT: Self = Self {
        keep_millis: HISTORY_KEEP_MILLIS,
        budget_bytes: HISTORY_BUDGET_BYTES,
    };
}

/// One partition's retention state on this node, kept between rounds. Process memory: a
/// restart starts it afresh.
#[derive(Debug, Default)]
pub(crate) struct RetentionState {
    /// The applied head over time, of the generation named.
    pub(crate) samples: Option<(Generation, RetentionSamples)>,
    /// The copies logged behind the floor and not counted again since.
    pub(crate) behind: BTreeSet<CopyId>,
    /// The open grace: the generation whose `Recovered` began it, and when.
    pub(crate) grace: Option<(Generation, Tick)>,
    /// The generation the last grace began in, kept after it ends, so a replayed `Recovered`
    /// of that generation does not open a second one (PR #240 r3: one grace per generation).
    pub(crate) graced: Option<Generation>,
    /// Per copy, the overruns in a row of its transfers from this node (issue #61 S3-R4 (b),
    /// owner ruling 2026-11-20, option 1): one more each time the cap ends one
    /// `budget_overran`, reset by any round for the copy that is not an overrun: a `caught_up`
    /// release, or a round whose floor stays at or below its open transfer's pin. A held round
    /// decides no floor, so it neither counts nor resets. The simulator's twin returns under #239.
    pub(crate) overruns: BTreeMap<CopyId, u32>,
    /// Per copy this node stopped offering at [`OverrunAction::GiveUp`]: the budget and the
    /// generation in force then, and the overruns counted. Named in `NodeStatus::needs_operator`
    /// until a caught-up release, a raised budget or a newer generation clears it (lead ruling
    /// R27).
    pub(crate) given_up: BTreeMap<CopyId, GivenUp>,
}

/// A copy this node gave up offering snapshots to (issue #61 S3-R4 (b)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GivenUp {
    /// `history_budget_bytes` in force at the give-up: a higher one clears it.
    pub(crate) budget_bytes: u64,
    /// The generation of the round that gave up: a newer one clears it.
    pub(crate) generation: Generation,
    /// The overruns in a row that gave up.
    pub(crate) attempts: u32,
}

impl Host {
    /// Every partition this node holds gets one retention round. A round that cannot run is
    /// logged `history_retention_failed{reason}` and the others go on.
    pub(super) fn retention_duty(&mut self, now: Tick) {
        let mut newest: BTreeMap<PartitionId, Generation> = BTreeMap::new();
        for (partition, generation) in self.engine.lineages() {
            let at = newest.entry(partition).or_insert(generation);
            *at = (*at).max(generation);
        }
        self.retention
            .retain(|partition, _| newest.contains_key(partition));
        for partition in newest.into_keys() {
            if let Err(reason) = self.retention_round(partition, now) {
                tracing::error!(
                    node = self.node.0,
                    partition = partition.0,
                    reason,
                    "history_retention_failed"
                );
            }
        }
    }

    /// One retention round for `partition` at `now` (module docs).
    ///
    /// # Errors
    ///
    /// `no_lineage` when this node holds none of the partition; the samples' refusal
    /// (`tick_backwards`, `applied_backwards`) within one generation, with nothing trimmed;
    /// and `Host::trim_history_through`'s refusal of the floor.
    pub(crate) fn retention_round(
        &mut self,
        partition: PartitionId,
        now: Tick,
    ) -> Result<RetentionDecision, String> {
        let generation = self.newest_held(partition).ok_or("no_lineage")?;
        self.clear_give_ups(partition, generation);
        let prev = self.engine.floor_seq(partition, generation);
        let applied = Seq(self.engine.buffered_applied(partition, generation).0);
        let own_durable = self.engine.durable(partition, generation);
        let settings = self.history_retention;
        let engine = &self.engine;
        let cap = cap_seq(prev, applied, settings.budget_bytes, |from| {
            engine.history_bytes(partition, generation, from)
        });
        let keep = self.retention_keep(partition, generation, applied, now)?;
        self.release_inventory_pin(partition, now);
        self.release_grace(partition, now);
        let copies = self
            .replication
            .primary(self.node, partition)
            .map(|primary| {
                let tracker = primary.tracker();
                tracker
                    .peers()
                    .map(|(copy, _)| copy)
                    .filter(|copy| *copy != tracker.own())
                    .filter_map(|copy| tracker.known(copy).map(|known| (copy, known.durable)))
                    .collect()
            });
        let served: Vec<(CopyId, Seq)> = self
            .transfers
            .iter()
            .filter(|((p, _), _)| *p == partition)
            .map(|((_, copy), transfer)| (*copy, transfer.at))
            .collect();
        let input = RetentionInput {
            prev,
            applied,
            own_durable,
            transfer_pins: served.iter().map(|(_, at)| *at).collect(),
            copies,
            keep,
            cap,
            hold: self.retention_hold(partition, generation),
        };
        let mut decision = retention_floor(&input);
        if let RetentionDecision::Floor(floor) = &mut decision {
            self.clamp_at_rebuild(partition, generation, prev, &input, floor);
        }
        let floor = match &decision {
            RetentionDecision::Held(hold) => {
                tracing::debug!(
                    node = self.node.0,
                    partition = partition.0,
                    reason = hold.reason(),
                    "history_trim_held"
                );
                return Ok(decision);
            }
            RetentionDecision::Floor(floor) => floor,
        };
        let state = self.retention.entry(partition).or_default();
        state.behind.retain(|copy| floor.behind.contains(copy));
        for copy in &floor.behind {
            if state.behind.insert(*copy) {
                tracing::warn!(
                    node = self.node.0,
                    partition = partition.0,
                    copy = copy.0,
                    floor = floor.floor.0,
                    "{HISTORY_COPY_BEHIND_FLOOR}"
                );
            }
        }
        // A round within budget for a copy's open transfer ends its run of overruns (lead
        // ruling 2026-11-21): the owner's three are consecutive.
        for (copy, _) in served.iter().filter(|(_, at)| *at >= floor.floor) {
            state.overruns.remove(copy);
        }
        for (copy, at) in served.iter().filter(|(_, at)| *at < floor.floor) {
            let state = self.retention.entry(partition).or_default();
            let attempts = state.overruns.entry(*copy).or_insert(0);
            *attempts = attempts.saturating_add(1);
            let attempts = *attempts;
            let gave_up = overrun_action(attempts) == OverrunAction::GiveUp;
            if gave_up {
                state.given_up.insert(
                    *copy,
                    GivenUp {
                        budget_bytes: settings.budget_bytes,
                        generation,
                        attempts,
                    },
                );
            }
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                copy = copy.0,
                pin = at.0,
                floor = floor.floor.0,
                attempts,
                gave_up,
                "{HISTORY_BUDGET_OVERRAN_PIN}"
            );
            self.end_transfer(
                partition,
                *copy,
                "budget_overran",
                "snapshot_transfer_abandoned",
            );
            if gave_up {
                tracing::warn!(
                    node = self.node.0,
                    partition = partition.0,
                    copy = copy.0,
                    attempts,
                    needs_operator = %needs_history_budget(*copy, self.node, attempts),
                    "history_budget_gave_up"
                );
            }
        }
        if floor.floor > prev {
            self.trim_history_through(partition, floor.floor)?;
        }
        Ok(decision)
    }

    /// While F1 here is `Rebuilding` its commit of `generation` below `Active`, keep the floor at
    /// or below that commit's cutoff, logged `history_trim_held_rebuild` when that lowers it.
    /// `activate` restates the commit at that cutoff, and the primary's R1 refuses a
    /// restatement whose cutoff rung it no longer holds (`BarrierNotDurable`), so a trim past it
    /// would make the primary refuse its own activation. This overrides the byte cap for the
    /// length of the rebuild (lead ruling, PR #240 r3 Q1). A commit F1 armed no rebuild for (a
    /// two-copy drop, a capped commit) never activates, so it is not held.
    fn clamp_at_rebuild(
        &self,
        partition: PartitionId,
        generation: Generation,
        prev: Seq,
        input: &RetentionInput,
        floor: &mut RetentionFloor,
    ) {
        let rebuilding = self
            .recoveries
            .get(&partition)
            .is_some_and(|f1| f1.phase() == RecoveryPhase::Rebuilding);
        let Some(watch) = self
            .rebuilds
            .get(&partition)
            .filter(|watch| rebuilding && watch.generation == generation)
        else {
            return;
        };
        let held = prev.max(watch.cutoff);
        if floor.floor <= held {
            return;
        }
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            mode = ?watch.mode,
            cutoff = watch.cutoff.0,
            floor_was = floor.floor.0,
            floor = held.0,
            "history_trim_held_rebuild"
        );
        floor.floor = held;
        floor.overran_pins.retain(|pin| *pin < held);
        let below: BTreeSet<CopyId> = input
            .copies
            .iter()
            .flatten()
            .filter(|(_, durable)| Seq(durable.0) < held)
            .map(|(copy, _)| *copy)
            .collect();
        floor.behind.retain(|copy| below.contains(copy));
    }

    /// Record `applied` in `partition`'s samples of `generation` and answer `keep`. A newer
    /// generation than the samples' starts them afresh, logged `retention_samples_reset`.
    fn retention_keep(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        applied: Seq,
        now: Tick,
    ) -> Result<Seq, String> {
        let keep_millis = self.history_retention.keep_millis;
        let node = self.node.0;
        let state = self.retention.entry(partition).or_default();
        match &state.samples {
            Some((held, _)) if *held == generation => {}
            held => {
                if let Some((from, _)) = held {
                    tracing::info!(
                        node,
                        partition = partition.0,
                        from_gen = from.0,
                        to_gen = generation.0,
                        "retention_samples_reset"
                    );
                }
                state.samples = Some((generation, RetentionSamples::new()));
            }
        }
        let (_, samples) = state.samples.as_mut().ok_or("no_samples")?;
        samples.record(now, applied, keep_millis)?;
        Ok(samples.keep_seq(now, keep_millis))
    }

    /// The no-trim gate the duty finds on `partition`, the first that holds, or `None` (design
    /// 3.1, "No trim while"; the simulator's twin returns under #239).
    ///
    /// * `Staging` / `Installing`: the newest lineage is mid copy or mid install, or this node
    ///   holds an install of the partition open.
    /// * `Recovering`: F1 here is in any phase but `Idle`, `Committed` or `Rebuilding`.
    /// * `LandingPending`: a landing of the partition is queued, or its link sync is awaited.
    /// * `InventoryPinned`: this node answered an inventory and holds its pin.
    /// * `Grace`, last, because the cap passes it and no other (lead ruling R23).
    pub(super) fn retention_hold(
        &self,
        partition: PartitionId,
        generation: Generation,
    ) -> Option<RetentionHold> {
        if self.engine.staging(partition, generation).is_some() {
            return Some(RetentionHold::Staging);
        }
        if self.engine.installing(partition, generation).is_some()
            || self.installs.contains_key(&partition)
        {
            return Some(RetentionHold::Installing);
        }
        let recovering = self.recoveries.get(&partition).is_some_and(|f1| {
            !matches!(
                f1.phase(),
                RecoveryPhase::Idle | RecoveryPhase::Committed | RecoveryPhase::Rebuilding
            )
        });
        if recovering {
            return Some(RetentionHold::Recovering);
        }
        let landing = self.delayed.values().any(
            |item| matches!(item, Delayed::Landing { site, .. } if site.partition == partition),
        ) || self.link_pending.keys().any(|(p, _)| *p == partition);
        if landing {
            return Some(RetentionHold::LandingPending);
        }
        if self.inventory_pins.contains_key(&partition) {
            return Some(RetentionHold::InventoryPinned);
        }
        self.retention
            .get(&partition)
            .and_then(|state| state.grace)
            .map(|_| RetentionHold::Grace)
    }

    /// Drop `partition`'s inventory pin when it no longer holds at `now`: silently once this
    /// node has landed a newer generation, logged `inventory_pin_expired` once
    /// `inventory_pin_millis` has passed. Answers whether it is still held.
    pub(super) fn release_inventory_pin(&mut self, partition: PartitionId, now: Tick) -> bool {
        let Some(pin) = self.inventory_pins.get(&partition).copied() else {
            return false;
        };
        let landed_past = self
            .landed
            .iter()
            .any(|(p, g)| *p == partition && *g > pin.generation);
        if landed_past {
            self.inventory_pins.remove(&partition);
            return false;
        }
        if now >= pin.since.plus_millis(self.inventory_pin_millis) {
            self.inventory_pins.remove(&partition);
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                generation = pin.generation.0,
                millis = self.inventory_pin_millis,
                "inventory_pin_expired"
            );
            return false;
        }
        true
    }

    /// Clear every give-up of `partition` the round at `generation` no longer stands by (lead
    /// ruling R27): `history_budget_bytes` is now higher than at the give-up (`budget_raised`),
    /// or the generation is newer (`new_generation`, a new plan). The count goes with it, logged
    /// [`HISTORY_BUDGET_GIVE_UP_CLEARED`], so the copy's next ask is offered again. A lower or
    /// equal budget clears nothing. The budget is fixed when the node starts, so in a running
    /// node `budget_raised` is reached only by a row that sets it; the operator's remedy is a
    /// reopen with a larger budget, which starts with no give-up. The simulator's twin returns
    /// under #239.
    fn clear_give_ups(&mut self, partition: PartitionId, generation: Generation) {
        let budget_bytes = self.history_retention.budget_bytes;
        let node = self.node.0;
        let Some(state) = self.retention.get_mut(&partition) else {
            return;
        };
        let cleared: Vec<_> = state
            .given_up
            .iter()
            .filter_map(|(copy, given)| {
                if generation > given.generation {
                    Some((*copy, "new_generation", *given))
                } else if budget_bytes > given.budget_bytes {
                    Some((*copy, "budget_raised", *given))
                } else {
                    None
                }
            })
            .collect();
        for (copy, why, given) in cleared {
            state.given_up.remove(&copy);
            state.overruns.remove(&copy);
            tracing::info!(
                node,
                partition = partition.0,
                copy = copy.0,
                why,
                budget_was = given.budget_bytes,
                budget = budget_bytes,
                generation_was = given.generation.0,
                generation = generation.0,
                "{HISTORY_BUDGET_GIVE_UP_CLEARED}"
            );
        }
    }

    /// A caught-up release of `copy` on `partition`: its run of overruns starts again and a
    /// give-up of it ends (owner ruling 2026-11-20, option 1).
    pub(super) fn overruns_caught_up(&mut self, partition: PartitionId, copy: CopyId) {
        if let Some(state) = self.retention.get_mut(&partition) {
            state.overruns.remove(&copy);
            state.given_up.remove(&copy);
        }
    }

    /// The give-up of `copy` on `partition`, if this node stopped offering it snapshots.
    pub(super) fn given_up(&self, partition: PartitionId, copy: CopyId) -> Option<GivenUp> {
        self.retention
            .get(&partition)
            .and_then(|state| state.given_up.get(&copy))
            .copied()
    }

    /// `NodeStatus::needs_operator`'s text for the first copy of `partition` this node gave up
    /// on ([`needs_history_budget`]).
    pub(super) fn needs_history_budget(&self, partition: PartitionId) -> Option<String> {
        let state = self.retention.get(&partition)?;
        let (copy, given) = state.given_up.iter().next()?;
        Some(needs_history_budget(*copy, self.node, given.attempts))
    }

    /// A `Recovered` of `generation` stepped into R1 here: when it leaves this node a primary,
    /// its window waits for its copies' acks (design 3.1 m2). A repeat in the last graced
    /// generation keeps an open grace's start (lead ruling R24) and leaves a closed one closed;
    /// a newer generation ends the open one `superseded` and begins afresh.
    pub(super) fn begin_grace(&mut self, partition: PartitionId, generation: Generation) {
        if self.replication.primary(self.node, partition).is_none() {
            return;
        }
        let now = self.clock.now();
        let node = self.node.0;
        let state = self.retention.entry(partition).or_default();
        if state.graced == Some(generation) {
            return;
        }
        let open = state.grace.map(|(held, _)| held);
        if let Some(held) = open {
            tracing::info!(
                node,
                partition = partition.0,
                generation = held.0,
                why = "superseded",
                "history_grace_ended"
            );
        }
        state.grace = Some((generation, now));
        state.graced = Some(generation);
        tracing::info!(
            node,
            partition = partition.0,
            generation = generation.0,
            "history_grace_begun"
        );
    }

    /// End `partition`'s grace when it no longer holds at `now` (lead ruling R23), logged
    /// `history_grace_ended{why}`: `acked` once every other copy the tracker names has an
    /// admitted ack (its last admitted ACK's `received` above 0), `expired` after
    /// `max(history_keep, inventory_pin_millis)`, `no_primary` when this node holds no primary.
    fn release_grace(&mut self, partition: PartitionId, now: Tick) {
        let Some((generation, since)) =
            self.retention.get(&partition).and_then(|state| state.grace)
        else {
            return;
        };
        let wait = self
            .history_retention
            .keep_millis
            .max(self.inventory_pin_millis);
        let why = match self.replication.primary(self.node, partition) {
            None => Some("no_primary"),
            Some(primary) => {
                let tracker = primary.tracker();
                let acked = tracker
                    .peers()
                    .filter(|(copy, _)| *copy != tracker.own())
                    .all(|(_, peer)| peer.progress.received.0 > 0);
                if acked {
                    Some("acked")
                } else if now >= since.plus_millis(wait) {
                    Some("expired")
                } else {
                    None
                }
            }
        };
        if let Some(why) = why {
            if let Some(state) = self.retention.get_mut(&partition) {
                state.grace = None;
            }
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                generation = generation.0,
                why,
                "history_grace_ended"
            );
        }
    }
}
