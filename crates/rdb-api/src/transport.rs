//! In-process peer links (M9 architecture §3, `rdb_api::transport`).
//!
//! A concrete struct, no trait until M10. Each rDB node registers its mailbox. A peer message —
//! a replication frame, or a recovery request and its answer — goes through [`Links::send`] and
//! honours a hold or a drop. A held link buffers in order and delivers on [`Links::heal`], so a
//! hold is a delay, never a loss: the frames arrive late, as a slow network delivers them. A
//! dropped direction ([`Links::drop_direction`]) loses what is sent on it: `send` discards the
//! message and still answers `Ok`, as a lossy network never tells the sender. Only a retransmit
//! after [`Links::heal`] gets the message through.
//!
//! [`Links::post`] skips the holds. It carries what does not travel on the peer link: client
//! calls, control completions and the control-plane `Recovered` watch.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, MutexGuard};

use rdb_core::contracts::ids::NodeId;
use rdb_core::contracts::transport::LinkFault;

use crate::host::Msg;

/// The peer links of one process.
#[derive(Debug, Default)]
pub struct Links {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    mailboxes: BTreeMap<NodeId, Sender<Msg>>,
    /// Held pairs, smaller node first. A hold covers both directions.
    held: BTreeSet<(NodeId, NodeId)>,
    /// Per direction, what a hold kept back, oldest first.
    buffered: BTreeMap<(NodeId, NodeId), VecDeque<Msg>>,
    /// Dropped directions, `(from, to)`: what is sent on one is lost.
    dropped: BTreeSet<(NodeId, NodeId)>,
}

const fn pair(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a.0 <= b.0 {
        (a, b)
    } else {
        (b, a)
    }
}

impl Links {
    /// No nodes, no holds.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A poisoned lock means a node thread panicked while holding it. Nothing here can be
        // half-written by a panic (every mutation is one map operation), so carry on.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Make `node` reachable at `mailbox`.
    pub fn register(&self, node: NodeId, mailbox: Sender<Msg>) {
        self.lock().mailboxes.insert(node, mailbox);
    }

    /// Whether `node` is registered.
    #[must_use]
    pub fn knows(&self, node: NodeId) -> bool {
        self.lock().mailboxes.contains_key(&node)
    }

    /// Send a peer message from `from` to `to`, honouring a drop and then a hold. A dropped
    /// direction discards the message and answers `Ok(())`: a loss is silent, never a
    /// [`LinkFault`], so the sender learns of it only by its own timers.
    ///
    /// # Errors
    ///
    /// [`LinkFault::Unreachable`] when `to` is not registered or its node has stopped.
    pub fn send(&self, from: NodeId, to: NodeId, msg: Msg) -> Result<(), LinkFault> {
        let mut inner = self.lock();
        if !inner.mailboxes.contains_key(&to) {
            return Err(LinkFault::Unreachable);
        }
        if inner.dropped.contains(&(from, to)) {
            tracing::info!(from = from.0, to = to.0, "link_dropped");
            return Ok(());
        }
        if inner.held.contains(&pair(from, to)) {
            let queue = inner.buffered.entry((from, to)).or_default();
            queue.push_back(msg);
            tracing::debug!(
                from = from.0,
                to = to.0,
                buffered = queue.len(),
                "link_buffered"
            );
            return Ok(());
        }
        inner.mailboxes[&to]
            .send(msg)
            .map_err(|_| LinkFault::Unreachable)
    }

    /// Deliver `msg` to `to` now, whatever the holds.
    ///
    /// # Errors
    ///
    /// [`LinkFault::Unreachable`] when `to` is not registered or its node has stopped.
    pub fn post(&self, to: NodeId, msg: Msg) -> Result<(), LinkFault> {
        let inner = self.lock();
        let mailbox = inner.mailboxes.get(&to).ok_or(LinkFault::Unreachable)?;
        mailbox.send(msg).map_err(|_| LinkFault::Unreachable)
    }

    /// Hold the link between `a` and `b`, both directions. Holding a held link is a no-op.
    pub fn hold(&self, a: NodeId, b: NodeId) {
        let added = self.lock().held.insert(pair(a, b));
        tracing::info!(a = a.0, b = b.0, added, "link_hold");
    }

    /// Lose every message sent from `from` to `to` until a heal of that pair. One direction
    /// only: `to` still reaches `from`. Dropping a dropped direction is a no-op.
    pub fn drop_direction(&self, from: NodeId, to: NodeId) {
        let added = self.lock().dropped.insert((from, to));
        tracing::info!(from = from.0, to = to.0, added, "link_drop");
    }

    /// Heal the link between `a` and `b`: lift its hold and the drops in both directions,
    /// and deliver what the hold buffered, in order per direction. What a drop lost stays lost.
    /// Returns how many messages were delivered.
    pub fn heal(&self, a: NodeId, b: NodeId) -> usize {
        let mut inner = self.lock();
        let removed = inner.held.remove(&pair(a, b));
        let undropped =
            usize::from(inner.dropped.remove(&(a, b))) + usize::from(inner.dropped.remove(&(b, a)));
        let mut flushed = 0;
        for direction in [(a, b), (b, a)] {
            let Some(queue) = inner.buffered.remove(&direction) else {
                continue;
            };
            for msg in queue {
                // A stopped node drops what it was sent; the sender was never told either way.
                if let Some(mailbox) = inner.mailboxes.get(&direction.1) {
                    if mailbox.send(msg).is_ok() {
                        flushed += 1;
                    }
                }
            }
        }
        tracing::info!(a = a.0, b = b.0, removed, undropped, flushed, "link_heal");
        flushed
    }

    /// Heal every held link and every dropped direction. Returns how many messages were
    /// delivered.
    pub fn heal_all(&self) -> usize {
        let pairs: BTreeSet<_> = {
            let inner = self.lock();
            let dropped = inner.dropped.iter().map(|&(from, to)| pair(from, to));
            inner.held.iter().copied().chain(dropped).collect()
        };
        pairs.into_iter().map(|(a, b)| self.heal(a, b)).sum()
    }

    /// The held links, smaller node first.
    #[must_use]
    pub fn held(&self) -> Vec<(NodeId, NodeId)> {
        self.lock().held.iter().copied().collect()
    }

    /// The dropped directions, as `(from, to)`.
    #[must_use]
    pub fn dropped(&self) -> Vec<(NodeId, NodeId)> {
        self.lock().dropped.iter().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{self, Receiver};

    /// A message `send` can carry, told apart by `detail`.
    fn msg(detail: &str) -> Msg {
        Msg::Fault {
            boot: crate::host::BOOT,
            kind: "probe",
            detail: detail.to_owned(),
        }
    }

    /// What `rx` holds now, by detail.
    fn got(rx: &Receiver<Msg>) -> Vec<String> {
        rx.try_iter()
            .map(|msg| match msg {
                Msg::Fault { detail, .. } => detail,
                other => panic!("not a probe: {other:?}"),
            })
            .collect()
    }

    /// M9 S3-1, S3-1n and S3-1h: a dropped direction loses what is sent on it and still answers
    /// `Ok`, so the sender learns of it only by its own timers (A-2). It is one direction: the
    /// reverse still delivers. A heal of the pair lifts it from either end, and `heal_all`
    /// lifts every drop as well as every hold (A-1). What a drop lost stays lost; what a hold
    /// kept is delivered. Unit.
    #[test]
    fn a_dropped_direction_loses_silently_one_way_until_a_heal() {
        let links = Links::new();
        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();
        let (tx3, rx3) = mpsc::channel();
        links.register(NodeId(1), tx1);
        links.register(NodeId(2), tx2);
        links.register(NodeId(3), tx3);

        links.drop_direction(NodeId(2), NodeId(1));
        assert_eq!(links.send(NodeId(2), NodeId(1), msg("ack lost")), Ok(()));
        assert_eq!(links.send(NodeId(1), NodeId(2), msg("frame")), Ok(()));
        assert_eq!(got(&rx1), Vec::<String>::new(), "2->1 is dropped");
        assert_eq!(got(&rx2), ["frame"], "1->2 still delivers");
        assert_eq!(links.dropped(), [(NodeId(2), NodeId(1))]);

        assert_eq!(
            links.heal(NodeId(1), NodeId(2)),
            0,
            "a drop buffers nothing"
        );
        assert_eq!(links.dropped(), [], "healed from the other end");
        assert_eq!(links.send(NodeId(2), NodeId(1), msg("ack")), Ok(()));
        assert_eq!(got(&rx1), ["ack"]);

        links.drop_direction(NodeId(3), NodeId(1));
        links.hold(NodeId(1), NodeId(2));
        assert_eq!(links.send(NodeId(3), NodeId(1), msg("lost")), Ok(()));
        assert_eq!(links.send(NodeId(1), NodeId(2), msg("held")), Ok(()));
        assert_eq!(links.heal_all(), 1, "only the held frame is delivered");
        assert_eq!((links.held(), links.dropped()), (vec![], vec![]));
        assert_eq!(got(&rx2), ["held"]);
        assert_eq!(links.send(NodeId(3), NodeId(1), msg("after")), Ok(()));
        assert_eq!(got(&rx1), ["after"], "the lost one stays lost");
        assert_eq!(got(&rx3), Vec::<String>::new());
    }
}
