//! In-process peer links (M9 architecture §3, `rdb_api::transport`).
//!
//! A concrete struct, no trait until M10. Each rDB node registers its mailbox. A peer message —
//! a replication frame, or a recovery request and its answer — goes through [`Links::send`] and
//! honours a hold. A held link buffers in order and delivers on [`Links::heal`], so a hold is a
//! delay, never a loss: the frames arrive late, as a slow network delivers them.
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

    /// Send a peer message from `from` to `to`, honouring a hold.
    ///
    /// # Errors
    ///
    /// [`LinkFault::Unreachable`] when `to` is not registered or its node has stopped.
    pub fn send(&self, from: NodeId, to: NodeId, msg: Msg) -> Result<(), LinkFault> {
        let mut inner = self.lock();
        if !inner.mailboxes.contains_key(&to) {
            return Err(LinkFault::Unreachable);
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

    /// Heal the link between `a` and `b` and deliver what it buffered, in order per direction.
    /// Returns how many messages were delivered.
    pub fn heal(&self, a: NodeId, b: NodeId) -> usize {
        let mut inner = self.lock();
        let removed = inner.held.remove(&pair(a, b));
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
        tracing::info!(a = a.0, b = b.0, removed, flushed, "link_heal");
        flushed
    }

    /// Heal every held link. Returns how many messages were delivered.
    pub fn heal_all(&self) -> usize {
        let held: Vec<_> = self.lock().held.iter().copied().collect();
        held.into_iter().map(|(a, b)| self.heal(a, b)).sum()
    }

    /// The held links, smaller node first.
    #[must_use]
    pub fn held(&self) -> Vec<(NodeId, NodeId)> {
        self.lock().held.iter().copied().collect()
    }
}
