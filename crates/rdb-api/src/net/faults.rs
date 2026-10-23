//! Message-level faults for a running node (arch-s2 M5): read from a file, applied after TLS.
//!
//! `rdb_node` polls `<run>/faults/node-<n>.txt` every [`POLL`]. The file is the whole fault
//! state: a line removed is a fault lifted. One fault per line:
//!
//! * `hold <n>` / `hold all`: every frame to or from node `n` (or any peer, apps included)
//!   waits, in order, until the line goes; then it is delivered. A delay, never a loss.
//! * `drop-request N`: the `N`-th client call this node receives (counted from 1) is lost
//!   before the node sees it.
//! * `drop-reply N`: the `N`-th client call is served, and its answer is lost.
//! * `cut-control`: every rEtcd call of this node fails `Unavailable`, and its watches end
//!   ([`CutStore`]).
//!
//! Every client call is logged `net_client_request{n, client, request}`, so a discovery run
//! maps `N` to a request before a fault run names it. Blank lines and `#` comments are
//! skipped; any other line is refused by name in the log (`net_fault_line_refused`), never
//! guessed at.

use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, ListPage,
    ListRequest, ListResponse, MutationResponse, PageRequest, PutRequest, WatchItem,
    WatchRequest, WatchStream,
};
use futures::Stream;
use tracing::Instrument;
use rdb_core::contracts::ids::NodeId;

use super::wire::Label;

/// How often the fault file is read.
pub const POLL: Duration = Duration::from_millis(50);

/// What the fault file says now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FaultSet {
    /// `hold all`.
    pub hold_all: bool,
    /// `hold <n>`.
    pub held: BTreeSet<NodeId>,
    /// `drop-request N`.
    pub drop_request: BTreeSet<u64>,
    /// `drop-reply N`.
    pub drop_reply: BTreeSet<u64>,
    /// `cut-control`.
    pub cut_control: bool,
}

/// Parse a fault file. Returns the faults and the lines refused.
#[must_use]
pub fn parse(text: &str) -> (FaultSet, Vec<String>) {
    let mut set = FaultSet::default();
    let mut refused = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let words: Vec<&str> = line.split_whitespace().collect();
        let node = |word: &str| {
            word.strip_prefix("node-")
                .unwrap_or(word)
                .parse::<u32>()
                .ok()
                .map(NodeId)
        };
        let count = |word: &str| word.parse::<u64>().ok().filter(|n| *n > 0);
        let ok = match words.as_slice() {
            ["hold", "all"] => {
                set.hold_all = true;
                true
            }
            ["hold", peer] => node(peer).map(|n| set.held.insert(n)).is_some(),
            ["drop-request", n] => count(n).map(|n| set.drop_request.insert(n)).is_some(),
            ["drop-reply", n] => count(n).map(|n| set.drop_reply.insert(n)).is_some(),
            ["cut-control"] => {
                set.cut_control = true;
                true
            }
            _ => false,
        };
        if !ok {
            refused.push(line.to_owned());
        }
    }
    (set, refused)
}

/// The faults one node applies, shared by its links and its control store.
#[derive(Debug, Default)]
pub struct Faults {
    set: Mutex<FaultSet>,
    cut: Arc<AtomicBool>,
    client_requests: AtomicU64,
    /// The last file text, so a change is logged once.
    last: Mutex<String>,
}

impl Faults {
    /// No faults.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FaultSet> {
        self.set
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Apply a fault file's text. Logs each change once (`net_faults`), and each refused line.
    pub fn apply(&self, text: &str) {
        {
            let mut last = self
                .last
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *last == text {
                return;
            }
            text.clone_into(&mut last);
        }
        let (set, refused) = parse(text);
        for line in &refused {
            tracing::error!(
                %line,
                "net_fault_line_refused: want hold <n>|hold all|drop-request N|drop-reply N|cut-control"
            );
        }
        tracing::info!(
            hold_all = set.hold_all,
            held = ?set.held.iter().map(|n| n.0).collect::<Vec<_>>(),
            drop_request = ?set.drop_request,
            drop_reply = ?set.drop_reply,
            cut_control = set.cut_control,
            "net_faults"
        );
        self.cut.store(set.cut_control, Ordering::SeqCst);
        *self.lock() = set;
    }

    /// Whether frames to or from `peer` wait.
    #[must_use]
    pub fn holds(&self, peer: &Label) -> bool {
        let set = self.lock();
        set.hold_all || matches!(peer, Label::Node(node) if set.held.contains(node))
    }

    /// Count one client call received; its number, from 1.
    pub fn next_client_request(&self) -> u64 {
        self.client_requests.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Whether client call `n` is lost before the node sees it.
    #[must_use]
    pub fn drops_request(&self, n: u64) -> bool {
        self.lock().drop_request.contains(&n)
    }

    /// Whether client call `n`'s answer is lost.
    #[must_use]
    pub fn drops_reply(&self, n: u64) -> bool {
        self.lock().drop_reply.contains(&n)
    }

    /// Whether this node is cut from rEtcd.
    #[must_use]
    pub fn control_cut(&self) -> bool {
        self.cut.load(Ordering::SeqCst)
    }

    /// Read `path` every [`POLL`] and apply it, until the runtime stops. A missing file is no
    /// faults; a file that cannot be read keeps the last faults and says so once per error.
    pub fn watch_file(self: &Arc<Self>, path: std::path::PathBuf) -> tokio::task::JoinHandle<()> {
        let faults = Arc::clone(self);
        tokio::spawn(async move {
            let mut last_error = None;
            loop {
                match tokio::fs::read_to_string(&path).await {
                    Ok(text) => {
                        last_error = None;
                        faults.apply(&text);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        last_error = None;
                        faults.apply("");
                    }
                    Err(error) => {
                        let text = error.to_string();
                        if last_error.as_ref() != Some(&text) {
                            tracing::error!(path = %path.display(), error = %text, "net_fault_file_unreadable");
                            last_error = Some(text);
                        }
                    }
                }
                tokio::time::sleep(POLL).await;
            }
        }
        .instrument(tracing::Span::current()))
    }
}

/// A [`ConfigStore`] that fails every call while its node is cut from rEtcd (`cut-control`).
/// A watch open when the cut lands ends with an `Unavailable` error, as a lost connection
/// would end it.
pub struct CutStore {
    inner: Arc<dyn ConfigStore>,
    faults: Arc<Faults>,
}

impl CutStore {
    /// Wrap `inner`, cut whenever `faults` says so.
    #[must_use]
    pub fn new(inner: Arc<dyn ConfigStore>, faults: Arc<Faults>) -> Arc<Self> {
        Arc::new(Self { inner, faults })
    }

    fn check(&self, call: &'static str) -> Result<(), ConfigError> {
        if self.faults.control_cut() {
            tracing::info!(call, "net_control_cut");
            return Err(cut());
        }
        Ok(())
    }
}

fn cut() -> ConfigError {
    ConfigError::Unavailable {
        reason: "this node is cut from rEtcd (fault cut-control)".to_owned(),
    }
}

/// A watch that ends with [`cut`] once its node is cut.
struct CutWatch {
    inner: WatchStream,
    faults: Arc<Faults>,
    ended: bool,
}

impl Stream for CutWatch {
    type Item = Result<WatchItem, ConfigError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if self.faults.control_cut() {
            self.ended = true;
            return Poll::Ready(Some(Err(cut())));
        }
        let polled = self.inner.as_mut().poll_next(cx);
        // A pending stream is not woken by the cut itself; re-poll on the fault cadence so a
        // quiet watch still ends within one poll of the cut.
        if polled.is_pending() {
            let waker = cx.waker().clone();
            tokio::spawn(async move {
                tokio::time::sleep(POLL).await;
                waker.wake();
            });
        }
        polled
    }
}

#[async_trait::async_trait]
impl ConfigStore for CutStore {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.check("get")?;
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.check("list")?;
        self.inner.list(request).await
    }

    async fn list_page(&self, request: PageRequest) -> Result<ListPage, ConfigError> {
        self.check("list_page")?;
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.check("put")?;
        self.inner.put(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.check("delete")?;
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.check("watch")?;
        let inner = self.inner.watch(request).await?;
        Ok(Box::pin(CutWatch {
            inner,
            faults: Arc::clone(&self.faults),
            ended: false,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fault grammar: each kind parses, anything else is refused by line, never guessed.
    #[test]
    fn a_fault_file_parses_each_kind_and_refuses_the_rest() {
        let (set, refused) = parse(
            "# comment\n\nhold 2\nhold node-3\ndrop-request 4\ndrop-reply 7\ncut-control\n\
             hold\ndrop-request 0\nfreeze 2\n",
        );
        assert!(!set.hold_all);
        assert_eq!(set.held, BTreeSet::from([NodeId(2), NodeId(3)]));
        assert_eq!(set.drop_request, BTreeSet::from([4]));
        assert_eq!(set.drop_reply, BTreeSet::from([7]));
        assert!(set.cut_control);
        assert_eq!(refused, ["hold", "drop-request 0", "freeze 2"]);
        assert!(parse("hold all").0.hold_all);
    }

    /// `hold all` holds apps too; `hold <n>` only that node.
    #[test]
    fn a_hold_names_who_waits() {
        let faults = Faults::new();
        faults.apply("hold 2\n");
        assert!(faults.holds(&Label::Node(NodeId(2))));
        assert!(!faults.holds(&Label::Node(NodeId(3))));
        assert!(!faults.holds(&Label::App("a".to_owned())));
        faults.apply("hold all\n");
        assert!(faults.holds(&Label::App("a".to_owned())));
        faults.apply("");
        assert!(!faults.holds(&Label::Node(NodeId(2))));
    }
}
