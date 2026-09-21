//! M6-65..M6-84 — revision-pinned pagination on a live node (D6.3, ADR-0029).
//!
//! The token's bytes are `config-core`'s; the *behaviour* is here: one pinned revision across
//! a walk, a bounded LRU, a TTL on the injected clock, and a check order in which nothing
//! reads a key until every binding has been verified.
//!
//! Nothing here sleeps and nothing reads a wall clock. The TTL rows drive [`ManualClock`]
//! (anti-flake rule 33), and the eviction row lowers `max_pinned_snapshots` rather than
//! opening sixty-four walks.

mod common;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use common::{key, principal, Cluster};
use config_core::{
    open_token, seal_token, ConfigError, ConfigStore, ListRequest, NodeId, PageRequest,
    PageTokenExpiredReason, Pagination, Principal, PrincipalKind, Record, StatusClass,
    PAGE_TOKEN_VERSION, REASON_PREFIX_MISMATCH, REASON_TOKEN_PRINCIPAL,
};
use config_engine::{ConfigNode, ManualClock, PaginationConfig, Paginator};
use config_storage::EphemeralStore;

const TOKEN_KEY: [u8; 32] = [0x5a; 32];
/// Four, not sixty-four: eviction has to be reachable inside a test (test plan §5 config).
const MAX_PINNED: u32 = 4;
const TTL: Duration = Duration::from_secs(60);
const PAGE: u32 = 10;
const POPULATION: usize = 100;

/// One node, a hundred keys under `/p/`, and a paginator wired to a clock a test controls.
struct Fixture {
    cluster: Cluster,
    clock: Arc<ManualClock>,
    paginator: Arc<Paginator>,
}

impl Fixture {
    async fn start() -> Self {
        Self::start_with(MAX_PINNED, TTL).await
    }

    async fn start_with(max_pinned: u32, ttl: Duration) -> Self {
        let cluster = Cluster::formed(1).await;
        let clock = Arc::new(ManualClock::new());
        // The clock starts at zero and the paginator records that as its start instant, so a
        // token minted "before this process existed" has to be constructed deliberately
        // (m6_83) rather than happening by accident to every token in the suite.
        clock.advance(Duration::from_secs(1_000));
        let paginator = Arc::new(paginator_for(
            cluster.store(NodeId(1)),
            NodeId(1),
            Arc::clone(&clock),
            max_pinned,
            ttl,
        ));

        let fixture = Self {
            cluster,
            clock,
            paginator,
        };
        fixture.populate().await;
        fixture
    }

    async fn populate(&self) {
        for i in 0..POPULATION {
            self.cluster
                .put(NodeId(1), &format!("/p/{i:04}"), "v")
                .await
                .expect("seed put");
        }
    }

    fn node(&self) -> &ConfigNode {
        self.cluster.node(1)
    }

    /// One page, as `principal()`, over `/p/`.
    async fn page(
        &self,
        token: Option<bytes::Bytes>,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.page_as(&principal(), "/p/", token).await
    }

    async fn page_as(
        &self,
        who: &Principal,
        prefix: &str,
        token: Option<bytes::Bytes>,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.paginator
            .list_page(
                self.node(),
                who,
                PageRequest {
                    list: ListRequest {
                        prefix: key(prefix),
                        max_items: PAGE,
                        max_bytes: 0,
                    },
                    page_token: token,
                },
            )
            .await
    }

    /// Walk to exhaustion, returning every record and every revision reported on the way.
    async fn walk(&self) -> (Vec<Record>, Vec<u64>) {
        let mut all = Vec::new();
        let mut revisions = Vec::new();
        let mut token = None;
        loop {
            let page = self.page(token).await.expect("page");
            revisions.push(page.revision);
            all.extend(page.items.clone());
            match page.next_page_token {
                Some(next) => token = Some(next),
                None => {
                    assert!(!page.truncated, "the last page is not truncated");
                    return (all, revisions);
                }
            }
        }
    }
}

fn paginator_for(
    store: &EphemeralStore,
    node_id: NodeId,
    clock: Arc<ManualClock>,
    max_pinned: u32,
    ttl: Duration,
) -> Paginator {
    Paginator::new(
        node_id,
        store.reader(),
        clock,
        config_core::Limits::DEFAULT,
        PaginationConfig {
            max_pinned,
            ttl,
            token_key: TOKEN_KEY,
        },
    )
}

// ---------------------------------------------------------------------------
// M6-65 / M6-66 — the walk itself
// ---------------------------------------------------------------------------

/// M6-65 — every key exactly once, in order, at one revision, ending cleanly.
#[config_log::retcd_test]
async fn m6_65_page_token_round_trip_returns_every_key_once() {
    let fixture = Fixture::start().await;

    let (records, revisions) = fixture.walk().await;

    let keys: Vec<Vec<u8>> = records.iter().map(|r| r.key.to_vec()).collect();
    let expected: Vec<Vec<u8>> = (0..POPULATION)
        .map(|i| format!("/p/{i:04}").into_bytes())
        .collect();
    assert_eq!(
        keys, expected,
        "the concatenated pages are the whole prefix, in unsigned bytewise order, once each"
    );
    assert_eq!(
        revisions.len(),
        POPULATION / PAGE as usize,
        "a hundred keys at ten a page is ten pages"
    );
    assert!(
        revisions.windows(2).all(|w| w[0] == w[1]),
        "every page reports the same read_revision: {revisions:?}"
    );
}

/// M6-66 — the row the whole feature exists for.
///
/// Fifty puts and twenty deletes land *inside* the prefix while the walk is in flight, hitting
/// both keys already returned and keys not yet reached. The walk must return the state as of
/// the pin and nothing else.
#[config_log::retcd_test]
async fn m6_66_pages_are_consistent_at_one_revision_under_concurrent_writes() {
    let fixture = Fixture::start().await;

    let first = fixture.page(None).await.expect("page 1");
    let pinned_revision = first.revision;
    let mut all = first.items.clone();
    let mut token = first.next_page_token.expect("more pages");

    // Already returned (0000..0009), not yet reached (0050..0089), and brand new (9000..9009).
    for i in 0..10 {
        fixture
            .cluster
            .put(NodeId(1), &format!("/p/{i:04}"), "rewritten")
            .await
            .expect("overwrite a returned key");
        fixture
            .cluster
            .put(NodeId(1), &format!("/p/{:04}", 9000 + i), "new")
            .await
            .expect("create a key after the pin");
    }
    for i in 50..80 {
        fixture
            .cluster
            .put(NodeId(1), &format!("/p/{i:04}"), "rewritten")
            .await
            .expect("overwrite an unreached key");
    }
    for i in 80..100 {
        fixture
            .cluster
            .delete(NodeId(1), &format!("/p/{i:04}"))
            .await
            .expect("delete an unreached key");
    }

    loop {
        let page = fixture.page(Some(token)).await.expect("page");
        assert_eq!(
            page.revision, pinned_revision,
            "a page served after 70 mutations still reports the pinned revision"
        );
        all.extend(page.items.clone());
        match page.next_page_token {
            Some(next) => token = next,
            None => break,
        }
    }

    let keys: Vec<Vec<u8>> = all.iter().map(|r| r.key.to_vec()).collect();
    let expected: Vec<Vec<u8>> = (0..POPULATION)
        .map(|i| format!("/p/{i:04}").into_bytes())
        .collect();
    assert_eq!(
        keys, expected,
        "no key created after the pin appears, and no key deleted after it is missing"
    );
    assert!(
        all.iter().all(|r| r.mod_revision <= pinned_revision),
        "every returned record was already at its pinned-revision value"
    );
    assert!(
        all.iter().all(|r| r.value.as_ref() == b"v"),
        "a value rewritten after the pin must not leak into a later page"
    );
}

// ---------------------------------------------------------------------------
// M6-67..M6-71 — the five refusals, each on its own
// ---------------------------------------------------------------------------

/// M6-67 — a tampered token is `mac`, and the counter moves with it.
#[config_log::retcd_test]
async fn m6_67_tampered_token_mac_is_rejected() {
    let fixture = Fixture::start().await;
    let token = fixture
        .page(None)
        .await
        .expect("page 1")
        .next_page_token
        .expect("a cursor");

    let before = fixture.paginator.stats().misses_by_reason["mac"];
    for index in [0usize, 5, token.len() - 1] {
        let mut forged = token.to_vec();
        forged[index] ^= 0b0100_0000;
        let err = fixture
            .page(Some(forged.into()))
            .await
            .expect_err("a flipped bit");
        assert_expired(&err, PageTokenExpiredReason::Mac);
    }
    assert_eq!(
        fixture.paginator.stats().misses_by_reason["mac"] - before,
        3,
        "one counter increment per rejected attempt (Q-30)"
    );

    // The untampered token still works: the three refusals did not disturb the pin.
    fixture.page(Some(token)).await.expect("page 2");
}

/// M6-68 — the TTL releases the pin, and the release is observable.
#[config_log::retcd_test]
async fn m6_68_expired_token_is_rejected_by_ttl() {
    let fixture = Fixture::start_with(MAX_PINNED, Duration::from_secs(60)).await;
    let token = fixture
        .page(None)
        .await
        .expect("page 1")
        .next_page_token
        .expect("a cursor");
    assert_eq!(fixture.paginator.stats().len, 1, "one walk, one pin");

    fixture.clock.advance(Duration::from_secs(61));

    let err = fixture.page(Some(token)).await.expect_err("past the TTL");
    assert_expired(&err, PageTokenExpiredReason::Expired);

    let stats = fixture.paginator.stats();
    assert_eq!(
        stats.len, 0,
        "a pin that outlives its token would pin SST files forever (§19.12)"
    );
    assert_eq!(stats.expiries, 1, "the release is counted, not silent");
}

/// M6-69 — the LRU is bounded, and the *other* walks survive being bounded.
///
/// Each walk pins a different revision (a write between walks moves it), so five walks need
/// five entries in a table of four. A build that dropped all of them would satisfy "the cap
/// exists" and fail this row.
#[config_log::retcd_test]
async fn m6_69_lru_eviction_rejects_the_oldest_token() {
    let fixture = Fixture::start_with(4, TTL).await;

    let mut tokens = Vec::new();
    for i in 0..5 {
        if i > 0 {
            // Move the revision, so walk `i` pins its own snapshot rather than sharing one.
            fixture
                .cluster
                .put(NodeId(1), &format!("/other/{i}"), "v")
                .await
                .expect("bump the revision");
        }
        tokens.push(
            fixture
                .page(None)
                .await
                .expect("page 1")
                .next_page_token
                .expect("a cursor"),
        );
    }

    let stats = fixture.paginator.stats();
    assert_eq!(stats.len, 4, "the table never exceeds its capacity");
    assert_eq!(stats.evictions, 1, "exactly one pin was dropped");

    let err = fixture
        .page(Some(tokens[0].clone()))
        .await
        .expect_err("the least recently used walk");
    assert_expired(&err, PageTokenExpiredReason::Evicted);

    for (i, token) in tokens.iter().enumerate().skip(1) {
        let page = fixture
            .page(Some(token.clone()))
            .await
            .unwrap_or_else(|e| panic!("walk {i} must survive the eviction: {e}"));
        assert_eq!(
            page.items.len(),
            PAGE as usize,
            "walk {i} continues normally"
        );
    }
}

/// M6-70 — a token minted by another node is `node`, and the documented recovery works.
#[config_log::retcd_test]
async fn m6_70_token_from_another_leader_is_rejected() {
    let fixture = Fixture::start().await;
    let real = fixture
        .page(None)
        .await
        .expect("page 1")
        .next_page_token
        .expect("a cursor");

    // Same key, same pin, same revision — only the issuing node differs, which is exactly the
    // state a client is in after a failover: it holds a valid token for a pin that is
    // somewhere else.
    let mut foreign = open_token(&real, &TOKEN_KEY).expect("our own token");
    foreign.node_id = NodeId(7);
    let forged = seal_token(&foreign, &TOKEN_KEY);

    let err = fixture
        .page(Some(forged))
        .await
        .expect_err("another node's pin");
    assert_expired(&err, PageTokenExpiredReason::Node);

    // The documented recovery: restart the walk. It succeeds immediately.
    let restarted = fixture.page(None).await.expect("a fresh walk");
    assert_eq!(restarted.items.len(), PAGE as usize);
}

/// G-04 — a continuation that reached a follower is still refused, and now says where to go.
///
/// The refusal is unchanged and deliberately so: the pin is on the leader, so the token really
/// is unusable here and `reason` stays `node` for every client that only reads `reason`
/// (M6-70 and M6-83 above are those clients, and neither moves). What is new is that the
/// refusal names the leader, so the restarted walk does not first have to spend a `NotLeader`
/// round trip finding it.
///
/// The hint's *content* is what is asserted, against the hint the node's own `NotLeader` path
/// produces — a row that only checked `is_some()` would pass against a hint naming the wrong
/// node, which is worse than no hint at all.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn g_04_a_followers_page_token_refusal_carries_the_leader_hint() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let follower = cluster.followers()[0];
    for i in 0..POPULATION {
        cluster
            .put(leader, &format!("/p/{i:04}"), "v")
            .await
            .expect("seed put");
    }

    let clock = Arc::new(ManualClock::new());
    clock.advance(Duration::from_secs(1_000));
    let on_leader = paginator_for(
        cluster.store(leader),
        leader,
        Arc::clone(&clock),
        MAX_PINNED,
        TTL,
    );
    let on_follower = paginator_for(
        cluster.store(follower),
        follower,
        Arc::clone(&clock),
        MAX_PINNED,
        TTL,
    );
    // Both tables exist before the token is minted, so `issued_ms < started_ms` cannot fire and
    // the refusal below can only be the wrong-node clause.
    clock.advance(Duration::from_secs(1));

    let walk = || ListRequest {
        prefix: key("/p/"),
        max_items: PAGE,
        max_bytes: 0,
    };
    let token = on_leader
        .list_page(
            cluster.get_node(leader),
            &principal(),
            PageRequest {
                list: walk(),
                page_token: None,
            },
        )
        .await
        .expect("page 1 from the leader")
        .next_page_token
        .expect("a cursor");

    // The misdirected continuation: a real token for a real pin, arriving at the wrong node.
    let err = on_follower
        .list_page(
            cluster.get_node(follower),
            &principal(),
            PageRequest::resume(walk(), token.clone()),
        )
        .await
        .expect_err("the follower has no such pin");

    // The oracle: the hint this same follower hands out on the established `NotLeader` path.
    // Taken from the node rather than from `leader`, so the assertion is about agreement
    // between the two paths and not about a value the test computed for itself.
    let oracle = match cluster
        .put(follower, "/p/unwritable", "v")
        .await
        .expect_err("a follower refuses a write")
    {
        ConfigError::NotLeader { hint: Some(hint) } => hint,
        other => panic!("expected NotLeader with a hint, got {other}"),
    };
    assert_eq!(oracle.node_id, leader, "the oracle names the real leader");

    assert_expired(&err, PageTokenExpiredReason::Node);
    match err {
        ConfigError::PageTokenExpired {
            hint: Some(hint), ..
        } => assert_eq!(
            hint, oracle,
            "the page-token hint is the same node and the same client endpoint the \
             `NotLeader` path would have given"
        ),
        other => panic!("expected a leader hint on the refusal, got {other}"),
    }

    // And the rule that keeps a hint honest: a node never points a caller back at itself. The
    // leader refusing a token minted by some earlier leader knows only one leader — itself —
    // so it withholds the hint rather than offering a redirect into the same refusal.
    let mut foreign = open_token(&token, &TOKEN_KEY).expect("our own token");
    foreign.node_id = NodeId(7);
    let stale = on_leader
        .list_page(
            cluster.get_node(leader),
            &principal(),
            PageRequest::resume(walk(), seal_token(&foreign, &TOKEN_KEY)),
        )
        .await
        .expect_err("a token from a node that is not this one");
    assert_expired(&stale, PageTokenExpiredReason::Node);
    assert!(
        matches!(stale, ConfigError::PageTokenExpired { hint: None, .. }),
        "no self-redirect: {stale}"
    );

    cluster.shutdown().await;
}

/// M6-83 — a pin does not survive a restart, and says so with **one** reason.
///
/// Modelled as a paginator built after the token was issued: that is what a restart is from
/// the pin table's point of view — same node id, empty table, and a token from before it
/// existed. The reason must be `node`, not a race between `evicted` and `expired`.
#[config_log::retcd_test]
async fn m6_83_pins_do_not_survive_a_restart() {
    let fixture = Fixture::start().await;
    let token = fixture
        .page(None)
        .await
        .expect("page 1")
        .next_page_token
        .expect("a cursor");

    fixture.clock.advance(Duration::from_secs(1));
    let restarted = Arc::new(paginator_for(
        fixture.cluster.store(NodeId(1)),
        NodeId(1),
        Arc::clone(&fixture.clock),
        MAX_PINNED,
        TTL,
    ));
    assert_eq!(restarted.stats().len, 0, "a restarted table is empty");

    let err = restarted
        .list_page(
            fixture.node(),
            &principal(),
            PageRequest::resume(
                ListRequest {
                    prefix: key("/p/"),
                    max_items: PAGE,
                    max_bytes: 0,
                },
                token,
            ),
        )
        .await
        .expect_err("a token from before this process");
    assert_expired(&err, PageTokenExpiredReason::Node);
    assert_eq!(
        restarted.stats().misses_by_reason["evicted"],
        0,
        "one reason, deterministically: `evicted` must not also fire"
    );
}

/// M6-71 — a policy version change invalidates the token before any key is read.
#[config_log::retcd_test]
async fn m6_71_policy_version_change_rejects_the_token() {
    let cluster = Cluster::formed(1).await;
    let clock = Arc::new(ManualClock::new());
    clock.advance(Duration::from_secs(1_000));
    let policy = Arc::new(AtomicU64::new(7));
    let mut paginator = paginator_for(
        cluster.store(NodeId(1)),
        NodeId(1),
        Arc::clone(&clock),
        MAX_PINNED,
        TTL,
    );
    paginator.bind_policy_version(Arc::clone(&policy));
    let paginator = Arc::new(paginator);
    for i in 0..POPULATION {
        cluster
            .put(NodeId(1), &format!("/p/{i:04}"), "v")
            .await
            .expect("seed put");
    }

    let request = |token: Option<bytes::Bytes>| PageRequest {
        list: ListRequest {
            prefix: key("/p/"),
            max_items: PAGE,
            max_bytes: 0,
        },
        page_token: token,
    };

    let token = paginator
        .list_page(cluster.node(1), &principal(), request(None))
        .await
        .expect("page 1 under policy v7")
        .next_page_token
        .expect("a cursor");

    policy.store(8, std::sync::atomic::Ordering::Relaxed);

    let err = paginator
        .list_page(cluster.node(1), &principal(), request(Some(token)))
        .await
        .expect_err("a token minted under v7");
    assert_expired(&err, PageTokenExpiredReason::PolicyVersion);
    assert_eq!(
        paginator.stats().misses_by_reason["policy_version"],
        1,
        "the counter and the reason are the same closed set (Q-30)"
    );
}

// ---------------------------------------------------------------------------
// M6-76..M6-78 — the bindings that are *not* expiries
// ---------------------------------------------------------------------------

/// M6-76 — a mutated prefix is a caller bug, refused before any key is read.
#[config_log::retcd_test]
async fn m6_76_token_is_bound_to_the_requested_prefix() {
    let fixture = Fixture::start().await;
    for i in 0..5 {
        fixture
            .cluster
            .put(NodeId(1), &format!("/q/{i:04}"), "secret")
            .await
            .expect("seed the other prefix");
    }
    let token = fixture
        .page(None)
        .await
        .expect("page 1")
        .next_page_token
        .expect("a cursor");

    let err = fixture
        .page_as(&principal(), "/q/", Some(token))
        .await
        .expect_err("a token from a different prefix");

    assert_eq!(
        err.kind(),
        StatusClass::InvalidArgument,
        "OQ-62: a client bug, not an expiry — retrying would loop"
    );
    assert!(
        matches!(&err, ConfigError::InvalidArgument { detail } if detail == REASON_PREFIX_MISMATCH),
        "the documented detail string: {err}"
    );
    assert!(
        !matches!(err, ConfigError::PageTokenExpired { .. }),
        "and explicitly not an expiry"
    );
    // Nothing under `/q/` was read to reach that decision.
    assert_eq!(
        fixture.paginator.stats().hits,
        0,
        "the refusal happened before the pin was even consulted"
    );
}

/// M6-77 — a token is not transferable between principals.
#[config_log::retcd_test]
async fn m6_77_token_is_bound_to_the_principal() {
    let fixture = Fixture::start().await;
    let token = fixture
        .page(None)
        .await
        .expect("page 1")
        .next_page_token
        .expect("a cursor");

    let other = Principal::new("client-y", PrincipalKind::Development);
    let err = fixture
        .page_as(&other, "/p/", Some(token))
        .await
        .expect_err("another principal's cursor");

    assert_eq!(
        err.kind(),
        StatusClass::PermissionDenied,
        "a transferable cursor is a lateral-movement primitive"
    );
    assert!(
        matches!(&err, ConfigError::PermissionDenied { detail } if detail == REASON_TOKEN_PRINCIPAL),
        "the documented detail string: {err}"
    );
    assert!(
        !err.to_string().contains(&principal().name),
        "the refusal never reveals the identity the token was issued to: {err}"
    );
}

/// M6-78 — an unknown envelope version is refused on a live node too.
#[config_log::retcd_test]
async fn m6_78_token_version_is_explicit_and_unknown_versions_are_rejected() {
    let fixture = Fixture::start().await;
    let real = fixture
        .page(None)
        .await
        .expect("page 1")
        .next_page_token
        .expect("a cursor");

    let mut future = open_token(&real, &TOKEN_KEY).expect("our own token");
    assert_eq!(future.token_version, PAGE_TOKEN_VERSION);
    future.token_version = 2;

    let err = fixture
        .page(Some(seal_token(&future, &TOKEN_KEY)))
        .await
        .expect_err("a version this build does not implement");
    assert_expired(&err, PageTokenExpiredReason::TokenVersion);
}

/// M6-79 — rotating the token key invalidates every outstanding token, and new walks work.
#[config_log::retcd_test]
async fn m6_79_rotating_the_token_key_invalidates_outstanding_tokens() {
    let fixture = Fixture::start().await;
    let token = fixture
        .page(None)
        .await
        .expect("page 1")
        .next_page_token
        .expect("a cursor");

    let rotated = Arc::new(Paginator::new(
        NodeId(1),
        fixture.cluster.store(NodeId(1)).reader(),
        Arc::clone(&fixture.clock) as Arc<dyn config_engine::LeaderClock>,
        config_core::Limits::DEFAULT,
        PaginationConfig {
            max_pinned: MAX_PINNED,
            ttl: TTL,
            token_key: [0x11; 32],
        },
    ));

    let request = |token: Option<bytes::Bytes>| PageRequest {
        list: ListRequest {
            prefix: key("/p/"),
            max_items: PAGE,
            max_bytes: 0,
        },
        page_token: token,
    };

    let err = rotated
        .list_page(fixture.node(), &principal(), request(Some(token)))
        .await
        .expect_err("a token minted under the previous key");
    assert_expired(&err, PageTokenExpiredReason::Mac);

    let fresh = rotated
        .list_page(fixture.node(), &principal(), request(None))
        .await
        .expect("a walk started under the new key");
    assert!(
        fresh.next_page_token.is_some(),
        "rotation does not break pagination, only outstanding cursors"
    );
}

// ---------------------------------------------------------------------------
// M6-73, M6-80..M6-84 — caps, capability, isolation, and the M3 contract
// ---------------------------------------------------------------------------

/// M6-80 — both caps are clamped, and a page stopped by bytes still hands back a cursor.
#[config_log::retcd_test]
async fn m6_80_max_items_and_max_bytes_are_both_honoured_and_capped() {
    let fixture = Fixture::start().await;

    let huge = fixture
        .paginator
        .list_page(
            fixture.node(),
            &principal(),
            PageRequest::first(ListRequest {
                prefix: key("/p/"),
                max_items: 10_000,
                max_bytes: 1 << 30,
            }),
        )
        .await
        .expect("an over-large request");
    let cap = config_core::Limits::DEFAULT.max_list_items as usize;
    assert!(
        huge.items.len() <= cap,
        "max_items is clamped to the server cap, not honoured as asked"
    );

    // A byte cap small enough that it, not the item cap, ends the page.
    let by_bytes = fixture
        .paginator
        .list_page(
            fixture.node(),
            &principal(),
            PageRequest::first(ListRequest {
                prefix: key("/p/"),
                max_items: 50,
                max_bytes: 100,
            }),
        )
        .await
        .expect("a byte-bounded page");
    assert!(
        by_bytes.items.len() < 50,
        "the byte cap, not the item cap, ended this page"
    );
    assert!(
        by_bytes.truncated && by_bytes.next_page_token.is_some(),
        "a page that hits max_bytes first still returns a usable cursor"
    );
}

/// M6-73 — the capability reports the configured bounds, and `Unsupported` without a key.
#[config_log::retcd_test]
async fn m6_73_capability_reports_revision_pinned_pagination() {
    let fixture = Fixture::start().await;

    let paginating = fixture
        .node()
        .direct_client(principal())
        .with_pagination(Arc::clone(&fixture.paginator));
    assert_eq!(
        paginating.capabilities().pagination,
        Pagination::RevisionPinned {
            max_pinned: MAX_PINNED,
            ttl_ms: TTL.as_millis() as u64,
        },
        "the effective bounds, not a bare boolean"
    );

    // A build with no token key has no paginator, so it keeps reporting `Unsupported` and
    // refuses to issue a token rather than issuing an unauthenticated one.
    let plain = fixture.node().direct_client(principal());
    assert_eq!(plain.capabilities().pagination, Pagination::Unsupported);
    let refused = plain
        .list_page(PageRequest::resume(
            ListRequest {
                prefix: key("/p/"),
                max_items: PAGE,
                max_bytes: 0,
            },
            bytes::Bytes::from_static(b"whatever"),
        ))
        .await
        .expect_err("a token against a node without pagination");
    assert_eq!(refused.kind(), StatusClass::Unavailable);
}

/// M6-81 — a held pin does not stop Raft applying.
///
/// Asserts *progress*, not wall-clock time (anti-flake rule 23): with four pins held, a write
/// burst still commits, every write is visible, and the table never exceeds its cap.
#[config_log::retcd_test]
async fn m6_81_a_pinned_snapshot_does_not_block_raft_apply() {
    let fixture = Fixture::start_with(4, TTL).await;

    let mut held = Vec::new();
    for i in 0..4 {
        if i > 0 {
            fixture
                .cluster
                .put(NodeId(1), &format!("/other/{i}"), "v")
                .await
                .expect("bump the revision");
        }
        held.push(fixture.page(None).await.expect("page 1"));
    }
    assert_eq!(fixture.paginator.stats().len, 4, "four pins held");

    let before = fixture.node().applied_index();
    for i in 0..50 {
        fixture
            .cluster
            .put(NodeId(1), &format!("/burst/{i:04}"), "v")
            .await
            .expect("a write while four snapshots are pinned");
    }
    let after = fixture.node().applied_index();

    assert!(
        after >= before + 50,
        "apply advanced by at least the burst ({before} -> {after})"
    );
    assert_eq!(
        fixture
            .cluster
            .list(NodeId(1), "/burst/")
            .await
            .expect("list the burst")
            .records
            .len(),
        50,
        "every write landed and is readable"
    );
    assert!(
        fixture.paginator.stats().len <= 4,
        "the table never exceeds its cap under write pressure"
    );

    // And the pinned walks are untouched by all of it.
    for page in held {
        let token = page.next_page_token.expect("a cursor");
        let next = fixture.page(Some(token)).await.expect("continue");
        assert_eq!(next.revision, page.revision, "still the pinned revision");
    }
}

/// M6-81, the compaction half — a pin outlives a compaction that reclaimed its revision.
///
/// The claim is not "a `Compact` may run next to a pin". On this backend a pin is a clone of
/// the record map ([`config_storage::PinnedView`]), so a walk that never pinned anything would
/// also survive a bare compaction, and a row that only ran one would pass against a completely
/// broken pin. The claim is the one §19.12 actually makes: the replicated `Compact` applies —
/// a pin never blocks it — *and* the walk still reads the state it pinned afterwards, from a
/// revision the journal has since reclaimed.
///
/// So the row makes the compaction bite before it asserts survival. It deletes half the prefix
/// the walk has not reached yet, compacts past the walk's own revision, and proves the
/// reclamation two ways: `compact_revision` is above the pinned revision, and a watcher asking
/// to replay that revision is refused `RevisionCompacted`. Only then does it resume the walk.
#[config_log::retcd_test]
async fn m6_81_a_pin_survives_a_compaction_that_reclaimed_its_revision() {
    let fixture = Fixture::start().await;

    // Page one pins the walk's revision. Pages 6..10 are still owed, and are exactly the keys
    // the deletes below remove from live state.
    let first = fixture.page(None).await.expect("page 1");
    let pinned_revision = first.revision;
    let token = first.next_page_token.clone().expect("a cursor");
    assert_eq!(fixture.paginator.stats().len, 1, "one pin held");

    // Half the prefix leaves live state, so the pinned content and the live content differ by
    // something the walk has not yet returned.
    let mut last = 0;
    for i in (POPULATION / 2)..POPULATION {
        last = fixture
            .cluster
            .delete(NodeId(1), &format!("/p/{i:04}"))
            .await
            .expect("delete a key the walk has not reached")
            .revision;
    }

    // The replicated `Compact` applies while the pin is held — the test plan's own half of the
    // row — and it compacts *past* the pinned revision rather than up to some earlier one.
    let floor = fixture
        .node()
        .propose_compact(&principal(), last)
        .await
        .expect("a compaction must not be blocked by a held pin");
    assert!(
        floor > pinned_revision,
        "the compaction has to reach past the pin to be testing anything \
         (floor {floor}, pinned {pinned_revision})"
    );
    assert_eq!(
        fixture.node().compact_revision(),
        floor,
        "the floor the node is holding"
    );

    // What "reclaimed" means, stated as a refusal rather than as a number: a consumer asking to
    // replay the walk's own revision is told that revision is gone. This is the assertion that
    // makes the survival below mean something.
    let replay = fixture
        .node()
        .watch(
            &principal(),
            config_core::WatchRequest {
                prefix: key("/p/"),
                start_after_revision: pinned_revision - 1,
                progress_interval: Some(Duration::from_secs(3600)),
            },
        )
        .await
        .err();
    match replay {
        Some(ConfigError::RevisionCompacted {
            minimum_available_revision,
        }) => assert!(
            minimum_available_revision > pinned_revision,
            "history at the pinned revision was reclaimed \
             (minimum {minimum_available_revision}, pinned {pinned_revision})"
        ),
        other => panic!("expected the pinned revision to be reclaimed, got {other:?}"),
    }

    // The counterfactual, so the survival is not read as "nothing changed": an unpinned walk
    // would now see half the prefix.
    assert_eq!(
        fixture
            .cluster
            .list(NodeId(1), "/p/")
            .await
            .expect("list live state")
            .records
            .len(),
        POPULATION / 2,
        "live state lost the second half"
    );

    // And the pin survived all of it: the walk finishes at its own revision, returning keys
    // that live state no longer has.
    let mut items = first.items.clone();
    let mut token = Some(token);
    while let Some(cursor) = token.take() {
        let page = fixture
            .page(Some(cursor))
            .await
            .expect("a continuation across the compaction");
        assert_eq!(
            page.revision, pinned_revision,
            "every page still reports the pinned revision"
        );
        items.extend(page.items.clone());
        token = page.next_page_token;
    }
    let keys: Vec<Vec<u8>> = items.iter().map(|r| r.key.to_vec()).collect();
    let expected: Vec<Vec<u8>> = (0..POPULATION)
        .map(|i| format!("/p/{i:04}").into_bytes())
        .collect();
    assert_eq!(
        keys, expected,
        "the whole prefix as it was at the pinned revision, including the deleted half"
    );
}

/// M6-82 — an abandoned walk's pin is released by the TTL rather than leaking.
#[config_log::retcd_test]
async fn m6_82_pins_are_released_when_a_walk_is_abandoned() {
    let fixture = Fixture::start_with(4, Duration::from_secs(60)).await;

    for i in 0..4 {
        if i > 0 {
            fixture
                .cluster
                .put(NodeId(1), &format!("/other/{i}"), "v")
                .await
                .expect("bump the revision");
        }
        // The cursor is dropped on the floor: this is a client that walked away.
        let _abandoned = fixture.page(None).await.expect("page 1").next_page_token;
    }
    assert_eq!(fixture.paginator.stats().len, 4, "four abandoned pins");

    fixture.clock.advance(Duration::from_secs(61));
    // Any table operation sweeps; a fresh walk is the cheapest one to drive here.
    fixture.page(None).await.expect("a new walk");

    let stats = fixture.paginator.stats();
    assert_eq!(
        stats.len, 1,
        "only the new walk's pin survives; the four abandoned ones were released"
    );
    assert_eq!(stats.expiries, 4, "each release is counted");
}

/// M6-84 — a `List` with no token is exactly the M3 call, and creates no pin.
#[config_log::retcd_test]
async fn m6_84_list_without_a_token_keeps_exact_m3_semantics() {
    let fixture = Fixture::start().await;

    let m3 = fixture
        .cluster
        .node(1)
        .list(
            &principal(),
            ListRequest {
                prefix: key("/p/"),
                max_items: PAGE,
                max_bytes: 0,
            },
        )
        .await
        .expect("the M3 List");

    assert!(m3.truncated, "ten of a hundred keys is a truncated M3 read");
    assert_eq!(m3.records.len(), PAGE as usize);
    assert_eq!(
        fixture.paginator.stats().len,
        0,
        "`list` creates no pin, computes no MAC, and costs an existing caller nothing"
    );

    // The paginated first page returns the same records; it is the *cursor* that is new.
    let page = fixture.page(None).await.expect("the paginated first page");
    assert_eq!(
        page.items, m3.records,
        "opting in changes what comes back with the page, not what is in it"
    );
    assert!(page.next_page_token.is_some());
    assert_eq!(fixture.paginator.stats().len, 1, "opting in pins once");
}

/// M6-72 — the pinned walk is served from the ephemeral store's clone-on-pin snapshot.
///
/// The persistent backend's parity row waits on `RocksStore::pin` (a patch note to
/// `config-storage/src/rocks.rs`); until it lands, this row pins the behaviour the engine
/// requires of *any* backend, and the default `StateReader::pin` proves a backend that cannot
/// pin says so instead of serving a drifting walk.
#[config_log::retcd_test]
async fn m6_72_a_backend_that_cannot_pin_refuses_instead_of_drifting() {
    let fixture = Fixture::start().await;
    // The ephemeral backend does pin, and the whole suite above runs on it.
    assert!(
        fixture
            .page(None)
            .await
            .expect("page 1")
            .next_page_token
            .is_some(),
        "the ephemeral store's clone-on-pin snapshot supports a walk"
    );

    let blind = Arc::new(Paginator::new(
        NodeId(1),
        Arc::new(NoPin) as Arc<dyn config_storage::StateReader>,
        Arc::clone(&fixture.clock) as Arc<dyn config_engine::LeaderClock>,
        config_core::Limits::DEFAULT,
        PaginationConfig::new(TOKEN_KEY),
    ));
    let err = blind
        .list_page(
            fixture.node(),
            &principal(),
            PageRequest::first(ListRequest {
                prefix: key("/p/"),
                max_items: PAGE,
                max_bytes: 0,
            }),
        )
        .await
        .expect_err("a backend with the default `pin`");
    assert_eq!(
        err.kind(),
        StatusClass::Unavailable,
        "not activated here, and retryable once it is: {err}"
    );
    assert!(
        matches!(&err, ConfigError::Unavailable { reason } if reason == config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED),
        "the reserved reason string: {err}"
    );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

#[track_caller]
fn assert_expired(err: &ConfigError, expected: PageTokenExpiredReason) {
    assert_eq!(
        err.kind(),
        StatusClass::FailedPrecondition,
        "an expiry is a precondition failure: {err}"
    );
    match err {
        ConfigError::PageTokenExpired { reason, .. } => assert_eq!(
            *reason, expected,
            "expected reason `{expected}`, got `{reason}`"
        ),
        other => panic!("expected PageTokenExpired{{{expected}}}, got {other}"),
    }
}

/// A reader that inherits the default `pin` — i.e. every pre-M6 store, and any backend whose
/// pinning is not wired up yet.
struct NoPin;

impl config_storage::StateReader for NoPin {
    fn with_state(&self, _f: &mut dyn FnMut(&config_core::KvState)) {}

    fn last_applied(&self) -> Option<openraft::LogId<config_storage::RaftNodeId>> {
        None
    }

    fn membership(
        &self,
    ) -> openraft::StoredMembership<config_storage::RaftNodeId, config_storage::RaftNode> {
        openraft::StoredMembership::default()
    }

    fn compact_revision(&self) -> Result<u64, config_storage::StorageReadError> {
        Ok(0)
    }

    fn read_events(
        &self,
        _from_exclusive: u64,
        _to_inclusive: u64,
        _prefix: &[u8],
        _limit: usize,
    ) -> Result<Vec<config_core::MutationEvent>, config_storage::StorageReadError> {
        Ok(Vec::new())
    }

    fn journal_stats(
        &self,
    ) -> Result<config_storage::JournalStats, config_storage::StorageReadError> {
        Ok(config_storage::JournalStats::default())
    }

    fn journal_hash(
        &self,
        _from_exclusive: u64,
    ) -> Result<[u8; 32], config_storage::StorageReadError> {
        Ok([0u8; 32])
    }
}
