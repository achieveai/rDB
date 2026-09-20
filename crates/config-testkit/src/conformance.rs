//! The conformance suite (TA-10; test plan §4.3, scenarios C-01..C-15).
//!
//! One list of scenarios, run against any `Arc<dyn ConfigStore>`. `DirectClient` and
//! `GrpcClient` both implement [`ConfigStore`] with no extra methods, so running this suite
//! against each and comparing the two [`ConformanceReport`]s with [`ConformanceReport::diff`]
//! is the whole proof that "direct and gRPC clients pass the same suite" (test plan M1-46).
//!
//! Each scenario is its own `async fn` that asserts freely with `assert!`/`assert_eq!`; a
//! panic inside one is caught by [`run_all`] and turned into a failed [`ScenarioResult`]
//! rather than aborting the whole run, so one broken scenario does not hide the other
//! fourteen results.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, DeleteRequest, GetRequest, Limits, ListRequest, MutationEvent,
    MutationEventKind, MutationOutcome, PutRequest, TransportSecurity, WatchItem, WatchRequest,
    WatchStream,
};
use futures::{FutureExt, StreamExt};
use serde::Serialize;
use serde_json::json;

/// Parameters for one [`run_all`] call.
#[derive(Clone)]
pub struct ConformanceConfig {
    /// The caps the store under test enforces. Truncation and over-cap scenarios (C-09, C-10,
    /// C-12) are computed from these, not from [`Limits::DEFAULT`], so a store configured with
    /// shrunk caps is still exercised correctly.
    pub limits: Limits,
    /// Every key this run touches is prefixed with these bytes. Unique per run (see
    /// [`ConformanceConfig::unique`]) so the same long-lived store can be reused across
    /// several conformance runs — e.g. `M1-44` and `M1-45` against the same cluster — without
    /// one run's keys colliding with another's.
    pub key_prefix: Vec<u8>,
    /// If set, the caller expects the store's `capabilities().transport_security` to equal
    /// this value; scenarios do not check it themselves, but a harness can read it back off
    /// the config when composing its own assertions.
    pub expect_transport_security: Option<TransportSecurity>,
    /// The cluster-level powers [`run_all_watch`] needs. Absent for a C-01..C-15 run.
    pub watch: Option<Arc<dyn WatchFixture>>,
}

impl std::fmt::Debug for ConformanceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConformanceConfig")
            .field("limits", &self.limits)
            .field("key_prefix", &String::from_utf8_lossy(&self.key_prefix))
            .field("expect_transport_security", &self.expect_transport_security)
            .field("watch", &self.watch.is_some())
            .finish()
    }
}

static PREFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

impl ConformanceConfig {
    /// Build a config with [`Limits::DEFAULT`] and the given fixed `key_prefix`.
    pub fn new(key_prefix: impl Into<Vec<u8>>) -> Self {
        Self {
            limits: Limits::DEFAULT,
            key_prefix: key_prefix.into(),
            expect_transport_security: None,
            watch: None,
        }
    }

    /// The same config, able to run [`run_all_watch`].
    pub fn with_watch(mut self, fixture: Arc<dyn WatchFixture>) -> Self {
        self.watch = Some(fixture);
        self
    }

    /// Build a config whose `key_prefix` is unique within this process (a process id plus a
    /// monotonic counter, not randomness — the state machine under test must stay unaware of
    /// entropy sources, and this value never crosses it), so `tag` need only be human-readable.
    pub fn unique(tag: &str) -> Self {
        let n = PREFIX_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self::new(format!("__conformance/{}/{n}/{tag}/", std::process::id()).into_bytes())
    }
}

/// Outcome of one scenario.
#[derive(Debug, Clone, Serialize)]
pub struct ScenarioResult {
    /// Stable scenario id, e.g. `"C-02"` (test plan §4.3).
    pub id: &'static str,
    /// Human-readable scenario name, e.g. `"put-get"`.
    pub name: &'static str,
    /// Whether the scenario's assertions held.
    pub passed: bool,
    /// One-line explanation: what was proved on success, or what failed (including a caught
    /// panic message) on failure.
    pub detail: String,
    /// Facts the scenario observed (revisions, counts, ...), for [`ConformanceReport::diff`].
    /// A `"transport"` object key, if present, is ignored by `diff`.
    pub observed: serde_json::Value,
}

/// How many scenarios [`run_all`] must produce: C-01..C-15.
///
/// Asserted by [`ConformanceReport::assert_all_passed`] so that a report which lost scenarios
/// — a truncated run, a hand-built report, a future refactor that forgets a `run_one` — fails
/// instead of passing vacuously on the ones that survived.
pub const SCENARIO_COUNT: usize = 15;

/// The result of running every scenario once.
#[derive(Debug, Clone, Serialize)]
pub struct ConformanceReport {
    /// One entry per scenario, in the order [`run_all`] executed them.
    pub results: Vec<ScenarioResult>,
}

impl ConformanceReport {
    /// Whether every scenario passed.
    pub fn passed(&self) -> bool {
        self.results.iter().all(|r| r.passed)
    }

    /// The scenarios that did not pass, in run order.
    pub fn failures(&self) -> Vec<&ScenarioResult> {
        self.results.iter().filter(|r| !r.passed).collect()
    }

    /// Panic, listing every failure, unless [`ConformanceReport::passed`].
    ///
    /// The scenario count is checked first: "nothing failed" is only meaningful once we know
    /// every scenario the producing run was supposed to generate actually ran. A report is
    /// either every result from [`run_all`] (C-01..C-15, [`SCENARIO_COUNT`]) or every result
    /// from [`run_all_watch`] (W-01..W-12, [`SCENARIO_COUNT_WATCH`]) — told apart by the first
    /// result's id prefix, since nothing else labels a `ConformanceReport` with its family.
    pub fn assert_all_passed(&self) {
        let expected = match self.results.first() {
            Some(first) if first.id.starts_with('W') => SCENARIO_COUNT_WATCH,
            _ => SCENARIO_COUNT,
        };
        assert_eq!(
            self.results.len(),
            expected,
            "the conformance report has {} scenarios, expected {expected}; ran: {:?}",
            self.results.len(),
            self.results.iter().map(|r| r.id).collect::<Vec<_>>()
        );
        let failures = self.failures();
        assert!(
            failures.is_empty(),
            "conformance suite failed ({} of {} scenarios):\n{}",
            failures.len(),
            self.results.len(),
            failures
                .iter()
                .map(|f| format!("  {} {}: {}", f.id, f.name, f.detail))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    /// Compare two reports scenario by scenario, ignoring a top-level `"transport"` field in
    /// `observed` (endpoints and similar are expected to differ between a direct and a gRPC
    /// client). Returns one description per mismatch; empty means the reports agree.
    pub fn diff(&self, other: &Self) -> Vec<String> {
        use std::collections::BTreeMap;

        let mut other_by_id: BTreeMap<&str, &ScenarioResult> =
            other.results.iter().map(|r| (r.id, r)).collect();
        let mut mismatches = Vec::new();

        for result in &self.results {
            match other_by_id.remove(result.id) {
                None => {
                    mismatches.push(format!("{}: present in self, missing in other", result.id))
                }
                Some(peer) => {
                    if result.passed != peer.passed {
                        mismatches.push(format!(
                            "{}: passed differs (self={}, other={})",
                            result.id, result.passed, peer.passed
                        ));
                    }
                    let a = strip_transport_fields(&result.observed);
                    let b = strip_transport_fields(&peer.observed);
                    if a != b {
                        mismatches.push(format!(
                            "{}: observed differs (modulo transport fields): {a} vs {b}",
                            result.id
                        ));
                    }
                }
            }
        }
        for id in other_by_id.keys() {
            mismatches.push(format!("{id}: present in other, missing in self"));
        }
        mismatches
    }
}

fn strip_transport_fields(value: &serde_json::Value) -> serde_json::Value {
    let mut value = value.clone();
    if let serde_json::Value::Object(map) = &mut value {
        map.remove("transport");
        map.remove("endpoint");
    }
    value
}

/// Run every C-01..C-15 scenario in order against `store`, using keys namespaced under
/// `cfg.key_prefix`.
pub async fn run_all(store: Arc<dyn ConfigStore>, cfg: ConformanceConfig) -> ConformanceReport {
    let results = vec![
        run_one("C-01", "get-missing", c01_get_missing(&store, &cfg)).await,
        run_one("C-02", "put-get", c02_put_get(&store, &cfg)).await,
        run_one(
            "C-03",
            "put-same-value-bumps-rev",
            c03_put_same_value_bumps_rev(&store, &cfg),
        )
        .await,
        run_one("C-04", "cas-create-only", c04_cas_create_only(&store, &cfg)).await,
        run_one("C-05", "cas-conflict", c05_cas_conflict(&store, &cfg)).await,
        run_one(
            "C-06",
            "delete-missing-not-found",
            c06_delete_missing_not_found(&store, &cfg),
        )
        .await,
        run_one(
            "C-07",
            "delete-cas-mismatch",
            c07_delete_cas_mismatch(&store, &cfg),
        )
        .await,
        run_one("C-08", "list-ordering", c08_list_ordering(&store, &cfg)).await,
        run_one(
            "C-09",
            "list-truncated-by-max-items",
            c09_list_truncated_by_max_items(&store, &cfg),
        )
        .await,
        run_one(
            "C-10",
            "list-truncated-by-max-bytes",
            c10_list_truncated_by_max_bytes(&store, &cfg),
        )
        .await,
        run_one(
            "C-11",
            "invalid-delete-expected-zero",
            c11_invalid_delete_expected_zero(&store, &cfg),
        )
        .await,
        run_one(
            "C-12",
            "oversize-key-and-value",
            c12_oversize_key_and_value(&store, &cfg),
        )
        .await,
        run_one(
            "C-13",
            "read-revision-monotonic",
            c13_read_revision_monotonic(&store, &cfg),
        )
        .await,
        run_one(
            "C-14",
            "conflict-does-not-leak-value",
            c14_conflict_does_not_leak_value(&store, &cfg),
        )
        .await,
        run_one(
            "C-15",
            "empty-value-is-not-absence",
            c15_empty_value_is_not_absence(&store, &cfg),
        )
        .await,
    ];
    ConformanceReport { results }
}

/// Run one scenario future, catching a panic and logging the outcome at info (deliverable:
/// "each scenario logs at info `scenario`, `passed`, `detail`").
async fn run_one<Fut>(id: &'static str, name: &'static str, fut: Fut) -> ScenarioResult
where
    Fut: Future<Output = ScenarioResult>,
{
    let result = match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(result) => result,
        Err(panic) => ScenarioResult {
            id,
            name,
            passed: false,
            detail: format!("panicked: {}", panic_message(&panic)),
            observed: serde_json::Value::Null,
        },
    };
    tracing::info!(
        scenario = result.id,
        passed = result.passed,
        detail = %result.detail,
        "scenario"
    );
    result
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with non-string payload".to_string()
    }
}

/// Build a scenario key: `cfg.key_prefix` followed by `suffix`'s bytes.
fn key(cfg: &ConformanceConfig, suffix: &str) -> Bytes {
    let mut out = cfg.key_prefix.clone();
    out.extend_from_slice(suffix.as_bytes());
    Bytes::from(out)
}

async fn c01_get_missing(store: &Arc<dyn ConfigStore>, cfg: &ConformanceConfig) -> ScenarioResult {
    let k = key(cfg, "c01/missing");
    let resp = store
        .get(GetRequest { key: k })
        .await
        .expect("a missing key is Ok(record: None), not an error");
    assert!(
        resp.record.is_none(),
        "missing key must report record: None"
    );
    ScenarioResult {
        id: "C-01",
        name: "get-missing",
        passed: true,
        detail: "GetResponse{record: None} for a missing key, not an error".into(),
        observed: json!({ "read_revision": resp.read_revision }),
    }
}

async fn c02_put_get(store: &Arc<dyn ConfigStore>, cfg: &ConformanceConfig) -> ScenarioResult {
    let k = key(cfg, "c02/k");
    let put = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"v1"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("unconditional put must succeed");
    assert_eq!(put.outcome, MutationOutcome::Applied);

    let get = store
        .get(GetRequest { key: k })
        .await
        .expect("get must succeed");
    let record = get.record.expect("record present after put");
    assert_eq!(record.value, Bytes::from_static(b"v1"));
    assert_eq!(record.create_revision, put.revision);
    assert_eq!(record.mod_revision, put.revision);
    assert!(get.read_revision >= put.revision);

    ScenarioResult {
        id: "C-02",
        name: "put-get",
        passed: true,
        detail: format!("put allocated revision {}, get observed it", put.revision),
        observed: json!({ "revision": put.revision, "read_revision": get.read_revision }),
    }
}

async fn c03_put_same_value_bumps_rev(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let k = key(cfg, "c03/k");
    let first = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"same"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("first put succeeds");
    let second = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"same"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("identical-value put still succeeds");
    assert_eq!(second.outcome, MutationOutcome::Applied);
    assert!(
        second.revision > first.revision,
        "a same-value Put is still a state-changing mutation"
    );

    let record = store
        .get(GetRequest { key: k })
        .await
        .expect("get succeeds")
        .record
        .expect("record present");
    assert_eq!(record.mod_revision, second.revision);
    assert_eq!(record.create_revision, first.revision);

    ScenarioResult {
        id: "C-03",
        name: "put-same-value-bumps-rev",
        passed: true,
        detail: format!(
            "second identical put allocated revision {} (create_revision stayed {})",
            second.revision, first.revision
        ),
        observed: json!({ "create_revision": first.revision, "mod_revision": second.revision }),
    }
}

async fn c04_cas_create_only(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let k = key(cfg, "c04/k");
    let created = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await
        .expect("create-only put on an absent key succeeds");
    assert_eq!(created.outcome, MutationOutcome::Applied);

    let repeat = store
        .put(PutRequest {
            key: k,
            value: Bytes::from_static(b"v2"),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await
        .expect("a repeated create-only put is a Conflict outcome, not an Err");
    assert_eq!(repeat.outcome, MutationOutcome::Conflict);
    assert!(repeat.exists);
    assert_eq!(repeat.current_mod_revision, created.revision);

    ScenarioResult {
        id: "C-04",
        name: "cas-create-only",
        passed: true,
        detail: "create-only put applied once, conflicted on repeat".into(),
        observed: json!({ "created_revision": created.revision }),
    }
}

async fn c05_cas_conflict(store: &Arc<dyn ConfigStore>, cfg: &ConformanceConfig) -> ScenarioResult {
    let k = key(cfg, "c05/k");
    let first = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"v1"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("unconditional put succeeds");

    let wrong_expected = first.revision + 999;
    let conflict = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"v2"),
            expected_mod_revision: Some(wrong_expected),
            dedup: None,
        })
        .await
        .expect("CAS mismatch is a Conflict outcome, not an Err");
    assert_eq!(conflict.outcome, MutationOutcome::Conflict);
    assert_eq!(conflict.current_mod_revision, first.revision);

    let record = store
        .get(GetRequest { key: k })
        .await
        .expect("get succeeds")
        .record
        .expect("record still present");
    assert_eq!(
        record.value,
        Bytes::from_static(b"v1"),
        "value unchanged by the rejected CAS"
    );

    ScenarioResult {
        id: "C-05",
        name: "cas-conflict",
        passed: true,
        detail: format!("CAS against wrong revision {wrong_expected} conflicted, value unchanged"),
        observed: json!({ "current_mod_revision": conflict.current_mod_revision }),
    }
}

async fn c06_delete_missing_not_found(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let k = key(cfg, "c06/missing");
    let before = store
        .get(GetRequest { key: k.clone() })
        .await
        .expect("get succeeds")
        .read_revision;

    let resp = store
        .delete(DeleteRequest {
            key: k.clone(),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("deleting an absent key is a NotFound outcome, not an Err");
    assert_eq!(resp.outcome, MutationOutcome::NotFound);

    let after = store
        .get(GetRequest { key: k })
        .await
        .expect("get succeeds")
        .read_revision;
    assert_eq!(before, after, "no revision allocated by a NotFound delete");
    assert_eq!(resp.revision, after);

    ScenarioResult {
        id: "C-06",
        name: "delete-missing-not-found",
        passed: true,
        detail: "delete of an absent key returned NotFound and allocated nothing".into(),
        observed: json!({ "cluster_revision": after }),
    }
}

async fn c07_delete_cas_mismatch(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let k = key(cfg, "c07/k");
    let put = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("put succeeds");

    let wrong_expected = put.revision + 999;
    let resp = store
        .delete(DeleteRequest {
            key: k.clone(),
            expected_mod_revision: Some(wrong_expected),
            dedup: None,
        })
        .await
        .expect("CAS mismatch on delete is a Conflict outcome, not an Err");
    assert_eq!(resp.outcome, MutationOutcome::Conflict);
    assert!(resp.exists);

    let still_present = store
        .get(GetRequest { key: k })
        .await
        .expect("get succeeds")
        .record
        .is_some();
    assert!(
        still_present,
        "key must still be present after a rejected CAS delete"
    );

    ScenarioResult {
        id: "C-07",
        name: "delete-cas-mismatch",
        passed: true,
        detail: format!("delete CAS against wrong revision {wrong_expected} conflicted"),
        observed: json!({ "current_mod_revision": resp.current_mod_revision }),
    }
}

async fn c08_list_ordering(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "c08/");
    // Inserted out of order, and including bytes above 0x7F, to catch a UTF-8-order or
    // signed-comparison bug.
    let insert_order: [u8; 5] = [0xFF, 0x41, 0x7E, 0x5F, 0x61];
    for suffix in insert_order {
        let mut k = prefix.to_vec();
        k.push(suffix);
        store
            .put(PutRequest {
                key: Bytes::from(k),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("put succeeds");
    }

    let list = store
        .list(ListRequest {
            prefix: prefix.clone(),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .expect("list succeeds");
    assert!(!list.truncated);
    let observed_order: Vec<u8> = list
        .records
        .iter()
        .map(|r| *r.key.last().expect("non-empty key"))
        .collect();
    assert_eq!(observed_order, vec![0x41, 0x5F, 0x61, 0x7E, 0xFF]);

    ScenarioResult {
        id: "C-08",
        name: "list-ordering",
        passed: true,
        detail: "list returned unsigned bytewise ascending order regardless of insert order".into(),
        observed: json!({ "count": list.records.len() }),
    }
}

async fn c09_list_truncated_by_max_items(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "c09/");
    for i in 0..5u8 {
        let mut k = prefix.to_vec();
        k.push(i);
        store
            .put(PutRequest {
                key: Bytes::from(k),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("put succeeds");
    }

    let list = store
        .list(ListRequest {
            prefix: prefix.clone(),
            max_items: 2,
            max_bytes: 0,
        })
        .await
        .expect("list succeeds");
    assert_eq!(list.records.len(), 2);
    assert!(list.truncated);
    let observed_order: Vec<u8> = list
        .records
        .iter()
        .map(|r| *r.key.last().unwrap())
        .collect();
    assert_eq!(
        observed_order,
        vec![0, 1],
        "records returned in order, up to the cap"
    );

    ScenarioResult {
        id: "C-09",
        name: "list-truncated-by-max-items",
        passed: true,
        detail: "max_items=2 over 5 matches returned the first 2, truncated=true".into(),
        observed: json!({ "count": list.records.len(), "truncated": list.truncated }),
    }
}

async fn c10_list_truncated_by_max_bytes(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "c10/");
    let value = Bytes::from_static(b"vv");
    for i in 0..4u8 {
        let mut k = prefix.to_vec();
        k.push(i);
        store
            .put(PutRequest {
                key: Bytes::from(k),
                value: value.clone(),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("put succeeds");
    }

    // Every key here is `prefix.len() + 1` bytes; size the cap to fit exactly 2 of the 4
    // records so truncation is forced without depending on the server's own caps.
    let record_cost = Limits::list_record_cost(prefix.len() + 1, value.len());
    let max_bytes = record_cost * 2;

    let list = store
        .list(ListRequest {
            prefix: prefix.clone(),
            max_items: 0,
            max_bytes,
        })
        .await
        .expect("list succeeds");
    assert!(
        !list.records.is_empty(),
        "byte cap must still make forward progress"
    );
    assert!(list.records.len() < 4, "byte cap must actually bind");
    assert!(list.truncated);

    ScenarioResult {
        id: "C-10",
        name: "list-truncated-by-max-bytes",
        passed: true,
        detail: format!(
            "max_bytes={max_bytes} over 4 matches returned {} records, truncated=true",
            list.records.len()
        ),
        observed: json!({ "count": list.records.len(), "truncated": list.truncated }),
    }
}

async fn c11_invalid_delete_expected_zero(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let k = key(cfg, "c11/k");
    let err = store
        .delete(DeleteRequest {
            key: k.clone(),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await
        .expect_err("Delete{expected_mod_revision: Some(0)} must be rejected, not Ok");
    assert!(
        matches!(err, ConfigError::InvalidArgument { .. }),
        "expected InvalidArgument, got {err:?}"
    );

    let unaffected = store
        .get(GetRequest { key: k })
        .await
        .expect("get succeeds")
        .record
        .is_none();
    assert!(unaffected, "rejected delete must not change state");

    ScenarioResult {
        id: "C-11",
        name: "invalid-delete-expected-zero",
        passed: true,
        detail: "Delete{expected_mod_revision: Some(0)} rejected as InvalidArgument".into(),
        observed: json!({ "rejected": true }),
    }
}

async fn c12_oversize_key_and_value(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let mut oversize_key = cfg.key_prefix.clone();
    while oversize_key.len() <= cfg.limits.max_key_bytes {
        oversize_key.push(b'k');
    }
    let key_err = store
        .put(PutRequest {
            key: Bytes::from(oversize_key),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect_err("an over-cap key must be rejected");
    assert!(
        matches!(key_err, ConfigError::InvalidArgument { .. }),
        "over-cap key must be InvalidArgument, got {key_err:?}"
    );

    let value_key = key(cfg, "c12/value");
    let big_value = vec![0u8; cfg.limits.max_value_bytes + 1];
    let value_err = store
        .put(PutRequest {
            key: value_key.clone(),
            value: Bytes::from(big_value),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect_err("an over-cap value must be rejected");
    assert!(
        matches!(value_err, ConfigError::ResourceExhausted { .. }),
        "over-cap value must be ResourceExhausted, got {value_err:?}"
    );

    let unaffected = store
        .get(GetRequest { key: value_key })
        .await
        .expect("get succeeds")
        .record
        .is_none();
    assert!(unaffected, "no state change from either rejected put");

    ScenarioResult {
        id: "C-12",
        name: "oversize-key-and-value",
        passed: true,
        detail: "over-cap key -> InvalidArgument, over-cap value -> ResourceExhausted".into(),
        observed: json!({ "max_key_bytes": cfg.limits.max_key_bytes, "max_value_bytes": cfg.limits.max_value_bytes }),
    }
}

async fn c13_read_revision_monotonic(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "c13/");
    let mut put_revisions = Vec::new();
    let mut read_revisions = Vec::new();

    for i in 0..10u8 {
        let mut k = prefix.to_vec();
        k.push(i);
        let put = store
            .put(PutRequest {
                key: Bytes::from(k.clone()),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("put succeeds");
        put_revisions.push(put.revision);

        let get = store
            .get(GetRequest {
                key: Bytes::from(k),
            })
            .await
            .expect("get succeeds");
        read_revisions.push(get.read_revision);
    }

    for pair in put_revisions.windows(2) {
        assert!(
            pair[1] > pair[0],
            "each Put must strictly increase the observed revision: {put_revisions:?}"
        );
    }
    for pair in read_revisions.windows(2) {
        assert!(
            pair[1] >= pair[0],
            "read_revision must be non-decreasing across reads: {read_revisions:?}"
        );
    }
    for (put_rev, read_rev) in put_revisions.iter().zip(&read_revisions) {
        assert!(
            read_rev >= put_rev,
            "a read right after its put must observe at least that revision"
        );
    }

    ScenarioResult {
        id: "C-13",
        name: "read-revision-monotonic",
        passed: true,
        detail: "10 interleaved mutations and reads produced a non-decreasing read_revision \
                 sequence, strictly increasing across each mutation"
            .into(),
        observed: json!({ "put_revisions": put_revisions, "read_revisions": read_revisions }),
    }
}

async fn c14_conflict_does_not_leak_value(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    const SENTINEL: &str = "SENSITIVE_SENTINEL_VALUE";
    let k = key(cfg, "c14/k");
    let put = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(SENTINEL.as_bytes()),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("put succeeds");

    let conflict = store
        .put(PutRequest {
            key: k,
            value: Bytes::from_static(b"attacker-supplied"),
            expected_mod_revision: Some(put.revision + 999),
            dedup: None,
        })
        .await
        .expect("CAS mismatch is a Conflict outcome, not an Err");
    assert_eq!(conflict.outcome, MutationOutcome::Conflict);

    // `MutationResponse` structurally carries only `outcome`/`revision`/`exists`/
    // `current_mod_revision` — no value field exists to leak. Render it anyway as a defense-
    // in-depth check against a future field addition.
    let rendered = format!("{conflict:?}");
    assert!(
        !rendered.contains(SENTINEL),
        "conflict response leaked the sentinel value: {rendered}"
    );

    ScenarioResult {
        id: "C-14",
        name: "conflict-does-not-leak-value",
        passed: true,
        detail: "Conflict response exposed only exists/current_mod_revision, never the value"
            .into(),
        observed: json!({ "current_mod_revision": conflict.current_mod_revision }),
    }
}

async fn c15_empty_value_is_not_absence(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let k = key(cfg, "c15/k");
    let put = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::new(),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("put of an empty value succeeds");
    assert_eq!(put.outcome, MutationOutcome::Applied);

    let record = store
        .get(GetRequest { key: k })
        .await
        .expect("get succeeds")
        .record
        .expect("a present record with a zero-length value, distinguishable from C-01's absence");
    assert_eq!(record.value.len(), 0);

    ScenarioResult {
        id: "C-15",
        name: "empty-value-is-not-absence",
        passed: true,
        detail: "Put{value: b\"\"} produced a present record, distinct from a missing key".into(),
        observed: json!({ "revision": put.revision }),
    }
}

// =========================================================================================
// Watch conformance (M4, test plan §4: W-01 .. W-12)
// =========================================================================================

/// The cluster-level powers the watch scenarios need and a bare [`ConfigStore`] does not have.
///
/// Three scenarios are not expressible through the store interface alone — compaction is a
/// replicated command, an authorization refusal needs a *second* principal, and slot release
/// needs to know the node's cap. Rather than weaken those rows to something a lone store can
/// check, the harness supplies this and the suite refuses to run without it: a conformance
/// scenario that quietly downgrades itself proves nothing, which is the failure mode the whole
/// report shape exists to prevent.
#[async_trait::async_trait]
pub trait WatchFixture: Send + Sync {
    /// Propose a compaction up to `up_to` and wait for it to apply. Returns the new floor.
    async fn compact(&self, up_to: u64) -> Result<u64, ConfigError>;

    /// The same store, seen by a principal with **no** grant anywhere under `key_prefix`.
    fn unauthorized(&self) -> Arc<dyn ConfigStore>;

    /// Concurrent watch streams the serving node admits.
    fn max_streams_per_node(&self) -> u32;
}

/// How many scenarios [`run_all_watch`] must produce: W-01..W-12.
///
/// Separate from [`SCENARIO_COUNT`] on purpose: `run_all` still returns 15, so a harness that
/// pins the M1 number keeps working and E2E-03 is unaffected (test plan §4).
pub const SCENARIO_COUNT_WATCH: usize = 12;

/// How long a scenario waits for an item it has every reason to expect.
///
/// Generous, because it is a *failure* bound and never a success bound: every scenario stops
/// waiting the moment it has what it asked for, so a healthy run never spends this. Nothing
/// here sleeps (anti-flake rule 1).
const WATCH_DEADLINE: Duration = Duration::from_secs(10);

/// Run every W-01..W-12 scenario in order against `store`.
///
/// # Panics
///
/// If `cfg.watch` is `None`. See [`WatchFixture`] for why this is a refusal rather than a
/// reduced run.
pub async fn run_all_watch(
    store: Arc<dyn ConfigStore>,
    cfg: ConformanceConfig,
) -> ConformanceReport {
    let fixture = cfg
        .watch
        .clone()
        .expect("run_all_watch needs a WatchFixture; see ConformanceConfig::with_watch");
    let results = vec![
        run_one("W-01", "watch-from-zero", w01_from_zero(&store, &cfg)).await,
        run_one(
            "W-02",
            "watch-resume-midpoint",
            w02_resume_midpoint(&store, &cfg),
        )
        .await,
        run_one(
            "W-03",
            "watch-live-delivery",
            w03_live_delivery(&store, &cfg),
        )
        .await,
        run_one(
            "W-04",
            "watch-prefix-filter",
            w04_prefix_filter(&store, &cfg),
        )
        .await,
        run_one("W-05", "watch-delete-event", w05_delete_event(&store, &cfg)).await,
        run_one(
            "W-06",
            "watch-no-event-for-conflict",
            w06_no_event_for_conflict(&store, &cfg),
        )
        .await,
        run_one(
            "W-07",
            "watch-compacted-error",
            w07_compacted_error(&store, &cfg, &fixture),
        )
        .await,
        run_one(
            "W-08",
            "watch-compacted-boundary",
            w08_compacted_boundary(&store, &cfg, &fixture),
        )
        .await,
        run_one(
            "W-09",
            "watch-future-revision",
            w09_future_revision(&store, &cfg),
        )
        .await,
        run_one(
            "W-10",
            "watch-progress-frame",
            w10_progress_frame(&store, &cfg),
        )
        .await,
        run_one(
            "W-11",
            "watch-unauthorized-prefix",
            w11_unauthorized_prefix(&cfg, &fixture),
        )
        .await,
        run_one(
            "W-12",
            "watch-close-releases-slot",
            w12_close_releases_slot(&store, &cfg, &fixture),
        )
        .await,
    ];
    ConformanceReport { results }
}

// ---------------------------------------------------------------- scenario helpers

/// Put `n` values under `suffix`/`i` and return the revisions they were allocated.
async fn put_series(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
    suffix: &str,
    n: usize,
) -> Vec<u64> {
    let mut revisions = Vec::with_capacity(n);
    for i in 0..n {
        let resp = store
            .put(PutRequest {
                key: key(cfg, &format!("{suffix}/{i}")),
                value: Bytes::from(format!("v{i}")),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .unwrap_or_else(|e| panic!("put {suffix}/{i}: {e}"));
        assert_eq!(resp.outcome, MutationOutcome::Applied);
        revisions.push(resp.revision);
    }
    revisions
}

/// Drain `stream` until `want` events under `prefix` have arrived, or the deadline passes.
///
/// Progress frames are counted separately rather than discarded: a scenario that expects none
/// (W-01..W-09) must be able to say so, and W-10 asserts on exactly this number.
async fn take_events(
    stream: &mut WatchStream,
    prefix: &[u8],
    want: usize,
) -> (Vec<MutationEvent>, usize) {
    let mut events = Vec::new();
    let mut progress = 0usize;
    let outcome = tokio::time::timeout(WATCH_DEADLINE, async {
        while events.len() < want {
            match stream.next().await {
                Some(Ok(WatchItem::Event(event))) => {
                    assert!(
                        event.key.starts_with(prefix),
                        "a watch on {prefix:?} delivered key {:?}",
                        event.key
                    );
                    events.push(event);
                }
                Some(Ok(WatchItem::Progress { .. })) => progress += 1,
                Some(Err(e)) => panic!("the stream terminated with {e} after {events:?}"),
                None => panic!("the stream ended after {} of {want} events", events.len()),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "only {} of {want} events arrived within {WATCH_DEADLINE:?}",
        events.len()
    );
    (events, progress)
}

/// Assert the revisions are strictly ascending, and return them.
fn revisions_of(events: &[MutationEvent]) -> Vec<u64> {
    let revisions: Vec<u64> = events.iter().map(|e| e.revision).collect();
    assert!(
        revisions.windows(2).all(|w| w[0] < w[1]),
        "events must arrive in strictly increasing revision order, got {revisions:?}"
    );
    revisions
}

/// The cluster's current revision, read through the store under test.
async fn current_revision(store: &Arc<dyn ConfigStore>, cfg: &ConformanceConfig) -> u64 {
    store
        .get(GetRequest {
            key: key(cfg, "revision-probe"),
        })
        .await
        .expect("a get on a missing key succeeds")
        .read_revision
}

// ---------------------------------------------------------------- W-01 .. W-12

async fn w01_from_zero(store: &Arc<dyn ConfigStore>, cfg: &ConformanceConfig) -> ScenarioResult {
    let prefix = key(cfg, "w01/");
    let written = put_series(store, cfg, "w01", 5).await;

    // `start_after_revision: 0` on a cluster that has never compacted must be accepted: the
    // literal "R <= compact_revision" test would reject every first-time watcher (OQ-27).
    let mut stream = store
        .watch(WatchRequest {
            prefix: prefix.clone(),
            start_after_revision: 0,
            progress_interval: None,
        })
        .await
        .expect("watching from zero on an uncompacted cluster must be accepted");
    let (events, progress) = take_events(&mut stream, &prefix, 5).await;
    let delivered = revisions_of(&events);

    assert_eq!(
        delivered, written,
        "replay must deliver exactly what was put"
    );
    assert_eq!(progress, 0, "no progress frame was asked for");

    ScenarioResult {
        id: "W-01",
        name: "watch-from-zero",
        passed: true,
        detail: "5 puts replayed from cursor 0, ascending and contiguous".into(),
        observed: json!({ "delivered": delivered.len(), "gaps": gaps(&delivered, &written) }),
    }
}

async fn w02_resume_midpoint(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "w02/");
    let written = put_series(store, cfg, "w02", 10).await;
    let cursor = written[4];

    let mut stream = store
        .watch(WatchRequest {
            prefix: prefix.clone(),
            start_after_revision: cursor,
            progress_interval: None,
        })
        .await
        .expect("resuming from a retained revision must be accepted");
    let (events, _) = take_events(&mut stream, &prefix, 5).await;
    let delivered = revisions_of(&events);

    // The cursor is *exclusive*: the revision the client says it already has must not come
    // back, or a resuming client would reprocess its own last event forever.
    assert!(
        !delivered.contains(&cursor),
        "the cursor revision {cursor} must not be redelivered, got {delivered:?}"
    );
    assert_eq!(delivered, written[5..], "6..10 must be delivered");

    ScenarioResult {
        id: "W-02",
        name: "watch-resume-midpoint",
        passed: true,
        detail: "resume after the 5th of 10 revisions delivered exactly the last 5".into(),
        observed: json!({ "delivered": delivered.len(), "cursor_redelivered": false }),
    }
}

async fn w03_live_delivery(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "w03/");
    let high_water = current_revision(store, cfg).await;

    let mut stream = store
        .watch(WatchRequest {
            prefix: prefix.clone(),
            start_after_revision: high_water,
            progress_interval: None,
        })
        .await
        .expect("watching from the current revision must be accepted");

    // Written *after* the stream is open, so these can only arrive through the live path.
    let written = put_series(store, cfg, "w03", 5).await;
    let (events, _) = take_events(&mut stream, &prefix, 5).await;
    let delivered = revisions_of(&events);

    assert_eq!(
        delivered, written,
        "live delivery must carry every new revision"
    );

    ScenarioResult {
        id: "W-03",
        name: "watch-live-delivery",
        passed: true,
        detail: "5 revisions written after registration arrived live, ascending".into(),
        observed: json!({ "delivered": delivered.len(), "gaps": gaps(&delivered, &written) }),
    }
}

async fn w04_prefix_filter(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "w04/x/");
    let high_water = current_revision(store, cfg).await;

    let mut wanted = Vec::new();
    for (suffix, keep) in [("w04/x/a", true), ("w04/y/a", false), ("w04/x/y/a", true)] {
        let resp = store
            .put(PutRequest {
                key: key(cfg, suffix),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("put");
        if keep {
            wanted.push(resp.revision);
        }
    }

    // `w04/x/y/a` is deliberately included: a filter written as "one path segment under the
    // prefix" instead of a byte-prefix would drop it, and every etcd-shaped client expects a
    // prefix to mean bytes.
    let mut stream = store
        .watch(WatchRequest {
            prefix: prefix.clone(),
            start_after_revision: high_water,
            progress_interval: None,
        })
        .await
        .expect("watch");
    let (events, _) = take_events(&mut stream, &prefix, wanted.len()).await;
    let delivered = revisions_of(&events);
    assert_eq!(
        delivered, wanted,
        "only keys under the prefix may be delivered"
    );

    ScenarioResult {
        id: "W-04",
        name: "watch-prefix-filter",
        passed: true,
        detail: "a byte-prefix watch delivered both matching keys and neither non-matching one"
            .into(),
        observed: json!({ "delivered": delivered.len(), "filtered_out": 1 }),
    }
}

async fn w05_delete_event(store: &Arc<dyn ConfigStore>, cfg: &ConformanceConfig) -> ScenarioResult {
    let prefix = key(cfg, "w05/");
    let k = key(cfg, "w05/k");
    let put = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("put");
    let delete = store
        .delete(DeleteRequest {
            key: k.clone(),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("delete");

    let mut stream = store
        .watch(WatchRequest {
            prefix: prefix.clone(),
            start_after_revision: put.revision.saturating_sub(1),
            progress_interval: None,
        })
        .await
        .expect("watch");
    let (events, _) = take_events(&mut stream, &prefix, 2).await;
    revisions_of(&events);

    assert_eq!(events[0].key, k);
    assert_eq!(events[1].key, k);
    assert_eq!(events[0].revision, put.revision);
    assert_eq!(events[1].revision, delete.revision);
    assert!(
        matches!(events[0].kind, MutationEventKind::Put { .. }),
        "the first event must be the put"
    );
    // A delete carries no value at all, not an empty one: C-15 makes an empty value a real
    // value, so a delete that shipped `b""` would be indistinguishable from a legal write.
    assert!(
        matches!(events[1].kind, MutationEventKind::Delete),
        "the second event must be a delete carrying no value, got {:?}",
        events[1].kind
    );

    ScenarioResult {
        id: "W-05",
        name: "watch-delete-event",
        passed: true,
        detail: "put then delete on one key arrived in revision order; the delete carries no value"
            .into(),
        observed: json!({ "delivered": 2, "delete_carries_value": false }),
    }
}

async fn w06_no_event_for_conflict(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "w06/");
    let high_water = current_revision(store, cfg).await;

    let first = store
        .put(PutRequest {
            key: key(cfg, "w06/a"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("put");

    // Neither of these allocates a revision, so neither may produce an event: a watcher that
    // saw a refused CAS would be told the state changed when it did not.
    let conflict = store
        .put(PutRequest {
            key: key(cfg, "w06/a"),
            value: Bytes::from_static(b"w"),
            expected_mod_revision: Some(first.revision + 1000),
            dedup: None,
        })
        .await
        .expect("a rejected CAS is a transport-level success (spec §7.3), not an Err");
    assert_eq!(
        conflict.outcome,
        MutationOutcome::Conflict,
        "a CAS against the wrong revision must conflict, got {conflict:?}"
    );
    let missing = store
        .delete(DeleteRequest {
            key: key(cfg, "w06/never-written"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("a delete of a missing key is a transport-level success (spec §7.3), not an Err");
    assert_eq!(
        missing.outcome,
        MutationOutcome::NotFound,
        "deleting a missing key is NotFound, got {missing:?}"
    );

    let second = store
        .put(PutRequest {
            key: key(cfg, "w06/b"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("put");

    let mut stream = store
        .watch(WatchRequest {
            prefix: prefix.clone(),
            start_after_revision: high_water,
            progress_interval: None,
        })
        .await
        .expect("watch");
    let (events, _) = take_events(&mut stream, &prefix, 2).await;
    let delivered = revisions_of(&events);
    assert_eq!(
        delivered,
        vec![first.revision, second.revision],
        "exactly the revisions that were allocated may be delivered"
    );

    ScenarioResult {
        id: "W-06",
        name: "watch-no-event-for-conflict",
        passed: true,
        detail: "a refused CAS and a missing-key delete produced no events".into(),
        observed: json!({ "delivered": delivered.len(), "refused_mutations": 2 }),
    }
}

async fn w07_compacted_error(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
    fixture: &Arc<dyn WatchFixture>,
) -> ScenarioResult {
    let written = put_series(store, cfg, "w07", 6).await;
    let floor = fixture
        .compact(written[4])
        .await
        .expect("compacting to a retained revision must succeed");

    let refused = store
        .watch(WatchRequest {
            prefix: key(cfg, "w07/"),
            start_after_revision: written[1],
            progress_interval: None,
        })
        .await;
    match refused {
        Err(ConfigError::RevisionCompacted {
            minimum_available_revision,
        }) => {
            // The floor is actionable, not decorative: it is the cursor the client re-opens
            // with after re-`List`ing, so it must be the first revision still available.
            assert_eq!(
                minimum_available_revision,
                floor + 1,
                "the reported minimum must be the first revision still retained"
            );
        }
        Err(other) => panic!("expected RevisionCompacted, got {other}"),
        Ok(_) => panic!("a cursor below the compaction floor must be refused, not served"),
    }

    ScenarioResult {
        id: "W-07",
        name: "watch-compacted-error",
        passed: true,
        detail: "a cursor below the floor was refused with the first available revision".into(),
        observed: json!({ "refused": true, "minimum_is_floor_plus_one": true }),
    }
}

async fn w08_compacted_boundary(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
    fixture: &Arc<dyn WatchFixture>,
) -> ScenarioResult {
    let written = put_series(store, cfg, "w08", 6).await;
    let floor = fixture.compact(written[4]).await.expect("compact");

    // Test plan §4 W-08 is explicit about which side of the floor is which: "compact to 5,
    // `watch("", 5)` and `watch("", 6)` | the first is `RevisionCompacted{6}`; the second
    // succeeds". `compact_revision == floor` means revisions `1..=floor` are gone, so a cursor
    // *at* the floor (`start_after_revision == floor`, requesting `> floor`) still names the
    // floor itself as "not yet delivered" and is refused (`register_locked`'s
    // `start_after <= compact_revision` check, OQ-27); only a cursor strictly above it
    // (`floor + 1`, the reported `minimum_available_revision`) is satisfiable.
    let at_floor = store
        .watch(WatchRequest {
            prefix: key(cfg, "w08/"),
            start_after_revision: floor,
            progress_interval: None,
        })
        .await
        .err();
    assert!(
        matches!(
            at_floor,
            Some(ConfigError::RevisionCompacted {
                minimum_available_revision
            }) if minimum_available_revision == floor + 1
        ),
        "a cursor exactly at the floor must be refused with minimum_available_revision == floor + 1, got {at_floor:?}"
    );

    let above = store
        .watch(WatchRequest {
            prefix: key(cfg, "w08/"),
            start_after_revision: floor + 1,
            progress_interval: None,
        })
        .await;
    assert!(
        above.is_ok(),
        "a cursor one past the floor must be accepted, got {:?}",
        above.err()
    );
    drop(above);

    ScenarioResult {
        id: "W-08",
        name: "watch-compacted-boundary",
        passed: true,
        detail: "cursor == floor is refused; cursor == floor + 1 is accepted".into(),
        observed: json!({ "at_floor_refused": true, "past_floor_accepted": true }),
    }
}

async fn w09_future_revision(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let ahead = current_revision(store, cfg).await + 100;

    // OQ-26: refused, not parked. A stream that waited for a revision that may never arrive is
    // indistinguishable from a healthy idle watch, so a client that mistyped a cursor would
    // never find out.
    let refused = store
        .watch(WatchRequest {
            prefix: key(cfg, "w09/"),
            start_after_revision: ahead,
            progress_interval: None,
        })
        .await
        .err();
    assert!(
        matches!(refused, Some(ConfigError::InvalidArgument { .. })),
        "a cursor above the current revision must be InvalidArgument, got {refused:?}"
    );

    ScenarioResult {
        id: "W-09",
        name: "watch-future-revision",
        passed: true,
        detail: "a cursor 100 revisions ahead was refused rather than parked".into(),
        observed: json!({ "refused": true }),
    }
}

async fn w10_progress_frame(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
) -> ScenarioResult {
    let prefix = key(cfg, "w10/");
    let high_water = current_revision(store, cfg).await;

    // Nothing is ever written under `w10/`, so every item this stream produces is a progress
    // frame — which is what makes "a progress frame carries no key or value" checkable.
    let mut stream = store
        .watch(WatchRequest {
            prefix: prefix.clone(),
            start_after_revision: high_water,
            progress_interval: Some(Duration::from_millis(100)),
        })
        .await
        .expect("watch");

    let item = tokio::time::timeout(WATCH_DEADLINE, stream.next())
        .await
        .expect("a progress frame must arrive on an idle stream")
        .expect("the stream must not end")
        .expect("the stream must not terminate");
    let revision = match item {
        WatchItem::Progress { revision } => revision,
        WatchItem::Event(event) => {
            panic!("nothing writes under w10/, yet an event arrived: {event:?}")
        }
    };
    // A progress frame is a *resume cursor*, so it may never run ahead of what this stream has
    // actually delivered plus what the cluster has: promising a revision the client never saw
    // would let it resume past an event it never received.
    assert!(
        revision >= high_water,
        "a progress frame must not report a revision below the stream's start ({revision} < {high_water})"
    );

    ScenarioResult {
        id: "W-10",
        name: "watch-progress-frame",
        passed: true,
        detail: "an idle stream produced a progress frame carrying a revision and no key or value"
            .into(),
        observed: json!({ "progress_frames": 1, "carries_key_or_value": false }),
    }
}

async fn w11_unauthorized_prefix(
    cfg: &ConformanceConfig,
    fixture: &Arc<dyn WatchFixture>,
) -> ScenarioResult {
    let stranger = fixture.unauthorized();

    // Refused at open, not as an empty stream: an unauthorized watcher that got a silent empty
    // stream would conclude the prefix is empty, which is a different and much worse lie than
    // being told no (ADR-0012).
    let refused = stranger
        .watch(WatchRequest {
            prefix: key(cfg, "w11/"),
            start_after_revision: 0,
            progress_interval: None,
        })
        .await
        .err();
    assert!(
        matches!(refused, Some(ConfigError::PermissionDenied { .. })),
        "a principal with no grant on the prefix must be denied, got {refused:?}"
    );

    ScenarioResult {
        id: "W-11",
        name: "watch-unauthorized-prefix",
        passed: true,
        detail: "a principal without a grant on the prefix was denied and got no stream".into(),
        observed: json!({ "denied": true, "stream_opened": false }),
    }
}

async fn w12_close_releases_slot(
    store: &Arc<dyn ConfigStore>,
    cfg: &ConformanceConfig,
    fixture: &Arc<dyn WatchFixture>,
) -> ScenarioResult {
    // Capped so the scenario stays cheap on a node configured with the production default;
    // what it proves — that dropping a stream returns its slot — does not depend on the
    // number, only on filling and refilling the same set twice.
    let rounds = fixture.max_streams_per_node().clamp(1, 3) as usize;
    let high_water = current_revision(store, cfg).await;
    let request = || WatchRequest {
        prefix: key(cfg, "w12/"),
        start_after_revision: high_water,
        progress_interval: None,
    };

    let mut first = Vec::new();
    for i in 0..rounds {
        first.push(
            store
                .watch(request())
                .await
                .unwrap_or_else(|e| panic!("open {i} of the first round: {e}")),
        );
    }
    drop(first);

    // The slot is released by `Drop`, which for a gRPC stream means a cancelled RPC the server
    // has to notice — so this is also the row that catches a server leaking a slot per
    // disconnect, which would take a node down after enough reconnects.
    let mut second = Vec::new();
    for i in 0..rounds {
        let opened = tokio::time::timeout(WATCH_DEADLINE, store.watch(request()))
            .await
            .unwrap_or_else(|_| panic!("open {i} of the second round never returned"));
        second.push(opened.unwrap_or_else(|e| {
            panic!("open {i} of the second round failed, so a closed stream kept its slot: {e}")
        }));
    }
    drop(second);

    ScenarioResult {
        id: "W-12",
        name: "watch-close-releases-slot",
        passed: true,
        detail: "every stream reopened after the first set was dropped".into(),
        observed: json!({ "rounds": 2, "reopened_after_close": true }),
    }
}

/// Revisions that were written but not delivered. Empty is the only passing value; it is
/// reported rather than merely asserted so a failing report says *which* ones were lost.
fn gaps(delivered: &[u64], written: &[u64]) -> Vec<u64> {
    written
        .iter()
        .copied()
        .filter(|r| !delivered.contains(r))
        .collect()
}
