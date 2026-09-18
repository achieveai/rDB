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

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, DeleteRequest, GetRequest, Limits, ListRequest, MutationOutcome,
    PutRequest, TransportSecurity,
};
use futures::FutureExt;
use serde::Serialize;
use serde_json::json;

/// Parameters for one [`run_all`] call.
#[derive(Debug, Clone)]
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
}

static PREFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

impl ConformanceConfig {
    /// Build a config with [`Limits::DEFAULT`] and the given fixed `key_prefix`.
    pub fn new(key_prefix: impl Into<Vec<u8>>) -> Self {
        Self {
            limits: Limits::DEFAULT,
            key_prefix: key_prefix.into(),
            expect_transport_security: None,
        }
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
    /// all [`SCENARIO_COUNT`] scenarios actually ran.
    pub fn assert_all_passed(&self) {
        assert_eq!(
            self.results.len(),
            SCENARIO_COUNT,
            "the conformance report has {} scenarios, expected {SCENARIO_COUNT} (C-01..C-15); ran: {:?}",
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
        })
        .await
        .expect("first put succeeds");
    let second = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"same"),
            expected_mod_revision: None,
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
        })
        .await
        .expect("create-only put on an absent key succeeds");
    assert_eq!(created.outcome, MutationOutcome::Applied);

    let repeat = store
        .put(PutRequest {
            key: k,
            value: Bytes::from_static(b"v2"),
            expected_mod_revision: Some(0),
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
        })
        .await
        .expect("unconditional put succeeds");

    let wrong_expected = first.revision + 999;
    let conflict = store
        .put(PutRequest {
            key: k.clone(),
            value: Bytes::from_static(b"v2"),
            expected_mod_revision: Some(wrong_expected),
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
        })
        .await
        .expect("put succeeds");

    let wrong_expected = put.revision + 999;
    let resp = store
        .delete(DeleteRequest {
            key: k.clone(),
            expected_mod_revision: Some(wrong_expected),
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
        })
        .await
        .expect("put succeeds");

    let conflict = store
        .put(PutRequest {
            key: k,
            value: Bytes::from_static(b"attacker-supplied"),
            expected_mod_revision: Some(put.revision + 999),
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
