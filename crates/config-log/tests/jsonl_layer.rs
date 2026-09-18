//! Proves ADR-0013: canonical fields, flattened span fields, per-test routing, trace
//! context propagation through headers, and `TraceContext::current()`.

use std::sync::{Arc, Mutex};

use config_log::layer::{test_file_path, JsonlLayer};
use config_log::{TraceContext, HEADER_PARENT_SPAN, HEADER_REQUEST_ID, HEADER_TRACE_ID};
use serde_json::Value;
use tracing::Instrument;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Buf {
    fn lines(&self) -> Vec<Value> {
        let bytes = self.0.lock().unwrap().clone();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).expect("valid json line"))
            .collect()
    }
}

fn subscriber(buf: &Buf, test_dir: Option<std::path::PathBuf>) -> impl tracing::Subscriber {
    tracing_subscriber::registry().with(JsonlLayer::new(
        "unit",
        Box::new(buf.clone()),
        test_dir,
        true,
    ))
}

#[test]
fn canonical_fields_and_flattened_spans() {
    let buf = Buf::default();
    tracing::subscriber::with_default(subscriber(&buf, None), || {
        let outer = tracing::info_span!("outer", node_id = 7u64, role = "leader", shared = "outer");
        let _o = outer.enter();
        let inner = tracing::info_span!("inner", shared = "inner", log_index = 42u64);
        let _i = inner.enter();
        tracing::warn!(key_hex = "6162", value = "SECRET", "applied {}", "x");
    });
    let lines = buf.lines();
    assert_eq!(lines.len(), 1);
    let l = &lines[0];
    assert_eq!(l["@l"], "Warning");
    assert_eq!(l["@m"], "applied x");
    assert_eq!(l["@logger"], "jsonl_layer");
    assert_eq!(l["application"], "unit");
    assert_eq!(l["span"], "inner");
    assert_eq!(l["node_id"], 7);
    assert_eq!(l["role"], "leader");
    assert_eq!(l["shared"], "inner", "leaf span overrides root");
    assert_eq!(l["log_index"], 42);
    assert_eq!(l["key_hex"], "6162");
    assert_eq!(l["value"], "<redacted>", "value fields are always redacted");
    assert!(l["@t"].as_str().unwrap().ends_with('Z'));
    assert!(l.get("file").is_some() && l.get("line").is_some());
}

#[test]
fn per_test_routing_writes_to_module_method_file() {
    let dir = tempfile::tempdir().unwrap();
    let buf = Buf::default();
    tracing::subscriber::with_default(subscriber(&buf, Some(dir.path().to_path_buf())), || {
        let span = tracing::info_span!(
            "test",
            testModule = "m1_cluster::tests",
            testMethod = "forms_leader",
            testRun = "run1"
        );
        let _g = span.enter();
        tracing::info!("inside test");
        drop(_g);
        tracing::info!("outside test");
    });
    let path = test_file_path(dir.path(), "m1_cluster::tests", "forms_leader");
    assert!(
        path.ends_with("m1_cluster.tests/forms_leader.jsonl"),
        "{}",
        path.display()
    );
    let content = std::fs::read_to_string(&path).unwrap();
    let v: Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
    assert_eq!(v["testMethod"], "forms_leader");
    assert_eq!(v["testModule"], "m1_cluster::tests");
    assert_eq!(v["testRun"], "run1");
    assert_eq!(v["@m"], "inside test");
    let default_lines = buf.lines();
    assert_eq!(default_lines.len(), 1);
    assert_eq!(default_lines[0]["@m"], "outside test");
}

#[tokio::test]
async fn trace_context_round_trips_and_is_readable_from_span() {
    let buf = Buf::default();
    let sub = subscriber(&buf, None);
    let _g = tracing::subscriber::set_default(sub);

    let root = TraceContext::new_root();
    assert_eq!(root.trace_id.len(), 32);
    assert_eq!(root.span_id.len(), 16);

    // client side
    let headers = root.to_headers();
    let get = |k: &str| {
        headers
            .iter()
            .find(|(n, _)| *n == k)
            .map(|(_, v)| v.as_str())
    };
    let server_ctx = TraceContext::from_headers(
        get(HEADER_TRACE_ID),
        get(HEADER_PARENT_SPAN),
        get(HEADER_REQUEST_ID),
    );
    assert_eq!(server_ctx.trace_id, root.trace_id);
    assert_eq!(server_ctx.request_id, root.request_id);
    assert_eq!(
        server_ctx.parent_span_id.as_deref(),
        Some(root.span_id.as_str())
    );
    assert_ne!(server_ctx.span_id, root.span_id);

    // server side: inside the hop span, current() recovers the context even from a nested span
    let observed = async {
        let nested = tracing::info_span!("apply", log_index = 3u64);
        async {
            tracing::debug!("applying");
            TraceContext::current()
        }
        .instrument(nested)
        .await
    }
    .instrument(server_ctx.span("put"))
    .await
    .expect("context visible");
    assert_eq!(observed.trace_id, root.trace_id);
    assert_eq!(observed.span_id, server_ctx.span_id);
    assert_eq!(observed.request_id, root.request_id);

    let lines = buf.lines();
    let apply = lines.iter().find(|l| l["@m"] == "applying").unwrap();
    assert_eq!(apply["trace_id"], root.trace_id);
    assert_eq!(apply["op"], "put");
    assert_eq!(apply["log_index"], 3);
    assert_eq!(apply["parent_span_id"], root.span_id);

    assert!(TraceContext::current().is_none(), "no span -> none");
}

#[tokio::test]
async fn garbage_trace_header_yields_fresh_root() {
    let ctx = TraceContext::from_headers(Some("not-hex"), None, None);
    assert_eq!(ctx.trace_id.len(), 32);
    assert!(ctx.parent_span_id.is_none());
}
