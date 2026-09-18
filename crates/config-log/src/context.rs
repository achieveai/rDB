//! Cross-wire trace context (ADR-0013).
//!
//! A [`TraceContext`] is carried explicitly through request structs and gRPC metadata and
//! recorded as span fields `trace_id`, `span_id`, `parent_span_id`, `request_id`. Because the
//! JSONL layer flattens span fields, every line inside such a span carries them, on every node
//! the request touches.

use tracing::Span;
use tracing_subscriber::registry::LookupSpan;

/// gRPC metadata key for the trace id (32 hex chars).
pub const HEADER_TRACE_ID: &str = "retcd-trace-id";
/// gRPC metadata key for the caller's span id (16 hex chars).
pub const HEADER_PARENT_SPAN: &str = "retcd-parent-span";
/// gRPC metadata key for the client request id.
pub const HEADER_REQUEST_ID: &str = "retcd-request-id";

/// Identifies one logical operation across processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// Shared by every span of the operation, on every node.
    pub trace_id: String,
    /// This hop's span id.
    pub span_id: String,
    /// The caller's span id, if any.
    pub parent_span_id: Option<String>,
    /// Client-chosen request id (defaults to a fresh uuid).
    pub request_id: String,
}

fn hex32() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

impl TraceContext {
    /// A brand new root context.
    pub fn new_root() -> Self {
        let trace = hex32();
        Self {
            span_id: trace[..16].to_string(),
            request_id: hex32(),
            trace_id: trace,
            parent_span_id: None,
        }
    }

    /// A child context for the next hop (same trace and request, new span, parent = self).
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: hex32()[..16].to_string(),
            parent_span_id: Some(self.span_id.clone()),
            request_id: self.request_id.clone(),
        }
    }

    /// Rebuild from wire headers; missing/invalid trace id yields a fresh root.
    pub fn from_headers(
        trace_id: Option<&str>,
        parent_span: Option<&str>,
        request_id: Option<&str>,
    ) -> Self {
        let trace_id = match trace_id {
            Some(t) if t.len() == 32 && t.bytes().all(|b| b.is_ascii_hexdigit()) => t.to_string(),
            _ => hex32(),
        };
        Self {
            trace_id,
            span_id: hex32()[..16].to_string(),
            parent_span_id: parent_span.map(str::to_string),
            request_id: request_id.map(str::to_string).unwrap_or_else(hex32),
        }
    }

    /// Header pairs to inject into an outgoing call. The remote side should call
    /// [`TraceContext::from_headers`] and then treat the result as its own hop.
    pub fn to_headers(&self) -> [(&'static str, String); 3] {
        [
            (HEADER_TRACE_ID, self.trace_id.clone()),
            (HEADER_PARENT_SPAN, self.span_id.clone()),
            (HEADER_REQUEST_ID, self.request_id.clone()),
        ]
    }

    /// Open a span for this hop with the standard field names. `op` is e.g. `"put"`.
    pub fn span(&self, op: &'static str) -> Span {
        tracing::info_span!(
            "op",
            op,
            trace_id = %self.trace_id,
            span_id = %self.span_id,
            parent_span_id = self.parent_span_id.as_deref().unwrap_or(""),
            request_id = %self.request_id,
        )
    }

    /// Read the context recorded in the current span scope, if any. Walks leaf → root and
    /// takes the first `trace_id`/`span_id`/`request_id` it finds. Returns `None` when no
    /// enclosing span carries a trace id (or the subscriber is not ours).
    pub fn current() -> Option<Self> {
        let span = Span::current();
        let id = span.id()?;
        tracing::dispatcher::get_default(|dispatch| {
            let registry = dispatch.downcast_ref::<tracing_subscriber::Registry>()?;
            let leaf = registry.span(&id)?;
            let mut trace_id = None;
            let mut span_id = None;
            let mut parent = None;
            let mut request_id = None;
            for s in leaf.scope() {
                let ext = s.extensions();
                let Some(f) = ext.get::<crate::layer::JsonFields>() else {
                    continue;
                };
                if trace_id.is_none() {
                    trace_id =
                        f.0.get("trace_id")
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                    span_id =
                        f.0.get("span_id")
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                    parent =
                        f.0.get("parent_span_id")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    request_id =
                        f.0.get("request_id")
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                }
                if trace_id.is_some() {
                    break;
                }
            }
            Some(Self {
                trace_id: trace_id?,
                span_id: span_id.unwrap_or_else(|| hex32()[..16].to_string()),
                parent_span_id: parent,
                request_id: request_id.unwrap_or_else(hex32),
            })
        })
    }

    /// [`TraceContext::current`] or a new root.
    pub fn current_or_root() -> Self {
        Self::current().unwrap_or_else(Self::new_root)
    }
}
