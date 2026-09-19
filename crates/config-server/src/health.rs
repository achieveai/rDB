//! The loopback health endpoint (OQ-16, ADR-0018 §2).
//!
//! `GET /health` returns [`config_engine::HealthPayload`] as JSON; `GET /metrics` returns the
//! ADR-0026 Prometheus exposition. Both payloads are ids, counts, revisions, enums and digests
//! — no keys and no values — which is why neither needs authentication (§15.2). Both are still
//! bound to loopback only; the address is rejected at configuration time otherwise.
//!
//! `/metrics` shares this listener rather than opening its own port: a second listener is a
//! second thing to bind, firewall and get wrong for one text route, and a deployment that
//! wants off-box scraping fronts this one with its own proxy (ADR-0026).
//!
//! # Why HTTP is written by hand
//!
//! Adding axum or hyper's server to the dependency graph for one route would be a new web
//! framework in a project whose only transport is gRPC. The subset below — read the request
//! line, answer, close — is all `GET /health` needs, and `Connection: close` means there is
//! no keep-alive state machine to get wrong.

use std::sync::Arc;

use config_engine::ConfigNode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Largest request head this endpoint will read before giving up. A health probe's request is
/// a few hundred bytes; anything larger is not one.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// Serve `GET /health` on `listener` until `shutdown` resolves.
///
/// Returns when the shutdown signal fires; in-flight responses are already written by then,
/// because each connection is answered and closed in one short task.
///
/// `metrics_enabled` switches `GET /metrics` off at the route: the path then 404s exactly
/// like any other unknown path, so a scraper sees a missing endpoint rather than an endpoint
/// that answers with nothing - the difference between "not exported here" and "exported and
/// idle".
pub async fn serve(
    listener: TcpListener,
    node: ConfigNode,
    shutdown: Arc<tokio::sync::Notify>,
    metrics_enabled: bool,
) {
    loop {
        let accepted = tokio::select! {
            biased;
            () = shutdown.notified() => return,
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((stream, _peer)) => {
                let node = node.clone();
                tokio::spawn(config_log::testing::in_current_span(async move {
                    if let Err(e) = handle(stream, node, metrics_enabled).await {
                        tracing::debug!(error = %e, "health connection ended early");
                    }
                }));
            }
            Err(e) => {
                // A failed accept on a loopback listener is not fatal to the node; the health
                // surface is an oracle, not a dependency.
                tracing::warn!(error = %e, "health listener accept failed");
                return;
            }
        }
    }
}

async fn handle(
    mut stream: TcpStream,
    node: ConfigNode,
    metrics_enabled: bool,
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    // Read until the end of the request head; a health probe sends no body.
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..read]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() >= MAX_REQUEST_BYTES {
            break;
        }
    }

    let head = String::from_utf8_lossy(&buf);
    let request_line = head.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let path = target.split('?').next().unwrap_or_default();

    let response = if method == "GET" && path == "/health" {
        let payload = node.health_payload().await;
        match serde_json::to_vec(&payload) {
            Ok(body) => http_response(200, "OK", "application/json", &body),
            Err(e) => {
                tracing::error!(error = %e, "health payload did not serialize");
                http_response(500, "Internal Server Error", "text/plain", b"error")
            }
        }
    } else if method == "GET" && path == "/metrics" && metrics_enabled {
        let body = node.metrics_report().await.render_prometheus().into_bytes();
        // The version parameter is not decoration: a scraper uses it to pick its parser, and
        // omitting it makes some scrapers fall back to a format this is not.
        http_response(200, "OK", "text/plain; version=0.0.4; charset=utf-8", &body)
    } else {
        http_response(404, "Not Found", "text/plain", b"not found")
    };

    stream.write_all(&response).await?;
    stream.flush().await
}

fn http_response(status: u16, reason: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Cache-Control: no-store\r\n\
         Connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_response_carries_an_accurate_content_length() {
        let body = br#"{"ready":true}"#;
        let response = http_response(200, "OK", "application/json", body);
        let text = String::from_utf8(response).expect("ascii head, json body");
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains(&format!("Content-Length: {}\r\n", body.len())));
        assert!(text.ends_with(r#"{"ready":true}"#));
    }

    #[test]
    fn a_404_is_not_a_json_payload() {
        let response = http_response(404, "Not Found", "text/plain", b"not found");
        let text = String::from_utf8(response).expect("ascii");
        assert!(text.starts_with("HTTP/1.1 404 Not Found"));
        assert!(!text.contains("application/json"));
    }
}
