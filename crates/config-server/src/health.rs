//! The loopback health endpoint (OQ-16, ADR-0018 §2).
//!
//! `GET /health` returns [`config_engine::HealthPayload`] as JSON, plus nothing else. The
//! payload is ids, counts, revisions, enums and a digest — no keys and no values — which is
//! why it can be served without authentication (§15.2). It is still bound to loopback only;
//! the address is rejected at configuration time otherwise.
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
pub async fn serve(listener: TcpListener, node: ConfigNode, shutdown: Arc<tokio::sync::Notify>) {
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
                    if let Err(e) = handle(stream, node).await {
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

async fn handle(mut stream: TcpStream, node: ConfigNode) -> std::io::Result<()> {
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
