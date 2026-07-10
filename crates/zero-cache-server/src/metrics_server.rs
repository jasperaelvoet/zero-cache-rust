//! A tiny HTTP endpoint for operations: Prometheus metrics + health/readiness.
//!
//! The sync server speaks WebSocket on its main port; production deployments
//! also need a scrapeable `/metrics` and liveness/readiness probes. Rather than
//! pull in a full HTTP framework, this is a minimal hand-rolled HTTP/1.1
//! responder (same approach as the OTLP mock collector in the tests) serving:
//!   * `GET /metrics`  → Prometheus text exposition (`render_prometheus`)
//!   * `GET /healthz`  → `200 ok` (process is alive)
//!   * `GET /readyz`   → `200 ready` once initial sync completed, else `503`
//!
//! Runs until `shutdown` fires.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use zero_cache_services::metrics::InMemoryBackend;

/// Serves the ops endpoint on `addr` until `shutdown` fires. `backend` is the
/// live metrics backend to render; `ready` reports initial-sync readiness.
pub async fn run_metrics_server(
    addr: &str,
    backend: Arc<InMemoryBackend>,
    ready: Arc<AtomicBool>,
    shutdown: oneshot::Receiver<()>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let Ok((mut sock, _)) = accepted else { continue };
                let backend = backend.clone();
                let ready = ready.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let Ok(n) = sock.read(&mut buf).await else { return };
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let target = request_target(&req);
                    let (status, content_type, body) = route(&target, &backend, &ready);
                    let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        }
    }
}

/// The request target from an HTTP request's first line (`GET /path HTTP/1.1`).
fn request_target(req: &str) -> String {
    req.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .to_string()
}

/// Routes a request target to `(status, content_type, body)`.
fn route(
    target: &str,
    backend: &InMemoryBackend,
    ready: &AtomicBool,
) -> (&'static str, &'static str, String) {
    match target {
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4",
            backend.render_prometheus(),
        ),
        "/healthz" => ("200 OK", "text/plain", "ok".to_string()),
        "/readyz" => {
            if ready.load(Ordering::SeqCst) {
                ("200 OK", "text/plain", "ready".to_string())
            } else {
                (
                    "503 Service Unavailable",
                    "text/plain",
                    "not ready".to_string(),
                )
            }
        }
        _ => ("404 Not Found", "text/plain", "not found".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;
    use zero_cache_services::metrics::{Category, Metrics};

    async fn http_get(addr: &str, path: &str) -> String {
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
        let mut resp = String::new();
        sock.read_to_string(&mut resp).await.unwrap();
        resp
    }

    #[tokio::test]
    async fn serves_metrics_health_and_readiness() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        metrics
            .get_or_create_counter(Category::Server, "connections")
            .add(5.0);
        let ready = Arc::new(AtomicBool::new(false));
        let (tx, rx) = oneshot::channel();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener); // free the port for run_metrics_server to bind
        let addr2 = addr.clone();
        let backend2 = backend.clone();
        let ready2 = ready.clone();
        let server =
            tokio::spawn(async move { run_metrics_server(&addr2, backend2, ready2, rx).await });
        // Give it a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // /metrics carries the counter.
        let m = http_get(&addr, "/metrics").await;
        assert!(m.starts_with("HTTP/1.1 200"), "{m}");
        assert!(m.contains("zero_server_connections 5"), "{m}");

        // /healthz is always ok.
        assert!(http_get(&addr, "/healthz").await.contains("200 OK"));

        // /readyz is 503 until ready flips, then 200.
        assert!(http_get(&addr, "/readyz").await.contains("503"));
        ready.store(true, Ordering::SeqCst);
        assert!(http_get(&addr, "/readyz").await.contains("200 OK"));

        // Unknown path 404s.
        assert!(http_get(&addr, "/nope").await.contains("404"));

        let _ = tx.send(());
        let _ = server.await;
    }
}
