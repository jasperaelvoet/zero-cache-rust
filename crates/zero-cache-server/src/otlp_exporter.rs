//! OTLP/HTTP metrics push exporter — the delivery half of OTLP export. The
//! payload serialization lives in
//! `zero_cache_services::metrics::InMemoryBackend::render_otlp_json`; this
//! module POSTs that body to a configured OTel-collector `/v1/metrics`
//! endpoint over HTTP with `Content-Type: application/json`.
//!
//! A production process constructs one [`OtlpExporter`] with the collector URL
//! and calls [`OtlpExporter::push`] on a periodic tick. The only thing this
//! cannot do without a *running* collector is get a 2xx back from a real one —
//! but the request construction and delivery are exercised end-to-end against a
//! mock collector in the tests below.

use zero_cache_services::metrics::InMemoryBackend;

/// A push exporter targeting one OTLP/HTTP collector endpoint.
pub struct OtlpExporter {
    client: reqwest::Client,
    endpoint: String,
}

#[derive(Debug, thiserror::Error)]
pub enum OtlpExportError {
    #[error("OTLP export request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("OTLP collector returned non-success status {0}")]
    Status(u16),
}

impl OtlpExporter {
    /// `endpoint` is the collector's metrics URL (e.g.
    /// `http://localhost:4318/v1/metrics`).
    pub fn new(endpoint: impl Into<String>) -> Self {
        OtlpExporter {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
        }
    }

    /// POSTs the given OTLP/HTTP JSON body to the collector. Returns the HTTP
    /// status on a 2xx, else [`OtlpExportError::Status`].
    pub async fn push_body(&self, otlp_json_body: String) -> Result<u16, OtlpExportError> {
        let resp = self
            .client
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .body(otlp_json_body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        if resp.status().is_success() {
            Ok(status)
        } else {
            Err(OtlpExportError::Status(status))
        }
    }

    /// Renders the backend's current metrics as OTLP/HTTP JSON and pushes them —
    /// the one call a periodic exporter tick makes.
    pub async fn push(&self, backend: &InMemoryBackend) -> Result<u16, OtlpExportError> {
        self.push_body(backend.render_otlp_json()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use zero_cache_services::metrics::{Category, Metrics};

    /// A minimal mock OTLP collector: accepts one HTTP/1.1 POST, captures the
    /// request bytes, and replies `200 OK`. Returns (addr, captured-body-cell).
    async fn mock_collector() -> (String, Arc<Mutex<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(String::new()));
        let captured_srv = captured.clone();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 65536];
            let n = sock.read(&mut buf).await.unwrap();
            captured_srv
                .lock()
                .unwrap()
                .push_str(&String::from_utf8_lossy(&buf[..n]));
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        (format!("http://{addr}/v1/metrics"), captured)
    }

    #[tokio::test]
    async fn pushes_otlp_body_to_a_collector_and_gets_2xx() {
        let (endpoint, captured) = mock_collector().await;

        let backend = Arc::new(zero_cache_services::metrics::InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        metrics
            .get_or_create_counter(Category::Replication, "commit")
            .add(7.0);

        let exporter = OtlpExporter::new(endpoint);
        let status = exporter.push(&backend).await.unwrap();
        assert_eq!(status, 200, "mock collector accepted the OTLP push");

        // The collector actually received the OTLP payload for our metric.
        let req = captured.lock().unwrap().clone();
        assert!(req.starts_with("POST /v1/metrics HTTP/1.1"), "got:\n{req}");
        assert!(
            req.contains("content-type: application/json"),
            "got:\n{req}"
        );
        assert!(
            req.contains("\"name\":\"zero.replication.commit\""),
            "OTLP body delivered:\n{req}"
        );
        assert!(req.contains("\"asDouble\":7"), "got:\n{req}");
    }

    #[tokio::test]
    async fn non_success_status_is_an_error() {
        // A collector that replies 503.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = sock.read(&mut buf).await.unwrap();
            let resp = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });

        let exporter = OtlpExporter::new(format!("http://{addr}/v1/metrics"));
        let err = exporter.push_body("{}".into()).await.unwrap_err();
        assert!(matches!(err, OtlpExportError::Status(503)), "got {err:?}");
    }
}
