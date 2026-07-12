//! HTTP request inspection and small responses shared by the public and
//! change-streamer listeners.
//!
//! The server keeps tokio-tungstenite as the WebSocket implementation.  A TCP
//! request is inspected with `peek` before the WebSocket handshake consumes it,
//! allowing the same listener to serve Zero's ordinary HTTP routes.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_HEAD_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    pub method: String,
    pub target: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
}

impl RequestHead {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    pub fn is_websocket_upgrade(&self) -> bool {
        self.header("upgrade")
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
            && self.header("connection").is_some_and(|value| {
                value
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("upgrade"))
            })
    }
}

pub async fn peek_request(stream: &TcpStream) -> std::io::Result<RequestHead> {
    let mut buf = vec![0_u8; MAX_HEAD_BYTES];
    let n = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let n = stream.peek(&mut buf).await?;
            if n == 0 || buf[..n].windows(4).any(|window| window == b"\r\n\r\n") {
                return Ok::<usize, std::io::Error>(n);
            }
            if n == buf.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "HTTP request headers are too large",
                ));
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "HTTP header timeout"))??;

    let text = std::str::from_utf8(&buf[..n])
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 HTTP head"))?;
    parse_request_head(text)
}

fn parse_request_head(text: &str) -> std::io::Result<RequestHead> {
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let target = request_parts.next().unwrap_or_default();
    let version = request_parts.next().unwrap_or_default();
    if method.is_empty() || target.is_empty() || !version.starts_with("HTTP/1.") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed HTTP request line",
        ));
    }
    let mut headers = BTreeMap::new();
    for line in lines.take_while(|line| !line.is_empty()) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        headers
            .entry(name)
            .and_modify(|current: &mut String| {
                current.push_str(", ");
                current.push_str(value);
            })
            .or_insert_with(|| value.to_string());
    }
    let path = target.split('?').next().unwrap_or("/").to_string();
    Ok(RequestHead {
        method: method.to_string(),
        target: target.to_string(),
        path,
        headers,
    })
}

pub struct HttpResponse {
    pub status: &'static str,
    pub content_type: &'static str,
    pub body: Vec<u8>,
    pub headers: Vec<(&'static str, String)>,
    /// Send status and headers (including the Content-Length of `body`) but
    /// not the body itself — the response to a HEAD request. Upstream fastify
    /// exposes a HEAD route for every GET route (`exposeHeadRoutes`).
    pub head_only: bool,
}

impl HttpResponse {
    pub fn text(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.into().into_bytes(),
            headers: Vec::new(),
            head_only: false,
        }
    }

    pub fn json(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: body.into().into_bytes(),
            headers: Vec::new(),
            head_only: false,
        }
    }

    pub fn for_method(mut self, method: &str) -> Self {
        self.head_only = method == "HEAD";
        self
    }
}

pub async fn send_response(mut stream: TcpStream, response: HttpResponse) {
    // `peek_request` deliberately leaves the request for a possible WebSocket
    // handshake. For an ordinary HTTP response, consume the header first so
    // closing the socket does not produce a TCP reset with unread input.
    let mut request = Vec::with_capacity(1024);
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        let mut chunk = [0_u8; 1024];
        while request.len() < MAX_HEAD_BYTES {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..n]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    })
    .await;
    let mut head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    for (name, value) in response.headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(&value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes()).await;
    if !response.head_only {
        let _ = stream.write_all(&response.body).await;
    }
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_upgrade_request() {
        let req = parse_request_head(
            "GET /sync/v51/connect?clientID=c HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\n\r\n",
        )
        .unwrap();
        assert_eq!(req.path, "/sync/v51/connect");
        assert!(req.is_websocket_upgrade());
    }
}
