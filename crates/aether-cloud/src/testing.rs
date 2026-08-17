//! A tiny HTTP server for testing the adapters.
//!
//! Cloud credentials cannot live in a test suite, so the adapters are tested
//! against their **HTTP contract** instead: the request they send and the
//! response they parse. That catches the things that actually break — a wrong
//! path, a missing header, a misread field — without an account.

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// One request the server received.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl RecordedRequest {
    /// Header lookup, case-insensitive as HTTP requires.
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
    }
}

/// Serves a scripted list of responses, in order, and records what it was asked.
pub struct MockServer {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl MockServer {
    /// Starts a server that answers with `(status, body)` in sequence, reusing
    /// the last entry once the script runs out.
    pub async fn start(responses: Vec<(u16, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));

        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let mut index = 0usize;
            while let Ok((stream, _)) = listener.accept().await {
                let (status, body) = responses
                    .get(index.min(responses.len().saturating_sub(1)))
                    .cloned()
                    .unwrap_or((200, "{}".to_string()));
                index += 1;

                if let Some(request) = serve_one(stream, status, &body).await {
                    recorded.lock().await.push(request);
                }
            }
        });

        Self {
            address,
            requests,
            _task: task,
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Everything the server has been asked so far.
    pub async fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().await.clone()
    }
}

/// Reads one request, answers it, and returns what was read.
async fn serve_one(
    stream: tokio::net::TcpStream,
    status: u16,
    body: &str,
) -> Option<RecordedRequest> {
    let mut reader = BufReader::new(stream);

    let mut request_line = String::new();
    reader.read_line(&mut request_line).await.ok()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await.ok()?;
        let line = line.trim_end().to_string();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_string();
            let value = value.trim().to_string();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
    }

    let mut body_bytes = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body_bytes).await.ok()?;
    }

    let response = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = reader.into_inner();
    stream.write_all(response.as_bytes()).await.ok()?;
    stream.flush().await.ok()?;

    Some(RecordedRequest {
        method,
        path,
        headers,
        body: String::from_utf8_lossy(&body_bytes).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_server_records_what_it_was_asked() {
        let server = MockServer::start(vec![(200, r#"{"ok":true}"#.to_string())]).await;
        let client = reqwest::Client::new();

        let response = client
            .post(format!("{}/things", server.base_url()))
            .header("x-test", "yes")
            .body("hello")
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), r#"{"ok":true}"#);

        let requests = server.requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/things");
        assert_eq!(requests[0].body, "hello");
        assert_eq!(requests[0].header("X-Test").as_deref(), Some("yes"));
    }
}
