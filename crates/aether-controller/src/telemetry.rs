//! A scrape endpoint, so the counters reach a dashboard instead of a log line.
//!
//! The counters have always existed and always been formatted for Prometheus;
//! there was simply no way to fetch them without reading the process output.
//! This serves them over HTTP.
//!
//! It speaks the smallest useful subset of HTTP/1.1 — read a request line,
//! discard headers, write one response, close — rather than pulling a web
//! framework into the control plane for two read-only routes.
//!
//! # What it does not expose
//!
//! Counters and aggregate gauges only: no hostnames, no node ids, no addresses,
//! no task payloads. The endpoint has no authentication, and an operator will
//! eventually put it somewhere convenient rather than somewhere safe, so it is
//! built to be uninteresting to whoever finds it. Bind it to localhost anyway.

use std::io::ErrorKind;
use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{debug, warn};

use crate::state::MeshState;

/// Most of a request that is ever read: line plus headers.
///
/// The reader is capped rather than each line, so a client that never sends a
/// newline runs into the same limit as one that sends a thousand headers.
const MAX_REQUEST_BYTES: u64 = 16 * 1024;

/// Headers read before giving up. A scrape has a handful.
const MAX_HEADERS: usize = 64;

/// Binds the telemetry listener, returning the address actually bound.
pub async fn bind_metrics(addr: SocketAddr) -> std::io::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    Ok((listener, local_addr))
}

/// Serves `/metrics` and `/healthz` until the listener fails.
pub async fn serve_metrics(listener: TcpListener, state: MeshState) -> std::io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            match handle(stream, state).await {
                Ok(()) => debug!(%peer, "metrics scraped"),
                Err(error) => debug!(%peer, %error, "metrics request failed"),
            }
        });
    }
}

/// Answers one request and closes.
async fn handle<S>(stream: S, state: MeshState) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader.take(MAX_REQUEST_BYTES));

    let mut request_line = String::new();
    if reader.read_line(&mut request_line).await? == 0 {
        return Ok(());
    }

    // Headers are read only so the socket drains cleanly before the response.
    let mut header = String::new();
    for _ in 0..MAX_HEADERS {
        header.clear();
        let read = reader.read_line(&mut header).await?;
        if read == 0 || header.trim().is_empty() {
            break;
        }
    }

    let response = route(&request_line, &state);
    writer.write_all(response.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

/// Picks the response for a request line like `GET /metrics HTTP/1.1`.
fn route(request_line: &str, state: &MeshState) -> String {
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    // A query string is accepted and ignored; scrapers add them.
    let path = target.split('?').next().unwrap_or_default();

    if method != "GET" && method != "HEAD" {
        return response(405, "text/plain; charset=utf-8", "only GET is supported\n");
    }

    match path {
        "/metrics" => response(
            200,
            // The version suffix is what Prometheus looks for to skip sniffing.
            "text/plain; version=0.0.4; charset=utf-8",
            &metrics_body(state),
        ),
        "/healthz" => response(200, "text/plain; charset=utf-8", "ok\n"),
        _ => response(
            404,
            "text/plain; charset=utf-8",
            "try /metrics or /healthz\n",
        ),
    }
}

/// The counters, plus gauges read from the live mesh.
fn metrics_body(state: &MeshState) -> String {
    let nodes = state.nodes();
    let connected = nodes
        .iter()
        .filter(|node| state.connections.is_connected(node.id))
        .count();

    // Averages rather than per-node series: a node id in a metric label is how
    // an unauthenticated endpoint turns into an inventory of the network.
    let (cpu, memory) = if nodes.is_empty() {
        (0.0, 0.0)
    } else {
        let count = nodes.len() as f32;
        (
            nodes.iter().map(|node| node.metrics.cpu_usage).sum::<f32>() / count,
            nodes
                .iter()
                .map(|node| node.metrics.memory_usage)
                .sum::<f32>()
                / count,
        )
    };

    let (datasets, dataset_bytes) = state.catalog.totals();
    let gauges = [
        ("aethermesh_nodes", nodes.len() as f64),
        ("aethermesh_nodes_connected", connected as f64),
        ("aethermesh_cpu_usage_mean", f64::from(cpu)),
        ("aethermesh_memory_usage_mean", f64::from(memory)),
        ("aethermesh_datasets", datasets as f64),
        ("aethermesh_dataset_bytes", dataset_bytes as f64),
    ];

    // Traffic is what the project claims to improve, so it belongs on the
    // endpoint an operator actually watches.
    let traffic = state.traffic.snapshot();
    let counters = [
        ("aethermesh_data_bytes_sent_total", traffic.data_bytes_sent),
        (
            "aethermesh_data_bytes_uncompressed_total",
            traffic.data_bytes_uncompressed,
        ),
        (
            "aethermesh_transfers_skipped_total",
            traffic.transfers_skipped,
        ),
        ("aethermesh_chunks_skipped_total", traffic.chunks_skipped),
        ("aethermesh_task_retries_total", traffic.retries),
    ];

    let mut body = state.metrics.snapshot().to_prometheus();
    for (name, value) in counters {
        body.push_str(&format!(
            "
# TYPE {name} counter
{name} {value}"
        ));
    }
    for (name, value) in gauges {
        body.push_str(&format!("\n# TYPE {name} gauge\n{name} {value}"));
    }
    body.push('\n');
    body
}

fn response(status: u16, content_type: &str, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };

    format!(
        "HTTP/1.1 {status} {reason}\r\n\
         content-type: {content_type}\r\n\
         content-length: {len}\r\n\
         cache-control: no-store\r\n\
         connection: close\r\n\
         \r\n\
         {body}",
        len = body.len()
    )
}

/// Logs a listener that stopped, since nothing else is watching it.
pub fn report_metrics_exit(result: std::io::Result<()>) {
    if let Err(error) = result
        && error.kind() != ErrorKind::Interrupted
    {
        warn!(%error, "telemetry listener stopped");
    }
}

#[cfg(test)]
mod tests {
    use aether_core::{NodeId, NodeInfo, NodeMetrics};

    use super::*;

    fn state_with_a_busy_node() -> MeshState {
        let state = MeshState::new();
        let mut info = NodeInfo::new(NodeId::generate(), "worker", "127.0.0.1:7001", 4);
        info.update_metrics(NodeMetrics::new(0.5, 0.25, 1024));
        state
            .registry
            .lock()
            .expect("registry mutex poisoned")
            .register(info);
        state.metrics.record_task(true);
        state
    }

    #[test]
    fn metrics_carries_the_counters_and_the_gauges() {
        let body = metrics_body(&state_with_a_busy_node());

        assert!(body.contains("aethermesh_tasks_completed_total 1"));
        assert!(body.contains("aethermesh_nodes 1"));
        assert!(body.contains("# TYPE aethermesh_nodes gauge"));
        assert!(body.contains("aethermesh_cpu_usage_mean 0.5"));
        assert!(body.ends_with('\n'), "exposition format is line-oriented");
    }

    #[test]
    fn traffic_reaches_the_scrape_endpoint() {
        let state = state_with_a_busy_node();
        state.traffic.record_sent(300, 1000);
        state.traffic.record_transfer_skipped();

        let body = metrics_body(&state);

        // These are the numbers the project exists to improve; an operator
        // watching a dashboard should not have to open a client connection.
        assert!(body.contains("aethermesh_data_bytes_sent_total 300"));
        assert!(body.contains("aethermesh_data_bytes_uncompressed_total 1000"));
        assert!(body.contains("aethermesh_transfers_skipped_total 1"));
        assert!(body.contains("# TYPE aethermesh_task_retries_total counter"));
    }

    #[test]
    fn an_empty_mesh_reports_zero_rather_than_nan() {
        let body = metrics_body(&MeshState::new());

        assert!(body.contains("aethermesh_nodes 0"));
        assert!(body.contains("aethermesh_cpu_usage_mean 0"));
        assert!(!body.contains("NaN"), "a mean over no nodes is not a NaN");
    }

    #[test]
    fn no_node_is_individually_identifiable() {
        let state = state_with_a_busy_node();
        let body = metrics_body(&state);
        let node = state.nodes()[0].clone();

        // The endpoint has no authentication, so it must not be an inventory.
        assert!(!body.contains(&node.hostname));
        assert!(!body.contains(&node.id.to_string()));
        assert!(!body.contains(&node.address));
    }

    #[test]
    fn routes_are_matched_and_query_strings_ignored() {
        let state = MeshState::new();

        assert!(route("GET /metrics HTTP/1.1", &state).starts_with("HTTP/1.1 200 OK"));
        assert!(route("GET /metrics?x=1 HTTP/1.1", &state).contains("aethermesh_nodes 0"));
        assert!(route("GET /healthz HTTP/1.1", &state).contains("ok"));
        assert!(route("GET /nodes HTTP/1.1", &state).starts_with("HTTP/1.1 404"));
        assert!(route("POST /metrics HTTP/1.1", &state).starts_with("HTTP/1.1 405"));
        assert!(route("", &state).starts_with("HTTP/1.1 405"));
    }

    #[test]
    fn the_content_length_matches_the_body() {
        let text = route("GET /metrics HTTP/1.1", &MeshState::new());
        let (head, body) = text.split_once("\r\n\r\n").expect("headers end");
        let declared: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .expect("content-length is set")
            .parse()
            .expect("content-length is a number");

        assert_eq!(declared, body.len());
    }

    #[tokio::test]
    async fn a_request_over_a_socket_gets_a_full_response() {
        let (client, server) = tokio::io::duplex(8 * 1024);
        let state = state_with_a_busy_node();
        tokio::spawn(async move {
            let _ = handle(server, state).await;
        });

        let (mut reader, mut writer) = tokio::io::split(client);
        writer
            .write_all(b"GET /metrics HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .expect("request written");

        let mut response = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut response)
            .await
            .expect("response read");

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("aethermesh_tasks_completed_total 1"));
    }
}
