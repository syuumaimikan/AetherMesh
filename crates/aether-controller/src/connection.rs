//! The client half of the client protocol.
//!
//! [`crate::client`] is the server side; this is what talks to it. It lives
//! beside the request and response types rather than being restated in every
//! tool that needs it, so a caller that has drifted from the controller is a
//! compile error instead of a wrong number on a screen.
//!
//! Four bytes of big-endian length, then one JSON object, both directions.
//!
//! ```no_run
//! # use std::time::Duration;
//! # use aether_controller::connection::Connection;
//! # async fn example() -> Result<(), aether_controller::connection::ConnectionError> {
//! let mut mesh = Connection::connect("127.0.0.1:7100", None, Duration::from_secs(10)).await?;
//! let published = mesh.publish(b"input".to_vec()).await?;
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use aether_core::{DataId, Priority};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::client::{ClientRequest, ClientResponse, MAX_CLIENT_FRAME_BYTES};

/// Talking to a controller failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("connecting to {addr}: {source}")]
    Connect {
        addr: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{addr} did not answer within {timeout:?}")]
    Timeout { addr: String, timeout: Duration },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("the controller sent invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("the controller announced a {0} byte frame")]
    FrameTooLarge(usize),
    /// The controller answered, and the answer was no.
    #[error("{0}")]
    Refused(String),
    #[error("expected {expected}, got {actual}")]
    Unexpected {
        expected: &'static str,
        actual: &'static str,
    },
}

/// What a submission asks for beyond the task itself.
#[derive(Debug, Clone, Default)]
pub struct SubmitOptions {
    /// Published datasets the task reads.
    pub inputs: Vec<DataId>,
    /// Where it is allowed to run: `gpu=true`, `region!=us-east`, `nvme`.
    pub constraints: Vec<String>,
    /// How urgently it wants a node once a backlog forms.
    pub priority: Priority,
    /// How long it is willing to wait for one.
    pub timeout: Option<Duration>,
    /// A published WebAssembly module to run, instead of a built-in kind.
    pub module: Option<DataId>,
}

impl SubmitOptions {
    pub fn reading(inputs: Vec<DataId>) -> Self {
        Self {
            inputs,
            ..Self::default()
        }
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_constraints(mut self, constraints: Vec<String>) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn running(mut self, module: DataId) -> Self {
        self.module = Some(module);
        self
    }
}

/// A connection to a controller's client API.
///
/// Replies are matched to requests by order, so one connection serves one
/// caller at a time. Open one per worker.
pub struct Connection {
    stream: TcpStream,
    addr: String,
    timeout: Duration,
}

impl Connection {
    /// Connects and completes the handshake.
    pub async fn connect(
        addr: &str,
        token: Option<String>,
        timeout: Duration,
    ) -> Result<Self, ConnectionError> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ConnectionError::Timeout {
                addr: addr.to_string(),
                timeout,
            })?
            .map_err(|source| ConnectionError::Connect {
                addr: addr.to_string(),
                source,
            })?;

        let mut connection = Self {
            stream,
            addr: addr.to_string(),
            timeout,
        };
        match connection.request(&ClientRequest::Hello { token }).await? {
            ClientResponse::Welcome { .. } => Ok(connection),
            other => Err(refusal(other, "welcome")),
        }
    }

    /// The address this is connected to.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Stores data on the controller. Identical bytes yield the same id.
    pub async fn publish(&mut self, data: Vec<u8>) -> Result<Published, ConnectionError> {
        let request = ClientRequest::Publish {
            data: BASE64.encode(data),
        };
        match self.request(&request).await? {
            ClientResponse::Published {
                data_id,
                size_bytes,
            } => Ok(Published {
                data_id: data_id
                    .parse()
                    .map_err(|_| ConnectionError::Refused(format!("bad data id {data_id}")))?,
                size_bytes,
            }),
            other => Err(refusal(other, "published")),
        }
    }

    /// Runs one task and waits for its result.
    ///
    /// A task that ran and failed comes back as `Ok`: only transport and
    /// protocol problems, and refusals by the controller, are errors here.
    pub async fn submit(
        &mut self,
        kind: &str,
        payload: Vec<u8>,
        options: &SubmitOptions,
    ) -> Result<Finished, ConnectionError> {
        let request = ClientRequest::Submit {
            kind: kind.to_string(),
            payload: BASE64.encode(payload),
            inputs: options.inputs.iter().map(DataId::to_string).collect(),
            constraints: options.constraints.clone(),
            priority: Some(options.priority.to_string()),
            timeout_ms: options.timeout.map(|timeout| timeout.as_millis() as u64),
            module: options.module.map(|module| module.to_string()),
        };

        match self.request(&request).await? {
            ClientResponse::Result {
                node_id,
                success,
                output,
                duration_ms,
                error,
                ..
            } => Ok(Finished {
                node_id,
                success,
                output: BASE64
                    .decode(output)
                    .map_err(|error| ConnectionError::Refused(error.to_string()))?,
                duration_ms,
                error,
            }),
            other => Err(refusal(other, "result")),
        }
    }

    /// The nodes currently registered.
    pub async fn nodes(&mut self) -> Result<Vec<crate::client::NodeSummary>, ConnectionError> {
        match self.request(&ClientRequest::Nodes).await? {
            ClientResponse::Nodes { nodes } => Ok(nodes),
            other => Err(refusal(other, "nodes")),
        }
    }

    /// Everything the mesh has moved, saved, run, and queued.
    pub async fn stats(&mut self) -> Result<Stats, ConnectionError> {
        match self.request(&ClientRequest::Stats).await? {
            ClientResponse::Stats {
                traffic,
                mesh,
                queue,
                nodes,
                nodes_connected,
                datasets,
                dataset_bytes,
            } => Ok(Stats {
                traffic,
                mesh,
                queue,
                nodes,
                nodes_connected,
                datasets,
                dataset_bytes,
            }),
            other => Err(refusal(other, "stats")),
        }
    }

    /// Sends one frame and reads the reply.
    pub async fn request(
        &mut self,
        request: &ClientRequest,
    ) -> Result<ClientResponse, ConnectionError> {
        tokio::time::timeout(self.timeout, self.exchange(request))
            .await
            .map_err(|_| ConnectionError::Timeout {
                addr: self.addr.clone(),
                timeout: self.timeout,
            })?
    }

    async fn exchange(
        &mut self,
        request: &ClientRequest,
    ) -> Result<ClientResponse, ConnectionError> {
        let payload = serde_json::to_vec(request)?;
        self.stream
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await?;
        self.stream.write_all(&payload).await?;
        self.stream.flush().await?;

        let mut header = [0u8; 4];
        self.stream.read_exact(&mut header).await?;
        let length = u32::from_be_bytes(header) as usize;
        if length > MAX_CLIENT_FRAME_BYTES {
            return Err(ConnectionError::FrameTooLarge(length));
        }

        let mut body = vec![0u8; length];
        self.stream.read_exact(&mut body).await?;
        Ok(serde_json::from_slice(&body)?)
    }
}

/// A dataset the controller now holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Published {
    pub data_id: DataId,
    pub size_bytes: u64,
}

/// What the mesh has moved, saved, run, and queued.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Stats {
    pub traffic: crate::client::TrafficSummary,
    pub mesh: crate::observability::MetricsSnapshot,
    pub queue: crate::observability::QueueSnapshot,
    pub nodes: usize,
    pub nodes_connected: usize,
    pub datasets: usize,
    pub dataset_bytes: u64,
}

/// What a task produced.
#[derive(Debug, Clone, PartialEq)]
pub struct Finished {
    pub node_id: String,
    pub success: bool,
    pub output: Vec<u8>,
    pub duration_ms: f64,
    pub error: Option<String>,
}

/// Turns an unexpected reply into an error, preferring the controller's own
/// words when it gave any.
fn refusal(response: ClientResponse, expected: &'static str) -> ConnectionError {
    match response {
        ClientResponse::Error { message } => ConnectionError::Refused(message),
        other => ConnectionError::Unexpected {
            expected,
            actual: name_of(&other),
        },
    }
}

fn name_of(response: &ClientResponse) -> &'static str {
    match response {
        ClientResponse::Welcome { .. } => "welcome",
        ClientResponse::Published { .. } => "published",
        ClientResponse::Result { .. } => "result",
        ClientResponse::Nodes { .. } => "nodes",
        ClientResponse::Stats { .. } => "stats",
        ClientResponse::Workflow { .. } => "workflow",
        ClientResponse::Error { .. } => "error",
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aether_core::task::kind;
    use aether_scheduler::{DataCatalog, LeastLoadedScheduler};

    use super::*;
    use crate::client::{ClientGateway, bind_clients, run_dispatcher, serve_clients};
    use crate::dispatch::Controller;
    use crate::security::SecurityConfig;
    use crate::sim::SimulatedMesh;
    use crate::state::MeshState;

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// A controller on a real port, with one node registered.
    async fn controller(token: Option<&str>) -> (String, MeshState) {
        let state = MeshState::new();
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            DataCatalog::new(),
        )
        .with_traffic_stats(state.traffic.clone());

        let (gateway, commands) = ClientGateway::new(8);
        tokio::spawn(run_dispatcher(controller, state.clone(), commands));

        let security = match token {
            Some(token) => SecurityConfig::with_token(token),
            None => SecurityConfig::open(),
        };
        let (listener, addr) = bind_clients("127.0.0.1:0".parse().unwrap()).await.unwrap();
        tokio::spawn(serve_clients(listener, gateway, security));

        let info = aether_core::NodeInfo::new(
            aether_core::NodeId::generate(),
            "worker",
            "127.0.0.1:7001",
            4,
        );
        state.registry.lock().unwrap().register(info);

        (addr.to_string(), state)
    }

    #[tokio::test]
    async fn a_dataset_published_twice_gets_the_same_id() {
        let (addr, _state) = controller(None).await;
        let mut mesh = Connection::connect(&addr, None, TIMEOUT).await.unwrap();

        let first = mesh.publish(vec![7u8; 4096]).await.unwrap();
        let second = mesh.publish(vec![7u8; 4096]).await.unwrap();

        assert_eq!(first, second);
        assert_eq!(first.size_bytes, 4096);
    }

    #[tokio::test]
    async fn a_task_runs_and_says_where() {
        let (addr, _state) = controller(None).await;
        let mut mesh = Connection::connect(&addr, None, TIMEOUT).await.unwrap();

        let finished = mesh
            .submit(kind::ECHO, b"hello".to_vec(), &SubmitOptions::default())
            .await
            .unwrap();

        assert!(finished.success, "{finished:?}");
        assert_eq!(finished.output, b"hello");
        assert!(!finished.node_id.is_empty());
    }

    #[tokio::test]
    async fn a_task_reading_a_dataset_carries_its_id() {
        let (addr, _state) = controller(None).await;
        let mut mesh = Connection::connect(&addr, None, TIMEOUT).await.unwrap();

        let published = mesh.publish(vec![3u8; 8192]).await.unwrap();
        let finished = mesh
            .submit(
                kind::ECHO,
                Vec::new(),
                &SubmitOptions::reading(vec![published.data_id]),
            )
            .await
            .unwrap();

        assert!(finished.success, "{finished:?}");
        // The transfer counters only move if the input actually travelled.
        let stats = mesh.stats().await.unwrap();
        assert_eq!(stats.traffic.bytes_uncompressed, 8192);
        assert_eq!(stats.nodes, 1);
    }

    #[tokio::test]
    async fn a_constraint_no_node_satisfies_is_an_error_not_a_silent_pass() {
        let (addr, _state) = controller(None).await;
        let mut mesh = Connection::connect(&addr, None, TIMEOUT).await.unwrap();

        let outcome = mesh
            .submit(
                kind::ECHO,
                Vec::new(),
                &SubmitOptions::default().with_constraints(vec!["gpu=true".to_string()]),
            )
            .await;

        assert!(
            matches!(outcome, Err(ConnectionError::Refused(_))),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_token_the_controller_wants_is_a_token_it_gets() {
        let (addr, _state) = controller(Some("s3cret")).await;

        assert!(Connection::connect(&addr, None, TIMEOUT).await.is_err());
        assert!(
            Connection::connect(&addr, Some("s3cret".to_string()), TIMEOUT)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_controller_that_is_not_there_is_an_error_not_a_hang() {
        let outcome = Connection::connect("127.0.0.1:1", None, Duration::from_millis(500)).await;
        assert!(matches!(
            outcome,
            Err(ConnectionError::Connect { .. } | ConnectionError::Timeout { .. })
        ));
    }
}
