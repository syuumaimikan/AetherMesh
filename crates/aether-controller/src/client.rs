//! Client-facing API: publish data, submit tasks, list nodes.
//!
//! Agents speak the compact bincode protocol because they are Rust and the
//! traffic is large. Clients get **length-prefixed JSON** instead: a `u32`
//! big-endian length followed by one JSON object. That is a few lines of code
//! in any language — see `sdk/typescript` for the TypeScript implementation —
//! and it keeps the mesh usable from outside Rust without dragging a bincode
//! port into every SDK.

use std::io::ErrorKind;
use std::net::SocketAddr;

use aether_core::{DataDescriptor, DataId, NodeInfo, Task, TaskResult};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::dispatch::{Controller, DispatchError, TaskTransport};
use crate::security::SecurityConfig;
use crate::state::MeshState;

/// Version of the client protocol, reported in the welcome message.
pub const CLIENT_PROTOCOL_VERSION: u32 = 1;

/// Largest client frame accepted.
pub const MAX_CLIENT_FRAME_BYTES: usize = 256 * 1024 * 1024;

/// What a client asks for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientRequest {
    /// First message on a connection.
    Hello { token: Option<String> },
    /// Stores data on the controller and returns its content address.
    Publish { data: String },
    /// Runs a task and waits for its result.
    Submit {
        kind: String,
        #[serde(default)]
        payload: String,
        #[serde(default)]
        inputs: Vec<String>,
        /// Where this task is allowed to run: `"gpu=true"`, `"region!=us-east"`,
        /// `"nvme"`. Empty means anywhere.
        #[serde(default)]
        constraints: Vec<String>,
        #[serde(default)]
        module: Option<String>,
    },
    /// Lists the nodes currently in the mesh.
    Nodes,
}

/// What the controller answers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientResponse {
    Welcome {
        protocol: u32,
    },
    Published {
        data_id: String,
        size_bytes: u64,
    },
    Result {
        task_id: String,
        node_id: String,
        success: bool,
        output: String,
        duration_ms: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Nodes {
        nodes: Vec<NodeSummary>,
    },
    Error {
        message: String,
    },
}

/// One node, as a client sees it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeSummary {
    pub node_id: String,
    pub hostname: String,
    pub cpu_cores: u32,
    pub cpu_usage: f32,
    pub memory_usage: f32,
    /// What this node claims to be, so a client can see which constraints it
    /// could satisfy before submitting anything.
    #[serde(default)]
    pub labels: aether_core::Labels,
}

impl From<&NodeInfo> for NodeSummary {
    fn from(info: &NodeInfo) -> Self {
        Self {
            node_id: info.id.to_string(),
            hostname: info.hostname.clone(),
            cpu_cores: info.cpu_cores,
            cpu_usage: info.metrics.cpu_usage,
            memory_usage: info.metrics.memory_usage,
            labels: info.labels.clone(),
        }
    }
}

/// Work the dispatcher performs on behalf of a client.
///
/// `Controller::submit` needs `&mut self`, so exactly one task owns the
/// controller and everything else talks to it through this channel.
pub enum ClientCommand {
    Publish {
        bytes: Vec<u8>,
        reply: oneshot::Sender<DataDescriptor>,
    },
    Submit {
        task: Task,
        reply: oneshot::Sender<Result<TaskResult, DispatchError>>,
    },
    Nodes {
        reply: oneshot::Sender<Vec<NodeInfo>>,
    },
}

/// Handle clients use to reach the dispatcher. Cheap to clone.
#[derive(Clone)]
pub struct ClientGateway {
    commands: mpsc::Sender<ClientCommand>,
}

impl ClientGateway {
    /// Creates the gateway and the receiver [`run_dispatcher`] consumes.
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<ClientCommand>) {
        let (commands, receiver) = mpsc::channel(capacity.max(1));
        (Self { commands }, receiver)
    }

    async fn send(&self, command: ClientCommand) -> Result<(), String> {
        self.commands
            .send(command)
            .await
            .map_err(|_| "controller is shutting down".to_string())
    }
}

/// Owns the controller and serves client commands one at a time.
///
/// The node list is refreshed from the live mesh before every placement, so a
/// client never gets scheduled onto a node that just left.
pub async fn run_dispatcher<S, T>(
    mut controller: Controller<S, T>,
    state: MeshState,
    mut commands: mpsc::Receiver<ClientCommand>,
) where
    S: aether_scheduler::Scheduler,
    T: TaskTransport + Send,
{
    while let Some(command) = commands.recv().await {
        controller.sync_registry(state.nodes());

        match command {
            ClientCommand::Publish { bytes, reply } => {
                let descriptor = controller.publish(bytes);
                let _ = reply.send(descriptor);
            }
            ClientCommand::Submit { task, reply } => {
                let result = controller.submit(task).await;
                let _ = reply.send(result);
            }
            ClientCommand::Nodes { reply } => {
                let _ = reply.send(controller.registry().nodes());
            }
        }
    }
}

/// Accepts client connections until the listener fails.
pub async fn serve_clients(
    listener: TcpListener,
    gateway: ClientGateway,
    security: SecurityConfig,
) -> std::io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let gateway = gateway.clone();
        let security = security.clone();
        tokio::spawn(async move {
            match handle_client(stream, gateway, security).await {
                Ok(()) => debug!(%peer, "client disconnected"),
                Err(error) => warn!(%peer, %error, "client connection failed"),
            }
        });
    }
}

/// Serves one client connection until it closes.
pub async fn handle_client<S>(
    stream: S,
    gateway: ClientGateway,
    security: SecurityConfig,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut authorized = !security.requires_auth();

    loop {
        let request: ClientRequest = match read_frame(&mut reader).await {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::InvalidData => {
                write_frame(
                    &mut writer,
                    &ClientResponse::Error {
                        message: error.to_string(),
                    },
                )
                .await?;
                continue;
            }
            Err(error) => return Err(error),
        };

        let response = match (&request, authorized) {
            (ClientRequest::Hello { token }, _) => match security.authorize(token.as_deref()) {
                Ok(()) => {
                    authorized = true;
                    info!("client connected");
                    ClientResponse::Welcome {
                        protocol: CLIENT_PROTOCOL_VERSION,
                    }
                }
                Err(error) => ClientResponse::Error {
                    message: error.to_string(),
                },
            },
            (_, false) => ClientResponse::Error {
                message: "send hello with a valid token first".to_string(),
            },
            (request, true) => serve_request(request, &gateway).await,
        };

        write_frame(&mut writer, &response).await?;
    }
}

/// Runs one authorized request.
async fn serve_request(request: &ClientRequest, gateway: &ClientGateway) -> ClientResponse {
    let outcome = match request {
        ClientRequest::Hello { .. } => unreachable!("handled by the caller"),
        ClientRequest::Publish { data } => publish(data, gateway).await,
        ClientRequest::Submit {
            kind,
            payload,
            inputs,
            constraints,
            module,
        } => {
            submit(
                kind,
                payload,
                inputs,
                constraints,
                module.as_deref(),
                gateway,
            )
            .await
        }
        ClientRequest::Nodes => nodes(gateway).await,
    };

    outcome.unwrap_or_else(|message| ClientResponse::Error { message })
}

async fn publish(data: &str, gateway: &ClientGateway) -> Result<ClientResponse, String> {
    let bytes = decode(data)?;
    let (reply, answer) = oneshot::channel();
    gateway
        .send(ClientCommand::Publish { bytes, reply })
        .await?;
    let descriptor = answer
        .await
        .map_err(|_| "publish was dropped".to_string())?;

    Ok(ClientResponse::Published {
        data_id: descriptor.id.to_string(),
        size_bytes: descriptor.size_bytes,
    })
}

async fn submit(
    kind: &str,
    payload: &str,
    inputs: &[String],
    constraints: &[String],
    module: Option<&str>,
    gateway: &ClientGateway,
) -> Result<ClientResponse, String> {
    let payload = decode(payload)?;
    let inputs = inputs
        .iter()
        .map(|id| parse_data_id(id))
        .collect::<Result<Vec<_>, _>>()?;
    let constraints = constraints
        .iter()
        .map(|text| text.parse::<aether_core::Constraint>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;

    let task = match module {
        Some(module) => Task::wasm(parse_data_id(module)?, payload).with_inputs(inputs),
        None => Task::new(kind, payload).with_inputs(inputs),
    }
    .with_constraints(constraints);

    let (reply, answer) = oneshot::channel();
    gateway.send(ClientCommand::Submit { task, reply }).await?;
    let result = answer
        .await
        .map_err(|_| "submission was dropped".to_string())?;

    Ok(match result {
        Ok(result) => ClientResponse::Result {
            task_id: result.task_id.to_string(),
            node_id: result.node_id.to_string(),
            success: result.is_success(),
            output: BASE64.encode(result.output().unwrap_or_default()),
            duration_ms: result.duration.as_secs_f64() * 1000.0,
            error: match &result.outcome {
                aether_core::TaskOutcome::Failure { message } => Some(message.clone()),
                aether_core::TaskOutcome::Success { .. } => None,
            },
        },
        Err(error) => ClientResponse::Error {
            message: error.to_string(),
        },
    })
}

async fn nodes(gateway: &ClientGateway) -> Result<ClientResponse, String> {
    let (reply, answer) = oneshot::channel();
    gateway.send(ClientCommand::Nodes { reply }).await?;
    let nodes = answer
        .await
        .map_err(|_| "node listing was dropped".to_string())?;

    Ok(ClientResponse::Nodes {
        nodes: nodes.iter().map(NodeSummary::from).collect(),
    })
}

fn decode(data: &str) -> Result<Vec<u8>, String> {
    BASE64
        .decode(data)
        .map_err(|error| format!("payload is not valid base64: {error}"))
}

fn parse_data_id(id: &str) -> Result<DataId, String> {
    id.parse::<DataId>()
        .map_err(|error| format!("{id} is not a data id: {error}"))
}

/// Reads one length-prefixed JSON frame. `Ok(None)` means the client hung up.
async fn read_frame<R, T>(reader: &mut R) -> std::io::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut length = [0u8; 4];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }

    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_CLIENT_FRAME_BYTES {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            format!("frame of {length} bytes exceeds the {MAX_CLIENT_FRAME_BYTES} byte limit"),
        ));
    }

    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload)
        .map(Some)
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error.to_string()))
}

/// Writes one length-prefixed JSON frame.
async fn write_frame<W, T>(writer: &mut W, value: &T) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(value)
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error.to_string()))?;
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&payload).await?;
    writer.flush().await
}

/// Binds a client listener, returning the address actually bound.
pub async fn bind_clients(addr: SocketAddr) -> std::io::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    Ok((listener, local_addr))
}

#[cfg(test)]
mod tests {
    use aether_core::task::kind;
    use aether_scheduler::{DataCatalog, LeastLoadedScheduler};

    use super::*;
    use crate::sim::SimulatedMesh;

    /// Runs a dispatcher over the in-process mesh and returns a gateway to it.
    fn gateway() -> (ClientGateway, MeshState) {
        let state = MeshState::new();
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            DataCatalog::new(),
        );
        let (gateway, commands) = ClientGateway::new(8);
        tokio::spawn(run_dispatcher(controller, state.clone(), commands));
        (gateway, state)
    }

    fn register(state: &MeshState, hostname: &str) {
        let info = NodeInfo::new(aether_core::NodeId::generate(), hostname, "127.0.0.1:1", 4);
        state.registry.lock().unwrap().register(info);
    }

    #[tokio::test]
    async fn publishing_returns_a_content_address() {
        let (gateway, _state) = gateway();

        let response = serve_request(
            &ClientRequest::Publish {
                data: BASE64.encode(b"dataset"),
            },
            &gateway,
        )
        .await;

        let expected = aether_core::DataId::of(b"dataset").to_string();
        assert_eq!(
            response,
            ClientResponse::Published {
                data_id: expected,
                size_bytes: 7,
            }
        );
    }

    #[tokio::test]
    async fn submitting_runs_the_task_on_a_node() {
        let (gateway, state) = gateway();
        register(&state, "worker");

        let response = serve_request(
            &ClientRequest::Submit {
                kind: kind::ECHO.to_string(),
                payload: BASE64.encode(b"hello"),
                inputs: Vec::new(),
                constraints: Vec::new(),
                module: None,
            },
            &gateway,
        )
        .await;

        match response {
            ClientResponse::Result {
                success, output, ..
            } => {
                assert!(success);
                assert_eq!(BASE64.decode(output).unwrap(), b"hello");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn submitting_without_nodes_returns_an_error_response() {
        let (gateway, _state) = gateway();

        let response = serve_request(
            &ClientRequest::Submit {
                kind: kind::ECHO.to_string(),
                payload: String::new(),
                inputs: Vec::new(),
                constraints: Vec::new(),
                module: None,
            },
            &gateway,
        )
        .await;

        assert!(matches!(response, ClientResponse::Error { .. }));
    }

    #[tokio::test]
    async fn a_constraint_no_node_satisfies_leaves_the_task_unplaced() {
        let (gateway, state) = gateway();
        register(&state, "plain-worker");

        let response = serve_request(
            &ClientRequest::Submit {
                kind: kind::ECHO.to_string(),
                payload: String::new(),
                inputs: Vec::new(),
                constraints: vec!["gpu=true".to_string()],
                module: None,
            },
            &gateway,
        )
        .await;

        assert!(matches!(response, ClientResponse::Error { .. }));
    }

    #[tokio::test]
    async fn a_malformed_constraint_is_reported_not_ignored() {
        let (gateway, state) = gateway();
        register(&state, "worker");

        let response = serve_request(
            &ClientRequest::Submit {
                kind: kind::ECHO.to_string(),
                payload: String::new(),
                inputs: Vec::new(),
                constraints: vec!["=nonsense".to_string()],
                module: None,
            },
            &gateway,
        )
        .await;

        // Silently dropping it would run the task somewhere it was not allowed.
        assert!(matches!(response, ClientResponse::Error { .. }));
    }

    #[tokio::test]
    async fn listing_nodes_shows_the_live_mesh() {
        let (gateway, state) = gateway();
        register(&state, "desktop");
        register(&state, "rpi4");

        // The first command syncs the registry, so ask twice.
        serve_request(&ClientRequest::Nodes, &gateway).await;
        let response = serve_request(&ClientRequest::Nodes, &gateway).await;

        match response {
            ClientResponse::Nodes { mut nodes } => {
                nodes.sort_by(|a, b| a.hostname.cmp(&b.hostname));
                assert_eq!(nodes.len(), 2);
                assert_eq!(nodes[0].hostname, "desktop");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_base64_is_reported_not_panicked_on() {
        let (gateway, _state) = gateway();

        let response = serve_request(
            &ClientRequest::Publish {
                data: "not base64!!".to_string(),
            },
            &gateway,
        )
        .await;

        assert!(matches!(response, ClientResponse::Error { .. }));
    }

    #[tokio::test]
    async fn frames_round_trip() {
        let mut buffer = Vec::new();
        let request = ClientRequest::Hello {
            token: Some("s3cret".to_string()),
        };
        write_frame(&mut buffer, &request).await.unwrap();

        let mut reader = buffer.as_slice();
        let decoded: Option<ClientRequest> = read_frame(&mut reader).await.unwrap();
        assert_eq!(decoded, Some(request));

        // A closed stream reads as "no more frames".
        let mut empty: &[u8] = &[];
        let end: Option<ClientRequest> = read_frame(&mut empty).await.unwrap();
        assert_eq!(end, None);
    }

    #[tokio::test]
    async fn a_client_must_say_hello_before_anything_else() {
        let (gateway, _state) = gateway();
        let (client, server) = tokio::io::duplex(4096);

        tokio::spawn(handle_client(
            server,
            gateway,
            SecurityConfig::with_token("s3cret"),
        ));

        let (mut reader, mut writer) = tokio::io::split(client);
        write_frame(&mut writer, &ClientRequest::Nodes)
            .await
            .unwrap();
        let response: Option<ClientResponse> = read_frame(&mut reader).await.unwrap();
        assert!(matches!(response, Some(ClientResponse::Error { .. })));

        write_frame(
            &mut writer,
            &ClientRequest::Hello {
                token: Some("s3cret".to_string()),
            },
        )
        .await
        .unwrap();
        let response: Option<ClientResponse> = read_frame(&mut reader).await.unwrap();
        assert_eq!(
            response,
            Some(ClientResponse::Welcome {
                protocol: CLIENT_PROTOCOL_VERSION
            })
        );
    }

    #[tokio::test]
    async fn a_client_with_the_wrong_token_gets_nowhere() {
        let (gateway, _state) = gateway();
        let (client, server) = tokio::io::duplex(4096);

        tokio::spawn(handle_client(
            server,
            gateway,
            SecurityConfig::with_token("s3cret"),
        ));

        let (mut reader, mut writer) = tokio::io::split(client);
        write_frame(
            &mut writer,
            &ClientRequest::Hello {
                token: Some("guess".to_string()),
            },
        )
        .await
        .unwrap();

        let response: Option<ClientResponse> = read_frame(&mut reader).await.unwrap();
        assert!(matches!(response, Some(ClientResponse::Error { .. })));
    }
}
