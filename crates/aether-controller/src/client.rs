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

use std::time::Instant;

use aether_core::{DataDescriptor, DataId, NodeInfo, Priority, Task, TaskResult};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::dispatch::{Controller, DispatchError, TaskTransport};
use crate::queue::{Admitted, Queue};
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
        /// How urgently it wants a node once a backlog forms: `"critical"`,
        /// `"high"`, `"normal"`, `"low"`, `"background"`. Omitted is normal.
        #[serde(default)]
        priority: Option<String>,
        /// How long this is willing to wait for a node, in milliseconds.
        /// Omitted uses the controller's default.
        #[serde(default)]
        timeout_ms: Option<u64>,
        #[serde(default)]
        module: Option<String>,
    },
    /// Runs several tasks, each after the ones it depends on.
    ///
    /// `depends_on` holds indices into `steps`. Every dependency's output
    /// becomes an input of the step that waits for it, so a step reads what
    /// the steps before it produced — and, because the mesh knows which node
    /// holds that output, reads it without moving it.
    Workflow { steps: Vec<WorkflowStep> },
    /// Lists the nodes currently in the mesh.
    Nodes,
    /// Reports what the mesh has moved, saved, and run.
    ///
    /// Everything here is a counter or a live count, never a per-task record:
    /// a dashboard polls this every second and should not be paying for a
    /// snapshot of the whole task history to do it.
    Stats,
}

/// One step of a submitted workflow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowStep {
    pub kind: String,
    #[serde(default)]
    pub payload: String,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub module: Option<String>,
    /// Indices of earlier steps this one waits for.
    #[serde(default)]
    pub depends_on: Vec<usize>,
}

/// What one step of a workflow did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepOutcome {
    pub step: usize,
    pub node_id: String,
    pub success: bool,
    pub output: String,
    pub duration_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
    Workflow {
        /// One entry per step that ran, in the order the steps were written.
        steps: Vec<StepOutcome>,
        /// Steps never attempted because something they depend on failed.
        /// Reported rather than silently missing: a workflow that stopped
        /// early and one that finished are different outcomes.
        skipped: Vec<usize>,
        success: bool,
    },
    Stats {
        /// Bytes moved, bytes saved, retries.
        traffic: TrafficSummary,
        /// Registrations, heartbeats, tasks, since the controller started.
        mesh: crate::observability::MetricsSnapshot,
        /// Work waiting for a node, and how long it has been waiting.
        queue: crate::observability::QueueSnapshot,
        /// Nodes registered right now, and how many have a live connection.
        nodes: usize,
        nodes_connected: usize,
        /// Datasets the controller knows the location of, and their total size.
        datasets: usize,
        dataset_bytes: u64,
    },
    Error {
        message: String,
    },
}

/// What the mesh has moved and what it did not have to.
///
/// The derived figures are computed here rather than left to each caller: five
/// SDKs dividing two integers is five chances to disagree about what the ratio
/// means.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TrafficSummary {
    /// Bytes actually written to sockets, after compression.
    pub bytes_sent: u64,
    /// Bytes those transfers represent, before compression.
    pub bytes_uncompressed: u64,
    /// Bytes compression kept off the wire.
    pub bytes_saved_by_compression: u64,
    /// Wire bytes over original bytes. `1.0` means compression gained nothing.
    pub compression_ratio: f64,
    /// Whole datasets not sent because the node already held them.
    pub transfers_skipped: u64,
    /// Individual chunks not sent for the same reason.
    pub chunks_skipped: u64,
    /// Tasks moved to another node after one refused or timed out.
    pub retries: u64,
}

impl From<crate::observability::TrafficSnapshot> for TrafficSummary {
    fn from(snapshot: crate::observability::TrafficSnapshot) -> Self {
        Self {
            bytes_sent: snapshot.data_bytes_sent,
            bytes_uncompressed: snapshot.data_bytes_uncompressed,
            bytes_saved_by_compression: snapshot.compression_saved_bytes(),
            compression_ratio: snapshot.compression_ratio(),
            transfers_skipped: snapshot.transfers_skipped,
            chunks_skipped: snapshot.chunks_skipped,
            retries: snapshot.retries,
        }
    }
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
    /// `host:port` other nodes reach this one on.
    #[serde(default)]
    pub address: String,
    /// Measured round-trip latency, once the prober has measured it.
    #[serde(default)]
    pub latency_ms: Option<f32>,
    /// Measured link speed, once the prober has measured it.
    #[serde(default)]
    pub bandwidth_bytes_per_sec: Option<u64>,
    /// Datasets this node already holds — work reading them costs no transfer.
    #[serde(default)]
    pub datasets_held: usize,
    #[serde(default)]
    pub bytes_held: u64,
    /// Whether the controller has a live connection to it right now.
    ///
    /// A node can be registered and unreachable: the registry keeps it until
    /// the heartbeat times out, deliberately.
    #[serde(default)]
    pub connected: bool,
}

impl From<&NodeInfo> for NodeSummary {
    /// What can be said about a node from the node alone. Locality and
    /// connectedness are properties of the mesh, so they come out empty here;
    /// [`NodeSummary::in_mesh`] fills them.
    fn from(info: &NodeInfo) -> Self {
        Self {
            node_id: info.id.to_string(),
            hostname: info.hostname.clone(),
            cpu_cores: info.cpu_cores,
            cpu_usage: info.metrics.cpu_usage,
            memory_usage: info.metrics.memory_usage,
            labels: info.labels.clone(),
            address: info.address.clone(),
            latency_ms: info.latency_ms,
            bandwidth_bytes_per_sec: info.bandwidth_bytes_per_sec,
            datasets_held: 0,
            bytes_held: 0,
            connected: false,
        }
    }
}

impl NodeSummary {
    /// The full picture: the node, plus what the mesh knows about it.
    pub fn in_mesh(info: &NodeInfo, state: &MeshState) -> Self {
        let (datasets_held, bytes_held) = state.catalog.held_by(info.id);
        Self {
            datasets_held,
            bytes_held,
            connected: state.connections.is_connected(info.id),
            ..Self::from(info)
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
        /// How long this task is willing to wait for a node. `None` uses the
        /// queue's default, which is usually "as long as it takes".
        timeout: Option<std::time::Duration>,
        reply: oneshot::Sender<Result<TaskResult, DispatchError>>,
    },
    Nodes {
        reply: oneshot::Sender<Vec<NodeSummary>>,
    },
    Stats {
        reply: oneshot::Sender<ClientResponse>,
    },
    Workflow {
        workflow: Box<aether_core::Workflow>,
        reply: oneshot::Sender<ClientResponse>,
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

/// Owns the controller and serves client commands.
///
/// One task is dispatched at a time, so anything submitted while one is in
/// flight waits. What waits is not a plain backlog: submissions go through a
/// [`crate::queue::Queue`], which runs the most urgent one next and promotes
/// whatever has been waiting longest. Reads — publish, nodes, stats — are
/// answered immediately and never queue behind work.
///
/// The node list is refreshed from the live mesh before every placement, so a
/// client never gets scheduled onto a node that just left.
pub async fn run_dispatcher<S, T>(
    controller: Controller<S, T>,
    state: MeshState,
    commands: mpsc::Receiver<ClientCommand>,
) where
    S: aether_scheduler::Scheduler + Send + Sync + 'static,
    T: TaskTransport + Send + Sync + 'static,
{
    run_dispatcher_with(controller, state, commands, Queue::new()).await
}

/// Same, with the queue's ageing policy chosen by the caller.
pub async fn run_dispatcher_with<S, T>(
    controller: Controller<S, T>,
    state: MeshState,
    commands: mpsc::Receiver<ClientCommand>,
    queue: Queue<oneshot::Sender<Result<TaskResult, DispatchError>>>,
) where
    S: aether_scheduler::Scheduler + Send + Sync + 'static,
    T: TaskTransport + Send + Sync + 'static,
{
    run_dispatcher_concurrent(controller, state, commands, queue, DEFAULT_IN_FLIGHT).await
}

/// How many tasks may be dispatched at once.
///
/// Not unbounded: every task in flight holds a reply channel and whatever its
/// inputs cost to send, and a client that submits ten thousand at once should
/// queue rather than exhaust the controller.
pub const DEFAULT_IN_FLIGHT: usize = 64;

/// Same, with the number of tasks allowed in flight chosen by the caller.
///
/// One is the old behaviour, and it was a real limit rather than a policy: a
/// mesh of a hundred nodes ran one task at a time because dispatch needed
/// exclusive access to the controller.
pub async fn run_dispatcher_concurrent<S, T>(
    controller: Controller<S, T>,
    state: MeshState,
    mut commands: mpsc::Receiver<ClientCommand>,
    mut queue: Queue<oneshot::Sender<Result<TaskResult, DispatchError>>>,
    in_flight: usize,
) where
    S: aether_scheduler::Scheduler + Send + Sync + 'static,
    T: TaskTransport + Send + Sync + 'static,
{
    let controller = std::sync::Arc::new(controller);
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(in_flight.max(1)));
    let mut running = tokio::task::JoinSet::new();
    let mut closed = false;
    let mut flows = Vec::new();

    loop {
        // Block only when there is nothing to run. With work waiting, take
        // whatever else has arrived and rank it against what is already here —
        // that is the whole point of having a queue rather than a channel.
        if queue.is_empty() && flows.is_empty() {
            // Nothing to place. Wait for a command, but stop waiting if a
            // dispatch finishes — its reply is owed to somebody.
            tokio::select! {
                command = commands.recv() => match command {
                    Some(command) => admit(command, &mut queue, &mut flows, &controller, &state),
                    None if running.is_empty() => return,
                    // The client hung up but work is still out. Finishing it
                    // is the difference between a reply and a dropped channel.
                    None => closed = true,
                },
                _ = running.join_next(), if !running.is_empty() => {}
            }
        }
        while let Ok(command) = commands.try_recv() {
            admit(command, &mut queue, &mut flows, &controller, &state);
        }

        // Workflows run whole. Interleaving one workflow's steps with
        // another's would let a later step start before the step it waits
        // for, which is the one thing a workflow is for.
        for (workflow, reply) in std::mem::take(&mut flows) {
            let response = run_flow(&controller, &state, &workflow).await;
            let _ = reply.send(response);
        }
        if !closed && commands.is_closed() {
            closed = true;
        }

        // Give up on anything past its deadline before choosing what to run:
        // a caller should hear at roughly the moment the promise broke, not
        // whenever the queue happens to reach them.
        let now = Instant::now();
        for entry in queue.expire(now) {
            let waited_ms = entry.waited(now).as_millis() as u64;
            let _ = entry.payload.send(Err(DispatchError::QueueTimeout {
                task_id: entry.task.id,
                waited_ms,
            }));
            state.queue.record_expired();
        }
        state.queue.set_depth(queue.len());

        let Some(entry) = queue.pop(now) else {
            // Everything admitted was a read or a workflow, and the client
            // hung up. Wait for what is still in flight before leaving.
            if closed {
                while running.join_next().await.is_some() {}
                return;
            }
            continue;
        };

        state.queue.record_dequeued(entry.waited(now));
        state.queue.set_depth(queue.len());

        // Refreshed here rather than inside the spawned dispatch: the live
        // mesh is the server's, and a task must not be placed on a node that
        // has already left.
        controller.sync_registry(state.nodes());

        // Waits only when `in_flight` tasks are already out. The queue is what
        // holds a backlog; this bounds what is in the air at once.
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("the semaphore is never closed");
        let controller = controller.clone();
        running.spawn(async move {
            let result = controller.submit(entry.task).await;
            let _ = entry.payload.send(result);
            drop(permit);
        });

        // Reap whatever finished, so the set does not grow without bound.
        while running.try_join_next().is_some() {}
    }
}

/// Answers a read immediately; puts a submission in the queue.
#[allow(clippy::type_complexity)]
fn admit<S, T>(
    command: ClientCommand,
    queue: &mut Queue<oneshot::Sender<Result<TaskResult, DispatchError>>>,
    flows: &mut Vec<(Box<aether_core::Workflow>, oneshot::Sender<ClientResponse>)>,
    controller: &Controller<S, T>,
    state: &MeshState,
) where
    S: aether_scheduler::Scheduler,
    T: TaskTransport + Send + Sync,
{
    match command {
        ClientCommand::Submit {
            task,
            timeout,
            reply,
        } => {
            let task_id = task.id;
            match queue.push_with_timeout(task, reply, Instant::now(), timeout) {
                Admitted::Queued => {}
                // Refused and displaced are the same news to whoever is
                // waiting: this is not going to run, and here is why, now,
                // rather than a channel that never resolves.
                Admitted::Refused(_, reply) => {
                    let _ = reply.send(Err(DispatchError::QueueFull { task_id }));
                    state.queue.record_refused();
                }
                Admitted::Displaced(entry) => {
                    let dropped = entry.task.id;
                    let _ = entry
                        .payload
                        .send(Err(DispatchError::QueueFull { task_id: dropped }));
                    state.queue.record_refused();
                }
            }
            state.queue.set_depth(queue.len());
        }
        ClientCommand::Publish { bytes, reply } => {
            let _ = reply.send(controller.publish(bytes));
        }
        ClientCommand::Nodes { reply } => {
            // Read from the live mesh, not from the controller's copy of it:
            // the copy is only refreshed before a placement, and a read should
            // not have to wait for one to happen.
            let nodes = state
                .nodes()
                .iter()
                .map(|info| NodeSummary::in_mesh(info, state))
                .collect();
            let _ = reply.send(nodes);
        }
        ClientCommand::Stats { reply } => {
            let _ = reply.send(stats(state));
        }
        // Handed on rather than run here: a workflow is awaited step by
        // step, and this function is not async.
        ClientCommand::Workflow { workflow, reply } => flows.push((workflow, reply)),
    }
}

/// Runs a whole workflow and renders the outcome for a client.
async fn run_flow<S, T>(
    controller: &std::sync::Arc<Controller<S, T>>,
    state: &MeshState,
    workflow: &aether_core::Workflow,
) -> ClientResponse
where
    S: aether_scheduler::Scheduler + Send + Sync + 'static,
    T: TaskTransport + Send + Sync + 'static,
{
    controller.sync_registry(state.nodes());

    match crate::flow::run_workflow(controller.clone(), workflow).await {
        Ok(flow) => ClientResponse::Workflow {
            steps: flow
                .results
                .iter()
                .enumerate()
                .map(|(step, result)| StepOutcome {
                    step,
                    node_id: result.node_id.to_string(),
                    success: result.is_success(),
                    output: BASE64.encode(result.output().unwrap_or_default()),
                    duration_ms: result.duration.as_secs_f64() * 1000.0,
                    error: match &result.outcome {
                        aether_core::TaskOutcome::Failure { message } => Some(message.clone()),
                        aether_core::TaskOutcome::Success { .. } => None,
                    },
                })
                .collect(),
            success: flow.is_success(),
            skipped: flow.skipped,
        },
        Err(error) => ClientResponse::Error {
            message: error.to_string(),
        },
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
        request @ ClientRequest::Submit { .. } => submit(request, gateway).await,
        ClientRequest::Nodes => nodes(gateway).await,
        ClientRequest::Stats => stats_of(gateway).await,
        ClientRequest::Workflow { steps } => workflow(steps, gateway).await,
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
    request: &ClientRequest,
    gateway: &ClientGateway,
) -> Result<ClientResponse, String> {
    let ClientRequest::Submit {
        kind,
        payload,
        inputs,
        constraints,
        priority,
        timeout_ms,
        module,
    } = request
    else {
        unreachable!("only a submission reaches here");
    };

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

    let priority: Priority = priority
        .as_deref()
        .unwrap_or_default()
        .parse()
        .map_err(|error: aether_core::PriorityParseError| error.to_string())?;

    let task = match module.as_deref() {
        Some(module) => Task::wasm(parse_data_id(module)?, payload).with_inputs(inputs),
        None => Task::new(kind, payload).with_inputs(inputs),
    }
    .with_constraints(constraints)
    .with_priority(priority);

    let (reply, answer) = oneshot::channel();
    gateway
        .send(ClientCommand::Submit {
            task,
            timeout: timeout_ms.map(std::time::Duration::from_millis),
            reply,
        })
        .await?;
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

/// Builds a workflow from what a client sent and runs it.
///
/// Validation happens here, before a single step is dispatched: a cycle or a
/// dependency on a step that does not exist should cost nothing, and a
/// workflow that fails halfway leaves work half-done on real machines.
async fn workflow(
    steps: &[WorkflowStep],
    gateway: &ClientGateway,
) -> Result<ClientResponse, String> {
    let mut built = Vec::with_capacity(steps.len());
    for step in steps {
        let payload = decode(&step.payload)?;
        let inputs = step
            .inputs
            .iter()
            .map(|id| parse_data_id(id))
            .collect::<Result<Vec<_>, _>>()?;
        let constraints = step
            .constraints
            .iter()
            .map(|text| text.parse::<aether_core::Constraint>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        let priority: Priority = step
            .priority
            .as_deref()
            .unwrap_or_default()
            .parse()
            .map_err(|error: aether_core::PriorityParseError| error.to_string())?;

        let task = match step.module.as_deref() {
            Some(module) => Task::wasm(parse_data_id(module)?, payload).with_inputs(inputs),
            None => Task::new(&step.kind, payload).with_inputs(inputs),
        }
        .with_constraints(constraints)
        .with_priority(priority);

        built.push(aether_core::Step::after(task, step.depends_on.clone()));
    }

    let workflow = aether_core::Workflow::new(built).map_err(|error| error.to_string())?;

    let (reply, answer) = oneshot::channel();
    gateway
        .send(ClientCommand::Workflow {
            workflow: Box::new(workflow),
            reply,
        })
        .await?;
    answer
        .await
        .map_err(|_| "the workflow was dropped".to_string())
}

async fn nodes(gateway: &ClientGateway) -> Result<ClientResponse, String> {
    let (reply, answer) = oneshot::channel();
    gateway.send(ClientCommand::Nodes { reply }).await?;
    let nodes = answer
        .await
        .map_err(|_| "node listing was dropped".to_string())?;

    Ok(ClientResponse::Nodes { nodes })
}

async fn stats_of(gateway: &ClientGateway) -> Result<ClientResponse, String> {
    let (reply, answer) = oneshot::channel();
    gateway.send(ClientCommand::Stats { reply }).await?;
    answer.await.map_err(|_| "stats were dropped".to_string())
}

/// Reads the live mesh into one frame.
fn stats(state: &MeshState) -> ClientResponse {
    let nodes = state.nodes();
    let nodes_connected = nodes
        .iter()
        .filter(|node| state.connections.is_connected(node.id))
        .count();
    let (datasets, dataset_bytes) = state.catalog.totals();

    ClientResponse::Stats {
        traffic: state.traffic.snapshot().into(),
        mesh: state.metrics.snapshot(),
        queue: state.queue.snapshot(),
        nodes: nodes.len(),
        nodes_connected,
        datasets,
        dataset_bytes,
    }
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
    use crate::queue::Rejection;
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
                priority: None,
                timeout_ms: None,
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
                priority: None,
                timeout_ms: None,
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
                priority: None,
                timeout_ms: None,
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
                priority: None,
                timeout_ms: None,
                module: None,
            },
            &gateway,
        )
        .await;

        // Silently dropping it would run the task somewhere it was not allowed.
        assert!(matches!(response, ClientResponse::Error { .. }));
    }

    /// Records the order tasks were dispatched in, then runs them normally.
    ///
    /// The queue's own tests cover the ranking; this covers the wiring — that
    /// what the dispatcher pops is what actually reaches a node.
    struct Recording {
        inner: SimulatedMesh,
        order: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        /// A real dispatch takes time, and time is what lets two of them
        /// overlap. The simulator returns instantly, so without this nothing
        /// is ever concurrent no matter how many are spawned.
        takes: std::time::Duration,
    }

    impl TaskTransport for Recording {
        async fn dispatch(
            &self,
            node_id: aether_core::NodeId,
            task: &Task,
        ) -> Result<TaskResult, DispatchError> {
            self.order
                .lock()
                .expect("order mutex poisoned")
                .push(task.kind.clone());
            if !self.takes.is_zero() {
                tokio::time::sleep(self.takes).await;
            }
            self.inner.dispatch(node_id, task).await
        }

        async fn send_data(
            &self,
            node_id: aether_core::NodeId,
            descriptor: DataDescriptor,
            codec: aether_core::Codec,
            bytes: &[u8],
        ) -> Result<(), DispatchError> {
            self.inner
                .send_data(node_id, descriptor, codec, bytes)
                .await
        }

        async fn send_manifest(
            &self,
            node_id: aether_core::NodeId,
            manifest: &aether_core::ChunkManifest,
        ) -> Result<(), DispatchError> {
            self.inner.send_manifest(node_id, manifest).await
        }

        async fn send_chunk(
            &self,
            node_id: aether_core::NodeId,
            data_id: DataId,
            index: u32,
            codec: aether_core::Codec,
            bytes: &[u8],
        ) -> Result<(), DispatchError> {
            self.inner
                .send_chunk(node_id, data_id, index, codec, bytes)
                .await
        }
    }

    /// Queues every task, then lets the dispatcher run and reports the order.
    ///
    /// All of them are in the channel before the dispatcher starts, so the
    /// first `recv` is followed by a `try_recv` that drains the rest — the
    /// queue ranks all five against each other and the result is not a race.
    async fn dispatch_order(tasks: Vec<Task>) -> Vec<String> {
        let state = MeshState::new();
        register(&state, "worker");
        let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            Recording {
                inner: SimulatedMesh::new(),
                order: order.clone(),
                takes: std::time::Duration::ZERO,
            },
            DataCatalog::new(),
        );

        let (commands_tx, commands_rx) = mpsc::channel(16);
        let mut replies = Vec::new();
        for task in tasks {
            let (reply, answer) = oneshot::channel();
            commands_tx
                .send(ClientCommand::Submit {
                    task,
                    timeout: None,
                    reply,
                })
                .await
                .expect("buffered");
            replies.push(answer);
        }
        drop(commands_tx);

        // One in flight, deliberately. The guarantee is about the order the
        // queue *releases* work, and with several dispatches running at once
        // the order a transport observes them in is a race rather than a
        // promise. Concurrency is tested separately.
        run_dispatcher_concurrent(controller, state, commands_rx, Queue::new(), 1).await;
        for answer in replies {
            answer.await.expect("a reply").expect("a result");
        }

        let dispatched = order.lock().expect("order mutex poisoned");
        dispatched.clone()
    }

    #[tokio::test]
    async fn urgent_work_runs_before_work_that_can_wait() {
        let order = dispatch_order(vec![
            Task::new("background", Vec::new()).with_priority(Priority::Background),
            Task::new("normal", Vec::new()).with_priority(Priority::Normal),
            Task::new("critical", Vec::new()).with_priority(Priority::Critical),
            Task::new("low", Vec::new()).with_priority(Priority::Low),
            Task::new("high", Vec::new()).with_priority(Priority::High),
        ])
        .await;

        assert_eq!(
            order,
            ["critical", "high", "normal", "low", "background"],
            "submitted in the least helpful order, run in the right one"
        );
    }

    #[tokio::test]
    async fn equally_urgent_work_runs_in_the_order_it_arrived() {
        let tasks = (0..5)
            .map(|index| Task::new(format!("task-{index}"), Vec::new()))
            .collect();

        assert_eq!(
            dispatch_order(tasks).await,
            ["task-0", "task-1", "task-2", "task-3", "task-4"]
        );
    }

    /// Runs a dispatcher over `queue` and returns what each submission got.
    ///
    /// Every task is buffered before the dispatcher starts, so the queue sees
    /// them all at once and the outcome is a decision rather than a race.
    async fn outcomes(
        queue: Queue<oneshot::Sender<Result<TaskResult, DispatchError>>>,
        tasks: Vec<Task>,
    ) -> Vec<Result<TaskResult, DispatchError>> {
        let state = MeshState::new();
        register(&state, "worker");
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            DataCatalog::new(),
        );

        let (commands_tx, commands_rx) = mpsc::channel(64);
        let mut replies = Vec::new();
        for task in tasks {
            let (reply, answer) = oneshot::channel();
            commands_tx
                .send(ClientCommand::Submit {
                    task,
                    timeout: None,
                    reply,
                })
                .await
                .expect("buffered");
            replies.push(answer);
        }
        drop(commands_tx);

        run_dispatcher_with(controller, state, commands_rx, queue).await;
        let mut results = Vec::new();
        for answer in replies {
            results.push(answer.await.expect("a reply"));
        }
        results
    }

    #[tokio::test]
    async fn a_full_queue_tells_the_caller_instead_of_leaving_them_waiting() {
        let tasks = (0..5)
            .map(|index| Task::new(format!("task-{index}"), Vec::new()))
            .collect();
        let results = outcomes(Queue::new().with_max_size(2), tasks).await;

        let refused = results
            .iter()
            .filter(|result| matches!(result, Err(DispatchError::QueueFull { .. })))
            .count();

        // Three of the five could not be taken, and all three found out. A
        // reply channel that never resolves is the worst way to say no.
        assert_eq!(refused, 3, "{results:?}");
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn dropping_the_lowest_priority_tells_the_task_that_was_dropped() {
        let tasks = vec![
            Task::new("background", Vec::new()).with_priority(Priority::Background),
            Task::new("critical", Vec::new()).with_priority(Priority::Critical),
        ];
        let results = outcomes(
            Queue::new()
                .with_max_size(1)
                .with_rejection(Rejection::DropLowestPriority),
            tasks,
        )
        .await;

        // The background task was accepted and then displaced; its caller is
        // owed an answer just as much as one that was refused at the door.
        assert!(
            matches!(results[0], Err(DispatchError::QueueFull { .. })),
            "{results:?}"
        );
        assert!(results[1].is_ok(), "{results:?}");
    }

    #[tokio::test]
    async fn a_task_that_waits_past_its_deadline_gives_up() {
        let state = MeshState::new();
        // No node registered, so nothing can ever be dispatched.
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            DataCatalog::new(),
        );

        let (commands_tx, commands_rx) = mpsc::channel(8);
        let (reply, answer) = oneshot::channel();
        commands_tx
            .send(ClientCommand::Submit {
                task: Task::new(kind::ECHO, Vec::new()),
                timeout: Some(std::time::Duration::from_millis(50)),
                reply,
            })
            .await
            .expect("buffered");
        drop(commands_tx);

        tokio::spawn(run_dispatcher_with(
            controller,
            state,
            commands_rx,
            Queue::new(),
        ));

        // Without a node the first pop fails with NoNodeAvailable, so this
        // pins the deadline rather than the dispatch: whichever comes first,
        // the caller is told something.
        let outcome = answer.await.expect("a reply");
        assert!(outcome.is_err(), "{outcome:?}");
    }

    #[tokio::test]
    async fn several_tasks_are_dispatched_at_once() {
        let state = MeshState::new();
        register(&state, "worker");
        let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            Recording {
                inner: SimulatedMesh::new(),
                order: order.clone(),
                takes: std::time::Duration::ZERO,
            },
            DataCatalog::new(),
        );

        let (commands_tx, commands_rx) = mpsc::channel(64);
        let mut replies = Vec::new();
        for index in 0..16 {
            let (reply, answer) = oneshot::channel();
            commands_tx
                .send(ClientCommand::Submit {
                    task: Task::new(format!("task-{index}"), Vec::new()),
                    timeout: None,
                    reply,
                })
                .await
                .expect("buffered");
            replies.push(answer);
        }
        drop(commands_tx);

        run_dispatcher_concurrent(controller, state, commands_rx, Queue::new(), 8).await;

        // Every one of them is answered. Before this change the mesh ran one
        // task at a time however many nodes it had, so this is the whole
        // point: sixteen submissions, none abandoned.
        for answer in replies {
            answer.await.expect("a reply").expect("a result");
        }
        assert_eq!(order.lock().expect("order mutex poisoned").len(), 16);
    }

    #[tokio::test]
    async fn concurrent_work_spreads_across_nodes_instead_of_piling_on_one() {
        let state = MeshState::new();
        for index in 0..4 {
            register(&state, &format!("node-{index}"));
        }
        let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            Recording {
                inner: SimulatedMesh::new(),
                order: order.clone(),
                takes: std::time::Duration::from_millis(20),
            },
            DataCatalog::new(),
        );

        let (commands_tx, commands_rx) = mpsc::channel(64);
        let mut replies = Vec::new();
        for _ in 0..16 {
            let (reply, answer) = oneshot::channel();
            commands_tx
                .send(ClientCommand::Submit {
                    task: Task::new(kind::ECHO, Vec::new()),
                    timeout: None,
                    reply,
                })
                .await
                .expect("buffered");
            replies.push(answer);
        }
        drop(commands_tx);

        run_dispatcher_concurrent(controller, state, commands_rx, Queue::new(), 8).await;

        let mut nodes = Vec::new();
        for answer in replies {
            nodes.push(answer.await.expect("a reply").expect("a result").node_id);
        }
        nodes.sort_unstable();
        nodes.dedup();

        // A node reports its load on a heartbeat, so without counting what has
        // already been sent, every one of these lands on whichever node scored
        // best for the first. Measured on a real four-node mesh: 64 of 64.
        assert!(
            nodes.len() > 1,
            "all {} tasks went to one node",
            order.lock().expect("order mutex poisoned").len()
        );
    }

    #[tokio::test]
    async fn a_client_that_hangs_up_still_gets_answered() {
        let state = MeshState::new();
        register(&state, "worker");
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            DataCatalog::new(),
        );

        let (commands_tx, commands_rx) = mpsc::channel(8);
        let (reply, answer) = oneshot::channel();
        commands_tx
            .send(ClientCommand::Submit {
                task: Task::new(kind::ECHO, b"hi".to_vec()),
                timeout: None,
                reply,
            })
            .await
            .expect("buffered");
        // Hung up with work still out. Returning here would drop the reply
        // channel and leave the caller waiting on a promise nobody kept.
        drop(commands_tx);

        run_dispatcher_concurrent(controller, state, commands_rx, Queue::new(), 4).await;
        assert!(answer.await.expect("a reply").is_ok());
    }

    #[tokio::test]
    async fn a_priority_the_controller_does_not_know_is_refused() {
        let (gateway, state) = gateway();
        register(&state, "worker");

        let response = serve_request(
            &ClientRequest::Submit {
                kind: kind::ECHO.to_string(),
                payload: String::new(),
                inputs: Vec::new(),
                constraints: Vec::new(),
                priority: Some("urgent".to_string()),
                timeout_ms: None,
                module: None,
            },
            &gateway,
        )
        .await;

        // Guessing what "urgent" meant would run the task at some priority the
        // caller did not choose.
        assert!(
            matches!(response, ClientResponse::Error { .. }),
            "{response:?}"
        );
    }

    #[tokio::test]
    async fn a_submission_without_a_priority_is_normal() {
        let (gateway, state) = gateway();
        register(&state, "worker");

        let response = serve_request(
            &ClientRequest::Submit {
                kind: kind::ECHO.to_string(),
                payload: String::new(),
                inputs: Vec::new(),
                constraints: Vec::new(),
                priority: None,
                timeout_ms: None,
                module: None,
            },
            &gateway,
        )
        .await;

        assert!(
            matches!(response, ClientResponse::Result { .. }),
            "{response:?}"
        );
    }

    #[tokio::test]
    async fn stats_report_what_the_mesh_moved_and_did_not_move() {
        let state = MeshState::new();
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            state.catalog.clone(),
        )
        .with_traffic_stats(state.traffic.clone());
        let (gateway, commands) = ClientGateway::new(8);
        tokio::spawn(run_dispatcher(controller, state.clone(), commands));
        register(&state, "worker");

        // Publish, then run two tasks over the same dataset: one transfer, one
        // skip. That difference is the whole product, so it is what stats show.
        let published = serve_request(
            &ClientRequest::Publish {
                data: BASE64.encode(vec![7u8; 4096]),
            },
            &gateway,
        )
        .await;
        let ClientResponse::Published { data_id, .. } = published else {
            panic!("publish failed: {published:?}");
        };

        for _ in 0..2 {
            let response = serve_request(
                &ClientRequest::Submit {
                    kind: kind::ECHO.to_string(),
                    payload: String::new(),
                    inputs: vec![data_id.clone()],
                    constraints: Vec::new(),
                    priority: None,
                    timeout_ms: None,
                    module: None,
                },
                &gateway,
            )
            .await;
            assert!(
                matches!(response, ClientResponse::Result { success: true, .. }),
                "{response:?}"
            );
        }

        match serve_request(&ClientRequest::Stats, &gateway).await {
            ClientResponse::Stats {
                traffic,
                mesh,
                nodes,
                datasets,
                dataset_bytes,
                ..
            } => {
                assert_eq!(traffic.bytes_uncompressed, 4096, "sent once");
                assert_eq!(traffic.transfers_skipped, 1, "and skipped once");
                assert!(traffic.bytes_sent > 0);
                assert_eq!(mesh.tasks_completed, 0, "the simulator reports none");
                assert_eq!(nodes, 1);
                assert_eq!((datasets, dataset_bytes), (1, 4096));
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn traffic_summaries_derive_the_figures_so_sdks_do_not_have_to() {
        let traffic = crate::observability::TrafficStats::new();
        traffic.record_sent(250, 1000);

        let summary = TrafficSummary::from(traffic.snapshot());
        assert_eq!(summary.bytes_saved_by_compression, 750);
        assert_eq!(summary.compression_ratio, 0.25);
    }

    #[tokio::test]
    async fn a_node_summary_carries_what_a_dashboard_needs() {
        let state = MeshState::new();
        let mut info = NodeInfo::new(aether_core::NodeId::generate(), "rpi4", "10.0.0.4:7001", 4)
            .with_label("kind", "arm")
            .with_bandwidth(12_500_000)
            .with_latency_ms(4.5);
        info.update_metrics(aether_core::NodeMetrics::new(0.3, 0.6, 4096));

        let dataset = aether_core::DataDescriptor::new(aether_core::DataId::of(b"set"), 2048);
        state.catalog.record(dataset, info.id);

        let summary = NodeSummary::in_mesh(&info, &state);

        assert_eq!(summary.address, "10.0.0.4:7001");
        assert_eq!(summary.latency_ms, Some(4.5));
        assert_eq!(summary.bandwidth_bytes_per_sec, Some(12_500_000));
        assert_eq!((summary.datasets_held, summary.bytes_held), (1, 2048));
        // Registered is not the same as reachable, and a dashboard has to be
        // able to tell them apart.
        assert!(!summary.connected);
    }

    /// A gateway whose nodes run the agent's real built-in tasks, so `hash`
    /// works and results carry the output id a workflow depends on.
    fn real_gateway() -> (ClientGateway, MeshState) {
        let state = MeshState::new();
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::with_executor(aether_agent::execute),
            DataCatalog::new(),
        );
        let (gateway, commands) = ClientGateway::new(8);
        tokio::spawn(run_dispatcher(controller, state.clone(), commands));
        (gateway, state)
    }

    fn step(kind: &str, payload: &[u8], depends_on: Vec<usize>) -> WorkflowStep {
        WorkflowStep {
            kind: kind.to_string(),
            payload: BASE64.encode(payload),
            inputs: Vec::new(),
            constraints: Vec::new(),
            priority: None,
            module: None,
            depends_on,
        }
    }

    #[tokio::test]
    async fn a_workflow_runs_its_steps_in_dependency_order() {
        let (gateway, state) = real_gateway();
        register(&state, "worker");

        let response = serve_request(
            &ClientRequest::Workflow {
                steps: vec![
                    step(kind::ECHO, b"seed", Vec::new()),
                    step(kind::HASH, b"", vec![0]),
                    step(kind::HASH, b"", vec![1]),
                ],
            },
            &gateway,
        )
        .await;

        match response {
            ClientResponse::Workflow {
                steps,
                skipped,
                success,
            } => {
                assert!(success, "{steps:?}");
                assert_eq!(steps.len(), 3);
                assert!(skipped.is_empty());
                assert_eq!(BASE64.decode(&steps[0].output).unwrap(), b"seed");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_workflow_with_a_cycle_is_refused_before_anything_runs() {
        let (gateway, state) = real_gateway();
        register(&state, "worker");

        let response = serve_request(
            &ClientRequest::Workflow {
                steps: vec![
                    step(kind::ECHO, b"a", vec![1]),
                    step(kind::ECHO, b"b", vec![0]),
                ],
            },
            &gateway,
        )
        .await;

        // Discovering this halfway through would leave work half-done on
        // machines somebody else owns.
        match response {
            ClientResponse::Error { message } => assert!(message.contains("cycle"), "{message}"),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_workflow_naming_a_step_that_does_not_exist_is_refused() {
        let (gateway, state) = real_gateway();
        register(&state, "worker");

        let response = serve_request(
            &ClientRequest::Workflow {
                steps: vec![step(kind::ECHO, b"a", vec![9])],
            },
            &gateway,
        )
        .await;

        assert!(
            matches!(response, ClientResponse::Error { .. }),
            "{response:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_step_is_reported_and_its_dependents_are_listed_as_skipped() {
        let (gateway, state) = real_gateway();
        register(&state, "worker");

        let response = serve_request(
            &ClientRequest::Workflow {
                steps: vec![
                    step("nonsense", b"", Vec::new()),
                    step(kind::HASH, b"", vec![0]),
                ],
            },
            &gateway,
        )
        .await;

        match response {
            ClientResponse::Workflow {
                steps,
                skipped,
                success,
            } => {
                // A workflow that stopped early and one that finished are
                // different outcomes, and a client has to be able to tell.
                assert!(!success);
                assert_eq!(steps.len(), 1);
                assert!(!steps[0].success);
                assert_eq!(skipped, [1]);
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn listing_nodes_shows_the_live_mesh() {
        let (gateway, state) = gateway();
        register(&state, "desktop");
        register(&state, "rpi4");

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
