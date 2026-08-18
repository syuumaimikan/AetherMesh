//! Agent side of the control connection: register, heartbeat, run assigned tasks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aether_core::compress::decompress;
use aether_core::{ChunkAssembler, DataStore, NodeId, NodeInfo, NodeMetrics};
use aether_protocol::{Message, NetError, read_message, write_message};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::executor;
use crate::heartbeat::HeartbeatPacer;
use crate::metrics::MetricsCollector;

/// Failure while talking to the controller.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Net(#[from] NetError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("controller replied with {0} instead of accepting the registration")]
    UnexpectedReply(&'static str),
    #[error("controller accepted a different node id: {0}")]
    NodeIdMismatch(NodeId),
    #[error("controller refused the registration: {0}")]
    Rejected(String),
    #[error("connection to the controller is closed")]
    Disconnected,
}

/// A registered connection to the controller.
///
/// Generic over the stream so a plain socket and a TLS session share one code
/// path.
pub struct AgentClient<S = OwnedReadHalf> {
    node_id: NodeId,
    reader: S,
    outbound: mpsc::UnboundedSender<Message>,
    writer_task: JoinHandle<()>,
    store: DataStore,
    /// Shared with the data-channel readers, which assemble the same datasets.
    assembler: Arc<Mutex<ChunkAssembler>>,
    /// Proof that a data connection belongs to this node, issued at registration.
    channel_token: Option<String>,
    /// How long the controller waits before evicting a silent node, as it
    /// reported at registration. Bounds how far heartbeats may slow down.
    heartbeat_timeout: Duration,
    /// Counts messages that represent real work, so the heartbeat task can tell
    /// an idle node from a working one without inspecting what it did.
    activity: Arc<AtomicU64>,
    /// How many tasks this node will run at once.
    ///
    /// Without this the read loop awaited each task before reading the next
    /// message, so a sixteen-core machine ran one task at a time. The limit is
    /// backpressure rather than a queue: at capacity the agent stops reading
    /// assignments, which is how a node says "not yet" to a controller.
    task_permits: Arc<tokio::sync::Semaphore>,
}

impl AgentClient<OwnedReadHalf> {
    /// Connects over plain TCP and registers.
    pub async fn connect(addr: impl ToSocketAddrs, info: NodeInfo) -> Result<Self, ClientError> {
        Self::connect_with_token(addr, info, None).await
    }

    /// Same, presenting a shared secret the controller may require.
    pub async fn connect_with_token(
        addr: impl ToSocketAddrs,
        info: NodeInfo,
        token: Option<String>,
    ) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        Self::register(reader, writer, info, token).await
    }
}

impl<S> AgentClient<S> {
    /// Caps how many bytes of other people's data this node will hold.
    ///
    /// Without a cap the store grows for as long as the agent runs, which is
    /// survivable on a workstation and fatal on a small board. Over the cap,
    /// the least recently used datasets are dropped and the controller is told
    /// so its catalog does not keep claiming they are here.
    pub fn with_storage_budget(mut self, budget_bytes: u64) -> Self {
        self.store = DataStore::with_budget(budget_bytes);
        self
    }

    /// Keeps the data from a previous connection.
    ///
    /// This is what makes reconnecting worth doing rather than merely
    /// possible: the agent process still holds everything it was sent, so a
    /// controller that restarted can be told about it instead of shipping it
    /// all again.
    pub fn with_store(mut self, store: DataStore) -> Self {
        self.store = store;
        self
    }

    /// The store's byte budget, if it has one.
    pub fn storage_budget(&self) -> Option<u64> {
        self.store.budget()
    }

    /// Sets how many tasks this node runs at once.
    ///
    /// Defaults to the number of logical CPUs. One is the old behaviour and
    /// is worth having: a node doing something else with its cores should be
    /// able to say so.
    pub fn with_max_concurrent_tasks(mut self, tasks: usize) -> Self {
        self.task_permits = Arc::new(tokio::sync::Semaphore::new(tasks.max(1)));
        self
    }

    /// How many tasks this node will run at once.
    pub fn max_concurrent_tasks(&self) -> usize {
        self.task_permits.available_permits() + self.running_tasks()
    }

    /// Tasks running on this node right now.
    pub fn running_tasks(&self) -> usize {
        RUNNING.load(Ordering::Relaxed) as usize
    }
}

/// Tasks executing across every client in this process.
///
/// One counter rather than one per client: an agent has a single client, and
/// a test with several is measuring the same machine's cores either way.
static RUNNING: AtomicU64 = AtomicU64::new(0);

/// One task per logical CPU.
///
/// A task is a whole CPU's worth of work by construction — the built-ins are
/// compute and the WASM ones have a fuel budget — so oversubscribing buys
/// nothing but context switches.
fn default_task_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

impl<S> AgentClient<S>
where
    S: AsyncRead + Unpin,
{
    /// Performs the registration handshake on an already-connected stream.
    pub async fn register<W>(
        mut reader: S,
        mut writer: W,
        info: NodeInfo,
        token: Option<String>,
    ) -> Result<Self, ClientError>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let node_id = info.id;
        write_message(&mut writer, &Message::register_with_token(info, token)).await?;

        let (channel_token, heartbeat_timeout) = match read_message(&mut reader).await? {
            Message::RegisterAccepted {
                node_id: accepted,
                channel_token,
                heartbeat_timeout_secs,
            } if accepted == node_id => {
                (channel_token, Duration::from_secs(heartbeat_timeout_secs))
            }
            Message::RegisterAccepted {
                node_id: accepted, ..
            } => {
                return Err(ClientError::NodeIdMismatch(accepted));
            }
            Message::RegisterRejected { reason } => return Err(ClientError::Rejected(reason)),
            other => return Err(ClientError::UnexpectedReply(other.kind())),
        };
        info!(%node_id, "registered with the controller");

        let (outbound, mut outbound_rx) = mpsc::unbounded_channel::<Message>();
        let writer_task = tokio::spawn(async move {
            while let Some(message) = outbound_rx.recv().await {
                if let Err(error) = write_message(&mut writer, &message).await {
                    warn!(%error, kind = message.kind(), "failed to send message to controller");
                    break;
                }
            }
        });

        Ok(Self {
            node_id,
            reader,
            outbound,
            writer_task,
            store: DataStore::new(),
            assembler: Arc::new(Mutex::new(ChunkAssembler::new())),
            channel_token,
            heartbeat_timeout,
            activity: Arc::new(AtomicU64::new(0)),
            task_permits: Arc::new(tokio::sync::Semaphore::new(default_task_concurrency())),
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Data this node currently holds.
    pub fn store(&self) -> &DataStore {
        &self.store
    }

    /// The eviction window the controller declared at registration.
    ///
    /// `Duration::ZERO` means it declared none, and heartbeats stay at the
    /// configured rate rather than backing off into a guess.
    pub fn heartbeat_timeout(&self) -> Duration {
        self.heartbeat_timeout
    }

    /// Tells the controller what this node is already holding.
    ///
    /// Nothing is sent when the store is empty, which is every first
    /// connection — a fresh agent has nothing to declare.
    pub fn announce_holdings(&self) -> Result<(), ClientError> {
        let datasets = self.store.descriptors();
        if datasets.is_empty() {
            return Ok(());
        }
        let bytes: u64 = datasets
            .iter()
            .map(|descriptor| descriptor.size_bytes)
            .sum();
        info!(count = datasets.len(), bytes, "reporting data already held");
        self.outbound
            .send(Message::DataHeld {
                node_id: self.node_id,
                datasets,
            })
            .map_err(|_| ClientError::Disconnected)
    }

    /// Queues one heartbeat.
    pub fn send_heartbeat(&self, metrics: NodeMetrics) -> Result<(), ClientError> {
        self.outbound
            .send(Message::Heartbeat {
                node_id: self.node_id,
                metrics,
            })
            .map_err(|_| ClientError::Disconnected)?;
        debug!(node_id = %self.node_id, cpu = metrics.cpu_usage, "heartbeat sent");
        Ok(())
    }

    /// Reads and handles one message from the controller.
    ///
    /// Task execution runs on a blocking thread so heartbeats keep flowing.
    pub async fn handle_next(&mut self) -> Result<(), ClientError> {
        let message = read_message(&mut self.reader).await?;
        // Anything that arrives means this node is not idle, so the heartbeat
        // pacer stops stretching its interval.
        self.activity.fetch_add(1, Ordering::Relaxed);

        match message {
            Message::DataTransfer {
                descriptor,
                codec,
                bytes,
                ..
            } => {
                let wire_size = bytes.len();
                match decompress(codec, &bytes) {
                    Ok(bytes) => match self.store.insert_evicting(descriptor, bytes) {
                        Ok((true, evicted)) => {
                            debug!(data_id = %descriptor.id, wire_size, ?codec, "data stored");
                            self.report_evictions(evicted);
                        }
                        Ok((false, _)) => debug!(data_id = %descriptor.id, "data already held"),
                        Err(error) => warn!(%error, "rejecting transferred data"),
                    },
                    Err(error) => warn!(%error, "rejecting transferred data"),
                }
                Ok(())
            }
            Message::DataManifest { manifest, .. } => {
                let data_id = manifest.data.id;
                debug!(%data_id, chunks = manifest.len(), "manifest received");
                // Chunks this node already holds count as received, which is
                // why the controller may skip sending them.
                let assembled = self
                    .assembler
                    .lock()
                    .expect("assembler mutex poisoned")
                    .begin_with(manifest, &self.store);
                match assembled {
                    Ok(assembled) => self.finish_data(data_id, assembled),
                    Err(error) => warn!(%error, "rejecting manifest"),
                }
                Ok(())
            }
            Message::DataChunk {
                data_id,
                index,
                codec,
                bytes,
                ..
            } => {
                match decompress(codec, &bytes)
                    .map_err(|error| error.to_string())
                    .and_then(|bytes| {
                        self.assembler
                            .lock()
                            .expect("assembler mutex poisoned")
                            .add_stored(&self.store, data_id, index, bytes)
                            .map_err(|error| error.to_string())
                    }) {
                    Ok(assembled) => {
                        debug!(%data_id, index, "chunk stored");
                        self.finish_data(data_id, assembled);
                    }
                    Err(error) => warn!(%error, "rejecting chunk"),
                }
                Ok(())
            }
            // Ballast is dropped on arrival: the controller is timing the
            // transfer, not asking for anything back.
            Message::Ping { nonce, .. } => self
                .outbound
                .send(Message::Pong { nonce })
                .map_err(|_| ClientError::Disconnected),
            Message::TaskAssignment { node_id, task } => {
                debug!(%task.kind, task_id = %task.id, "task received");

                // Waits only when this node is already running as many tasks
                // as it agreed to. Not reading the next assignment is how a
                // saturated node declines more work; nothing already running
                // is waiting on a message, so this cannot deadlock.
                let permit = self
                    .task_permits
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| ClientError::Disconnected)?;

                let store = self.store.clone();
                let outbound = self.outbound.clone();
                let task_id = task.id;

                // Spawned rather than awaited: awaiting it here is what made a
                // sixteen-core machine run one task at a time.
                tokio::spawn(async move {
                    RUNNING.fetch_add(1, Ordering::Relaxed);
                    let outcome = tokio::task::spawn_blocking(move || {
                        executor::execute(node_id, &task, &store)
                    })
                    .await;
                    RUNNING.fetch_sub(1, Ordering::Relaxed);
                    drop(permit);

                    match outcome {
                        Ok(result) => {
                            let _ = outbound.send(Message::TaskCompleted { result });
                        }
                        // The blocking pool panicked or was shut down. The
                        // controller times the task out and places it again.
                        Err(error) => warn!(%error, %task_id, "task thread failed"),
                    }
                });
                Ok(())
            }
            other => {
                warn!(kind = other.kind(), "unexpected message from controller");
                Ok(())
            }
        }
    }

    /// Stores a dataset once its last chunk has been collected, and tells the
    /// controller it is safe to send the task that reads it.
    fn finish_data(&self, data_id: aether_core::DataId, assembled: Option<Vec<u8>>) {
        if let Some(bytes) = assembled {
            let size = bytes.len();
            let (_, evicted) = self.store.put_evicting(bytes);
            debug!(%data_id, size, "chunked data assembled");

            let _ = self.outbound.send(Message::DataReady {
                node_id: self.node_id,
                data_id,
            });
            self.report_evictions(evicted);
        }
    }

    /// Tells the controller which datasets this node no longer holds.
    ///
    /// Without it the catalog keeps crediting this node with data it dropped,
    /// and the scheduler keeps picking it as the cheapest place to run work
    /// whose inputs are no longer here.
    fn report_evictions(&self, data_ids: Vec<aether_core::DataId>) {
        if data_ids.is_empty() {
            return;
        }
        debug!(
            count = data_ids.len(),
            "evicted data to stay inside the budget"
        );

        let _ = self.outbound.send(Message::DataEvicted {
            node_id: self.node_id,
            data_ids,
        });
    }

    /// Opens `count` extra TCP connections for bulk data.
    ///
    /// Chunks are self-describing, so the controller can spread them across
    /// these; the store and the assembler are shared, and the control
    /// connection is the one that reports a dataset ready.
    pub async fn open_data_channels(
        &self,
        addr: impl ToSocketAddrs + Clone,
        count: usize,
        _token: Option<String>,
    ) -> Result<Vec<JoinHandle<()>>, ClientError> {
        if count > 0 && self.channel_token.is_none() {
            return Err(ClientError::Rejected(
                "controller issued no data channel token".to_string(),
            ));
        }
        let mut handles = Vec::with_capacity(count);

        for _ in 0..count {
            let stream = TcpStream::connect(addr.clone()).await?;
            let (mut reader, mut writer) = stream.into_split();
            // The channel token, not the mesh token: it is what proves these
            // connections belong to this node.
            write_message(
                &mut writer,
                &Message::register_data_channel(self.node_id, self.channel_token.clone()),
            )
            .await?;

            let store = self.store.clone();
            let assembler = Arc::clone(&self.assembler);
            let outbound = self.outbound.clone();
            let node_id = self.node_id;

            handles.push(tokio::spawn(async move {
                // Nothing is sent back on this connection; the writer half is
                // kept alive so the socket stays open.
                let _writer = writer;
                loop {
                    let message = match read_message(&mut reader).await {
                        Ok(message) => message,
                        Err(error) => {
                            debug!(%error, "data channel closed");
                            return;
                        }
                    };

                    let Message::DataChunk {
                        data_id,
                        index,
                        codec,
                        bytes,
                        ..
                    } = message
                    else {
                        warn!("unexpected message on a data channel");
                        continue;
                    };

                    let assembled = decompress(codec, &bytes)
                        .map_err(|error| error.to_string())
                        .and_then(|bytes| {
                            assembler
                                .lock()
                                .expect("assembler mutex poisoned")
                                .add_stored(&store, data_id, index, bytes)
                                .map_err(|error| error.to_string())
                        });

                    match assembled {
                        Ok(Some(bytes)) => {
                            store.put(bytes);
                            let _ = outbound.send(Message::DataReady { node_id, data_id });
                        }
                        Ok(None) => {}
                        Err(error) => warn!(%error, "rejecting chunk on a data channel"),
                    }
                }
            }));
        }

        debug!(node_id = %self.node_id, count, "data channels opened");
        Ok(handles)
    }

    /// Runs until the connection drops: heartbeats on `interval`, tasks as they arrive.
    ///
    /// Heartbeats run in their own task rather than in a `select!` arm: reading a
    /// frame is not cancel-safe, and dropping a half-read frame desyncs the stream.
    ///
    /// `interval` is the rate for a node that is doing something. An idle node
    /// stretches the gap up to half the controller's eviction timeout — see
    /// [`crate::heartbeat`] for why that is the ceiling.
    pub async fn run(
        &mut self,
        mut collector: MetricsCollector,
        interval: Duration,
    ) -> Result<(), ClientError> {
        // Before anything else: if this is a reconnection, the controller may
        // be a new process that knows nothing about the data sitting here.
        self.announce_holdings()?;

        let node_id = self.node_id;
        let sender = self.outbound.clone();
        let activity = self.activity.clone();
        let interval = interval.max(crate::MIN_SAMPLE_INTERVAL);
        let ceiling = HeartbeatPacer::ceiling_for_timeout(interval, self.heartbeat_timeout);

        let _heartbeats = AbortOnDrop(tokio::spawn(async move {
            let mut pacer = HeartbeatPacer::new(interval, ceiling);
            let mut seen = activity.load(Ordering::Relaxed);

            loop {
                tokio::time::sleep(pacer.interval()).await;

                let metrics = collector.sample();
                if sender
                    .send(Message::Heartbeat { node_id, metrics })
                    .is_err()
                {
                    break;
                }

                let now = activity.load(Ordering::Relaxed);
                let did_work = now != seen;
                seen = now;

                let next = pacer.record(metrics, did_work);
                debug!(
                    %node_id,
                    cpu = metrics.cpu_usage,
                    next_secs = next.as_secs_f32(),
                    "heartbeat sent"
                );
            }
        }));

        loop {
            self.handle_next().await?;
        }
    }
}

/// Stops a background task as soon as its owner goes away.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<S> Drop for AgentClient<S> {
    fn drop(&mut self) {
        self.writer_task.abort();
    }
}
