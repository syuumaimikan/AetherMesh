//! Agent side of the control connection: register, heartbeat, run assigned tasks.

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
    assembler: ChunkAssembler,
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

        match read_message(&mut reader).await? {
            Message::RegisterAccepted { node_id: accepted } if accepted == node_id => {}
            Message::RegisterAccepted { node_id: accepted } => {
                return Err(ClientError::NodeIdMismatch(accepted));
            }
            Message::RegisterRejected { reason } => return Err(ClientError::Rejected(reason)),
            other => return Err(ClientError::UnexpectedReply(other.kind())),
        }
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
            assembler: ChunkAssembler::new(),
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Data this node currently holds.
    pub fn store(&self) -> &DataStore {
        &self.store
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
        match read_message(&mut self.reader).await? {
            Message::DataTransfer {
                descriptor,
                codec,
                bytes,
                ..
            } => {
                let wire_size = bytes.len();
                match decompress(codec, &bytes) {
                    Ok(bytes) => match self.store.insert(descriptor, bytes) {
                        Ok(true) => {
                            debug!(data_id = %descriptor.id, wire_size, ?codec, "data stored")
                        }
                        Ok(false) => debug!(data_id = %descriptor.id, "data already held"),
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
                match self.assembler.begin_with(manifest, &self.store) {
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
            Message::TaskAssignment { node_id, task } => {
                debug!(%task.kind, task_id = %task.id, "task received");
                let store = self.store.clone();
                let result =
                    tokio::task::spawn_blocking(move || executor::execute(node_id, &task, &store))
                        .await
                        .map_err(|error| {
                            std::io::Error::other(format!("task thread failed: {error}"))
                        })?;

                self.outbound
                    .send(Message::TaskCompleted { result })
                    .map_err(|_| ClientError::Disconnected)
            }
            other => {
                warn!(kind = other.kind(), "unexpected message from controller");
                Ok(())
            }
        }
    }

    /// Stores a dataset once its last chunk has been collected.
    fn finish_data(&self, data_id: aether_core::DataId, assembled: Option<Vec<u8>>) {
        if let Some(bytes) = assembled {
            let size = bytes.len();
            self.store.put(bytes);
            debug!(%data_id, size, "chunked data assembled");
        }
    }

    /// Runs until the connection drops: heartbeats on `interval`, tasks as they arrive.
    ///
    /// Heartbeats run in their own task rather than in a `select!` arm: reading a
    /// frame is not cancel-safe, and dropping a half-read frame desyncs the stream.
    pub async fn run(
        &mut self,
        mut collector: MetricsCollector,
        interval: Duration,
    ) -> Result<(), ClientError> {
        let node_id = self.node_id;
        let sender = self.outbound.clone();
        let interval = interval.max(crate::MIN_SAMPLE_INTERVAL);

        let _heartbeats = AbortOnDrop(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let metrics = collector.sample();
                if sender
                    .send(Message::Heartbeat { node_id, metrics })
                    .is_err()
                {
                    break;
                }
                debug!(%node_id, cpu = metrics.cpu_usage, "heartbeat sent");
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
