//! [`TaskTransport`] backed by live agent connections.

use std::time::Duration;

use aether_core::{ChunkManifest, Codec, DataDescriptor, DataId, NodeId, Task, TaskResult};
use aether_protocol::Message;
use tracing::debug;

use crate::connections::Connections;
use crate::dispatch::{DispatchError, TaskTransport};

/// How long a node has to answer before the task is given up on.
pub const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(30);

/// Sends assignments over the connection the agent registered on.
#[derive(Clone)]
pub struct NetworkTransport {
    connections: Connections,
    timeout: Duration,
}

impl NetworkTransport {
    pub fn new(connections: Connections) -> Self {
        Self {
            connections,
            timeout: DEFAULT_TASK_TIMEOUT,
        }
    }

    /// Overrides the per-task timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn connections(&self) -> &Connections {
        &self.connections
    }
}

impl TaskTransport for NetworkTransport {
    async fn dispatch(
        &mut self,
        node_id: NodeId,
        task: &Task,
    ) -> Result<TaskResult, DispatchError> {
        let task_id = task.id;
        let receiver = self.connections.expect_result(task_id);

        let assignment = Message::TaskAssignment {
            node_id,
            task: task.clone(),
        };
        if let Err(error) = self.connections.send(node_id, assignment) {
            self.connections.forget(task_id);
            return Err(error);
        }
        debug!(%node_id, %task_id, kind = %task.kind, "task assigned");

        match tokio::time::timeout(self.timeout, receiver).await {
            Ok(Ok(result)) => Ok(result),
            // The connection task dropped the waiter, i.e. the agent went away.
            Ok(Err(_)) => Err(DispatchError::Unreachable {
                node_id,
                reason: "connection closed before the result arrived".to_string(),
            }),
            Err(_) => {
                self.connections.forget(task_id);
                Err(DispatchError::Timeout { node_id, task_id })
            }
        }
    }

    /// Queues the data on the same connection the task will travel on, so it is
    /// always processed by the agent before the task that reads it.
    async fn send_data(
        &mut self,
        node_id: NodeId,
        descriptor: DataDescriptor,
        codec: Codec,
        bytes: &[u8],
    ) -> Result<(), DispatchError> {
        debug!(%node_id, data_id = %descriptor.id, wire_size = bytes.len(), ?codec, "sending data");
        self.connections.send(
            node_id,
            Message::DataTransfer {
                node_id,
                descriptor,
                codec,
                bytes: bytes.to_vec(),
            },
        )
    }

    async fn send_manifest(
        &mut self,
        node_id: NodeId,
        manifest: &ChunkManifest,
    ) -> Result<(), DispatchError> {
        debug!(%node_id, data_id = %manifest.data.id, chunks = manifest.len(), "sending manifest");
        self.connections.send(
            node_id,
            Message::DataManifest {
                node_id,
                manifest: manifest.clone(),
            },
        )
    }

    async fn send_chunk(
        &mut self,
        node_id: NodeId,
        data_id: DataId,
        index: u32,
        codec: Codec,
        bytes: &[u8],
    ) -> Result<(), DispatchError> {
        self.connections.send(
            node_id,
            Message::DataChunk {
                node_id,
                data_id,
                index,
                codec,
                bytes: bytes.to_vec(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dispatching_to_an_unconnected_node_fails_fast() {
        let mut transport = NetworkTransport::new(Connections::new());
        let task = Task::new("echo", Vec::new());

        let error = transport
            .dispatch(NodeId::generate(), &task)
            .await
            .unwrap_err();
        assert!(matches!(error, DispatchError::Unreachable { .. }));
    }

    #[tokio::test]
    async fn a_silent_node_times_out() {
        let connections = Connections::new();
        let node_id = NodeId::generate();
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        connections.attach(node_id, sender);

        let mut transport =
            NetworkTransport::new(connections).with_timeout(Duration::from_millis(50));
        let task = Task::new("echo", Vec::new());
        let task_id = task.id;

        let error = transport.dispatch(node_id, &task).await.unwrap_err();
        assert_eq!(error, DispatchError::Timeout { node_id, task_id });
    }

    #[tokio::test]
    async fn a_result_delivered_by_the_connection_resolves_the_dispatch() {
        let connections = Connections::new();
        let node_id = NodeId::generate();
        let (sender, mut outbound) = tokio::sync::mpsc::unbounded_channel();
        connections.attach(node_id, sender);

        let replier = connections.clone();
        tokio::spawn(async move {
            let Some(Message::TaskAssignment { node_id, task }) = outbound.recv().await else {
                return;
            };
            replier.complete(TaskResult::success(
                task.id,
                node_id,
                b"done".to_vec(),
                Duration::from_millis(1),
            ));
        });

        let mut transport = NetworkTransport::new(connections);
        let result = transport
            .dispatch(node_id, &Task::new("echo", Vec::new()))
            .await
            .unwrap();

        assert_eq!(result.output(), Some(&b"done"[..]));
        assert_eq!(result.node_id, node_id);
    }
}
