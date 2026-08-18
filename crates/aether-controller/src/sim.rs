//! In-process stand-in for the network, used for tests and benchmarks.
//!
//! Messages still go through the real protocol encoding, so the byte counts are
//! representative and swapping in a socket changes only the transport.

use aether_core::compress::decompress;
use aether_core::{
    ChunkAssembler, ChunkManifest, Codec, DataDescriptor, DataId, DataStore, NodeId, Task,
    TaskResult,
};
use aether_protocol::{Message, decode, encode};

use crate::dispatch::{DispatchError, TaskTransport};

/// Runs a task in place of a remote agent.
pub type ExecuteFn = fn(NodeId, &Task, &DataStore) -> TaskResult;

/// Simulated mesh: encodes the assignment, "runs" it, decodes the reply.
#[derive(Debug)]
pub struct SimulatedMesh {
    /// Shared rather than owned: the transport trait no longer hands out
    /// exclusive access, because a mesh that dispatches one task at a time is
    /// not a mesh.
    bytes_transferred: std::sync::Arc<std::sync::atomic::AtomicU64>,
    execute: ExecuteFn,
    /// Stands in for the data every simulated node holds.
    store: DataStore,
    assembler: std::sync::Arc<std::sync::Mutex<ChunkAssembler>>,
}

impl Default for SimulatedMesh {
    fn default() -> Self {
        Self {
            bytes_transferred: std::sync::Arc::default(),
            execute: run,
            store: DataStore::new(),
            assembler: std::sync::Arc::new(std::sync::Mutex::new(ChunkAssembler::new())),
        }
    }
}

impl SimulatedMesh {
    pub fn new() -> Self {
        Self::default()
    }

    /// Uses a different executor, e.g. the agent's real built-in tasks.
    pub fn with_executor(execute: ExecuteFn) -> Self {
        Self {
            execute,
            ..Self::default()
        }
    }

    /// Bytes that would have crossed the network, in both directions.
    pub fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn assembler(&self) -> std::sync::MutexGuard<'_, ChunkAssembler> {
        aether_core::lock(&self.assembler)
    }

    /// Data the simulated nodes hold.
    pub fn store(&self) -> &DataStore {
        &self.store
    }

    /// Encodes, "sends", and decodes one message, counting the bytes.
    fn transfer(&self, node_id: NodeId, message: &Message) -> Result<Message, DispatchError> {
        let unreachable = |error: aether_protocol::CodecError| DispatchError::Unreachable {
            node_id,
            reason: error.to_string(),
        };

        let bytes = encode(message).map_err(unreachable)?;
        self.bytes_transferred
            .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        decode(&bytes).map_err(unreachable)
    }
}

impl TaskTransport for SimulatedMesh {
    async fn dispatch(&self, node_id: NodeId, task: &Task) -> Result<TaskResult, DispatchError> {
        let assignment = Message::TaskAssignment {
            traceparent: None,
            node_id,
            task: task.clone(),
        };

        let Message::TaskAssignment { node_id, task, .. } = self.transfer(node_id, &assignment)?
        else {
            return Err(DispatchError::Unreachable {
                node_id,
                reason: "unexpected message on the assignment path".to_string(),
            });
        };

        let completed = Message::TaskCompleted {
            result: (self.execute)(node_id, &task, &self.store),
        };

        match self.transfer(node_id, &completed)? {
            Message::TaskCompleted { result } => Ok(result),
            _ => Err(DispatchError::Unreachable {
                node_id,
                reason: "unexpected message on the result path".to_string(),
            }),
        }
    }

    async fn send_data(
        &self,
        node_id: NodeId,
        descriptor: DataDescriptor,
        codec: Codec,
        bytes: &[u8],
    ) -> Result<(), DispatchError> {
        let message = Message::DataTransfer {
            node_id,
            descriptor,
            codec,
            bytes: bytes.to_vec(),
        };

        match self.transfer(node_id, &message)? {
            Message::DataTransfer {
                descriptor,
                codec,
                bytes,
                ..
            } => {
                let bytes =
                    decompress(codec, &bytes).map_err(|error| DispatchError::Unreachable {
                        node_id,
                        reason: error.to_string(),
                    })?;
                self.store
                    .insert(descriptor, bytes)
                    .map(|_| ())
                    .map_err(|error| DispatchError::Unreachable {
                        node_id,
                        reason: error.to_string(),
                    })
            }
            _ => Err(DispatchError::Unreachable {
                node_id,
                reason: "unexpected message on the data path".to_string(),
            }),
        }
    }

    async fn send_manifest(
        &self,
        node_id: NodeId,
        manifest: &ChunkManifest,
    ) -> Result<(), DispatchError> {
        let message = Message::DataManifest {
            node_id,
            manifest: manifest.clone(),
        };

        match self.transfer(node_id, &message)? {
            Message::DataManifest { manifest, .. } => {
                let assembled =
                    self.assembler()
                        .begin_with(manifest, &self.store)
                        .map_err(|error| DispatchError::Unreachable {
                            node_id,
                            reason: error.to_string(),
                        })?;
                if let Some(bytes) = assembled {
                    self.store.put(bytes);
                }
                Ok(())
            }
            _ => Err(DispatchError::Unreachable {
                node_id,
                reason: "unexpected message on the manifest path".to_string(),
            }),
        }
    }

    async fn send_chunk(
        &self,
        node_id: NodeId,
        data_id: DataId,
        index: u32,
        codec: Codec,
        bytes: &[u8],
    ) -> Result<(), DispatchError> {
        let message = Message::DataChunk {
            node_id,
            data_id,
            index,
            codec,
            bytes: bytes.to_vec(),
        };

        let Message::DataChunk {
            data_id,
            index,
            codec,
            bytes,
            ..
        } = self.transfer(node_id, &message)?
        else {
            return Err(DispatchError::Unreachable {
                node_id,
                reason: "unexpected message on the chunk path".to_string(),
            });
        };

        let bytes = decompress(codec, &bytes).map_err(|error| DispatchError::Unreachable {
            node_id,
            reason: error.to_string(),
        })?;

        match self
            .assembler()
            .add_stored(&self.store, data_id, index, bytes)
        {
            Ok(Some(assembled)) => {
                self.store.put(assembled);
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => Err(DispatchError::Unreachable {
                node_id,
                reason: error.to_string(),
            }),
        }
    }
}

/// Stands in for agent-side execution when no real executor is injected.
fn run(node_id: NodeId, task: &Task, _store: &DataStore) -> TaskResult {
    let started = std::time::Instant::now();
    match task.kind.as_str() {
        "echo" => TaskResult::success(task.id, node_id, task.payload.clone(), started.elapsed()),
        other => TaskResult::failure(
            task.id,
            node_id,
            format!("unknown task kind: {other}"),
            started.elapsed(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo_returns_the_payload_and_counts_both_directions() {
        let mesh = SimulatedMesh::new();
        let node_id = NodeId::generate();
        let task = Task::new("echo", b"aethermesh".to_vec());

        let result = mesh.dispatch(node_id, &task).await.unwrap();

        assert_eq!(result.task_id, task.id);
        assert_eq!(result.node_id, node_id);
        assert_eq!(result.output(), Some(&b"aethermesh"[..]));
        assert!(mesh.bytes_transferred() >= 2 * task.payload_len() as u64);
    }

    #[tokio::test]
    async fn byte_counter_accumulates_across_dispatches() {
        let mesh = SimulatedMesh::new();
        let node_id = NodeId::generate();

        mesh.dispatch(node_id, &Task::new("echo", vec![1, 2, 3]))
            .await
            .unwrap();
        let after_first = mesh.bytes_transferred();
        mesh.dispatch(node_id, &Task::new("echo", vec![1, 2, 3]))
            .await
            .unwrap();

        // Not exactly doubled: the measured duration is part of the encoded result.
        assert!(mesh.bytes_transferred() > after_first);
    }

    #[tokio::test]
    async fn transferred_data_lands_in_the_simulated_store() {
        let mesh = SimulatedMesh::new();
        let node_id = NodeId::generate();
        let descriptor = DataDescriptor::of(b"dataset");

        mesh.send_data(node_id, descriptor, Codec::None, b"dataset")
            .await
            .unwrap();

        assert_eq!(
            mesh.store().get(descriptor.id).unwrap().as_ref(),
            b"dataset"
        );
        assert!(mesh.bytes_transferred() >= 7);
    }

    #[tokio::test]
    async fn compressed_data_is_restored_on_arrival() {
        let mesh = SimulatedMesh::new();
        let node_id = NodeId::generate();
        let dataset = vec![0xcd; 32 * 1024];
        let descriptor = DataDescriptor::of(&dataset);
        let payload = aether_core::compress::compress(Codec::Lz4, &dataset);

        assert!(payload.len() < dataset.len());
        mesh.send_data(node_id, descriptor, Codec::Lz4, &payload)
            .await
            .unwrap();

        assert_eq!(
            mesh.store().get(descriptor.id).unwrap().as_ref(),
            dataset.as_slice()
        );
    }
}
