//! Messages exchanged between clients, the controller, and agents.

use serde::{Deserialize, Serialize};

use aether_core::{
    ChunkManifest, Codec, DataDescriptor, DataId, NodeId, NodeInfo, NodeMetrics, Task, TaskResult,
};

/// Wire format version, checked at registration time.
pub const PROTOCOL_VERSION: u16 = 1;

/// A single protocol message.
///
/// The transport is not decided yet (Phase 8); this enum stays independent of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// Agent -> controller: join the mesh.
    ///
    /// `token` is the shared secret when the controller requires one. It is a
    /// bearer credential: only send it over TLS.
    RegisterNode {
        protocol_version: u16,
        info: NodeInfo,
        token: Option<String>,
    },
    /// Controller -> agent: registration accepted.
    RegisterAccepted { node_id: NodeId },
    /// Controller -> agent: registration refused; the connection then closes.
    RegisterRejected { reason: String },
    /// Agent -> controller: still alive, with fresh metrics.
    Heartbeat {
        node_id: NodeId,
        metrics: NodeMetrics,
    },
    /// Client -> controller: run this task somewhere.
    SubmitTask { task: Task },
    /// Controller -> agent: keep these bytes; a task will need them.
    ///
    /// Sent only when the controller believes the node does not have the data
    /// yet, and always before the task that reads it.
    ///
    /// `bytes` is the wire form: decode it with `codec` before checking it
    /// against `descriptor`.
    DataTransfer {
        node_id: NodeId,
        descriptor: DataDescriptor,
        codec: Codec,
        bytes: Vec<u8>,
    },
    /// Controller -> agent: a large dataset follows as chunks.
    DataManifest {
        node_id: NodeId,
        manifest: ChunkManifest,
    },
    /// Controller -> agent: one chunk of an announced dataset.
    ///
    /// Chunks are self-describing, so they may be sent in any order or over
    /// several connections.
    DataChunk {
        node_id: NodeId,
        data_id: DataId,
        index: u32,
        codec: Codec,
        bytes: Vec<u8>,
    },
    /// Controller -> agent: run this task here.
    TaskAssignment { node_id: NodeId, task: Task },
    /// Agent -> controller: the task finished.
    TaskCompleted { result: TaskResult },
}

impl Message {
    /// Builds a registration message stamped with the current version.
    pub fn register(info: NodeInfo) -> Self {
        Self::RegisterNode {
            protocol_version: PROTOCOL_VERSION,
            info,
            token: None,
        }
    }

    /// Same, carrying the shared secret the controller expects.
    pub fn register_with_token(info: NodeInfo, token: Option<String>) -> Self {
        Self::RegisterNode {
            protocol_version: PROTOCOL_VERSION,
            info,
            token,
        }
    }

    /// Short name of the variant, for logs and metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::RegisterNode { .. } => "register_node",
            Self::RegisterAccepted { .. } => "register_accepted",
            Self::RegisterRejected { .. } => "register_rejected",
            Self::Heartbeat { .. } => "heartbeat",
            Self::SubmitTask { .. } => "submit_task",
            Self::DataTransfer { .. } => "data_transfer",
            Self::DataManifest { .. } => "data_manifest",
            Self::DataChunk { .. } => "data_chunk",
            Self::TaskAssignment { .. } => "task_assignment",
            Self::TaskCompleted { .. } => "task_completed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_stamps_the_current_version() {
        let info = NodeInfo::new(NodeId::generate(), "rpi4", "10.0.0.2:7000", 4);
        match Message::register(info) {
            Message::RegisterNode {
                protocol_version, ..
            } => assert_eq!(protocol_version, PROTOCOL_VERSION),
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }
}
