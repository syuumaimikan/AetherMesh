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
    /// Agent -> controller: an extra connection for bulk data.
    ///
    /// Chunks are self-describing, so they can travel over several connections
    /// at once; this is how an agent offers more than one.
    RegisterDataChannel {
        protocol_version: u16,
        node_id: NodeId,
        token: Option<String>,
    },
    /// Controller -> agent: registration accepted.
    ///
    /// `channel_token` is what the agent presents when it opens extra data
    /// connections. It proves those connections belong to this node, which the
    /// mesh token — shared by every agent — cannot.
    ///
    /// `heartbeat_timeout_secs` is how long the controller waits before
    /// evicting a silent node. The agent needs it to know how far it may slow
    /// its heartbeats down while idle; guessing would either waste power or
    /// get the node evicted. `0` means the agent should not slow down at all.
    RegisterAccepted {
        node_id: NodeId,
        #[serde(default)]
        channel_token: Option<String>,
        #[serde(default)]
        heartbeat_timeout_secs: u64,
    },
    /// Controller -> agent: registration refused; the connection then closes.
    RegisterRejected { reason: String },
    /// Agent -> controller: these datasets are no longer held here.
    ///
    /// An agent with a storage budget drops the least recently used data to
    /// stay inside it. Without this message the controller's catalog would keep
    /// claiming the node has them, and would keep scoring it as the cheapest
    /// place to run work whose inputs it has actually thrown away.
    DataEvicted {
        node_id: NodeId,
        data_ids: Vec<aether_core::DataId>,
    },
    /// Agent -> controller: these datasets are here, in case you forgot.
    ///
    /// Sent after registering, when the node is already holding data. The
    /// case this exists for is a controller that restarted: its catalog is in
    /// memory, so it comes back knowing nothing about where anything is, while
    /// the agents are still sitting on all of it. Without this the mesh spends
    /// the next hour re-sending data to the machines that already have it.
    ///
    /// Only ever additive. A node saying it holds something it does not costs
    /// a failed task; the controller re-sends on demand.
    DataHeld {
        node_id: NodeId,
        datasets: Vec<aether_core::DataDescriptor>,
    },
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
    /// Agent -> controller: a dataset is complete and verified locally.
    ///
    /// With chunks arriving over several connections there is no ordering to
    /// rely on, so the controller waits for this before dispatching the task
    /// that reads the data.
    DataReady { node_id: NodeId, data_id: DataId },
    /// Controller -> agent: measure the link.
    ///
    /// `padding` is ballast: timing a small ping against a large one is what
    /// turns two round trips into a bandwidth estimate.
    Ping { nonce: u64, padding: Vec<u8> },
    /// Agent -> controller: ping received.
    Pong { nonce: u64 },
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

    /// Offers an extra connection for bulk data on behalf of `node_id`.
    pub fn register_data_channel(node_id: NodeId, token: Option<String>) -> Self {
        Self::RegisterDataChannel {
            protocol_version: PROTOCOL_VERSION,
            node_id,
            token,
        }
    }

    /// Short name of the variant, for logs and metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::RegisterNode { .. } => "register_node",
            Self::RegisterAccepted { .. } => "register_accepted",
            Self::RegisterRejected { .. } => "register_rejected",
            Self::DataEvicted { .. } => "data_evicted",
            Self::DataHeld { .. } => "data_held",
            Self::Heartbeat { .. } => "heartbeat",
            Self::SubmitTask { .. } => "submit_task",
            Self::DataTransfer { .. } => "data_transfer",
            Self::DataManifest { .. } => "data_manifest",
            Self::DataChunk { .. } => "data_chunk",
            Self::TaskAssignment { .. } => "task_assignment",
            Self::TaskCompleted { .. } => "task_completed",
            Self::Ping { .. } => "ping",
            Self::Pong { .. } => "pong",
            Self::RegisterDataChannel { .. } => "register_data_channel",
            Self::DataReady { .. } => "data_ready",
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
