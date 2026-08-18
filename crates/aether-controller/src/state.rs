//! State the server, the scheduler, and dispatch all share.

use std::sync::{Arc, Mutex};

use aether_scheduler::DataCatalog;

use crate::connections::Connections;
use crate::observability::{MeshMetrics, TrafficStats};
use crate::registry::NodeRegistry;

/// Registry shared between the accept loop and every connection task.
pub type SharedRegistry = Arc<Mutex<NodeRegistry>>;

pub fn shared_registry() -> SharedRegistry {
    Arc::new(Mutex::new(NodeRegistry::new()))
}

/// Everything the control plane keeps about the live mesh. Cheap to clone.
#[derive(Clone, Default)]
pub struct MeshState {
    pub registry: SharedRegistry,
    pub connections: Connections,
    pub catalog: DataCatalog,
    pub metrics: MeshMetrics,
    /// Bytes moved and bytes saved. Hand this to the `Controller` with
    /// `with_traffic_stats` and the client API reports what it is doing.
    pub traffic: TrafficStats,
    /// How long a node may stay silent before it is evicted.
    ///
    /// The eviction monitor enforces it; registration reports it, so an idle
    /// agent knows how far it may slow its heartbeats down without guessing.
    /// `None` means the agent is told nothing and should not slow down at all.
    heartbeat_timeout: Option<std::time::Duration>,
}

impl MeshState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares the eviction window agents are held to.
    pub fn with_heartbeat_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }

    /// The eviction window, as reported to agents at registration.
    pub fn heartbeat_timeout(&self) -> Option<std::time::Duration> {
        self.heartbeat_timeout
    }

    /// Snapshot of the registered nodes, for the scheduler.
    pub fn nodes(&self) -> Vec<aether_core::NodeInfo> {
        self.registry
            .lock()
            .expect("registry mutex poisoned")
            .nodes()
    }
}
