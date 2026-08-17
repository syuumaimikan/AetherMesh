//! State the server, the scheduler, and dispatch all share.

use std::sync::{Arc, Mutex};

use aether_scheduler::DataCatalog;

use crate::connections::Connections;
use crate::observability::MeshMetrics;
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
}

impl MeshState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of the registered nodes, for the scheduler.
    pub fn nodes(&self) -> Vec<aether_core::NodeInfo> {
        self.registry
            .lock()
            .expect("registry mutex poisoned")
            .nodes()
    }
}
