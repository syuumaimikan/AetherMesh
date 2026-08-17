//! In-memory registry of the nodes currently known to the controller.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use aether_core::{NodeId, NodeInfo, NodeMetrics};

/// Registry operation failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("unknown node: {0}")]
    UnknownNode(NodeId),
}

/// A registered node and when it was last heard from.
#[derive(Debug, Clone)]
pub struct NodeEntry {
    pub info: NodeInfo,
    pub last_seen: Instant,
}

/// Tracks live nodes. Not thread-safe by itself; the controller owns one and
/// serializes access to it.
#[derive(Debug, Default)]
pub struct NodeRegistry {
    nodes: HashMap<NodeId, NodeEntry>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a node, or replaces it if it registered before (agent restart).
    /// Returns the previous entry, if any.
    pub fn register(&mut self, info: NodeInfo) -> Option<NodeEntry> {
        let entry = NodeEntry {
            info,
            last_seen: Instant::now(),
        };
        self.nodes.insert(entry.info.id, entry)
    }

    /// Removes a node, returning its entry if it was registered.
    pub fn remove(&mut self, node_id: NodeId) -> Option<NodeEntry> {
        self.nodes.remove(&node_id)
    }

    /// Applies a heartbeat: refreshes metrics and the last-seen timestamp.
    pub fn record_heartbeat(
        &mut self,
        node_id: NodeId,
        metrics: NodeMetrics,
    ) -> Result<(), RegistryError> {
        let entry = self
            .nodes
            .get_mut(&node_id)
            .ok_or(RegistryError::UnknownNode(node_id))?;
        entry.info.update_metrics(metrics);
        entry.last_seen = Instant::now();
        Ok(())
    }

    pub fn get(&self, node_id: NodeId) -> Option<&NodeEntry> {
        self.nodes.get(&node_id)
    }

    pub fn contains(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }

    /// Snapshot of every registered node, for the scheduler to pick from.
    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .values()
            .map(|entry| entry.info.clone())
            .collect()
    }

    /// Nodes whose last heartbeat is older than `timeout`.
    pub fn stale_nodes(&self, timeout: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        self.nodes
            .values()
            .filter(|entry| now.duration_since(entry.last_seen) > timeout)
            .map(|entry| entry.info.id)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(hostname: &str) -> NodeInfo {
        NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7000", 4)
    }

    #[test]
    fn register_then_lookup_and_remove() {
        let mut registry = NodeRegistry::new();
        let info = node("rpi4");
        let id = info.id;

        assert!(registry.register(info.clone()).is_none());
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.get(id).unwrap().info, info);
        assert!(registry.contains(id));

        let removed = registry.remove(id).unwrap();
        assert_eq!(removed.info, info);
        assert!(registry.is_empty());
        assert!(registry.remove(id).is_none());
    }

    #[test]
    fn re_registration_replaces_the_previous_entry() {
        let mut registry = NodeRegistry::new();
        let mut info = node("desktop");
        registry.register(info.clone());

        info.address = "10.0.0.5:7000".to_string();
        let previous = registry.register(info.clone()).unwrap();

        assert_eq!(previous.info.address, "127.0.0.1:7000");
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.get(info.id).unwrap().info.address, "10.0.0.5:7000");
    }

    #[test]
    fn heartbeat_updates_metrics() {
        let mut registry = NodeRegistry::new();
        let info = node("vm");
        let id = info.id;
        registry.register(info);

        let metrics = NodeMetrics::new(0.42, 0.5, 2048);
        registry.record_heartbeat(id, metrics).unwrap();
        assert_eq!(registry.get(id).unwrap().info.metrics, metrics);
    }

    #[test]
    fn heartbeat_from_an_unregistered_node_is_rejected() {
        let mut registry = NodeRegistry::new();
        let id = NodeId::generate();
        assert_eq!(
            registry.record_heartbeat(id, NodeMetrics::default()),
            Err(RegistryError::UnknownNode(id))
        );
    }

    #[test]
    fn stale_nodes_respects_the_timeout() {
        let mut registry = NodeRegistry::new();
        let info = node("rpi4");
        let id = info.id;
        registry.register(info);

        assert!(registry.stale_nodes(Duration::from_secs(30)).is_empty());
        assert_eq!(registry.stale_nodes(Duration::ZERO), vec![id]);
    }

    #[test]
    fn nodes_returns_every_registered_node() {
        let mut registry = NodeRegistry::new();
        registry.register(node("a"));
        registry.register(node("b"));

        let mut hostnames: Vec<String> = registry
            .nodes()
            .into_iter()
            .map(|info| info.hostname)
            .collect();
        hostnames.sort();
        assert_eq!(hostnames, vec!["a", "b"]);
    }
}
