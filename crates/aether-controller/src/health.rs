//! Drops nodes that stopped sending heartbeats.
//!
//! A closed connection is noticed immediately by the server; this catches the
//! quieter failures — a node that froze, lost power, or fell off the network
//! without the socket ever reporting it.

use std::time::Duration;

use aether_core::NodeId;
use tracing::warn;

use crate::state::MeshState;

/// Nodes silent for longer than this are considered gone.
pub const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the mesh is checked.
pub const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Removes every node whose last heartbeat is older than `timeout`.
///
/// Returns the nodes it evicted.
pub fn evict_stale_nodes(state: &MeshState, timeout: Duration) -> Vec<NodeId> {
    let stale = aether_core::lock(&state.registry).stale_nodes(timeout);

    for node_id in &stale {
        aether_core::lock(&state.registry).remove(*node_id);
        state.connections.detach(*node_id);
        // Its store went with it, so the data it held is no longer reachable.
        state.catalog.forget_node(*node_id);
        warn!(%node_id, ?timeout, "evicting node after heartbeat timeout");
    }
    state.metrics.record_evictions(stale.len() as u64);

    stale
}

/// Checks the mesh every `interval` until the task is dropped.
pub async fn monitor(state: MeshState, timeout: Duration, interval: Duration) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(10)));
    loop {
        ticker.tick().await;
        evict_stale_nodes(&state, timeout);
    }
}

#[cfg(test)]
mod tests {
    use aether_core::NodeInfo;

    use super::*;

    fn register(state: &MeshState, hostname: &str) -> NodeId {
        let info = NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7001", 4);
        let node_id = info.id;
        state.registry.lock().unwrap().register(info);
        node_id
    }

    #[test]
    fn a_silent_node_is_evicted() {
        let state = MeshState::new();
        let node_id = register(&state, "gone");
        let descriptor = aether_core::DataDescriptor::of(b"dataset");
        state.catalog.record(descriptor, node_id);

        let evicted = evict_stale_nodes(&state, Duration::ZERO);

        assert_eq!(evicted, vec![node_id]);
        assert!(state.registry.lock().unwrap().is_empty());
        assert!(!state.catalog.holds(descriptor.id, node_id));
    }

    #[test]
    fn a_fresh_node_is_left_alone() {
        let state = MeshState::new();
        let node_id = register(&state, "healthy");

        assert!(evict_stale_nodes(&state, Duration::from_secs(30)).is_empty());
        assert!(state.registry.lock().unwrap().contains(node_id));
    }

    #[tokio::test]
    async fn the_monitor_keeps_checking() {
        let state = MeshState::new();
        register(&state, "gone");

        let monitoring = tokio::spawn(monitor(
            state.clone(),
            Duration::ZERO,
            Duration::from_millis(10),
        ));

        for _ in 0..100 {
            if state.registry.lock().unwrap().is_empty() {
                monitoring.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        monitoring.abort();
        panic!("stale node was never evicted");
    }
}
