//! Node selection for submitted tasks.

pub mod advanced;
pub mod locality;

pub use advanced::{AdvancedScheduler, Score, ScoreWeights};
pub use locality::DataCatalog;

use aether_core::labels::satisfies_all;
use aether_core::{NodeId, NodeInfo, Task};

/// The nodes a task is *allowed* to run on.
///
/// Every scheduler filters through this before it starts comparing costs: a
/// constraint is a hard requirement, not a preference, so a node that fails one
/// is never the cheapest option — it is not an option.
pub fn eligible<'a>(nodes: &'a [NodeInfo], task: &'a Task) -> impl Iterator<Item = &'a NodeInfo> {
    nodes
        .iter()
        .filter(|node| satisfies_all(&node.labels, &task.constraints))
}

/// Picks the node a task should run on.
pub trait Scheduler {
    /// Returns the chosen node, or `None` when no node can take the task.
    fn select_node(&self, nodes: &[NodeInfo], task: &Task) -> Option<NodeId>;
}

impl<S: Scheduler + ?Sized> Scheduler for Box<S> {
    fn select_node(&self, nodes: &[NodeInfo], task: &Task) -> Option<NodeId> {
        (**self).select_node(nodes, task)
    }
}

/// Sends the task to the least busy node: lowest CPU usage, memory usage as
/// the tie-breaker.
#[derive(Debug, Default, Clone, Copy)]
pub struct LeastLoadedScheduler;

impl LeastLoadedScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Scheduler for LeastLoadedScheduler {
    fn select_node(&self, nodes: &[NodeInfo], task: &Task) -> Option<NodeId> {
        eligible(nodes, task)
            .min_by(|a, b| load_order(a, b))
            .map(|node| node.id)
    }
}

/// Moves the task to the data: prefers the node holding the most input bytes
/// already, and falls back to load when no node has an edge.
///
/// Bandwidth and explicit transfer cost enter the score in Phase 15.
#[derive(Debug, Default, Clone)]
pub struct LocalityScheduler {
    catalog: DataCatalog,
}

impl LocalityScheduler {
    pub fn new(catalog: DataCatalog) -> Self {
        Self { catalog }
    }

    pub fn catalog(&self) -> &DataCatalog {
        &self.catalog
    }
}

impl Scheduler for LocalityScheduler {
    fn select_node(&self, nodes: &[NodeInfo], task: &Task) -> Option<NodeId> {
        eligible(nodes, task)
            .max_by(|a, b| {
                let a_local = self.catalog.local_bytes(a.id, &task.inputs);
                let b_local = self.catalog.local_bytes(b.id, &task.inputs);
                // More local bytes wins; equal locality falls back to load.
                a_local.cmp(&b_local).then_with(|| load_order(b, a))
            })
            .map(|node| node.id)
    }
}

/// Orders nodes from least to most loaded.
pub(crate) fn load_order(a: &NodeInfo, b: &NodeInfo) -> std::cmp::Ordering {
    a.metrics
        .cpu_usage
        .total_cmp(&b.metrics.cpu_usage)
        .then_with(|| a.metrics.memory_usage.total_cmp(&b.metrics.memory_usage))
}

#[cfg(test)]
mod tests {
    use aether_core::{DataDescriptor, DataId, NodeMetrics};

    use super::*;

    fn node(hostname: &str, cpu: f32, memory: f32) -> NodeInfo {
        let mut info = NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7000", 4);
        info.update_metrics(NodeMetrics::new(cpu, memory, 1024));
        info
    }

    fn task() -> Task {
        Task::new("hash", b"data".to_vec())
    }

    fn data(seed: u8, size: u64) -> DataDescriptor {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        DataDescriptor::new(DataId::from_bytes(bytes), size)
    }

    #[test]
    fn no_nodes_means_no_selection() {
        assert!(
            LeastLoadedScheduler::new()
                .select_node(&[], &task())
                .is_none()
        );
        assert!(
            LocalityScheduler::default()
                .select_node(&[], &task())
                .is_none()
        );
    }

    #[test]
    fn lowest_cpu_usage_wins() {
        let nodes = vec![node("busy", 0.9, 0.1), node("idle", 0.1, 0.9)];
        let selected = LeastLoadedScheduler::new().select_node(&nodes, &task());
        assert_eq!(selected, Some(nodes[1].id));
    }

    #[test]
    fn equal_cpu_is_broken_by_memory_usage() {
        let nodes = vec![node("full", 0.5, 0.8), node("free", 0.5, 0.2)];
        let selected = LeastLoadedScheduler::new().select_node(&nodes, &task());
        assert_eq!(selected, Some(nodes[1].id));
    }

    #[test]
    fn a_full_tie_is_resolved_deterministically() {
        let nodes = vec![node("a", 0.5, 0.5), node("b", 0.5, 0.5)];
        let scheduler = LeastLoadedScheduler::new();
        assert_eq!(scheduler.select_node(&nodes, &task()), Some(nodes[0].id));
        assert_eq!(scheduler.select_node(&nodes, &task()), Some(nodes[0].id));
    }

    #[test]
    fn single_node_is_always_selected() {
        let nodes = vec![node("only", 1.0, 1.0)];
        let selected = LeastLoadedScheduler::new().select_node(&nodes, &task());
        assert_eq!(selected, Some(nodes[0].id));
    }

    #[test]
    fn the_node_holding_the_data_wins_even_when_busier() {
        let nodes = vec![node("idle", 0.1, 0.1), node("has-data", 0.9, 0.9)];
        let input = data(1, 1024);
        let catalog = DataCatalog::new();
        catalog.record(input, nodes[1].id);

        let selected = LocalityScheduler::new(catalog)
            .select_node(&nodes, &task().with_inputs(vec![input.id]));
        assert_eq!(selected, Some(nodes[1].id));
    }

    #[test]
    fn more_local_bytes_beats_fewer() {
        let nodes = vec![node("small-share", 0.1, 0.1), node("big-share", 0.1, 0.1)];
        let small = data(2, 100);
        let big = data(3, 10_000);
        let catalog = DataCatalog::new();
        catalog.record(small, nodes[0].id);
        catalog.record(big, nodes[1].id);

        let selected = LocalityScheduler::new(catalog)
            .select_node(&nodes, &task().with_inputs(vec![small.id, big.id]));
        assert_eq!(selected, Some(nodes[1].id));
    }

    #[test]
    fn without_any_local_data_the_least_loaded_node_wins() {
        let nodes = vec![node("busy", 0.9, 0.9), node("idle", 0.2, 0.2)];
        let input = data(4, 512);
        let catalog = DataCatalog::new();
        catalog.record(input, NodeId::generate());

        let selected = LocalityScheduler::new(catalog)
            .select_node(&nodes, &task().with_inputs(vec![input.id]));
        assert_eq!(selected, Some(nodes[1].id));
    }

    #[test]
    fn equal_locality_falls_back_to_load() {
        let nodes = vec![node("busy", 0.8, 0.8), node("idle", 0.3, 0.3)];
        let input = data(5, 2048);
        let catalog = DataCatalog::new();
        catalog.record(input, nodes[0].id);
        catalog.record(input, nodes[1].id);

        let selected = LocalityScheduler::new(catalog)
            .select_node(&nodes, &task().with_inputs(vec![input.id]));
        assert_eq!(selected, Some(nodes[1].id));
    }

    #[test]
    fn a_constraint_rules_out_a_node_the_load_would_have_picked() {
        let nodes = vec![
            node("idle-cpu-box", 0.1, 0.1),
            node("busy-gpu-box", 0.9, 0.9).with_label("gpu", "true"),
        ];
        let task = task().requiring(aether_core::Constraint::equals("gpu", "true"));

        assert_eq!(
            LeastLoadedScheduler::new().select_node(&nodes, &task),
            Some(nodes[1].id)
        );
        assert_eq!(
            LocalityScheduler::default().select_node(&nodes, &task),
            Some(nodes[1].id)
        );
    }

    #[test]
    fn a_task_nothing_qualifies_for_is_not_placed() {
        let nodes = vec![node("plain", 0.1, 0.1)];
        let task = task().requiring(aether_core::Constraint::exists("gpu"));

        assert!(
            LeastLoadedScheduler::new()
                .select_node(&nodes, &task)
                .is_none()
        );
        assert!(
            LocalityScheduler::default()
                .select_node(&nodes, &task)
                .is_none()
        );
    }

    #[test]
    fn a_task_without_inputs_is_scheduled_by_load() {
        let nodes = vec![node("busy", 0.7, 0.7), node("idle", 0.1, 0.1)];
        let catalog = DataCatalog::new();
        catalog.record(data(6, 999), nodes[0].id);

        let selected = LocalityScheduler::new(catalog).select_node(&nodes, &task());
        assert_eq!(selected, Some(nodes[1].id));
    }
}
