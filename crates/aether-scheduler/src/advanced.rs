//! Scoring scheduler: weighs compute, transfer, latency, and data locality.
//!
//! ```text
//! score = compute_cost + transfer_cost + latency_penalty - locality_bonus
//! ```
//!
//! Lower is better. Every term is a small dimensionless number so the weights
//! decide what actually matters; all of them are configurable.

use aether_core::{NodeId, NodeInfo, Task};

use crate::locality::DataCatalog;
use crate::{Scheduler, load_order};

/// Assumed link speed when a node does not report one: about 100 Mbps.
pub const DEFAULT_BANDWIDTH_BYTES_PER_SEC: u64 = 12_500_000;

/// Assumed round-trip latency when a node does not report one.
pub const DEFAULT_LATENCY_MS: f32 = 50.0;

/// How much each term counts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreWeights {
    /// Applied to CPU usage (0..=1).
    pub cpu: f32,
    /// Applied to memory usage (0..=1).
    pub memory: f32,
    /// Applied to the estimated transfer time in seconds.
    pub transfer: f32,
    /// Applied to the link latency in seconds.
    pub latency: f32,
    /// Subtracted in proportion to the input bytes the node already holds (0..=1).
    pub locality: f32,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            cpu: 1.0,
            memory: 0.5,
            transfer: 1.0,
            latency: 1.0,
            locality: 1.0,
        }
    }
}

/// The score of one node for one task, broken down.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Score {
    pub compute_cost: f32,
    pub transfer_cost: f32,
    pub latency_penalty: f32,
    pub locality_bonus: f32,
}

impl Score {
    /// Total score. Lower is better.
    pub fn total(&self) -> f32 {
        self.compute_cost + self.transfer_cost + self.latency_penalty - self.locality_bonus
    }
}

/// Places tasks by scoring every node.
#[derive(Debug, Default, Clone)]
pub struct AdvancedScheduler {
    catalog: DataCatalog,
    weights: ScoreWeights,
}

impl AdvancedScheduler {
    pub fn new(catalog: DataCatalog) -> Self {
        Self {
            catalog,
            weights: ScoreWeights::default(),
        }
    }

    pub fn with_weights(mut self, weights: ScoreWeights) -> Self {
        self.weights = weights;
        self
    }

    pub fn weights(&self) -> ScoreWeights {
        self.weights
    }

    pub fn catalog(&self) -> &DataCatalog {
        &self.catalog
    }

    /// Scores one node for one task, term by term.
    pub fn score(&self, node: &NodeInfo, task: &Task) -> Score {
        let compute_cost = node.metrics.cpu_usage * self.weights.cpu
            + node.metrics.memory_usage * self.weights.memory;

        let total_bytes: u64 = task
            .inputs
            .iter()
            .filter_map(|data_id| self.catalog.descriptor(*data_id))
            .map(|descriptor| descriptor.size_bytes)
            .sum();
        let local_bytes = self.catalog.local_bytes(node.id, &task.inputs);
        let missing_bytes = total_bytes.saturating_sub(local_bytes) + task.payload_len() as u64;

        let bandwidth = node
            .bandwidth_bytes_per_sec
            .unwrap_or(DEFAULT_BANDWIDTH_BYTES_PER_SEC)
            .max(1);
        let transfer_cost = (missing_bytes as f32 / bandwidth as f32) * self.weights.transfer;

        let latency_penalty =
            (node.latency_ms.unwrap_or(DEFAULT_LATENCY_MS) / 1000.0) * self.weights.latency;

        // Bonus scales with the share of the inputs already in place, so a node
        // holding half the data gets half the bonus.
        let locality_bonus = if total_bytes == 0 {
            0.0
        } else {
            (local_bytes as f32 / total_bytes as f32) * self.weights.locality
        };

        Score {
            compute_cost,
            transfer_cost,
            latency_penalty,
            locality_bonus,
        }
    }
}

impl Scheduler for AdvancedScheduler {
    fn select_node(&self, nodes: &[NodeInfo], task: &Task) -> Option<NodeId> {
        crate::eligible(nodes, task)
            .min_by(|a, b| {
                self.score(a, task)
                    .total()
                    .total_cmp(&self.score(b, task).total())
                    // Identical scores fall back to load, keeping the choice stable.
                    .then_with(|| load_order(a, b))
            })
            .map(|node| node.id)
    }
}

#[cfg(test)]
mod tests {
    use aether_core::{DataDescriptor, DataId, NodeMetrics};

    use super::*;

    fn node(hostname: &str, cpu: f32) -> NodeInfo {
        let mut info = NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7000", 4);
        info.update_metrics(NodeMetrics::new(cpu, cpu, 1024));
        info
    }

    fn data(seed: u8, size: u64) -> DataDescriptor {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        DataDescriptor::new(DataId::from_bytes(bytes), size)
    }

    fn task_with(inputs: Vec<DataId>) -> Task {
        Task::new("hash", Vec::new()).with_inputs(inputs)
    }

    #[test]
    fn no_nodes_means_no_selection() {
        let scheduler = AdvancedScheduler::new(DataCatalog::new());
        assert!(scheduler.select_node(&[], &task_with(vec![])).is_none());
    }

    #[test]
    fn without_data_the_least_loaded_node_wins() {
        let scheduler = AdvancedScheduler::new(DataCatalog::new());
        let nodes = vec![node("busy", 0.9), node("idle", 0.1)];

        assert_eq!(
            scheduler.select_node(&nodes, &task_with(vec![])),
            Some(nodes[1].id)
        );
    }

    #[test]
    fn a_large_dataset_outweighs_a_busy_cpu() {
        let catalog = DataCatalog::new();
        let nodes = vec![node("idle", 0.1), node("has-data", 0.9)];
        // 100 MB over the default link is several seconds of transfer.
        let input = data(1, 100 * 1024 * 1024);
        catalog.record(input, nodes[1].id);

        let scheduler = AdvancedScheduler::new(catalog);
        assert_eq!(
            scheduler.select_node(&nodes, &task_with(vec![input.id])),
            Some(nodes[1].id)
        );
    }

    #[test]
    fn a_tiny_dataset_does_not_outweigh_a_busy_cpu() {
        let catalog = DataCatalog::new();
        let nodes = vec![node("idle", 0.05), node("has-data", 0.95)];
        let input = data(2, 64);
        catalog.record(input, nodes[1].id);

        let scheduler = AdvancedScheduler::new(catalog);
        assert_eq!(
            scheduler.select_node(&nodes, &task_with(vec![input.id])),
            Some(nodes[0].id)
        );
    }

    #[test]
    fn latency_breaks_a_tie_between_equal_nodes() {
        let scheduler = AdvancedScheduler::new(DataCatalog::new());
        let far = node("far", 0.5).with_latency_ms(200.0);
        let near = node("near", 0.5).with_latency_ms(1.0);
        let nodes = vec![far, near];

        assert_eq!(
            scheduler.select_node(&nodes, &task_with(vec![])),
            Some(nodes[1].id)
        );
    }

    #[test]
    fn a_faster_link_wins_when_the_data_has_to_move() {
        let scheduler = AdvancedScheduler::new(DataCatalog::new());
        let slow = node("slow", 0.5).with_bandwidth(1024 * 1024);
        let fast = node("fast", 0.5).with_bandwidth(1024 * 1024 * 1024);
        let nodes = vec![slow, fast];
        let task = Task::new("echo", vec![0u8; 8 * 1024 * 1024]);

        assert_eq!(scheduler.select_node(&nodes, &task), Some(nodes[1].id));
    }

    #[test]
    fn weights_can_switch_locality_off() {
        let catalog = DataCatalog::new();
        let nodes = vec![node("idle", 0.1), node("has-data", 0.9)];
        let input = data(3, 100 * 1024 * 1024);
        catalog.record(input, nodes[1].id);

        let scheduler = AdvancedScheduler::new(catalog).with_weights(ScoreWeights {
            transfer: 0.0,
            locality: 0.0,
            ..ScoreWeights::default()
        });

        // With transfer and locality ignored, only load is left.
        assert_eq!(
            scheduler.select_node(&nodes, &task_with(vec![input.id])),
            Some(nodes[0].id)
        );
    }

    #[test]
    fn the_score_breaks_down_into_its_terms() {
        let catalog = DataCatalog::new();
        let node = node("only", 0.5)
            .with_bandwidth(1_000_000)
            .with_latency_ms(10.0);
        let input = data(4, 1_000_000);
        catalog.record(input, node.id);

        let scheduler = AdvancedScheduler::new(catalog);
        let score = scheduler.score(&node, &task_with(vec![input.id]));

        assert_eq!(score.compute_cost, 0.5 * 1.0 + 0.5 * 0.5);
        // The data is already there, so nothing has to move.
        assert_eq!(score.transfer_cost, 0.0);
        assert_eq!(score.latency_penalty, 0.01);
        assert_eq!(score.locality_bonus, 1.0);
        assert_eq!(score.total(), 0.75 + 0.01 - 1.0);
    }

    #[test]
    fn a_constraint_wins_over_every_cost_term() {
        let catalog = DataCatalog::new();
        let nodes = vec![
            node("idle", 0.05),
            node("busy-eu", 0.95)
                .with_latency_ms(300.0)
                .with_label("region", "eu-west"),
        ];
        let input = data(7, 100 * 1024 * 1024);
        catalog.record(input, nodes[0].id);

        let scheduler = AdvancedScheduler::new(catalog);
        let task = task_with(vec![input.id])
            .requiring(aether_core::Constraint::equals("region", "eu-west"));

        // Slower, busier, further away, and without the data — but it is the
        // only node the data is allowed to leave for.
        assert_eq!(scheduler.select_node(&nodes, &task), Some(nodes[1].id));
    }

    #[test]
    fn nothing_qualifying_means_nothing_is_scheduled() {
        let scheduler = AdvancedScheduler::new(DataCatalog::new());
        let nodes = vec![node("plain", 0.1)];
        let task = task_with(vec![]).requiring(aether_core::Constraint::exists("gpu"));

        assert!(scheduler.select_node(&nodes, &task).is_none());
    }

    #[test]
    fn partial_locality_earns_a_partial_bonus() {
        let catalog = DataCatalog::new();
        let node = node("half", 0.0);
        let here = data(5, 1000);
        let elsewhere = data(6, 3000);
        catalog.record(here, node.id);
        catalog.record(elsewhere, NodeId::generate());

        let scheduler = AdvancedScheduler::new(catalog);
        let score = scheduler.score(&node, &task_with(vec![here.id, elsewhere.id]));

        assert_eq!(score.locality_bonus, 0.25);
    }
}
