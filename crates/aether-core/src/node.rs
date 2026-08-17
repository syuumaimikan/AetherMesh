//! Node description and the resource metrics reported by agents.

use serde::{Deserialize, Serialize};

use crate::id::NodeId;

/// Resource usage sampled on a node.
///
/// `cpu_usage` and `memory_usage` are ratios in `0.0..=1.0`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub memory_total_bytes: u64,
}

impl NodeMetrics {
    /// Builds metrics, clamping both ratios into `0.0..=1.0`.
    pub fn new(cpu_usage: f32, memory_usage: f32, memory_total_bytes: u64) -> Self {
        Self {
            cpu_usage: clamp_ratio(cpu_usage),
            memory_usage: clamp_ratio(memory_usage),
            memory_total_bytes,
        }
    }

    /// Bytes currently in use, derived from the ratio and the total.
    pub fn memory_used_bytes(&self) -> u64 {
        (self.memory_total_bytes as f64 * f64::from(self.memory_usage)) as u64
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new(0.0, 0.0, 0)
    }
}

/// Ratios arrive from OS probes, so NaN and out-of-range values are possible.
fn clamp_ratio(value: f32) -> f32 {
    if value.is_nan() {
        0.0
    } else {
        value.clamp(0.0, 1.0)
    }
}

/// Everything the controller knows about a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub hostname: String,
    /// `host:port` the node accepts connections on.
    pub address: String,
    pub cpu_cores: u32,
    /// Measured or configured link speed toward this node. `None` means unknown.
    pub bandwidth_bytes_per_sec: Option<u64>,
    /// Round-trip latency to this node in milliseconds. `None` means unknown.
    pub latency_ms: Option<f32>,
    pub metrics: NodeMetrics,
}

impl NodeInfo {
    pub fn new(
        id: NodeId,
        hostname: impl Into<String>,
        address: impl Into<String>,
        cpu_cores: u32,
    ) -> Self {
        Self {
            id,
            hostname: hostname.into(),
            address: address.into(),
            cpu_cores,
            bandwidth_bytes_per_sec: None,
            latency_ms: None,
            metrics: NodeMetrics::default(),
        }
    }

    /// Declares the link speed toward this node.
    pub fn with_bandwidth(mut self, bytes_per_sec: u64) -> Self {
        self.bandwidth_bytes_per_sec = Some(bytes_per_sec);
        self
    }

    /// Declares the round-trip latency to this node.
    pub fn with_latency_ms(mut self, latency_ms: f32) -> Self {
        self.latency_ms = Some(latency_ms);
        self
    }

    /// Replaces the last reported metrics.
    pub fn update_metrics(&mut self, metrics: NodeMetrics) {
        self.metrics = metrics;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_clamp_out_of_range_and_nan_values() {
        let metrics = NodeMetrics::new(1.5, -0.2, 1024);
        assert_eq!(metrics.cpu_usage, 1.0);
        assert_eq!(metrics.memory_usage, 0.0);
        assert_eq!(NodeMetrics::new(f32::NAN, f32::NAN, 0).cpu_usage, 0.0);
    }

    #[test]
    fn memory_used_bytes_follows_the_ratio() {
        let metrics = NodeMetrics::new(0.0, 0.25, 8_000);
        assert_eq!(metrics.memory_used_bytes(), 2_000);
    }

    #[test]
    fn new_node_starts_idle_and_accepts_metric_updates() {
        let mut node = NodeInfo::new(NodeId::generate(), "rpi4", "192.168.1.10:7000", 4);
        assert_eq!(node.metrics, NodeMetrics::default());

        let metrics = NodeMetrics::new(0.5, 0.5, 4_096);
        node.update_metrics(metrics);
        assert_eq!(node.metrics, metrics);
    }
}
