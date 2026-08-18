//! Local resource sampling. All OS-specific work is delegated to `sysinfo`.

use std::time::Duration;

use aether_core::{NodeId, NodeInfo, NodeMetrics};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

/// Shortest gap between two samples that still yields a meaningful CPU figure.
pub const MIN_SAMPLE_INTERVAL: Duration = sysinfo::MINIMUM_CPU_UPDATE_INTERVAL;

/// Samples CPU and memory usage of the machine the agent runs on.
///
/// CPU usage is a difference between consecutive samples, so a sample taken
/// immediately after construction reports whatever the near-zero interval
/// happened to produce — anywhere from 0 % to 100 %, depending on the platform.
/// Wait at least [`MIN_SAMPLE_INTERVAL`] before reading a number you intend to
/// act on.
pub struct MetricsCollector {
    system: System,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let mut system = System::new_with_specifics(
            RefreshKind::nothing()
                .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
                .with_memory(MemoryRefreshKind::nothing().with_ram()),
        );
        system.refresh_cpu_usage();
        system.refresh_memory();
        Self { system }
    }

    /// Takes a fresh sample of CPU and memory usage.
    pub fn sample(&mut self) -> NodeMetrics {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();

        let total = self.system.total_memory();
        let memory_usage = if total == 0 {
            0.0
        } else {
            self.system.used_memory() as f32 / total as f32
        };

        NodeMetrics::new(self.system.global_cpu_usage() / 100.0, memory_usage, total)
    }

    /// Number of logical CPUs visible to this machine.
    pub fn cpu_cores(&self) -> u32 {
        self.system.cpus().len() as u32
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Machine hostname, or `"unknown"` when the OS does not report one.
pub fn hostname() -> String {
    System::host_name().unwrap_or_else(|| "unknown".to_string())
}

/// Describes this machine for registration, including a first metrics sample.
///
/// Blocks for [`MIN_SAMPLE_INTERVAL`] first. CPU usage is a difference between
/// two samples, and two samples taken back to back produce a number that is not
/// merely imprecise but arbitrary — a node registering with a fabricated 100 %
/// gets no work until its first heartbeat corrects it, and one registering with
/// a fabricated 0 % gets everyone's.
pub fn local_node_info(id: NodeId, address: impl Into<String>) -> NodeInfo {
    let mut collector = MetricsCollector::new();
    std::thread::sleep(MIN_SAMPLE_INTERVAL);

    let mut info = NodeInfo::new(id, hostname(), address, collector.cpu_cores());
    info.update_metrics(collector.sample());
    info
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_stays_within_range_and_reports_memory() {
        let mut collector = MetricsCollector::new();
        std::thread::sleep(MIN_SAMPLE_INTERVAL);
        let metrics = collector.sample();

        assert!((0.0..=1.0).contains(&metrics.cpu_usage));
        assert!((0.0..=1.0).contains(&metrics.memory_usage));
        assert!(metrics.memory_total_bytes > 0);
        assert!(metrics.memory_used_bytes() <= metrics.memory_total_bytes);
    }

    #[test]
    fn machine_description_is_populated() {
        assert!(!hostname().is_empty());

        let info = local_node_info(NodeId::generate(), "127.0.0.1:7000");
        assert!(!info.hostname.is_empty());
        assert!(info.cpu_cores >= 1);
    }

    #[test]
    fn registration_waits_long_enough_for_a_real_first_sample() {
        let started = std::time::Instant::now();
        let info = local_node_info(NodeId::generate(), "127.0.0.1:7000");

        // Without the wait the CPU figure is arbitrary, and the scheduler acts
        // on it until the first heartbeat replaces it.
        assert!(started.elapsed() >= MIN_SAMPLE_INTERVAL);
        assert!((0.0..=1.0).contains(&info.metrics.cpu_usage));
        assert!(info.metrics.memory_total_bytes > 0);
    }
}
