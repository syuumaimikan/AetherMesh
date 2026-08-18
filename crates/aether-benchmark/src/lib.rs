//! Measures AetherMesh latency, throughput, and transferred bytes, and compares
//! it against a baseline that has none of the optimisations turned on.
//!
//! The measurements here run in one process against a simulated mesh, which is
//! right for scheduler arithmetic and worthless as evidence about a network.
//! [`network`] measures a controller that is actually running, over sockets.
//!
//! Both sides run against the same in-process mesh with the same real message
//! encoding and the same task executor, so the difference is the optimisation
//! layer and nothing else.

use std::time::{Duration, Instant};

use aether_agent::execute;
use aether_controller::{Controller, DispatchError, SimulatedMesh};
use aether_core::task::kind;
use aether_core::{CompressionPolicy, DataId, NodeId, NodeInfo, NodeMetrics, Task};
use aether_scheduler::{AdvancedScheduler, DataCatalog, LeastLoadedScheduler, Scheduler};
use serde::{Deserialize, Serialize};

/// Which configuration to measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// No data reuse, no chunking, no compression, load-only scheduling.
    Baseline,
    /// Locality-aware scheduling with dedup, chunking, and adaptive compression.
    AetherMesh,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::AetherMesh => "aethermesh",
        }
    }
}

/// What to run.
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub tasks: usize,
    pub nodes: usize,
    pub kind: String,
    /// Payload size for `echo`/`hash` tasks.
    pub payload_bytes: usize,
    /// Iteration count for `cpu` tasks.
    pub cpu_iterations: u64,
    /// Size of a shared dataset every task reads. Zero means no inputs.
    pub dataset_bytes: usize,
    pub chunk_size: usize,
    /// Link speed of every node, used by the compression policy and scheduler.
    pub bandwidth_bytes_per_sec: u64,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            tasks: 100,
            nodes: 3,
            kind: kind::HASH.to_string(),
            payload_bytes: 4096,
            cpu_iterations: 100_000,
            dataset_bytes: 4 * 1024 * 1024,
            chunk_size: 1024 * 1024,
            // ~80 Mbps: a link where saved bytes are worth CPU time.
            bandwidth_bytes_per_sec: 10 * 1024 * 1024,
        }
    }
}

impl BenchmarkConfig {
    /// Builds the payload one task carries.
    fn payload(&self) -> Vec<u8> {
        if self.kind == kind::CPU {
            self.cpu_iterations.to_le_bytes().to_vec()
        } else {
            vec![0xab; self.payload_bytes]
        }
    }

    /// The shared dataset, if this run uses one.
    fn dataset(&self) -> Option<Vec<u8>> {
        (self.dataset_bytes > 0).then(|| {
            (0..self.dataset_bytes)
                // Repetitive but not uniform: compressible, like real data.
                .map(|i| ((i / 64) % 251) as u8)
                .collect()
        })
    }
}

/// What one run measured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub mode: Mode,
    pub tasks: usize,
    pub nodes: usize,
    pub kind: String,
    pub payload_bytes: usize,
    pub dataset_bytes: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub total_duration_ms: f64,
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub throughput_tasks_per_sec: f64,
    /// Every byte the mesh put on the wire, in both directions.
    pub transferred_bytes: u64,
    /// Input data bytes on the wire, after compression.
    pub data_bytes_sent: u64,
    /// What those inputs would have cost uncompressed.
    pub data_bytes_uncompressed: u64,
}

impl BenchmarkReport {
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    /// One-screen summary for the terminal.
    pub fn to_text(&self) -> String {
        format!(
            "mode:             {}\n\
             tasks:            {}\n\
             nodes:            {}\n\
             kind:             {}\n\
             payload bytes:    {}\n\
             dataset bytes:    {}\n\
             succeeded:        {}\n\
             failed:           {}\n\
             total time:       {:.3} ms\n\
             avg latency:      {:.3} ms\n\
             p50 latency:      {:.3} ms\n\
             p95 latency:      {:.3} ms\n\
             p99 latency:      {:.3} ms\n\
             throughput:       {:.1} tasks/s\n\
             transferred:      {} bytes\n\
             data sent:        {} bytes (uncompressed {})",
            self.mode.as_str(),
            self.tasks,
            self.nodes,
            self.kind,
            self.payload_bytes,
            self.dataset_bytes,
            self.succeeded,
            self.failed,
            self.total_duration_ms,
            self.avg_latency_ms,
            self.p50_latency_ms,
            self.p95_latency_ms,
            self.p99_latency_ms,
            self.throughput_tasks_per_sec,
            self.transferred_bytes,
            self.data_bytes_sent,
            self.data_bytes_uncompressed,
        )
    }
}

/// Baseline against AetherMesh, with the deltas worked out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub baseline: BenchmarkReport,
    pub aethermesh: BenchmarkReport,
    /// Percentage of wire bytes the mesh avoided.
    pub traffic_reduction_percent: f64,
    /// Baseline time divided by AetherMesh time; above 1.0 means faster.
    pub speedup: f64,
}

impl ComparisonReport {
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    pub fn to_text(&self) -> String {
        format!(
            "{}\n\n{}\n\n\
             transferred bytes: {} -> {}\n\
             traffic reduction: {:.1} %\n\
             execution time:    {:.3} ms -> {:.3} ms\n\
             speedup:           {:.2}x\n\
             p50 / p95 / p99:   {:.3} / {:.3} / {:.3} ms -> {:.3} / {:.3} / {:.3} ms",
            self.baseline.to_text(),
            self.aethermesh.to_text(),
            self.baseline.transferred_bytes,
            self.aethermesh.transferred_bytes,
            self.traffic_reduction_percent,
            self.baseline.total_duration_ms,
            self.aethermesh.total_duration_ms,
            self.speedup,
            self.baseline.p50_latency_ms,
            self.baseline.p95_latency_ms,
            self.baseline.p99_latency_ms,
            self.aethermesh.p50_latency_ms,
            self.aethermesh.p95_latency_ms,
            self.aethermesh.p99_latency_ms,
        )
    }
}

/// Builds the controller for a mode. Both share the executor and the mesh.
fn controller_for(
    mode: Mode,
    config: &BenchmarkConfig,
) -> Controller<Box<dyn Scheduler>, SimulatedMesh> {
    let catalog = DataCatalog::new();
    let mesh = SimulatedMesh::with_executor(execute);

    match mode {
        Mode::Baseline => Controller::new(
            Box::new(LeastLoadedScheduler::new()) as Box<dyn Scheduler>,
            mesh,
            catalog,
        )
        .with_compression(CompressionPolicy::disabled())
        .with_chunk_size(usize::MAX)
        .with_data_reuse(false),
        Mode::AetherMesh => Controller::new(
            Box::new(AdvancedScheduler::new(catalog.clone())) as Box<dyn Scheduler>,
            mesh,
            catalog,
        )
        .with_chunk_size(config.chunk_size),
    }
}

/// Runs the benchmark in one mode.
pub async fn run(config: &BenchmarkConfig, mode: Mode) -> Result<BenchmarkReport, DispatchError> {
    let controller = controller_for(mode, config);

    for index in 0..config.nodes {
        let mut info = NodeInfo::new(
            NodeId::generate(),
            format!("bench-node-{index}"),
            "127.0.0.1:7000",
            4,
        )
        .with_bandwidth(config.bandwidth_bytes_per_sec)
        .with_latency_ms(5.0);
        // Spread the load so the scheduler has a reason to prefer one node.
        info.update_metrics(NodeMetrics::new(index as f32 / config.nodes as f32, 0.5, 0));
        controller.register(info);
    }

    let payload = config.payload();
    let inputs: Vec<DataId> = config
        .dataset()
        .map(|dataset| vec![controller.publish(dataset).id])
        .unwrap_or_default();

    let mut succeeded = 0;
    let mut failed = 0;
    let mut latencies = Vec::with_capacity(config.tasks);

    let started = Instant::now();
    for _ in 0..config.tasks {
        let task = Task::new(config.kind.clone(), payload.clone()).with_inputs(inputs.clone());
        let task_started = Instant::now();
        let result = controller.submit(task).await?;
        latencies.push(task_started.elapsed());

        if result.is_success() {
            succeeded += 1;
        } else {
            failed += 1;
        }
    }
    let total = started.elapsed();

    latencies.sort_unstable();
    let latency_total: Duration = latencies.iter().sum();
    let tasks = config.tasks.max(1) as f64;

    Ok(BenchmarkReport {
        mode,
        tasks: config.tasks,
        nodes: config.nodes,
        kind: config.kind.clone(),
        payload_bytes: payload.len(),
        dataset_bytes: config.dataset_bytes,
        succeeded,
        failed,
        total_duration_ms: total.as_secs_f64() * 1000.0,
        avg_latency_ms: latency_total.as_secs_f64() * 1000.0 / tasks,
        p50_latency_ms: percentile_ms(&latencies, 0.50),
        p95_latency_ms: percentile_ms(&latencies, 0.95),
        p99_latency_ms: percentile_ms(&latencies, 0.99),
        throughput_tasks_per_sec: if total.is_zero() {
            0.0
        } else {
            config.tasks as f64 / total.as_secs_f64()
        },
        transferred_bytes: controller.transport().bytes_transferred(),
        data_bytes_sent: controller.data_bytes_sent(),
        data_bytes_uncompressed: controller.data_bytes_uncompressed(),
    })
}

/// Runs both modes and reports the difference.
pub async fn compare(config: &BenchmarkConfig) -> Result<ComparisonReport, DispatchError> {
    let baseline = run(config, Mode::Baseline).await?;
    let aethermesh = run(config, Mode::AetherMesh).await?;

    let traffic_reduction_percent = if baseline.transferred_bytes == 0 {
        0.0
    } else {
        let saved = baseline.transferred_bytes as f64 - aethermesh.transferred_bytes as f64;
        saved / baseline.transferred_bytes as f64 * 100.0
    };
    let speedup = if aethermesh.total_duration_ms == 0.0 {
        0.0
    } else {
        baseline.total_duration_ms / aethermesh.total_duration_ms
    };

    Ok(ComparisonReport {
        baseline,
        aethermesh,
        traffic_reduction_percent,
        speedup,
    })
}

/// Nearest-rank percentile over a sorted slice, in milliseconds.
fn percentile_ms(sorted: &[Duration], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (quantile * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1].as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(kind: &str) -> BenchmarkConfig {
        BenchmarkConfig {
            tasks: 10,
            nodes: 2,
            kind: kind.to_string(),
            payload_bytes: 128,
            cpu_iterations: 1_000,
            dataset_bytes: 256 * 1024,
            chunk_size: 64 * 1024,
            ..Default::default()
        }
    }

    #[test]
    fn percentiles_use_the_nearest_rank() {
        let sorted: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();

        assert_eq!(percentile_ms(&sorted, 0.50), 50.0);
        assert_eq!(percentile_ms(&sorted, 0.95), 95.0);
        assert_eq!(percentile_ms(&sorted, 0.99), 99.0);
        assert_eq!(percentile_ms(&[], 0.5), 0.0);
    }

    #[tokio::test]
    async fn a_hash_run_reports_every_metric() {
        let report = run(&config(kind::HASH), Mode::AetherMesh).await.unwrap();

        assert_eq!(report.succeeded, 10);
        assert_eq!(report.failed, 0);
        assert_eq!(report.payload_bytes, 128);
        assert!(report.total_duration_ms > 0.0);
        assert!(report.throughput_tasks_per_sec > 0.0);
        assert!(report.p50_latency_ms <= report.p95_latency_ms);
        assert!(report.p95_latency_ms <= report.p99_latency_ms);
    }

    #[tokio::test]
    async fn a_cpu_run_sends_the_iteration_count_as_its_payload() {
        let mut config = config(kind::CPU);
        config.dataset_bytes = 0;
        let report = run(&config, Mode::AetherMesh).await.unwrap();

        assert_eq!(report.payload_bytes, 8);
        assert_eq!(report.succeeded, 10);
        assert_eq!(report.data_bytes_sent, 0);
    }

    #[tokio::test]
    async fn unsupported_kinds_are_counted_as_failures() {
        let mut config = config("nonexistent");
        config.dataset_bytes = 0;
        let report = run(&config, Mode::AetherMesh).await.unwrap();

        assert_eq!(report.failed, 10);
        assert_eq!(report.succeeded, 0);
    }

    #[tokio::test]
    async fn running_without_nodes_fails() {
        let mut config = config(kind::ECHO);
        config.nodes = 0;

        assert!(matches!(
            run(&config, Mode::Baseline).await,
            Err(DispatchError::NoNodeAvailable(_))
        ));
    }

    #[tokio::test]
    async fn the_baseline_resends_the_dataset_for_every_task() {
        let config = config(kind::HASH);
        let report = run(&config, Mode::Baseline).await.unwrap();

        assert_eq!(report.succeeded, 10);
        assert_eq!(
            report.data_bytes_uncompressed,
            (config.dataset_bytes * config.tasks) as u64
        );
        // Baseline never compresses.
        assert_eq!(report.data_bytes_sent, report.data_bytes_uncompressed);
    }

    #[tokio::test]
    async fn aethermesh_sends_the_dataset_once_and_compressed() {
        let config = config(kind::HASH);
        let report = run(&config, Mode::AetherMesh).await.unwrap();

        assert_eq!(report.succeeded, 10);
        assert_eq!(report.data_bytes_uncompressed, config.dataset_bytes as u64);
        assert!(report.data_bytes_sent < config.dataset_bytes as u64);
    }

    #[tokio::test]
    async fn the_comparison_shows_the_traffic_saved() {
        let comparison = compare(&config(kind::HASH)).await.unwrap();

        assert_eq!(comparison.baseline.mode, Mode::Baseline);
        assert_eq!(comparison.aethermesh.mode, Mode::AetherMesh);
        assert!(comparison.aethermesh.transferred_bytes < comparison.baseline.transferred_bytes);
        assert!(comparison.traffic_reduction_percent > 50.0);
        assert!(comparison.speedup > 0.0);
    }

    #[tokio::test]
    async fn the_report_round_trips_through_json() {
        let report = run(&config(kind::ECHO), Mode::AetherMesh).await.unwrap();
        let json = report.to_json().unwrap();
        let parsed: BenchmarkReport = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.mode, report.mode);
        assert_eq!(parsed.tasks, report.tasks);
        assert_eq!(parsed.transferred_bytes, report.transferred_bytes);
        // Floats are only compared approximately: the text form rounds the last digit.
        assert!((parsed.avg_latency_ms - report.avg_latency_ms).abs() < 1e-9);
    }
}

pub mod network;

pub mod regression;
