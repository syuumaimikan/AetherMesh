//! End-to-end dispatch with no network: registry -> scheduler -> simulated node.
//!
//! Run with: cargo run -p aether-controller --example dispatch_simulation

use aether_controller::{Controller, SimulatedMesh};
use aether_core::{NodeId, NodeInfo, NodeMetrics, Task};
use aether_scheduler::{DataCatalog, LeastLoadedScheduler};

#[tokio::main]
async fn main() {
    let controller = Controller::new(
        LeastLoadedScheduler::new(),
        SimulatedMesh::new(),
        DataCatalog::new(),
    );

    for (hostname, cpu) in [("desktop", 0.80), ("rpi4", 0.15), ("cloud-vm", 0.45)] {
        let mut info = NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7000", 4);
        info.update_metrics(NodeMetrics::new(cpu, 0.5, 4 * 1024 * 1024 * 1024));
        controller.register(info);
    }

    for size in [16usize, 1024, 65536] {
        let task = Task::new("echo", vec![0xab; size]);
        match controller.submit(task).await {
            Ok(result) => {
                let node = controller
                    .registry()
                    .get(result.node_id)
                    .map(|entry| entry.info.hostname.clone())
                    .unwrap_or_else(|| result.node_id.to_string());
                println!(
                    "task {size:>6} B -> {node:<9} success={} in {:?}",
                    result.is_success(),
                    result.duration
                );
            }
            Err(error) => println!("task {size:>6} B -> dispatch failed: {error}"),
        }
    }

    println!(
        "simulated bytes transferred: {}",
        controller.transport().bytes_transferred()
    );
}
