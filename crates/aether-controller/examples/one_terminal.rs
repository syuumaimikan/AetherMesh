//! A controller, an agent, and three tasks — in one process, over real sockets.
//!
//! Run with: cargo run -p aether-controller --example one_terminal

use std::time::Duration;

use aether_controller::{Controller, MeshState, NetworkTransport, SecurityConfig, bind, serve};
use aether_core::task::kind;
use aether_core::{NodeId, NodeInfo, Task};
use aether_scheduler::AdvancedScheduler;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = MeshState::new();
    let (listener, addr) = bind("127.0.0.1:0".parse()?).await?;

    let serving = state.clone();
    tokio::spawn(async move {
        let _ = serve(listener, serving, SecurityConfig::open()).await;
    });

    // One worker. In a real deployment this is `aether-agent` on another
    // machine; nothing about the code below knows the difference.
    let info = NodeInfo::new(NodeId::generate(), "local", "127.0.0.1:7001", 4);
    let agent = aether_agent::AgentClient::connect(addr, info).await?;
    tokio::spawn(async move {
        let mut agent = agent;
        let _ = agent
            .run(
                aether_agent::MetricsCollector::new(),
                Duration::from_secs(2),
            )
            .await;
    });

    // Wait for the node to show up, then take a snapshot for the scheduler.
    #[allow(unused_mut)]
    let mut controller = Controller::new(
        AdvancedScheduler::new(state.catalog.clone()),
        NetworkTransport::new(state.connections.clone()),
        state.catalog.clone(),
    );
    for _ in 0..100 {
        controller.sync_registry(state.nodes());
        if !controller.registry().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!("mesh ready: {} node", controller.registry().len());

    let echo = controller
        .submit(Task::new(kind::ECHO, b"hello".to_vec()))
        .await?;
    println!(
        "echo    -> {:?}          in {:.1} ms",
        String::from_utf8_lossy(echo.output().unwrap_or_default()),
        echo.duration.as_secs_f64() * 1000.0
    );

    let hash = controller
        .submit(Task::new(kind::HASH, b"hello".to_vec()))
        .await?;
    println!(
        "hash    -> {}…            in {:.1} ms",
        hex_prefix(hash.output().unwrap_or_default()),
        hash.duration.as_secs_f64() * 1000.0
    );

    let cpu = controller
        .submit(Task::new(kind::CPU, 100_000u64.to_le_bytes().to_vec()))
        .await?;
    println!(
        "cpu     -> 100000 rounds    in {:.1} ms",
        cpu.duration.as_secs_f64() * 1000.0
    );

    Ok(())
}

/// First four bytes of a digest, for a line that fits on a screen.
fn hex_prefix(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(4)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
