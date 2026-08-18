//! A mesh inside your own program, with no controller process to deploy.
//!
//!     cargo run -p aether-controller --example embedded
//!
//! Everything the controller binary does is a library first: the registry, the
//! catalog, the scheduler, the dispatch loop. A service that wants to spread
//! its own work across machines can hold those directly and skip the client
//! API entirely — one less process, one less port, one less thing to secure.
//!
//! This runs the control plane in-process and connects real agents over TCP.
//! The agents are the ordinary binary; nothing here is a simulation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aether_controller::{
    Controller, MeshState, NetworkTransport, RetryPolicy, SecurityConfig, bind, serve,
};
use aether_core::Task;
use aether_core::task::kind;
use aether_scheduler::AdvancedScheduler;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // The mesh's shared state: who is registered, who is connected, and which
    // node holds which dataset. One value, cloned wherever it is needed.
    let state = MeshState::new();
    let (listener, addr) = bind("127.0.0.1:0".parse()?).await?;
    println!("agents may register at {addr}");
    println!("start one with:\n  aether-agent --controller {addr}\n");

    // Accepting agents is a task, not a process.
    let serving = state.clone();
    tokio::spawn(async move {
        let _ = serve(listener, serving, SecurityConfig::open()).await;
    });

    // The controller itself. `Arc` because submitting from several places at
    // once is the normal case for a service, and dispatch takes `&self`.
    let controller = Arc::new(
        Controller::new(
            AdvancedScheduler::new(state.catalog.clone()),
            NetworkTransport::new(state.connections.clone()),
            state.catalog.clone(),
        )
        .with_retry(RetryPolicy {
            max_attempts: 3,
            backoff: Duration::from_millis(50),
        }),
    );

    // Wait for somebody to show up. A real service would carry on and let its
    // own callers wait, or refuse until the mesh is big enough.
    let deadline = Instant::now() + Duration::from_secs(60);
    while state.registry.lock().unwrap().is_empty() {
        if Instant::now() > deadline {
            anyhow::bail!("no agent registered within a minute");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The controller keeps its own copy of the registry, refreshed before a
    // placement rather than shared, so a read never waits on a dispatch.
    controller.sync_registry(state.nodes());
    println!("{} node(s) registered\n", state.nodes().len());

    // Published once. Every task below reads it, and it crosses the wire at
    // most once per node that actually runs one of them.
    // Varied rather than a repeated byte: a dataset of all sevens is one chunk
    // repeated, so chunk dedup would flatter the transfer number below.
    let mut seed = 0x243f_6a88_85a3_08d3u64;
    let dataset: Vec<u8> = (0..4 * 1024 * 1024)
        .map(|_| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) as u8
        })
        .collect();
    let descriptor = controller.publish(dataset);
    println!(
        "published {} bytes as {}",
        descriptor.size_bytes, descriptor.id
    );

    // Submit concurrently: `submit` takes `&self`, so this is what a request
    // handler would do, not a special batch mode.
    let started = Instant::now();
    let mut running = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let controller = controller.clone();
        let input = descriptor.id;
        running.spawn(async move {
            controller
                .submit(Task::new(kind::HASH, Vec::new()).with_inputs(vec![input]))
                .await
        });
    }

    let mut nodes = std::collections::BTreeMap::new();
    let mut failures = 0;
    while let Some(finished) = running.join_next().await {
        match finished? {
            Ok(result) if result.is_success() => {
                *nodes
                    .entry(result.node_id.to_string()[..8].to_string())
                    .or_insert(0) += 1;
            }
            Ok(result) => {
                failures += 1;
                eprintln!("task failed: {:?}", result.outcome);
            }
            Err(error) => {
                failures += 1;
                eprintln!("could not place a task: {error}");
            }
        }
    }

    println!(
        "\n16 tasks in {:?}, {failures} failed\n  spread {nodes:?}",
        started.elapsed()
    );
    println!(
        "  {} bytes moved for a 4 MiB dataset, {} transfers skipped",
        controller.data_bytes_uncompressed(),
        controller.transfers_skipped()
    );
    println!("\nThe dataset moved once per node that ran work, not once per task.");

    Ok(())
}
