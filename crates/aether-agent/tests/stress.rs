//! Sustained load against a real mesh, looking for the failures that only
//! appear under contention.
//!
//! The ordinary tests each check one behaviour on a quiet mesh. Every bug this
//! project has actually shipped was a concurrency one — a dispatch racing a
//! transfer, a waiter overwritten by a second request for the same dataset —
//! and none of them would have shown up in a test that submits one task.
//!
//! These are slow by design. They are still unit-test-shaped rather than a
//! benchmark: each asserts something exact, so a failure names what broke.

use std::sync::Arc;
use std::time::Duration;

use aether_agent::{AgentClient, MetricsCollector};
use aether_controller::{Controller, MeshState, NetworkTransport, SecurityConfig, bind, serve};
use aether_core::task::kind;
use aether_core::{NodeId, NodeInfo, Task};
use aether_scheduler::AdvancedScheduler;
use tokio::task::JoinSet;

/// A controller and `nodes` real agents over real sockets.
async fn mesh(
    nodes: usize,
) -> (
    MeshState,
    Arc<Controller<AdvancedScheduler, NetworkTransport>>,
) {
    let state = MeshState::new();
    let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();

    let serving = state.clone();
    tokio::spawn(async move {
        let _ = serve(listener, serving, SecurityConfig::open()).await;
    });

    for index in 0..nodes {
        let info = NodeInfo::new(
            NodeId::generate(),
            format!("node-{index}"),
            "127.0.0.1:1",
            4,
        );
        let node_id = info.id;
        let mut client = AgentClient::connect(addr, info).await.unwrap();
        tokio::spawn(async move {
            let _ = client
                .run(MetricsCollector::new(), Duration::from_millis(200))
                .await;
        });

        for _ in 0..300 {
            if state.connections.is_connected(node_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            state.connections.is_connected(node_id),
            "node never arrived"
        );
    }

    let controller = Arc::new(Controller::new(
        AdvancedScheduler::new(state.catalog.clone()),
        NetworkTransport::new(state.connections.clone()),
        state.catalog.clone(),
    ));
    controller.sync_registry(state.nodes());
    (state, controller)
}

/// Varied bytes, so chunk dedup does not flatter a transfer count.
fn dataset(size: usize) -> Vec<u8> {
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    (0..size)
        .map(|_| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) as u8
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_concurrent_tasks_over_one_dataset_move_it_once_per_node() {
    let (_state, controller) = mesh(3).await;

    let bytes = dataset(16 * 1024 * 1024);
    let descriptor = controller.publish(bytes);

    let mut running = JoinSet::new();
    for _ in 0..100 {
        let controller = controller.clone();
        let input = descriptor.id;
        running.spawn(async move {
            controller
                .submit(Task::new(kind::HASH, Vec::new()).with_inputs(vec![input]))
                .await
        });
    }

    let mut nodes = std::collections::HashSet::new();
    while let Some(finished) = running.join_next().await {
        let result = finished.unwrap().expect("every task should be placed");
        assert!(result.is_success(), "{result:?}");
        nodes.insert(result.node_id);
    }

    // The invariant, stated as *attempts* rather than bytes. Bytes hide the
    // bug: a duplicated transfer to a node that already has every chunk sends
    // nothing, so the byte count still looks perfect while two transfers of the
    // same dataset are racing each other on the wire. Counting attempts is what
    // actually distinguishes single-flight from luck — measured, after a
    // byte-based version of this assertion passed against the broken code.
    let transferred = 100 - controller.transfers_skipped();
    assert_eq!(
        transferred,
        nodes.len() as u64,
        "{transferred} transfers for {} node(s): a dataset went to one of them twice",
        nodes.len()
    );

    let moved = controller.data_bytes_uncompressed();
    assert_eq!(moved, descriptor.size_bytes * nodes.len() as u64);
    assert_eq!(
        controller.retries(),
        0,
        "a retry means something went wrong"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thousand_small_tasks_all_come_back() {
    let (_state, controller) = mesh(2).await;

    let mut running = JoinSet::new();
    for index in 0..1_000u32 {
        let controller = controller.clone();
        running.spawn(async move {
            controller
                .submit(Task::new(kind::ECHO, index.to_le_bytes().to_vec()))
                .await
        });
    }

    let mut seen = std::collections::HashSet::new();
    while let Some(finished) = running.join_next().await {
        let result = finished.unwrap().expect("every task should be placed");
        assert!(result.is_success(), "{result:?}");
        // Echo returns its payload, so the output identifies the submission:
        // a mesh that answered the wrong task would show up as a duplicate.
        seen.insert(result.output().expect("echo returns bytes").to_vec());
    }

    assert_eq!(seen.len(), 1_000, "some task got somebody else's answer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_datasets_to_many_nodes_do_not_confuse_each_other() {
    let (_state, controller) = mesh(3).await;

    // Twenty distinct datasets, each read by five tasks, all at once. Every
    // transfer is racing nineteen others to the same three nodes.
    let descriptors: Vec<_> = (0..20)
        .map(|index| controller.publish(dataset(64 * 1024 + index)))
        .collect();

    let mut running = JoinSet::new();
    for descriptor in &descriptors {
        for _ in 0..5 {
            let controller = controller.clone();
            let input = descriptor.id;
            running.spawn(async move {
                controller
                    .submit(Task::new(kind::HASH, Vec::new()).with_inputs(vec![input]))
                    .await
            });
        }
    }

    let mut outputs = std::collections::HashMap::new();
    while let Some(finished) = running.join_next().await {
        let result = finished.unwrap().expect("every task should be placed");
        assert!(result.is_success(), "{result:?}");
        outputs
            .entry(result.output().expect("hash returns bytes").to_vec())
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    // Twenty distinct inputs, so twenty distinct digests, five of each. A
    // dataset assembled from another's chunks would show up here as a digest
    // nobody asked for.
    assert_eq!(outputs.len(), 20, "digests: {outputs:?}");
    assert!(outputs.values().all(|count| *count == 5));
}
