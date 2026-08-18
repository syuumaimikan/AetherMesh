//! A node that outlives the controller it registered with.
//!
//! The controller's catalog is in memory: restart it and it comes back knowing
//! nothing about where any data is, while the agents are still holding all of
//! it. So an agent announces what it has after registering, and a "restarted
//! controller" here is exactly what it is in production — a fresh `MeshState`
//! with an empty catalog.

use std::time::Duration;

use aether_agent::{AgentClient, MetricsCollector};
use aether_controller::{MeshState, SecurityConfig, bind, serve};
use aether_core::{DataStore, NodeId, NodeInfo};

/// Starts a controller and returns its state and address.
async fn controller() -> (MeshState, std::net::SocketAddr) {
    let state = MeshState::new();
    let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let serving = state.clone();
    tokio::spawn(async move {
        let _ = serve(listener, serving, SecurityConfig::open()).await;
    });
    (state, addr)
}

async fn wait_until(check: impl Fn() -> bool) -> bool {
    for _ in 0..300 {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

fn node(node_id: NodeId) -> NodeInfo {
    NodeInfo::new(node_id, "holder", "127.0.0.1:7001", 4)
}

#[tokio::test]
async fn a_node_that_already_holds_data_says_so_when_it_registers() {
    let (state, addr) = controller().await;

    // Data this node was sent by some earlier controller.
    let store = DataStore::new();
    let descriptor = store.put(vec![9u8; 4096]);

    let node_id = NodeId::generate();
    let mut client = AgentClient::connect(addr, node(node_id))
        .await
        .unwrap()
        .with_store(store);
    tokio::spawn(async move {
        let _ = client
            .run(MetricsCollector::new(), Duration::from_millis(100))
            .await;
    });

    let known = wait_until(|| state.catalog.locations(descriptor.id).contains(&node_id)).await;

    assert!(known, "the controller never learned where the data was");
    assert_eq!(
        state
            .catalog
            .descriptor(descriptor.id)
            .map(|d| d.size_bytes),
        Some(4096),
        "the size came back with it, so the scheduler can cost a transfer"
    );
}

#[tokio::test]
async fn a_node_holding_nothing_announces_nothing() {
    let (state, addr) = controller().await;

    let node_id = NodeId::generate();
    let mut client = AgentClient::connect(addr, node(node_id)).await.unwrap();
    tokio::spawn(async move {
        let _ = client
            .run(MetricsCollector::new(), Duration::from_millis(100))
            .await;
    });

    // Registered, so the connection is up and an announcement would have
    // arrived by now if there were one to make.
    assert!(wait_until(|| state.connections.is_connected(node_id)).await);
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(state.catalog.len(), 0);
}

#[tokio::test]
async fn the_data_survives_the_controller_it_arrived_on() {
    let store = DataStore::new();
    let descriptor = store.put(vec![3u8; 2048]);
    let node_id = NodeId::generate();

    // First controller. The node registers and reports what it holds.
    let (first, first_addr) = controller().await;
    let mut client = AgentClient::connect(first_addr, node(node_id))
        .await
        .unwrap()
        .with_store(store.clone());
    let running = tokio::spawn(async move {
        let _ = client
            .run(MetricsCollector::new(), Duration::from_millis(100))
            .await;
    });
    assert!(wait_until(|| first.catalog.locations(descriptor.id).contains(&node_id)).await);

    // It goes away. This is a new process as far as anything here can tell:
    // a new catalog, a new registry, no memory of the mesh at all.
    running.abort();
    let (second, second_addr) = controller().await;
    assert_eq!(second.catalog.len(), 0);

    // The agent reconnects with the same store, which is the whole point of
    // it having stayed alive.
    let mut client = AgentClient::connect(second_addr, node(node_id))
        .await
        .unwrap()
        .with_store(store);
    tokio::spawn(async move {
        let _ = client
            .run(MetricsCollector::new(), Duration::from_millis(100))
            .await;
    });

    let known = wait_until(|| second.catalog.locations(descriptor.id).contains(&node_id)).await;

    assert!(known, "the new controller never learned the data was there");
}
