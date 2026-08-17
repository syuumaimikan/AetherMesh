//! Three agents, one controller, real TCP sockets.
//!
//! This is the local stand-in for the physical setup described in
//! `docs/multi-node.md` (desktop + Raspberry Pi + cloud VM): same code path,
//! same protocol, three independent connections.

use std::time::Duration;

use aether_agent::{AgentClient, MetricsCollector};
use aether_controller::{
    Controller, MeshState, NetworkTransport, RetryPolicy, SecurityConfig, bind, evict_stale_nodes,
    serve,
};
use aether_core::task::kind;
use aether_core::{NodeId, NodeInfo, Task};
use aether_scheduler::AdvancedScheduler;
use tokio::task::JoinHandle;

struct Mesh {
    state: MeshState,
    addr: std::net::SocketAddr,
    agents: Vec<(NodeId, JoinHandle<()>)>,
}

impl Mesh {
    async fn start(nodes: usize) -> Self {
        let state = MeshState::new();
        let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();

        let serve_state = state.clone();
        tokio::spawn(async move {
            let _ = serve(listener, serve_state, SecurityConfig::open()).await;
        });

        let mut mesh = Self {
            state,
            addr,
            agents: Vec::new(),
        };
        for index in 0..nodes {
            let node = mesh.attach(&format!("node-{index}")).await;
            mesh.agents.push(node);
        }
        mesh
    }

    async fn attach(&self, hostname: &str) -> (NodeId, JoinHandle<()>) {
        let info = NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7001", 4)
            .with_bandwidth(10 * 1024 * 1024)
            .with_latency_ms(2.0);
        let node_id = info.id;
        let mut client = AgentClient::connect(self.addr, info).await.unwrap();

        let handle = tokio::spawn(async move {
            let _ = client
                .run(MetricsCollector::new(), Duration::from_millis(100))
                .await;
        });

        self.wait_until(|state| state.connections.is_connected(node_id))
            .await;
        (node_id, handle)
    }

    async fn wait_until(&self, check: impl Fn(&MeshState) -> bool) {
        for _ in 0..300 {
            if check(&self.state) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached within 3s");
    }

    fn controller(&self) -> Controller<AdvancedScheduler, NetworkTransport> {
        let mut controller = Controller::new(
            AdvancedScheduler::new(self.state.catalog.clone()),
            NetworkTransport::new(self.state.connections.clone())
                .with_timeout(Duration::from_secs(5)),
            self.state.catalog.clone(),
        )
        .with_retry(RetryPolicy {
            max_attempts: 3,
            backoff: Duration::from_millis(10),
        });

        for info in self.state.registry.lock().unwrap().nodes() {
            controller.registry_mut().register(info);
        }
        controller
    }
}

#[tokio::test]
async fn three_nodes_register_and_run_tasks() {
    let mesh = Mesh::start(3).await;
    assert_eq!(mesh.state.registry.lock().unwrap().len(), 3);

    let mut controller = mesh.controller();
    let dataset = vec![7u8; 512 * 1024];
    let descriptor = controller.publish(dataset.clone());

    for _ in 0..12 {
        let task = Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]);
        let result = controller.submit(task).await.unwrap();
        assert!(result.is_success(), "task failed: {result:?}");
        assert_eq!(
            result.output(),
            Some(blake3::hash(&dataset).as_bytes().as_slice())
        );
    }

    // Locality keeps the work on the node that already holds the dataset.
    assert_eq!(controller.data_bytes_uncompressed(), dataset.len() as u64);
    assert_eq!(controller.transfers_skipped(), 11);
}

#[tokio::test]
async fn work_continues_after_a_node_disappears() {
    let mesh = Mesh::start(3).await;
    let mut controller = mesh.controller();

    // Pin the dataset to whichever node runs first.
    let descriptor = controller.publish(vec![3u8; 64 * 1024]);
    let first = controller
        .submit(Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]))
        .await
        .unwrap();
    assert!(first.is_success());

    // That node dies mid-flight.
    let (dead_id, handle) = mesh
        .agents
        .iter()
        .find(|(node_id, _)| *node_id == first.node_id)
        .map(|(node_id, handle)| (*node_id, handle))
        .expect("result came from a known node");
    handle.abort();
    mesh.wait_until(|state| !state.connections.is_connected(dead_id))
        .await;

    // The task is re-dispatched to a survivor, data and all.
    let result = controller
        .submit(Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]))
        .await
        .unwrap();

    assert!(result.is_success(), "task failed: {result:?}");
    assert_ne!(result.node_id, dead_id);
    assert!(controller.retries() >= 1);
}

#[tokio::test]
async fn a_frozen_node_is_evicted_by_the_health_check() {
    let mesh = Mesh::start(2).await;

    // Nothing has timed out yet.
    assert!(evict_stale_nodes(&mesh.state, Duration::from_secs(30)).is_empty());

    // With a zero timeout every node counts as silent.
    let evicted = evict_stale_nodes(&mesh.state, Duration::ZERO);

    assert_eq!(evicted.len(), 2);
    assert!(mesh.state.registry.lock().unwrap().is_empty());
}
