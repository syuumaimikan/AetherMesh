//! Agent <-> controller over a real TCP connection: register, heartbeat, run tasks.

use std::time::Duration;

use aether_agent::{AgentClient, MetricsCollector};
use aether_controller::{
    Controller, DispatchError, MeshState, NetworkTransport, RetryPolicy, SecurityConfig, bind,
    serve,
};
use aether_core::task::kind;
use aether_core::{NodeId, NodeInfo, NodeMetrics, Task};
use aether_scheduler::{LeastLoadedScheduler, LocalityScheduler};

struct Harness {
    state: MeshState,
    addr: std::net::SocketAddr,
}

impl Harness {
    async fn start() -> Self {
        let state = MeshState::new();
        let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();

        let serve_state = state.clone();
        tokio::spawn(async move {
            let _ = serve(listener, serve_state, SecurityConfig::open()).await;
        });

        Self { state, addr }
    }

    /// Connects an agent and keeps its run loop going in the background.
    async fn attach_agent(&self, hostname: &str) -> NodeId {
        self.attach_agent_handle(hostname).await.0
    }

    /// Same, but hands back the task so a test can drop the agent mid-run.
    async fn attach_agent_handle(&self, hostname: &str) -> (NodeId, tokio::task::JoinHandle<()>) {
        let info = NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7001", 4);
        let node_id = info.id;
        let mut client = AgentClient::connect(self.addr, info).await.unwrap();

        let handle = tokio::spawn(async move {
            let _ = client
                .run(MetricsCollector::new(), Duration::from_millis(200))
                .await;
        });

        self.wait_until(|registry| registry.contains(node_id)).await;
        (node_id, handle)
    }

    /// Waits until `check` holds, so tests do not depend on task scheduling order.
    async fn wait_until(&self, check: impl Fn(&aether_controller::NodeRegistry) -> bool) {
        for _ in 0..200 {
            if check(&self.state.registry.lock().unwrap()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached within 2s");
    }

    fn transport(&self) -> NetworkTransport {
        NetworkTransport::new(self.state.connections.clone()).with_timeout(Duration::from_secs(5))
    }

    fn controller(&self) -> Controller<LeastLoadedScheduler, NetworkTransport> {
        self.fill(Controller::new(
            LeastLoadedScheduler::new(),
            self.transport(),
            self.state.catalog.clone(),
        ))
    }

    /// A controller that moves tasks toward the data.
    fn locality_controller(&self) -> Controller<LocalityScheduler, NetworkTransport> {
        self.fill(Controller::new(
            LocalityScheduler::new(self.state.catalog.clone()),
            self.transport(),
            self.state.catalog.clone(),
        ))
    }

    /// Same, but splitting published data into chunks of `chunk_size`.
    fn chunked_controller(
        &self,
        chunk_size: usize,
    ) -> Controller<LocalityScheduler, NetworkTransport> {
        self.fill(
            Controller::new(
                LocalityScheduler::new(self.state.catalog.clone()),
                self.transport(),
                self.state.catalog.clone(),
            )
            .with_chunk_size(chunk_size),
        )
    }

    /// Copies the live registry into a controller's own view.
    fn fill<S, T>(&self, controller: Controller<S, T>) -> Controller<S, T>
    where
        S: aether_scheduler::Scheduler,
        T: aether_controller::TaskTransport + Send + Sync,
    {
        for info in self.state.registry.lock().unwrap().nodes() {
            controller.register(info);
        }
        controller
    }
}

#[tokio::test]
async fn agent_registers_and_appears_in_the_registry() {
    let harness = Harness::start().await;
    let node_id = harness.attach_agent("rpi4").await;

    assert_eq!(
        harness
            .state
            .registry
            .lock()
            .unwrap()
            .get(node_id)
            .unwrap()
            .info
            .hostname,
        "rpi4"
    );
    assert!(harness.state.connections.is_connected(node_id));
}

#[tokio::test]
async fn the_agent_learns_the_eviction_window_it_is_held_to() {
    let state = MeshState::new().with_heartbeat_timeout(Duration::from_secs(60));
    let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    tokio::spawn(async move {
        let _ = serve(listener, state, SecurityConfig::open()).await;
    });

    let info = NodeInfo::new(NodeId::generate(), "laptop", "127.0.0.1:7001", 4);
    let client = AgentClient::connect(addr, info).await.unwrap();

    // Without this the agent cannot slow its heartbeats down safely: it would
    // be choosing a gap against an eviction deadline it cannot see.
    assert_eq!(client.heartbeat_timeout(), Duration::from_secs(60));
}

#[tokio::test]
async fn a_controller_that_declares_no_window_leaves_heartbeats_alone() {
    let harness = Harness::start().await;
    let info = NodeInfo::new(NodeId::generate(), "desktop", "127.0.0.1:7001", 4);
    let client = AgentClient::connect(harness.addr, info).await.unwrap();

    assert_eq!(client.heartbeat_timeout(), Duration::ZERO);
}

#[tokio::test]
async fn heartbeats_update_the_stored_metrics() {
    let harness = Harness::start().await;
    let info = NodeInfo::new(NodeId::generate(), "desktop", "127.0.0.1:7001", 4);
    let node_id = info.id;
    let client = AgentClient::connect(harness.addr, info).await.unwrap();
    harness
        .wait_until(|registry| registry.contains(node_id))
        .await;

    let metrics = NodeMetrics::new(0.75, 0.25, 8192);
    client.send_heartbeat(metrics).unwrap();

    harness
        .wait_until(|registry| {
            registry
                .get(node_id)
                .is_some_and(|entry| entry.info.metrics == metrics)
        })
        .await;
}

#[tokio::test]
async fn a_hash_task_runs_on_the_agent_and_comes_back() {
    let harness = Harness::start().await;
    let node_id = harness.attach_agent("worker").await;
    let controller = harness.controller();

    let payload = b"aethermesh".to_vec();
    let result = controller
        .submit(Task::new(kind::HASH, payload.clone()))
        .await
        .unwrap();

    assert_eq!(result.node_id, node_id);
    assert!(result.is_success());
    assert_eq!(
        result.output(),
        Some(blake3::hash(&payload).as_bytes().as_slice())
    );
}

#[tokio::test]
async fn a_cpu_task_returns_a_deterministic_result() {
    let harness = Harness::start().await;
    harness.attach_agent("worker").await;
    let controller = harness.controller();

    let payload = 100_000u64.to_le_bytes().to_vec();
    let first = controller
        .submit(Task::new(kind::CPU, payload.clone()))
        .await
        .unwrap();
    let second = controller
        .submit(Task::new(kind::CPU, payload))
        .await
        .unwrap();

    assert!(first.is_success());
    assert_eq!(first.output(), second.output());
}

#[tokio::test]
async fn an_unknown_task_kind_fails_on_the_agent_not_in_transit() {
    let harness = Harness::start().await;
    harness.attach_agent("worker").await;
    let controller = harness.controller();

    let result = controller
        .submit(Task::new("definitely-not-supported", Vec::new()))
        .await
        .unwrap();

    assert!(!result.is_success());
}

#[tokio::test]
async fn an_input_is_transferred_once_and_reused_by_later_tasks() {
    let harness = Harness::start().await;
    let node_id = harness.attach_agent("worker").await;
    let controller = harness.locality_controller();

    let dataset = vec![9u8; 32 * 1024];
    let descriptor = controller.publish(dataset.clone());

    let mut outputs = Vec::new();
    for _ in 0..3 {
        let task = Task::new(kind::HASH, b"seed".to_vec()).with_inputs(vec![descriptor.id]);
        let result = controller.submit(task).await.unwrap();
        assert!(result.is_success(), "task failed: {result:?}");
        outputs.push(result.output().unwrap().to_vec());
    }

    // Sent once, then skipped twice, and the agent hashed the real bytes.
    assert_eq!(controller.data_bytes_uncompressed(), dataset.len() as u64);
    assert_eq!(controller.transfers_skipped(), 2);
    assert!(harness.state.catalog.holds(descriptor.id, node_id));

    let mut expected = blake3::Hasher::new();
    expected.update(b"seed");
    expected.update(&dataset);
    assert_eq!(outputs[0], expected.finalize().as_bytes().to_vec());
    assert_eq!(outputs[1], outputs[0]);
    assert_eq!(outputs[2], outputs[0]);
}

#[tokio::test]
async fn a_node_that_hung_up_is_skipped_rather_than_tried_and_failed() {
    let harness = Harness::start().await;
    let (gone, handle) = harness.attach_agent_handle("gone").await;
    let alive = harness.attach_agent("alive").await;

    handle.abort();
    harness
        .wait_until(|_| !harness.state.connections.is_connected(gone))
        .await;

    // The registry still lists the dead node — the health monitor is
    // deliberately slow, because a late heartbeat is not a death. A closed
    // socket is not ambiguous, so dispatch should not spend an attempt on it.
    assert!(harness.state.registry.lock().unwrap().contains(gone));

    let mut controller = harness.controller();
    controller = controller.with_retry(RetryPolicy::none());

    let result = controller
        .submit(Task::new(kind::ECHO, b"hi".to_vec()))
        .await
        .unwrap();

    assert_eq!(result.node_id, alive);
    assert_eq!(
        controller.retries(),
        0,
        "no attempt was wasted on the dead node"
    );
}

#[tokio::test]
async fn a_node_over_its_storage_budget_drops_data_and_says_so() {
    let harness = Harness::start().await;

    // A budget that fits one dataset and not two.
    let info = NodeInfo::new(NodeId::generate(), "small-board", "127.0.0.1:7001", 4);
    let node_id = info.id;
    let mut client = AgentClient::connect(harness.addr, info)
        .await
        .unwrap()
        .with_storage_budget(48 * 1024);
    tokio::spawn(async move {
        let _ = client
            .run(MetricsCollector::new(), Duration::from_secs(60))
            .await;
    });
    harness
        .wait_until(|registry| registry.contains(node_id))
        .await;

    let controller = harness.locality_controller();
    let first = controller.publish(vec![1u8; 32 * 1024]);
    let second = controller.publish(vec![2u8; 32 * 1024]);

    for descriptor in [first, second] {
        let task = Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]);
        let result = controller.submit(task).await.unwrap();
        assert!(result.is_success(), "task failed: {result:?}");
    }

    // The second dataset pushed the first out, and the controller was told —
    // otherwise it would keep scoring this node as the cheapest place to run
    // work whose input it has thrown away.
    harness
        .wait_until(|_| !harness.state.catalog.holds(first.id, node_id))
        .await;
    assert!(harness.state.catalog.holds(second.id, node_id));
}

#[tokio::test]
async fn a_large_input_arrives_as_chunks_and_is_reassembled() {
    let harness = Harness::start().await;
    harness.attach_agent("worker").await;
    let controller = harness.chunked_controller(64 * 1024);

    let dataset: Vec<u8> = (0..(256 * 1024)).map(|i| (i % 251) as u8).collect();
    let descriptor = controller.publish(dataset.clone());
    assert_eq!(controller.manifest(descriptor.id).unwrap().len(), 4);

    let task = Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]);
    let result = controller.submit(task).await.unwrap();

    // The agent could only produce this hash from the fully reassembled data.
    assert!(result.is_success(), "task failed: {result:?}");
    assert_eq!(
        result.output(),
        Some(blake3::hash(&dataset).as_bytes().as_slice())
    );
    assert_eq!(controller.data_bytes_uncompressed(), dataset.len() as u64);
}

#[tokio::test]
async fn a_repeated_chunk_is_not_sent_again() {
    let harness = Harness::start().await;
    harness.attach_agent("worker").await;
    let controller = harness.chunked_controller(16 * 1024);

    // Four identical chunks share one content address.
    let dataset = vec![42u8; 64 * 1024];
    let descriptor = controller.publish(dataset.clone());
    let task = Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]);

    let result = controller.submit(task).await.unwrap();

    assert!(result.is_success(), "task failed: {result:?}");
    assert_eq!(
        result.output(),
        Some(blake3::hash(&dataset).as_bytes().as_slice())
    );
    assert_eq!(controller.data_bytes_uncompressed(), 16 * 1024);
    assert_eq!(controller.chunks_skipped(), 3);
    // Uniform bytes: the one chunk that travelled was compressed hard.
    assert!(controller.data_bytes_sent() < 1024);
}

#[tokio::test]
async fn a_disconnecting_node_loses_its_catalog_entries() {
    let harness = Harness::start().await;
    let (node_id, agent) = harness.attach_agent_handle("temporary").await;

    let controller = harness.locality_controller();
    let descriptor = controller.publish(vec![3u8; 64]);
    controller
        .submit(Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]))
        .await
        .unwrap();
    assert!(harness.state.catalog.holds(descriptor.id, node_id));

    agent.abort();
    for _ in 0..200 {
        if !harness.state.catalog.holds(descriptor.id, node_id) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("catalog still credits the data to a disconnected node");
}

#[tokio::test]
async fn submitting_without_a_connected_agent_fails() {
    let harness = Harness::start().await;
    let controller = harness.controller();
    controller.register(NodeInfo::new(NodeId::generate(), "ghost", "127.0.0.1:9", 1));

    let task = Task::new(kind::ECHO, Vec::new());
    let task_id = task.id;
    let error = controller.submit(task).await.unwrap_err();

    // A registered node with no connection is not a candidate, so the task is
    // refused outright rather than dispatched at a socket that is not there.
    assert_eq!(error, DispatchError::NoNodeAvailable(task_id));
    assert_eq!(controller.retries(), 0, "nothing was attempted");
}

#[tokio::test]
async fn a_node_that_dies_mid_dispatch_is_retried_elsewhere() {
    let harness = Harness::start().await;
    let (dying, handle) = harness.attach_agent_handle("dying").await;
    let alive = harness.attach_agent("alive").await;

    // Registered before the abort, so the controller's own view still lists it
    // as connected — this is the race the retry path exists for, as opposed to
    // the already-closed case above.
    let controller = harness.controller();
    handle.abort();
    harness
        .wait_until(|_| !harness.state.connections.is_connected(dying))
        .await;

    let result = controller
        .submit(Task::new(kind::ECHO, b"hi".to_vec()))
        .await
        .unwrap();

    assert_eq!(result.node_id, alive);
    assert_eq!(result.output(), Some(&b"hi"[..]));
}
