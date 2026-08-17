//! Chunks over several connections at once.
//!
//! The point of the extra connections is throughput; the point of these tests
//! is that correctness does not depend on them. A dataset split across four
//! sockets has no ordering guarantee against the task that reads it, so the
//! agent confirms assembly and the controller waits for that confirmation.

use std::time::Duration;

use aether_agent::{AgentClient, MetricsCollector};
use aether_controller::{Controller, MeshState, NetworkTransport, SecurityConfig, bind, serve};
use aether_core::task::kind;
use aether_core::{NodeId, NodeInfo, Task};
use aether_scheduler::LocalityScheduler;

struct Mesh {
    state: MeshState,
    addr: std::net::SocketAddr,
}

impl Mesh {
    async fn start() -> Self {
        let state = MeshState::new();
        let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();

        let serve_state = state.clone();
        tokio::spawn(async move {
            let _ = serve(listener, serve_state, SecurityConfig::open()).await;
        });
        Self { state, addr }
    }

    /// Attaches an agent with `channels` extra bulk-data connections.
    async fn attach_agent(&self, channels: usize) -> NodeId {
        let info = NodeInfo::new(NodeId::generate(), "worker", "127.0.0.1:7001", 4);
        let node_id = info.id;
        let mut client = AgentClient::connect(self.addr, info).await.unwrap();

        let handles = client
            .open_data_channels(self.addr, channels, None)
            .await
            .unwrap();

        tokio::spawn(async move {
            let _handles = handles;
            let _ = client
                .run(MetricsCollector::new(), Duration::from_millis(200))
                .await;
        });

        self.wait_until(|state| {
            state.connections.is_connected(node_id)
                && state.connections.data_channel_count(node_id) == channels
        })
        .await;
        node_id
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

    fn controller(&self, chunk_size: usize) -> Controller<LocalityScheduler, NetworkTransport> {
        let mut controller = Controller::new(
            LocalityScheduler::new(self.state.catalog.clone()),
            NetworkTransport::new(self.state.connections.clone())
                .with_timeout(Duration::from_secs(10)),
            self.state.catalog.clone(),
        )
        .with_chunk_size(chunk_size);

        for info in self.state.registry.lock().unwrap().nodes() {
            controller.registry_mut().register(info);
        }
        controller
    }
}

/// Repetitive but not uniform, so every chunk has a distinct hash.
fn dataset(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i / 64 % 251) as u8).collect()
}

#[tokio::test]
async fn a_dataset_split_across_four_connections_arrives_intact() {
    let mesh = Mesh::start().await;
    let node_id = mesh.attach_agent(4).await;
    let mut controller = mesh.controller(64 * 1024);

    let data = dataset(1024 * 1024);
    let descriptor = controller.publish(data.clone());
    assert_eq!(controller.manifest(descriptor.id).unwrap().len(), 16);

    let task = Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]);
    let result = controller.submit(task).await.unwrap();

    // Only a fully reassembled dataset hashes to this.
    assert!(result.is_success(), "task failed: {result:?}");
    assert_eq!(result.node_id, node_id);
    assert_eq!(
        result.output(),
        Some(blake3::hash(&data).as_bytes().as_slice())
    );
    assert_eq!(controller.data_bytes_uncompressed(), data.len() as u64);
}

#[tokio::test]
async fn the_same_transfer_works_without_extra_connections() {
    let mesh = Mesh::start().await;
    mesh.attach_agent(0).await;
    let mut controller = mesh.controller(64 * 1024);

    let data = dataset(256 * 1024);
    let descriptor = controller.publish(data.clone());
    let result = controller
        .submit(Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]))
        .await
        .unwrap();

    assert!(result.is_success(), "task failed: {result:?}");
    assert_eq!(
        result.output(),
        Some(blake3::hash(&data).as_bytes().as_slice())
    );
}

#[tokio::test]
async fn a_reused_dataset_is_not_resent_over_the_channels() {
    let mesh = Mesh::start().await;
    mesh.attach_agent(2).await;
    let mut controller = mesh.controller(32 * 1024);

    let data = dataset(128 * 1024);
    let descriptor = controller.publish(data.clone());

    for _ in 0..3 {
        let result = controller
            .submit(Task::new(kind::HASH, Vec::new()).with_inputs(vec![descriptor.id]))
            .await
            .unwrap();
        assert!(result.is_success(), "task failed: {result:?}");
    }

    // Sent once; the next two submissions skipped the whole dataset.
    assert_eq!(controller.data_bytes_uncompressed(), data.len() as u64);
    assert_eq!(controller.transfers_skipped(), 2);
}

#[tokio::test]
async fn a_stranger_cannot_attach_a_data_channel_to_someone_elses_node() {
    use aether_protocol::{Message, read_message, write_message};
    use tokio::net::TcpStream;

    let mesh = Mesh::start().await;
    let victim = mesh.attach_agent(1).await;
    assert_eq!(mesh.state.connections.data_channel_count(victim), 1);

    // An attacker who knows the node id — it appears in logs and in the client
    // API — but not the channel token issued to that node at registration.
    let mut stream = TcpStream::connect(mesh.addr).await.unwrap();
    let (mut reader, mut writer) = stream.split();
    write_message(
        &mut writer,
        &Message::register_data_channel(victim, Some("guessed".to_string())),
    )
    .await
    .unwrap();

    // The controller closes the connection instead of attaching it.
    assert!(read_message(&mut reader).await.is_err());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        mesh.state.connections.data_channel_count(victim),
        1,
        "a stranger attached a data channel and would receive the node's data"
    );
}

#[tokio::test]
async fn a_data_channel_without_a_token_is_refused() {
    use aether_protocol::{Message, read_message, write_message};
    use tokio::net::TcpStream;

    let mesh = Mesh::start().await;
    let victim = mesh.attach_agent(0).await;

    let mut stream = TcpStream::connect(mesh.addr).await.unwrap();
    let (mut reader, mut writer) = stream.split();
    write_message(&mut writer, &Message::register_data_channel(victim, None))
        .await
        .unwrap();

    assert!(read_message(&mut reader).await.is_err());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(mesh.state.connections.data_channel_count(victim), 0);
}

#[tokio::test]
async fn data_channels_are_counted_and_released_with_the_node() {
    let mesh = Mesh::start().await;
    let node_id = mesh.attach_agent(3).await;

    assert_eq!(mesh.state.connections.data_channel_count(node_id), 3);

    // Evicting the node takes its channels with it.
    aether_controller::evict_stale_nodes(&mesh.state, Duration::ZERO);
    assert_eq!(mesh.state.connections.data_channel_count(node_id), 0);
}
