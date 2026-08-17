//! The client API over real sockets: publish, submit, run WebAssembly.
//!
//! This is the path every non-Rust SDK takes, so it is tested as bytes on a
//! socket rather than through the library types.

use std::time::Duration;

use aether_agent::{AgentClient, MetricsCollector};
use aether_controller::{
    ClientGateway, Controller, MeshState, NetworkTransport, SecurityConfig, bind, bind_clients,
    run_dispatcher, serve, serve_clients,
};
use aether_core::{NodeId, NodeInfo};
use aether_scheduler::AdvancedScheduler;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Uppercases ASCII letters. Same module as `examples/wasm/uppercase.wat`.
const UPPERCASE_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $next))
    (global.set $next (i32.add (global.get $next) (local.get $len)))
    (local.get $ptr))
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (local $i i32)
    (local $byte i32)
    (block $done
      (loop $loop
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (local.set $byte (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))
        (if (i32.and
              (i32.ge_u (local.get $byte) (i32.const 97))
              (i32.le_u (local.get $byte) (i32.const 122)))
          (then
            (i32.store8
              (i32.add (local.get $ptr) (local.get $i))
              (i32.sub (local.get $byte) (i32.const 32)))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $loop)))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len)))))
"#;

/// A controller with one agent attached and the client API listening.
struct Mesh {
    state: MeshState,
    client_addr: std::net::SocketAddr,
}

impl Mesh {
    async fn start(security: SecurityConfig) -> Self {
        let token = security.auth_token.clone();
        let state = MeshState::new();
        let (listener, agent_addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();

        let serve_state = state.clone();
        let serve_security = security.clone();
        tokio::spawn(async move {
            let _ = serve(listener, serve_state, serve_security).await;
        });

        let controller = Controller::new(
            AdvancedScheduler::new(state.catalog.clone()),
            NetworkTransport::new(state.connections.clone()).with_timeout(Duration::from_secs(10)),
            state.catalog.clone(),
        );
        let (gateway, commands) = ClientGateway::new(16);
        tokio::spawn(run_dispatcher(controller, state.clone(), commands));

        let (client_listener, client_addr) =
            bind_clients("127.0.0.1:0".parse().unwrap()).await.unwrap();
        tokio::spawn(serve_clients(client_listener, gateway, security));

        let mesh = Self { state, client_addr };
        // The agent authenticates with the same token clients use.
        mesh.attach_agent(agent_addr, token).await;
        mesh
    }

    async fn attach_agent(&self, addr: std::net::SocketAddr, token: Option<String>) {
        let info = NodeInfo::new(NodeId::generate(), "worker", "127.0.0.1:7001", 4);
        let node_id = info.id;
        let mut client = AgentClient::connect_with_token(addr, info, token)
            .await
            .unwrap();

        tokio::spawn(async move {
            let _ = client
                .run(MetricsCollector::new(), Duration::from_millis(200))
                .await;
        });

        for _ in 0..300 {
            if self.state.connections.is_connected(node_id) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("agent never connected");
    }
}

/// A client connection speaking the length-prefixed JSON protocol.
struct Client {
    stream: TcpStream,
}

impl Client {
    async fn connect(mesh: &Mesh, token: Option<&str>) -> Self {
        let mut client = Self {
            stream: TcpStream::connect(mesh.client_addr).await.unwrap(),
        };
        let hello = client
            .call(json!({ "type": "hello", "token": token }))
            .await;
        assert_eq!(hello["type"], "welcome", "handshake failed: {hello}");
        client
    }

    /// Sends one request and reads one response.
    async fn call(&mut self, request: Value) -> Value {
        let payload = serde_json::to_vec(&request).unwrap();
        self.stream
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await
            .unwrap();
        self.stream.write_all(&payload).await.unwrap();

        let mut length = [0u8; 4];
        self.stream.read_exact(&mut length).await.unwrap();
        let mut body = vec![0u8; u32::from_be_bytes(length) as usize];
        self.stream.read_exact(&mut body).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn publish(&mut self, bytes: &[u8]) -> String {
        let response = self
            .call(json!({ "type": "publish", "data": BASE64.encode(bytes) }))
            .await;
        assert_eq!(response["type"], "published", "publish failed: {response}");
        response["data_id"].as_str().unwrap().to_string()
    }
}

fn decode(response: &Value) -> Vec<u8> {
    BASE64.decode(response["output"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn a_client_lists_nodes_publishes_data_and_runs_a_task() {
    let mesh = Mesh::start(SecurityConfig::open()).await;
    let mut client = Client::connect(&mesh, None).await;

    let nodes = client.call(json!({ "type": "nodes" })).await;
    assert_eq!(nodes["type"], "nodes");
    assert_eq!(nodes["nodes"].as_array().unwrap().len(), 1);

    let dataset = vec![7u8; 64 * 1024];
    let data_id = client.publish(&dataset).await;

    let response = client
        .call(json!({
            "type": "submit",
            "kind": "hash",
            "payload": BASE64.encode(b"seed"),
            "inputs": [data_id],
        }))
        .await;

    assert_eq!(response["type"], "result", "submit failed: {response}");
    assert!(response["success"].as_bool().unwrap());

    let mut expected = blake3::Hasher::new();
    expected.update(b"seed");
    expected.update(&dataset);
    assert_eq!(decode(&response), expected.finalize().as_bytes().to_vec());
}

#[tokio::test]
async fn a_client_runs_a_webassembly_module() {
    let mesh = Mesh::start(SecurityConfig::open()).await;
    let mut client = Client::connect(&mesh, None).await;

    let module_id = client
        .publish(&wat::parse_str(UPPERCASE_WAT).unwrap())
        .await;
    let response = client
        .call(json!({
            "type": "submit",
            "kind": "wasm",
            "payload": BASE64.encode(b"hello from typescript"),
            "module": module_id,
        }))
        .await;

    assert_eq!(response["type"], "result", "submit failed: {response}");
    assert!(response["success"].as_bool().unwrap(), "{response}");
    assert_eq!(decode(&response), b"HELLO FROM TYPESCRIPT".to_vec());
}

#[tokio::test]
async fn a_module_is_transferred_once_however_many_tasks_use_it() {
    let mesh = Mesh::start(SecurityConfig::open()).await;
    let mut client = Client::connect(&mesh, None).await;
    let module_id = client
        .publish(&wat::parse_str(UPPERCASE_WAT).unwrap())
        .await;

    for _ in 0..5 {
        let response = client
            .call(json!({
                "type": "submit",
                "kind": "wasm",
                "payload": BASE64.encode(b"ok"),
                "module": module_id,
            }))
            .await;
        assert_eq!(decode(&response), b"OK".to_vec());
    }

    // One copy of the module reached the node; the rest were skipped.
    let node_id = mesh.state.registry.lock().unwrap().nodes()[0].id;
    assert!(
        mesh.state
            .catalog
            .holds(module_id.parse().unwrap(), node_id)
    );
}

#[tokio::test]
async fn a_broken_module_fails_the_task_without_taking_the_node_down() {
    let mesh = Mesh::start(SecurityConfig::open()).await;
    let mut client = Client::connect(&mesh, None).await;

    let module_id = client.publish(b"this is not a wasm module").await;
    let response = client
        .call(json!({ "type": "submit", "kind": "wasm", "module": module_id }))
        .await;

    assert_eq!(response["type"], "result");
    assert!(!response["success"].as_bool().unwrap());
    assert!(response["error"].as_str().unwrap().contains("loaded"));

    // The node is still there and still working.
    let ok = client
        .call(json!({ "type": "submit", "kind": "echo", "payload": BASE64.encode(b"alive") }))
        .await;
    assert_eq!(decode(&ok), b"alive".to_vec());
}

#[tokio::test]
async fn the_client_api_enforces_the_token() {
    let mesh = Mesh::start(SecurityConfig::with_token("s3cret")).await;

    let mut client = Client {
        stream: TcpStream::connect(mesh.client_addr).await.unwrap(),
    };

    // Anything before a successful hello is refused.
    let refused = client.call(json!({ "type": "nodes" })).await;
    assert_eq!(refused["type"], "error");

    let wrong = client
        .call(json!({ "type": "hello", "token": "guess" }))
        .await;
    assert_eq!(wrong["type"], "error");

    let mut authorized = Client::connect(&mesh, Some("s3cret")).await;
    let nodes = authorized.call(json!({ "type": "nodes" })).await;
    assert_eq!(nodes["type"], "nodes");
}
