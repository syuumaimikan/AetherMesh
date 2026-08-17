//! Kubernetes adapter.
//!
//! Discovery lists schedulable nodes; deploying a worker creates a Pod running
//! the agent image; metrics read the Pod's phase. Credentials are the ones a
//! Pod already has — the mounted service account token and CA — so nothing has
//! to be configured when the controller runs inside the cluster.

use aether_core::{NodeId, NodeMetrics};
use serde::Deserialize;

use crate::http::{Credentials, HttpClient};
use crate::{CloudError, CloudProvider, CloudResource, DeployedWorker, WorkerSpec};

/// Where a Pod finds its service account.
const TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

/// Kubernetes, reached over its REST API.
pub struct KubernetesProvider {
    name: String,
    client: HttpClient,
    namespace: String,
    /// Image the agent runs from.
    image: String,
    workers: std::sync::Mutex<std::collections::HashMap<NodeId, DeployedWorker>>,
}

impl KubernetesProvider {
    /// Builds a provider from an explicit endpoint and token.
    pub fn new(
        api_url: impl Into<String>,
        token: impl Into<String>,
        namespace: impl Into<String>,
        image: impl Into<String>,
        ca_certificate: Option<&[u8]>,
    ) -> Result<Self, CloudError> {
        Ok(Self {
            name: "kubernetes".to_string(),
            client: HttpClient::new(api_url, Credentials::Bearer(token.into()), ca_certificate)?,
            namespace: namespace.into(),
            image: image.into(),
            workers: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Builds a provider from the service account mounted into this Pod.
    ///
    /// This is the path that needs no configuration: in-cluster, the token,
    /// the CA, and the API address are all already there.
    pub fn in_cluster(
        namespace: impl Into<String>,
        image: impl Into<String>,
    ) -> Result<Self, CloudError> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST")
            .map_err(|_| CloudError::Request("KUBERNETES_SERVICE_HOST is not set".to_string()))?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".to_string());

        let token = std::fs::read_to_string(TOKEN_PATH)
            .map_err(|error| CloudError::Request(format!("{TOKEN_PATH}: {error}")))?;
        let ca = std::fs::read(CA_PATH)
            .map_err(|error| CloudError::Request(format!("{CA_PATH}: {error}")))?;

        Self::new(
            format!("https://{host}:{port}"),
            token.trim(),
            namespace,
            image,
            Some(&ca),
        )
    }

    /// The Pod manifest one worker runs from.
    fn pod_manifest(&self, name: &str, spec: &WorkerSpec) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "labels": { "app": "aether-agent" }
            },
            "spec": {
                // A worker that dies should not be resurrected by Kubernetes
                // with a stale identity; the controller notices and reschedules.
                "restartPolicy": "Never",
                "containers": [{
                    "name": "aether-agent",
                    "image": self.image,
                    "args": [
                        "--controller", spec.controller_address,
                        "--heartbeat-secs", spec.heartbeat_secs.to_string(),
                    ]
                }]
            }
        })
    }
}

/// The parts of a node list this adapter reads.
#[derive(Debug, Deserialize)]
struct NodeList {
    #[serde(default)]
    items: Vec<Node>,
    /// Kubernetes pages with a continue token in the list metadata.
    #[serde(default)]
    metadata: ListMetadata,
}

#[derive(Debug, Default, Deserialize)]
struct ListMetadata {
    #[serde(default, rename = "continue")]
    continue_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Node {
    metadata: Metadata,
    #[serde(default)]
    status: NodeStatus,
    #[serde(default)]
    spec: NodeSpec,
}

#[derive(Debug, Default, Deserialize)]
struct NodeSpec {
    /// A node with taints is not simply schedulable; treat it as unavailable.
    #[serde(default)]
    taints: Vec<serde_json::Value>,
    #[serde(default)]
    unschedulable: bool,
}

#[derive(Debug, Deserialize)]
struct Metadata {
    #[serde(default)]
    name: String,
    #[serde(default)]
    labels: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
struct NodeStatus {
    #[serde(default)]
    capacity: Capacity,
    #[serde(default)]
    phase: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct Capacity {
    #[serde(default)]
    cpu: Option<String>,
    #[serde(default)]
    memory: Option<String>,
}

/// Kubernetes reports CPU as a count or in millicores (`"4"`, `"3800m"`).
fn parse_cpu(value: Option<&String>) -> u32 {
    let Some(value) = value else { return 1 };
    match value.strip_suffix('m') {
        Some(millis) => millis
            .parse::<u32>()
            .map(|millis| (millis / 1000).max(1))
            .unwrap_or(1),
        None => value.parse().unwrap_or(1),
    }
}

/// Memory comes with a binary suffix (`"16265652Ki"`).
fn parse_memory(value: Option<&String>) -> u64 {
    let Some(value) = value else { return 0 };
    let (digits, multiplier) = match value {
        v if v.ends_with("Ki") => (&v[..v.len() - 2], 1024),
        v if v.ends_with("Mi") => (&v[..v.len() - 2], 1024 * 1024),
        v if v.ends_with("Gi") => (&v[..v.len() - 2], 1024 * 1024 * 1024),
        v => (v.as_str(), 1),
    };
    digits.parse::<u64>().unwrap_or(0) * multiplier
}

impl CloudProvider for KubernetesProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn discover_resources(&self) -> Result<Vec<CloudResource>, CloudError> {
        // A large cluster answers in pages; a small one answers in one.
        let pages: Vec<NodeList> = self
            .client
            .get_all_pages(
                "/api/v1/nodes",
                |page: &NodeList| page.metadata.continue_token.clone(),
                |path, token| format!("{path}?continue={token}"),
            )
            .await?;

        Ok(pages
            .into_iter()
            .flat_map(|page| page.items)
            .filter(|node| !node.spec.unschedulable && node.spec.taints.is_empty())
            .map(|node| {
                let region = node
                    .metadata
                    .labels
                    .get("topology.kubernetes.io/region")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let class = node
                    .metadata
                    .labels
                    .get("node.kubernetes.io/instance-type")
                    .cloned()
                    .unwrap_or_else(|| "node".to_string());

                CloudResource::new(
                    node.metadata.name,
                    region,
                    parse_cpu(node.status.capacity.cpu.as_ref()),
                )
                .with_class(class)
                .with_memory(parse_memory(node.status.capacity.memory.as_ref()))
                .with_label("provider", "kubernetes")
            })
            .collect())
    }

    async fn deploy_worker(
        &self,
        resource_id: &str,
        spec: &WorkerSpec,
    ) -> Result<DeployedWorker, CloudError> {
        let node_id = NodeId::generate();
        // The Pod name carries the identity, so a stray Pod can be traced back.
        let pod_name = format!("aether-agent-{}", &node_id.to_string()[..8]);
        let manifest = self.pod_manifest(&pod_name, spec);

        let _: serde_json::Value = self
            .client
            .post_json(
                &format!("/api/v1/namespaces/{}/pods", self.namespace),
                &manifest,
            )
            .await
            .map_err(|error| CloudError::DeployFailed {
                resource: resource_id.to_string(),
                reason: error.to_string(),
            })?;

        let worker = DeployedWorker {
            node_id,
            resource_id: pod_name,
            address: spec.controller_address.clone(),
        };
        self.workers
            .lock()
            .expect("workers mutex poisoned")
            .insert(node_id, worker.clone());
        Ok(worker)
    }

    async fn get_metrics(&self, node_id: NodeId) -> Result<NodeMetrics, CloudError> {
        let pod_name = self
            .workers
            .lock()
            .expect("workers mutex poisoned")
            .get(&node_id)
            .map(|worker| worker.resource_id.clone())
            .ok_or_else(|| CloudError::UnknownResource(node_id.to_string()))?;

        let pod: Node = self
            .client
            .get_json(&format!(
                "/api/v1/namespaces/{}/pods/{pod_name}",
                self.namespace
            ))
            .await?;

        // Kubernetes reports liveness, not utilisation; the agent's heartbeats
        // carry the real numbers.
        match pod.status.phase.as_deref() {
            Some("Running") | Some("Pending") => Ok(NodeMetrics::default()),
            Some(phase) => Err(CloudError::Request(format!("pod is {phase}"))),
            None => Ok(NodeMetrics::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockServer;

    const NODE_LIST: &str = r#"{
        "items": [
            {
                "metadata": {
                    "name": "worker-1",
                    "labels": {
                        "topology.kubernetes.io/region": "eu-west-1",
                        "node.kubernetes.io/instance-type": "m5.large"
                    }
                },
                "status": { "capacity": { "cpu": "4", "memory": "16265652Ki" } },
                "spec": {}
            },
            {
                "metadata": { "name": "worker-2", "labels": {} },
                "status": { "capacity": { "cpu": "3800m", "memory": "8Gi" } },
                "spec": {}
            },
            {
                "metadata": { "name": "control-plane", "labels": {} },
                "status": { "capacity": { "cpu": "8" } },
                "spec": { "taints": [{ "key": "node-role.kubernetes.io/control-plane" }] }
            }
        ]
    }"#;

    async fn provider(server: &MockServer) -> KubernetesProvider {
        KubernetesProvider::new(
            server.base_url(),
            "service-account-token",
            "aethermesh",
            "ghcr.io/example/aether-agent:latest",
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn discovery_lists_schedulable_nodes() {
        let server = MockServer::start(vec![(200, NODE_LIST.to_string())]).await;
        let resources = provider(&server).await.discover_resources().await.unwrap();

        // The tainted control plane is left out.
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].id, "worker-1");
        assert_eq!(resources[0].region, "eu-west-1");
        assert_eq!(resources[0].class, "m5.large");
        assert_eq!(resources[0].cpu_cores, 4);
        assert_eq!(resources[0].memory_bytes, 16_265_652 * 1024);
        // Millicores round down to whole cores.
        assert_eq!(resources[1].cpu_cores, 3);
        assert_eq!(resources[1].memory_bytes, 8 * 1024 * 1024 * 1024);

        let request = &server.requests().await[0];
        assert_eq!(request.path, "/api/v1/nodes");
        assert_eq!(
            request.header("authorization").as_deref(),
            Some("Bearer service-account-token")
        );
    }

    #[tokio::test]
    async fn deploying_creates_a_pod_running_the_agent() {
        let server =
            MockServer::start(vec![(201, r#"{"metadata":{"name":"ok"}}"#.to_string())]).await;
        let provider = provider(&server).await;

        let worker = provider
            .deploy_worker("worker-1", &WorkerSpec::new("mesh.example.com:7000"))
            .await
            .unwrap();

        assert!(worker.resource_id.starts_with("aether-agent-"));

        let request = &server.requests().await[0];
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/api/v1/namespaces/aethermesh/pods");
        let body: serde_json::Value = serde_json::from_str(&request.body).unwrap();
        assert_eq!(body["kind"], "Pod");
        assert_eq!(
            body["spec"]["containers"][0]["image"],
            "ghcr.io/example/aether-agent:latest"
        );
        assert_eq!(
            body["spec"]["containers"][0]["args"][1],
            "mesh.example.com:7000"
        );
    }

    #[tokio::test]
    async fn a_rejected_pod_is_a_deploy_failure() {
        let server = MockServer::start(vec![(403, r#"{"message":"forbidden"}"#.to_string())]).await;

        let error = provider(&server)
            .await
            .deploy_worker("worker-1", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap_err();

        assert!(matches!(error, CloudError::DeployFailed { .. }), "{error}");
    }

    #[tokio::test]
    async fn metrics_read_the_pod_phase() {
        let server = MockServer::start(vec![
            (201, r#"{"metadata":{"name":"ok"}}"#.to_string()),
            (
                200,
                r#"{"metadata":{"name":"p"},"status":{"phase":"Running"}}"#.to_string(),
            ),
        ])
        .await;
        let provider = provider(&server).await;

        let worker = provider
            .deploy_worker("worker-1", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap();
        assert_eq!(
            provider.get_metrics(worker.node_id).await.unwrap(),
            NodeMetrics::default()
        );

        let path = &server.requests().await[1].path;
        assert!(
            path.starts_with("/api/v1/namespaces/aethermesh/pods/aether-agent-"),
            "{path}"
        );
    }

    #[tokio::test]
    async fn a_failed_pod_is_reported_as_an_error() {
        let server = MockServer::start(vec![
            (201, r#"{"metadata":{"name":"ok"}}"#.to_string()),
            (
                200,
                r#"{"metadata":{"name":"p"},"status":{"phase":"Failed"}}"#.to_string(),
            ),
        ])
        .await;
        let provider = provider(&server).await;
        let worker = provider
            .deploy_worker("worker-1", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap();

        let error = provider.get_metrics(worker.node_id).await.unwrap_err();
        assert!(error.to_string().contains("Failed"), "{error}");
    }

    #[tokio::test]
    async fn discovery_follows_the_continue_token() {
        let first = r#"{
            "items": [{ "metadata": { "name": "page-1" }, "status": {}, "spec": {} }],
            "metadata": { "continue": "token-2" }
        }"#;
        let second = r#"{
            "items": [{ "metadata": { "name": "page-2" }, "status": {}, "spec": {} }],
            "metadata": {}
        }"#;
        let server =
            MockServer::start(vec![(200, first.to_string()), (200, second.to_string())]).await;

        let resources = provider(&server).await.discover_resources().await.unwrap();

        assert_eq!(resources.len(), 2);
        assert_eq!(resources[1].id, "page-2");
        let requests = server.requests().await;
        assert_eq!(requests[1].path, "/api/v1/nodes?continue=token-2");
    }

    #[tokio::test]
    async fn a_throttled_request_is_retried() {
        let server = MockServer::start(vec![
            (429, r#"{"message":"slow down"}"#.to_string()),
            (200, NODE_LIST.to_string()),
        ])
        .await;

        let resources = provider(&server).await.discover_resources().await.unwrap();

        assert_eq!(resources.len(), 2);
        // Two requests: the throttled one and the retry.
        assert_eq!(server.requests().await.len(), 2);
    }

    #[tokio::test]
    async fn rejected_credentials_are_not_retried() {
        let server = MockServer::start(vec![(403, r#"{"message":"denied"}"#.to_string())]).await;

        let error = provider(&server)
            .await
            .discover_resources()
            .await
            .unwrap_err();

        assert!(matches!(error, CloudError::Unauthorized { .. }), "{error}");
        assert_eq!(server.requests().await.len(), 1);
    }

    #[tokio::test]
    async fn metrics_for_an_unknown_worker_fail() {
        let server = MockServer::start(vec![(200, "{}".to_string())]).await;
        assert!(
            provider(&server)
                .await
                .get_metrics(NodeId::generate())
                .await
                .is_err()
        );
    }
}
