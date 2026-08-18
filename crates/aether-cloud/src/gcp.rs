//! Google Cloud adapter (Compute Engine).
//!
//! Discovery lists instances in a zone; deploying a worker inserts one with the
//! agent in its startup script; metrics read the instance status. The token
//! comes from the metadata server, which is what a VM inside GCP already has.

use std::time::Duration;

use aether_core::{NodeId, NodeMetrics};
use serde::Deserialize;

use crate::http::{Credentials, HttpClient};
use crate::{CloudError, CloudProvider, CloudResource, DeployedWorker, WorkerSpec};

/// Where a VM asks for its own access token.
pub const METADATA_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/serviceAccounts/default/token";

/// What one instance needs to become a worker.
#[derive(Debug, Clone)]
pub struct InstanceTemplate {
    pub machine_type: String,
    pub source_image: String,
    /// Path to the agent binary on the image.
    pub agent_path: String,
    pub network: String,
}

impl InstanceTemplate {
    pub fn new(
        machine_type: impl Into<String>,
        source_image: impl Into<String>,
        agent_path: impl Into<String>,
    ) -> Self {
        Self {
            machine_type: machine_type.into(),
            source_image: source_image.into(),
            agent_path: agent_path.into(),
            network: "global/networks/default".to_string(),
        }
    }

    pub fn with_network(mut self, network: impl Into<String>) -> Self {
        self.network = network.into();
        self
    }
}

/// Compute Engine, reached over its REST API.
pub struct GcpProvider {
    name: String,
    client: HttpClient,
    project: String,
    zone: String,
    template: InstanceTemplate,
    workers: std::sync::Mutex<std::collections::HashMap<NodeId, DeployedWorker>>,
}

impl GcpProvider {
    pub fn new(
        project: impl Into<String>,
        zone: impl Into<String>,
        token: impl Into<String>,
        template: InstanceTemplate,
    ) -> Result<Self, CloudError> {
        Self::with_endpoint(
            "https://compute.googleapis.com/compute/v1",
            project,
            zone,
            token,
            template,
        )
    }

    /// Same, against another endpoint: an emulator, or a test double.
    pub fn with_endpoint(
        endpoint: impl Into<String>,
        project: impl Into<String>,
        zone: impl Into<String>,
        token: impl Into<String>,
        template: InstanceTemplate,
    ) -> Result<Self, CloudError> {
        Ok(Self {
            name: "gcp".to_string(),
            client: HttpClient::new(endpoint, Credentials::Bearer(token.into()), None)?,
            project: project.into(),
            zone: zone.into(),
            template,
            workers: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Fetches an access token from the metadata server.
    ///
    /// Only works from inside GCP, which is the point: no key file to leak.
    pub async fn token_from_metadata() -> Result<String, CloudError> {
        #[derive(Deserialize)]
        struct Token {
            access_token: String,
        }

        let response = reqwest::Client::new()
            .get(METADATA_TOKEN_URL)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|error| CloudError::Request(error.to_string()))?;
        let token: Token = response
            .json()
            .await
            .map_err(|error| CloudError::Request(error.to_string()))?;

        Ok(token.access_token)
    }

    fn instance_body(&self, name: &str, spec: &WorkerSpec) -> serde_json::Value {
        let startup = format!(
            "#!/bin/sh\nexec {} --controller {} --heartbeat-secs {}\n",
            self.template.agent_path, spec.controller_address, spec.heartbeat_secs
        );

        serde_json::json!({
            "name": name,
            "machineType": format!("zones/{}/machineTypes/{}", self.zone, self.template.machine_type),
            "disks": [{
                "boot": true,
                "autoDelete": true,
                "initializeParams": { "sourceImage": self.template.source_image }
            }],
            "networkInterfaces": [{ "network": self.template.network }],
            "metadata": {
                "items": [{ "key": "startup-script", "value": startup }]
            },
            "labels": { "app": "aether-agent" }
        })
    }
}

#[derive(Debug, Deserialize)]
struct InstanceList {
    #[serde(default)]
    items: Vec<Instance>,
    /// GCP pages with a token in the body.
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// A GCP create call answers with an operation, not the instance.
#[derive(Debug, Deserialize)]
struct Operation {
    #[serde(default)]
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct Instance {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "machineType")]
    machine_type: String,
    #[serde(default)]
    zone: String,
    #[serde(default)]
    status: Option<String>,
}

/// GCP returns fully qualified URLs; the useful part is the last segment.
fn last_segment(value: &str) -> &str {
    value.rsplit('/').next().unwrap_or(value)
}

/// `e2-standard-4` and `n1-highcpu-8` end in their core count.
fn cores_from_machine_type(machine_type: &str) -> u32 {
    last_segment(machine_type)
        .rsplit('-')
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
}

impl CloudProvider for GcpProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn discover_resources(&self) -> Result<Vec<CloudResource>, CloudError> {
        let pages: Vec<InstanceList> = self
            .client
            .get_all_pages(
                &format!("/projects/{}/zones/{}/instances", self.project, self.zone),
                |page: &InstanceList| page.next_page_token.clone(),
                |path, token| format!("{path}?pageToken={token}"),
            )
            .await?;

        Ok(pages
            .into_iter()
            .flat_map(|page| page.items)
            .filter(|instance| instance.status.as_deref() == Some("RUNNING"))
            .map(|instance| {
                let machine_type = last_segment(&instance.machine_type).to_string();
                CloudResource::new(
                    instance.name,
                    last_segment(&instance.zone),
                    cores_from_machine_type(&machine_type),
                )
                .with_class(machine_type)
                .with_label("provider", "gcp")
                .with_label("project", &self.project)
            })
            .collect())
    }

    async fn deploy_worker(
        &self,
        resource_id: &str,
        spec: &WorkerSpec,
    ) -> Result<DeployedWorker, CloudError> {
        let node_id = NodeId::generate();
        let name = format!("aether-agent-{}", &node_id.to_string()[..8]);

        let operation: Operation = self
            .client
            .post_json(
                &format!("/projects/{}/zones/{}/instances", self.project, self.zone),
                &self.instance_body(&name, spec),
            )
            .await
            .map_err(|error| CloudError::DeployFailed {
                resource: resource_id.to_string(),
                reason: error.to_string(),
            })?;

        // The instance exists only once the operation is DONE; returning before
        // that would hand back a worker that does not exist yet.
        if operation.status.as_deref() != Some("DONE") && !operation.name.is_empty() {
            let finished: Operation = self
                .client
                .poll_until(
                    &format!(
                        "/projects/{}/zones/{}/operations/{}",
                        self.project, self.zone, operation.name
                    ),
                    |operation: &Operation| operation.status.as_deref() == Some("DONE"),
                    Duration::from_millis(500),
                    Duration::from_secs(180),
                )
                .await
                .map_err(|error| CloudError::DeployFailed {
                    resource: resource_id.to_string(),
                    reason: error.to_string(),
                })?;

            if let Some(error) = finished.error {
                return Err(CloudError::DeployFailed {
                    resource: resource_id.to_string(),
                    reason: error.to_string(),
                });
            }
        }

        let worker = DeployedWorker {
            node_id,
            resource_id: name,
            address: spec.controller_address.clone(),
        };
        aether_core::lock(&self.workers).insert(node_id, worker.clone());
        Ok(worker)
    }

    async fn get_metrics(&self, node_id: NodeId) -> Result<NodeMetrics, CloudError> {
        let name = aether_core::lock(&self.workers)
            .get(&node_id)
            .map(|worker| worker.resource_id.clone())
            .ok_or_else(|| CloudError::UnknownResource(node_id.to_string()))?;

        let instance: Instance = self
            .client
            .get_json(&format!(
                "/projects/{}/zones/{}/instances/{name}",
                self.project, self.zone
            ))
            .await?;

        match instance.status.as_deref() {
            Some("RUNNING") | Some("PROVISIONING") | Some("STAGING") => Ok(NodeMetrics::default()),
            Some(status) => Err(CloudError::Request(format!("instance is {status}"))),
            None => Ok(NodeMetrics::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockServer;

    const INSTANCES: &str = r#"{
        "items": [
            {
                "name": "worker-1",
                "machineType": "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/machineTypes/e2-standard-4",
                "zone": "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a",
                "status": "RUNNING"
            },
            {
                "name": "stopped-one",
                "machineType": "zones/us-central1-a/machineTypes/e2-medium",
                "zone": "zones/us-central1-a",
                "status": "TERMINATED"
            }
        ]
    }"#;

    async fn provider(server: &MockServer) -> GcpProvider {
        GcpProvider::with_endpoint(
            server.base_url(),
            "my-project",
            "us-central1-a",
            "ya29.token",
            InstanceTemplate::new(
                "e2-standard-4",
                "projects/debian-cloud/global/images/family/debian-12",
                "/usr/local/bin/aether-agent",
            ),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn discovery_lists_running_instances() {
        let server = MockServer::start(vec![(200, INSTANCES.to_string())]).await;
        let resources = provider(&server).await.discover_resources().await.unwrap();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].id, "worker-1");
        assert_eq!(resources[0].region, "us-central1-a");
        assert_eq!(resources[0].class, "e2-standard-4");
        assert_eq!(resources[0].cpu_cores, 4);

        let request = &server.requests().await[0];
        assert_eq!(
            request.path,
            "/projects/my-project/zones/us-central1-a/instances"
        );
        assert_eq!(
            request.header("authorization").as_deref(),
            Some("Bearer ya29.token")
        );
    }

    #[tokio::test]
    async fn deploying_waits_for_the_operation_to_finish() {
        let server = MockServer::start(vec![
            (200, r#"{"name":"op-1","status":"RUNNING"}"#.to_string()),
            (200, r#"{"name":"op-1","status":"DONE"}"#.to_string()),
        ])
        .await;

        provider(&server)
            .await
            .deploy_worker("worker-1", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap();

        let requests = server.requests().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].path,
            "/projects/my-project/zones/us-central1-a/operations/op-1"
        );
    }

    #[tokio::test]
    async fn an_operation_that_fails_is_a_deploy_failure() {
        let server = MockServer::start(vec![
            (200, r#"{"name":"op-1","status":"RUNNING"}"#.to_string()),
            (
                200,
                r#"{"name":"op-1","status":"DONE","error":{"errors":[{"code":"QUOTA_EXCEEDED"}]}}"#
                    .to_string(),
            ),
        ])
        .await;

        let error = provider(&server)
            .await
            .deploy_worker("worker-1", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("QUOTA_EXCEEDED"), "{error}");
    }

    #[tokio::test]
    async fn discovery_follows_the_page_token() {
        let first = r#"{"items":[{"name":"a","machineType":"z/machineTypes/e2-standard-2","zone":"zones/z","status":"RUNNING"}],"nextPageToken":"tok"}"#;
        let second = r#"{"items":[{"name":"b","machineType":"z/machineTypes/e2-standard-2","zone":"zones/z","status":"RUNNING"}]}"#;
        let server =
            MockServer::start(vec![(200, first.to_string()), (200, second.to_string())]).await;

        let resources = provider(&server).await.discover_resources().await.unwrap();

        assert_eq!(resources.len(), 2);
        assert!(
            server.requests().await[1].path.contains("pageToken=tok"),
            "{}",
            server.requests().await[1].path
        );
    }

    #[tokio::test]
    async fn deploying_inserts_an_instance_with_a_startup_script() {
        let server =
            MockServer::start(vec![(200, r#"{"name":"op","status":"DONE"}"#.to_string())]).await;

        let worker = provider(&server)
            .await
            .deploy_worker("worker-1", &WorkerSpec::new("mesh.example.com:7000"))
            .await
            .unwrap();

        assert!(worker.resource_id.starts_with("aether-agent-"));

        let request = &server.requests().await[0];
        assert_eq!(request.method, "POST");
        let body: serde_json::Value = serde_json::from_str(&request.body).unwrap();
        assert!(
            body["machineType"]
                .as_str()
                .unwrap()
                .ends_with("machineTypes/e2-standard-4")
        );
        let startup = body["metadata"]["items"][0]["value"].as_str().unwrap();
        assert!(
            startup.contains("--controller mesh.example.com:7000"),
            "{startup}"
        );
    }

    #[tokio::test]
    async fn metrics_read_the_instance_status() {
        let server = MockServer::start(vec![
            (200, r#"{"name":"op","status":"DONE"}"#.to_string()),
            (200, r#"{"name":"w","status":"RUNNING"}"#.to_string()),
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
    }

    #[tokio::test]
    async fn a_terminated_instance_is_reported_as_an_error() {
        let server = MockServer::start(vec![
            (200, r#"{"name":"op","status":"DONE"}"#.to_string()),
            (200, r#"{"name":"w","status":"TERMINATED"}"#.to_string()),
        ])
        .await;
        let provider = provider(&server).await;
        let worker = provider
            .deploy_worker("worker-1", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap();

        let error = provider.get_metrics(worker.node_id).await.unwrap_err();
        assert!(error.to_string().contains("TERMINATED"), "{error}");
    }

    #[test]
    fn machine_types_give_up_their_core_count() {
        assert_eq!(cores_from_machine_type("e2-standard-4"), 4);
        assert_eq!(
            cores_from_machine_type("zones/z/machineTypes/n1-highcpu-16"),
            16
        );
        assert_eq!(cores_from_machine_type("custom"), 1);
    }
}
