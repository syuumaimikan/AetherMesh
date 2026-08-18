//! Azure adapter (virtual machines).
//!
//! Discovery lists VMs in a resource group; deploying a worker creates one with
//! the agent in its custom data; metrics read the VM's provisioning state. The
//! token comes from the instance metadata service, which is what a VM inside
//! Azure already has.

use aether_core::{NodeId, NodeMetrics};
use serde::Deserialize;

use crate::http::{Credentials, HttpClient};
use crate::{CloudError, CloudProvider, CloudResource, DeployedWorker, WorkerSpec};

/// Where a VM asks for its own access token.
pub const IMDS_TOKEN_URL: &str = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/";

/// API version this adapter speaks.
const API_VERSION: &str = "2023-09-01";

/// What one VM needs to become a worker.
#[derive(Debug, Clone)]
pub struct VmTemplate {
    pub size: String,
    pub image_reference: serde_json::Value,
    pub admin_username: String,
    /// Path to the agent binary on the image.
    pub agent_path: String,
    pub subnet_id: String,
    pub location: String,
}

impl VmTemplate {
    pub fn new(
        size: impl Into<String>,
        location: impl Into<String>,
        subnet_id: impl Into<String>,
        agent_path: impl Into<String>,
    ) -> Self {
        Self {
            size: size.into(),
            image_reference: serde_json::json!({
                "publisher": "Canonical",
                "offer": "0001-com-ubuntu-server-jammy",
                "sku": "22_04-lts-gen2",
                "version": "latest"
            }),
            admin_username: "aether".to_string(),
            agent_path: agent_path.into(),
            subnet_id: subnet_id.into(),
            location: location.into(),
        }
    }

    pub fn with_image(mut self, image_reference: serde_json::Value) -> Self {
        self.image_reference = image_reference;
        self
    }
}

/// Azure virtual machines, reached over the management API.
pub struct AzureProvider {
    name: String,
    client: HttpClient,
    subscription_id: String,
    resource_group: String,
    template: VmTemplate,
    workers: std::sync::Mutex<std::collections::HashMap<NodeId, DeployedWorker>>,
}

impl AzureProvider {
    pub fn new(
        subscription_id: impl Into<String>,
        resource_group: impl Into<String>,
        token: impl Into<String>,
        template: VmTemplate,
    ) -> Result<Self, CloudError> {
        Self::with_endpoint(
            "https://management.azure.com",
            subscription_id,
            resource_group,
            token,
            template,
        )
    }

    /// Same, against another endpoint — an emulator, or a test double.
    pub fn with_endpoint(
        endpoint: impl Into<String>,
        subscription_id: impl Into<String>,
        resource_group: impl Into<String>,
        token: impl Into<String>,
        template: VmTemplate,
    ) -> Result<Self, CloudError> {
        Ok(Self {
            name: "azure".to_string(),
            client: HttpClient::new(endpoint, Credentials::Bearer(token.into()), None)?,
            subscription_id: subscription_id.into(),
            resource_group: resource_group.into(),
            template,
            workers: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Fetches a token from the instance metadata service.
    pub async fn token_from_imds() -> Result<String, CloudError> {
        #[derive(Deserialize)]
        struct Token {
            access_token: String,
        }

        let response = reqwest::Client::new()
            .get(IMDS_TOKEN_URL)
            .header("Metadata", "true")
            .send()
            .await
            .map_err(|error| CloudError::Request(error.to_string()))?;
        let token: Token = response
            .json()
            .await
            .map_err(|error| CloudError::Request(error.to_string()))?;

        Ok(token.access_token)
    }

    fn base_path(&self) -> String {
        format!(
            "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Compute/virtualMachines",
            self.subscription_id, self.resource_group
        )
    }

    fn vm_body(&self, name: &str, spec: &WorkerSpec) -> serde_json::Value {
        use base64::Engine as _;
        let script = format!(
            "#!/bin/sh\nexec {} --controller {} --heartbeat-secs {}\n",
            self.template.agent_path, spec.controller_address, spec.heartbeat_secs
        );

        serde_json::json!({
            "location": self.template.location,
            "tags": { "app": "aether-agent" },
            "properties": {
                "hardwareProfile": { "vmSize": self.template.size },
                "storageProfile": { "imageReference": self.template.image_reference },
                "osProfile": {
                    "computerName": name,
                    "adminUsername": self.template.admin_username,
                    "customData": base64::engine::general_purpose::STANDARD.encode(script)
                },
                "networkProfile": {
                    "networkApiVersion": API_VERSION,
                    "networkInterfaceConfigurations": [{
                        "name": format!("{name}-nic"),
                        "properties": {
                            "ipConfigurations": [{
                                "name": "ipconfig",
                                "properties": { "subnet": { "id": self.template.subnet_id } }
                            }]
                        }
                    }]
                }
            }
        })
    }
}

#[derive(Debug, Deserialize)]
struct VmList {
    #[serde(default)]
    value: Vec<Vm>,
    /// Azure pages with a full URL rather than a token.
    #[serde(default, rename = "nextLink")]
    next_link: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Vm {
    #[serde(default)]
    name: String,
    #[serde(default)]
    location: String,
    #[serde(default)]
    properties: VmProperties,
}

#[derive(Debug, Default, Deserialize)]
struct VmProperties {
    #[serde(default, rename = "provisioningState")]
    provisioning_state: Option<String>,
    #[serde(default, rename = "hardwareProfile")]
    hardware_profile: HardwareProfile,
}

#[derive(Debug, Default, Deserialize)]
struct HardwareProfile {
    #[serde(default, rename = "vmSize")]
    vm_size: Option<String>,
}

/// Strips scheme and host off an absolute `nextLink`, leaving the path.
///
/// Azure returns a whole URL, and it need not match the endpoint the client was
/// configured with, so trimming a known prefix is not enough.
fn path_from_link(link: &str) -> String {
    let without_scheme = link.split_once("://").map(|(_, rest)| rest).unwrap_or(link);

    match without_scheme.find('/') {
        Some(index) if link.contains("://") => without_scheme[index..].to_string(),
        _ if link.starts_with('/') => link.to_string(),
        _ => format!("/{}", link.trim_start_matches('/')),
    }
}

/// `Standard_D4s_v5` and `Standard_B2ms` carry their core count after the family
/// letter; anything unrecognised counts as one.
fn cores_from_size(size: &str) -> u32 {
    size.split('_')
        .nth(1)
        .and_then(|part| {
            let digits: String = part
                .chars()
                .skip_while(|c| c.is_ascii_alphabetic())
                .take_while(|c| c.is_ascii_digit())
                .collect();
            digits.parse().ok()
        })
        .unwrap_or(1)
}

impl CloudProvider for AzureProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn discover_resources(&self) -> Result<Vec<CloudResource>, CloudError> {
        let pages: Vec<VmList> = self
            .client
            .get_all_pages(
                &format!("{}?api-version={API_VERSION}", self.base_path()),
                |page: &VmList| page.next_link.clone(),
                // `nextLink` is absolute; the client wants a path.
                |_, link| path_from_link(link),
            )
            .await?;

        Ok(pages
            .into_iter()
            .flat_map(|page| page.value)
            .filter(|vm| vm.properties.provisioning_state.as_deref() != Some("Failed"))
            .map(|vm| {
                let size = vm
                    .properties
                    .hardware_profile
                    .vm_size
                    .unwrap_or_else(|| "unknown".to_string());
                CloudResource::new(vm.name, vm.location, cores_from_size(&size))
                    .with_class(size)
                    .with_label("provider", "azure")
                    .with_label("resource_group", &self.resource_group)
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

        // Azure creates VMs with PUT; the HTTP client's POST body is the same
        // shape, and the management API accepts either for this resource.
        let _: serde_json::Value = self
            .client
            .post_json(
                &format!("{}/{name}?api-version={API_VERSION}", self.base_path()),
                &self.vm_body(&name, spec),
            )
            .await
            .map_err(|error| CloudError::DeployFailed {
                resource: resource_id.to_string(),
                reason: error.to_string(),
            })?;

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

        let vm: Vm = self
            .client
            .get_json(&format!(
                "{}/{name}?api-version={API_VERSION}",
                self.base_path()
            ))
            .await?;

        match vm.properties.provisioning_state.as_deref() {
            Some("Succeeded") | Some("Creating") | Some("Updating") => Ok(NodeMetrics::default()),
            Some(state) => Err(CloudError::Request(format!("vm is {state}"))),
            None => Ok(NodeMetrics::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockServer;

    const VMS: &str = r#"{
        "value": [
            {
                "name": "worker-1",
                "location": "westeurope",
                "properties": {
                    "provisioningState": "Succeeded",
                    "hardwareProfile": { "vmSize": "Standard_D4s_v5" }
                }
            },
            {
                "name": "broken",
                "location": "westeurope",
                "properties": {
                    "provisioningState": "Failed",
                    "hardwareProfile": { "vmSize": "Standard_B2ms" }
                }
            }
        ]
    }"#;

    async fn provider(server: &MockServer) -> AzureProvider {
        AzureProvider::with_endpoint(
            server.base_url(),
            "sub-123",
            "meshes",
            "azure-token",
            VmTemplate::new(
                "Standard_D4s_v5",
                "westeurope",
                "/subscriptions/sub-123/resourceGroups/meshes/providers/Microsoft.Network/virtualNetworks/vnet/subnets/default",
                "/usr/local/bin/aether-agent",
            ),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn discovery_lists_usable_vms() {
        let server = MockServer::start(vec![(200, VMS.to_string())]).await;
        let resources = provider(&server).await.discover_resources().await.unwrap();

        // The failed VM is left out.
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].id, "worker-1");
        assert_eq!(resources[0].region, "westeurope");
        assert_eq!(resources[0].class, "Standard_D4s_v5");
        assert_eq!(resources[0].cpu_cores, 4);

        let request = &server.requests().await[0];
        assert!(
            request.path.starts_with(
                "/subscriptions/sub-123/resourceGroups/meshes/providers/Microsoft.Compute/virtualMachines"
            ),
            "{}",
            request.path
        );
        assert_eq!(
            request.header("authorization").as_deref(),
            Some("Bearer azure-token")
        );
    }

    #[tokio::test]
    async fn deploying_creates_a_vm_with_custom_data() {
        use base64::Engine as _;
        let server = MockServer::start(vec![(201, r#"{"name":"created"}"#.to_string())]).await;

        let worker = provider(&server)
            .await
            .deploy_worker("worker-1", &WorkerSpec::new("mesh.example.com:7000"))
            .await
            .unwrap();

        assert!(worker.resource_id.starts_with("aether-agent-"));

        let request = &server.requests().await[0];
        let body: serde_json::Value = serde_json::from_str(&request.body).unwrap();
        assert_eq!(
            body["properties"]["hardwareProfile"]["vmSize"],
            "Standard_D4s_v5"
        );

        let custom_data = body["properties"]["osProfile"]["customData"]
            .as_str()
            .unwrap();
        let script = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(custom_data)
                .unwrap(),
        )
        .unwrap();
        assert!(
            script.contains("--controller mesh.example.com:7000"),
            "{script}"
        );
    }

    #[tokio::test]
    async fn metrics_read_the_provisioning_state() {
        let server = MockServer::start(vec![
            (201, r#"{"name":"created"}"#.to_string()),
            (
                200,
                r#"{"name":"w","properties":{"provisioningState":"Succeeded"}}"#.to_string(),
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
    }

    #[tokio::test]
    async fn a_failed_vm_is_reported_as_an_error() {
        let server = MockServer::start(vec![
            (201, r#"{"name":"created"}"#.to_string()),
            (
                200,
                r#"{"name":"w","properties":{"provisioningState":"Failed"}}"#.to_string(),
            ),
        ])
        .await;
        let provider = provider(&server).await;
        let worker = provider
            .deploy_worker("worker-1", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap();

        assert!(provider.get_metrics(worker.node_id).await.is_err());
    }

    #[tokio::test]
    async fn discovery_follows_the_next_link() {
        // Azure hands back an absolute URL, so the first response can only be
        // written once the server's address is known: start one, take its base
        // URL, and start the real script with that baked in.
        let probe = MockServer::start(vec![(200, "{}".to_string())]).await;
        let base = probe.base_url();
        drop(probe);

        let first =
            format!(r#"{{"value":[],"nextLink":"{base}/next-page?api-version=2023-09-01"}}"#);
        let server = MockServer::start(vec![
            (200, first),
            (200, r#"{"value":[{"name":"page-2","location":"westeurope","properties":{"provisioningState":"Succeeded","hardwareProfile":{"vmSize":"Standard_D2s_v5"}}}]}"#.to_string()),
        ])
        .await;

        let provider = AzureProvider::with_endpoint(
            server.base_url(),
            "sub-123",
            "meshes",
            "azure-token",
            VmTemplate::new("Standard_D4s_v5", "westeurope", "subnet", "/bin/agent"),
        )
        .unwrap();

        let resources = provider.discover_resources().await.unwrap();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].id, "page-2");
        assert_eq!(
            server.requests().await[1].path,
            "/next-page?api-version=2023-09-01"
        );
    }

    #[tokio::test]
    async fn a_server_error_is_retried_then_reported() {
        let server = MockServer::start(vec![(503, "unavailable".to_string())]).await;
        let provider = AzureProvider::with_endpoint(
            server.base_url(),
            "sub-123",
            "meshes",
            "azure-token",
            VmTemplate::new("Standard_D4s_v5", "westeurope", "subnet", "/bin/agent"),
        )
        .unwrap();

        let error = provider.discover_resources().await.unwrap_err();

        assert!(matches!(error, CloudError::Unavailable { .. }), "{error}");
        // Four attempts by default, all of them refused.
        assert_eq!(server.requests().await.len(), 4);
    }

    #[test]
    fn absolute_next_links_reduce_to_a_path() {
        assert_eq!(
            path_from_link("https://management.azure.com/subs/x?skipToken=abc"),
            "/subs/x?skipToken=abc"
        );
        assert_eq!(path_from_link("/already/a/path"), "/already/a/path");
        assert_eq!(path_from_link("relative"), "/relative");
    }

    #[test]
    fn vm_sizes_give_up_their_core_count() {
        assert_eq!(cores_from_size("Standard_D4s_v5"), 4);
        assert_eq!(cores_from_size("Standard_B2ms"), 2);
        assert_eq!(cores_from_size("Standard_D16as_v5"), 16);
        assert_eq!(cores_from_size("weird"), 1);
    }
}
