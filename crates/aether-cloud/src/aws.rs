//! AWS EC2 adapter.
//!
//! Discovery lists running instances; deploying a worker launches one with the
//! agent started from user data; metrics read the instance state. Requests are
//! signed with SigV4 from credentials in the environment — no AWS SDK.

use aether_core::{NodeId, NodeMetrics};

use crate::http::{Credentials, HttpClient};
use crate::{CloudError, CloudProvider, CloudResource, DeployedWorker, WorkerSpec};

/// EC2 API version this adapter speaks.
const API_VERSION: &str = "2016-11-15";

/// What one instance needs to become a worker.
#[derive(Debug, Clone)]
pub struct LaunchTemplate {
    pub image_id: String,
    pub instance_type: String,
    /// Path to the agent binary on the image.
    pub agent_path: String,
    /// Optional subnet, security group, and IAM profile.
    pub subnet_id: Option<String>,
    pub security_group_id: Option<String>,
}

impl LaunchTemplate {
    pub fn new(
        image_id: impl Into<String>,
        instance_type: impl Into<String>,
        agent_path: impl Into<String>,
    ) -> Self {
        Self {
            image_id: image_id.into(),
            instance_type: instance_type.into(),
            agent_path: agent_path.into(),
            subnet_id: None,
            security_group_id: None,
        }
    }

    pub fn with_subnet(mut self, subnet_id: impl Into<String>) -> Self {
        self.subnet_id = Some(subnet_id.into());
        self
    }

    pub fn with_security_group(mut self, security_group_id: impl Into<String>) -> Self {
        self.security_group_id = Some(security_group_id.into());
        self
    }
}

/// EC2, reached over its query API.
pub struct AwsProvider {
    name: String,
    client: HttpClient,
    region: String,
    template: LaunchTemplate,
    workers: std::sync::Mutex<std::collections::HashMap<NodeId, DeployedWorker>>,
}

impl AwsProvider {
    /// Builds a provider from explicit credentials.
    pub fn new(
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
        template: LaunchTemplate,
    ) -> Result<Self, CloudError> {
        let region = region.into();
        let endpoint = format!("https://ec2.{region}.amazonaws.com");
        Self::with_endpoint(
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            session_token,
            template,
        )
    }

    /// Same, against another endpoint — a local emulator, or a test double.
    pub fn with_endpoint(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
        template: LaunchTemplate,
    ) -> Result<Self, CloudError> {
        let region = region.into();
        Ok(Self {
            name: "aws".to_string(),
            client: HttpClient::new(
                endpoint,
                Credentials::AwsSigV4 {
                    access_key_id: access_key_id.into(),
                    secret_access_key: secret_access_key.into(),
                    session_token,
                    region: region.clone(),
                    service: "ec2".to_string(),
                },
                None,
            )?,
            region,
            template,
            workers: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Builds a provider from `AWS_ACCESS_KEY_ID` and friends.
    pub fn from_env(template: LaunchTemplate) -> Result<Self, CloudError> {
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .map_err(|_| CloudError::Request("AWS_REGION is not set".to_string()))?;
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| CloudError::Request("AWS_ACCESS_KEY_ID is not set".to_string()))?;
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| CloudError::Request("AWS_SECRET_ACCESS_KEY is not set".to_string()))?;

        Self::new(
            region,
            access_key_id,
            secret_access_key,
            std::env::var("AWS_SESSION_TOKEN").ok(),
            template,
        )
    }

    /// Cloud-init that starts the agent when the instance boots.
    fn user_data(&self, spec: &WorkerSpec) -> String {
        use base64::Engine as _;
        let script = format!(
            "#!/bin/sh\nexec {} --controller {} --heartbeat-secs {}\n",
            self.template.agent_path, spec.controller_address, spec.heartbeat_secs
        );
        base64::engine::general_purpose::STANDARD.encode(script)
    }
}

/// EC2's query API answers XML; these two helpers read the few fields needed
/// without pulling in an XML parser.
fn xml_values<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut rest = xml;

    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        values.push(&after[..end]);
        rest = &after[end + close.len()..];
    }
    values
}

fn xml_value<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    xml_values(xml, tag).into_iter().next()
}

/// Splits a `DescribeInstances` reply into its `<item>` blocks.
fn instance_items(xml: &str) -> Vec<&str> {
    xml_values(xml, "instancesSet")
        .into_iter()
        .flat_map(|set| xml_values(set, "item"))
        .collect()
}

impl CloudProvider for AwsProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn discover_resources(&self) -> Result<Vec<CloudResource>, CloudError> {
        let xml = self
            .client
            .send(
                reqwest::Method::GET,
                &format!(
                    "/?Action=DescribeInstances&Version={API_VERSION}&Filter.1.Name=instance-state-name&Filter.1.Value.1=running"
                ),
                None,
            )
            .await?;

        Ok(instance_items(&xml)
            .into_iter()
            .filter_map(|item| {
                let id = xml_value(item, "instanceId")?;
                let instance_type = xml_value(item, "instanceType").unwrap_or("unknown");
                let zone = xml_value(item, "availabilityZone").unwrap_or(&self.region);
                let cores = xml_value(item, "coreCount")
                    .and_then(|value| value.parse::<u32>().ok())
                    .unwrap_or(1);

                Some(
                    CloudResource::new(id, zone, cores)
                        .with_class(instance_type)
                        .with_label("provider", "aws")
                        .with_label("region", &self.region),
                )
            })
            .collect())
    }

    async fn deploy_worker(
        &self,
        resource_id: &str,
        spec: &WorkerSpec,
    ) -> Result<DeployedWorker, CloudError> {
        let mut path = format!(
            "/?Action=RunInstances&Version={API_VERSION}&ImageId={}&InstanceType={}&MinCount=1&MaxCount=1&UserData={}",
            self.template.image_id,
            self.template.instance_type,
            self.user_data(spec)
                .replace('+', "%2B")
                .replace('=', "%3D")
                .replace('/', "%2F")
        );
        if let Some(subnet) = &self.template.subnet_id {
            path.push_str(&format!("&SubnetId={subnet}"));
        }
        if let Some(group) = &self.template.security_group_id {
            path.push_str(&format!("&SecurityGroupId.1={group}"));
        }

        let xml = self
            .client
            .send(reqwest::Method::GET, &path, None)
            .await
            .map_err(|error| CloudError::DeployFailed {
                resource: resource_id.to_string(),
                reason: error.to_string(),
            })?;

        let instance_id = xml_value(&xml, "instanceId")
            .ok_or_else(|| CloudError::DeployFailed {
                resource: resource_id.to_string(),
                reason: "RunInstances returned no instanceId".to_string(),
            })?
            .to_string();

        let worker = DeployedWorker {
            node_id: NodeId::generate(),
            resource_id: instance_id,
            address: spec.controller_address.clone(),
        };
        aether_core::lock(&self.workers).insert(worker.node_id, worker.clone());
        Ok(worker)
    }

    async fn get_metrics(&self, node_id: NodeId) -> Result<NodeMetrics, CloudError> {
        let instance_id = aether_core::lock(&self.workers)
            .get(&node_id)
            .map(|worker| worker.resource_id.clone())
            .ok_or_else(|| CloudError::UnknownResource(node_id.to_string()))?;

        let xml = self
            .client
            .send(
                reqwest::Method::GET,
                &format!(
                    "/?Action=DescribeInstances&Version={API_VERSION}&InstanceId.1={instance_id}"
                ),
                None,
            )
            .await?;

        // EC2 reports lifecycle, not load; utilisation would be a CloudWatch
        // call, and the agent's own heartbeats are better anyway.
        match xml_value(&xml, "name") {
            Some("running") | Some("pending") => Ok(NodeMetrics::default()),
            Some(state) => Err(CloudError::Request(format!("instance is {state}"))),
            None => Ok(NodeMetrics::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockServer;

    const DESCRIBE: &str = r#"<DescribeInstancesResponse>
      <reservationSet><item><instancesSet>
        <item>
          <instanceId>i-0123456789abcdef0</instanceId>
          <instanceType>m5.large</instanceType>
          <placement><availabilityZone>eu-west-1a</availabilityZone></placement>
          <cpuOptions><coreCount>2</coreCount></cpuOptions>
          <instanceState><name>running</name></instanceState>
        </item>
        <item>
          <instanceId>i-aaaa</instanceId>
          <instanceType>t3.micro</instanceType>
          <placement><availabilityZone>eu-west-1b</availabilityZone></placement>
          <instanceState><name>running</name></instanceState>
        </item>
      </instancesSet></item></reservationSet>
    </DescribeInstancesResponse>"#;

    const RUN: &str = r#"<RunInstancesResponse>
      <instancesSet><item>
        <instanceId>i-newworker</instanceId>
        <instanceState><name>pending</name></instanceState>
      </item></instancesSet>
    </RunInstancesResponse>"#;

    async fn provider(server: &MockServer) -> AwsProvider {
        AwsProvider::with_endpoint(
            server.base_url(),
            "eu-west-1",
            "AKIDEXAMPLE",
            "SECRET",
            None,
            LaunchTemplate::new("ami-123", "m5.large", "/usr/local/bin/aether-agent")
                .with_subnet("subnet-abc")
                .with_security_group("sg-def"),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn discovery_reads_running_instances() {
        let server = MockServer::start(vec![(200, DESCRIBE.to_string())]).await;
        let resources = provider(&server).await.discover_resources().await.unwrap();

        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].id, "i-0123456789abcdef0");
        assert_eq!(resources[0].class, "m5.large");
        assert_eq!(resources[0].region, "eu-west-1a");
        assert_eq!(resources[0].cpu_cores, 2);
        // No coreCount in the second item: one core is the safe assumption.
        assert_eq!(resources[1].cpu_cores, 1);

        let request = &server.requests().await[0];
        assert!(
            request.path.contains("Action=DescribeInstances"),
            "{}",
            request.path
        );
        assert!(
            request.path.contains("instance-state-name"),
            "{}",
            request.path
        );
        assert!(
            request
                .header("authorization")
                .unwrap_or_default()
                .starts_with("AWS4-HMAC-SHA256")
        );
    }

    #[tokio::test]
    async fn deploying_launches_an_instance_with_agent_user_data() {
        let server = MockServer::start(vec![(200, RUN.to_string())]).await;

        let worker = provider(&server)
            .await
            .deploy_worker("i-any", &WorkerSpec::new("mesh.example.com:7000"))
            .await
            .unwrap();

        assert_eq!(worker.resource_id, "i-newworker");

        let path = &server.requests().await[0].path;
        assert!(path.contains("Action=RunInstances"), "{path}");
        assert!(path.contains("ImageId=ami-123"), "{path}");
        assert!(path.contains("SubnetId=subnet-abc"), "{path}");
        assert!(path.contains("SecurityGroupId.1=sg-def"), "{path}");
        assert!(path.contains("UserData="), "{path}");
    }

    #[test]
    fn user_data_starts_the_agent_against_the_controller() {
        use base64::Engine as _;
        let template = LaunchTemplate::new("ami-1", "t3.small", "/usr/bin/aether-agent");
        let provider = AwsProvider::with_endpoint(
            "http://localhost",
            "eu-west-1",
            "AKID",
            "SECRET",
            None,
            template,
        )
        .unwrap();

        let encoded = provider.user_data(&WorkerSpec::new("mesh:7000"));
        let script = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap();

        assert!(script.contains("/usr/bin/aether-agent"), "{script}");
        assert!(script.contains("--controller mesh:7000"), "{script}");
    }

    #[tokio::test]
    async fn a_rejected_launch_is_a_deploy_failure() {
        let server =
            MockServer::start(vec![(400, "<Response><Errors/></Response>".to_string())]).await;

        let error = provider(&server)
            .await
            .deploy_worker("i-any", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap_err();

        assert!(matches!(error, CloudError::DeployFailed { .. }), "{error}");
    }

    #[tokio::test]
    async fn metrics_read_the_instance_state() {
        let server =
            MockServer::start(vec![(200, RUN.to_string()), (200, DESCRIBE.to_string())]).await;
        let provider = provider(&server).await;

        let worker = provider
            .deploy_worker("i-any", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap();

        assert_eq!(
            provider.get_metrics(worker.node_id).await.unwrap(),
            NodeMetrics::default()
        );
        assert!(
            server.requests().await[1]
                .path
                .contains("InstanceId.1=i-newworker")
        );
    }

    #[test]
    fn xml_helpers_read_repeated_tags() {
        assert_eq!(xml_values("<a>1</a><a>2</a>", "a"), vec!["1", "2"]);
        assert_eq!(xml_value("<a><b>x</b></a>", "b"), Some("x"));
        assert_eq!(xml_value("<a>1</a>", "missing"), None);
    }
}
