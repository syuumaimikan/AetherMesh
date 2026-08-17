//! Where nodes come from.
//!
//! A [`CloudProvider`] turns "somewhere I can run a worker" into nodes the mesh
//! can schedule on: it lists what capacity exists, starts an agent on it, and
//! reports what the provider itself knows about a running worker.
//!
//! This crate is the seam only. No provider SDK is a dependency here, and none
//! should become one: an AWS or Azure adapter belongs in its own crate behind
//! this trait, so a Raspberry Pi build never pays for a cloud SDK.

use std::collections::BTreeMap;

use aether_core::{NodeId, NodeMetrics};
use serde::{Deserialize, Serialize};

pub mod process;
pub mod r#static;

#[cfg(feature = "cloud-http")]
pub mod aws;
#[cfg(feature = "cloud-http")]
pub mod azure;
#[cfg(feature = "cloud-http")]
pub mod gcp;
#[cfg(feature = "cloud-http")]
pub mod http;
#[cfg(feature = "cloud-http")]
pub mod kubernetes;
#[cfg(feature = "cloud-http")]
mod sigv4;

#[cfg(all(test, feature = "cloud-http"))]
mod testing;

pub use process::ProcessProvider;
pub use r#static::StaticProvider;

#[cfg(feature = "cloud-http")]
pub use aws::{AwsProvider, LaunchTemplate};
#[cfg(feature = "cloud-http")]
pub use azure::{AzureProvider, VmTemplate};
#[cfg(feature = "cloud-http")]
pub use gcp::{GcpProvider, InstanceTemplate};
#[cfg(feature = "cloud-http")]
pub use http::{Credentials, HttpClient};
#[cfg(feature = "cloud-http")]
pub use kubernetes::KubernetesProvider;

/// Something went wrong talking to a provider.
#[derive(Debug, thiserror::Error)]
pub enum CloudError {
    #[error("resource {0} is not known to this provider")]
    UnknownResource(String),
    #[error("worker for resource {resource} could not be deployed: {reason}")]
    DeployFailed { resource: String, reason: String },
    #[error("provider request failed: {0}")]
    Request(String),
    /// The provider asked us to slow down. Retried automatically.
    #[error("provider throttled the request: {detail}")]
    Throttled { detail: String },
    /// A transient server-side failure. Retried automatically.
    #[error("provider is unavailable: {detail}")]
    Unavailable { detail: String },
    /// The credentials were rejected. Retrying will not help.
    #[error("provider rejected the credentials: {detail}")]
    Unauthorized { detail: String },
    /// The resource is not there.
    #[error("provider has no such resource: {detail}")]
    NotFound { detail: String },
}

/// A place a worker could run: a VM, a Kubernetes node, a Pi on a shelf.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CloudResource {
    /// Provider-scoped identifier, e.g. an instance id.
    pub id: String,
    /// What the provider calls this region or site.
    pub region: String,
    /// Instance type, machine class, or free-form label.
    pub class: String,
    pub cpu_cores: u32,
    pub memory_bytes: u64,
    /// Cost per hour in the provider's currency, if it publishes one.
    pub hourly_cost: Option<f64>,
    /// Anything provider-specific worth keeping, such as an availability zone.
    pub labels: BTreeMap<String, String>,
}

impl CloudResource {
    pub fn new(id: impl Into<String>, region: impl Into<String>, cpu_cores: u32) -> Self {
        Self {
            id: id.into(),
            region: region.into(),
            class: "default".to_string(),
            cpu_cores,
            memory_bytes: 0,
            hourly_cost: None,
            labels: BTreeMap::new(),
        }
    }

    pub fn with_class(mut self, class: impl Into<String>) -> Self {
        self.class = class.into();
        self
    }

    pub fn with_memory(mut self, memory_bytes: u64) -> Self {
        self.memory_bytes = memory_bytes;
        self
    }

    pub fn with_hourly_cost(mut self, hourly_cost: f64) -> Self {
        self.hourly_cost = Some(hourly_cost);
        self
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }
}

/// An agent the provider started on a resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployedWorker {
    /// Identity the agent will register with.
    pub node_id: NodeId,
    /// Resource it runs on.
    pub resource_id: String,
    /// Address the agent advertises.
    pub address: String,
}

/// What to hand a worker when starting it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSpec {
    /// Controller the agent should register with.
    pub controller_address: String,
    /// Seconds between heartbeats.
    pub heartbeat_secs: u64,
}

impl WorkerSpec {
    pub fn new(controller_address: impl Into<String>) -> Self {
        Self {
            controller_address: controller_address.into(),
            heartbeat_secs: 5,
        }
    }
}

/// A source of compute the mesh can grow into.
///
/// Implementations are expected to be cheap to clone and safe to call from
/// several tasks at once.
pub trait CloudProvider: Send + Sync {
    /// Short name used in logs and configuration, e.g. `"aws"`.
    fn name(&self) -> &str;

    /// Capacity this provider can offer right now.
    fn discover_resources(
        &self,
    ) -> impl Future<Output = Result<Vec<CloudResource>, CloudError>> + Send;

    /// Starts an agent on one resource and returns the identity it will use.
    fn deploy_worker(
        &self,
        resource_id: &str,
        spec: &WorkerSpec,
    ) -> impl Future<Output = Result<DeployedWorker, CloudError>> + Send;

    /// Provider-side view of a running worker.
    ///
    /// This is not a replacement for agent heartbeats: it is what the platform
    /// reports, which is useful when the agent itself has stopped answering.
    fn get_metrics(
        &self,
        node_id: NodeId,
    ) -> impl Future<Output = Result<NodeMetrics, CloudError>> + Send;
}
