//! A provider backed by a fixed list of machines.
//!
//! This covers the machines you already own — home PCs, a Pi, a rented VPS —
//! and doubles as the reference implementation of [`CloudProvider`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aether_core::{NodeId, NodeMetrics};

use crate::{CloudError, CloudProvider, CloudResource, DeployedWorker, WorkerSpec};

#[derive(Debug, Default)]
struct Inner {
    resources: Vec<CloudResource>,
    /// Workers this provider believes it started.
    workers: HashMap<NodeId, DeployedWorker>,
    /// Last metrics reported for a worker, e.g. by a platform API.
    metrics: HashMap<NodeId, NodeMetrics>,
}

/// Serves a fixed inventory. Deploying does not start a process: it records the
/// identity a manually launched agent is expected to use.
#[derive(Debug, Clone, Default)]
pub struct StaticProvider {
    name: String,
    inner: Arc<Mutex<Inner>>,
}

impl StaticProvider {
    pub fn new(name: impl Into<String>, resources: Vec<CloudResource>) -> Self {
        Self {
            name: name.into(),
            inner: Arc::new(Mutex::new(Inner {
                resources,
                ..Inner::default()
            })),
        }
    }

    /// Records metrics for a worker, standing in for a platform monitoring API.
    pub fn set_metrics(&self, node_id: NodeId, metrics: NodeMetrics) {
        self.lock().metrics.insert(node_id, metrics);
    }

    /// Workers this provider has deployed.
    pub fn workers(&self) -> Vec<DeployedWorker> {
        self.lock().workers.values().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        aether_core::lock(&self.inner)
    }
}

impl CloudProvider for StaticProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn discover_resources(&self) -> Result<Vec<CloudResource>, CloudError> {
        Ok(self.lock().resources.clone())
    }

    async fn deploy_worker(
        &self,
        resource_id: &str,
        spec: &WorkerSpec,
    ) -> Result<DeployedWorker, CloudError> {
        let mut inner = self.lock();
        let resource = inner
            .resources
            .iter()
            .find(|resource| resource.id == resource_id)
            .ok_or_else(|| CloudError::UnknownResource(resource_id.to_string()))?;

        let worker = DeployedWorker {
            node_id: NodeId::generate(),
            resource_id: resource.id.clone(),
            address: spec.controller_address.clone(),
        };
        inner.workers.insert(worker.node_id, worker.clone());
        Ok(worker)
    }

    async fn get_metrics(&self, node_id: NodeId) -> Result<NodeMetrics, CloudError> {
        let inner = self.lock();
        if !inner.workers.contains_key(&node_id) {
            return Err(CloudError::UnknownResource(node_id.to_string()));
        }
        Ok(inner.metrics.get(&node_id).copied().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> StaticProvider {
        StaticProvider::new(
            "homelab",
            vec![
                CloudResource::new("desktop", "home", 16)
                    .with_class("bare-metal")
                    .with_memory(32 * 1024 * 1024 * 1024),
                CloudResource::new("rpi4", "home", 4)
                    .with_class("raspberry-pi")
                    .with_hourly_cost(0.0)
                    .with_label("arch", "aarch64"),
            ],
        )
    }

    #[tokio::test]
    async fn discovery_lists_the_inventory() {
        let provider = provider();
        let resources = provider.discover_resources().await.unwrap();

        assert_eq!(provider.name(), "homelab");
        assert_eq!(resources.len(), 2);
        assert_eq!(
            resources[1].labels.get("arch").map(String::as_str),
            Some("aarch64")
        );
    }

    #[tokio::test]
    async fn deploying_assigns_an_identity() {
        let provider = provider();
        let spec = WorkerSpec::new("127.0.0.1:7000");

        let worker = provider.deploy_worker("rpi4", &spec).await.unwrap();

        assert_eq!(worker.resource_id, "rpi4");
        assert_eq!(provider.workers(), vec![worker]);
    }

    #[tokio::test]
    async fn deploying_to_an_unknown_resource_fails() {
        let provider = provider();
        let error = provider
            .deploy_worker("mainframe", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap_err();

        assert!(matches!(error, CloudError::UnknownResource(_)));
    }

    #[tokio::test]
    async fn metrics_come_back_for_deployed_workers_only() {
        let provider = provider();
        let worker = provider
            .deploy_worker("desktop", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap();

        assert_eq!(
            provider.get_metrics(worker.node_id).await.unwrap(),
            NodeMetrics::default()
        );

        let metrics = NodeMetrics::new(0.4, 0.6, 1024);
        provider.set_metrics(worker.node_id, metrics);
        assert_eq!(provider.get_metrics(worker.node_id).await.unwrap(), metrics);

        assert!(provider.get_metrics(NodeId::generate()).await.is_err());
    }
}
