//! A provider that deploys agents as local processes.
//!
//! This is the adapter that actually starts something: it treats one machine as
//! a pool of slots and launches `aether-agent` into them. Useful on a big
//! server, and the reference for what a real cloud adapter has to do — discover
//! capacity, start a worker, report what the platform sees.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};

use aether_core::{NodeId, NodeMetrics};

use crate::{CloudError, CloudProvider, CloudResource, DeployedWorker, WorkerSpec};

#[derive(Debug, Default)]
struct Inner {
    workers: HashMap<NodeId, DeployedWorker>,
    /// Agent processes, kept so they can be stopped again.
    processes: HashMap<NodeId, Child>,
}

/// Runs agents as child processes on this machine.
#[derive(Debug, Clone)]
pub struct ProcessProvider {
    name: String,
    /// Path to the `aether-agent` binary.
    agent_path: PathBuf,
    /// Where each agent keeps its identity file, so restarts stay one node.
    state_dir: PathBuf,
    slots: u32,
    inner: Arc<Mutex<Inner>>,
}

impl ProcessProvider {
    /// `slots` is how many agents this machine is willing to run.
    pub fn new(
        name: impl Into<String>,
        agent_path: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
        slots: u32,
    ) -> Self {
        Self {
            name: name.into(),
            agent_path: agent_path.into(),
            state_dir: state_dir.into(),
            slots: slots.max(1),
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    /// Stops a deployed agent.
    pub fn stop(&self, node_id: NodeId) -> Result<(), CloudError> {
        let mut inner = self.lock();
        let mut child = inner
            .processes
            .remove(&node_id)
            .ok_or_else(|| CloudError::UnknownResource(node_id.to_string()))?;
        inner.workers.remove(&node_id);

        child
            .kill()
            .map_err(|error| CloudError::Request(error.to_string()))?;
        let _ = child.wait();
        Ok(())
    }

    /// Agents this provider started and has not stopped.
    pub fn running(&self) -> usize {
        self.lock().processes.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        aether_core::lock(&self.inner)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // A provider going away should not leave orphaned agents behind.
        for child in self.processes.values_mut() {
            let _ = child.kill();
        }
    }
}

impl CloudProvider for ProcessProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn discover_resources(&self) -> Result<Vec<CloudResource>, CloudError> {
        let cores = std::thread::available_parallelism()
            .map(|count| count.get() as u32)
            .unwrap_or(1);

        Ok((0..self.slots)
            .map(|slot| {
                CloudResource::new(format!("slot-{slot}"), "local", cores.max(1))
                    .with_class("process")
                    .with_label("provider", "process")
            })
            .collect())
    }

    async fn deploy_worker(
        &self,
        resource_id: &str,
        spec: &WorkerSpec,
    ) -> Result<DeployedWorker, CloudError> {
        if !resource_id.starts_with("slot-") {
            return Err(CloudError::UnknownResource(resource_id.to_string()));
        }

        std::fs::create_dir_all(&self.state_dir).map_err(|error| CloudError::DeployFailed {
            resource: resource_id.to_string(),
            reason: error.to_string(),
        })?;
        let identity_path = self.state_dir.join(format!("{resource_id}-node-id"));

        let mut command = Command::new(&self.agent_path);
        command
            .arg("--controller")
            .arg(&spec.controller_address)
            .arg("--heartbeat-secs")
            .arg(spec.heartbeat_secs.to_string())
            .arg("--identity-path")
            .arg(&identity_path);

        let child = command.spawn().map_err(|error| CloudError::DeployFailed {
            resource: resource_id.to_string(),
            reason: format!("{}: {error}", self.agent_path.display()),
        })?;

        // The agent decides its own id from the identity file; until it
        // registers, this is the handle the provider tracks it by.
        let worker = DeployedWorker {
            node_id: NodeId::generate(),
            resource_id: resource_id.to_string(),
            address: spec.controller_address.clone(),
        };

        let mut inner = self.lock();
        inner.processes.insert(worker.node_id, child);
        inner.workers.insert(worker.node_id, worker.clone());
        Ok(worker)
    }

    async fn get_metrics(&self, node_id: NodeId) -> Result<NodeMetrics, CloudError> {
        let mut inner = self.lock();
        let Some(child) = inner.processes.get_mut(&node_id) else {
            return Err(CloudError::UnknownResource(node_id.to_string()));
        };

        // The platform view here is coarse on purpose: alive or not. Real
        // metrics come from the agent's own heartbeats.
        match child.try_wait() {
            Ok(Some(status)) => Err(CloudError::Request(format!("worker exited: {status}"))),
            Ok(None) => Ok(NodeMetrics::default()),
            Err(error) => Err(CloudError::Request(error.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(agent: &str) -> ProcessProvider {
        let dir = std::env::temp_dir().join(format!("aethermesh-process-{}", std::process::id()));
        ProcessProvider::new("local", agent, dir, 2)
    }

    #[tokio::test]
    async fn discovery_offers_one_resource_per_slot() {
        let resources = provider("aether-agent").discover_resources().await.unwrap();

        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].id, "slot-0");
        assert_eq!(resources[0].class, "process");
    }

    #[tokio::test]
    async fn deploying_to_an_unknown_slot_fails() {
        let error = provider("aether-agent")
            .deploy_worker("mainframe", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap_err();

        assert!(matches!(error, CloudError::UnknownResource(_)));
    }

    #[tokio::test]
    async fn a_missing_binary_is_reported_with_its_path() {
        let error = provider("definitely-not-a-binary")
            .deploy_worker("slot-0", &WorkerSpec::new("127.0.0.1:7000"))
            .await
            .unwrap_err();

        match error {
            CloudError::DeployFailed { reason, .. } => {
                assert!(reason.contains("definitely-not-a-binary"), "{reason}");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn metrics_for_an_undeployed_worker_fail() {
        let provider = provider("aether-agent");
        assert!(provider.get_metrics(NodeId::generate()).await.is_err());
        assert_eq!(provider.running(), 0);
    }
}
