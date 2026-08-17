//! Submit -> schedule -> transfer missing data -> dispatch -> result.
//!
//! The hop to the node sits behind [`TaskTransport`], so the same logic drives
//! the in-process simulation and a real connection.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use aether_core::{
    ChunkManifest, Codec, CompressionPolicy, DEFAULT_CHUNK_SIZE, DataDescriptor, DataId, DataStore,
    NodeId, Task, TaskId, TaskResult,
};
use aether_scheduler::{DataCatalog, Scheduler};
use tracing::{debug, warn};

use crate::registry::NodeRegistry;

/// How hard to try when a node fails to take a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts, counting the first one. `1` disables retrying.
    pub max_attempts: u32,
    /// Pause before the next attempt.
    pub backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: Duration::from_millis(100),
        }
    }
}

impl RetryPolicy {
    /// Fails on the first delivery problem.
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            backoff: Duration::ZERO,
        }
    }
}

/// A task could not be handed to a node.
///
/// A task that ran and failed is not an error here: that is a
/// [`TaskResult`] carrying `TaskOutcome::Failure`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DispatchError {
    #[error("no node available for task {0}")]
    NoNodeAvailable(TaskId),
    #[error("node {node_id} is not reachable: {reason}")]
    Unreachable { node_id: NodeId, reason: String },
    #[error("node {node_id} did not return a result for task {task_id} in time")]
    Timeout { node_id: NodeId, task_id: TaskId },
    #[error("task input {0} was never published to the controller")]
    UnknownInput(DataId),
}

/// Carries data and tasks to a node and brings results back.
pub trait TaskTransport {
    fn dispatch(
        &mut self,
        node_id: NodeId,
        task: &Task,
    ) -> impl Future<Output = Result<TaskResult, DispatchError>> + Send;

    /// Delivers a small dataset in one piece. Must arrive before any task that
    /// reads it. `bytes` is the wire form produced with `codec`.
    fn send_data(
        &mut self,
        node_id: NodeId,
        descriptor: DataDescriptor,
        codec: Codec,
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;

    /// Announces a chunked dataset before its chunks are sent.
    fn send_manifest(
        &mut self,
        node_id: NodeId,
        manifest: &ChunkManifest,
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;

    /// Delivers one chunk of an announced dataset. Chunks are independent, so
    /// a transport is free to send them concurrently.
    fn send_chunk(
        &mut self,
        node_id: NodeId,
        data_id: DataId,
        index: u32,
        codec: Codec,
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;
}

/// Whether another node is worth trying.
///
/// A missing input is the submitter's mistake and no node can fix it.
fn is_retryable(error: &DispatchError) -> bool {
    matches!(
        error,
        DispatchError::Unreachable { .. } | DispatchError::Timeout { .. }
    )
}

/// Owns the registry and drives task placement.
pub struct Controller<S, T> {
    registry: NodeRegistry,
    scheduler: S,
    transport: T,
    catalog: DataCatalog,
    store: DataStore,
    /// Chunk layout of every published dataset large enough to be split.
    manifests: HashMap<DataId, ChunkManifest>,
    chunk_size: usize,
    compression: CompressionPolicy,
    /// When false, inputs are re-sent even if the node already has them.
    /// Only useful for baseline measurements.
    reuse_data: bool,
    retry: RetryPolicy,
    retries: u64,
    data_bytes_sent: u64,
    data_bytes_uncompressed: u64,
    transfers_skipped: u64,
    chunks_skipped: u64,
}

impl<S: Scheduler, T: TaskTransport> Controller<S, T> {
    /// Builds a controller whose scheduler reads the same catalog it writes.
    pub fn new(scheduler: S, transport: T, catalog: DataCatalog) -> Self {
        Self {
            registry: NodeRegistry::new(),
            scheduler,
            transport,
            catalog,
            store: DataStore::new(),
            manifests: HashMap::new(),
            chunk_size: DEFAULT_CHUNK_SIZE,
            compression: CompressionPolicy::default(),
            reuse_data: true,
            retry: RetryPolicy::default(),
            retries: 0,
            data_bytes_sent: 0,
            data_bytes_uncompressed: 0,
            transfers_skipped: 0,
            chunks_skipped: 0,
        }
    }

    /// Sets when transfers are compressed.
    pub fn with_compression(mut self, compression: CompressionPolicy) -> Self {
        self.compression = compression;
        self
    }

    /// Turns off reuse of data a node already holds, so every task re-sends its
    /// inputs. Exists to measure what the mesh saves.
    pub fn with_data_reuse(mut self, reuse_data: bool) -> Self {
        self.reuse_data = reuse_data;
        self
    }

    /// Sets how a task is retried when a node cannot take it.
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Tasks re-dispatched after a delivery failure.
    pub fn retries(&self) -> u64 {
        self.retries
    }

    /// Sets the chunk size used for datasets larger than one chunk.
    /// Only affects data published afterwards.
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = if chunk_size == 0 {
            DEFAULT_CHUNK_SIZE
        } else {
            chunk_size
        };
        self
    }

    pub fn registry(&self) -> &NodeRegistry {
        &self.registry
    }

    pub fn registry_mut(&mut self) -> &mut NodeRegistry {
        &mut self.registry
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn catalog(&self) -> &DataCatalog {
        &self.catalog
    }

    /// Bytes actually put on the wire for task inputs, after compression.
    pub fn data_bytes_sent(&self) -> u64 {
        self.data_bytes_sent
    }

    /// What those transfers would have cost uncompressed.
    pub fn data_bytes_uncompressed(&self) -> u64 {
        self.data_bytes_uncompressed
    }

    /// Transfers avoided because the node already held the data.
    pub fn transfers_skipped(&self) -> u64 {
        self.transfers_skipped
    }

    /// Chunks avoided because the node already held that exact chunk.
    pub fn chunks_skipped(&self) -> u64 {
        self.chunks_skipped
    }

    /// Chunk layout of a published dataset, if it is large enough to be split.
    pub fn manifest(&self, data_id: DataId) -> Option<&ChunkManifest> {
        self.manifests.get(&data_id)
    }

    /// Registers data with the controller and returns its content address.
    /// Publishing the same bytes twice yields the same descriptor.
    ///
    /// Data larger than one chunk is split now, so the layout is computed once
    /// however many nodes it is later sent to.
    pub fn publish(&mut self, bytes: Vec<u8>) -> DataDescriptor {
        let descriptor = self.store.put(bytes);
        if descriptor.size_bytes > self.chunk_size as u64 {
            let bytes = self.store.get(descriptor.id).expect("just stored");
            self.manifests
                .entry(descriptor.id)
                .or_insert_with(|| ChunkManifest::split(&bytes, self.chunk_size));
        }
        descriptor
    }

    /// Places one task on a node and waits for its result.
    ///
    /// A node that cannot take the task is set aside and the task goes to the
    /// next best node, up to [`RetryPolicy::max_attempts`]. A task that *ran*
    /// and failed is returned as-is: rerunning it would just fail again.
    pub async fn submit(&mut self, task: Task) -> Result<TaskResult, DispatchError> {
        let mut unusable: HashSet<NodeId> = HashSet::new();

        for attempt in 1..=self.retry.max_attempts {
            let nodes: Vec<_> = self
                .registry
                .nodes()
                .into_iter()
                .filter(|node| !unusable.contains(&node.id))
                .collect();

            let node_id = self
                .scheduler
                .select_node(&nodes, &task)
                .ok_or(DispatchError::NoNodeAvailable(task.id))?;

            match self.try_once(node_id, &task).await {
                Ok(result) => return Ok(result),
                Err(error) if attempt < self.retry.max_attempts && is_retryable(&error) => {
                    warn!(%node_id, task_id = %task.id, attempt, %error, "retrying on another node");
                    // The data this node was credited with is no longer usable.
                    self.catalog.forget_node(node_id);
                    unusable.insert(node_id);
                    self.retries += 1;
                    if !self.retry.backoff.is_zero() {
                        tokio::time::sleep(self.retry.backoff).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }

        Err(DispatchError::NoNodeAvailable(task.id))
    }

    /// One placement attempt: move the missing inputs, then run the task.
    async fn try_once(
        &mut self,
        node_id: NodeId,
        task: &Task,
    ) -> Result<TaskResult, DispatchError> {
        self.ensure_inputs(node_id, task).await?;
        self.transport.dispatch(node_id, task).await
    }

    /// Sends the task's inputs that the node does not have yet.
    async fn ensure_inputs(&mut self, node_id: NodeId, task: &Task) -> Result<(), DispatchError> {
        for data_id in &task.inputs {
            if self.reuse_data && self.catalog.holds(*data_id, node_id) {
                self.transfers_skipped += 1;
                debug!(%node_id, %data_id, "input already on the node");
                continue;
            }

            let bytes = self
                .store
                .get(*data_id)
                .ok_or(DispatchError::UnknownInput(*data_id))?;
            let descriptor = DataDescriptor::new(*data_id, bytes.len() as u64);

            let bandwidth = self.bandwidth_to(node_id);
            match self.manifests.get(data_id).cloned() {
                Some(manifest) => {
                    self.send_chunked(node_id, &manifest, &bytes, bandwidth)
                        .await?
                }
                None => {
                    let (codec, payload) = self.compression.encode(&bytes, bandwidth);
                    self.transport
                        .send_data(node_id, descriptor, codec, &payload)
                        .await?;
                    self.data_bytes_sent += payload.len() as u64;
                    self.data_bytes_uncompressed += descriptor.size_bytes;
                }
            }

            self.catalog.record(descriptor, node_id);
            debug!(%node_id, %data_id, size = descriptor.size_bytes, "input transferred");
        }
        Ok(())
    }

    /// Link speed toward a node, as far as the registry knows.
    fn bandwidth_to(&self, node_id: NodeId) -> Option<u64> {
        self.registry
            .get(node_id)
            .and_then(|entry| entry.info.bandwidth_bytes_per_sec)
    }

    /// Sends a manifest followed by the chunks the node is still missing.
    async fn send_chunked(
        &mut self,
        node_id: NodeId,
        manifest: &ChunkManifest,
        bytes: &[u8],
        bandwidth: Option<u64>,
    ) -> Result<(), DispatchError> {
        let data_id = manifest.data.id;
        self.transport.send_manifest(node_id, manifest).await?;

        for (index, chunk) in manifest.indexed() {
            // Chunks are content-addressed, so a repeated chunk - inside this
            // dataset or shared with another - is never sent to a node twice.
            if self.reuse_data && self.catalog.holds(chunk.id, node_id) {
                self.chunks_skipped += 1;
                continue;
            }

            let range = manifest
                .chunk_range(index)
                .expect("index comes from the manifest");
            // Each chunk is judged on its own: a compressible chunk is
            // compressed even if its neighbours are not.
            let (codec, payload) = self.compression.encode(&bytes[range], bandwidth);
            self.transport
                .send_chunk(node_id, data_id, index, codec, &payload)
                .await?;
            self.catalog.record(chunk, node_id);
            self.data_bytes_sent += payload.len() as u64;
            self.data_bytes_uncompressed += chunk.size_bytes;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use aether_core::task::kind;
    use aether_core::{NodeInfo, NodeMetrics};
    use aether_scheduler::{LeastLoadedScheduler, LocalityScheduler};

    use super::*;
    use crate::sim::SimulatedMesh;

    fn node(hostname: &str, cpu: f32) -> NodeInfo {
        let mut info = NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7000", 4);
        info.update_metrics(NodeMetrics::new(cpu, 0.5, 1024));
        info
    }

    fn controller() -> Controller<LeastLoadedScheduler, SimulatedMesh> {
        Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            DataCatalog::new(),
        )
    }

    #[tokio::test]
    async fn submitting_without_nodes_fails() {
        let mut controller = controller();
        let task = Task::new(kind::ECHO, b"hi".to_vec());
        let id = task.id;

        assert_eq!(
            controller.submit(task).await,
            Err(DispatchError::NoNodeAvailable(id))
        );
    }

    #[tokio::test]
    async fn task_lands_on_the_least_loaded_node() {
        let mut controller = controller();
        let busy = node("busy", 0.9);
        let idle = node("idle", 0.1);
        let idle_id = idle.id;
        controller.registry_mut().register(busy);
        controller.registry_mut().register(idle);

        let result = controller
            .submit(Task::new(kind::ECHO, b"payload".to_vec()))
            .await
            .unwrap();

        assert_eq!(result.node_id, idle_id);
        assert!(result.is_success());
        assert_eq!(result.output(), Some(&b"payload"[..]));
    }

    #[tokio::test]
    async fn an_unknown_task_kind_comes_back_as_a_failed_result() {
        let mut controller = controller();
        controller.registry_mut().register(node("only", 0.2));

        let result = controller
            .submit(Task::new("quantum", Vec::new()))
            .await
            .unwrap();

        assert!(!result.is_success());
        assert_eq!(result.output(), None);
    }

    #[tokio::test]
    async fn dispatching_counts_transferred_bytes() {
        let mut controller = controller();
        controller.registry_mut().register(node("only", 0.2));
        assert_eq!(controller.transport().bytes_transferred(), 0);

        controller
            .submit(Task::new(kind::ECHO, vec![0u8; 256]))
            .await
            .unwrap();

        assert!(controller.transport().bytes_transferred() > 256);
    }

    #[tokio::test]
    async fn an_input_is_transferred_once_and_then_reused() {
        let catalog = DataCatalog::new();
        let mut controller = Controller::new(
            LocalityScheduler::new(catalog.clone()),
            SimulatedMesh::new(),
            catalog,
        );
        controller.registry_mut().register(node("only", 0.2));

        let descriptor = controller.publish(vec![7u8; 4096]);
        for _ in 0..3 {
            // The simulated executor only knows `echo`; the point here is the transfer.
            let task = Task::new(kind::ECHO, Vec::new()).with_inputs(vec![descriptor.id]);
            assert!(controller.submit(task).await.unwrap().is_success());
        }

        assert_eq!(controller.data_bytes_uncompressed(), 4096);
        assert_eq!(controller.transfers_skipped(), 2);
    }

    #[tokio::test]
    async fn each_node_receives_the_data_once() {
        let catalog = DataCatalog::new();
        let mut controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        );
        let first = node("a", 0.1);
        let second = node("b", 0.9);
        controller.registry_mut().register(first.clone());
        controller.registry_mut().register(second.clone());

        let descriptor = controller.publish(vec![1u8; 100]);
        let task = Task::new(kind::ECHO, Vec::new()).with_inputs(vec![descriptor.id]);
        controller.submit(task).await.unwrap();

        // The least loaded node was chosen, so only it holds the data.
        assert!(catalog.holds(descriptor.id, first.id));
        assert!(!catalog.holds(descriptor.id, second.id));
        assert_eq!(controller.data_bytes_sent(), 100);
    }

    #[tokio::test]
    async fn a_large_input_is_split_into_chunks() {
        let catalog = DataCatalog::new();
        let mut controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        )
        .with_chunk_size(1024);
        controller.registry_mut().register(node("only", 0.2));

        let dataset: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let descriptor = controller.publish(dataset.clone());
        assert_eq!(controller.manifest(descriptor.id).unwrap().len(), 4);

        controller
            .submit(Task::new(kind::ECHO, Vec::new()).with_inputs(vec![descriptor.id]))
            .await
            .unwrap();

        // Every chunk arrived and the receiver rebuilt the original bytes.
        assert_eq!(controller.data_bytes_sent(), 4096);
        assert_eq!(
            controller
                .transport()
                .store()
                .get(descriptor.id)
                .unwrap()
                .as_ref(),
            dataset.as_slice()
        );
    }

    #[tokio::test]
    async fn repeated_chunks_are_sent_only_once() {
        let catalog = DataCatalog::new();
        let mut controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        )
        .with_chunk_size(1024);
        controller.registry_mut().register(node("only", 0.2));

        // Four identical chunks: only the first is worth sending.
        let descriptor = controller.publish(vec![5u8; 4096]);
        controller
            .submit(Task::new(kind::ECHO, Vec::new()).with_inputs(vec![descriptor.id]))
            .await
            .unwrap();

        assert_eq!(controller.data_bytes_sent(), 1024);
        assert_eq!(controller.chunks_skipped(), 3);
    }

    #[tokio::test]
    async fn a_compressible_input_goes_over_the_wire_smaller() {
        let catalog = DataCatalog::new();
        let mut controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        );
        // Slow link: compression is worth the CPU.
        let mut info = node("slow", 0.2);
        info.bandwidth_bytes_per_sec = Some(1024 * 1024);
        controller.registry_mut().register(info);

        let dataset = vec![0xcd; 256 * 1024];
        let descriptor = controller.publish(dataset.clone());
        controller
            .submit(Task::new(kind::ECHO, Vec::new()).with_inputs(vec![descriptor.id]))
            .await
            .unwrap();

        assert_eq!(controller.data_bytes_uncompressed(), dataset.len() as u64);
        assert!(controller.data_bytes_sent() < dataset.len() as u64 / 4);
        assert_eq!(
            controller
                .transport()
                .store()
                .get(descriptor.id)
                .unwrap()
                .as_ref(),
            dataset.as_slice()
        );
    }

    #[tokio::test]
    async fn a_fast_link_skips_compression() {
        let catalog = DataCatalog::new();
        let mut controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        );
        let mut info = node("fast", 0.2);
        info.bandwidth_bytes_per_sec = Some(10 * 1024 * 1024 * 1024);
        controller.registry_mut().register(info);

        let descriptor = controller.publish(vec![0xcd; 256 * 1024]);
        controller
            .submit(Task::new(kind::ECHO, Vec::new()).with_inputs(vec![descriptor.id]))
            .await
            .unwrap();

        assert_eq!(controller.data_bytes_sent(), 256 * 1024);
    }

    #[tokio::test]
    async fn compression_can_be_turned_off() {
        let catalog = DataCatalog::new();
        let mut controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        )
        .with_compression(aether_core::CompressionPolicy::disabled());
        controller.registry_mut().register(node("slow", 0.2));

        let descriptor = controller.publish(vec![0xcd; 128 * 1024]);
        controller
            .submit(Task::new(kind::ECHO, Vec::new()).with_inputs(vec![descriptor.id]))
            .await
            .unwrap();

        assert_eq!(controller.data_bytes_sent(), 128 * 1024);
    }

    /// A transport where a chosen set of nodes is unreachable.
    struct FlakyMesh {
        broken: HashSet<NodeId>,
        attempts: Vec<NodeId>,
    }

    impl FlakyMesh {
        fn new(broken: impl IntoIterator<Item = NodeId>) -> Self {
            Self {
                broken: broken.into_iter().collect(),
                attempts: Vec::new(),
            }
        }
    }

    impl TaskTransport for FlakyMesh {
        async fn dispatch(
            &mut self,
            node_id: NodeId,
            task: &Task,
        ) -> Result<TaskResult, DispatchError> {
            self.attempts.push(node_id);
            if self.broken.contains(&node_id) {
                return Err(DispatchError::Unreachable {
                    node_id,
                    reason: "node is down".to_string(),
                });
            }
            Ok(TaskResult::success(
                task.id,
                node_id,
                b"ok".to_vec(),
                Duration::from_millis(1),
            ))
        }

        async fn send_data(
            &mut self,
            _node_id: NodeId,
            _descriptor: DataDescriptor,
            _codec: Codec,
            _bytes: &[u8],
        ) -> Result<(), DispatchError> {
            Ok(())
        }

        async fn send_manifest(
            &mut self,
            _node_id: NodeId,
            _manifest: &ChunkManifest,
        ) -> Result<(), DispatchError> {
            Ok(())
        }

        async fn send_chunk(
            &mut self,
            _node_id: NodeId,
            _data_id: DataId,
            _index: u32,
            _codec: Codec,
            _bytes: &[u8],
        ) -> Result<(), DispatchError> {
            Ok(())
        }
    }

    fn flaky_controller(
        broken: impl IntoIterator<Item = NodeId>,
    ) -> Controller<LeastLoadedScheduler, FlakyMesh> {
        Controller::new(
            LeastLoadedScheduler::new(),
            FlakyMesh::new(broken),
            DataCatalog::new(),
        )
        .with_retry(RetryPolicy {
            max_attempts: 3,
            backoff: Duration::ZERO,
        })
    }

    #[tokio::test]
    async fn a_task_moves_to_another_node_when_one_is_down() {
        let first = node("down", 0.1);
        let second = node("up", 0.5);
        let mut controller = flaky_controller([first.id]);
        controller.registry_mut().register(first.clone());
        controller.registry_mut().register(second.clone());

        let result = controller
            .submit(Task::new(kind::ECHO, Vec::new()))
            .await
            .unwrap();

        assert_eq!(result.node_id, second.id);
        assert_eq!(controller.retries(), 1);
        assert_eq!(controller.transport().attempts, vec![first.id, second.id]);
    }

    #[tokio::test]
    async fn a_task_fails_once_every_node_has_been_tried() {
        let first = node("down-a", 0.1);
        let second = node("down-b", 0.5);
        let mut controller = flaky_controller([first.id, second.id]);
        controller.registry_mut().register(first);
        controller.registry_mut().register(second);

        let task = Task::new(kind::ECHO, Vec::new());
        let id = task.id;

        assert_eq!(
            controller.submit(task).await,
            Err(DispatchError::NoNodeAvailable(id))
        );
        assert_eq!(controller.retries(), 2);
    }

    #[tokio::test]
    async fn retrying_can_be_switched_off() {
        let only = node("down", 0.1);
        let mut controller = Controller::new(
            LeastLoadedScheduler::new(),
            FlakyMesh::new([only.id]),
            DataCatalog::new(),
        )
        .with_retry(RetryPolicy::none());
        controller.registry_mut().register(only.clone());

        let error = controller
            .submit(Task::new(kind::ECHO, Vec::new()))
            .await
            .unwrap_err();

        assert!(matches!(error, DispatchError::Unreachable { .. }));
        assert_eq!(controller.retries(), 0);
    }

    #[tokio::test]
    async fn a_task_that_ran_and_failed_is_not_retried() {
        let mut controller = controller();
        controller.registry_mut().register(node("only", 0.2));

        let result = controller
            .submit(Task::new("quantum", Vec::new()))
            .await
            .unwrap();

        assert!(!result.is_success());
        assert_eq!(controller.retries(), 0);
    }

    #[tokio::test]
    async fn an_unpublished_input_is_rejected_before_dispatch() {
        let mut controller = controller();
        controller.registry_mut().register(node("only", 0.2));
        let missing = DataId::of(b"never published");

        let task = Task::new(kind::ECHO, Vec::new()).with_inputs(vec![missing]);
        assert_eq!(
            controller.submit(task).await,
            Err(DispatchError::UnknownInput(missing))
        );
    }
}
