//! Submit -> schedule -> transfer missing data -> dispatch -> result.
//!
//! The hop to the node sits behind [`TaskTransport`], so the same logic drives
//! the in-process simulation and a real connection.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
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
    #[error("the queue is full; task {task_id} was not accepted")]
    QueueFull { task_id: TaskId },
    #[error("task {task_id} waited {waited_ms} ms for a node and gave up")]
    QueueTimeout { task_id: TaskId, waited_ms: u64 },
}

/// Carries data and tasks to a node and brings results back.
pub trait TaskTransport {
    fn dispatch(
        &self,
        node_id: NodeId,
        task: &Task,
    ) -> impl Future<Output = Result<TaskResult, DispatchError>> + Send;

    /// Delivers a small dataset in one piece. Must arrive before any task that
    /// reads it. `bytes` is the wire form produced with `codec`.
    fn send_data(
        &self,
        node_id: NodeId,
        descriptor: DataDescriptor,
        codec: Codec,
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;

    /// Announces a chunked dataset before its chunks are sent.
    fn send_manifest(
        &self,
        node_id: NodeId,
        manifest: &ChunkManifest,
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;

    /// Delivers one chunk of an announced dataset. Chunks are independent, so
    /// a transport is free to send them concurrently.
    fn send_chunk(
        &self,
        node_id: NodeId,
        data_id: DataId,
        index: u32,
        codec: Codec,
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;

    /// Delivers several chunks at once.
    ///
    /// The default sends them one after another. A transport that can pipeline
    /// — queue every chunk without waiting for each to be flushed — should
    /// override this; over a socket that is the difference between one
    /// round trip per chunk and one for the batch.
    fn send_chunks(
        &self,
        node_id: NodeId,
        data_id: DataId,
        chunks: Vec<(u32, Codec, Vec<u8>)>,
    ) -> impl Future<Output = Result<(), DispatchError>> + Send
    where
        Self: Sync,
    {
        async move {
            for (index, codec, bytes) in chunks {
                self.send_chunk(node_id, data_id, index, codec, &bytes)
                    .await?;
            }
            Ok(())
        }
    }

    /// Whether this node can be reached right now.
    ///
    /// The registry is refreshed on a timer and heartbeats have a timeout, so
    /// between a node closing its connection and the health monitor noticing,
    /// the scheduler will happily pick it. Asking the transport first turns
    /// that into a skip rather than a failed dispatch and a burnt retry.
    ///
    /// The default says yes: a transport with no notion of a connection — a
    /// simulated mesh, a test double — has nothing useful to report.
    fn is_available(&self, node_id: NodeId) -> bool {
        let _ = node_id;
        true
    }
}

/// Whether another node is worth trying.
///
/// A missing input is the submitter's mistake and no node can fix it, and a
/// task that never got out of the queue never reached a node to begin with.
fn is_retryable(error: &DispatchError) -> bool {
    matches!(
        error,
        DispatchError::Unreachable { .. } | DispatchError::Timeout { .. }
    )
}

/// Held while one dataset is on its way to one node.
type TransferLock = Arc<tokio::sync::Mutex<()>>;

/// Owns the registry and drives task placement.
pub struct Controller<S, T> {
    registry: Arc<Mutex<NodeRegistry>>,
    scheduler: S,
    transport: T,
    catalog: DataCatalog,
    store: DataStore,
    /// Chunk layout of every published dataset large enough to be split.
    /// Shared so publishing does not need exclusive access to the whole
    /// controller while tasks are being dispatched.
    manifests: Arc<Mutex<HashMap<DataId, ChunkManifest>>>,
    /// Tasks dispatched to each node and not yet finished.
    ///
    /// A node reports its load on a heartbeat, so for the seconds between one
    /// and the next it looks exactly as idle as it did before the work
    /// arrived. Without this, dispatching sixty-four tasks at once sends all
    /// sixty-four to whichever node scored best for the first — measured, on a
    /// four-node mesh.
    in_flight: Arc<Mutex<HashMap<NodeId, u32>>>,
    chunk_size: usize,
    compression: CompressionPolicy,
    /// When false, inputs are re-sent even if the node already has them.
    /// Only useful for baseline measurements.
    reuse_data: bool,
    retry: RetryPolicy,
    /// Results of finished work, when the operator turned caching on.
    cache: Option<crate::cache::ResultCache>,
    /// Bytes moved and bytes saved. Shared rather than private so the client
    /// API and the scrape endpoint can read them; this task owns the
    /// `Controller` exclusively, and these numbers are the point of the project.
    traffic: crate::observability::TrafficStats,
    /// The last few finished tasks, so a watcher can see what ran rather than
    /// only how many. Shared: the client API reads it while this task writes.
    history: crate::history::History,
    /// One lock per (node, dataset) being transferred right now.
    ///
    /// Two tasks dispatched at the same time can want the same input on the
    /// same node. Without this they both send it: the bytes go twice, and —
    /// worse — the agent sees two interleaved chunk streams for one dataset
    /// and rejects the ones whose manifest it has not seen. Measured before
    /// this existed: sixteen concurrent tasks over one 4 MiB dataset moved
    /// 20 MiB across three nodes and retried eleven times.
    transfers: Arc<Mutex<HashMap<(NodeId, DataId), TransferLock>>>,
    /// Where finished workflow steps are recorded, when the operator asked for
    /// it. Shared because a workflow's steps run concurrently and each one
    /// records itself as it lands.
    checkpoint: Option<Arc<crate::checkpoint::Journal>>,
}

// `Send` is required because the transport batches chunk sends, and a batched
// send has to be able to cross an await point in a spawned task.
impl<S: Scheduler, T: TaskTransport + Send + Sync> Controller<S, T> {
    /// Builds a controller whose scheduler reads the same catalog it writes.
    pub fn new(scheduler: S, transport: T, catalog: DataCatalog) -> Self {
        Self {
            registry: Arc::new(Mutex::new(NodeRegistry::new())),
            scheduler,
            transport,
            catalog,
            store: DataStore::new(),
            manifests: Arc::new(Mutex::new(HashMap::new())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            chunk_size: DEFAULT_CHUNK_SIZE,
            compression: CompressionPolicy::default(),
            reuse_data: true,
            retry: RetryPolicy::default(),
            cache: None,
            traffic: crate::observability::TrafficStats::new(),
            history: crate::history::History::default(),
            transfers: Arc::new(Mutex::new(HashMap::new())),
            checkpoint: None,
        }
    }

    /// The last few finished tasks.
    pub fn history(&self) -> &crate::history::History {
        &self.history
    }

    /// Uses an existing ring, so the client API and this controller agree on
    /// what ran even when they were built separately.
    pub fn with_history(mut self, history: crate::history::History) -> Self {
        self.history = history;
        self
    }

    /// Records finished workflow steps to `journal`, so a named run that is
    /// submitted again picks up where it stopped.
    ///
    /// Only [`crate::flow::run_workflow_resumable`] reads or writes it. An
    /// ordinary submission is not journalled: a single task has nothing to
    /// resume onto.
    pub fn with_checkpoint(mut self, journal: Arc<crate::checkpoint::Journal>) -> Self {
        self.checkpoint = Some(journal);
        self
    }

    /// The journal, when one was configured.
    pub fn checkpoint(&self) -> Option<&Arc<crate::checkpoint::Journal>> {
        self.checkpoint.as_ref()
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

    /// Reports traffic into these shared counters instead of its own.
    ///
    /// Pass `MeshState::traffic` and the client API sees the same numbers this
    /// controller is producing.
    pub fn with_traffic_stats(mut self, traffic: crate::observability::TrafficStats) -> Self {
        self.traffic = traffic;
        self
    }

    /// Everything moved and everything saved, so far.
    pub fn traffic(&self) -> crate::observability::TrafficSnapshot {
        self.traffic.snapshot()
    }

    /// Sets how a task is retried when a node cannot take it.
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Answers repeated work from a cache instead of dispatching it.
    ///
    /// Only correct for deterministic tasks. A module granted the clock or
    /// randomness is not one, which is why this is a decision the operator
    /// makes rather than a default.
    pub fn with_result_cache(mut self, cache: crate::cache::ResultCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// The result cache, if one is attached.
    pub fn cache(&self) -> Option<&crate::cache::ResultCache> {
        self.cache.as_ref()
    }

    /// Tasks re-dispatched after a delivery failure.
    pub fn retries(&self) -> u64 {
        self.traffic.snapshot().retries
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

    /// The controller's view of the mesh, locked for reading.
    pub fn registry(&self) -> std::sync::MutexGuard<'_, NodeRegistry> {
        aether_core::lock(&self.registry)
    }

    /// Adds a node to the controller's view.
    ///
    /// Shared rather than owned so a dispatch does not need exclusive access
    /// to the whole controller: with several tasks in flight there is no
    /// single owner to hand a `&mut` to.
    pub fn register(&self, info: aether_core::NodeInfo) {
        aether_core::lock(&self.registry).register(info);
    }

    /// Replaces the controller's view of the mesh with `nodes`.
    ///
    /// The live registry belongs to the server; a long-running controller calls
    /// this before scheduling so it never places work on a node that has left.
    pub fn sync_registry(&self, nodes: Vec<aether_core::NodeInfo>) {
        let mut registry = NodeRegistry::new();
        for info in nodes {
            registry.register(info);
        }
        *aether_core::lock(&self.registry) = registry;
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn catalog(&self) -> &DataCatalog {
        &self.catalog
    }

    /// Bytes actually put on the wire for task inputs, after compression.
    pub fn data_bytes_sent(&self) -> u64 {
        self.traffic.snapshot().data_bytes_sent
    }

    /// What those transfers would have cost uncompressed.
    pub fn data_bytes_uncompressed(&self) -> u64 {
        self.traffic.snapshot().data_bytes_uncompressed
    }

    /// Transfers avoided because the node already held the data.
    pub fn transfers_skipped(&self) -> u64 {
        self.traffic.snapshot().transfers_skipped
    }

    /// Chunks avoided because the node already held that exact chunk.
    pub fn chunks_skipped(&self) -> u64 {
        self.traffic.snapshot().chunks_skipped
    }

    /// Chunk layout of a published dataset, if it is large enough to be split.
    pub fn manifest(&self, data_id: DataId) -> Option<ChunkManifest> {
        self.manifests().get(&data_id).cloned()
    }

    fn manifests(&self) -> std::sync::MutexGuard<'_, HashMap<DataId, ChunkManifest>> {
        aether_core::lock(&self.manifests)
    }

    /// Registers data with the controller and returns its content address.
    /// Publishing the same bytes twice yields the same descriptor.
    ///
    /// Data larger than one chunk is split now, so the layout is computed once
    /// however many nodes it is later sent to.
    pub fn publish(&self, bytes: Vec<u8>) -> DataDescriptor {
        let descriptor = self.store.put(bytes);
        if descriptor.size_bytes > self.chunk_size as u64 {
            // Only a bounded store can lose a blob between putting and reading
            // it, and the controller's is not bounded today. Handled anyway:
            // the cost of being wrong is the dataset travelling whole instead
            // of in chunks, which is slower, not incorrect.
            let Some(bytes) = self.store.get(descriptor.id) else {
                warn!(data_id = %descriptor.id, "published data was dropped before it could be split");
                return descriptor;
            };
            self.manifests()
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
    #[tracing::instrument(
        name = "submit",
        skip_all,
        fields(
            task_id = %task.id,
            kind = %task.kind,
            priority = ?task.priority,
            inputs = task.inputs.len(),
            node_id = tracing::field::Empty,
            attempts = tracing::field::Empty,
        )
    )]
    pub async fn submit(&self, task: Task) -> Result<TaskResult, DispatchError> {
        // Identical work has an identical answer, so the cheapest dispatch is
        // the one that does not happen.
        if let Some(cache) = &self.cache
            && let Some(result) = cache.get(&task)
        {
            debug!(task_id = %task.id, "answered from the result cache");
            return Ok(result);
        }

        let mut unusable: HashSet<NodeId> = HashSet::new();

        for attempt in 1..=self.retry.max_attempts {
            let nodes: Vec<_> = self
                .nodes_with_pending_work()
                .into_iter()
                .filter(|node| !unusable.contains(&node.id) && self.transport.is_available(node.id))
                .collect();

            let node_id = self
                .scheduler
                .select_node(&nodes, &task)
                .ok_or(DispatchError::NoNodeAvailable(task.id))?;

            let span = tracing::Span::current();
            span.record("node_id", tracing::field::display(node_id));
            span.record("attempts", attempt);

            self.begin_on(node_id);
            let attempted = self.try_once(node_id, &task).await;
            self.finished_on(node_id);

            match attempted {
                Ok(result) => {
                    self.history.record(&task, &result);
                    self.record_output(&result);
                    if let Some(cache) = &self.cache {
                        cache.put(&task, &result);
                    }
                    return Ok(result);
                }
                Err(error) if attempt < self.retry.max_attempts && is_retryable(&error) => {
                    warn!(%node_id, task_id = %task.id, attempt, %error, "retrying on another node");
                    // The data this node was credited with is no longer usable.
                    self.catalog.forget_node(node_id);
                    unusable.insert(node_id);
                    self.traffic.record_retry();
                    if !self.retry.backoff.is_zero() {
                        tokio::time::sleep(self.retry.backoff).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }

        Err(DispatchError::NoNodeAvailable(task.id))
    }

    /// Notes where a task left its output, so a later task that reads it is
    /// scheduled onto the node that already has it.
    ///
    /// The controller keeps a copy too — the result came back over the wire
    /// regardless — which is what lets a dependent run somewhere else if the
    /// producing node has since left.
    fn record_output(&self, result: &TaskResult) {
        let Some(output_id) = result.output_id else {
            return;
        };
        let Some(output) = result.output() else {
            return;
        };

        let descriptor = DataDescriptor::new(output_id, output.len() as u64);
        // The store verifies the hash, so a node cannot make the controller
        // hold bytes under an id they do not belong to.
        if let Err(error) = self.store.insert(descriptor, output.to_vec()) {
            warn!(%error, node_id = %result.node_id, "rejecting a task output");
            return;
        }
        self.catalog.record(descriptor, result.node_id);
        debug!(
            %output_id,
            node_id = %result.node_id,
            bytes = output.len(),
            "task output is available where it was produced"
        );
    }

    /// One placement attempt: move the missing inputs, then run the task.
    async fn try_once(&self, node_id: NodeId, task: &Task) -> Result<TaskResult, DispatchError> {
        self.ensure_inputs(node_id, task).await?;
        self.dispatch_to(node_id, task).await
    }

    /// The wait for a node to answer, as its own span.
    #[tracing::instrument(name = "dispatch", skip_all, fields(%node_id, task_id = %task.id))]
    async fn dispatch_to(&self, node_id: NodeId, task: &Task) -> Result<TaskResult, DispatchError> {
        self.transport.dispatch(node_id, task).await
    }

    /// Sends the task's inputs that the node does not have yet.
    #[tracing::instrument(
        name = "send_inputs",
        skip_all,
        fields(%node_id, inputs = task.inputs.len())
    )]
    async fn ensure_inputs(&self, node_id: NodeId, task: &Task) -> Result<(), DispatchError> {
        for data_id in &task.inputs {
            // One transfer of a dataset to a node at a time. Whoever gets here
            // second waits, and then finds the catalog already says the node
            // has it — so the second check below is not redundant with the
            // first, it is the whole point.
            let transfer = self.transfer_lock(node_id, *data_id);
            let guard = transfer.lock().await;

            if self.reuse_data && self.catalog.holds(*data_id, node_id) {
                self.traffic.record_transfer_skipped();
                debug!(%node_id, %data_id, "input already on the node");
                drop(guard);
                self.release_transfer(node_id, *data_id, transfer);
                continue;
            }

            let bytes = self
                .store
                .get(*data_id)
                .ok_or(DispatchError::UnknownInput(*data_id))?;
            let descriptor = DataDescriptor::new(*data_id, bytes.len() as u64);

            let bandwidth = self.bandwidth_to(node_id);
            let manifest = self.manifests().get(data_id).cloned();
            match manifest {
                Some(manifest) => {
                    self.send_chunked(node_id, &manifest, &bytes, bandwidth)
                        .await?
                }
                None => {
                    let (codec, payload) = self.compression.encode(&bytes, bandwidth);
                    self.transport
                        .send_data(node_id, descriptor, codec, &payload)
                        .await?;
                    self.traffic
                        .record_sent(payload.len() as u64, descriptor.size_bytes);
                }
            }

            self.catalog.record(descriptor, node_id);
            debug!(%node_id, %data_id, size = descriptor.size_bytes, "input transferred");
            drop(guard);
            self.release_transfer(node_id, *data_id, transfer);
        }
        Ok(())
    }

    /// The lock covering "this dataset going to this node", creating it if
    /// this is the first task to want it.
    fn transfer_lock(&self, node_id: NodeId, data_id: DataId) -> TransferLock {
        aether_core::lock(&self.transfers)
            .entry((node_id, data_id))
            .or_default()
            .clone()
    }

    /// Forgets the lock once nobody else is holding it.
    ///
    /// Without this the map grows by one entry per dataset per node and never
    /// shrinks, which on a long-lived controller is a slow leak rather than a
    /// bug you would notice.
    fn release_transfer(&self, node_id: NodeId, data_id: DataId, transfer: TransferLock) {
        let mut transfers = aether_core::lock(&self.transfers);
        // Two references: the map's and ours. Anything more means somebody
        // else is waiting on it and it has to stay.
        if Arc::strong_count(&transfer) <= 2 {
            transfers.remove(&(node_id, data_id));
        }
    }

    /// The node list as the scheduler should see it: reported load, plus the
    /// work this controller has already sent and not yet heard back about.
    ///
    /// Each outstanding task counts as one core's worth of the node, which is
    /// crude and is meant to be: the point is that a node holding four tasks
    /// stops looking as attractive as one holding none, not that the number is
    /// an accurate prediction of anything.
    fn nodes_with_pending_work(&self) -> Vec<aether_core::NodeInfo> {
        let in_flight = aether_core::lock(&self.in_flight);
        self.registry()
            .nodes()
            .into_iter()
            .map(|mut node| {
                let pending = in_flight.get(&node.id).copied().unwrap_or(0);
                if pending > 0 {
                    let share = pending as f32 / node.cpu_cores.max(1) as f32;
                    node.metrics = aether_core::NodeMetrics::new(
                        node.metrics.cpu_usage + share,
                        node.metrics.memory_usage,
                        node.metrics.memory_total_bytes,
                    );
                }
                node
            })
            .collect()
    }

    fn begin_on(&self, node_id: NodeId) {
        *aether_core::lock(&self.in_flight)
            .entry(node_id)
            .or_insert(0) += 1;
    }

    fn finished_on(&self, node_id: NodeId) {
        let mut in_flight = aether_core::lock(&self.in_flight);
        if let Some(pending) = in_flight.get_mut(&node_id) {
            *pending = pending.saturating_sub(1);
            if *pending == 0 {
                in_flight.remove(&node_id);
            }
        }
    }

    /// Link speed toward a node, as far as the registry knows.
    fn bandwidth_to(&self, node_id: NodeId) -> Option<u64> {
        self.registry()
            .get(node_id)
            .and_then(|entry| entry.info.bandwidth_bytes_per_sec)
    }

    /// Sends a manifest followed by the chunks the node is still missing.
    async fn send_chunked(
        &self,
        node_id: NodeId,
        manifest: &ChunkManifest,
        bytes: &[u8],
        bandwidth: Option<u64>,
    ) -> Result<(), DispatchError> {
        let data_id = manifest.data.id;
        self.transport.send_manifest(node_id, manifest).await?;

        let mut batch = Vec::new();
        for (index, chunk) in manifest.indexed() {
            // Chunks are content-addressed, so a repeated chunk - inside this
            // dataset or shared with another - is never sent to a node twice.
            if self.reuse_data && self.catalog.holds(chunk.id, node_id) {
                self.traffic.record_chunk_skipped();
                continue;
            }

            let range = manifest
                .chunk_range(index)
                .expect("index comes from the manifest");
            // Each chunk is judged on its own: a compressible chunk is
            // compressed even if its neighbours are not.
            let (codec, payload) = self.compression.encode(&bytes[range], bandwidth);

            self.catalog.record(chunk, node_id);
            self.traffic
                .record_sent(payload.len() as u64, chunk.size_bytes);
            batch.push((index, codec, payload));
        }

        if batch.is_empty() {
            return Ok(());
        }
        // Handed over as one batch so the transport can pipeline it.
        self.transport.send_chunks(node_id, data_id, batch).await
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
        let controller = controller();
        let task = Task::new(kind::ECHO, b"hi".to_vec());
        let id = task.id;

        assert_eq!(
            controller.submit(task).await,
            Err(DispatchError::NoNodeAvailable(id))
        );
    }

    #[tokio::test]
    async fn task_lands_on_the_least_loaded_node() {
        let controller = controller();
        let busy = node("busy", 0.9);
        let idle = node("idle", 0.1);
        let idle_id = idle.id;
        controller.register(busy);
        controller.register(idle);

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
        let controller = controller();
        controller.register(node("only", 0.2));

        let result = controller
            .submit(Task::new("quantum", Vec::new()))
            .await
            .unwrap();

        assert!(!result.is_success());
        assert_eq!(result.output(), None);
    }

    #[tokio::test]
    async fn dispatching_counts_transferred_bytes() {
        let controller = controller();
        controller.register(node("only", 0.2));
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
        let controller = Controller::new(
            LocalityScheduler::new(catalog.clone()),
            SimulatedMesh::new(),
            catalog,
        );
        controller.register(node("only", 0.2));

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
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        );
        let first = node("a", 0.1);
        let second = node("b", 0.9);
        controller.register(first.clone());
        controller.register(second.clone());

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
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        )
        .with_chunk_size(1024);
        controller.register(node("only", 0.2));

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
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        )
        .with_chunk_size(1024);
        controller.register(node("only", 0.2));

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
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        );
        // Slow link: compression is worth the CPU.
        let mut info = node("slow", 0.2);
        info.bandwidth_bytes_per_sec = Some(1024 * 1024);
        controller.register(info);

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
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        );
        let mut info = node("fast", 0.2);
        info.bandwidth_bytes_per_sec = Some(10 * 1024 * 1024 * 1024);
        controller.register(info);

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
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::new(),
            catalog.clone(),
        )
        .with_compression(aether_core::CompressionPolicy::disabled());
        controller.register(node("slow", 0.2));

        let descriptor = controller.publish(vec![0xcd; 128 * 1024]);
        controller
            .submit(Task::new(kind::ECHO, Vec::new()).with_inputs(vec![descriptor.id]))
            .await
            .unwrap();

        assert_eq!(controller.data_bytes_sent(), 128 * 1024);
    }

    /// A transport that counts transfers and takes long enough for a second
    /// dispatch to arrive while the first is still sending.
    struct SlowTransfer {
        sends: Arc<Mutex<Vec<(NodeId, DataId)>>>,
        takes: Duration,
    }

    impl TaskTransport for SlowTransfer {
        async fn dispatch(
            &self,
            node_id: NodeId,
            task: &Task,
        ) -> Result<TaskResult, DispatchError> {
            Ok(TaskResult::success(
                task.id,
                node_id,
                b"ok".to_vec(),
                Duration::from_millis(1),
            ))
        }

        async fn send_data(
            &self,
            node_id: NodeId,
            descriptor: DataDescriptor,
            _codec: Codec,
            _bytes: &[u8],
        ) -> Result<(), DispatchError> {
            tokio::time::sleep(self.takes).await;
            self.sends
                .lock()
                .expect("sends mutex poisoned")
                .push((node_id, descriptor.id));
            Ok(())
        }

        async fn send_manifest(
            &self,
            _node_id: NodeId,
            _manifest: &ChunkManifest,
        ) -> Result<(), DispatchError> {
            Ok(())
        }

        async fn send_chunk(
            &self,
            _node_id: NodeId,
            _data_id: DataId,
            _index: u32,
            _codec: Codec,
            _bytes: &[u8],
        ) -> Result<(), DispatchError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn one_dataset_reaches_a_node_once_however_many_tasks_want_it() {
        let sends = Arc::new(Mutex::new(Vec::new()));
        let controller = Arc::new(Controller::new(
            LeastLoadedScheduler::new(),
            SlowTransfer {
                sends: sends.clone(),
                takes: Duration::from_millis(50),
            },
            DataCatalog::new(),
        ));
        controller.register(node("only", 0.1));

        // Small enough not to be chunked, so every transfer is one send_data.
        let descriptor = controller.publish(vec![0xab; 4096]);

        // Eight tasks reading the same input, dispatched at once. Before this
        // was single-flighted they all sent it: the bytes went eight times,
        // and the agent rejected chunk streams it had no manifest for.
        let mut running = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let controller = controller.clone();
            let input = descriptor.id;
            running.spawn(async move {
                controller
                    .submit(Task::new(kind::ECHO, Vec::new()).with_inputs(vec![input]))
                    .await
            });
        }
        while let Some(finished) = running.join_next().await {
            assert!(finished.unwrap().is_ok());
        }

        let sends = sends.lock().unwrap();
        assert_eq!(sends.len(), 1, "sent {} times: {sends:?}", sends.len());
        assert_eq!(controller.transfers_skipped(), 7);
    }

    /// A transport where a chosen set of nodes is unreachable.
    struct FlakyMesh {
        broken: HashSet<NodeId>,
        attempts: std::sync::Mutex<Vec<NodeId>>,
    }

    impl FlakyMesh {
        fn new(broken: impl IntoIterator<Item = NodeId>) -> Self {
            Self {
                broken: broken.into_iter().collect(),
                attempts: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl TaskTransport for FlakyMesh {
        async fn dispatch(
            &self,
            node_id: NodeId,
            task: &Task,
        ) -> Result<TaskResult, DispatchError> {
            self.attempts
                .lock()
                .expect("attempts mutex poisoned")
                .push(node_id);
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
            &self,
            _node_id: NodeId,
            _descriptor: DataDescriptor,
            _codec: Codec,
            _bytes: &[u8],
        ) -> Result<(), DispatchError> {
            Ok(())
        }

        async fn send_manifest(
            &self,
            _node_id: NodeId,
            _manifest: &ChunkManifest,
        ) -> Result<(), DispatchError> {
            Ok(())
        }

        async fn send_chunk(
            &self,
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
        let controller = flaky_controller([first.id]);
        controller.register(first.clone());
        controller.register(second.clone());

        let result = controller
            .submit(Task::new(kind::ECHO, Vec::new()))
            .await
            .unwrap();

        assert_eq!(result.node_id, second.id);
        assert_eq!(controller.retries(), 1);
        assert_eq!(
            *controller
                .transport()
                .attempts
                .lock()
                .expect("attempts mutex poisoned"),
            vec![first.id, second.id]
        );
    }

    #[tokio::test]
    async fn a_task_fails_once_every_node_has_been_tried() {
        let first = node("down-a", 0.1);
        let second = node("down-b", 0.5);
        let controller = flaky_controller([first.id, second.id]);
        controller.register(first);
        controller.register(second);

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
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            FlakyMesh::new([only.id]),
            DataCatalog::new(),
        )
        .with_retry(RetryPolicy::none());
        controller.register(only.clone());

        let error = controller
            .submit(Task::new(kind::ECHO, Vec::new()))
            .await
            .unwrap_err();

        assert!(matches!(error, DispatchError::Unreachable { .. }));
        assert_eq!(controller.retries(), 0);
    }

    #[tokio::test]
    async fn repeated_work_is_answered_from_the_cache() {
        let controller = controller().with_result_cache(crate::cache::ResultCache::new(16));
        controller.register(node("only", 0.2));

        let first = controller
            .submit(Task::new(kind::ECHO, b"same".to_vec()))
            .await
            .unwrap();
        let bytes_after_first = controller.transport().bytes_transferred();

        // A different task doing identical work.
        let second = controller
            .submit(Task::new(kind::ECHO, b"same".to_vec()))
            .await
            .unwrap();

        assert_eq!(first.output(), second.output());
        assert_eq!(
            controller.transport().bytes_transferred(),
            bytes_after_first,
            "the second submission should not have reached a node"
        );
        assert_eq!(controller.cache().unwrap().stats(), (1, 1));
    }

    #[tokio::test]
    async fn different_work_still_reaches_a_node() {
        let controller = controller().with_result_cache(crate::cache::ResultCache::new(16));
        controller.register(node("only", 0.2));

        controller
            .submit(Task::new(kind::ECHO, b"one".to_vec()))
            .await
            .unwrap();
        let bytes_after_first = controller.transport().bytes_transferred();

        let second = controller
            .submit(Task::new(kind::ECHO, b"two".to_vec()))
            .await
            .unwrap();

        assert_eq!(second.output(), Some(&b"two"[..]));
        assert!(controller.transport().bytes_transferred() > bytes_after_first);
    }

    #[tokio::test]
    async fn caching_is_off_unless_asked_for() {
        let controller = controller();
        controller.register(node("only", 0.2));

        controller
            .submit(Task::new(kind::ECHO, b"same".to_vec()))
            .await
            .unwrap();
        let bytes_after_first = controller.transport().bytes_transferred();
        controller
            .submit(Task::new(kind::ECHO, b"same".to_vec()))
            .await
            .unwrap();

        assert!(controller.transport().bytes_transferred() > bytes_after_first);
        assert!(controller.cache().is_none());
    }

    #[tokio::test]
    async fn a_task_that_ran_and_failed_is_not_retried() {
        let controller = controller();
        controller.register(node("only", 0.2));

        let result = controller
            .submit(Task::new("quantum", Vec::new()))
            .await
            .unwrap();

        assert!(!result.is_success());
        assert_eq!(controller.retries(), 0);
    }

    #[tokio::test]
    async fn an_unpublished_input_is_rejected_before_dispatch() {
        let controller = controller();
        controller.register(node("only", 0.2));
        let missing = DataId::of(b"never published");

        let task = Task::new(kind::ECHO, Vec::new()).with_inputs(vec![missing]);
        assert_eq!(
            controller.submit(task).await,
            Err(DispatchError::UnknownInput(missing))
        );
    }
}
