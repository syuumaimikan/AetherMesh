//! Live agent connections and the tasks currently in flight on them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aether_core::{DataId, NodeId, TaskId, TaskResult};
use aether_protocol::Message;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::dispatch::DispatchError;

#[derive(Default)]
struct Inner {
    /// Outbound queue of each connected agent.
    senders: HashMap<NodeId, mpsc::UnboundedSender<Message>>,
    /// Extra connections an agent offered for bulk data, plus the cursor used
    /// to spread chunks across them.
    data_channels: HashMap<NodeId, Vec<mpsc::UnboundedSender<Message>>>,
    next_channel: HashMap<NodeId, usize>,
    /// Secret handed to each node at registration, required to attach a data
    /// channel in that node's name.
    channel_tokens: HashMap<NodeId, String>,
    /// Submitters waiting for a result, keyed by task.
    pending: HashMap<TaskId, oneshot::Sender<TaskResult>>,
    /// Link probes waiting for their pong.
    pending_pongs: HashMap<(NodeId, u64), oneshot::Sender<()>>,
    /// Transfers waiting for the agent to confirm a dataset is complete.
    pending_data: HashMap<(NodeId, DataId), oneshot::Sender<()>>,
}

/// Shared handle to every connected agent. Cheap to clone.
#[derive(Clone, Default)]
pub struct Connections {
    inner: Arc<Mutex<Inner>>,
}

impl Connections {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the outbound queue of a freshly connected agent, and returns
    /// the secret its extra data connections must present.
    ///
    /// The mesh token says "you may join"; this says "you are that node". Only
    /// the agent that just registered is told this value, so nobody else can
    /// attach a data channel in its name and be handed its data.
    pub fn attach(&self, node_id: NodeId, sender: mpsc::UnboundedSender<Message>) -> String {
        let token = channel_token();
        let mut inner = self.lock();
        inner.senders.insert(node_id, sender);
        inner.channel_tokens.insert(node_id, token.clone());
        token
    }

    /// Checks a data channel's claim to belong to `node_id`.
    ///
    /// The token stays valid for the life of the registration, because an agent
    /// may open its channels at any point while it is connected.
    pub fn claim_channel_token(&self, node_id: NodeId, presented: Option<&str>) -> bool {
        let inner = self.lock();
        let (Some(expected), Some(presented)) = (inner.channel_tokens.get(&node_id), presented)
        else {
            return false;
        };

        // Constant time, like every other credential comparison here.
        expected.len() == presented.len()
            && expected
                .bytes()
                .zip(presented.bytes())
                .fold(0u8, |difference, (a, b)| difference | (a ^ b))
                == 0
    }

    /// Adds an extra connection an agent offered for bulk data.
    pub fn attach_data_channel(&self, node_id: NodeId, sender: mpsc::UnboundedSender<Message>) {
        self.lock()
            .data_channels
            .entry(node_id)
            .or_default()
            .push(sender);
    }

    /// How many bulk-data connections a node currently offers.
    pub fn data_channel_count(&self, node_id: NodeId) -> usize {
        self.lock()
            .data_channels
            .get(&node_id)
            .map(Vec::len)
            .unwrap_or(0)
    }

    /// Drops an agent that disconnected, along with its data channels.
    pub fn detach(&self, node_id: NodeId) {
        let mut inner = self.lock();
        inner.senders.remove(&node_id);
        inner.data_channels.remove(&node_id);
        inner.next_channel.remove(&node_id);
        inner.channel_tokens.remove(&node_id);
    }

    pub fn is_connected(&self, node_id: NodeId) -> bool {
        self.lock().senders.contains_key(&node_id)
    }

    pub fn connected_count(&self) -> usize {
        self.lock().senders.len()
    }

    /// Queues a message for an agent.
    pub fn send(&self, node_id: NodeId, message: Message) -> Result<(), DispatchError> {
        let inner = self.lock();
        let sender = inner
            .senders
            .get(&node_id)
            .ok_or_else(|| DispatchError::Unreachable {
                node_id,
                reason: "no live connection".to_string(),
            })?;

        sender
            .send(message)
            .map_err(|_| DispatchError::Unreachable {
                node_id,
                reason: "connection is shutting down".to_string(),
            })
    }

    /// Starts waiting for the result of `task_id`.
    pub fn expect_result(&self, task_id: TaskId) -> oneshot::Receiver<TaskResult> {
        let (sender, receiver) = oneshot::channel();
        self.lock().pending.insert(task_id, sender);
        receiver
    }

    /// Stops waiting for a task (timeout, or the send failed).
    pub fn forget(&self, task_id: TaskId) {
        self.lock().pending.remove(&task_id);
    }

    /// Queues a message on one of the node's bulk-data connections, falling
    /// back to the control connection when it offered none.
    ///
    /// Successive calls rotate through the channels, so a dataset's chunks go
    /// out over every connection the agent opened rather than one of them.
    pub fn send_bulk(&self, node_id: NodeId, message: Message) -> Result<(), DispatchError> {
        let sender = {
            let mut inner = self.lock();
            let Some(channels) = inner
                .data_channels
                .get(&node_id)
                .filter(|channels| !channels.is_empty())
                .cloned()
            else {
                drop(inner);
                return self.send(node_id, message);
            };

            let cursor = inner.next_channel.entry(node_id).or_insert(0);
            let index = *cursor % channels.len();
            *cursor = cursor.wrapping_add(1);
            channels[index].clone()
        };

        sender
            .send(message)
            .map_err(|_| DispatchError::Unreachable {
                node_id,
                reason: "data channel is shutting down".to_string(),
            })
    }

    /// Starts waiting for the agent to confirm it has assembled `data_id`.
    pub fn expect_data(&self, node_id: NodeId, data_id: DataId) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        self.lock().pending_data.insert((node_id, data_id), sender);
        receiver
    }

    /// Stops waiting for a dataset.
    pub fn forget_data(&self, node_id: NodeId, data_id: DataId) {
        self.lock().pending_data.remove(&(node_id, data_id));
    }

    /// Wakes whoever is waiting for this dataset to land.
    pub fn complete_data(&self, node_id: NodeId, data_id: DataId) {
        if let Some(sender) = self.lock().pending_data.remove(&(node_id, data_id)) {
            let _ = sender.send(());
        }
    }

    /// Starts waiting for the pong answering `nonce`.
    pub fn expect_pong(&self, node_id: NodeId, nonce: u64) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        self.lock().pending_pongs.insert((node_id, nonce), sender);
        receiver
    }

    /// Stops waiting for a pong.
    pub fn forget_pong(&self, node_id: NodeId, nonce: u64) {
        self.lock().pending_pongs.remove(&(node_id, nonce));
    }

    /// Wakes the probe waiting on this pong.
    pub fn complete_pong(&self, node_id: NodeId, nonce: u64) {
        if let Some(sender) = self.lock().pending_pongs.remove(&(node_id, nonce)) {
            let _ = sender.send(());
        }
    }

    /// Hands a finished result to whoever submitted the task.
    pub fn complete(&self, result: TaskResult) {
        let task_id = result.task_id;
        let waiter = self.lock().pending.remove(&task_id);
        match waiter {
            Some(sender) => {
                if sender.send(result).is_err() {
                    warn!(%task_id, "submitter stopped waiting for the result");
                }
            }
            None => warn!(%task_id, "result for an unknown or expired task"),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        aether_core::lock(&self.inner)
    }
}

/// A per-registration secret for data channels.
///
/// A UUID: unguessable, and it never leaves the pair of connections that need
/// it 窶・the controller hands it to the agent, the agent hands it back.
fn channel_token() -> String {
    aether_core::NodeId::generate().to_string()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn messages_reach_the_attached_agent() {
        let connections = Connections::new();
        let node_id = NodeId::generate();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        connections.attach(node_id, sender);

        connections
            .send(
                node_id,
                Message::RegisterAccepted {
                    node_id,
                    channel_token: None,
                    heartbeat_timeout_secs: 0,
                },
            )
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            Message::RegisterAccepted {
                node_id,
                channel_token: None,
                heartbeat_timeout_secs: 0
            }
        );
        assert!(connections.is_connected(node_id));
    }

    #[tokio::test]
    async fn sending_to_a_detached_agent_fails() {
        let connections = Connections::new();
        let node_id = NodeId::generate();
        let (sender, _receiver) = mpsc::unbounded_channel();
        connections.attach(node_id, sender);
        connections.detach(node_id);

        let error = connections
            .send(
                node_id,
                Message::RegisterAccepted {
                    node_id,
                    channel_token: None,
                    heartbeat_timeout_secs: 0,
                },
            )
            .unwrap_err();
        assert!(matches!(error, DispatchError::Unreachable { .. }));
        assert_eq!(connections.connected_count(), 0);
    }

    #[tokio::test]
    async fn a_completed_result_wakes_the_submitter() {
        let connections = Connections::new();
        let node_id = NodeId::generate();
        let task_id = TaskId::generate();
        let receiver = connections.expect_result(task_id);

        let result = TaskResult::success(task_id, node_id, vec![1], Duration::from_millis(1));
        connections.complete(result.clone());

        assert_eq!(receiver.await.unwrap(), result);
    }

    #[tokio::test]
    async fn forgotten_tasks_are_no_longer_delivered() {
        let connections = Connections::new();
        let task_id = TaskId::generate();
        let receiver = connections.expect_result(task_id);
        connections.forget(task_id);

        connections.complete(TaskResult::failure(
            task_id,
            NodeId::generate(),
            "late",
            Duration::ZERO,
        ));
        assert!(receiver.await.is_err());
    }
}
