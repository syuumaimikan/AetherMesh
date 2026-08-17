//! Live agent connections and the tasks currently in flight on them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aether_core::{NodeId, TaskId, TaskResult};
use aether_protocol::Message;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::dispatch::DispatchError;

#[derive(Default)]
struct Inner {
    /// Outbound queue of each connected agent.
    senders: HashMap<NodeId, mpsc::UnboundedSender<Message>>,
    /// Submitters waiting for a result, keyed by task.
    pending: HashMap<TaskId, oneshot::Sender<TaskResult>>,
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

    /// Registers the outbound queue of a freshly connected agent.
    pub fn attach(&self, node_id: NodeId, sender: mpsc::UnboundedSender<Message>) {
        self.lock().senders.insert(node_id, sender);
    }

    /// Drops an agent that disconnected.
    pub fn detach(&self, node_id: NodeId) {
        self.lock().senders.remove(&node_id);
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
        self.inner.lock().expect("connections mutex poisoned")
    }
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
            .send(node_id, Message::RegisterAccepted { node_id })
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            Message::RegisterAccepted { node_id }
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
            .send(node_id, Message::RegisterAccepted { node_id })
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
