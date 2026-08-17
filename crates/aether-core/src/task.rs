//! Unit of work sent to a node and the result it sends back.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::data::DataId;
use crate::id::{NodeId, TaskId};

/// Names of the built-in operations every agent understands.
///
/// Tasks reference work by name; executable code is never sent over the wire.
pub mod kind {
    /// Returns the payload unchanged. Used to measure round-trip latency.
    pub const ECHO: &str = "echo";
    /// Returns the BLAKE3 digest of the payload.
    pub const HASH: &str = "hash";
    /// Runs a fixed amount of integer arithmetic (payload: iteration count).
    pub const CPU: &str = "cpu";
}

/// A unit of work.
///
/// `kind` names a built-in operation the agent knows how to run; `payload` is
/// its opaque input. Arbitrary code is never shipped in a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub kind: String,
    pub payload: Vec<u8>,
    /// Datasets this task reads. The scheduler prefers nodes that already hold them.
    pub inputs: Vec<DataId>,
}

impl Task {
    pub fn new(kind: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            id: TaskId::generate(),
            kind: kind.into(),
            payload,
            inputs: Vec::new(),
        }
    }

    /// Declares the datasets this task needs.
    pub fn with_inputs(mut self, inputs: Vec<DataId>) -> Self {
        self.inputs = inputs;
        self
    }

    /// Size of the input in bytes, used later by transfer-cost estimates.
    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }
}

/// How a task ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskOutcome {
    Success { output: Vec<u8> },
    Failure { message: String },
}

/// The outcome of running a task, reported by the node that ran it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: TaskId,
    pub node_id: NodeId,
    pub outcome: TaskOutcome,
    pub duration: Duration,
}

impl TaskResult {
    pub fn success(task_id: TaskId, node_id: NodeId, output: Vec<u8>, duration: Duration) -> Self {
        Self {
            task_id,
            node_id,
            outcome: TaskOutcome::Success { output },
            duration,
        }
    }

    pub fn failure(
        task_id: TaskId,
        node_id: NodeId,
        message: impl Into<String>,
        duration: Duration,
    ) -> Self {
        Self {
            task_id,
            node_id,
            outcome: TaskOutcome::Failure {
                message: message.into(),
            },
            duration,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self.outcome, TaskOutcome::Success { .. })
    }

    /// Output bytes on success, `None` on failure.
    pub fn output(&self) -> Option<&[u8]> {
        match &self.outcome {
            TaskOutcome::Success { output } => Some(output),
            TaskOutcome::Failure { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tasks_get_distinct_ids() {
        let a = Task::new("hash", vec![1, 2, 3]);
        let b = Task::new("hash", vec![1, 2, 3]);
        assert_ne!(a.id, b.id);
        assert_eq!(a.payload_len(), 3);
    }

    #[test]
    fn success_exposes_output() {
        let result = TaskResult::success(
            TaskId::generate(),
            NodeId::generate(),
            vec![7],
            Duration::from_millis(5),
        );
        assert!(result.is_success());
        assert_eq!(result.output(), Some(&[7u8][..]));
    }

    #[test]
    fn failure_carries_a_message_and_no_output() {
        let result = TaskResult::failure(
            TaskId::generate(),
            NodeId::generate(),
            "unknown kind",
            Duration::ZERO,
        );
        assert!(!result.is_success());
        assert_eq!(result.output(), None);
        assert_eq!(
            result.outcome,
            TaskOutcome::Failure {
                message: "unknown kind".to_string()
            }
        );
    }
}
