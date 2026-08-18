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
    /// Runs the task's WebAssembly module over the payload.
    ///
    /// This is how work written in TypeScript, Go, C, or anything else with a
    /// WASM target runs on a node — sandboxed, with no host access.
    pub const WASM: &str = "wasm";
}

/// How urgently a task wants a node.
///
/// The order matters and is the whole point: `Critical` outranks `High`
/// outranks `Normal`, and so on down. Anything that does not say gets
/// `Normal`, so a caller who never heard of priorities is unaffected.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    /// Runs after everything else, if there is room. Backfill work.
    Background,
    Low,
    #[default]
    Normal,
    High,
    /// Ahead of everything waiting. Reserve it for work that is worth
    /// delaying other people's.
    Critical,
}

impl Priority {
    /// Every level, lowest first.
    pub const ALL: [Self; 5] = [
        Self::Background,
        Self::Low,
        Self::Normal,
        Self::High,
        Self::Critical,
    ];

    /// The next level up, or `Critical` if there is none.
    ///
    /// This is what waiting buys a task: a queue that only ever ran the
    /// highest priority would never run the lowest at all.
    pub fn promoted(self) -> Self {
        match self {
            Self::Background => Self::Low,
            Self::Low => Self::Normal,
            Self::Normal => Self::High,
            Self::High | Self::Critical => Self::Critical,
        }
    }

    /// The name used on the wire and in a CLI.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Background => "background",
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A priority could not be read from its name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{input}` is not a priority (critical, high, normal, low, background)")]
pub struct PriorityParseError {
    pub input: String,
}

impl std::str::FromStr for Priority {
    type Err = PriorityParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.trim().to_ascii_lowercase().as_str() {
            "critical" => Ok(Self::Critical),
            "high" => Ok(Self::High),
            "normal" | "" => Ok(Self::Normal),
            "low" => Ok(Self::Low),
            "background" => Ok(Self::Background),
            _ => Err(PriorityParseError {
                input: input.to_string(),
            }),
        }
    }
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
    /// How urgently this wants a node, once more work is waiting than there
    /// are nodes to take it.
    #[serde(default)]
    pub priority: Priority,
    /// Conditions a node must satisfy to be allowed to run this.
    ///
    /// Load and locality decide where this is cheapest; these decide where it
    /// is permitted at all.
    #[serde(default)]
    pub constraints: Vec<crate::labels::Constraint>,
    /// WebAssembly module to run, for `kind::WASM` tasks.
    ///
    /// The module is published like any other dataset, so it is content-
    /// addressed, deduplicated, and transferred to each node only once.
    pub module: Option<DataId>,
}

impl Task {
    pub fn new(kind: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            id: TaskId::generate(),
            kind: kind.into(),
            payload,
            inputs: Vec::new(),
            priority: Priority::default(),
            constraints: Vec::new(),
            module: None,
        }
    }

    /// A task that runs `module` over `payload`.
    ///
    /// The module is listed as an input too, so it travels the same
    /// deduplicated path as data and the scheduler counts it toward locality.
    pub fn wasm(module: DataId, payload: Vec<u8>) -> Self {
        Self {
            id: TaskId::generate(),
            kind: kind::WASM.to_string(),
            payload,
            inputs: vec![module],
            priority: Priority::default(),
            constraints: Vec::new(),
            module: Some(module),
        }
    }

    /// Declares the datasets this task needs.
    ///
    /// The module of a WASM task stays in the list: it has to reach the node
    /// like everything else the task reads.
    pub fn with_inputs(mut self, inputs: Vec<DataId>) -> Self {
        self.inputs = match self.module {
            Some(module) if !inputs.contains(&module) => {
                std::iter::once(module).chain(inputs).collect()
            }
            _ => inputs,
        };
        self
    }

    /// Restricts this task to nodes whose labels satisfy every condition.
    ///
    /// If nothing qualifies the task is not placed at all: a task that needs a
    /// GPU waiting is better than the same task running without one.
    pub fn with_constraints(mut self, constraints: Vec<crate::labels::Constraint>) -> Self {
        self.constraints = constraints;
        self
    }

    /// Adds one condition on top of the existing ones.
    pub fn requiring(mut self, constraint: crate::labels::Constraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Sets how urgently this task wants a node.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
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
    /// Content address of the output, which the node kept a copy of.
    ///
    /// This is what lets a workflow keep its intermediate results still. A
    /// later task that reads this id finds it already on the node that
    /// produced it, so the locality bonus applies and nothing has to move.
    /// `None` on failure, and on a node too old to report one.
    #[serde(default)]
    pub output_id: Option<DataId>,
}

impl TaskResult {
    pub fn success(task_id: TaskId, node_id: NodeId, output: Vec<u8>, duration: Duration) -> Self {
        Self {
            task_id,
            node_id,
            outcome: TaskOutcome::Success { output },
            duration,
            output_id: None,
        }
    }

    /// Same, naming the output the node kept so later tasks can read it in
    /// place.
    pub fn produced(
        task_id: TaskId,
        node_id: NodeId,
        output: Vec<u8>,
        duration: Duration,
        output_id: DataId,
    ) -> Self {
        Self {
            output_id: Some(output_id),
            ..Self::success(task_id, node_id, output, duration)
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
            // Nothing was produced, so there is nothing to point at.
            output_id: None,
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
    fn a_wasm_task_carries_its_module_as_an_input() {
        let module = crate::DataId::of(b"module bytes");
        let task = Task::wasm(module, b"input".to_vec());

        assert_eq!(task.kind, kind::WASM);
        assert_eq!(task.module, Some(module));
        assert_eq!(task.inputs, vec![module]);
    }

    #[test]
    fn declaring_extra_inputs_keeps_the_module() {
        let module = crate::DataId::of(b"module bytes");
        let dataset = crate::DataId::of(b"dataset");

        let task = Task::wasm(module, Vec::new()).with_inputs(vec![dataset]);

        assert_eq!(task.inputs, vec![module, dataset]);
    }

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

    #[test]
    fn priorities_order_from_background_up_to_critical() {
        assert!(Priority::Critical > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
        assert!(Priority::Low > Priority::Background);

        let mut shuffled = vec![
            Priority::Normal,
            Priority::Critical,
            Priority::Background,
            Priority::High,
            Priority::Low,
        ];
        shuffled.sort();
        assert_eq!(shuffled, Priority::ALL);
    }

    #[test]
    fn a_task_that_says_nothing_is_normal() {
        assert_eq!(Task::new("hash", Vec::new()).priority, Priority::Normal);
        assert_eq!(Priority::default(), Priority::Normal);
    }

    #[test]
    fn promotion_climbs_one_level_and_stops_at_the_top() {
        assert_eq!(Priority::Background.promoted(), Priority::Low);
        assert_eq!(Priority::Normal.promoted(), Priority::High);
        assert_eq!(Priority::High.promoted(), Priority::Critical);
        assert_eq!(Priority::Critical.promoted(), Priority::Critical);
    }

    #[test]
    fn priorities_round_trip_through_their_names() {
        for priority in Priority::ALL {
            assert_eq!(priority.to_string().parse(), Ok(priority));
        }
        assert_eq!("CRITICAL".parse(), Ok(Priority::Critical));
        assert_eq!("".parse(), Ok(Priority::Normal), "unsaid means normal");
        assert!("urgent".parse::<Priority>().is_err());
    }

    #[test]
    fn an_older_task_without_a_priority_still_deserializes() {
        // Agents and clients are upgraded separately; a task encoded before
        // priorities existed has to keep working.
        let json = r#"{"id":"c8e6f6e0-0000-4000-8000-000000000000","kind":"hash",
            "payload":[1,2],"inputs":[],"constraints":[],"module":null}"#;
        let task: Task = serde_json::from_str(json).expect("an older task");
        assert_eq!(task.priority, Priority::Normal);
    }
}
