//! Running a workflow: each step after the ones it waits for.
//!
//! The interesting part is not the ordering — [`aether_core::Workflow`] does
//! that — but what a step *reads*. Every dependency's output is added to a
//! step's inputs before it is dispatched, which means the ordinary locality
//! score sees it: the node that produced the data already holds it, so that
//! node wins, so the intermediate result never moves.
//!
//! No new transfer path, no separate placement rule. The mechanism that keeps
//! a published dataset still is the one that keeps a computed one still.

use std::collections::HashMap;

use aether_core::{TaskResult, Workflow, WorkflowError};
use aether_scheduler::Scheduler;
use tracing::debug;

use crate::dispatch::{Controller, DispatchError, TaskTransport};

/// A workflow could not be run.
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    #[error(transparent)]
    Invalid(#[from] WorkflowError),
    #[error("step {step} could not be placed: {source}")]
    Dispatch {
        step: usize,
        #[source]
        source: DispatchError,
    },
}

/// What a workflow produced.
#[derive(Debug, Clone, PartialEq)]
pub struct FlowResult {
    /// One result per step, in the order the steps were written — not the
    /// order they ran in, which nobody asked about.
    pub results: Vec<TaskResult>,
    /// Steps that were never attempted because something earlier failed.
    pub skipped: Vec<usize>,
}

impl FlowResult {
    /// Whether every step ran and succeeded.
    pub fn is_success(&self) -> bool {
        self.skipped.is_empty() && self.results.iter().all(TaskResult::is_success)
    }

    /// The first step that ran and failed, if any.
    pub fn first_failure(&self) -> Option<(usize, &TaskResult)> {
        self.results
            .iter()
            .enumerate()
            .find(|(_, result)| !result.is_success())
    }
}

/// Runs every step, each after the ones it depends on.
///
/// A step that fails stops the steps waiting on it: running D on B's output
/// when B failed produces a confident answer computed from nothing. Branches
/// that do not depend on the failure still run — a diamond with one bad arm
/// should still tell you about the good one.
pub async fn run_workflow<S, T>(
    controller: &mut Controller<S, T>,
    workflow: &Workflow,
) -> Result<FlowResult, FlowError>
where
    S: Scheduler,
    T: TaskTransport + Send,
{
    let order = workflow.order()?;
    let mut outputs: HashMap<usize, aether_core::DataId> = HashMap::new();
    let mut results: HashMap<usize, TaskResult> = HashMap::new();
    let mut abandoned: Vec<usize> = Vec::new();

    for step in order {
        let dependencies = &workflow.steps[step].depends_on;

        // Anything downstream of a failure is not attempted. Its inputs do
        // not exist, and a task that runs on missing data is worse than one
        // that did not run.
        if dependencies
            .iter()
            .any(|dependency| abandoned.contains(dependency) || failed(&results, *dependency))
        {
            debug!(step, "skipping: something it depends on did not finish");
            abandoned.push(step);
            continue;
        }

        let mut task = workflow.steps[step].task.clone();
        // The reason a workflow keeps its data still: the scheduler sees these
        // as inputs, the catalog says which node holds them, and the locality
        // bonus does the rest.
        for dependency in dependencies {
            if let Some(output_id) = outputs.get(dependency)
                && !task.inputs.contains(output_id)
            {
                task.inputs.push(*output_id);
            }
        }

        let result = controller
            .submit(task)
            .await
            .map_err(|source| FlowError::Dispatch { step, source })?;

        if let Some(output_id) = result.output_id {
            outputs.insert(step, output_id);
        }
        results.insert(step, result);
    }

    abandoned.sort_unstable();
    Ok(FlowResult {
        results: (0..workflow.len())
            .filter_map(|step| results.remove(&step))
            .collect(),
        skipped: abandoned,
    })
}

fn failed(results: &HashMap<usize, TaskResult>, step: usize) -> bool {
    results
        .get(&step)
        .is_some_and(|result| !result.is_success())
}

#[cfg(test)]
mod tests {
    use aether_core::task::kind;
    use aether_core::{NodeId, NodeInfo, Step, Task};
    use aether_scheduler::{DataCatalog, LeastLoadedScheduler, LocalityScheduler};

    use super::*;
    use crate::sim::SimulatedMesh;

    /// Nodes that run the agent's real built-in tasks, so `hash` works and a
    /// result carries the `output_id` a workflow depends on.
    fn controller_with(nodes: usize) -> Controller<LeastLoadedScheduler, SimulatedMesh> {
        let mut controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::with_executor(aether_agent::execute),
            DataCatalog::new(),
        );
        for index in 0..nodes {
            controller.registry_mut().register(NodeInfo::new(
                NodeId::generate(),
                format!("node-{index}"),
                "127.0.0.1:1",
                4,
            ));
        }
        controller
    }

    fn task(kind: &str) -> Task {
        Task::new(kind, Vec::new())
    }

    #[tokio::test]
    async fn every_step_of_a_chain_runs_in_order() {
        let mut controller = controller_with(1);
        let workflow = Workflow::chain(vec![
            Task::new(kind::ECHO, b"one".to_vec()),
            Task::new(kind::HASH, Vec::new()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let outcome = run_workflow(&mut controller, &workflow).await.unwrap();

        assert!(outcome.is_success(), "{outcome:?}");
        assert_eq!(outcome.results.len(), 3);
        assert!(outcome.skipped.is_empty());
    }

    #[tokio::test]
    async fn a_step_reads_what_the_step_before_it_produced() {
        let mut controller = controller_with(1);
        let workflow = Workflow::chain(vec![
            Task::new(kind::ECHO, b"payload".to_vec()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let outcome = run_workflow(&mut controller, &workflow).await.unwrap();
        let second = &outcome.results[1];

        // The second step hashed nothing but its predecessor's output, so the
        // digest is the digest of that output and of nothing else.
        let produced = outcome.results[0].output().expect("the first output");
        assert_eq!(
            second.output(),
            Some(blake3::hash(produced).as_bytes().as_slice()),
            "the dependency's output was not what got hashed"
        );
    }

    #[tokio::test]
    async fn an_intermediate_result_does_not_move() {
        // Locality scheduling, several nodes: the second step should land on
        // whichever node produced the data rather than pulling it elsewhere.
        let catalog = DataCatalog::new();
        let mut controller = Controller::new(
            LocalityScheduler::new(catalog.clone()),
            SimulatedMesh::with_executor(aether_agent::execute),
            catalog,
        );
        for index in 0..3 {
            controller.registry_mut().register(NodeInfo::new(
                NodeId::generate(),
                format!("node-{index}"),
                "127.0.0.1:1",
                4,
            ));
        }

        let workflow = Workflow::chain(vec![
            Task::new(kind::ECHO, vec![7u8; 64 * 1024]),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let before = controller.data_bytes_uncompressed();
        let outcome = run_workflow(&mut controller, &workflow).await.unwrap();

        assert!(outcome.is_success(), "{outcome:?}");
        assert_eq!(
            outcome.results[0].node_id, outcome.results[1].node_id,
            "the second step ran somewhere else"
        );
        assert_eq!(
            controller.data_bytes_uncompressed(),
            before,
            "the intermediate result crossed the wire"
        );
    }

    #[tokio::test]
    async fn both_arms_of_a_diamond_run_before_the_join() {
        let mut controller = controller_with(2);
        let workflow = Workflow::new(vec![
            Step::new(Task::new(kind::ECHO, b"seed".to_vec())),
            Step::after(task(kind::HASH), vec![0]),
            Step::after(task(kind::HASH), vec![0]),
            Step::after(task(kind::HASH), vec![1, 2]),
        ])
        .unwrap();

        let outcome = run_workflow(&mut controller, &workflow).await.unwrap();

        assert!(outcome.is_success(), "{outcome:?}");
        assert_eq!(outcome.results.len(), 4);
    }

    #[tokio::test]
    async fn a_join_reads_both_arms() {
        let mut controller = controller_with(1);
        let workflow = Workflow::new(vec![
            Step::new(Task::new(kind::ECHO, b"a".to_vec())),
            Step::new(Task::new(kind::ECHO, b"b".to_vec())),
            Step::after(task(kind::HASH), vec![0, 1]),
        ])
        .unwrap();

        let outcome = run_workflow(&mut controller, &workflow).await.unwrap();

        // `hash` digests the payload then every input in order, so the join's
        // answer is only reachable if it received both arms.
        let mut expected = blake3::Hasher::new();
        expected.update(b"a");
        expected.update(b"b");
        assert_eq!(
            outcome.results[2].output(),
            Some(expected.finalize().as_bytes().as_slice())
        );
    }

    #[tokio::test]
    async fn a_failed_step_stops_what_depends_on_it() {
        let mut controller = controller_with(1);
        let workflow = Workflow::chain(vec![
            Task::new("nonsense", Vec::new()),
            Task::new(kind::HASH, Vec::new()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let outcome = run_workflow(&mut controller, &workflow).await.unwrap();

        // Running step two on step one's output when step one failed would
        // produce a confident answer computed from nothing.
        assert!(!outcome.is_success());
        assert_eq!(outcome.results.len(), 1);
        assert_eq!(outcome.skipped, [1, 2]);
        assert_eq!(outcome.first_failure().map(|(step, _)| step), Some(0));
    }

    #[tokio::test]
    async fn a_failure_in_one_arm_does_not_stop_the_other() {
        let mut controller = controller_with(1);
        let workflow = Workflow::new(vec![
            Step::new(Task::new("nonsense", Vec::new())),
            Step::after(task(kind::HASH), vec![0]),
            Step::new(Task::new(kind::ECHO, b"unaffected".to_vec())),
        ])
        .unwrap();

        let outcome = run_workflow(&mut controller, &workflow).await.unwrap();

        // A diamond with one bad arm should still tell you about the good one.
        assert_eq!(outcome.skipped, [1]);
        assert_eq!(outcome.results.len(), 2);
        assert!(outcome.results.iter().any(|result| result.is_success()));
    }

    #[tokio::test]
    async fn a_workflow_with_nowhere_to_run_reports_which_step() {
        let mut controller = controller_with(0);
        let workflow = Workflow::parallel(vec![task(kind::ECHO)]).unwrap();

        match run_workflow(&mut controller, &workflow).await {
            Err(FlowError::Dispatch { step, .. }) => assert_eq!(step, 0),
            other => panic!("expected a dispatch failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn results_come_back_in_the_order_the_steps_were_written() {
        let mut controller = controller_with(1);
        // Written so that the run order is not the written order.
        let workflow = Workflow::new(vec![
            Step::after(Task::new(kind::ECHO, b"last".to_vec()), vec![1]),
            Step::new(Task::new(kind::ECHO, b"first".to_vec())),
        ])
        .unwrap();

        let outcome = run_workflow(&mut controller, &workflow).await.unwrap();

        assert_eq!(outcome.results[0].output(), Some(&b"last"[..]));
        assert_eq!(outcome.results[1].output(), Some(&b"first"[..]));
    }
}
