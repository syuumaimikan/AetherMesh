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
use std::sync::Arc;

use aether_core::{NodeId, TaskResult, Workflow, WorkflowError};
use aether_scheduler::Scheduler;
use tokio::task::JoinSet;
use tracing::{debug, warn};

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
    #[error(transparent)]
    Checkpoint(#[from] crate::checkpoint::CheckpointError),
    /// A step's task panicked or was cancelled.
    ///
    /// A bug rather than an outcome, but the workflow reporting it beats the
    /// dispatcher panicking in sympathy and taking every other client's work
    /// with it.
    #[error("step {step} did not finish: {reason}")]
    Lost { step: usize, reason: String },
}

/// Where one step ran and what it cost to put it there.
///
/// The three things worth knowing about a workflow's data movement, per the
/// question this answers: how big the intermediate result was, whether its
/// inputs were already in place, and how many bytes had to move because they
/// were not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    pub step: usize,
    pub node_id: NodeId,
    /// Size of what this step produced, and therefore what the next step
    /// would have to move if it ran elsewhere.
    pub output_bytes: u64,
    /// Dependencies whose output was already on the chosen node.
    pub inputs_local: usize,
    /// Dependencies whose output had to be sent there.
    pub inputs_moved: usize,
}

/// What a workflow produced.
#[derive(Debug, Clone, PartialEq)]
pub struct FlowResult {
    /// One result per step that ran, in the order the steps were written —
    /// not the order they ran in, which nobody asked about.
    ///
    /// Steps that were skipped or resumed are absent, so this is not indexed
    /// by step number. [`FlowResult::steps_run`] says which step each entry
    /// belongs to.
    pub results: Vec<TaskResult>,
    /// The step index of each entry in [`FlowResult::results`].
    pub steps_run: Vec<usize>,
    /// Steps that were never attempted because something earlier failed.
    pub skipped: Vec<usize>,
    /// Steps that were not run because a previous run of this workflow already
    /// finished them and their output is still on a node. Empty unless the run
    /// was resumed.
    pub resumed: Vec<usize>,
    /// Where each step ran and whether its inputs were already there.
    pub placements: Vec<Placement>,
    /// Bytes that crossed the wire to run this workflow.
    ///
    /// Measured across the whole run rather than per step. Steps that do not
    /// depend on each other now run at the same time, and a counter sampled
    /// around one of them would be picking up another one's transfers — a
    /// number that looks precise and is not.
    pub bytes_moved: u64,
}

impl FlowResult {
    /// How many distinct nodes the workflow used.
    ///
    /// Reported beside [`FlowResult::bytes_moved`] because the two pull
    /// against each other: keeping everything on one node moves nothing and
    /// uses one machine, and there is no setting that is right for everybody.
    pub fn nodes_used(&self) -> usize {
        let mut nodes: Vec<NodeId> = self
            .placements
            .iter()
            .map(|placement| placement.node_id)
            .collect();
        nodes.sort_unstable();
        nodes.dedup();
        nodes.len()
    }

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
/// Steps that do not depend on each other go out together, so a workflow's
/// wall clock is the depth of the graph rather than the sum of its steps.
///
/// A step that fails stops the steps waiting on it: running D on B's output
/// when B failed produces a confident answer computed from nothing. Branches
/// that do not depend on the failure still run — a diamond with one bad arm
/// should still tell you about the good one.
pub async fn run_workflow<S, T>(
    controller: Arc<Controller<S, T>>,
    workflow: &Workflow,
) -> Result<FlowResult, FlowError>
where
    S: Scheduler + Send + Sync + 'static,
    T: TaskTransport + Send + Sync + 'static,
{
    run_inner(controller, workflow, None).await
}

/// Same, but a step this run finishes is recorded, and a step an earlier run
/// under the same name already finished is not run again.
///
/// Needs [`Controller::with_checkpoint`]; without a journal this is exactly
/// [`run_workflow`], which is the useful behaviour for an operator who has not
/// configured one — the workflow runs, it just runs from the start.
///
/// Two things have to be true before a step is skipped, and both are checked
/// rather than assumed. The workflow has to be the one the run was recorded
/// against, or a step is skipped on the strength of some other graph's
/// progress. And the output has to still be somewhere in the mesh: the journal
/// records what happened, not a promise that the node holding it survived.
pub async fn run_workflow_resumable<S, T>(
    controller: Arc<Controller<S, T>>,
    workflow: &Workflow,
    run: &str,
) -> Result<FlowResult, FlowError>
where
    S: Scheduler + Send + Sync + 'static,
    T: TaskTransport + Send + Sync + 'static,
{
    run_inner(controller, workflow, Some(run)).await
}

async fn run_inner<S, T>(
    controller: Arc<Controller<S, T>>,
    workflow: &Workflow,
    run: Option<&str>,
) -> Result<FlowResult, FlowError>
where
    S: Scheduler + Send + Sync + 'static,
    T: TaskTransport + Send + Sync + 'static,
{
    // Validate before dispatching anything: a cycle discovered halfway
    // through leaves work half-done on machines somebody else owns.
    let order = workflow.order()?;

    let mut waiting: Vec<usize> = workflow
        .steps
        .iter()
        .map(|step| step.depends_on.len())
        .collect();
    let mut outputs: HashMap<usize, aether_core::DataId> = HashMap::new();
    let mut results: HashMap<usize, TaskResult> = HashMap::new();
    let mut placements: Vec<Placement> = Vec::new();
    let mut abandoned: Vec<usize> = Vec::new();
    let mut resumed: Vec<usize> = Vec::new();

    // Anything an earlier run finished, whose output is still out there.
    let journal = run.and(controller.checkpoint().cloned());
    let fingerprint = journal
        .as_ref()
        .map(|_| crate::checkpoint::fingerprint(workflow));
    if let (Some(journal), Some(run), Some(fingerprint)) = (&journal, run, &fingerprint) {
        let completed = journal.completed(run, fingerprint)?;
        for step in order {
            let Some(record) = completed.get(&step) else {
                continue;
            };
            if controller.catalog().locations(record.output_id).is_empty() {
                debug!(
                    step,
                    output = %record.output_id,
                    "running again: what it produced is no longer anywhere in the mesh"
                );
                continue;
            }
            debug!(step, node = %record.node_id, "resuming: already finished");
            outputs.insert(step, record.output_id);
            resumed.push(step);
            for dependent in workflow.dependents_of(step) {
                waiting[dependent] -= 1;
            }
        }
    }

    let mut ready: Vec<usize> = (0..workflow.len())
        .filter(|step| waiting[*step] == 0 && !resumed.contains(step))
        .collect();

    let before = controller.data_bytes_uncompressed();
    let mut running: JoinSet<(usize, Result<TaskResult, DispatchError>)> = JoinSet::new();

    loop {
        // Everything whose dependencies are done goes out together. This is
        // the whole difference from running the topological order one at a
        // time: independent branches of a workflow are independent, and there
        // is no reason for the second to wait for the first.
        for step in std::mem::take(&mut ready) {
            let task = task_for(workflow, step, &outputs);
            let controller = controller.clone();
            running.spawn(async move { (step, controller.submit(task).await) });
        }

        let Some(finished) = running.join_next().await else {
            break;
        };
        let (step, outcome) = match finished {
            Ok(finished) => finished,
            Err(error) => {
                return Err(FlowError::Lost {
                    // The step is inside the task that failed, so it cannot be
                    // named here. Everything else about it can be.
                    step: usize::MAX,
                    reason: error.to_string(),
                });
            }
        };

        let result = outcome.map_err(|source| FlowError::Dispatch { step, source })?;
        placements.push(placement_for(
            &controller,
            workflow,
            step,
            &outputs,
            &result,
        ));

        if let Some(output_id) = result.output_id {
            outputs.insert(step, output_id);
        }
        let succeeded = result.is_success();
        if let (Some(journal), Some(run), Some(fingerprint), Some(output_id)) =
            (&journal, run, &fingerprint, result.output_id)
            && succeeded
        {
            let record = crate::checkpoint::Record {
                run: run.to_string(),
                fingerprint: fingerprint.clone(),
                step,
                task_id: result.task_id,
                node_id: result.node_id,
                output_id,
                duration_ms: result.duration.as_millis() as u64,
            };
            // A journal that cannot be written costs the next run its head
            // start. It does not make this run's answer wrong, so it is not
            // worth failing a workflow that is otherwise going fine.
            if let Err(error) = journal.append(&record) {
                warn!(step, %error, "could not record a finished step");
            }
        }
        results.insert(step, result);

        // Release whatever was waiting on this step, or give up on it.
        for dependent in workflow.dependents_of(step) {
            waiting[dependent] -= 1;
            if waiting[dependent] > 0 {
                continue;
            }
            if succeeded && !abandoned.contains(&step) {
                ready.push(dependent);
            } else {
                // Running a step on a predecessor's output when the
                // predecessor failed produces a confident answer computed
                // from nothing.
                debug!(
                    step = dependent,
                    "skipping: something it depends on did not finish"
                );
                abandoned.push(dependent);
                for further in workflow.dependents_of(dependent) {
                    waiting[further] -= 1;
                    if waiting[further] == 0 {
                        abandoned.push(further);
                    }
                }
            }
        }
        ready.sort_unstable();
    }

    abandoned.sort_unstable();
    abandoned.dedup();
    resumed.sort_unstable();
    placements.sort_unstable_by_key(|placement| placement.step);

    let mut steps_run = Vec::with_capacity(results.len());
    let mut ordered = Vec::with_capacity(results.len());
    for step in 0..workflow.len() {
        if let Some(result) = results.remove(&step) {
            steps_run.push(step);
            ordered.push(result);
        }
    }

    Ok(FlowResult {
        results: ordered,
        steps_run,
        skipped: abandoned,
        resumed,
        placements,
        bytes_moved: controller.data_bytes_uncompressed() - before,
    })
}

/// The task for one step, reading whatever the steps before it produced.
///
/// Adding a dependency's output as an input is the reason a workflow keeps
/// its data still: the scheduler sees an input, the catalog says which node
/// holds it, and the locality bonus does the rest.
fn task_for(
    workflow: &Workflow,
    step: usize,
    outputs: &HashMap<usize, aether_core::DataId>,
) -> aether_core::Task {
    let mut task = workflow.steps[step].task.clone();
    for dependency in &workflow.steps[step].depends_on {
        if let Some(output_id) = outputs.get(dependency)
            && !task.inputs.contains(output_id)
        {
            task.inputs.push(*output_id);
        }
    }
    task
}

/// Where a step ran, and whether its inputs were already there.
fn placement_for<S, T>(
    controller: &Controller<S, T>,
    workflow: &Workflow,
    step: usize,
    outputs: &HashMap<usize, aether_core::DataId>,
    result: &TaskResult,
) -> Placement
where
    S: Scheduler,
    T: TaskTransport + Send + Sync,
{
    let dependencies: Vec<aether_core::DataId> = workflow.steps[step]
        .depends_on
        .iter()
        .filter_map(|dependency| outputs.get(dependency).copied())
        .collect();
    let local = dependencies
        .iter()
        .filter(|input| controller.catalog().holds(**input, result.node_id))
        .count();

    Placement {
        step,
        node_id: result.node_id,
        output_bytes: result.output().map_or(0, |output| output.len() as u64),
        inputs_local: local,
        inputs_moved: dependencies.len() - local,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aether_core::task::kind;
    use aether_core::{NodeId, NodeInfo, Step, Task};
    use aether_scheduler::{DataCatalog, LeastLoadedScheduler, LocalityScheduler};

    use super::*;
    use crate::sim::SimulatedMesh;

    /// Nodes that run the agent's real built-in tasks, so `hash` works and a
    /// result carries the `output_id` a workflow depends on.
    fn controller_with(nodes: usize) -> Arc<Controller<LeastLoadedScheduler, SimulatedMesh>> {
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::with_executor(aether_agent::execute),
            DataCatalog::new(),
        );
        for index in 0..nodes {
            controller.register(NodeInfo::new(
                NodeId::generate(),
                format!("node-{index}"),
                "127.0.0.1:1",
                4,
            ));
        }
        Arc::new(controller)
    }

    fn task(kind: &str) -> Task {
        Task::new(kind, Vec::new())
    }

    /// Same, but every finished step is recorded to a fresh journal file.
    fn controller_journalling(
        nodes: usize,
        name: &str,
    ) -> (
        Arc<Controller<LeastLoadedScheduler, SimulatedMesh>>,
        Arc<crate::checkpoint::Journal>,
    ) {
        let path = std::env::temp_dir()
            .join(format!("aethermesh-flow-{}", std::process::id()))
            .join(format!("{name}.jsonl"));
        let _ = std::fs::remove_file(&path);
        let journal = Arc::new(crate::checkpoint::Journal::open(&path).unwrap());

        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            SimulatedMesh::with_executor(aether_agent::execute),
            DataCatalog::new(),
        )
        .with_checkpoint(journal.clone());
        for index in 0..nodes {
            controller.register(NodeInfo::new(
                NodeId::generate(),
                format!("node-{index}"),
                "127.0.0.1:1",
                4,
            ));
        }
        (Arc::new(controller), journal)
    }

    fn chain(payload: &[u8]) -> Workflow {
        Workflow::chain(vec![
            Task::new(kind::ECHO, payload.to_vec()),
            Task::new(kind::HASH, Vec::new()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn a_resumed_run_does_not_repeat_what_it_already_did() {
        let (controller, journal) = controller_journalling(1, "resume");

        let first = run_workflow_resumable(controller.clone(), &chain(b"seed"), "nightly")
            .await
            .unwrap();
        assert_eq!(first.results.len(), 3);
        assert!(first.resumed.is_empty());
        assert_eq!(journal.records().unwrap().len(), 3);

        // The same workflow, submitted again under the same name. The outputs
        // are still on the node, so there is nothing left to do.
        let second = run_workflow_resumable(controller.clone(), &chain(b"seed"), "nightly")
            .await
            .unwrap();

        assert_eq!(second.resumed, vec![0, 1, 2]);
        assert!(second.results.is_empty());
        assert!(second.is_success());
        // And nothing new was recorded: it ran nothing.
        assert_eq!(journal.records().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn only_the_steps_that_finished_are_resumed() {
        let (controller, _journal) = controller_journalling(1, "partial");

        // A chain whose second step fails: the first is recorded, the third
        // never runs.
        let broken = Workflow::chain(vec![
            Task::new(kind::ECHO, b"seed".to_vec()),
            Task::new("no-such-kind", Vec::new()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();
        let first = run_workflow_resumable(controller.clone(), &broken, "nightly")
            .await
            .unwrap();
        assert!(!first.is_success());
        assert_eq!(first.skipped, vec![2]);

        let second = run_workflow_resumable(controller.clone(), &broken, "nightly")
            .await
            .unwrap();

        // Step 0 succeeded and is skipped. Step 1 failed, so it is tried
        // again rather than treated as done.
        assert_eq!(second.resumed, vec![0]);
        assert_eq!(second.steps_run, vec![1]);
        assert_eq!(second.skipped, vec![2]);
    }

    #[tokio::test]
    async fn a_step_whose_output_is_gone_runs_again() {
        let (controller, _journal) = controller_journalling(1, "lost");

        let workflow = chain(b"seed");
        let first = run_workflow_resumable(controller.clone(), &workflow, "nightly")
            .await
            .unwrap();
        assert_eq!(first.results.len(), 3);

        // The node holding everything leaves the mesh.
        let node = first.results[0].node_id;
        controller.catalog().forget_node(node);

        let second = run_workflow_resumable(controller.clone(), &workflow, "nightly")
            .await
            .unwrap();

        // The journal still says these steps finished. It is a record of what
        // happened, not a promise that the data survived.
        assert!(second.resumed.is_empty());
        assert_eq!(second.results.len(), 3);
    }

    #[tokio::test]
    async fn a_run_name_cannot_be_reused_for_a_different_workflow() {
        let (controller, _journal) = controller_journalling(1, "fingerprint");

        run_workflow_resumable(controller.clone(), &chain(b"seed"), "nightly")
            .await
            .unwrap();

        // Same name, different work. Resuming this would skip step 0 because
        // some other graph's step 0 finished.
        let error = run_workflow_resumable(controller.clone(), &chain(b"different"), "nightly")
            .await
            .unwrap_err();

        assert!(matches!(error, FlowError::Checkpoint(_)), "{error:?}");
    }

    #[tokio::test]
    async fn an_unnamed_run_is_not_recorded() {
        let (controller, journal) = controller_journalling(1, "unnamed");

        let outcome = run_workflow(controller.clone(), &chain(b"seed"))
            .await
            .unwrap();

        assert!(outcome.is_success());
        assert!(outcome.resumed.is_empty());
        assert!(journal.records().unwrap().is_empty());
    }

    #[tokio::test]
    async fn results_say_which_step_they_belong_to() {
        let controller = controller_with(1);
        // Step 1 fails, so step 2 never runs and step 3 is unaffected.
        let workflow = Workflow::new(vec![
            Step::new(Task::new(kind::ECHO, b"a".to_vec())),
            Step::new(Task::new("no-such-kind", Vec::new())),
            Step::after(Task::new(kind::HASH, Vec::new()), vec![1]),
            Step::new(Task::new(kind::ECHO, b"d".to_vec())),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        // Three results for four steps. Reporting the third of them as "step 2"
        // would blame the failure on the wrong step.
        assert_eq!(outcome.steps_run, vec![0, 1, 3]);
        assert_eq!(outcome.skipped, vec![2]);
        assert!(!outcome.results[1].is_success());
    }

    #[tokio::test]
    async fn every_step_of_a_chain_runs_in_order() {
        let controller = controller_with(1);
        let workflow = Workflow::chain(vec![
            Task::new(kind::ECHO, b"one".to_vec()),
            Task::new(kind::HASH, Vec::new()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        assert!(outcome.is_success(), "{outcome:?}");
        assert_eq!(outcome.results.len(), 3);
        assert!(outcome.skipped.is_empty());
    }

    #[tokio::test]
    async fn a_step_reads_what_the_step_before_it_produced() {
        let controller = controller_with(1);
        let workflow = Workflow::chain(vec![
            Task::new(kind::ECHO, b"payload".to_vec()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();
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
        let controller = Controller::new(
            LocalityScheduler::new(catalog.clone()),
            SimulatedMesh::with_executor(aether_agent::execute),
            catalog,
        );
        for index in 0..3 {
            controller.register(NodeInfo::new(
                NodeId::generate(),
                format!("node-{index}"),
                "127.0.0.1:1",
                4,
            ));
        }
        let controller = Arc::new(controller);

        let workflow = Workflow::chain(vec![
            Task::new(kind::ECHO, vec![7u8; 64 * 1024]),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let before = controller.data_bytes_uncompressed();
        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

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
        let controller = controller_with(2);
        let workflow = Workflow::new(vec![
            Step::new(Task::new(kind::ECHO, b"seed".to_vec())),
            Step::after(task(kind::HASH), vec![0]),
            Step::after(task(kind::HASH), vec![0]),
            Step::after(task(kind::HASH), vec![1, 2]),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        assert!(outcome.is_success(), "{outcome:?}");
        assert_eq!(outcome.results.len(), 4);
    }

    #[tokio::test]
    async fn a_join_reads_both_arms() {
        let controller = controller_with(1);
        let workflow = Workflow::new(vec![
            Step::new(Task::new(kind::ECHO, b"a".to_vec())),
            Step::new(Task::new(kind::ECHO, b"b".to_vec())),
            Step::after(task(kind::HASH), vec![0, 1]),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

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
        let controller = controller_with(1);
        let workflow = Workflow::chain(vec![
            Task::new("nonsense", Vec::new()),
            Task::new(kind::HASH, Vec::new()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        // Running step two on step one's output when step one failed would
        // produce a confident answer computed from nothing.
        assert!(!outcome.is_success());
        assert_eq!(outcome.results.len(), 1);
        assert_eq!(outcome.skipped, [1, 2]);
        assert_eq!(outcome.first_failure().map(|(step, _)| step), Some(0));
    }

    #[tokio::test]
    async fn a_failure_in_one_arm_does_not_stop_the_other() {
        let controller = controller_with(1);
        let workflow = Workflow::new(vec![
            Step::new(Task::new("nonsense", Vec::new())),
            Step::after(task(kind::HASH), vec![0]),
            Step::new(Task::new(kind::ECHO, b"unaffected".to_vec())),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        // A diamond with one bad arm should still tell you about the good one.
        assert_eq!(outcome.skipped, [1]);
        assert_eq!(outcome.results.len(), 2);
        assert!(outcome.results.iter().any(|result| result.is_success()));
    }

    #[tokio::test]
    async fn a_chain_reports_that_nothing_moved() {
        let catalog = DataCatalog::new();
        let controller = Controller::new(
            LocalityScheduler::new(catalog.clone()),
            SimulatedMesh::with_executor(aether_agent::execute),
            catalog,
        );
        for index in 0..3 {
            controller.register(NodeInfo::new(
                NodeId::generate(),
                format!("node-{index}"),
                "127.0.0.1:1",
                4,
            ));
        }
        let controller = Arc::new(controller);

        let workflow = Workflow::chain(vec![
            Task::new(kind::ECHO, vec![9u8; 32 * 1024]),
            Task::new(kind::HASH, Vec::new()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        assert!(outcome.is_success(), "{outcome:?}");
        assert_eq!(outcome.bytes_moved, 0, "an intermediate result travelled");
        // Zero movement and one node are the same fact seen twice, which is
        // exactly the trade-off worth reporting rather than hiding.
        assert_eq!(outcome.nodes_used(), 1);

        assert_eq!(outcome.placements.len(), 3);
        assert_eq!(outcome.placements[0].inputs_moved, 0);
        assert_eq!(outcome.placements[1].inputs_local, 1);
        assert_eq!(outcome.placements[2].inputs_local, 1);
        assert_eq!(outcome.placements[0].output_bytes, 32 * 1024);
    }

    #[tokio::test]
    async fn a_placement_names_the_step_it_describes() {
        let controller = controller_with(1);
        // Written out of order, so a placement indexed by position would be
        // wrong and a placement carrying its step number would not.
        let workflow = Workflow::new(vec![
            Step::after(Task::new(kind::ECHO, b"second".to_vec()), vec![1]),
            Step::new(Task::new(kind::ECHO, b"first".to_vec())),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        assert_eq!(outcome.placements[0].step, 0);
        assert_eq!(outcome.placements[1].step, 1);
        assert_eq!(outcome.placements[0].output_bytes, b"second".len() as u64);
    }

    #[tokio::test]
    async fn a_workflow_that_stopped_early_reports_only_what_ran() {
        let controller = controller_with(1);
        let workflow = Workflow::chain(vec![
            Task::new("nonsense", Vec::new()),
            Task::new(kind::HASH, Vec::new()),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        assert_eq!(outcome.placements.len(), 1);
        assert_eq!(outcome.bytes_moved, 0);
    }

    /// A transport where each dispatch takes measurable time, and which
    /// records the most that were ever in flight at once.
    struct Slow {
        inner: SimulatedMesh,
        takes: Duration,
        running: Arc<std::sync::atomic::AtomicUsize>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl TaskTransport for Slow {
        async fn dispatch(
            &self,
            node_id: NodeId,
            task: &Task,
        ) -> Result<TaskResult, DispatchError> {
            use std::sync::atomic::Ordering;
            let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);

            tokio::time::sleep(self.takes).await;
            let result = self.inner.dispatch(node_id, task).await;

            self.running.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn send_data(
            &self,
            node_id: NodeId,
            descriptor: aether_core::DataDescriptor,
            codec: aether_core::Codec,
            bytes: &[u8],
        ) -> Result<(), DispatchError> {
            self.inner
                .send_data(node_id, descriptor, codec, bytes)
                .await
        }

        async fn send_manifest(
            &self,
            node_id: NodeId,
            manifest: &aether_core::ChunkManifest,
        ) -> Result<(), DispatchError> {
            self.inner.send_manifest(node_id, manifest).await
        }

        async fn send_chunk(
            &self,
            node_id: NodeId,
            data_id: aether_core::DataId,
            index: u32,
            codec: aether_core::Codec,
            bytes: &[u8],
        ) -> Result<(), DispatchError> {
            self.inner
                .send_chunk(node_id, data_id, index, codec, bytes)
                .await
        }
    }

    #[tokio::test]
    async fn independent_steps_run_at_the_same_time() {
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            Slow {
                inner: SimulatedMesh::with_executor(aether_agent::execute),
                takes: Duration::from_millis(30),
                running: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                peak: peak.clone(),
            },
            DataCatalog::new(),
        );
        for index in 0..4 {
            controller.register(NodeInfo::new(
                NodeId::generate(),
                format!("node-{index}"),
                "127.0.0.1:1",
                4,
            ));
        }

        // One root, four independent branches, one join.
        let mut steps = vec![Step::new(Task::new(kind::ECHO, b"seed".to_vec()))];
        steps.extend((0..4).map(|_| Step::after(task(kind::HASH), vec![0])));
        steps.push(Step::after(task(kind::HASH), vec![1, 2, 3, 4]));

        let workflow = Workflow::new(steps).unwrap();
        let started = std::time::Instant::now();
        let outcome = run_workflow(Arc::new(controller), &workflow).await.unwrap();
        let elapsed = started.elapsed();

        assert!(outcome.is_success(), "{outcome:?}");
        assert!(
            peak.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "the branches ran one after another"
        );
        // Six steps at 30 ms each is 180 ms serially. The four independent
        // ones overlap, so three waves is the floor.
        assert!(
            elapsed < Duration::from_millis(150),
            "took {elapsed:?}, which is close to running them in sequence"
        );
    }

    #[tokio::test]
    async fn a_chain_still_runs_one_step_at_a_time() {
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let controller = Controller::new(
            LeastLoadedScheduler::new(),
            Slow {
                inner: SimulatedMesh::with_executor(aether_agent::execute),
                takes: Duration::from_millis(5),
                running: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                peak: peak.clone(),
            },
            DataCatalog::new(),
        );
        controller.register(NodeInfo::new(NodeId::generate(), "only", "127.0.0.1:1", 4));

        let workflow = Workflow::chain(vec![
            Task::new(kind::ECHO, b"a".to_vec()),
            task(kind::HASH),
            task(kind::HASH),
        ])
        .unwrap();

        run_workflow(Arc::new(controller), &workflow).await.unwrap();

        // Concurrency is not permission to ignore the dependency order.
        assert_eq!(peak.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_workflow_with_nowhere_to_run_reports_which_step() {
        let controller = controller_with(0);
        let workflow = Workflow::parallel(vec![task(kind::ECHO)]).unwrap();

        match run_workflow(controller.clone(), &workflow).await {
            Err(FlowError::Dispatch { step, .. }) => assert_eq!(step, 0),
            other => panic!("expected a dispatch failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn results_come_back_in_the_order_the_steps_were_written() {
        let controller = controller_with(1);
        // Written so that the run order is not the written order.
        let workflow = Workflow::new(vec![
            Step::after(Task::new(kind::ECHO, b"last".to_vec()), vec![1]),
            Step::new(Task::new(kind::ECHO, b"first".to_vec())),
        ])
        .unwrap();

        let outcome = run_workflow(controller.clone(), &workflow).await.unwrap();

        assert_eq!(outcome.results[0].output(), Some(&b"last"[..]));
        assert_eq!(outcome.results[1].output(), Some(&b"first"[..]));
    }
}
