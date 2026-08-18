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
    /// One result per step, in the order the steps were written — not the
    /// order they ran in, which nobody asked about.
    pub results: Vec<TaskResult>,
    /// Steps that were never attempted because something earlier failed.
    pub skipped: Vec<usize>,
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
/// One step at a time, including steps that do not depend on each other.
/// That is a real limitation rather than a choice: `Controller::submit` takes
/// `&mut self`, so running two steps at once needs the controller to stop
/// being exclusively owned for the length of a dispatch. Until then, a
/// workflow's wall clock is the sum of its steps, and independent branches
/// will concentrate on one node because there is never any contention to
/// spread them.
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
    // Validate before dispatching anything: a cycle discovered halfway
    // through leaves work half-done on machines somebody else owns.
    workflow.order()?;

    let mut waiting: Vec<usize> = workflow
        .steps
        .iter()
        .map(|step| step.depends_on.len())
        .collect();
    let mut outputs: HashMap<usize, aether_core::DataId> = HashMap::new();
    let mut results: HashMap<usize, TaskResult> = HashMap::new();
    let mut placements: Vec<Placement> = Vec::new();
    let mut abandoned: Vec<usize> = Vec::new();
    let mut ready: Vec<usize> = workflow.roots();

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
        let (step, outcome) = finished.expect("a dispatch task never panics");

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
    placements.sort_unstable_by_key(|placement| placement.step);
    Ok(FlowResult {
        results: (0..workflow.len())
            .filter_map(|step| results.remove(&step))
            .collect(),
        skipped: abandoned,
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
