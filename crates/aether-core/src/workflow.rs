//! Tasks that depend on other tasks.
//!
//! A single task is placed where it is cheapest to run. A *workflow* is where
//! that decision compounds: if B reads what A produced, then running B where A
//! ran costs nothing to move, and running it anywhere else costs the whole
//! intermediate result. The point of this project, applied to its own output.
//!
//! ```text
//!     A
//!    / \
//!   B   C
//!    \ /
//!     D
//! ```
//!
//! A workflow is a list of steps and, for each, which earlier steps it waits
//! for. Dependencies are indices into that list, which makes the common
//! mistakes — naming a step that does not exist, or a cycle — things this
//! module can refuse before anything runs.

use serde::{Deserialize, Serialize};

use crate::id::WorkflowId;
use crate::task::Task;

/// One step and what it waits for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub task: Task,
    /// Indices of earlier steps this one needs. Empty means it can start now.
    ///
    /// Each dependency's output becomes an input of this task when the
    /// workflow runs, so a step reads what the steps before it produced.
    #[serde(default)]
    pub depends_on: Vec<usize>,
}

impl Step {
    /// A step with nothing to wait for.
    pub fn new(task: Task) -> Self {
        Self {
            task,
            depends_on: Vec::new(),
        }
    }

    /// A step that runs after these earlier ones, reading what they produced.
    pub fn after(task: Task, depends_on: Vec<usize>) -> Self {
        Self { task, depends_on }
    }
}

/// A workflow was not runnable.
///
/// Every one of these is caught before a single task is dispatched: a workflow
/// that fails halfway leaves work half-done on real machines.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowError {
    #[error("a workflow needs at least one step")]
    Empty,
    #[error("step {step} depends on step {missing}, which does not exist")]
    UnknownDependency { step: usize, missing: usize },
    #[error("step {0} depends on itself")]
    SelfDependency(usize),
    #[error("steps {} form a cycle; a workflow has to finish", .0.iter().map(usize::to_string).collect::<Vec<_>>().join(" -> "))]
    Cycle(Vec<usize>),
    #[error("step {step} is not in this workflow")]
    UnknownStep { step: usize },
}

/// A set of tasks and the order they have to run in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workflow {
    pub id: WorkflowId,
    pub steps: Vec<Step>,
}

impl Workflow {
    /// Builds a workflow, refusing one that cannot finish.
    pub fn new(steps: Vec<Step>) -> Result<Self, WorkflowError> {
        let workflow = Self {
            id: WorkflowId::generate(),
            steps,
        };
        workflow.validate()?;
        Ok(workflow)
    }

    /// A workflow of independent tasks, for the fan-out case.
    pub fn parallel(tasks: Vec<Task>) -> Result<Self, WorkflowError> {
        Self::new(tasks.into_iter().map(Step::new).collect())
    }

    /// A workflow where each task reads what the one before it produced.
    pub fn chain(tasks: Vec<Task>) -> Result<Self, WorkflowError> {
        let steps = tasks
            .into_iter()
            .enumerate()
            .map(|(index, task)| match index {
                0 => Step::new(task),
                index => Step::after(task, vec![index - 1]),
            })
            .collect();
        Self::new(steps)
    }

    pub fn len(&self) -> usize {
        self.steps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Checks every dependency exists and that the whole thing can finish.
    pub fn validate(&self) -> Result<(), WorkflowError> {
        if self.steps.is_empty() {
            return Err(WorkflowError::Empty);
        }

        for (index, step) in self.steps.iter().enumerate() {
            for &dependency in &step.depends_on {
                if dependency == index {
                    return Err(WorkflowError::SelfDependency(index));
                }
                if dependency >= self.steps.len() {
                    return Err(WorkflowError::UnknownDependency {
                        step: index,
                        missing: dependency,
                    });
                }
            }
        }

        self.order().map(|_| ())
    }

    /// An order in which every step runs after everything it depends on.
    ///
    /// Kahn's algorithm, taking ready steps in index order so the same
    /// workflow always produces the same plan — a scheduler that reorders
    /// independent work run to run is one nobody can debug.
    pub fn order(&self) -> Result<Vec<usize>, WorkflowError> {
        let mut remaining: Vec<usize> = self
            .steps
            .iter()
            .map(|step| step.depends_on.len())
            .collect();
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); self.steps.len()];
        for (index, step) in self.steps.iter().enumerate() {
            for &dependency in &step.depends_on {
                if let Some(list) = dependents.get_mut(dependency) {
                    list.push(index);
                }
            }
        }

        let mut ready: Vec<usize> = (0..self.steps.len())
            .filter(|index| remaining[*index] == 0)
            .collect();
        let mut order = Vec::with_capacity(self.steps.len());

        while let Some(index) = ready.first().copied() {
            ready.remove(0);
            order.push(index);

            for &dependent in &dependents[index] {
                remaining[dependent] -= 1;
                if remaining[dependent] == 0 {
                    // Kept sorted so ties break by index, every time.
                    let position = ready.partition_point(|waiting| *waiting < dependent);
                    ready.insert(position, dependent);
                }
            }
        }

        if order.len() != self.steps.len() {
            // Whatever never became ready is in a cycle with something else.
            let stuck = (0..self.steps.len())
                .filter(|index| !order.contains(index))
                .collect();
            return Err(WorkflowError::Cycle(stuck));
        }
        Ok(order)
    }

    /// The steps that can start immediately.
    pub fn roots(&self) -> Vec<usize> {
        (0..self.steps.len())
            .filter(|index| self.steps[*index].depends_on.is_empty())
            .collect()
    }

    /// Steps that wait on this one.
    pub fn dependents_of(&self, step: usize) -> Vec<usize> {
        (0..self.steps.len())
            .filter(|index| self.steps[*index].depends_on.contains(&step))
            .collect()
    }

    /// Groups of steps that can run at the same time, earliest first.
    ///
    /// Only useful for showing someone the shape of their workflow; the
    /// controller runs by readiness, not by wave.
    pub fn waves(&self) -> Result<Vec<Vec<usize>>, WorkflowError> {
        let order = self.order()?;
        let mut depth = vec![0usize; self.steps.len()];
        for index in order {
            depth[index] = self.steps[index]
                .depends_on
                .iter()
                .map(|dependency| depth[*dependency] + 1)
                .max()
                .unwrap_or(0);
        }

        let mut waves = vec![Vec::new(); depth.iter().max().map_or(0, |deepest| deepest + 1)];
        for (index, level) in depth.iter().enumerate() {
            waves[*level].push(index);
        }
        Ok(waves)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(kind: &str) -> Task {
        Task::new(kind, Vec::new())
    }

    /// The diamond from the module docs: A, then B and C, then D.
    fn diamond() -> Workflow {
        Workflow::new(vec![
            Step::new(task("a")),
            Step::after(task("b"), vec![0]),
            Step::after(task("c"), vec![0]),
            Step::after(task("d"), vec![1, 2]),
        ])
        .expect("a valid diamond")
    }

    #[test]
    fn an_empty_workflow_is_refused() {
        assert_eq!(Workflow::new(Vec::new()), Err(WorkflowError::Empty));
    }

    #[test]
    fn independent_steps_all_start_at_once() {
        let workflow = Workflow::parallel(vec![task("a"), task("b"), task("c")]).unwrap();

        assert_eq!(workflow.roots(), [0, 1, 2]);
        assert_eq!(workflow.order().unwrap(), [0, 1, 2]);
        assert_eq!(workflow.waves().unwrap(), vec![vec![0, 1, 2]]);
    }

    #[test]
    fn a_chain_runs_in_the_order_it_was_written() {
        let workflow = Workflow::chain(vec![task("a"), task("b"), task("c")]).unwrap();

        assert_eq!(workflow.roots(), [0]);
        assert_eq!(workflow.order().unwrap(), [0, 1, 2]);
        assert_eq!(workflow.waves().unwrap(), vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn a_diamond_runs_its_middle_in_parallel_and_its_end_last() {
        let workflow = diamond();

        assert_eq!(workflow.roots(), [0]);
        assert_eq!(workflow.order().unwrap(), [0, 1, 2, 3]);
        assert_eq!(
            workflow.waves().unwrap(),
            vec![vec![0], vec![1, 2], vec![3]]
        );
        assert_eq!(workflow.dependents_of(0), [1, 2]);
        assert_eq!(workflow.dependents_of(3), Vec::<usize>::new());
    }

    #[test]
    fn a_step_always_comes_after_everything_it_waits_for() {
        // Written in an order that is not the order it can run in.
        let workflow = Workflow::new(vec![
            Step::after(task("last"), vec![1, 2]),
            Step::after(task("middle"), vec![3]),
            Step::after(task("other-middle"), vec![3]),
            Step::new(task("first")),
        ])
        .unwrap();

        let order = workflow.order().unwrap();
        let position = |step: usize| order.iter().position(|index| *index == step).unwrap();

        assert_eq!(order[0], 3, "the only root goes first");
        assert!(position(1) < position(0));
        assert!(position(2) < position(0));
    }

    #[test]
    fn the_same_workflow_always_produces_the_same_plan() {
        // Independent steps are taken in index order rather than whatever the
        // data structure happened to yield: a scheduler that reorders work run
        // to run is one nobody can debug.
        let workflow = diamond();
        let plans: Vec<_> = (0..20).map(|_| workflow.order().unwrap()).collect();

        assert!(plans.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn a_step_that_depends_on_itself_is_refused() {
        let outcome = Workflow::new(vec![Step::after(task("a"), vec![0])]);
        assert_eq!(outcome, Err(WorkflowError::SelfDependency(0)));
    }

    #[test]
    fn a_dependency_on_a_step_that_does_not_exist_is_refused() {
        let outcome = Workflow::new(vec![Step::new(task("a")), Step::after(task("b"), vec![7])]);
        assert_eq!(
            outcome,
            Err(WorkflowError::UnknownDependency {
                step: 1,
                missing: 7
            })
        );
    }

    #[test]
    fn a_cycle_is_refused_before_anything_runs() {
        // A -> B -> C -> A. Discovering this halfway through leaves work
        // half-done on machines somebody else owns.
        let outcome = Workflow::new(vec![
            Step::after(task("a"), vec![2]),
            Step::after(task("b"), vec![0]),
            Step::after(task("c"), vec![1]),
        ]);

        match outcome {
            Err(WorkflowError::Cycle(stuck)) => assert_eq!(stuck, [0, 1, 2]),
            other => panic!("expected a cycle, got {other:?}"),
        }
    }

    #[test]
    fn a_cycle_among_some_steps_does_not_hide_behind_the_ones_that_are_fine() {
        let outcome = Workflow::new(vec![
            Step::new(task("fine")),
            Step::after(task("stuck"), vec![2]),
            Step::after(task("also-stuck"), vec![1]),
        ]);

        match outcome {
            Err(WorkflowError::Cycle(stuck)) => assert_eq!(stuck, [1, 2]),
            other => panic!("expected a cycle, got {other:?}"),
        }
    }

    #[test]
    fn a_two_step_cycle_is_refused() {
        let outcome = Workflow::new(vec![
            Step::after(task("a"), vec![1]),
            Step::after(task("b"), vec![0]),
        ]);
        assert!(matches!(outcome, Err(WorkflowError::Cycle(_))));
    }

    #[test]
    fn a_wide_workflow_orders_without_trouble() {
        // One root, five hundred dependents, one join. Nothing here should
        // care about size, and this is where a quadratic mistake would show.
        let mut steps = vec![Step::new(task("root"))];
        steps.extend((0..500).map(|_| Step::after(task("leaf"), vec![0])));
        steps.push(Step::after(task("join"), (1..=500).collect()));

        let workflow = Workflow::new(steps).unwrap();
        let order = workflow.order().unwrap();

        assert_eq!(order.len(), 502);
        assert_eq!(order[0], 0);
        assert_eq!(*order.last().unwrap(), 501);
        assert_eq!(workflow.waves().unwrap().len(), 3);
    }

    #[test]
    fn a_workflow_survives_a_round_trip_through_json() {
        let workflow = diamond();
        let encoded = serde_json::to_string(&workflow).expect("serialisable");
        let decoded: Workflow = serde_json::from_str(&encoded).expect("readable");

        assert_eq!(decoded, workflow);
    }

    #[test]
    fn a_step_written_without_dependencies_deserializes_as_a_root() {
        let json = r#"{"task":{"id":"c8e6f6e0-0000-4000-8000-000000000000","kind":"hash",
            "payload":[],"inputs":[],"constraints":[],"module":null}}"#;
        let step: Step = serde_json::from_str(json).expect("a step");

        assert!(step.depends_on.is_empty());
    }
}
