//! What runs next when more work is waiting than there are nodes to take it.
//!
//! Placement decides *where*; this decides *when*. Three rules, and no more
//! than three, because a queue nobody can predict is worse than a slow one:
//!
//! 1. Higher priority first.
//! 2. Within one priority, the order they arrived.
//! 3. Waiting counts. A task gains a level for every [`Queue::aging`] it has
//!    spent waiting, so a stream of `Critical` work delays `Background` work
//!    rather than cancelling it. Without this rule the lowest priority is not
//!    a priority, it is a promise that is never kept.
//!
//! The queue is scanned linearly. It holds the tasks that arrived while one
//! was dispatching — tens, not millions — and a linear scan over that is both
//! faster than a heap and, more importantly, able to re-rank on every pop,
//! which is what rule 3 needs.

use std::time::{Duration, Instant};

use aether_core::{Priority, Task};

/// How long a task waits before it counts as one level more urgent.
///
/// Long enough that a busy mesh is not constantly re-ranking, short enough
/// that a `Background` task reaches the front inside a coffee break.
pub const DEFAULT_AGING: Duration = Duration::from_secs(30);

/// One task waiting for a node.
#[derive(Debug)]
pub struct Queued<T> {
    pub task: Task,
    /// Whatever the caller needs back when this finally runs — a reply channel,
    /// usually. The queue does not care what it is.
    pub payload: T,
    /// When it joined the queue.
    pub queued_at: Instant,
    /// Arrival order, which is what makes equal priorities FIFO.
    sequence: u64,
}

impl<T> Queued<T> {
    /// The priority this task is treated as right now, after waiting.
    pub fn effective_priority(&self, now: Instant, aging: Duration) -> Priority {
        let mut priority = self.task.priority;
        if aging.is_zero() {
            return priority;
        }

        let waited = now.saturating_duration_since(self.queued_at);
        // `as u32` saturates at the top, and the loop is bounded by the number
        // of levels anyway, so a task waiting for a week is simply Critical.
        let levels = (waited.as_secs_f64() / aging.as_secs_f64()) as u32;
        for _ in 0..levels.min(Priority::ALL.len() as u32) {
            priority = priority.promoted();
        }
        priority
    }

    /// How long this has been waiting.
    pub fn waited(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.queued_at)
    }
}

/// Tasks waiting for a node.
#[derive(Debug)]
pub struct Queue<T> {
    entries: Vec<Queued<T>>,
    next_sequence: u64,
    aging: Duration,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_sequence: 0,
            aging: DEFAULT_AGING,
        }
    }

    /// Sets how long a task waits before it counts as one level more urgent.
    ///
    /// Zero turns promotion off, which makes a low priority a genuine risk of
    /// never running. It exists for tests and for operators who mean it.
    pub fn with_aging(mut self, aging: Duration) -> Self {
        self.aging = aging;
        self
    }

    pub fn aging(&self) -> Duration {
        self.aging
    }

    /// Adds a task, stamped with its arrival.
    pub fn push(&mut self, task: Task, payload: T, now: Instant) {
        self.entries.push(Queued {
            task,
            payload,
            queued_at: now,
            sequence: self.next_sequence,
        });
        self.next_sequence += 1;
    }

    /// Takes the task that should run next, or `None` if nothing is waiting.
    pub fn pop(&mut self, now: Instant) -> Option<Queued<T>> {
        let aging = self.aging;
        let best = self
            .entries
            .iter()
            .enumerate()
            .max_by_key(|(_, entry)| {
                // Highest effective priority wins; the earliest arrival breaks
                // the tie, so `Reverse` on the sequence.
                (
                    entry.effective_priority(now, aging),
                    std::cmp::Reverse(entry.sequence),
                )
            })
            .map(|(index, _)| index)?;

        Some(self.entries.remove(best))
    }

    /// Everything waiting, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &Queued<T>> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many are waiting at each declared priority, lowest level first.
    ///
    /// Declared, not effective: an operator asking "what did I submit" is not
    /// asking what the queue has since decided about it.
    pub fn depth_by_priority(&self) -> [usize; 5] {
        let mut depth = [0; 5];
        for entry in &self.entries {
            depth[entry.task.priority as usize] += 1;
        }
        depth
    }

    /// The longest a task has been waiting.
    pub fn oldest_wait(&self, now: Instant) -> Duration {
        self.entries
            .iter()
            .map(|entry| entry.waited(now))
            .max()
            .unwrap_or(Duration::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(kind: &str, priority: Priority) -> Task {
        Task::new(kind, Vec::new()).with_priority(priority)
    }

    /// Pops everything and reports the order, by task kind.
    fn drain(queue: &mut Queue<()>, now: Instant) -> Vec<String> {
        let mut order = Vec::new();
        while let Some(entry) = queue.pop(now) {
            order.push(entry.task.kind);
        }
        order
    }

    #[test]
    fn an_empty_queue_has_nothing_to_run() {
        let mut queue = Queue::<()>::new();
        assert!(queue.is_empty());
        assert!(queue.pop(Instant::now()).is_none());
    }

    #[test]
    fn higher_priority_runs_first_whatever_the_arrival_order() {
        let now = Instant::now();
        let mut queue = Queue::new();

        // Submitted in the least helpful order possible.
        queue.push(task("background", Priority::Background), (), now);
        queue.push(task("normal", Priority::Normal), (), now);
        queue.push(task("critical", Priority::Critical), (), now);
        queue.push(task("low", Priority::Low), (), now);
        queue.push(task("high", Priority::High), (), now);

        assert_eq!(
            drain(&mut queue, now),
            ["critical", "high", "normal", "low", "background"]
        );
    }

    #[test]
    fn one_priority_is_first_in_first_out() {
        let now = Instant::now();
        let mut queue = Queue::new();
        for index in 0..5 {
            queue.push(task(&format!("task-{index}"), Priority::Normal), (), now);
        }

        assert_eq!(
            drain(&mut queue, now),
            ["task-0", "task-1", "task-2", "task-3", "task-4"]
        );
    }

    #[test]
    fn arrival_order_holds_across_pushes_that_interleave_with_pops() {
        let now = Instant::now();
        let mut queue = Queue::new();
        queue.push(task("first", Priority::Normal), (), now);
        queue.push(task("second", Priority::Normal), (), now);

        assert_eq!(queue.pop(now).unwrap().task.kind, "first");
        queue.push(task("third", Priority::Normal), (), now);

        // The sequence counter keeps counting; it is not the index in a Vec.
        assert_eq!(drain(&mut queue, now), ["second", "third"]);
    }

    #[test]
    fn waiting_promotes_a_task_a_level_at_a_time() {
        let start = Instant::now();
        let aging = Duration::from_secs(10);
        let mut queue = Queue::new().with_aging(aging);
        queue.push(task("patient", Priority::Low), (), start);

        let entry = queue.iter().next().expect("the queued task");
        assert_eq!(entry.effective_priority(start, aging), Priority::Low);
        assert_eq!(
            entry.effective_priority(start + Duration::from_secs(10), aging),
            Priority::Normal
        );
        assert_eq!(
            entry.effective_priority(start + Duration::from_secs(25), aging),
            Priority::High
        );
        assert_eq!(
            entry.effective_priority(start + Duration::from_secs(600), aging),
            Priority::Critical,
            "promotion stops at the top rather than overflowing"
        );
    }

    #[test]
    fn a_waiting_background_task_eventually_beats_fresh_urgent_work() {
        let start = Instant::now();
        let aging = Duration::from_secs(10);
        let mut queue = Queue::new().with_aging(aging);

        queue.push(task("patient", Priority::Background), (), start);
        // Forty seconds of Critical work keeps arriving.
        let later = start + Duration::from_secs(40);
        queue.push(task("urgent", Priority::Critical), (), later);

        // Four levels of promotion makes the background task Critical, and it
        // arrived first. A queue that only ever ran the newest Critical task
        // would never run the background one at all.
        assert_eq!(drain(&mut queue, later), ["patient", "urgent"]);
    }

    #[test]
    fn without_aging_a_low_priority_task_can_wait_forever() {
        let start = Instant::now();
        let mut queue = Queue::new().with_aging(Duration::ZERO);
        queue.push(task("patient", Priority::Background), (), start);
        queue.push(
            task("urgent", Priority::Critical),
            (),
            start + Duration::from_secs(3600),
        );

        // Turning promotion off is allowed, and this is what it means.
        assert_eq!(
            drain(&mut queue, start + Duration::from_secs(7200)),
            ["urgent", "patient"]
        );
    }

    #[test]
    fn a_promoted_task_still_loses_to_something_genuinely_higher() {
        let start = Instant::now();
        let aging = Duration::from_secs(10);
        let mut queue = Queue::new().with_aging(aging);

        // Waited one level: Low becomes Normal.
        queue.push(task("aged", Priority::Low), (), start);
        let later = start + Duration::from_secs(15);
        queue.push(task("high", Priority::High), (), later);

        assert_eq!(drain(&mut queue, later), ["high", "aged"]);
    }

    #[test]
    fn the_payload_comes_back_with_its_task() {
        let now = Instant::now();
        let mut queue = Queue::new();
        queue.push(task("a", Priority::Normal), "reply-channel", now);

        let entry = queue.pop(now).expect("the task");
        assert_eq!(entry.payload, "reply-channel");
        assert_eq!(entry.task.kind, "a");
    }

    #[test]
    fn depth_reports_what_was_submitted_not_what_aging_did_to_it() {
        let start = Instant::now();
        let mut queue = Queue::new().with_aging(Duration::from_secs(1));
        queue.push(task("a", Priority::Background), (), start);
        queue.push(task("b", Priority::Normal), (), start);
        queue.push(task("c", Priority::Normal), (), start);

        // Long enough that everything has been promoted several times.
        let now = start + Duration::from_secs(60);
        assert_eq!(queue.depth_by_priority(), [1, 0, 2, 0, 0]);
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.oldest_wait(now), Duration::from_secs(60));
    }

    #[test]
    fn the_oldest_wait_of_an_empty_queue_is_zero() {
        let queue = Queue::<()>::new();
        assert_eq!(queue.oldest_wait(Instant::now()), Duration::ZERO);
    }
}
