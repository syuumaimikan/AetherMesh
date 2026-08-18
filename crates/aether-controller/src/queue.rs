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
//! A queue can also be told when to stop accepting work: a size limit with a
//! policy for what gives way, and a deadline after which waiting is pointless.
//! Both are off by default, because a mesh that silently starts refusing work
//! is worse than one that visibly falls behind — the operator should choose.
//!
//! The queue is scanned linearly. It holds the tasks that arrived while one
//! was dispatching — tens, not millions — and a linear scan over that is both
//! faster than a heap and, more importantly, able to re-rank on every pop,
//! which is what rule 3 needs.

use std::time::{Duration, Instant};

use aether_core::{Priority, Task};
use serde::{Deserialize, Serialize};

/// How long a task waits before it counts as one level more urgent.
///
/// Long enough that a busy mesh is not constantly re-ranking, short enough
/// that a `Background` task reaches the front inside a coffee break.
pub const DEFAULT_AGING: Duration = Duration::from_secs(30);

/// What gives way when the queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rejection {
    /// Refuse the new task. Usually what a caller wants: a submission that
    /// comes back is one you can retry, report, or shed load over.
    #[default]
    Reject,
    /// Drop whatever has waited longest. Right when the newest work is the
    /// only work worth doing — a live feed rather than a batch.
    DropOldest,
    /// Drop the least urgent thing waiting, and refuse the new task if it is
    /// itself the least urgent. Keeps a full queue full of work that matters,
    /// at the cost of telling some callers no.
    DropLowestPriority,
}

/// What happened to a task offered to the queue.
#[derive(Debug)]
pub enum Admitted<T> {
    /// It is waiting for a node.
    Queued,
    /// The queue was full and the policy refused it. Handed straight back, so
    /// the caller can be told rather than left waiting for a result that is
    /// never coming.
    Refused(Task, T),
    /// It was accepted, and this was dropped to make room.
    Displaced(Queued<T>),
}

/// One task waiting for a node.
#[derive(Debug)]
pub struct Queued<T> {
    pub task: Task,
    /// Whatever the caller needs back when this finally runs — a reply channel,
    /// usually. The queue does not care what it is.
    pub payload: T,
    /// When it joined the queue.
    pub queued_at: Instant,
    /// When waiting stops being worth it. `None` means as long as it takes.
    pub deadline: Option<Instant>,
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

    /// Whether waiting any longer is pointless.
    pub fn expired(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|deadline| now >= deadline)
    }
}

/// Tasks waiting for a node.
#[derive(Debug)]
pub struct Queue<T> {
    entries: Vec<Queued<T>>,
    next_sequence: u64,
    aging: Duration,
    /// `None` means the queue grows until memory says otherwise.
    max_size: Option<usize>,
    /// Default deadline for a task that does not bring its own.
    timeout: Option<Duration>,
    rejection: Rejection,
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
            max_size: None,
            timeout: None,
            rejection: Rejection::default(),
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

    /// Caps how many tasks may wait at once. Zero means no cap.
    ///
    /// A cap is a decision about what to do when the mesh cannot keep up.
    /// Without one the answer is "accept everything and get slower" — which is
    /// also a decision, just not one anybody made on purpose.
    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = (max_size > 0).then_some(max_size);
        self
    }

    /// Sets what gives way when the queue is full.
    pub fn with_rejection(mut self, rejection: Rejection) -> Self {
        self.rejection = rejection;
        self
    }

    /// Sets how long a task waits before giving up. Zero means no deadline.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = (!timeout.is_zero()).then_some(timeout);
        self
    }

    pub fn aging(&self) -> Duration {
        self.aging
    }

    pub fn max_size(&self) -> Option<usize> {
        self.max_size
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn rejection(&self) -> Rejection {
        self.rejection
    }

    /// Whether the queue is at its limit.
    pub fn is_full(&self) -> bool {
        self.max_size
            .is_some_and(|max_size| self.entries.len() >= max_size)
    }

    /// Adds a task, giving it the queue's default deadline.
    pub fn push(&mut self, task: Task, payload: T, now: Instant) -> Admitted<T> {
        self.push_with_timeout(task, payload, now, None)
    }

    /// Same, with a deadline this task brought of its own.
    ///
    /// A caller who knows their work is worthless after five seconds should be
    /// able to say so without changing the deadline for everyone else.
    pub fn push_with_timeout(
        &mut self,
        task: Task,
        payload: T,
        now: Instant,
        timeout: Option<Duration>,
    ) -> Admitted<T> {
        let mut displaced = None;
        if self.is_full() {
            match self.make_room(&task, now) {
                Some(dropped) => displaced = dropped,
                None => return Admitted::Refused(task, payload),
            }
        }

        let deadline = timeout
            .or(self.timeout)
            .and_then(|timeout| now.checked_add(timeout));
        self.entries.push(Queued {
            task,
            payload,
            queued_at: now,
            deadline,
            sequence: self.next_sequence,
        });
        self.next_sequence += 1;

        match displaced {
            Some(entry) => Admitted::Displaced(entry),
            None => Admitted::Queued,
        }
    }

    /// Frees a slot according to the policy.
    ///
    /// `Some(None)` cannot happen today; the nesting exists so a future policy
    /// can accept without displacing anything.
    fn make_room(&mut self, incoming: &Task, now: Instant) -> Option<Option<Queued<T>>> {
        match self.rejection {
            Rejection::Reject => None,
            Rejection::DropOldest => {
                let oldest = self
                    .entries
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, entry)| entry.sequence)
                    .map(|(index, _)| index)?;
                Some(Some(self.entries.remove(oldest)))
            }
            Rejection::DropLowestPriority => {
                let aging = self.aging;
                let (index, victim) = self
                    .entries
                    .iter()
                    .enumerate()
                    // Least urgent, and the newest among equals: a task that
                    // has waited has earned its place more than one that has not.
                    .min_by_key(|(_, entry)| {
                        (entry.effective_priority(now, aging), entry.sequence)
                    })?;

                // The incoming task is judged as it stands: it has waited for
                // nothing yet, so there is no promotion to account for. Ties go
                // to whoever is already in the queue.
                if incoming.priority <= victim.effective_priority(now, aging) {
                    return None;
                }
                Some(Some(self.entries.remove(index)))
            }
        }
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

    /// Removes and returns everything whose deadline has passed.
    ///
    /// Waiting tasks are dropped here rather than at dispatch, so a caller is
    /// told at roughly the moment the promise broke, instead of whenever the
    /// queue happened to reach them.
    pub fn expire(&mut self, now: Instant) -> Vec<Queued<T>> {
        let mut expired = Vec::new();
        let mut index = 0;
        while index < self.entries.len() {
            if self.entries[index].expired(now) {
                expired.push(self.entries.remove(index));
            } else {
                index += 1;
            }
        }
        expired
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

    /// The kinds currently waiting, sorted so the assertion is stable.
    fn waiting(queue: &Queue<()>) -> Vec<String> {
        let mut kinds: Vec<_> = queue.iter().map(|entry| entry.task.kind.clone()).collect();
        kinds.sort();
        kinds
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

    #[test]
    fn a_queue_with_no_cap_accepts_everything() {
        let now = Instant::now();
        let mut queue = Queue::new();
        assert_eq!(queue.max_size(), None);

        for index in 0..500 {
            let admitted = queue.push(task(&format!("task-{index}"), Priority::Normal), (), now);
            assert!(matches!(admitted, Admitted::Queued));
        }
        assert_eq!(queue.len(), 500);
        assert!(!queue.is_full());
    }

    #[test]
    fn a_full_queue_refuses_by_default() {
        let now = Instant::now();
        let mut queue = Queue::new().with_max_size(2);
        queue.push(task("a", Priority::Normal), (), now);
        queue.push(task("b", Priority::Normal), (), now);

        assert!(queue.is_full());
        let admitted = queue.push(task("c", Priority::Critical), (), now);

        // Even a Critical task: Reject means reject, and the caller finds out
        // now rather than waiting for a result that is not coming.
        match admitted {
            Admitted::Refused(task, ()) => assert_eq!(task.kind, "c"),
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert_eq!(waiting(&queue), ["a", "b"]);
    }

    #[test]
    fn drop_oldest_makes_room_for_the_newest_work() {
        let now = Instant::now();
        let mut queue = Queue::new()
            .with_max_size(2)
            .with_rejection(Rejection::DropOldest);
        queue.push(task("a", Priority::Normal), (), now);
        queue.push(task("b", Priority::Normal), (), now);

        match queue.push(task("c", Priority::Normal), (), now) {
            Admitted::Displaced(entry) => assert_eq!(entry.task.kind, "a"),
            other => panic!("expected a displacement, got {other:?}"),
        }
        assert_eq!(waiting(&queue), ["b", "c"]);
    }

    #[test]
    fn drop_oldest_ignores_priority_entirely() {
        let now = Instant::now();
        let mut queue = Queue::new()
            .with_max_size(1)
            .with_rejection(Rejection::DropOldest);
        queue.push(task("critical", Priority::Critical), (), now);

        // "Newest wins" means exactly that. An operator choosing this policy
        // is saying stale work is worthless whatever it was labelled.
        match queue.push(task("background", Priority::Background), (), now) {
            Admitted::Displaced(entry) => assert_eq!(entry.task.kind, "critical"),
            other => panic!("expected a displacement, got {other:?}"),
        }
        assert_eq!(waiting(&queue), ["background"]);
    }

    #[test]
    fn drop_lowest_priority_evicts_the_least_urgent_waiter() {
        let now = Instant::now();
        let mut queue = Queue::new()
            .with_max_size(3)
            .with_rejection(Rejection::DropLowestPriority);
        queue.push(task("high", Priority::High), (), now);
        queue.push(task("background", Priority::Background), (), now);
        queue.push(task("normal", Priority::Normal), (), now);

        match queue.push(task("critical", Priority::Critical), (), now) {
            Admitted::Displaced(entry) => assert_eq!(entry.task.kind, "background"),
            other => panic!("expected a displacement, got {other:?}"),
        }
        assert_eq!(waiting(&queue), ["critical", "high", "normal"]);
    }

    #[test]
    fn drop_lowest_priority_refuses_work_that_is_itself_the_least_urgent() {
        let now = Instant::now();
        let mut queue = Queue::new()
            .with_max_size(1)
            .with_rejection(Rejection::DropLowestPriority);
        queue.push(task("normal", Priority::Normal), (), now);

        // Evicting something more urgent to admit something less urgent would
        // be the opposite of what this policy is for.
        assert!(matches!(
            queue.push(task("low", Priority::Low), (), now),
            Admitted::Refused(..)
        ));
        assert!(matches!(
            queue.push(task("same", Priority::Normal), (), now),
            Admitted::Refused(..)
        ));
        assert_eq!(waiting(&queue), ["normal"]);
    }

    #[test]
    fn a_waiting_task_is_harder_to_evict_than_a_fresh_one() {
        let start = Instant::now();
        let aging = Duration::from_secs(10);
        let mut queue = Queue::new()
            .with_max_size(2)
            .with_aging(aging)
            .with_rejection(Rejection::DropLowestPriority);

        queue.push(task("patient", Priority::Low), (), start);
        let later = start + Duration::from_secs(20);
        queue.push(task("fresh", Priority::Low), (), later);

        // `patient` has been promoted twice and `fresh` not at all, so the
        // newcomer is the one that goes. Waiting has to be worth something or
        // a busy queue would churn the same slot forever.
        match queue.push(task("normal", Priority::Normal), (), later) {
            Admitted::Displaced(entry) => assert_eq!(entry.task.kind, "fresh"),
            other => panic!("expected a displacement, got {other:?}"),
        }
        assert_eq!(waiting(&queue), ["normal", "patient"]);
    }

    #[test]
    fn nothing_expires_without_a_deadline() {
        let start = Instant::now();
        let mut queue = Queue::new();
        queue.push(task("patient", Priority::Normal), (), start);

        assert!(queue.timeout().is_none());
        assert!(queue.expire(start + Duration::from_secs(86_400)).is_empty());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn a_task_that_waited_past_its_deadline_is_handed_back() {
        let start = Instant::now();
        let mut queue = Queue::new().with_timeout(Duration::from_secs(5));
        queue.push(task("doomed", Priority::Normal), "reply", start);
        queue.push(
            task("kept", Priority::Normal),
            "reply",
            start + Duration::from_secs(4),
        );

        let expired = queue.expire(start + Duration::from_secs(6));

        // The payload comes back with it, so whoever is waiting can be told
        // rather than left holding a channel that never resolves.
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].task.kind, "doomed");
        assert_eq!(expired[0].payload, "reply");
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn a_task_can_bring_a_deadline_shorter_than_the_default() {
        let start = Instant::now();
        let mut queue = Queue::new().with_timeout(Duration::from_secs(60));
        queue.push_with_timeout(
            task("impatient", Priority::Normal),
            (),
            start,
            Some(Duration::from_secs(1)),
        );
        queue.push(task("patient", Priority::Normal), (), start);

        let expired = queue.expire(start + Duration::from_secs(2));
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].task.kind, "impatient");
    }

    #[test]
    fn expiring_several_at_once_leaves_the_rest_intact() {
        let start = Instant::now();
        let mut queue = Queue::new().with_timeout(Duration::from_secs(5));
        for index in 0..5 {
            queue.push(task(&format!("old-{index}"), Priority::Normal), (), start);
        }
        let later = start + Duration::from_secs(4);
        queue.push(task("newer", Priority::Normal), (), later);

        // Removing from a Vec while iterating it is exactly the sort of loop
        // that quietly skips every other element.
        let expired = queue.expire(start + Duration::from_secs(6));
        assert_eq!(expired.len(), 5);
        assert_eq!(waiting(&queue), ["newer"]);
    }

    #[test]
    fn a_zero_timeout_means_no_deadline_rather_than_an_instant_one() {
        let start = Instant::now();
        let mut queue = Queue::new().with_timeout(Duration::ZERO);
        queue.push(task("kept", Priority::Normal), (), start);

        assert!(queue.timeout().is_none());
        assert!(queue.expire(start + Duration::from_secs(3600)).is_empty());
    }

    #[test]
    fn a_zero_max_size_means_no_cap_rather_than_refusing_everything() {
        let now = Instant::now();
        let mut queue = Queue::new().with_max_size(0);

        assert_eq!(queue.max_size(), None);
        assert!(matches!(
            queue.push(task("a", Priority::Normal), (), now),
            Admitted::Queued
        ));
    }
}
