//! The last few finished tasks, so somebody watching can see what ran.
//!
//! Counters say a thousand tasks succeeded. They cannot say *your* task
//! succeeded, which is the question anyone actually has after submitting one
//! from another window. This is the smallest thing that answers it: a bounded
//! ring of what finished, on the authenticated client API only.
//!
//! Deliberately not on `/metrics`. That port has no authentication, and a list
//! of what ran, where, and what it produced is a different kind of thing from
//! a counter — the same reason the scrape endpoint reports no per-node labels.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aether_core::{NodeId, Task, TaskId, TaskResult};

/// How many finished tasks are kept.
///
/// Bounded because this is a debugging aid, not a record: a controller that
/// remembers every task it ever ran is a memory leak with a nice interface.
pub const DEFAULT_HISTORY: usize = 64;

/// Bytes of output kept for a preview.
///
/// Enough to recognise "HELLO FROM TYPESCRIPT", nowhere near enough to be a
/// way of reading data out of the mesh — outputs stay on the node that made
/// them, and this is a glance at the front of one.
pub const PREVIEW_BYTES: usize = 48;

/// One task that finished.
#[derive(Debug, Clone)]
pub struct Finished {
    pub task_id: TaskId,
    pub kind: String,
    pub node_id: NodeId,
    pub success: bool,
    pub duration: Duration,
    /// Size of the whole output, of which [`Finished::preview`] is the front.
    pub output_bytes: u64,
    pub preview: String,
    /// When it landed, for "how long ago" rather than a wall clock the client
    /// would have to trust agrees with its own.
    pub at: Instant,
}

impl Finished {
    /// How long ago this finished.
    pub fn age(&self) -> Duration {
        self.at.elapsed()
    }
}

/// A bounded ring of finished tasks. Cheap to clone; one ring underneath.
#[derive(Debug, Clone)]
pub struct History {
    entries: Arc<Mutex<VecDeque<Finished>>>,
    limit: usize,
}

impl Default for History {
    fn default() -> Self {
        Self::new(DEFAULT_HISTORY)
    }
}

impl History {
    pub fn new(limit: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::new())),
            limit: limit.max(1),
        }
    }

    /// Records a finished task, dropping the oldest when full.
    pub fn record(&self, task: &Task, result: &TaskResult) {
        let output = result.output().unwrap_or_default();
        let entry = Finished {
            task_id: result.task_id,
            kind: task.kind.clone(),
            node_id: result.node_id,
            success: result.is_success(),
            duration: result.duration,
            output_bytes: output.len() as u64,
            preview: preview(output),
            at: Instant::now(),
        };

        let mut entries = self.lock();
        entries.push_front(entry);
        while entries.len() > self.limit {
            entries.pop_back();
        }
    }

    /// The most recent finished tasks, newest first.
    pub fn recent(&self, limit: usize) -> Vec<Finished> {
        self.lock().iter().take(limit).cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Finished>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The front of an output, as something safe to print in a terminal.
///
/// Bytes that are not printable ASCII become `.`, because a task's output is
/// arbitrary data and a terminal will happily interpret escape sequences in it.
/// A watcher's screen is not a place for a task to write.
fn preview(output: &[u8]) -> String {
    let mut text: String = output
        .iter()
        .take(PREVIEW_BYTES)
        .map(|byte| match byte {
            0x20..=0x7e => *byte as char,
            _ => '.',
        })
        .collect();
    if output.len() > PREVIEW_BYTES {
        text.push('…');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_core::task::kind;

    fn task() -> Task {
        Task::new(kind::ECHO, Vec::new())
    }

    fn result(output: &[u8]) -> TaskResult {
        TaskResult::success(
            TaskId::generate(),
            NodeId::generate(),
            output.to_vec(),
            Duration::from_millis(3),
        )
    }

    #[test]
    fn the_newest_task_is_first() {
        let history = History::new(8);
        history.record(&task(), &result(b"one"));
        history.record(&task(), &result(b"two"));

        let recent = history.recent(8);

        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].preview, "two");
        assert_eq!(recent[1].preview, "one");
    }

    #[test]
    fn the_oldest_falls_off_the_end() {
        let history = History::new(2);
        for index in 0..5 {
            history.record(&task(), &result(format!("{index}").as_bytes()));
        }

        let recent = history.recent(10);

        // Bounded, not growing: five tasks in, two remembered.
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].preview, "4");
        assert_eq!(recent[1].preview, "3");
    }

    #[test]
    fn a_long_output_is_cut_and_says_it_was() {
        let history = History::new(4);
        let output = "x".repeat(PREVIEW_BYTES * 3);
        history.record(&task(), &result(output.as_bytes()));

        let entry = &history.recent(1)[0];

        assert_eq!(entry.preview.chars().count(), PREVIEW_BYTES + 1);
        assert!(entry.preview.ends_with('…'));
        // The real size is still reported, so a preview is never mistaken for
        // the whole thing.
        assert_eq!(entry.output_bytes, output.len() as u64);
    }

    #[test]
    fn a_task_cannot_write_to_a_watchers_terminal() {
        let history = History::new(4);
        history.record(&task(), &result(b"\x1b[2Jgone\x07"));

        // The escape would have cleared the screen of anybody watching.
        assert_eq!(history.recent(1)[0].preview, ".[2Jgone.");
    }

    #[test]
    fn a_failure_is_worth_remembering_too() {
        let history = History::new(4);
        let failed = TaskResult::failure(
            TaskId::generate(),
            NodeId::generate(),
            "no such kind".to_string(),
            Duration::from_millis(1),
        );
        history.record(&task(), &failed);

        let entry = &history.recent(1)[0];
        assert!(!entry.success);
        assert_eq!(entry.output_bytes, 0);
    }
}
