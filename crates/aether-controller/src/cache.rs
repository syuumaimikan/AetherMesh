//! Remembering what a task already produced.
//!
//! A task is a pure function of its kind, its payload, its module, and its
//! inputs — every one of which is content-addressed. Two submissions that agree
//! on all four cannot produce different answers, so the second one does not
//! need a node at all.
//!
//! This is off by default. Caching is only correct for deterministic tasks, and
//! a module granted the clock or randomness is no longer one.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aether_core::{DataId, Task, TaskResult};

/// Identity of a task's *work*, ignoring which submission it came from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkKey {
    kind: String,
    payload: DataId,
    module: Option<DataId>,
    inputs: Vec<DataId>,
}

impl WorkKey {
    /// Derives the key from a task. The payload is hashed rather than stored,
    /// so a large payload costs 32 bytes in the map.
    pub fn of(task: &Task) -> Self {
        Self {
            kind: task.kind.clone(),
            payload: DataId::of(&task.payload),
            module: task.module,
            inputs: task.inputs.clone(),
        }
    }
}

#[derive(Debug)]
struct Entry {
    result: TaskResult,
    stored: Instant,
}

/// Results of finished tasks, keyed by the work they did.
#[derive(Debug, Clone, Default)]
pub struct ResultCache {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<WorkKey, Entry>,
    hits: u64,
    misses: u64,
    capacity: usize,
    ttl: Option<Duration>,
}

impl ResultCache {
    /// A cache holding at most `capacity` results.
    ///
    /// Zero capacity means the cache is present but never stores anything,
    /// which is the honest way to express "disabled" without a second type.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                capacity,
                ..Inner::default()
            })),
        }
    }

    /// Also forgets entries older than `ttl`, for work whose inputs are
    /// content-addressed but whose *meaning* ages — a model that gets retrained
    /// under the same name, say.
    pub fn with_ttl(self, ttl: Duration) -> Self {
        self.lock().ttl = Some(ttl);
        self
    }

    /// Looks up a finished result for this task.
    pub fn get(&self, task: &Task) -> Option<TaskResult> {
        let key = WorkKey::of(task);
        let mut inner = self.lock();

        if let Some(ttl) = inner.ttl {
            inner
                .entries
                .retain(|_, entry| entry.stored.elapsed() < ttl);
        }

        match inner.entries.get(&key) {
            Some(entry) => {
                let mut result = entry.result.clone();
                inner.hits += 1;
                // The cached result belongs to the task that produced it; the
                // caller asked about this one.
                result.task_id = task.id;
                Some(result)
            }
            None => {
                inner.misses += 1;
                None
            }
        }
    }

    /// Remembers a result. Failures are not cached: they are usually about the
    /// node, not the work, and retrying elsewhere is the point.
    pub fn put(&self, task: &Task, result: &TaskResult) {
        if !result.is_success() {
            return;
        }

        let mut inner = self.lock();
        if inner.capacity == 0 {
            return;
        }

        // Simplest eviction that cannot grow without bound: when full, drop the
        // oldest entry. A cache this small does not need an LRU.
        if inner.entries.len() >= inner.capacity
            && let Some(oldest) = inner
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.stored)
                .map(|(key, _)| key.clone())
        {
            inner.entries.remove(&oldest);
        }

        inner.entries.insert(
            WorkKey::of(task),
            Entry {
                result: result.clone(),
                stored: Instant::now(),
            },
        );
    }

    /// How often the cache answered, and how often it did not.
    pub fn stats(&self) -> (u64, u64) {
        let inner = self.lock();
        (inner.hits, inner.misses)
    }

    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Forgets everything, for when the world changed underneath the cache.
    pub fn clear(&self) {
        self.lock().entries.clear();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        aether_core::lock(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use aether_core::NodeId;
    use aether_core::task::kind;

    use super::*;

    fn result(task: &Task, output: &[u8]) -> TaskResult {
        TaskResult::success(
            task.id,
            NodeId::generate(),
            output.to_vec(),
            Duration::from_millis(5),
        )
    }

    #[test]
    fn the_same_work_hits_even_from_a_different_submission() {
        let cache = ResultCache::new(8);
        let first = Task::new(kind::HASH, b"payload".to_vec());
        cache.put(&first, &result(&first, b"digest"));

        // A separate task: new id, identical work.
        let second = Task::new(kind::HASH, b"payload".to_vec());
        let hit = cache.get(&second).expect("cache hit");

        assert_eq!(hit.output(), Some(&b"digest"[..]));
        // The result is reported against the task that asked for it.
        assert_eq!(hit.task_id, second.id);
        assert_eq!(cache.stats(), (1, 0));
    }

    #[test]
    fn different_payloads_do_not_collide() {
        let cache = ResultCache::new(8);
        let first = Task::new(kind::HASH, b"a".to_vec());
        cache.put(&first, &result(&first, b"one"));

        assert!(cache.get(&Task::new(kind::HASH, b"b".to_vec())).is_none());
        assert_eq!(cache.stats(), (0, 1));
    }

    #[test]
    fn different_inputs_do_not_collide() {
        let cache = ResultCache::new(8);
        let dataset = DataId::of(b"dataset");
        let other = DataId::of(b"other");

        let first = Task::new(kind::HASH, Vec::new()).with_inputs(vec![dataset]);
        cache.put(&first, &result(&first, b"one"));

        let second = Task::new(kind::HASH, Vec::new()).with_inputs(vec![other]);
        assert!(cache.get(&second).is_none());
    }

    #[test]
    fn a_different_module_is_different_work() {
        let cache = ResultCache::new(8);
        let first = Task::wasm(DataId::of(b"module-a"), b"input".to_vec());
        cache.put(&first, &result(&first, b"one"));

        let second = Task::wasm(DataId::of(b"module-b"), b"input".to_vec());
        assert!(cache.get(&second).is_none());
    }

    #[test]
    fn failures_are_not_remembered() {
        let cache = ResultCache::new(8);
        let task = Task::new(kind::HASH, b"payload".to_vec());
        let failure = TaskResult::failure(
            task.id,
            NodeId::generate(),
            "node fell over",
            Duration::ZERO,
        );

        cache.put(&task, &failure);

        assert!(cache.is_empty());
        assert!(cache.get(&task).is_none());
    }

    #[test]
    fn a_zero_capacity_cache_stores_nothing() {
        let cache = ResultCache::new(0);
        let task = Task::new(kind::HASH, b"payload".to_vec());
        cache.put(&task, &result(&task, b"digest"));

        assert!(cache.get(&task).is_none());
    }

    #[test]
    fn the_oldest_entry_is_evicted_when_full() {
        let cache = ResultCache::new(2);
        let tasks: Vec<Task> = (0..3)
            .map(|i| Task::new(kind::HASH, vec![i as u8]))
            .collect();

        for task in &tasks {
            cache.put(task, &result(task, b"x"));
            std::thread::sleep(Duration::from_millis(2));
        }

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&tasks[0]).is_none(), "oldest should be gone");
        assert!(cache.get(&tasks[2]).is_some());
    }

    #[test]
    fn entries_expire_when_a_ttl_is_set() {
        let cache = ResultCache::new(8).with_ttl(Duration::from_millis(30));
        let task = Task::new(kind::HASH, b"payload".to_vec());
        cache.put(&task, &result(&task, b"digest"));

        assert!(cache.get(&task).is_some());
        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get(&task).is_none());
    }

    #[test]
    fn clearing_forgets_everything() {
        let cache = ResultCache::new(8);
        let task = Task::new(kind::HASH, b"payload".to_vec());
        cache.put(&task, &result(&task, b"digest"));

        cache.clear();
        assert!(cache.is_empty());
    }
}
