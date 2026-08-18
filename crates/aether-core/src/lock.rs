//! Taking a lock back after somebody panicked holding it.
//!
//! Rust poisons a `Mutex` when a thread panics while holding it, and the
//! default reaction — `unwrap()` — turns that into a panic in every thread
//! that touches the same lock afterwards. For a long-running control plane
//! that is the worst possible trade: one unlucky request takes out every
//! future request, and a mesh that was doing useful work stops entirely.
//!
//! So this recovers instead. It is safe to do *here* because of what is behind
//! these locks: maps of nodes, datasets, and counters, each entry independent
//! of the others. There is no invariant spanning two entries that a half-done
//! update could break, so the worst a recovered lock can hold is one entry
//! that was mid-write — and the alternative is losing all of them.
//!
//! It would be the wrong call for state where a partial update is meaningless.
//! If something like that ever lands here, it wants its own lock and its own
//! decision, not this one.

use std::sync::{Mutex, MutexGuard};

/// Locks `mutex`, taking it back if a previous holder panicked.
pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn a_panic_while_holding_the_lock_does_not_break_the_next_caller() {
        let shared = Arc::new(Mutex::new(vec![1, 2, 3]));

        let poisoner = Arc::clone(&shared);
        let panicked = std::thread::spawn(move || {
            let mut guard = lock(&poisoner);
            guard.push(4);
            panic!("something went wrong mid-update");
        })
        .join();
        assert!(panicked.is_err(), "the thread was supposed to panic");

        // The default `.lock().unwrap()` would panic here, and would keep
        // panicking for the rest of the process's life.
        let guard = lock(&shared);

        assert_eq!(*guard, vec![1, 2, 3, 4]);
    }
}
