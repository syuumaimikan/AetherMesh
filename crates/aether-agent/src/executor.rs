//! Runs built-in tasks. No task ever carries executable code.

use std::time::Instant;

use aether_core::task::kind;
use aether_core::{DataStore, NodeId, Task, TaskResult};

/// Upper bound on `cpu` task iterations, so one task cannot pin a node forever.
pub const MAX_CPU_ITERATIONS: u64 = 50_000_000;

/// Executes a task locally and reports how long it took.
///
/// `store` holds the datasets already transferred to this node; a task whose
/// declared inputs are missing fails instead of running on partial data.
/// Unsupported kinds and malformed payloads come back as a failed
/// [`TaskResult`], never as a panic.
pub fn execute(node_id: NodeId, task: &Task, store: &DataStore) -> TaskResult {
    let started = Instant::now();

    let outcome = match task.kind.as_str() {
        kind::ECHO => Ok(task.payload.clone()),
        kind::HASH => hash_task(task, store),
        kind::CPU => cpu_task(&task.payload),
        other => Err(format!("unknown task kind: {other}")),
    };

    let elapsed = started.elapsed();
    match outcome {
        Ok(output) => TaskResult::success(task.id, node_id, output, elapsed),
        Err(message) => TaskResult::failure(task.id, node_id, message, elapsed),
    }
}

/// Hashes the payload followed by every declared input, in order.
fn hash_task(task: &Task, store: &DataStore) -> Result<Vec<u8>, String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&task.payload);

    for data_id in &task.inputs {
        let bytes = store
            .get(*data_id)
            .ok_or_else(|| format!("input {data_id} is not present on this node"))?;
        hasher.update(&bytes);
    }

    Ok(hasher.finalize().as_bytes().to_vec())
}

/// Burns CPU deterministically: payload is an iteration count (u64, little-endian).
fn cpu_task(payload: &[u8]) -> Result<Vec<u8>, String> {
    let bytes: [u8; 8] = payload.try_into().map_err(|_| {
        format!(
            "cpu task expects an 8 byte iteration count, got {}",
            payload.len()
        )
    })?;

    let iterations = u64::from_le_bytes(bytes);
    if iterations > MAX_CPU_ITERATIONS {
        return Err(format!(
            "iteration count {iterations} exceeds the limit of {MAX_CPU_ITERATIONS}"
        ));
    }

    let mut accumulator: u64 = 0;
    for i in 0..iterations {
        accumulator = accumulator
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(i);
    }
    Ok(accumulator.to_le_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(kind: &str, payload: Vec<u8>) -> TaskResult {
        execute(
            NodeId::generate(),
            &Task::new(kind, payload),
            &DataStore::new(),
        )
    }

    #[test]
    fn echo_returns_the_payload() {
        let result = run(kind::ECHO, b"data".to_vec());
        assert_eq!(result.output(), Some(&b"data"[..]));
    }

    #[test]
    fn hash_matches_blake3() {
        let result = run(kind::HASH, b"aethermesh".to_vec());
        assert_eq!(
            result.output(),
            Some(blake3::hash(b"aethermesh").as_bytes().as_slice())
        );
    }

    #[test]
    fn hash_covers_the_declared_inputs() {
        let store = DataStore::new();
        let first = store.put(b"first".to_vec());
        let second = store.put(b"second".to_vec());
        let task = Task::new(kind::HASH, b"seed".to_vec()).with_inputs(vec![first.id, second.id]);

        let result = execute(NodeId::generate(), &task, &store);

        let mut expected = blake3::Hasher::new();
        expected.update(b"seed");
        expected.update(b"first");
        expected.update(b"second");
        assert_eq!(
            result.output(),
            Some(expected.finalize().as_bytes().as_slice())
        );
    }

    #[test]
    fn a_missing_input_fails_the_task() {
        let store = DataStore::new();
        let absent = aether_core::DataId::of(b"never transferred");
        let task = Task::new(kind::HASH, Vec::new()).with_inputs(vec![absent]);

        let result = execute(NodeId::generate(), &task, &store);
        assert!(!result.is_success());
    }

    #[test]
    fn cpu_task_is_deterministic() {
        let payload = 10_000u64.to_le_bytes().to_vec();
        let first = run(kind::CPU, payload.clone());
        let second = run(kind::CPU, payload);

        assert!(first.is_success());
        assert_eq!(first.output(), second.output());
    }

    #[test]
    fn cpu_task_rejects_a_malformed_payload() {
        assert!(!run(kind::CPU, vec![1, 2, 3]).is_success());
    }

    #[test]
    fn cpu_task_rejects_an_excessive_iteration_count() {
        let payload = (MAX_CPU_ITERATIONS + 1).to_le_bytes().to_vec();
        assert!(!run(kind::CPU, payload).is_success());
    }

    #[test]
    fn unknown_kinds_fail_without_panicking() {
        assert!(!run("rm -rf", Vec::new()).is_success());
    }
}
