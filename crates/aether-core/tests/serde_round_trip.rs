//! Core types travel over the wire in Phase 3, so they must survive a round trip.

use std::time::Duration;

use aether_core::{NodeId, NodeInfo, NodeMetrics, Task, TaskResult};

#[test]
fn node_info_round_trips() {
    let mut node = NodeInfo::new(NodeId::generate(), "desktop", "127.0.0.1:7000", 16);
    node.update_metrics(NodeMetrics::new(0.3, 0.6, 32 * 1024 * 1024 * 1024));

    let encoded = serde_json::to_string(&node).unwrap();
    assert_eq!(serde_json::from_str::<NodeInfo>(&encoded).unwrap(), node);
}

#[test]
fn task_and_result_round_trip() {
    let task = Task::new("hash", b"aethermesh".to_vec());
    let encoded = serde_json::to_string(&task).unwrap();
    assert_eq!(serde_json::from_str::<Task>(&encoded).unwrap(), task);

    let result = TaskResult::success(
        task.id,
        NodeId::generate(),
        vec![0xde, 0xad],
        Duration::from_millis(12),
    );
    let encoded = serde_json::to_string(&result).unwrap();
    assert_eq!(
        serde_json::from_str::<TaskResult>(&encoded).unwrap(),
        result
    );
}

#[test]
fn ids_serialize_as_plain_strings() {
    let id = NodeId::generate();
    assert_eq!(serde_json::to_string(&id).unwrap(), format!("\"{id}\""));
}
