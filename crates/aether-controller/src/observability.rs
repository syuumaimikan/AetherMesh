//! Counters describing what the mesh has been doing.
//!
//! Structured logs (via `tracing`) say what happened; these say how much. They
//! are plain atomics, so recording one costs nothing worth measuring.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Shared counters. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct MeshMetrics {
    inner: Arc<Counters>,
}

#[derive(Debug, Default)]
struct Counters {
    nodes_registered: AtomicU64,
    registrations_rejected: AtomicU64,
    nodes_disconnected: AtomicU64,
    nodes_evicted: AtomicU64,
    heartbeats: AtomicU64,
    tasks_completed: AtomicU64,
    tasks_failed: AtomicU64,
    messages_received: AtomicU64,
}

/// A point-in-time copy of the counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub nodes_registered: u64,
    pub registrations_rejected: u64,
    pub nodes_disconnected: u64,
    pub nodes_evicted: u64,
    pub heartbeats: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub messages_received: u64,
}

macro_rules! counter {
    ($record:ident, $field:ident) => {
        pub fn $record(&self) {
            self.inner.$field.fetch_add(1, Ordering::Relaxed);
        }
    };
}

impl MeshMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    counter!(record_registration, nodes_registered);
    counter!(record_rejection, registrations_rejected);
    counter!(record_disconnect, nodes_disconnected);
    counter!(record_heartbeat, heartbeats);
    counter!(record_message, messages_received);

    /// Records `count` nodes dropped by the health check.
    pub fn record_evictions(&self, count: u64) {
        self.inner.nodes_evicted.fetch_add(count, Ordering::Relaxed);
    }

    /// Records a finished task, successful or not.
    pub fn record_task(&self, success: bool) {
        let counter = if success {
            &self.inner.tasks_completed
        } else {
            &self.inner.tasks_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        MetricsSnapshot {
            nodes_registered: load(&self.inner.nodes_registered),
            registrations_rejected: load(&self.inner.registrations_rejected),
            nodes_disconnected: load(&self.inner.nodes_disconnected),
            nodes_evicted: load(&self.inner.nodes_evicted),
            heartbeats: load(&self.inner.heartbeats),
            tasks_completed: load(&self.inner.tasks_completed),
            tasks_failed: load(&self.inner.tasks_failed),
            messages_received: load(&self.inner.messages_received),
        }
    }
}

impl MetricsSnapshot {
    /// Prometheus text exposition, ready to serve or log.
    pub fn to_prometheus(&self) -> String {
        let lines = [
            ("aethermesh_nodes_registered_total", self.nodes_registered),
            (
                "aethermesh_registrations_rejected_total",
                self.registrations_rejected,
            ),
            (
                "aethermesh_nodes_disconnected_total",
                self.nodes_disconnected,
            ),
            ("aethermesh_nodes_evicted_total", self.nodes_evicted),
            ("aethermesh_heartbeats_total", self.heartbeats),
            ("aethermesh_tasks_completed_total", self.tasks_completed),
            ("aethermesh_tasks_failed_total", self.tasks_failed),
            ("aethermesh_messages_received_total", self.messages_received),
        ];

        lines
            .iter()
            .map(|(name, value)| format!("# TYPE {name} counter\n{name} {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        assert_eq!(MeshMetrics::new().snapshot(), MetricsSnapshot::default());
    }

    #[test]
    fn every_counter_records() {
        let metrics = MeshMetrics::new();
        metrics.record_registration();
        metrics.record_rejection();
        metrics.record_disconnect();
        metrics.record_evictions(3);
        metrics.record_heartbeat();
        metrics.record_message();
        metrics.record_task(true);
        metrics.record_task(false);
        metrics.record_task(false);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.nodes_registered, 1);
        assert_eq!(snapshot.registrations_rejected, 1);
        assert_eq!(snapshot.nodes_disconnected, 1);
        assert_eq!(snapshot.nodes_evicted, 3);
        assert_eq!(snapshot.heartbeats, 1);
        assert_eq!(snapshot.messages_received, 1);
        assert_eq!(snapshot.tasks_completed, 1);
        assert_eq!(snapshot.tasks_failed, 2);
    }

    #[test]
    fn clones_share_one_set_of_counters() {
        let metrics = MeshMetrics::new();
        let clone = metrics.clone();
        clone.record_heartbeat();

        assert_eq!(metrics.snapshot().heartbeats, 1);
    }

    #[test]
    fn the_prometheus_form_names_every_counter() {
        let metrics = MeshMetrics::new();
        metrics.record_task(true);
        let text = metrics.snapshot().to_prometheus();

        assert!(text.contains("aethermesh_tasks_completed_total 1"));
        assert!(text.contains("# TYPE aethermesh_heartbeats_total counter"));
    }
}
