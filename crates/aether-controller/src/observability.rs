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

/// Bytes moved, and bytes that did not have to move.
///
/// These used to be private fields on the `Controller`, which one task owns
/// exclusively — so the numbers that describe the whole point of the project
/// were unreadable from anywhere else in the process. They are shared counters
/// now, which is what lets the client API and the scrape endpoint report them.
#[derive(Debug, Clone, Default)]
pub struct TrafficStats {
    inner: Arc<TrafficCounters>,
}

#[derive(Debug, Default)]
struct TrafficCounters {
    data_bytes_sent: AtomicU64,
    data_bytes_uncompressed: AtomicU64,
    transfers_skipped: AtomicU64,
    chunks_skipped: AtomicU64,
    retries: AtomicU64,
}

/// A point-in-time copy of the traffic counters, with the derived figures.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct TrafficSnapshot {
    /// Bytes actually written to sockets, after compression.
    pub data_bytes_sent: u64,
    /// Bytes those transfers represent, before compression.
    pub data_bytes_uncompressed: u64,
    /// Whole datasets not sent because the node already held them.
    pub transfers_skipped: u64,
    /// Individual chunks not sent for the same reason.
    pub chunks_skipped: u64,
    /// Tasks moved to another node after one refused or timed out.
    pub retries: u64,
}

impl TrafficStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one transfer: `wire_bytes` went out, representing `original_bytes`.
    pub fn record_sent(&self, wire_bytes: u64, original_bytes: u64) {
        self.inner
            .data_bytes_sent
            .fetch_add(wire_bytes, Ordering::Relaxed);
        self.inner
            .data_bytes_uncompressed
            .fetch_add(original_bytes, Ordering::Relaxed);
    }

    /// Records a dataset the node already had.
    pub fn record_transfer_skipped(&self) {
        self.inner.transfers_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a chunk the node already had.
    pub fn record_chunk_skipped(&self) {
        self.inner.chunks_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a task sent to a second node after the first would not take it.
    pub fn record_retry(&self) {
        self.inner.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TrafficSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        TrafficSnapshot {
            data_bytes_sent: load(&self.inner.data_bytes_sent),
            data_bytes_uncompressed: load(&self.inner.data_bytes_uncompressed),
            transfers_skipped: load(&self.inner.transfers_skipped),
            chunks_skipped: load(&self.inner.chunks_skipped),
            retries: load(&self.inner.retries),
        }
    }
}

impl TrafficSnapshot {
    /// Bytes compression kept off the wire.
    ///
    /// Saturating: a compressed form can be larger than its input, and a
    /// negative saving reported as a huge unsigned number would be worse than
    /// reporting none.
    pub fn compression_saved_bytes(&self) -> u64 {
        self.data_bytes_uncompressed
            .saturating_sub(self.data_bytes_sent)
    }

    /// Wire bytes over original bytes. `1.0` means compression gained nothing;
    /// `0.0` means nothing has been sent yet.
    pub fn compression_ratio(&self) -> f64 {
        if self.data_bytes_uncompressed == 0 {
            0.0
        } else {
            self.data_bytes_sent as f64 / self.data_bytes_uncompressed as f64
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

    #[test]
    fn traffic_counters_are_shared_between_clones() {
        let traffic = TrafficStats::new();
        let clone = traffic.clone();

        clone.record_sent(300, 1000);
        clone.record_transfer_skipped();
        clone.record_chunk_skipped();
        clone.record_retry();

        // The Controller holds one clone and the client API another; if these
        // were separate the API would report zeroes forever.
        let snapshot = traffic.snapshot();
        assert_eq!(snapshot.data_bytes_sent, 300);
        assert_eq!(snapshot.data_bytes_uncompressed, 1000);
        assert_eq!(snapshot.transfers_skipped, 1);
        assert_eq!(snapshot.chunks_skipped, 1);
        assert_eq!(snapshot.retries, 1);
    }

    #[test]
    fn the_derived_figures_describe_what_compression_did() {
        let traffic = TrafficStats::new();
        traffic.record_sent(250, 1000);

        let snapshot = traffic.snapshot();
        assert_eq!(snapshot.compression_saved_bytes(), 750);
        assert_eq!(snapshot.compression_ratio(), 0.25);
    }

    #[test]
    fn an_idle_mesh_reports_no_saving_rather_than_a_division_by_zero() {
        let snapshot = TrafficStats::new().snapshot();
        assert_eq!(snapshot.compression_ratio(), 0.0);
        assert_eq!(snapshot.compression_saved_bytes(), 0);
    }

    #[test]
    fn a_transfer_that_grew_reports_no_saving_rather_than_a_huge_one() {
        let traffic = TrafficStats::new();
        // Random data compresses to slightly more than it started as.
        traffic.record_sent(1100, 1000);

        assert_eq!(traffic.snapshot().compression_saved_bytes(), 0);
    }
}
