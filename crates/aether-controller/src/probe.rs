//! Measuring what a link actually does.
//!
//! The scheduler weighs latency and bandwidth, so leaving them to be configured
//! by hand means scheduling on guesses. This measures them: one small ping for
//! the round trip, one padded ping for throughput, and the difference between
//! the two is the time the bytes took.

use std::time::{Duration, Instant};

use aether_core::NodeId;
use aether_protocol::Message;
use tracing::debug;

use crate::state::MeshState;

/// Ballast in the large ping. Big enough to time, small enough to be polite.
pub const DEFAULT_PROBE_BYTES: usize = 256 * 1024;

/// How often each node is measured.
pub const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// How long a probe waits before giving up.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Weight of a new sample against the running value. Low enough that one slow
/// moment does not rewrite the picture.
const SMOOTHING: f64 = 0.3;

/// What one probe learned about a link.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkMeasurement {
    pub latency_ms: f32,
    /// `None` when the two round trips were too close to tell anything.
    pub bandwidth_bytes_per_sec: Option<u64>,
}

/// Times one round trip, optionally with ballast attached.
async fn round_trip(state: &MeshState, node_id: NodeId, padding_bytes: usize) -> Option<Duration> {
    // Nonces only have to be unique per node while a probe is outstanding.
    let nonce = rand_nonce();
    let receiver = state.connections.expect_pong(node_id, nonce);

    let message = Message::Ping {
        nonce,
        padding: vec![0u8; padding_bytes],
    };
    if state.connections.send(node_id, message).is_err() {
        state.connections.forget_pong(node_id, nonce);
        return None;
    }

    let started = Instant::now();
    match tokio::time::timeout(PROBE_TIMEOUT, receiver).await {
        Ok(Ok(())) => Some(started.elapsed()),
        _ => {
            state.connections.forget_pong(node_id, nonce);
            None
        }
    }
}

/// Measures one node: latency from the empty ping, bandwidth from the padded
/// one minus the empty one.
pub async fn measure(
    state: &MeshState,
    node_id: NodeId,
    padding_bytes: usize,
) -> Option<LinkMeasurement> {
    let small = round_trip(state, node_id, 0).await?;
    let large = round_trip(state, node_id, padding_bytes).await?;

    let latency_ms = small.as_secs_f32() * 1000.0;
    let transfer = large.checked_sub(small).unwrap_or_default();

    // Below a millisecond the difference is scheduler noise, not throughput.
    let bandwidth_bytes_per_sec = (transfer > Duration::from_millis(1))
        .then(|| (padding_bytes as f64 / transfer.as_secs_f64()) as u64);

    Some(LinkMeasurement {
        latency_ms,
        bandwidth_bytes_per_sec,
    })
}

/// Measures every connected node and folds the result into the registry.
pub async fn probe_once(state: &MeshState, padding_bytes: usize) {
    let nodes: Vec<NodeId> = aether_core::lock(&state.registry)
        .nodes()
        .into_iter()
        .map(|info| info.id)
        .filter(|node_id| state.connections.is_connected(*node_id))
        .collect();

    for node_id in nodes {
        let Some(measurement) = measure(state, node_id, padding_bytes).await else {
            continue;
        };

        aether_core::lock(&state.registry).record_link(
            node_id,
            measurement.latency_ms,
            measurement.bandwidth_bytes_per_sec,
        );

        debug!(
            %node_id,
            latency_ms = measurement.latency_ms,
            bandwidth = measurement.bandwidth_bytes_per_sec,
            "link measured"
        );
    }
}

/// Keeps measuring until the task is dropped.
pub async fn monitor(state: MeshState, interval: Duration, padding_bytes: usize) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(100)));
    loop {
        ticker.tick().await;
        probe_once(&state, padding_bytes).await;
    }
}

/// Blends a new sample into a running value.
pub(crate) fn smooth(previous: Option<f64>, sample: f64) -> f64 {
    match previous {
        Some(previous) => previous * (1.0 - SMOOTHING) + sample * SMOOTHING,
        None => sample,
    }
}

/// A nonce that will not collide with another outstanding probe.
fn rand_nonce() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aether_core::NodeInfo;
    use tokio::sync::mpsc;

    use super::*;

    /// Answers pings after `delay_per_byte` worth of simulated transfer time.
    fn spawn_responder(
        state: MeshState,
        node_id: NodeId,
        mut outbound: mpsc::UnboundedReceiver<Message>,
        per_byte: Duration,
    ) {
        tokio::spawn(async move {
            while let Some(message) = outbound.recv().await {
                if let Message::Ping { nonce, padding } = message {
                    tokio::time::sleep(per_byte * padding.len() as u32).await;
                    state.connections.complete_pong(node_id, nonce);
                }
            }
        });
    }

    fn attach(state: &MeshState, per_byte: Duration) -> NodeId {
        let info = NodeInfo::new(NodeId::generate(), "probe-me", "127.0.0.1:1", 4);
        let node_id = info.id;
        state.registry.lock().unwrap().register(info);

        let (sender, receiver) = mpsc::unbounded_channel();
        state.connections.attach(node_id, sender);
        spawn_responder(state.clone(), node_id, receiver, per_byte);
        node_id
    }

    #[tokio::test]
    async fn a_probe_measures_latency() {
        let state = MeshState::new();
        let node_id = attach(&state, Duration::ZERO);

        let measurement = measure(&state, node_id, 1024).await.unwrap();

        // An instant responder still costs a task wake-up, so this is only
        // asserting that a number came back at all.
        assert!(measurement.latency_ms >= 0.0);
        assert!(measurement.latency_ms < 1000.0, "{measurement:?}");
    }

    #[tokio::test]
    async fn padding_that_takes_time_becomes_a_bandwidth_estimate() {
        let state = MeshState::new();
        // 200 us per byte over 512 bytes is ~100 ms of "transfer", comfortably
        // above anything the timer or the scheduler contributes.
        let node_id = attach(&state, Duration::from_micros(200));

        // Warm up: the first timer of a test run is the least trustworthy one.
        measure(&state, node_id, 0).await;
        let measurement = measure(&state, node_id, 512).await.unwrap();

        let bandwidth = measurement.bandwidth_bytes_per_sec.expect("measurable");
        assert!(bandwidth > 500 && bandwidth < 500_000, "{bandwidth}");
    }

    #[tokio::test]
    async fn probing_folds_the_result_into_the_registry() {
        let state = MeshState::new();
        let node_id = attach(&state, Duration::from_micros(200));

        probe_once(&state, 512).await;

        let registry = state.registry.lock().unwrap();
        let info = &registry.get(node_id).unwrap().info;
        // Latency always lands; bandwidth only when the padded trip was slow
        // enough to time, which is the point of the one-millisecond floor.
        assert!(info.latency_ms.is_some());
    }

    #[tokio::test]
    async fn a_node_that_never_answers_is_left_alone() {
        let state = MeshState::new();
        let info = NodeInfo::new(NodeId::generate(), "silent", "127.0.0.1:1", 4);
        let node_id = info.id;
        state.registry.lock().unwrap().register(info);

        // No connection at all: the probe gives up instead of hanging.
        assert_eq!(measure(&state, node_id, 128).await, None);
    }

    #[test]
    fn smoothing_favours_the_running_value() {
        assert_eq!(smooth(None, 10.0), 10.0);
        let blended = smooth(Some(10.0), 20.0);
        assert!(blended > 10.0 && blended < 15.0, "{blended}");
    }
}
