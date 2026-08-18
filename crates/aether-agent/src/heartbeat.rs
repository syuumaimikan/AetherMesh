//! How often an idle node has to speak up.
//!
//! A heartbeat is cheap on its own, but it is not free: it samples the CPU and
//! memory of the whole machine, wakes a core that was about to idle, and does
//! it again every few seconds forever. On a laptop or a Raspberry Pi sitting in
//! a mesh nobody is using, that is the entire power cost of participating.
//!
//! So the interval is not fixed. A node that is running work, or whose load has
//! visibly moved, reports at the configured rate. A node where nothing is
//! happening doubles its gap each time, up to a ceiling the *controller*
//! chooses — half its eviction timeout, so a single lost heartbeat still cannot
//! evict a node that is perfectly healthy.

use std::time::Duration;

use aether_core::NodeMetrics;

/// How much CPU or memory has to move before the node counts as busy again.
///
/// Small enough to catch a real workload starting, large enough that sampling
/// noise on an idle machine does not keep resetting the backoff.
const SIGNIFICANT_CHANGE: f32 = 0.05;

/// Decides the gap before the next heartbeat.
#[derive(Debug, Clone)]
pub struct HeartbeatPacer {
    base: Duration,
    ceiling: Duration,
    current: Duration,
    last: Option<NodeMetrics>,
}

impl HeartbeatPacer {
    /// A pacer that reports every `base` and stretches to at most `ceiling`.
    ///
    /// A ceiling below the base disables backoff, which is what a controller
    /// with no declared timeout gets.
    pub fn new(base: Duration, ceiling: Duration) -> Self {
        Self {
            base,
            ceiling: ceiling.max(base),
            current: base,
            last: None,
        }
    }

    /// The ceiling implied by a controller's eviction timeout.
    ///
    /// Half the timeout: two heartbeats fit in every eviction window, so losing
    /// one is survivable. A controller that reports no timeout gets `base`,
    /// which means no backoff at all — the safe reading of "unknown".
    pub fn ceiling_for_timeout(base: Duration, timeout: Duration) -> Duration {
        if timeout.is_zero() {
            base
        } else {
            (timeout / 2).max(base)
        }
    }

    /// The gap currently being used.
    pub fn interval(&self) -> Duration {
        self.current
    }

    /// Records a heartbeat and returns the gap to wait before the next one.
    ///
    /// `did_work` is whether the node handled anything since the last call.
    pub fn record(&mut self, metrics: NodeMetrics, did_work: bool) -> Duration {
        let moved = self.last.is_some_and(|last| changed(last, metrics));
        self.last = Some(metrics);

        self.current = if did_work || moved {
            // Something is happening. The controller's picture of this node is
            // worth keeping fresh, whatever it costs.
            self.base
        } else {
            (self.current * 2).min(self.ceiling)
        };
        self.current
    }
}

/// Whether two samples differ enough to be worth reporting promptly.
fn changed(before: NodeMetrics, after: NodeMetrics) -> bool {
    (before.cpu_usage - after.cpu_usage).abs() >= SIGNIFICANT_CHANGE
        || (before.memory_usage - after.memory_usage).abs() >= SIGNIFICANT_CHANGE
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: Duration = Duration::from_secs(5);
    const CEILING: Duration = Duration::from_secs(30);

    fn idle() -> NodeMetrics {
        NodeMetrics::new(0.02, 0.30, 8_000_000_000)
    }

    fn busy() -> NodeMetrics {
        NodeMetrics::new(0.90, 0.55, 8_000_000_000)
    }

    #[test]
    fn an_idle_node_doubles_its_gap_up_to_the_ceiling() {
        let mut pacer = HeartbeatPacer::new(BASE, CEILING);

        // The first sample has nothing to compare against, so it still backs off.
        assert_eq!(pacer.record(idle(), false), Duration::from_secs(10));
        assert_eq!(pacer.record(idle(), false), Duration::from_secs(20));
        assert_eq!(pacer.record(idle(), false), CEILING);
        assert_eq!(pacer.record(idle(), false), CEILING, "and stays there");
    }

    #[test]
    fn work_snaps_the_gap_back_to_the_base() {
        let mut pacer = HeartbeatPacer::new(BASE, CEILING);
        pacer.record(idle(), false);
        pacer.record(idle(), false);

        assert_eq!(pacer.record(idle(), true), BASE);
    }

    #[test]
    fn a_load_change_snaps_the_gap_back_even_without_a_task() {
        let mut pacer = HeartbeatPacer::new(BASE, CEILING);
        pacer.record(idle(), false);
        pacer.record(idle(), false);

        // Something the agent did not run is using the machine. The scheduler
        // needs to know that before it sends work here.
        assert_eq!(pacer.record(busy(), false), BASE);
    }

    #[test]
    fn sampling_noise_does_not_reset_the_backoff() {
        let mut pacer = HeartbeatPacer::new(BASE, CEILING);
        pacer.record(NodeMetrics::new(0.02, 0.30, 1024), false);

        let jittered = NodeMetrics::new(0.04, 0.31, 1024);
        assert_eq!(pacer.record(jittered, false), Duration::from_secs(20));
    }

    #[test]
    fn a_ceiling_below_the_base_disables_backoff() {
        let mut pacer = HeartbeatPacer::new(BASE, Duration::from_secs(1));
        assert_eq!(pacer.record(idle(), false), BASE);
        assert_eq!(pacer.record(idle(), false), BASE);
    }

    #[test]
    fn the_ceiling_leaves_room_for_one_lost_heartbeat() {
        let ceiling = HeartbeatPacer::ceiling_for_timeout(BASE, Duration::from_secs(60));
        assert_eq!(ceiling, Duration::from_secs(30));

        // Two heartbeats per eviction window, even at full backoff.
        assert!(ceiling * 2 <= Duration::from_secs(60));
    }

    #[test]
    fn an_unknown_timeout_means_no_backoff() {
        assert_eq!(
            HeartbeatPacer::ceiling_for_timeout(BASE, Duration::ZERO),
            BASE
        );
        // A timeout tighter than the base cannot make the gap shorter either.
        assert_eq!(
            HeartbeatPacer::ceiling_for_timeout(BASE, Duration::from_secs(4)),
            BASE
        );
    }
}
