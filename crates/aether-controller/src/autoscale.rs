//! Deciding whether the mesh needs more nodes, or fewer.
//!
//! This module **recommends and does not act**. It reads what the controller
//! already knows — how much work is waiting, how long it has waited, how busy
//! the nodes are — and returns a number. Turning that number into machines is
//! [`crate::cloud`]'s job, or an operator's, and keeping the two apart means a
//! bad reading costs a log line rather than a bill.
//!
//! # Why it is mostly refusals
//!
//! An autoscaler that reacts to every reading oscillates: it adds a node, the
//! queue drains, it removes the node, the queue fills, and the cost of the
//! churn exceeds anything the scaling saved. Three rules stop that, and they
//! matter more than the arithmetic they guard:
//!
//! 1. **Cooldown.** Nothing changes for a while after something changed. A new
//!    node takes time to boot, register, and start draining a queue; deciding
//!    again before then is deciding on stale evidence.
//! 2. **A dead band.** Being near the target is not being off it.
//! 3. **A backlog vetoes scaling down**, whatever the CPU says. Idle nodes
//!    with a full queue means work is not reaching them, and removing one is
//!    the opposite of the fix.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// What the mesh looks like right now.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Signals {
    /// Nodes registered and reachable.
    pub nodes: usize,
    /// Tasks waiting for one.
    pub queue_depth: u64,
    /// How long the task that has waited longest has waited.
    pub oldest_wait: Duration,
    /// Mean CPU usage across the nodes, 0..=1.
    pub cpu_usage: f32,
    /// Mean memory usage across the nodes, 0..=1.
    pub memory_usage: f32,
}

/// What the mesh is being scaled towards.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Target {
    /// Keep roughly this many tasks queued per node.
    ///
    /// The most direct signal there is: a queue is work that has arrived and
    /// has nowhere to go.
    QueueLength { per_node: f64 },
    /// Keep the nodes about this busy, 0..=1.
    ///
    /// Right when tasks are long and the queue rarely builds, so utilisation
    /// moves before depth does.
    CpuUtilization { fraction: f32 },
    /// Keep the longest wait under this.
    ///
    /// Right when what you promised somebody was a deadline rather than a
    /// throughput.
    Latency { max_wait_secs: f64 },
}

impl Default for Target {
    fn default() -> Self {
        Self::QueueLength { per_node: 2.0 }
    }
}

/// How eagerly to scale, and how far.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub target: Target,
    /// Never recommend fewer than this. One, normally: a mesh of zero nodes
    /// cannot report the queue that would scale it back up.
    pub min_nodes: usize,
    /// Never recommend more than this. The line between elastic and expensive.
    pub max_nodes: usize,
    /// How long to leave a change alone before deciding again.
    pub cooldown_secs: u64,
    /// How far off target counts as off target, as a fraction.
    ///
    /// Without this the mesh oscillates around the target forever, paying for
    /// a node's startup on every swing.
    pub tolerance: f64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            target: Target::default(),
            min_nodes: 1,
            max_nodes: 16,
            // Long enough for a machine to boot and register. Anything shorter
            // decides again before the last decision has had an effect.
            cooldown_secs: 120,
            tolerance: 0.25,
        }
    }
}

impl Policy {
    pub fn cooldown(&self) -> Duration {
        Duration::from_secs(self.cooldown_secs)
    }
}

/// What the mesh should do about it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Decision {
    /// Add nodes until there are this many.
    ScaleUp { to: usize, why: String },
    /// Remove nodes until there are this many.
    ScaleDown { to: usize, why: String },
    /// Leave it alone, and why.
    NoChange { why: String },
}

impl Decision {
    /// The node count this recommends, or `None` for no change.
    pub fn target_nodes(&self) -> Option<usize> {
        match self {
            Self::ScaleUp { to, .. } | Self::ScaleDown { to, .. } => Some(*to),
            Self::NoChange { .. } => None,
        }
    }

    pub fn why(&self) -> &str {
        match self {
            Self::ScaleUp { why, .. } | Self::ScaleDown { why, .. } | Self::NoChange { why } => why,
        }
    }

    fn no_change(why: impl Into<String>) -> Self {
        Self::NoChange { why: why.into() }
    }
}

/// Turns readings into recommendations.
#[derive(Debug, Clone)]
pub struct Autoscaler {
    policy: Policy,
    /// When the last recommendation to change was made, for the cooldown.
    last_change: Option<Instant>,
}

impl Autoscaler {
    pub fn new(policy: Policy) -> Self {
        Self {
            policy,
            last_change: None,
        }
    }

    pub fn policy(&self) -> Policy {
        self.policy
    }

    /// Whether a change was recommended recently enough to still be settling.
    pub fn cooling_down(&self, now: Instant) -> bool {
        self.last_change
            .is_some_and(|last| now.saturating_duration_since(last) < self.policy.cooldown())
    }

    /// Reads the mesh and says what it should be.
    ///
    /// Takes `&mut self` because a recommendation starts the cooldown: asking
    /// twice in a row and getting the same answer twice would be the
    /// oscillation this is meant to prevent.
    pub fn decide(&mut self, signals: &Signals, now: Instant) -> Decision {
        let decision = self.evaluate(signals, now);
        if decision.target_nodes().is_some() {
            self.last_change = Some(now);
        }
        decision
    }

    /// Same, without starting the cooldown. For showing somebody the reasoning.
    pub fn preview(&self, signals: &Signals, now: Instant) -> Decision {
        self.evaluate(signals, now)
    }

    fn evaluate(&self, signals: &Signals, now: Instant) -> Decision {
        if self.cooling_down(now) {
            return Decision::no_change("waiting for the last change to take effect");
        }

        // A mesh with nothing in it cannot report the queue that would scale it
        // up, so it gets its minimum however quiet it looks.
        if signals.nodes < self.policy.min_nodes {
            return Decision::ScaleUp {
                to: self.policy.min_nodes,
                why: format!(
                    "{} node(s), below the minimum of {}",
                    signals.nodes, self.policy.min_nodes
                ),
            };
        }

        let (ratio, description) = match self.policy.target {
            Target::QueueLength { per_node } => {
                let want = per_node.max(0.01) * signals.nodes.max(1) as f64;
                (
                    signals.queue_depth as f64 / want,
                    format!(
                        "{} queued against a target of {want:.0}",
                        signals.queue_depth
                    ),
                )
            }
            Target::CpuUtilization { fraction } => {
                let want = f64::from(fraction.max(0.01));
                (
                    f64::from(signals.cpu_usage) / want,
                    format!(
                        "{:.0}% cpu against a target of {:.0}%",
                        signals.cpu_usage * 100.0,
                        fraction * 100.0
                    ),
                )
            }
            Target::Latency { max_wait_secs } => {
                let want = max_wait_secs.max(0.001);
                (
                    signals.oldest_wait.as_secs_f64() / want,
                    format!(
                        "{:.1}s longest wait against a target of {want:.1}s",
                        signals.oldest_wait.as_secs_f64()
                    ),
                )
            }
        };

        if ratio > 1.0 + self.policy.tolerance {
            let wanted = ((signals.nodes.max(1) as f64) * ratio).ceil() as usize;
            let to = wanted.clamp(self.policy.min_nodes, self.policy.max_nodes);
            return if to > signals.nodes {
                Decision::ScaleUp {
                    to,
                    why: format!("{description}; adding capacity"),
                }
            } else {
                Decision::no_change(format!(
                    "{description}, but already at {} nodes",
                    self.policy.max_nodes
                ))
            };
        }

        if ratio < 1.0 - self.policy.tolerance {
            // Idle nodes and a full queue means work is not reaching them.
            // Removing one is the opposite of the fix, whatever the CPU says.
            if signals.queue_depth > 0 {
                return Decision::no_change(format!(
                    "{description}, but {} task(s) are still waiting",
                    signals.queue_depth
                ));
            }

            let wanted = ((signals.nodes as f64) * ratio.max(0.0)).floor().max(1.0) as usize;
            let to = wanted.clamp(self.policy.min_nodes, self.policy.max_nodes);
            return if to < signals.nodes {
                Decision::ScaleDown {
                    to,
                    why: format!("{description}; releasing capacity"),
                }
            } else {
                Decision::no_change(format!("{description}, but already at the minimum"))
            };
        }

        Decision::no_change(format!("{description}, which is close enough"))
    }
}

/// Reads the live mesh into the signals an autoscaler wants.
pub fn signals_from(state: &crate::state::MeshState) -> Signals {
    let nodes = state.nodes();
    let queue = state.queue.snapshot();

    let (cpu_usage, memory_usage) = if nodes.is_empty() {
        (0.0, 0.0)
    } else {
        let count = nodes.len() as f32;
        (
            nodes.iter().map(|node| node.metrics.cpu_usage).sum::<f32>() / count,
            nodes
                .iter()
                .map(|node| node.metrics.memory_usage)
                .sum::<f32>()
                / count,
        )
    };

    Signals {
        // Registered but unreachable is not capacity.
        nodes: nodes
            .iter()
            .filter(|node| state.connections.is_connected(node.id))
            .count(),
        queue_depth: queue.depth,
        oldest_wait: Duration::from_millis(queue.longest_wait_ms),
        cpu_usage,
        memory_usage,
    }
}

/// Logs what the mesh should be, on a timer.
///
/// Recommends; does not act. Provisioning is [`crate::cloud`]'s job or an
/// operator's, and keeping them apart means a bad reading costs a log line
/// rather than a bill.
pub async fn monitor(state: crate::state::MeshState, policy: Policy, interval: Duration) {
    let mut scaler = Autoscaler::new(policy);
    let mut ticker = tokio::time::interval(interval);

    loop {
        ticker.tick().await;
        let signals = signals_from(&state);
        let decision = scaler.decide(&signals, Instant::now());

        match &decision {
            Decision::NoChange { why } => {
                tracing::debug!(nodes = signals.nodes, %why, "autoscaler: no change")
            }
            Decision::ScaleUp { to, why } => tracing::info!(
                from = signals.nodes,
                to,
                %why,
                "autoscaler recommends more nodes (recommendation only)"
            ),
            Decision::ScaleDown { to, why } => tracing::info!(
                from = signals.nodes,
                to,
                %why,
                "autoscaler recommends fewer nodes (recommendation only)"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> Policy {
        Policy {
            target: Target::QueueLength { per_node: 2.0 },
            min_nodes: 1,
            max_nodes: 10,
            cooldown_secs: 60,
            tolerance: 0.25,
        }
    }

    fn signals(nodes: usize, queue_depth: u64) -> Signals {
        Signals {
            nodes,
            queue_depth,
            ..Signals::default()
        }
    }

    #[test]
    fn a_mesh_at_its_target_is_left_alone() {
        let mut scaler = Autoscaler::new(policy());
        // Four nodes want eight queued; eight is exactly right.
        let decision = scaler.decide(&signals(4, 8), Instant::now());

        assert_eq!(decision.target_nodes(), None);
        assert!(decision.why().contains("close enough"), "{decision:?}");
    }

    #[test]
    fn a_backlog_asks_for_more_nodes() {
        let mut scaler = Autoscaler::new(policy());
        // Four nodes, forty queued: five times the target.
        let decision = scaler.decide(&signals(4, 40), Instant::now());

        assert_eq!(decision.target_nodes(), Some(10), "{decision:?}");
        assert!(matches!(decision, Decision::ScaleUp { .. }));
    }

    #[test]
    fn an_empty_queue_releases_nodes() {
        let mut scaler = Autoscaler::new(policy());
        let decision = scaler.decide(&signals(8, 0), Instant::now());

        assert!(
            matches!(decision, Decision::ScaleDown { .. }),
            "{decision:?}"
        );
        assert_eq!(decision.target_nodes(), Some(1));
    }

    #[test]
    fn work_still_waiting_vetoes_scaling_down() {
        let mut scaler = Autoscaler::new(Policy {
            target: Target::CpuUtilization { fraction: 0.7 },
            ..policy()
        });

        // Idle nodes and a queue that is not draining: the work is not
        // reaching them, and taking a node away makes that worse.
        let decision = scaler.decide(
            &Signals {
                nodes: 4,
                queue_depth: 12,
                cpu_usage: 0.05,
                ..Signals::default()
            },
            Instant::now(),
        );

        assert_eq!(decision.target_nodes(), None, "{decision:?}");
        assert!(decision.why().contains("still waiting"), "{decision:?}");
    }

    #[test]
    fn nothing_changes_twice_in_a_row() {
        let mut scaler = Autoscaler::new(policy());
        let start = Instant::now();

        assert!(
            scaler
                .decide(&signals(4, 40), start)
                .target_nodes()
                .is_some()
        );

        // A new node has to boot, register, and start draining before the next
        // reading means anything.
        let during = scaler.decide(&signals(4, 40), start + Duration::from_secs(30));
        assert_eq!(during.target_nodes(), None);
        assert!(during.why().contains("take effect"), "{during:?}");

        let after = scaler.decide(&signals(4, 40), start + Duration::from_secs(61));
        assert!(after.target_nodes().is_some(), "{after:?}");
    }

    #[test]
    fn a_reading_just_off_target_is_inside_the_dead_band() {
        let mut scaler = Autoscaler::new(policy());
        let now = Instant::now();

        // Target is 8 for four nodes; 9 and 7 are both within 25 %.
        assert_eq!(scaler.decide(&signals(4, 9), now).target_nodes(), None);
        assert_eq!(scaler.decide(&signals(4, 7), now).target_nodes(), None);
    }

    #[test]
    fn a_mesh_does_not_oscillate_around_its_target() {
        let mut scaler = Autoscaler::new(Policy {
            cooldown_secs: 0,
            ..policy()
        });
        let start = Instant::now();
        let mut nodes = 4usize;
        let mut changes = 0;

        // The queue drifts around the target. Without a dead band this adds
        // and removes a node on almost every reading.
        for tick in 0..40u64 {
            let queue = if tick % 2 == 0 { 9 } else { 7 };
            let decision = scaler.decide(
                &signals(nodes, queue),
                start + Duration::from_secs(tick * 10),
            );
            if let Some(to) = decision.target_nodes() {
                nodes = to;
                changes += 1;
            }
        }

        assert_eq!(changes, 0, "the mesh flapped {changes} times");
        assert_eq!(nodes, 4);
    }

    #[test]
    fn a_recommendation_never_exceeds_the_maximum() {
        let mut scaler = Autoscaler::new(policy());
        // Four nodes, a thousand queued: unbounded arithmetic says 500.
        let decision = scaler.decide(&signals(4, 1000), Instant::now());

        assert_eq!(decision.target_nodes(), Some(10));
    }

    #[test]
    fn a_mesh_already_at_its_maximum_is_told_why_it_is_not_growing() {
        let mut scaler = Autoscaler::new(policy());
        let decision = scaler.decide(&signals(10, 1000), Instant::now());

        assert_eq!(decision.target_nodes(), None);
        assert!(decision.why().contains("already at 10"), "{decision:?}");
    }

    #[test]
    fn a_recommendation_never_goes_below_the_minimum() {
        let mut scaler = Autoscaler::new(Policy {
            min_nodes: 3,
            ..policy()
        });
        let decision = scaler.decide(&signals(4, 0), Instant::now());

        assert_eq!(decision.target_nodes(), Some(3), "{decision:?}");
    }

    #[test]
    fn an_empty_mesh_is_brought_up_to_its_minimum_however_quiet_it_looks() {
        let mut scaler = Autoscaler::new(Policy {
            min_nodes: 2,
            ..policy()
        });

        // Nothing queued, because there is nothing to queue onto. A mesh of
        // zero nodes cannot report the backlog that would scale it up.
        let decision = scaler.decide(&signals(0, 0), Instant::now());
        assert_eq!(decision.target_nodes(), Some(2), "{decision:?}");
    }

    #[test]
    fn utilisation_scales_when_the_queue_never_builds() {
        let mut scaler = Autoscaler::new(Policy {
            target: Target::CpuUtilization { fraction: 0.5 },
            ..policy()
        });

        // Long tasks: the nodes are pinned but nothing is waiting, so queue
        // depth would say everything is fine.
        let decision = scaler.decide(
            &Signals {
                nodes: 4,
                queue_depth: 0,
                cpu_usage: 0.95,
                ..Signals::default()
            },
            Instant::now(),
        );

        assert!(matches!(decision, Decision::ScaleUp { .. }), "{decision:?}");
        assert_eq!(decision.target_nodes(), Some(8));
    }

    #[test]
    fn latency_scales_on_the_promise_that_was_made() {
        let mut scaler = Autoscaler::new(Policy {
            target: Target::Latency { max_wait_secs: 1.0 },
            ..policy()
        });

        let decision = scaler.decide(
            &Signals {
                nodes: 2,
                queue_depth: 3,
                oldest_wait: Duration::from_secs(4),
                ..Signals::default()
            },
            Instant::now(),
        );

        assert_eq!(decision.target_nodes(), Some(8), "{decision:?}");
    }

    #[test]
    fn preview_does_not_start_the_cooldown() {
        let mut scaler = Autoscaler::new(policy());
        let now = Instant::now();

        assert!(
            scaler
                .preview(&signals(4, 40), now)
                .target_nodes()
                .is_some()
        );
        assert!(
            scaler
                .preview(&signals(4, 40), now)
                .target_nodes()
                .is_some()
        );
        // Showing somebody the reasoning is not making a decision.
        assert!(!scaler.cooling_down(now));

        scaler.decide(&signals(4, 40), now);
        assert!(scaler.cooling_down(now));
    }

    #[test]
    fn a_zero_target_does_not_divide_by_it() {
        let mut scaler = Autoscaler::new(Policy {
            target: Target::QueueLength { per_node: 0.0 },
            ..policy()
        });
        let decision = scaler.decide(&signals(4, 0), Instant::now());

        assert!(!format!("{decision:?}").contains("NaN"), "{decision:?}");
    }

    #[test]
    fn a_policy_round_trips_through_toml() {
        let policy = Policy {
            target: Target::Latency { max_wait_secs: 2.5 },
            min_nodes: 2,
            max_nodes: 32,
            cooldown_secs: 90,
            tolerance: 0.1,
        };

        let encoded = toml::to_string(&policy).expect("serialisable");
        let decoded: Policy = toml::from_str(&encoded).expect("readable");
        assert_eq!(decoded, policy);
    }
}
