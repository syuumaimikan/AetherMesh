//! Noticing when a change made the mesh worse.
//!
//! The trap here is gating on time. A shared CI runner's wall clock varies by
//! more than most real regressions, so a check built on it fails constantly,
//! gets marked flaky, and then gets ignored — which is worse than not having it
//! at all, because now nobody is watching.
//!
//! Bytes do not have that problem. `bytes_uncompressed` for a given task count
//! and dataset size is arithmetic, not measurement: verified identical across
//! restarts, across seeds, and between a one-agent and a two-agent mesh. It
//! moves when deduplication or locality breaks and at no other time, which is
//! exactly what a regression check should be watching.
//!
//! So bytes gate the build and timings are reported beside them. `--gate-timing`
//! exists for someone running on hardware they control.

use serde::{Deserialize, Serialize};

use crate::network::NetworkReport;

/// How far a number may drift before it counts as a regression.
pub const DEFAULT_TOLERANCE_PERCENT: f64 = 2.0;

/// Timings vary enough that a useful threshold is a different order of
/// magnitude from the one for bytes.
pub const DEFAULT_TIMING_TOLERANCE_PERCENT: f64 = 50.0;

/// Whether bigger is better for a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// A drop is a regression: traffic reduction, transfers skipped.
    HigherIsBetter,
    /// A rise is a regression: bytes moved, milliseconds taken.
    LowerIsBetter,
}

/// One metric, then and now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Check {
    pub metric: String,
    pub direction: Direction,
    pub baseline: f64,
    pub current: f64,
    /// Signed change, as a percentage of the baseline.
    pub change_percent: f64,
    pub tolerance_percent: f64,
    /// Whether this metric can fail the build.
    pub gating: bool,
    pub regressed: bool,
}

impl Check {
    fn new(
        metric: &str,
        direction: Direction,
        baseline: f64,
        current: f64,
        tolerance_percent: f64,
        gating: bool,
    ) -> Self {
        // A baseline of zero has no percentage to speak of: any movement away
        // from it is either everything or nothing.
        let change_percent = if baseline == 0.0 {
            if current == 0.0 { 0.0 } else { 100.0 }
        } else {
            (current - baseline) / baseline * 100.0
        };

        let regressed = match direction {
            Direction::HigherIsBetter => change_percent < -tolerance_percent,
            Direction::LowerIsBetter => change_percent > tolerance_percent,
        };

        Self {
            metric: metric.to_string(),
            direction,
            baseline,
            current,
            change_percent,
            tolerance_percent,
            gating,
            regressed,
        }
    }
}

/// Two reports cannot be compared.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MismatchError {
    #[error(
        "the reports measured different work ({baseline} vs {current} {what}); \
         comparing them would report a difference nobody made"
    )]
    DifferentWork {
        what: &'static str,
        baseline: String,
        current: String,
    },
}

/// A baseline against a fresh run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Comparison {
    pub checks: Vec<Check>,
    /// Where the baseline came from, so a failure can be traced.
    pub baseline_measured_at: String,
    pub current_measured_at: String,
    pub baseline_command: String,
}

impl Comparison {
    /// Compares two reports of the same work.
    ///
    /// Refuses reports that measured different configurations: a 20-task run
    /// against an 8-task one differs for reasons that have nothing to do with
    /// the code, and reporting that as a regression trains people to ignore it.
    pub fn of(
        baseline: &NetworkReport,
        current: &NetworkReport,
        tolerance_percent: f64,
        timing_tolerance_percent: Option<f64>,
    ) -> Result<Self, MismatchError> {
        if baseline.tasks != current.tasks {
            return Err(MismatchError::DifferentWork {
                what: "tasks",
                baseline: baseline.tasks.to_string(),
                current: current.tasks.to_string(),
            });
        }
        if baseline.dataset_bytes != current.dataset_bytes {
            return Err(MismatchError::DifferentWork {
                what: "dataset bytes",
                baseline: baseline.dataset_bytes.to_string(),
                current: current.dataset_bytes.to_string(),
            });
        }

        let gate_timing = timing_tolerance_percent.is_some();
        let timing_tolerance = timing_tolerance_percent.unwrap_or(DEFAULT_TIMING_TOLERANCE_PERCENT);

        let checks = vec![
            // Deterministic given the configuration. These are the gate.
            Check::new(
                "bytes moved",
                Direction::LowerIsBetter,
                baseline.aethermesh.bytes_uncompressed as f64,
                current.aethermesh.bytes_uncompressed as f64,
                tolerance_percent,
                true,
            ),
            Check::new(
                "traffic reduction %",
                Direction::HigherIsBetter,
                baseline.reduction_percent,
                current.reduction_percent,
                tolerance_percent,
                true,
            ),
            Check::new(
                "sends skipped",
                Direction::HigherIsBetter,
                baseline.aethermesh.transfers_skipped as f64,
                current.aethermesh.transfers_skipped as f64,
                tolerance_percent,
                true,
            ),
            // Measurement, not arithmetic. Reported either way; gating only if
            // the caller says the hardware is stable enough to mean it.
            Check::new(
                "wall clock ms",
                Direction::LowerIsBetter,
                baseline.aethermesh.wall_ms,
                current.aethermesh.wall_ms,
                timing_tolerance,
                gate_timing,
            ),
            Check::new(
                "mean task ms",
                Direction::LowerIsBetter,
                baseline.aethermesh.mean_task_ms(),
                current.aethermesh.mean_task_ms(),
                timing_tolerance,
                gate_timing,
            ),
        ];

        Ok(Self {
            checks,
            baseline_measured_at: baseline.environment.measured_at.clone(),
            current_measured_at: current.environment.measured_at.clone(),
            baseline_command: baseline.environment.command.clone(),
        })
    }

    /// Whether anything that can fail the build did.
    pub fn failed(&self) -> bool {
        self.checks
            .iter()
            .any(|check| check.gating && check.regressed)
    }

    /// Regressions that were noticed but are not failing the build.
    pub fn warnings(&self) -> impl Iterator<Item = &Check> {
        self.checks
            .iter()
            .filter(|check| check.regressed && !check.gating)
    }

    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "Regression check\n  baseline {}\n  current  {}\n\n",
            self.baseline_measured_at, self.current_measured_at
        ));
        out.push_str(&format!(
            "  {:<20}{:>14}{:>14}{:>10}\n",
            "", "baseline", "current", "change"
        ));

        for check in &self.checks {
            let mark = match (check.regressed, check.gating) {
                (true, true) => "FAIL",
                (true, false) => "warn",
                (false, _) => "ok",
            };
            out.push_str(&format!(
                "  {:<20}{:>14.1}{:>14.1}{:>9.1}%  {}\n",
                check.metric, check.baseline, check.current, check.change_percent, mark
            ));
        }

        if self.failed() {
            out.push_str(&format!(
                "\n  A gated metric regressed. Reproduce the baseline with:\n    {}\n",
                self.baseline_command
            ));
        } else {
            out.push_str("\n  No gated regression.\n");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{Environment, Measured, NetworkReport};

    fn measured(bytes_uncompressed: u64, skipped: u64, wall_ms: f64) -> Measured {
        Measured {
            bytes: bytes_uncompressed,
            bytes_uncompressed,
            transfers_skipped: skipped,
            chunks_skipped: 0,
            tasks: 8,
            wall_ms,
            node_ms: 8.0,
        }
    }

    fn report(aethermesh: Measured, reduction: f64) -> NetworkReport {
        NetworkReport {
            environment: Environment {
                measured_at: "2026-01-01T00:00:00Z".to_string(),
                client_os: "test".to_string(),
                client_arch: "test".to_string(),
                client_cpus: 8,
                controller: "127.0.0.1:7100".to_string(),
                version: "0.1.0".to_string(),
                nodes: Vec::new(),
                loopback_only: true,
                command: "cargo run -p aether-benchmark -- network --seed 1".to_string(),
            },
            tasks: 8,
            dataset_bytes: 1024 * 1024,
            seed: 1,
            baseline: measured(8 * 1024 * 1024, 0, 100.0),
            aethermesh,
            baseline_bytes: 8 * 1024 * 1024,
            aethermesh_bytes: aethermesh.bytes,
            reduction_percent: reduction,
            warnings: Vec::new(),
        }
    }

    /// A report that is exactly what the baseline was.
    fn unchanged() -> NetworkReport {
        report(measured(1024 * 1024, 7, 40.0), 87.5)
    }

    fn compare(current: &NetworkReport) -> Comparison {
        Comparison::of(&unchanged(), current, DEFAULT_TOLERANCE_PERCENT, None)
            .expect("the same work")
    }

    #[test]
    fn an_identical_run_passes() {
        let comparison = compare(&unchanged());

        assert!(!comparison.failed());
        assert!(comparison.checks.iter().all(|check| !check.regressed));
        assert!(comparison.to_text().contains("No gated regression"));
    }

    #[test]
    fn losing_deduplication_fails_the_build() {
        // Every task moving its own copy again: the exact regression this
        // whole check exists to catch.
        let broken = report(measured(8 * 1024 * 1024, 0, 40.0), 0.0);
        let comparison = compare(&broken);

        assert!(comparison.failed());
        let text = comparison.to_text();
        assert!(text.contains("FAIL"), "{text}");
        // A failure should hand over the command that reproduces the baseline.
        assert!(text.contains("--seed 1"), "{text}");
    }

    #[test]
    fn a_drop_in_traffic_reduction_fails_even_if_bytes_look_similar() {
        let worse = report(measured(1024 * 1024, 7, 40.0), 60.0);
        let comparison = compare(&worse);

        assert!(comparison.failed());
        let reduction = comparison
            .checks
            .iter()
            .find(|check| check.metric == "traffic reduction %")
            .expect("the metric");
        assert!(reduction.regressed);
        assert!(reduction.change_percent < 0.0);
    }

    #[test]
    fn an_improvement_is_never_a_regression() {
        let better = report(measured(512 * 1024, 8, 20.0), 93.0);
        let comparison = compare(&better);

        assert!(!comparison.failed());
        assert!(comparison.checks.iter().all(|check| !check.regressed));
    }

    #[test]
    fn a_slower_run_is_reported_but_does_not_fail_the_build() {
        // Ten times slower. On a shared CI runner this happens for reasons
        // that have nothing to do with the change being tested.
        let slow = report(measured(1024 * 1024, 7, 400.0), 87.5);
        let comparison = compare(&slow);

        assert!(!comparison.failed(), "timing must not gate by default");
        let warned: Vec<_> = comparison.warnings().map(|check| &check.metric).collect();
        assert!(warned.iter().any(|metric| *metric == "wall clock ms"));
        assert!(comparison.to_text().contains("warn"));
    }

    #[test]
    fn timing_gates_when_the_caller_asks_for_it() {
        let slow = report(measured(1024 * 1024, 7, 400.0), 87.5);
        let comparison = Comparison::of(&unchanged(), &slow, DEFAULT_TOLERANCE_PERCENT, Some(50.0))
            .expect("the same work");

        assert!(comparison.failed());
    }

    #[test]
    fn noise_inside_the_tolerance_is_not_a_regression() {
        // A byte count that moved by 1 %, under a 2 % tolerance.
        let jittered = report(measured(1024 * 1024 + 10_000, 7, 41.0), 87.4);
        assert!(!compare(&jittered).failed());
    }

    #[test]
    fn reports_of_different_work_are_refused_rather_than_compared() {
        let mut different = unchanged();
        different.tasks = 20;

        let outcome = Comparison::of(&unchanged(), &different, DEFAULT_TOLERANCE_PERCENT, None);
        let message = outcome.expect_err("different work").to_string();

        // Comparing 8 tasks against 20 would report a regression nobody caused,
        // and a check that cries wolf is a check that gets switched off.
        assert!(message.contains("8"), "{message}");
        assert!(message.contains("20"), "{message}");
    }

    #[test]
    fn a_different_dataset_size_is_refused_too() {
        let mut different = unchanged();
        different.dataset_bytes = 4 * 1024 * 1024;

        assert!(matches!(
            Comparison::of(&unchanged(), &different, DEFAULT_TOLERANCE_PERCENT, None),
            Err(MismatchError::DifferentWork {
                what: "dataset bytes",
                ..
            })
        ));
    }

    #[test]
    fn a_baseline_of_zero_does_not_divide_by_it() {
        let from_nothing = Check::new("m", Direction::LowerIsBetter, 0.0, 5.0, 2.0, true);
        assert_eq!(from_nothing.change_percent, 100.0);
        assert!(from_nothing.regressed);

        let still_nothing = Check::new("m", Direction::LowerIsBetter, 0.0, 0.0, 2.0, true);
        assert_eq!(still_nothing.change_percent, 0.0);
        assert!(!still_nothing.regressed);
    }

    #[test]
    fn a_comparison_round_trips_through_json() {
        let comparison = compare(&unchanged());
        let encoded = serde_json::to_string(&comparison).expect("serialisable");
        let decoded: Comparison = serde_json::from_str(&encoded).expect("readable");

        assert_eq!(decoded, comparison);
    }
}
