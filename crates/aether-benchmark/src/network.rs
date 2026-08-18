//! Measuring a mesh that is actually running, over actual sockets.
//!
//! The rest of this crate measures an in-process simulation: useful for
//! scheduler arithmetic, worthless as evidence. Nothing there touches a socket,
//! and the "bandwidth" it reasons about is a number somebody typed.
//!
//! This connects to a live controller as an ordinary client and measures what
//! happened. Point it at `127.0.0.1` and it measures loopback; point it at a
//! controller with a Raspberry Pi and a cloud VM registered and it measures
//! those, with no code change and no privileged access — everything here goes
//! through the same client API any program would use.
//!
//! # The baseline
//!
//! Two runs of the same work.
//!
//! **Naive**: every task publishes its own copy of the dataset. Different bytes
//! each time, so the content hash differs, so nothing deduplicates and no node
//! is ever already holding it. This is what a system that ships data to code
//! does.
//!
//! **AetherMesh**: publish once, then run every task against that one id.
//!
//! The difference in bytes actually written to sockets is the claim this
//! project makes, measured rather than argued.

use std::path::Path;
use std::time::{Duration, Instant};

use aether_controller::client::NodeSummary;
use aether_controller::connection::{Connection, ConnectionError, Stats, SubmitOptions};
use aether_core::task::kind;
use serde::{Deserialize, Serialize};

/// How long to wait for one reply.
const TIMEOUT: Duration = Duration::from_secs(120);

/// A mesh the benchmark expects to find.
///
/// Declaring it is what stops a one-node result being reported as a three-node
/// one. The benchmark refuses to measure a mesh that does not match.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodesConfig {
    /// Controller client API to connect to.
    pub controller: Option<String>,
    /// Shared secret, if the controller wants one.
    pub token: Option<String>,
    /// The nodes that should be present.
    pub nodes: Vec<ExpectedNode>,
}

/// One node the benchmark expects.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedNode {
    /// What to call it in the report. Free text.
    pub name: String,
    /// Hostname it should register under. Omit to accept whatever is there.
    #[serde(default)]
    pub hostname: Option<String>,
    /// Labels it should be carrying, as `key=value`.
    #[serde(default)]
    pub labels: Vec<String>,
}

/// The benchmark could not run, or could not be trusted.
#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error(
        "expected {expected} node(s) but the mesh has {found}; \
         a result measured on a different mesh is not the result you asked for"
    )]
    WrongMesh { expected: usize, found: usize },
    #[error("no node named {name} is registered (found: {present})")]
    MissingNode { name: String, present: String },
    #[error("node {name} is missing the label {label}")]
    MissingLabel { name: String, label: String },
    #[error("the mesh has no nodes; start an agent")]
    EmptyMesh,
    #[error("task {index} failed on the node: {reason}")]
    TaskFailed { index: usize, reason: String },
}

impl NodesConfig {
    pub fn load(path: &Path) -> Result<Self, NetworkError> {
        let display = path.display().to_string();
        let contents = std::fs::read_to_string(path).map_err(|source| NetworkError::Io {
            path: display.clone(),
            source,
        })?;
        toml::from_str(&contents).map_err(|source| NetworkError::Parse {
            path: display,
            source,
        })
    }

    /// Checks the live mesh is the one this configuration describes.
    ///
    /// An empty configuration accepts whatever is there — you get a number, and
    /// the report says which mesh produced it.
    pub fn check(&self, nodes: &[NodeSummary]) -> Result<(), NetworkError> {
        if nodes.is_empty() {
            return Err(NetworkError::EmptyMesh);
        }
        if self.nodes.is_empty() {
            return Ok(());
        }

        if nodes.len() != self.nodes.len() {
            return Err(NetworkError::WrongMesh {
                expected: self.nodes.len(),
                found: nodes.len(),
            });
        }

        for expected in &self.nodes {
            let wanted = expected.hostname.as_deref().unwrap_or(&expected.name);
            let found = nodes
                .iter()
                .find(|node| node.hostname == wanted)
                .ok_or_else(|| NetworkError::MissingNode {
                    name: wanted.to_string(),
                    present: nodes
                        .iter()
                        .map(|node| node.hostname.clone())
                        .collect::<Vec<_>>()
                        .join(", "),
                })?;

            for label in &expected.labels {
                let (key, value) = label.split_once('=').unwrap_or((label.as_str(), ""));
                let matches = match value {
                    "" => found.labels.contains_key(key),
                    value => found.labels.get(key).map(String::as_str) == Some(value),
                };
                if !matches {
                    return Err(NetworkError::MissingLabel {
                        name: expected.name.clone(),
                        label: label.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// What to measure.
#[derive(Debug, Clone)]
pub struct NetworkOptions {
    pub controller: String,
    pub token: Option<String>,
    pub tasks: usize,
    pub dataset_bytes: usize,
    /// Fixes the datasets, so a run can be repeated exactly — against a mesh
    /// that does not already hold them. See [`fresh_seed`].
    pub seed: u64,
}

/// A seed unlikely to collide with a previous run's.
///
/// The default rather than a fixed number, because the nodes remember: run the
/// same seed twice and the second run measures a mesh that already has
/// everything, which is not the question anyone was asking. The seed used is
/// printed in the report, so a run can still be repeated exactly — against a
/// mesh that has been restarted.
pub fn fresh_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(1)
}

impl Default for NetworkOptions {
    fn default() -> Self {
        Self {
            controller: "127.0.0.1:7100".to_string(),
            token: None,
            tasks: 20,
            dataset_bytes: 4 * 1024 * 1024,
            seed: fresh_seed(),
        }
    }
}

/// What one mode moved and how long it took.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Measured {
    /// Bytes written to sockets, after compression.
    pub bytes: u64,
    /// Bytes those transfers represent, before compression.
    pub bytes_uncompressed: u64,
    /// Whole datasets not sent because a node already had them.
    pub transfers_skipped: u64,
    /// Chunks deduplicated against data a node already held.
    pub chunks_skipped: u64,
    pub tasks: usize,
    pub wall_ms: f64,
    /// Time the nodes reported spending, summed.
    pub node_ms: f64,
}

impl Measured {
    pub fn mean_task_ms(&self) -> f64 {
        match self.tasks {
            0 => 0.0,
            tasks => self.node_ms / tasks as f64,
        }
    }
}

/// One node as it was when the benchmark ran.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub hostname: String,
    pub address: String,
    pub cpu_cores: u32,
    pub latency_ms: Option<f32>,
    pub bandwidth_bytes_per_sec: Option<u64>,
    pub labels: std::collections::BTreeMap<String, String>,
}

impl From<&NodeSummary> for NodeRecord {
    fn from(node: &NodeSummary) -> Self {
        Self {
            hostname: node.hostname.clone(),
            address: node.address.clone(),
            cpu_cores: node.cpu_cores,
            latency_ms: node.latency_ms,
            bandwidth_bytes_per_sec: node.bandwidth_bytes_per_sec,
            labels: node.labels.clone(),
        }
    }
}

/// Where and when this was measured.
///
/// A benchmark number without this is an anecdote. Every field here is
/// something a reader needs before deciding whether the number applies to them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Environment {
    pub measured_at: String,
    pub client_os: String,
    pub client_arch: String,
    pub controller: String,
    pub version: String,
    pub nodes: Vec<NodeRecord>,
    /// Whether every node's address is loopback, which is the difference
    /// between a measurement and a demonstration.
    pub loopback_only: bool,
}

/// The whole report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetworkReport {
    pub environment: Environment,
    pub tasks: usize,
    pub dataset_bytes: usize,
    pub seed: u64,
    pub baseline: Measured,
    pub aethermesh: Measured,
    /// The headline: `baseline_bytes` against `aethermesh_bytes`.
    pub baseline_bytes: u64,
    pub aethermesh_bytes: u64,
    pub reduction_percent: f64,
    /// Reasons to distrust the numbers above, in the report rather than in a
    /// footnote somebody has to go and find.
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl NetworkReport {
    /// Human-readable form.
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        let env = &self.environment;

        out.push_str(&format!(
            "AetherMesh network benchmark\n  {}\n  controller {} · aethermesh {}\n  client {} {}\n",
            env.measured_at, env.controller, env.version, env.client_os, env.client_arch
        ));

        out.push_str(&format!("\n{} node(s):\n", env.nodes.len()));
        for node in &env.nodes {
            let latency = node
                .latency_ms
                .map(|ms| format!("{ms:.1} ms"))
                .unwrap_or_else(|| "unmeasured".to_string());
            let link = node
                .bandwidth_bytes_per_sec
                .map(|rate| format!("{:.1} MiB/s", rate as f64 / (1024.0 * 1024.0)))
                .unwrap_or_else(|| "unmeasured".to_string());
            out.push_str(&format!(
                "  {:<16} {:<22} {} cores · rtt {} · link {}\n",
                node.hostname, node.address, node.cpu_cores, latency, link
            ));
        }

        if env.loopback_only {
            out.push_str(
                "\n  Every node is on loopback. This measures the software, not a network:\n\
                 \x20 the transfer saving is real, the latency and bandwidth are not.\n",
            );
        }

        out.push_str(&format!(
            "\n{} tasks over a {:.1} MiB dataset (seed {})\n\n",
            self.tasks,
            self.dataset_bytes as f64 / (1024.0 * 1024.0),
            self.seed
        ));
        out.push_str(&format!(
            "  {:<14}{:>14}{:>14}\n",
            "", "naive", "aethermesh"
        ));
        out.push_str(&format!(
            "  {:<14}{:>14}{:>14}\n",
            "bytes sent",
            bytes(self.baseline.bytes),
            bytes(self.aethermesh.bytes)
        ));
        out.push_str(&format!(
            "  {:<14}{:>14}{:>14}\n",
            "wall clock",
            format!("{:.0} ms", self.baseline.wall_ms),
            format!("{:.0} ms", self.aethermesh.wall_ms)
        ));
        out.push_str(&format!(
            "  {:<14}{:>14}{:>14}\n",
            "mean task",
            format!("{:.1} ms", self.baseline.mean_task_ms()),
            format!("{:.1} ms", self.aethermesh.mean_task_ms())
        ));
        out.push_str(&format!(
            "  {:<14}{:>14}{:>14}\n",
            "sends skipped", self.baseline.transfers_skipped, self.aethermesh.transfers_skipped
        ));

        out.push_str(&format!(
            "\n  traffic reduction: {:.1} %\n",
            self.reduction_percent
        ));

        for warning in &self.warnings {
            out.push_str(&format!("  ! {warning}\n"));
        }
        out
    }
}

fn bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = value as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{value} B"),
        _ => format!("{size:.1} {}", UNITS[unit]),
    }
}

/// Bytes that compress badly and differ per run, so a transfer is a transfer.
///
/// A dataset of zeroes would compress to nothing and make every number look
/// wonderful for the wrong reason.
pub fn dataset(seed: u64, size: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut out = Vec::with_capacity(size);
    while out.len() < size {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(size);
    out
}

/// Anything about this run that makes the numbers less than they look.
fn warnings(baseline: &Measured, aethermesh: &Measured) -> Vec<String> {
    let mut warnings = Vec::new();

    if baseline.bytes == 0 {
        // Almost always a repeat run: the nodes still hold the datasets from
        // last time, so nothing had to move and the "saving" is meaningless.
        warnings.push(
            [
                "The baseline moved no bytes, so there is nothing to compare against.",
                "The nodes most likely still hold this run's datasets from a previous",
                "one - pass a different --seed, or restart the agents.",
            ]
            .join(" "),
        );
    } else if aethermesh.transfers_skipped as usize >= aethermesh.tasks {
        warnings.push(
            [
                "Every AetherMesh task found its input already in place, including",
                "the first. The dataset was on a node before the run started, so the",
                "saving shown is larger than a cold mesh would give.",
            ]
            .join(" "),
        );
    }

    warnings
}

/// Runs both modes against a live controller and reports the difference.
pub async fn run(
    options: &NetworkOptions,
    expected: &NodesConfig,
) -> Result<NetworkReport, NetworkError> {
    let mut mesh = Connection::connect(&options.controller, options.token.clone(), TIMEOUT).await?;

    let nodes = mesh.nodes().await?;
    expected.check(&nodes)?;
    let environment = Environment {
        measured_at: timestamp(),
        client_os: std::env::consts::OS.to_string(),
        client_arch: std::env::consts::ARCH.to_string(),
        controller: options.controller.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        loopback_only: nodes.iter().all(|node| is_loopback(&node.address)),
        nodes: nodes.iter().map(NodeRecord::from).collect(),
    };

    // Naive first: it leaves the mesh holding a lot of one-off datasets, and
    // running it second would let those skew the second measurement.
    let baseline = measure_naive(&mut mesh, options).await?;
    let aethermesh = measure_reused(&mut mesh, options).await?;

    let reduction_percent = match baseline.bytes {
        0 => 0.0,
        sent => (1.0 - aethermesh.bytes as f64 / sent as f64) * 100.0,
    };

    Ok(NetworkReport {
        environment,
        tasks: options.tasks,
        dataset_bytes: options.dataset_bytes,
        seed: options.seed,
        baseline_bytes: baseline.bytes,
        aethermesh_bytes: aethermesh.bytes,
        warnings: warnings(&baseline, &aethermesh),
        baseline,
        aethermesh,
        reduction_percent,
    })
}

/// Every task ships its own copy: what a system without content addressing does.
async fn measure_naive(
    mesh: &mut Connection,
    options: &NetworkOptions,
) -> Result<Measured, NetworkError> {
    let before = mesh.stats().await?;
    let started = Instant::now();
    let mut node_ms = 0.0;

    for index in 0..options.tasks {
        // A different seed per task, so the bytes differ and nothing about
        // them can be reused. This is the whole point of the baseline.
        let bytes = dataset(baseline_seed(options.seed, index), options.dataset_bytes);
        let published = mesh.publish(bytes).await?;
        node_ms += run_one(mesh, index, published.data_id).await?;
    }

    let after = mesh.stats().await?;
    Ok(measured(before, after, options.tasks, started, node_ms))
}

/// The seed for one baseline task.
///
/// Offset past the shared dataset's seed on purpose. When the baseline's first
/// task used the same seed it published byte-for-byte the same data, so by the
/// time the second mode ran a node already held it — and the benchmark
/// reported a 100 % saving that was an accident of seeding rather than a
/// measurement of anything.
fn baseline_seed(seed: u64, index: usize) -> u64 {
    seed.wrapping_add(1).wrapping_add(index as u64)
}

/// Publish once, run everything against that: what this project is for.
async fn measure_reused(
    mesh: &mut Connection,
    options: &NetworkOptions,
) -> Result<Measured, NetworkError> {
    let bytes = dataset(options.seed, options.dataset_bytes);
    // Inside the measurement window: this mode still has to get the data onto
    // a node once, and leaving that out would report a saving nobody gets.
    let before = mesh.stats().await?;
    let published = mesh.publish(bytes).await?;
    let started = Instant::now();
    let mut node_ms = 0.0;

    for index in 0..options.tasks {
        node_ms += run_one(mesh, index, published.data_id).await?;
    }

    let after = mesh.stats().await?;
    Ok(measured(before, after, options.tasks, started, node_ms))
}

/// Runs one task over a dataset and returns what the node reported.
async fn run_one(
    mesh: &mut Connection,
    index: usize,
    data_id: aether_core::DataId,
) -> Result<f64, NetworkError> {
    let finished = mesh
        .submit(
            kind::HASH,
            index.to_string().into_bytes(),
            &SubmitOptions::reading(vec![data_id]),
        )
        .await?;

    if !finished.success {
        return Err(NetworkError::TaskFailed {
            index,
            reason: finished
                .error
                .unwrap_or_else(|| "no reason given".to_string()),
        });
    }
    Ok(finished.duration_ms)
}

/// The difference between two readings of the controller's own counters.
fn measured(before: Stats, after: Stats, tasks: usize, started: Instant, node_ms: f64) -> Measured {
    Measured {
        bytes: after
            .traffic
            .bytes_sent
            .saturating_sub(before.traffic.bytes_sent),
        bytes_uncompressed: after
            .traffic
            .bytes_uncompressed
            .saturating_sub(before.traffic.bytes_uncompressed),
        transfers_skipped: after
            .traffic
            .transfers_skipped
            .saturating_sub(before.traffic.transfers_skipped),
        chunks_skipped: after
            .traffic
            .chunks_skipped
            .saturating_sub(before.traffic.chunks_skipped),
        tasks,
        wall_ms: started.elapsed().as_secs_f64() * 1000.0,
        node_ms,
    }
}

/// Whether an address is on this machine.
///
/// Agents advertise `host:port`, and an IPv6 host brings its own colons, so
/// the port cannot simply be split off the end.
fn is_loopback(address: &str) -> bool {
    let host = match address.rsplit_once(']') {
        // `[::1]:7001` — the bracketed form, with or without a port.
        Some((bracketed, _)) => bracketed.trim_start_matches('['),
        None if address.matches(':').count() > 1 => address,
        None => address.rsplit_once(':').map_or(address, |(host, _)| host),
    };

    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Seconds since the epoch, as text. Enough to order two reports.
fn timestamp() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    format!("unix:{seconds}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(hostname: &str, address: &str) -> NodeSummary {
        NodeSummary {
            node_id: format!("{hostname}-id"),
            hostname: hostname.to_string(),
            cpu_cores: 4,
            cpu_usage: 0.1,
            memory_usage: 0.2,
            labels: Default::default(),
            address: address.to_string(),
            latency_ms: None,
            bandwidth_bytes_per_sec: None,
            datasets_held: 0,
            bytes_held: 0,
            connected: true,
        }
    }

    #[test]
    fn no_baseline_task_reuses_the_shared_dataset_seed() {
        let seed = 1;
        let baseline: Vec<_> = (0..50).map(|index| baseline_seed(seed, index)).collect();

        // A collision here means the second mode finds its data already on a
        // node and reports a saving it did not earn. It happened.
        assert!(!baseline.contains(&seed), "{baseline:?}");
        let mut unique = baseline.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            unique.len(),
            baseline.len(),
            "every task gets its own bytes"
        );
    }

    #[test]
    fn a_dataset_is_the_same_bytes_for_the_same_seed() {
        assert_eq!(dataset(7, 4096), dataset(7, 4096));
        assert_ne!(dataset(7, 4096), dataset(8, 4096));
        assert_eq!(dataset(1, 1000).len(), 1000, "an odd size is respected");
    }

    #[test]
    fn a_dataset_does_not_compress_away_to_nothing() {
        let data = dataset(3, 64 * 1024);
        let compressed = aether_core::compress::compress(aether_core::Codec::Lz4, &data);

        // Zeroes would compress to almost nothing and make every reduction
        // number look wonderful for entirely the wrong reason.
        assert!(
            compressed.len() as f64 > data.len() as f64 * 0.9,
            "{} of {}",
            compressed.len(),
            data.len()
        );
    }

    #[test]
    fn an_empty_expectation_accepts_whatever_is_running() {
        let config = NodesConfig::default();
        assert!(config.check(&[node("anything", "10.0.0.1:7001")]).is_ok());
    }

    #[test]
    fn an_empty_mesh_is_refused_whatever_was_expected() {
        assert!(matches!(
            NodesConfig::default().check(&[]),
            Err(NetworkError::EmptyMesh)
        ));
    }

    #[test]
    fn a_mesh_of_the_wrong_size_is_refused() {
        let config = NodesConfig {
            nodes: vec![
                ExpectedNode {
                    name: "local".to_string(),
                    hostname: None,
                    labels: Vec::new(),
                },
                ExpectedNode {
                    name: "pi".to_string(),
                    hostname: None,
                    labels: Vec::new(),
                },
            ],
            ..NodesConfig::default()
        };

        // Reporting a one-node number as a two-node one is the single easiest
        // way to publish a benchmark that is a lie.
        assert!(matches!(
            config.check(&[node("local", "127.0.0.1:7001")]),
            Err(NetworkError::WrongMesh {
                expected: 2,
                found: 1
            })
        ));
    }

    #[test]
    fn a_missing_node_is_named_along_with_what_was_found() {
        let config = NodesConfig {
            nodes: vec![ExpectedNode {
                name: "pi".to_string(),
                hostname: None,
                labels: Vec::new(),
            }],
            ..NodesConfig::default()
        };

        let error = config
            .check(&[node("laptop", "127.0.0.1:7001")])
            .expect_err("the wrong node");
        let message = error.to_string();
        assert!(message.contains("pi"), "{message}");
        assert!(message.contains("laptop"), "{message}");
    }

    #[test]
    fn a_node_missing_a_required_label_is_refused() {
        let config = NodesConfig {
            nodes: vec![ExpectedNode {
                name: "gpu-box".to_string(),
                hostname: None,
                labels: vec!["kind=gpu".to_string()],
            }],
            ..NodesConfig::default()
        };

        let mut plain = node("gpu-box", "10.0.0.2:7001");
        assert!(matches!(
            config.check(std::slice::from_ref(&plain)),
            Err(NetworkError::MissingLabel { .. })
        ));

        plain.labels.insert("kind".to_string(), "gpu".to_string());
        assert!(config.check(&[plain]).is_ok());
    }

    #[test]
    fn a_bare_label_only_asks_that_it_is_present() {
        let config = NodesConfig {
            nodes: vec![ExpectedNode {
                name: "any".to_string(),
                hostname: None,
                labels: vec!["region".to_string()],
            }],
            ..NodesConfig::default()
        };

        let mut labelled = node("any", "10.0.0.3:7001");
        labelled
            .labels
            .insert("region".to_string(), "anywhere".to_string());
        assert!(config.check(&[labelled]).is_ok());
    }

    #[test]
    fn loopback_is_recognised_so_a_report_can_say_so() {
        assert!(is_loopback("127.0.0.1:7001"));
        assert!(is_loopback("localhost:7001"));
        assert!(is_loopback("[::1]:7001"), "the bracketed form with a port");
        assert!(is_loopback("::1"), "a bare IPv6 address");
        assert!(!is_loopback("[2001:db8::1]:7001"));
        assert!(!is_loopback("192.168.1.10:7001"));
        assert!(!is_loopback("mesh.example.com:7001"));
    }

    #[test]
    fn a_report_says_when_it_measured_only_loopback() {
        let report = report_with(vec![node("local", "127.0.0.1:7001")], 1_000, 100);
        let text = report.to_text();

        // A reader should not have to work out that "0.1 ms" came from a
        // machine talking to itself.
        assert!(text.contains("loopback"), "{text}");
        assert!(text.contains("90.0 %"), "{text}");
    }

    #[test]
    fn a_report_over_a_real_network_does_not_carry_the_loopback_warning() {
        let report = report_with(vec![node("pi", "192.168.1.42:7001")], 1_000, 100);
        assert!(!report.to_text().contains("loopback"));
    }

    #[test]
    fn a_baseline_that_moved_nothing_reports_no_reduction_rather_than_a_nan() {
        let report = report_with(vec![node("local", "127.0.0.1:7001")], 0, 0);
        assert_eq!(report.reduction_percent, 0.0);
        assert!(!report.to_text().contains("NaN"));
    }

    #[test]
    fn a_baseline_that_moved_nothing_is_called_out_not_reported_as_a_win() {
        let nothing = Measured {
            bytes: 0,
            bytes_uncompressed: 0,
            transfers_skipped: 10,
            chunks_skipped: 0,
            tasks: 10,
            wall_ms: 5.0,
            node_ms: 5.0,
        };

        // The usual cause is a repeat run against nodes that still hold the
        // data. Reporting that as 0 % with no explanation sends someone
        // hunting for a bug in the mesh.
        let warnings = warnings(&nothing, &nothing);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("--seed"), "{warnings:?}");
    }

    #[test]
    fn a_warm_mesh_is_called_out_even_when_the_baseline_moved_bytes() {
        let moved = Measured {
            bytes: 1_000,
            bytes_uncompressed: 1_000,
            transfers_skipped: 0,
            chunks_skipped: 0,
            tasks: 10,
            wall_ms: 5.0,
            node_ms: 5.0,
        };
        let every_one_skipped = Measured {
            bytes: 0,
            transfers_skipped: 10,
            ..moved
        };

        let warnings = warnings(&moved, &every_one_skipped);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("already in place"), "{warnings:?}");
    }

    #[test]
    fn a_cold_run_carries_no_warnings() {
        let baseline = Measured {
            bytes: 80 * 1024 * 1024,
            bytes_uncompressed: 80 * 1024 * 1024,
            transfers_skipped: 0,
            chunks_skipped: 0,
            tasks: 20,
            wall_ms: 700.0,
            node_ms: 20.0,
        };
        let aethermesh = Measured {
            bytes: 4 * 1024 * 1024,
            transfers_skipped: 19,
            ..baseline
        };

        assert!(warnings(&baseline, &aethermesh).is_empty());
    }

    #[test]
    fn two_fresh_seeds_differ() {
        // Repeating a seed against a warm mesh measures nothing, so the
        // default has to move.
        assert_ne!(fresh_seed(), 0);
        let first = fresh_seed();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_ne!(first, fresh_seed());
    }

    #[test]
    fn a_report_round_trips_through_json() {
        let report = report_with(vec![node("local", "127.0.0.1:7001")], 1_000, 100);
        let encoded = serde_json::to_string(&report).expect("serialisable");
        let decoded: NetworkReport = serde_json::from_str(&encoded).expect("readable");

        assert_eq!(decoded, report);
        // The three fields the prompt asks for, by the names it asks for.
        assert!(encoded.contains("\"baseline_bytes\":1000"));
        assert!(encoded.contains("\"aethermesh_bytes\":100"));
        assert!(encoded.contains("\"reduction_percent\":90.0"));
    }

    fn report_with(nodes: Vec<NodeSummary>, baseline: u64, aethermesh: u64) -> NetworkReport {
        let measured = |bytes: u64| Measured {
            bytes,
            bytes_uncompressed: bytes,
            transfers_skipped: 0,
            chunks_skipped: 0,
            tasks: 10,
            wall_ms: 100.0,
            node_ms: 50.0,
        };
        let reduction = match baseline {
            0 => 0.0,
            sent => (1.0 - aethermesh as f64 / sent as f64) * 100.0,
        };

        NetworkReport {
            environment: Environment {
                measured_at: "unix:0".to_string(),
                client_os: "test".to_string(),
                client_arch: "test".to_string(),
                controller: "127.0.0.1:7100".to_string(),
                version: "0.1.0".to_string(),
                loopback_only: nodes.iter().all(|node| is_loopback(&node.address)),
                nodes: nodes.iter().map(NodeRecord::from).collect(),
            },
            tasks: 10,
            dataset_bytes: 4096,
            seed: 1,
            baseline_bytes: baseline,
            aethermesh_bytes: aethermesh,
            baseline: measured(baseline),
            aethermesh: measured(aethermesh),
            reduction_percent: reduction,
            warnings: Vec::new(),
        }
    }
}
