//! Agent entry point: registers with the controller, heartbeats, runs tasks.

use std::path::PathBuf;
use std::time::Duration;

use aether_agent::{
    AgentClient, AgentConfig, MetricsCollector, identity, local_node_info, with_reconnect,
};
use clap::Parser;
use tracing::info;

#[derive(Parser)]
#[command(name = "aether-agent", about = "AetherMesh worker node")]
struct Args {
    /// TOML configuration file. Flags below override its values.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Controller address to register with.
    #[arg(long)]
    controller: Option<String>,

    /// Address other nodes can reach this agent on.
    #[arg(long)]
    advertise: Option<String>,

    /// Seconds between heartbeats.
    #[arg(long)]
    heartbeat_secs: Option<u64>,

    /// Shared secret, when the controller requires one.
    #[arg(long, env = "AETHERMESH_TOKEN")]
    auth_token: Option<String>,

    /// Certificate to verify the controller against (enables TLS).
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// This agent's own certificate, for mutual TLS.
    #[arg(long, requires = "tls_client_key")]
    tls_client_cert: Option<PathBuf>,

    /// Key for `--tls-client-cert`.
    #[arg(long, requires = "tls_client_cert")]
    tls_client_key: Option<PathBuf>,

    /// File holding this node's persistent id. Several agents on one machine
    /// each need their own, or they register as the same node.
    #[arg(long)]
    identity_path: Option<PathBuf>,

    /// Extra connections to offer for bulk data transfer.
    #[arg(long)]
    data_channels: Option<usize>,

    /// Declares what this machine is, as `key=value`. Repeatable:
    /// `--label gpu=true --label region=eu-west`. Tasks can require these.
    #[arg(long = "label", value_name = "KEY=VALUE")]
    labels: Vec<String>,

    /// Megabytes of received data this node will hold before dropping the
    /// least recently used. Unset means no limit.
    #[arg(long, value_name = "MB")]
    storage_budget_mb: Option<u64>,

    /// Tasks to run at once. Unset means one per logical CPU.
    #[arg(long)]
    max_concurrent_tasks: Option<usize>,

    /// Longest gap between attempts to reach the controller again after the
    /// connection drops. `0` exits instead.
    #[arg(long)]
    reconnect_max_secs: Option<u64>,

    /// OTLP/HTTP endpoint to send traces to, e.g.
    /// `http://127.0.0.1:4318/v1/traces`. Needs a build with `--features otel`.
    #[arg(long)]
    otlp_endpoint: Option<String>,
}

/// Level the console shows when nobody set `RUST_LOG`.
const DEFAULT_LOG: &str = "info";

/// The console filter, from `RUST_LOG`.
///
/// Spelled out rather than left to `tracing_subscriber::fmt::init()`. Its own
/// default depends on whether the `env-filter` feature happens to be compiled
/// in — INFO without it, ERROR with it — and features are unified across a
/// workspace build, so enabling `env-filter` for one crate silently turned
/// both binaries mute for anyone who had not set `RUST_LOG`. A program that
/// starts up and says nothing looks broken, and was.
fn console_filter() -> tracing_subscriber::EnvFilter {
    filter_from("RUST_LOG")
}

fn filter_from(variable: &str) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_env(variable)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_LOG))
}

/// Logs to the console and nowhere else.
fn init_console_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(console_filter())
        .init();
}

/// Starts logging, and tracing export when an endpoint is configured.
///
/// The returned value has to outlive the work: dropping it flushes whatever
/// has not been exported yet.
fn start_tracing(endpoint: Option<&str>) -> anyhow::Result<Option<impl Sized>> {
    #[cfg(feature = "otel")]
    if let Some(endpoint) = endpoint {
        let guard = aether_agent::otel::init(endpoint)?;
        info!(endpoint, "exporting traces");
        return Ok(Some(guard));
    }

    #[cfg(not(feature = "otel"))]
    if endpoint.is_some() {
        init_console_logging();
        tracing::warn!(
            "otlp_endpoint is set but this build has no OTLP support; rebuild with --features otel"
        );
        return Ok(None::<()>);
    }

    init_console_logging();
    Ok(None)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _tracing = start_tracing(args.otlp_endpoint.as_deref())?;

    let mut config = AgentConfig::load_or_default(args.config.as_deref())?;
    if let Some(controller) = args.controller {
        config.controller = controller;
    }
    if let Some(advertise) = args.advertise {
        config.advertise = advertise;
    }
    if let Some(heartbeat_secs) = args.heartbeat_secs {
        config.heartbeat_secs = heartbeat_secs;
    }
    if args.auth_token.is_some() {
        config.auth_token = args.auth_token.clone();
    }
    if args.tls_ca.is_some() {
        config.tls_ca_path = args.tls_ca.clone();
    }
    if args.identity_path.is_some() {
        config.identity_path = args.identity_path.clone();
    }
    if args.tls_client_cert.is_some() {
        config.tls_client_cert_path = args.tls_client_cert.clone();
        config.tls_client_key_path = args.tls_client_key.clone();
    }
    if let Some(channels) = args.data_channels {
        config.data_channels = channels;
    }
    // Flags add to the file rather than replacing it: the file says what the
    // machine is, the flags say what this run of it is.
    config.labels.extend(args.labels);
    if let Some(megabytes) = args.storage_budget_mb {
        config.storage_budget_bytes = Some(megabytes.saturating_mul(1024 * 1024));
    }
    if args.max_concurrent_tasks.is_some() {
        config.max_concurrent_tasks = args.max_concurrent_tasks;
    }
    if let Some(secs) = args.reconnect_max_secs {
        config.reconnect_max_secs = secs;
    }

    // A restarted agent keeps its identity, so the controller sees one node.
    let identity_path = config
        .identity_path
        .clone()
        .unwrap_or_else(identity::default_identity_path);
    let node_id = identity::load_or_create(&identity_path)?;

    let mut info = local_node_info(node_id, config.advertise.clone());
    if let Some(bandwidth) = config.bandwidth_bytes_per_sec {
        info = info.with_bandwidth(bandwidth);
    }
    let labels = config.parsed_labels();
    info = info.with_labels(labels.clone());
    info!(
        %node_id,
        hostname = %info.hostname,
        cores = info.cpu_cores,
        tls = config.tls_ca_path.is_some(),
        labels = labels.len(),
        "starting agent"
    );

    let heartbeat = Duration::from_secs(config.heartbeat_secs);
    let reconnect = Duration::from_secs(config.reconnect_max_secs);
    // Built once and kept across reconnections. It is what the node holds, and
    // it does not stop being true because a socket closed.
    let store = match config.storage_budget_bytes {
        Some(budget) => aether_core::DataStore::with_budget(budget),
        None => aether_core::DataStore::new(),
    };
    match config.tls_ca_path.clone() {
        #[cfg(feature = "tls")]
        Some(ca_path) => {
            let connector = match config.client_identity() {
                Some((cert, key)) => {
                    aether_agent::tls::connector_with_client_cert(&ca_path, &cert, &key)?
                }
                None => aether_agent::tls::connector(&ca_path)?,
            };
            with_reconnect(reconnect, async || {
                let mut client = aether_agent::tls::connect(
                    &config.controller,
                    &config.server_name(),
                    &connector,
                    info.clone(),
                    config.auth_token.clone(),
                )
                .await?;
                client = client.with_store(store.clone());
                if let Some(tasks) = config.max_concurrent_tasks {
                    client = client.with_max_concurrent_tasks(tasks);
                }
                client.run(MetricsCollector::new(), heartbeat).await?;
                Ok(())
            })
            .await?;
        }
        #[cfg(not(feature = "tls"))]
        Some(_) => anyhow::bail!("this build has no TLS support; rebuild with --features tls"),
        None => {
            with_reconnect(reconnect, async || {
                let mut client = AgentClient::connect_with_token(
                    &config.controller,
                    info.clone(),
                    config.auth_token.clone(),
                )
                .await?;
                client = client.with_store(store.clone());
                if let Some(tasks) = config.max_concurrent_tasks {
                    client = client.with_max_concurrent_tasks(tasks);
                }

                // Extra connections are opened before the run loop starts, so
                // the first large transfer already has them.
                let _channels = client
                    .open_data_channels(
                        &config.controller,
                        config.data_channels,
                        config.auth_token.clone(),
                    )
                    .await?;

                client.run(MetricsCollector::new(), heartbeat).await?;
                Ok(())
            })
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::filter::LevelFilter;

    use super::*;

    #[test]
    fn an_unset_variable_still_shows_something() {
        // Reads a name nothing sets rather than clearing RUST_LOG, which would
        // race every other test in the process.
        let filter = filter_from("AETHERMESH_LOG_FILTER_THAT_IS_NEVER_SET");

        assert_eq!(filter.max_level_hint(), Some(LevelFilter::INFO));
    }
}
