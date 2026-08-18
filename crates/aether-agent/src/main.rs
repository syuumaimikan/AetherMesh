//! Agent entry point: registers with the controller, heartbeats, runs tasks.

use std::path::PathBuf;
use std::time::Duration;

use aether_agent::{AgentClient, AgentConfig, MetricsCollector, identity, local_node_info};
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

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
    match config.tls_ca_path.clone() {
        #[cfg(feature = "tls")]
        Some(ca_path) => {
            let connector = match config.client_identity() {
                Some((cert, key)) => {
                    aether_agent::tls::connector_with_client_cert(&ca_path, &cert, &key)?
                }
                None => aether_agent::tls::connector(&ca_path)?,
            };
            let mut client = aether_agent::tls::connect(
                &config.controller,
                &config.server_name(),
                &connector,
                info,
                config.auth_token.clone(),
            )
            .await?;
            if let Some(budget) = config.storage_budget_bytes {
                client = client.with_storage_budget(budget);
            }
            if let Some(tasks) = config.max_concurrent_tasks {
                client = client.with_max_concurrent_tasks(tasks);
            }
            client.run(MetricsCollector::new(), heartbeat).await?;
        }
        #[cfg(not(feature = "tls"))]
        Some(_) => anyhow::bail!("this build has no TLS support; rebuild with --features tls"),
        None => {
            let mut client = AgentClient::connect_with_token(
                &config.controller,
                info,
                config.auth_token.clone(),
            )
            .await?;
            if let Some(budget) = config.storage_budget_bytes {
                client = client.with_storage_budget(budget);
            }
            if let Some(tasks) = config.max_concurrent_tasks {
                client = client.with_max_concurrent_tasks(tasks);
            }

            // Extra connections are opened before the run loop starts, so the
            // first large transfer already has them.
            let _channels = client
                .open_data_channels(
                    &config.controller,
                    config.data_channels,
                    config.auth_token.clone(),
                )
                .await?;

            client.run(MetricsCollector::new(), heartbeat).await?;
        }
    }
    Ok(())
}
