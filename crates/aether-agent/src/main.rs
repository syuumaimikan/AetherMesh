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
    info!(
        %node_id,
        hostname = %info.hostname,
        cores = info.cpu_cores,
        tls = config.tls_ca_path.is_some(),
        "starting agent"
    );

    let heartbeat = Duration::from_secs(config.heartbeat_secs);
    match config.tls_ca_path.clone() {
        #[cfg(feature = "tls")]
        Some(ca_path) => {
            let connector = aether_agent::tls::connector(&ca_path)?;
            let mut client = aether_agent::tls::connect(
                &config.controller,
                &config.server_name(),
                &connector,
                info,
                config.auth_token.clone(),
            )
            .await?;
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
            client.run(MetricsCollector::new(), heartbeat).await?;
        }
    }
    Ok(())
}
