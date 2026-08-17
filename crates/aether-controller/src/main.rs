//! Controller entry point: serves agent registrations, heartbeats, and results.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use aether_controller::{ControllerConfig, DEFAULT_CHECK_INTERVAL, MeshState, bind, health, serve};
use clap::{Parser, Subcommand};
use tracing::info;

#[derive(Parser)]
#[command(name = "aether-controller", about = "AetherMesh control plane")]
struct Args {
    /// TOML configuration file. Flags below override its values.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Address to listen on.
    #[arg(long)]
    listen: Option<SocketAddr>,

    /// Seconds without a heartbeat before a node is evicted.
    #[arg(long)]
    heartbeat_timeout_secs: Option<u64>,

    /// Token every agent must present.
    #[arg(long, env = "AETHERMESH_TOKEN")]
    auth_token: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Writes a self-signed certificate and key for local or lab use.
    #[cfg(feature = "tls")]
    GenerateCert {
        /// Names the certificate is valid for.
        #[arg(long, default_values_t = ["localhost".to_string()])]
        host: Vec<String>,

        #[arg(long, default_value = "cert.pem")]
        cert_path: PathBuf,

        #[arg(long, default_value = "key.pem")]
        key_path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    #[cfg(feature = "tls")]
    if let Some(Command::GenerateCert {
        host,
        cert_path,
        key_path,
    }) = args.command
    {
        let config = aether_controller::TlsConfig::new(cert_path, key_path);
        aether_controller::tls::generate_self_signed(&config, host)?;
        return Ok(());
    }

    let mut config = ControllerConfig::load_or_default(args.config.as_deref())?;
    if let Some(listen) = args.listen {
        config.listen = listen;
    }
    if let Some(timeout) = args.heartbeat_timeout_secs {
        config.heartbeat_timeout_secs = timeout;
    }
    if args.auth_token.is_some() {
        config.auth_token = args.auth_token.clone();
    }

    let state = MeshState::new();
    let (listener, addr) = bind(config.listen).await?;
    info!(
        %addr,
        auth = config.auth_token.is_some(),
        tls = config.tls_paths().is_some(),
        "controller listening"
    );

    tokio::spawn(health::monitor(
        state.clone(),
        config.heartbeat_timeout(),
        DEFAULT_CHECK_INTERVAL,
    ));
    if config.metrics_interval_secs > 0 {
        tokio::spawn(log_metrics(
            state.clone(),
            Duration::from_secs(config.metrics_interval_secs),
        ));
    }

    match config.tls_paths() {
        #[cfg(feature = "tls")]
        Some((cert_path, key_path)) => {
            let tls = aether_controller::TlsConfig::new(cert_path, key_path);
            let acceptor = aether_controller::tls::acceptor(&tls)?;
            aether_controller::serve_tls(listener, state, config.security(), acceptor).await?;
        }
        #[cfg(not(feature = "tls"))]
        Some(_) => anyhow::bail!("this build has no TLS support; rebuild with --features tls"),
        None => serve(listener, state, config.security()).await?,
    }
    Ok(())
}

/// Logs the counters periodically, so a run leaves a usable trail.
async fn log_metrics(state: MeshState, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let snapshot = state.metrics.snapshot();
        info!(
            nodes = state
                .registry
                .lock()
                .expect("registry mutex poisoned")
                .len(),
            registered = snapshot.nodes_registered,
            rejected = snapshot.registrations_rejected,
            evicted = snapshot.nodes_evicted,
            heartbeats = snapshot.heartbeats,
            tasks_ok = snapshot.tasks_completed,
            tasks_failed = snapshot.tasks_failed,
            "mesh metrics"
        );
    }
}
