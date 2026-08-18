//! Controller entry point: serves agent registrations, heartbeats, and results.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use aether_controller::{
    ClientGateway, Controller, ControllerConfig, DEFAULT_CHECK_INTERVAL, MeshState,
    NetworkTransport, bind, bind_clients, health, run_dispatcher, serve, serve_clients,
};
use aether_scheduler::AdvancedScheduler;
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

    /// Address the client API listens on.
    #[arg(long)]
    client_listen: Option<SocketAddr>,

    /// Runs without the client API.
    #[arg(long)]
    no_client_api: bool,

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

        /// Also write a CA and sign the certificate with it, so agents can be
        /// issued client certificates for mutual TLS.
        #[arg(long)]
        with_ca: bool,

        #[arg(long, default_value = "ca.pem")]
        ca_cert_path: PathBuf,

        #[arg(long, default_value = "ca.key")]
        ca_key_path: PathBuf,
    },
    /// Issues a client certificate for one agent, signed by the CA.
    #[cfg(feature = "tls")]
    IssueClientCert {
        /// Name to put in the certificate, e.g. the node's hostname.
        #[arg(long)]
        name: String,

        #[arg(long, default_value = "ca.pem")]
        ca_cert_path: PathBuf,

        #[arg(long, default_value = "ca.key")]
        ca_key_path: PathBuf,

        #[arg(long, default_value = "client.pem")]
        cert_path: PathBuf,

        #[arg(long, default_value = "client.key")]
        key_path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    #[cfg(feature = "tls")]
    match args.command {
        Some(Command::GenerateCert {
            host,
            cert_path,
            key_path,
            with_ca,
            ca_cert_path,
            ca_key_path,
        }) => {
            let config = aether_controller::TlsConfig::new(cert_path, key_path);
            if with_ca {
                aether_controller::tls::generate_ca_and_cert(
                    &ca_cert_path,
                    &ca_key_path,
                    &config,
                    host,
                )?;
            } else {
                aether_controller::tls::generate_self_signed(&config, host)?;
            }
            return Ok(());
        }
        Some(Command::IssueClientCert {
            name,
            ca_cert_path,
            ca_key_path,
            cert_path,
            key_path,
        }) => {
            aether_controller::tls::issue_client_cert(
                &ca_cert_path,
                &ca_key_path,
                &cert_path,
                &key_path,
                vec![name],
            )?;
            return Ok(());
        }
        None => {}
    }

    let mut config = ControllerConfig::load_or_default(args.config.as_deref())?;
    if let Some(listen) = args.listen {
        config.listen = listen;
    }
    if args.client_listen.is_some() {
        config.client_listen = args.client_listen;
    }
    if args.no_client_api {
        config.client_listen = None;
    }
    if let Some(timeout) = args.heartbeat_timeout_secs {
        config.heartbeat_timeout_secs = timeout;
    }
    if args.auth_token.is_some() {
        config.auth_token = args.auth_token.clone();
    }

    // Agents are told the eviction window at registration, so an idle one can
    // slow its heartbeats down to the edge of it and no further.
    let state = MeshState::new().with_heartbeat_timeout(config.heartbeat_timeout());
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
    if config.probe_interval_secs > 0 {
        tokio::spawn(aether_controller::probe::monitor(
            state.clone(),
            Duration::from_secs(config.probe_interval_secs),
            config.probe_bytes,
        ));
    }
    if config.metrics_interval_secs > 0 {
        tokio::spawn(log_metrics(
            state.clone(),
            Duration::from_secs(config.metrics_interval_secs),
        ));
    }

    // The client API is what non-Rust callers use: publish data, submit tasks.
    if let Some(client_addr) = config.client_listen {
        let mut controller = Controller::new(
            AdvancedScheduler::new(state.catalog.clone()),
            NetworkTransport::new(state.connections.clone()),
            state.catalog.clone(),
        );
        if let Some(cache) = config.result_cache() {
            info!(
                entries = config.result_cache_entries,
                ttl_secs = config.result_cache_ttl_secs,
                "result cache enabled"
            );
            controller = controller.with_result_cache(cache);
        }
        let (gateway, commands) = ClientGateway::new(64);
        tokio::spawn(run_dispatcher(controller, state.clone(), commands));

        let (client_listener, client_addr) = bind_clients(client_addr).await?;
        info!(%client_addr, tls = config.tls_paths().is_some(), "client API listening");

        match config.tls_paths() {
            #[cfg(feature = "tls")]
            Some((cert_path, key_path)) => {
                let tls = tls_config(&config, cert_path, key_path);
                let acceptor = aether_controller::tls::acceptor(&tls)?;
                tokio::spawn(aether_controller::serve_clients_tls(
                    client_listener,
                    gateway,
                    config.security(),
                    acceptor,
                ));
            }
            #[cfg(not(feature = "tls"))]
            Some(_) => anyhow::bail!("this build has no TLS support; rebuild with --features tls"),
            None => {
                tokio::spawn(serve_clients(client_listener, gateway, config.security()));
            }
        }
    }

    match config.tls_paths() {
        #[cfg(feature = "tls")]
        Some((cert_path, key_path)) => {
            let tls = tls_config(&config, cert_path, key_path);
            let acceptor = aether_controller::tls::acceptor(&tls)?;
            aether_controller::serve_tls(listener, state, config.security(), acceptor).await?;
        }
        #[cfg(not(feature = "tls"))]
        Some(_) => anyhow::bail!("this build has no TLS support; rebuild with --features tls"),
        None => serve(listener, state, config.security()).await?,
    }
    Ok(())
}

/// Builds the TLS config, turning on mutual TLS when a client CA is set.
#[cfg(feature = "tls")]
fn tls_config(
    config: &ControllerConfig,
    cert_path: PathBuf,
    key_path: PathBuf,
) -> aether_controller::TlsConfig {
    let tls = aether_controller::TlsConfig::new(cert_path, key_path);
    match config.client_ca_path() {
        Some(client_ca) => tls.with_client_ca(client_ca),
        None => tls,
    }
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
