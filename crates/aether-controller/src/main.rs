//! Controller entry point: serves agent registrations, heartbeats, and results.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use aether_controller::{
    ClientGateway, Controller, ControllerConfig, DEFAULT_CHECK_INTERVAL, MeshState,
    NetworkTransport, bind, bind_clients, health, serve, serve_clients,
};
use aether_scheduler::AdvancedScheduler;
use clap::{Parser, Subcommand};
use tracing::info;
#[cfg(not(feature = "otel"))]
use tracing::warn;

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

    /// Address to serve `/metrics` (Prometheus) and `/healthz` on.
    #[arg(long)]
    metrics_listen: Option<SocketAddr>,

    /// Seconds without a heartbeat before a node is evicted.
    #[arg(long)]
    heartbeat_timeout_secs: Option<u64>,

    /// Token every agent must present.
    #[arg(long, env = "AETHERMESH_TOKEN")]
    auth_token: Option<String>,

    /// OTLP/HTTP endpoint to send traces to, e.g.
    /// `http://127.0.0.1:4318/v1/traces`. Needs a build with `--features otel`.
    #[arg(long)]
    otlp_endpoint: Option<String>,

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
/// The returned value has to outlive the process's work: dropping it flushes
/// whatever has not been exported, and the last spans before an exit are
/// usually the ones somebody wanted.
fn start_tracing(endpoint: Option<&str>) -> anyhow::Result<Option<impl Sized>> {
    #[cfg(feature = "otel")]
    if let Some(endpoint) = endpoint {
        let guard = aether_controller::otel::init(endpoint)?;
        info!(endpoint, "exporting traces");
        return Ok(Some(guard));
    }

    #[cfg(not(feature = "otel"))]
    if endpoint.is_some() {
        init_console_logging();
        warn!(
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

    let mut config = ControllerConfig::load_or_default(args.config.as_deref())?;
    if args.otlp_endpoint.is_some() {
        config.otlp_endpoint = args.otlp_endpoint.clone();
    }
    let _tracing = start_tracing(config.otlp_endpoint.as_deref())?;

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
    if args.metrics_listen.is_some() {
        config.metrics_listen = args.metrics_listen;
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
    if config.autoscale_interval_secs > 0 {
        info!(
            interval_secs = config.autoscale_interval_secs,
            target = ?config.autoscale.target,
            min = config.autoscale.min_nodes,
            max = config.autoscale.max_nodes,
            "autoscaler watching (recommends only; it does not provision)"
        );
        tokio::spawn(aether_controller::autoscale::monitor(
            state.clone(),
            config.autoscale,
            Duration::from_secs(config.autoscale_interval_secs),
        ));
    }
    if let Some(metrics_addr) = config.metrics_listen {
        let (listener, metrics_addr) = aether_controller::bind_metrics(metrics_addr).await?;
        info!(%metrics_addr, "serving /metrics and /healthz");
        let state = state.clone();
        tokio::spawn(async move {
            aether_controller::telemetry::report_metrics_exit(
                aether_controller::serve_metrics(listener, state).await,
            );
        });
    }

    // The client API is what non-Rust callers use: publish data, submit tasks.
    if let Some(client_addr) = config.client_listen {
        let mut controller = Controller::new(
            AdvancedScheduler::new(state.catalog.clone()).with_weights(config.scheduler_weights),
            NetworkTransport::new(state.connections.clone()),
            state.catalog.clone(),
        )
        // Without this the client API and /metrics report zeroes: the counters
        // would live on a Controller that only the dispatcher task can see.
        .with_traffic_stats(state.traffic.clone());
        if let Some(cache) = config.result_cache() {
            info!(
                entries = config.result_cache_entries,
                ttl_secs = config.result_cache_ttl_secs,
                "result cache enabled"
            );
            controller = controller.with_result_cache(cache);
        }
        if let Some(path) = &config.checkpoint_path {
            let journal = aether_controller::Journal::open(path)?;
            info!(path = %journal.path().display(), "recording finished workflow steps");
            controller = controller.with_checkpoint(std::sync::Arc::new(journal));
        }
        info!(
            cpu = config.scheduler_weights.cpu,
            transfer = config.scheduler_weights.transfer,
            latency = config.scheduler_weights.latency,
            locality = config.scheduler_weights.locality,
            "placement weights"
        );
        let (gateway, commands) = ClientGateway::new(64);
        let queue = config.task_queue();
        info!(
            aging_secs = config.queue_aging_secs,
            max_size = config.max_queue_size,
            timeout_secs = config.queue_timeout_secs,
            rejection = ?config.queue_rejection,
            "task queue ready (higher priority first, FIFO within a level)"
        );
        tokio::spawn(aether_controller::run_dispatcher_with(
            controller,
            state.clone(),
            commands,
            queue,
        ));

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
            nodes = aether_core::lock(&state.registry).len(),
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
