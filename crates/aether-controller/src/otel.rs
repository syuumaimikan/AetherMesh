//! Exporting traces to an OpenTelemetry collector.
//!
//! `/metrics` already answers "how much" — bytes moved, tasks run, queue depth.
//! This answers "what happened to *this* task": which node was chosen, how long
//! its inputs took to send, how long it then waited. Counters cannot answer
//! that, because by the time a counter has moved the task it describes is
//! anonymous.
//!
//! Off unless the operator names an endpoint, and behind a feature so a build
//! that will never point at a collector does not compile an exporter.

use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// Name this process reports itself as.
pub const SERVICE_NAME: &str = "aether-controller";

/// How long the exporter waits on a collector before giving up on a batch.
///
/// Short on purpose. A controller must not slow down because the thing
/// watching it is unwell.
pub const EXPORT_TIMEOUT: Duration = Duration::from_secs(3);

/// Tracing could not be set up.
#[derive(Debug, thiserror::Error)]
pub enum OtelError {
    #[error("building the OTLP exporter for {endpoint}: {source}")]
    Exporter {
        endpoint: String,
        #[source]
        source: opentelemetry_otlp::ExporterBuildError,
    },
}

/// Flushes what has not been exported yet when the process ends.
///
/// Without this the last spans of a run — which are usually the interesting
/// ones, because something just went wrong — are lost at exit.
pub struct Guard {
    provider: SdkTracerProvider,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Err(error) = self.provider.shutdown() {
            eprintln!("could not flush traces: {error}");
        }
    }
}

/// Console verbosity, from `RUST_LOG`.
fn log_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Trace verbosity, from `AETHERMESH_TRACE`. Separate from `RUST_LOG` on
/// purpose: quietening the terminal should not stop the export.
fn trace_filter() -> EnvFilter {
    EnvFilter::try_from_env("AETHERMESH_TRACE").unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Sends spans to an OTLP/HTTP collector at `endpoint`, alongside the usual
/// logs.
///
/// `endpoint` is the collector's trace URL, e.g.
/// `http://127.0.0.1:4318/v1/traces`.
pub fn init(endpoint: &str) -> Result<Guard, OtelError> {
    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_protocol(Protocol::HttpJson)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|source| OtelError::Exporter {
            endpoint: endpoint.to_string(),
            source,
        })?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(SERVICE_NAME)
                .with_attribute(opentelemetry::KeyValue::new(
                    "service.version",
                    env!("CARGO_PKG_VERSION"),
                ))
                .build(),
        )
        .build();

    // Two filters, not one. `RUST_LOG` says how noisy the console should be,
    // and someone who set it to `warn` to quieten a terminal has not asked for
    // their traces to stop — which is what one shared filter would do, because
    // an instrumented span is disabled before any layer sees it.
    let layer = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer(SERVICE_NAME))
        .with_filter(trace_filter());
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(log_filter()))
        .with(layer)
        .init();

    Ok(Guard { provider })
}
