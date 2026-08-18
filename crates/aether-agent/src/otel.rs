//! Exporting this node's spans, and joining the trace that sent the work.
//!
//! Deliberately a near-copy of the controller's module rather than a shared
//! one. What it contains is how *this binary* composes its subscriber, which
//! is a per-binary decision; the part that genuinely has to agree between the
//! two — the `traceparent` on the wire — is a W3C standard and lives in the
//! protocol. Forty lines of builder are cheaper than a crate that exists only
//! to hold them.

use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// Name this process reports itself as.
pub const SERVICE_NAME: &str = "aether-agent";

/// How long the exporter waits on a collector before giving up on a batch.
///
/// A node must not run work more slowly because the thing watching it is
/// unwell.
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

/// Trace verbosity, from `AETHERMESH_TRACE`. Separate from `RUST_LOG` so
/// quietening the terminal does not silently stop the export.
fn trace_filter() -> EnvFilter {
    EnvFilter::try_from_env("AETHERMESH_TRACE").unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Sends spans to an OTLP/HTTP collector at `endpoint`, alongside the usual
/// logs.
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

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let layer = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer(SERVICE_NAME))
        .with_filter(trace_filter());
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(log_filter()))
        .with(layer)
        .init();

    Ok(Guard { provider })
}

/// Attaches `span` to the trace the controller sent, if it sent one.
///
/// The parent is set rather than a link made: this node running the task is
/// part of that request, not a separate thing that happens to be related. A
/// missing or malformed header leaves the span as its own root, because a task
/// that ran is worth a trace even when nobody can say what asked for it.
pub fn adopt(span: &tracing::Span, traceparent: Option<&str>) {
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let Some(traceparent) = traceparent else {
        return;
    };

    let carrier =
        std::collections::HashMap::from([("traceparent".to_string(), traceparent.to_string())]);
    let context =
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&carrier));

    // A header this node cannot make sense of is not a reason to refuse the
    // work, so the span stays a root and the task runs.
    if let Err(error) = span.set_parent(context) {
        tracing::debug!(%error, traceparent, "could not join the controller's trace");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_header_leaves_the_span_alone() {
        // Nothing to assert on the span itself without a subscriber; what is
        // being pinned is that this does not panic on the ordinary case of a
        // controller that is not tracing.
        adopt(&tracing::Span::none(), None);
    }
}
