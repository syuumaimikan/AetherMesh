//! Control plane: node registry, task queue, dispatch, health monitoring.

pub mod autoscale;
pub mod cache;
pub mod checkpoint;
pub mod client;
pub mod config;
pub mod connection;
pub mod connections;
pub mod dispatch;
pub mod flow;
pub mod health;
pub mod network;
pub mod observability;
pub mod probe;
pub mod queue;
pub mod registry;
pub mod security;
pub mod server;
pub mod sim;
pub mod state;
pub mod telemetry;

#[cfg(feature = "tls")]
pub mod tls;

pub use autoscale::{Autoscaler, Decision, Policy, Signals, Target};
pub use cache::{ResultCache, WorkKey};
pub use checkpoint::{CheckpointError, Journal, Record};
pub use client::{
    CLIENT_PROTOCOL_VERSION, ClientGateway, ClientRequest, ClientResponse, bind_clients,
    run_dispatcher, run_dispatcher_with, serve_clients,
};
pub use config::{ConfigError, ControllerConfig};
pub use connection::{Connection, ConnectionError, Finished, Published, Stats, SubmitOptions};
pub use connections::Connections;
pub use dispatch::{Controller, DispatchError, RetryPolicy, TaskTransport};
pub use flow::{FlowError, FlowResult, run_workflow, run_workflow_resumable};
pub use health::{DEFAULT_CHECK_INTERVAL, DEFAULT_HEARTBEAT_TIMEOUT, evict_stale_nodes};
pub use network::{DEFAULT_TASK_TIMEOUT, NetworkTransport};
pub use observability::{MeshMetrics, MetricsSnapshot};
pub use probe::{DEFAULT_PROBE_BYTES, DEFAULT_PROBE_INTERVAL, LinkMeasurement};
pub use queue::{Admitted, DEFAULT_AGING, Queue, Queued, Rejection};
pub use registry::{NodeEntry, NodeRegistry, RegistryError};
pub use security::{AuthError, SecurityConfig};
pub use server::{bind, serve};
pub use sim::SimulatedMesh;
pub use state::{MeshState, SharedRegistry, shared_registry};
pub use telemetry::{bind_metrics, serve_metrics};

#[cfg(feature = "tls")]
pub use tls::{TlsConfig, TlsError, serve_clients_tls, serve_tls};
