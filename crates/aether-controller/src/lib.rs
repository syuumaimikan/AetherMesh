//! Control plane: node registry, task queue, dispatch, health monitoring.

pub mod config;
pub mod connections;
pub mod dispatch;
pub mod health;
pub mod network;
pub mod observability;
pub mod registry;
pub mod security;
pub mod server;
pub mod sim;
pub mod state;

#[cfg(feature = "tls")]
pub mod tls;

pub use config::{ConfigError, ControllerConfig};
pub use connections::Connections;
pub use dispatch::{Controller, DispatchError, RetryPolicy, TaskTransport};
pub use health::{DEFAULT_CHECK_INTERVAL, DEFAULT_HEARTBEAT_TIMEOUT, evict_stale_nodes};
pub use network::{DEFAULT_TASK_TIMEOUT, NetworkTransport};
pub use observability::{MeshMetrics, MetricsSnapshot};
pub use registry::{NodeEntry, NodeRegistry, RegistryError};
pub use security::{AuthError, SecurityConfig};
pub use server::{bind, serve};
pub use sim::SimulatedMesh;
pub use state::{MeshState, SharedRegistry, shared_registry};

#[cfg(feature = "tls")]
pub use tls::{TlsConfig, TlsError, serve_tls};
