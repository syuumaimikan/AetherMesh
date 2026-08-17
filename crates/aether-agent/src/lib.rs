//! Worker node: registers with the controller, reports metrics, runs tasks.

pub mod client;
pub mod config;
pub mod executor;
pub mod identity;
pub mod metrics;

#[cfg(feature = "tls")]
pub mod tls;

pub use client::{AgentClient, ClientError};
pub use config::{AgentConfig, ConfigError};
pub use executor::{MAX_CPU_ITERATIONS, execute};
pub use identity::{default_identity_path, load_or_create};
pub use metrics::{MIN_SAMPLE_INTERVAL, MetricsCollector, hostname, local_node_info};
