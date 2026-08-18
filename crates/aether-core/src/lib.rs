//! Shared types for AetherMesh (nodes, tasks, data, metrics).
//!
//! This crate holds data only: no networking, no scheduling policy.

pub mod chunk;
pub mod compress;
pub mod data;
pub mod id;
pub mod labels;
pub mod node;
pub mod store;
pub mod task;

pub use chunk::{ChunkAssembler, ChunkError, ChunkManifest, DEFAULT_CHUNK_SIZE};
pub use compress::{Codec, CompressError, CompressionPolicy};
pub use data::{DataDescriptor, DataId};
pub use id::{NodeId, TaskId};
pub use labels::{Constraint, ConstraintParseError, Labels};
pub use node::{NodeInfo, NodeMetrics};
pub use store::{DataStore, DataStoreError};
pub use task::{Task, TaskOutcome, TaskResult};
