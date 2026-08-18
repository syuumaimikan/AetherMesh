//! Terminal dashboard for AetherMesh.
//!
//! Split into a library so the parts worth testing — the protocol client, the
//! state transitions, the rendering — can be tested without a terminal or a
//! person in front of one.

pub mod app;
pub mod client;
pub mod ui;
