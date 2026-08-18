//! Terminal dashboard for AetherMesh.
//!
//! Split into a library so the parts worth testing — the protocol client, the
//! state transitions, the rendering — can be tested without a terminal or a
//! person in front of one.

pub mod app;
pub mod ui;

/// The protocol client lives in `aether-controller`, next to the request and
/// response types it speaks, so a dashboard that has drifted from the
/// controller is a compile error rather than a wrong number on a screen.
pub use aether_controller::connection::{Connection, ConnectionError, SubmitOptions};
