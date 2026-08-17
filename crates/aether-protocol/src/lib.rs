//! Wire messages exchanged between agents and the controller.
//!
//! Holds message shapes, their binary encoding, and async framing — but no
//! transport: sockets are opened by the agent and controller crates.

pub mod codec;
pub mod message;
pub mod net;

pub use codec::{CodecError, decode, encode};
pub use message::{Message, PROTOCOL_VERSION};
pub use net::{MAX_FRAME_BYTES, NetError, read_message, write_message};
