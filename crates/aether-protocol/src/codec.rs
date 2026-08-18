//! Binary encoding of protocol messages (bincode, little overhead on the wire).

use bincode::config::{Configuration, standard};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Encoding or decoding failure.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("failed to encode message: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("failed to decode message: {0}")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("trailing bytes after message: {0} unused")]
    TrailingBytes(usize),
}

fn config() -> Configuration {
    standard()
}

/// Serializes a message to bytes.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    Ok(bincode::serde::encode_to_vec(value, config())?)
}

/// Deserializes a message, rejecting input with leftover bytes.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    let (value, used) = bincode::serde::decode_from_slice(bytes, config())?;
    let trailing = bytes.len() - used;
    if trailing > 0 {
        return Err(CodecError::TrailingBytes(trailing));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aether_core::{NodeId, NodeInfo, NodeMetrics, Task, TaskResult};

    use super::*;
    use crate::message::Message;

    fn round_trip(message: Message) {
        let bytes = encode(&message).unwrap();
        assert_eq!(decode::<Message>(&bytes).unwrap(), message);
    }

    #[test]
    fn every_variant_round_trips() {
        let node_id = NodeId::generate();
        let mut info = NodeInfo::new(node_id, "desktop", "127.0.0.1:7000", 16);
        info.update_metrics(NodeMetrics::new(0.4, 0.7, 1024));
        let task = Task::new("hash", b"payload".to_vec());

        round_trip(Message::register(info));
        round_trip(Message::RegisterAccepted {
            node_id,
            channel_token: None,
            heartbeat_timeout_secs: 0,
        });
        round_trip(Message::Heartbeat {
            node_id,
            metrics: NodeMetrics::new(0.1, 0.2, 2048),
        });
        round_trip(Message::SubmitTask { task: task.clone() });
        round_trip(Message::TaskAssignment {
            node_id,
            task: task.clone(),
        });
        round_trip(Message::TaskCompleted {
            result: TaskResult::success(task.id, node_id, vec![1, 2], Duration::from_millis(3)),
        });
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = encode(&Message::RegisterAccepted {
            node_id: NodeId::generate(),
            channel_token: None,
            heartbeat_timeout_secs: 0,
        })
        .unwrap();
        bytes.push(0xff);
        assert!(matches!(
            decode::<Message>(&bytes),
            Err(CodecError::TrailingBytes(1))
        ));
    }

    #[test]
    fn truncated_input_is_an_error() {
        assert!(decode::<Message>(&[]).is_err());
    }
}
