//! Length-prefixed framing over any async byte stream.
//!
//! Frame layout: `u32` big-endian payload length, then the bincode-encoded
//! [`Message`]. Transport choice stays outside this module, so a QUIC stream can
//! replace a TCP one without touching the wire format.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::codec::{CodecError, decode, encode};
use crate::message::Message;

/// Largest frame accepted, to bound memory on hostile or corrupt input.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Framing failure.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("frame of {0} bytes exceeds the {MAX_FRAME_BYTES} byte limit")]
    FrameTooLarge(usize),
}

/// Writes one message and flushes. Returns the number of bytes written.
pub async fn write_message<W>(writer: &mut W, message: &Message) -> Result<usize, NetError>
where
    W: AsyncWrite + Unpin,
{
    let payload = encode(message)?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err(NetError::FrameTooLarge(payload.len()));
    }

    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(payload.len() + 4)
}

/// Reads one message. Returns `Err(NetError::Io)` with `UnexpectedEof` when the
/// peer closed the connection cleanly between frames.
pub async fn read_message<R>(reader: &mut R) -> Result<Message, NetError>
where
    R: AsyncRead + Unpin,
{
    let mut length = [0u8; 4];
    reader.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(NetError::FrameTooLarge(length));
    }

    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload).await?;
    Ok(decode(&payload)?)
}

#[cfg(test)]
mod tests {
    use aether_core::{NodeId, NodeInfo, NodeMetrics};

    use super::*;

    #[tokio::test]
    async fn messages_round_trip_over_a_byte_stream() {
        let node_id = NodeId::generate();
        let info = NodeInfo::new(node_id, "rpi4", "10.0.0.2:7000", 4);
        let sent = [
            Message::register(info),
            Message::Heartbeat {
                node_id,
                metrics: NodeMetrics::new(0.25, 0.5, 4096),
            },
        ];

        let mut buffer = Vec::new();
        for message in &sent {
            write_message(&mut buffer, message).await.unwrap();
        }

        let mut reader = buffer.as_slice();
        for message in &sent {
            assert_eq!(&read_message(&mut reader).await.unwrap(), message);
        }
    }

    #[tokio::test]
    async fn a_closed_stream_reports_eof() {
        let mut reader: &[u8] = &[];
        let error = read_message(&mut reader).await.unwrap_err();
        assert!(matches!(error, NetError::Io(_)));
    }

    #[tokio::test]
    async fn oversized_length_prefix_is_rejected_before_allocating() {
        let mut frame = (MAX_FRAME_BYTES as u32 + 1).to_be_bytes().to_vec();
        frame.extend_from_slice(&[0u8; 8]);

        let mut reader = frame.as_slice();
        assert!(matches!(
            read_message(&mut reader).await,
            Err(NetError::FrameTooLarge(_))
        ));
    }
}
