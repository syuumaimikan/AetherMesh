//! Deciding whether compressing a transfer is worth it, and doing it.
//!
//! The rule is deliberately simple: compress when the payload is big enough to
//! matter, the link is slow enough that bytes cost more than CPU, and the data
//! actually shrinks. Anything smarter belongs in a later phase.

use serde::{Deserialize, Serialize};

/// How a payload was encoded for transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Codec {
    /// Sent as-is.
    #[default]
    None,
    /// LZ4 block format with the original length prepended.
    Lz4,
}

/// Compressed data could not be restored.
#[derive(Debug, thiserror::Error)]
pub enum CompressError {
    #[error("lz4 decompression failed: {0}")]
    Lz4(#[from] lz4_flex::block::DecompressError),
}

/// Encodes `bytes` with `codec`.
pub fn compress(codec: Codec, bytes: &[u8]) -> Vec<u8> {
    match codec {
        Codec::None => bytes.to_vec(),
        Codec::Lz4 => lz4_flex::compress_prepend_size(bytes),
    }
}

/// Restores a payload encoded with `codec`.
pub fn decompress(codec: Codec, bytes: &[u8]) -> Result<Vec<u8>, CompressError> {
    match codec {
        Codec::None => Ok(bytes.to_vec()),
        Codec::Lz4 => Ok(lz4_flex::decompress_size_prepended(bytes)?),
    }
}

/// When to compress a transfer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompressionPolicy {
    /// Payloads smaller than this are never compressed.
    pub min_size_bytes: usize,
    /// Links at least this fast are left uncompressed: the CPU costs more than
    /// the bytes saved.
    pub fast_link_bytes_per_sec: u64,
    /// Fraction of the payload that must be saved for the result to be used.
    pub min_saving: f32,
}

impl Default for CompressionPolicy {
    fn default() -> Self {
        Self {
            min_size_bytes: 4 * 1024,
            // ~800 Mbps: a local wired link.
            fast_link_bytes_per_sec: 100 * 1024 * 1024,
            min_saving: 0.05,
        }
    }
}

impl CompressionPolicy {
    /// Never compresses anything.
    pub fn disabled() -> Self {
        Self {
            min_size_bytes: usize::MAX,
            ..Self::default()
        }
    }

    /// Codec to try for a payload of `size` bytes over a link of the given
    /// speed. `None` bandwidth means unknown, which is treated as a slow link.
    pub fn choose(&self, size: usize, bandwidth_bytes_per_sec: Option<u64>) -> Codec {
        if size < self.min_size_bytes {
            return Codec::None;
        }
        match bandwidth_bytes_per_sec {
            Some(bandwidth) if bandwidth >= self.fast_link_bytes_per_sec => Codec::None,
            _ => Codec::Lz4,
        }
    }

    /// Encodes a payload for transfer, keeping the compressed form only when it
    /// saves at least [`min_saving`](Self::min_saving) of the original size.
    pub fn encode(&self, bytes: &[u8], bandwidth_bytes_per_sec: Option<u64>) -> (Codec, Vec<u8>) {
        let codec = self.choose(bytes.len(), bandwidth_bytes_per_sec);
        if codec == Codec::None {
            return (Codec::None, bytes.to_vec());
        }

        let compressed = compress(codec, bytes);
        let saved = bytes.len().saturating_sub(compressed.len()) as f32 / bytes.len() as f32;
        if saved >= self.min_saving {
            (codec, compressed)
        } else {
            (Codec::None, bytes.to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compressible(len: usize) -> Vec<u8> {
        vec![0xab; len]
    }

    /// Bytes with no exploitable structure, so LZ4 cannot shrink them.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    #[test]
    fn lz4_round_trips() {
        let bytes = compressible(10_000);
        let encoded = compress(Codec::Lz4, &bytes);

        assert!(encoded.len() < bytes.len());
        assert_eq!(decompress(Codec::Lz4, &encoded).unwrap(), bytes);
    }

    #[test]
    fn the_none_codec_is_a_copy() {
        let bytes = compressible(10);
        assert_eq!(compress(Codec::None, &bytes), bytes);
        assert_eq!(decompress(Codec::None, &bytes).unwrap(), bytes);
    }

    #[test]
    fn corrupt_compressed_input_is_an_error() {
        assert!(decompress(Codec::Lz4, &[0xff; 8]).is_err());
    }

    #[test]
    fn small_payloads_are_never_compressed() {
        let policy = CompressionPolicy::default();
        assert_eq!(policy.choose(1024, None), Codec::None);
        assert_eq!(policy.choose(policy.min_size_bytes, None), Codec::Lz4);
    }

    #[test]
    fn fast_links_are_left_uncompressed() {
        let policy = CompressionPolicy::default();
        let size = 1024 * 1024;

        assert_eq!(policy.choose(size, Some(1024 * 1024 * 1024)), Codec::None);
        assert_eq!(policy.choose(size, Some(1024 * 1024)), Codec::Lz4);
        // Unknown bandwidth is assumed slow.
        assert_eq!(policy.choose(size, None), Codec::Lz4);
    }

    #[test]
    fn encoding_keeps_compression_only_when_it_helps() {
        let policy = CompressionPolicy::default();

        let (codec, encoded) = policy.encode(&compressible(64 * 1024), None);
        assert_eq!(codec, Codec::Lz4);
        assert!(encoded.len() < 64 * 1024);

        let noise = incompressible(64 * 1024);
        let (codec, encoded) = policy.encode(&noise, None);
        assert_eq!(codec, Codec::None);
        assert_eq!(encoded, noise);
    }

    #[test]
    fn a_disabled_policy_sends_everything_raw() {
        let policy = CompressionPolicy::disabled();
        let bytes = compressible(1024 * 1024);

        let (codec, encoded) = policy.encode(&bytes, None);
        assert_eq!(codec, Codec::None);
        assert_eq!(encoded, bytes);
    }
}
