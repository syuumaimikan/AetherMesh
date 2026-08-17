//! Identity and size of a dataset a task may need.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Length of a data identifier in bytes (a 256-bit digest).
pub const DATA_ID_LEN: usize = 32;

/// Identifies a piece of data by its content hash.
///
/// Phase 12 fills these in with BLAKE3 digests; here they are opaque bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DataId([u8; DATA_ID_LEN]);

impl DataId {
    /// Content address of `bytes`: their BLAKE3 digest.
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    pub const fn from_bytes(bytes: [u8; DATA_ID_LEN]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; DATA_ID_LEN] {
        &self.0
    }
}

impl fmt::Display for DataId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Why a hex string is not a valid [`DataId`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DataIdParseError {
    #[error("expected a 64 character hex string, got {0} characters")]
    WrongLength(usize),
    #[error("invalid hex character at byte {0}")]
    InvalidHex(usize),
}

impl FromStr for DataId {
    type Err = DataIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != DATA_ID_LEN * 2 {
            return Err(DataIdParseError::WrongLength(s.len()));
        }

        let mut bytes = [0u8; DATA_ID_LEN];
        for (index, byte) in bytes.iter_mut().enumerate() {
            let pair = &s[index * 2..index * 2 + 2];
            *byte =
                u8::from_str_radix(pair, 16).map_err(|_| DataIdParseError::InvalidHex(index))?;
        }
        Ok(Self(bytes))
    }
}

/// What the mesh knows about a dataset without holding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDescriptor {
    pub id: DataId,
    pub size_bytes: u64,
}

impl DataDescriptor {
    pub const fn new(id: DataId, size_bytes: u64) -> Self {
        Self { id, size_bytes }
    }

    /// Describes `bytes` by hashing them.
    pub fn of(bytes: &[u8]) -> Self {
        Self::new(DataId::of(bytes), bytes.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DataId {
        let mut bytes = [0u8; DATA_ID_LEN];
        bytes[0] = 0xab;
        bytes[DATA_ID_LEN - 1] = 0x0f;
        DataId::from_bytes(bytes)
    }

    #[test]
    fn display_and_parse_round_trip() {
        let id = sample();
        assert_eq!(id.to_string().parse::<DataId>().unwrap(), id);
        assert!(id.to_string().starts_with("ab"));
        assert!(id.to_string().ends_with("0f"));
    }

    #[test]
    fn parsing_rejects_malformed_input() {
        assert_eq!(
            "abcd".parse::<DataId>(),
            Err(DataIdParseError::WrongLength(4))
        );
        let bad_hex = "zz".to_string() + &"00".repeat(DATA_ID_LEN - 1);
        assert_eq!(
            bad_hex.parse::<DataId>(),
            Err(DataIdParseError::InvalidHex(0))
        );
    }

    #[test]
    fn descriptors_carry_the_size() {
        let descriptor = DataDescriptor::new(sample(), 1024);
        assert_eq!(descriptor.size_bytes, 1024);
        assert_eq!(descriptor.id, sample());
    }

    #[test]
    fn identical_content_gets_identical_ids() {
        assert_eq!(DataId::of(b"payload"), DataId::of(b"payload"));
        assert_ne!(DataId::of(b"payload"), DataId::of(b"payloae"));
    }

    #[test]
    fn ids_are_blake3_digests() {
        let expected = DataId::from_bytes(*blake3::hash(b"aethermesh").as_bytes());
        assert_eq!(DataId::of(b"aethermesh"), expected);

        let descriptor = DataDescriptor::of(b"aethermesh");
        assert_eq!(descriptor.size_bytes, 10);
        assert_eq!(descriptor.id, expected);
    }
}
